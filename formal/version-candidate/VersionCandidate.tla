------------------------------- MODULE VersionCandidate --------------------------------
(***************************************************************************)
(* S4.16 — durable version-candidate lifecycle (LightFS, branch            *)
(* s31-pack-proto). A SMALL companion model: it does NOT re-model the      *)
(* pack lifecycle (formal/pack-lifecycle); it models exactly the new       *)
(* per-file mechanism — every remote version-key PUT is preceded by a      *)
(* durable candidate row, the CAS consumes that row in the same SQLite     *)
(* transaction that makes the key live, only a CONFIRMED delete or a       *)
(* durable intent may remove the row, and a resolution sweep running at    *)
(* engine startup / offline maintenance settles undecided rows from HEAD   *)
(* evidence.                                                               *)
(*                                                                         *)
(* One write attempt (one version key K) is modeled end-to-end through     *)
(* its candidate row, the real remote object at K, the live-node           *)
(* reference on K, the durable deletion intent, the engine process phase   *)
(* and storage-fault periods. Strict and Write-Back share ONE machine      *)
(* here ON PURPOSE: the mechanism is one shared contract (core.rs          *)
(* cleanup_version_candidate_after_failed_put / resolve_version_candidates,*)
(* MetadataStore::retire_version_candidate_to_gc) wired into all three     *)
(* per-file PUT sites — that unification is what S4.16 delivers.           *)
(*                                                                         *)
(* Abstractions (declared, not hidden):                                    *)
(*   * SQLite is atomic at transaction granularity: every Rust single-     *)
(*     transaction step is ONE TLA+ action (CasCommit, SweepPresentUnref's *)
(*     intent-ensure + row-drop, LiveOwned drop). TLC checks the states    *)
(*     AFTER each transaction; that real SQLite commits atomically is an   *)
(*     implementation obligation (S4.2.1 discipline, file-db tests).       *)
(*   * ONE resolution-sweep pass is atomic. recover_stale_packs-style      *)
(*     iteration is per-key transactions; a pass is idempotent and the     *)
(*     next pass redoes undecided keys, so the atomic pass preserves the   *)
(*     convergence claims checked here.                                    *)
(*   * Crash durability of committed transactions (synchronous=FULL) and   *)
(*     host power loss are OUT OF SCOPE. A Crash changes ONLY the process  *)
(*     phase — every durable row survives.                                 *)
(*   * Object-store results are modeled independently of the real remote   *)
(*     state: a failed PUT/DELETE may leave the object EITHER state; only  *)
(*     a CONFIRMED delete (2xx or idempotent 404) forces Absent; HEAD is   *)
(*     decidable only when storage is healthy (failProne = FALSE). No      *)
(*     claim is made about any concrete S3 error code.                     *)
(*   * Version keys are unique per attempt (new_version_key_for): the      *)
(*     model needs no key algebra — one key instance is the whole story,   *)
(*     and "a CAS can never make an already-intended key live" is then     *)
(*     structural (CasCommit requires the candidate row, which the intent  *)
(*     path consumed).                                                     *)
(*   * Backup protection is upstream of this model: the gc executor        *)
(*     filters protected intents exactly as before (S4.3); the intent      *)
(*     handed over here enters that unchanged pipeline.                    *)
(***************************************************************************)
EXTENDS Integers, Naturals

VARIABLES cand,      (* durable candidate row: "None" | "Tracked" *)
          obj,       (* REAL remote object at K: "Absent" | "Present" (model-omniscient) *)
          live,      (* a live node's object_key == K *)
          intent,    (* durable deletion intent: "None" | "Pending" | "Done" *)
          phase,     (* ENGINE process phase: "Down" | "Serving" *)
          failProne  (* storage fault period: PUT/DELETE/HEAD unreliable *)

vars == <<cand, obj, live, intent, phase, failProne>>

StorageHealthy == failProne = FALSE

(* ---------------------------------------------------------------- init *)

Init ==
    /\ cand = "None"
    /\ obj = "Absent"
    /\ live = FALSE
    /\ intent = "None"
    /\ phase = "Serving"
    /\ failProne \in {TRUE, FALSE}

(* ------------------------------------------------- the write attempt *)

(* Spec §3.1: durable registration BEFORE the remote PUT. The guard
   encodes the key-uniqueness abstraction (new_version_key_for): ONE
   behavior is ONE write attempt on ONE fresh key — an attempt starts
   only from a pristine key state (no object, no live reference, no
   intent, no row), so an intended key can never re-enter the write
   path and later go live. *)
Register ==
    /\ phase = "Serving"
    /\ cand = "None"
    /\ obj = "Absent"
    /\ ~live
    /\ intent = "None"
    /\ cand' = "Tracked"
    /\ UNCHANGED <<obj, live, intent, phase, failProne>>

(* PUT confirmed Ok: bytes are on the remote and the call returned. *)
PutOk ==
    /\ phase = "Serving"
    /\ cand = "Tracked"
    /\ obj' = "Present"
    /\ UNCHANGED <<cand, live, intent, phase, failProne>>

(* PUT returned Err UNCERTAINLY (trait contract: any Err after which the
   remote state is UNSPECIFIED). The object may or may not have landed. *)
PutUncertain ==
    /\ phase = "Serving"
    /\ cand = "Tracked"
    /\ failProne
    /\ obj' \in {"Present", "Absent"}
    /\ UNCHANGED <<cand, live, intent, phase, failProne>>

(* Spec §3.2: the CAS transaction that makes K live consumes the
   candidate IN THE SAME transaction — one action here. *)
CasCommit ==
    /\ phase = "Serving"
    /\ cand = "Tracked"
    /\ obj = "Present"
    /\ live' = TRUE
    /\ cand' = "None"
    /\ UNCHANGED <<obj, intent, phase, failProne>>

(* Spec §3.3 failure-arm cleanup with a CONFIRMED delete (2xx or
   idempotent 404): the row is dropped only now. *)
CleanupConfirmed ==
    /\ phase = "Serving"
    /\ cand = "Tracked"
    /\ obj = "Present"
    /\ StorageHealthy
    /\ obj' = "Absent"
    /\ cand' = "None"
    /\ UNCHANGED <<live, intent, phase, failProne>>

(* The failure-arm delete is itself uncertain (5xx/transport): the row is
   KEPT — the resolution sweep settles it later from HEAD evidence. *)
CleanupUncertain ==
    /\ phase = "Serving"
    /\ cand = "Tracked"
    /\ failProne
    /\ obj' \in {"Present", "Absent"}
    /\ cand' = "Tracked"
    /\ UNCHANGED <<live, intent, phase, failProne>>

(* ------------------------------------------------ the resolution sweep *)

(* One atomic pass — the startup sweep of ANY engine entry (build_core)
   or an offline maintenance pass. The actions carry NO phase guard ON
   PURPOSE: whether a sweep/maintenance process may overlap the serving
   engine is the engine-lock discipline (fslock.rs, open_maintenance),
   which is delegated to the Rust tests, not checked here. What THIS
   model checks is the verdict logic and its atomicity. Verdicts come
   from HEAD evidence only, decided INSIDE one retire transaction
   (retire_version_candidate_to_gc). *)

(* The LiveOwned verdict (HEAD Ok, key live → drop the row WITHOUT an
   intent) is deliberately OUTSIDE this model's scope: under the
   key-uniqueness abstraction a CAS consumes its own candidate atomically,
   so a tracked row for a LIVE key is unreachable here by construction.
   That defensive branch (legacy/foreign rows) is covered by the Rust
   test s416_c14_retire_live_reference_never_enqueues, which pins
   CandidateRetire::LiveOwned = row dropped, no intent. Keeping it in the
   model would fake an unreachable action past the coverage gate — the
   S4.12 discipline says remove it and say so. *)

(* HEAD Ok and no live reference: ensure the durable intent AND drop the
   row in the SAME transaction (IntentEnsured) — no window between them. *)
SweepIntentEnsured ==
    /\ cand = "Tracked"
    /\ obj = "Present"
    /\ ~live
    /\ StorageHealthy
    /\ intent' = "Pending"
    /\ cand' = "None"
    /\ UNCHANGED <<obj, live, phase, failProne>>

(* HEAD 404: the object is confirmed absent — nothing to track. *)
SweepConfirmedAbsent ==
    /\ cand = "Tracked"
    /\ obj = "Absent"
    /\ StorageHealthy
    /\ cand' = "None"
    /\ UNCHANGED <<obj, live, intent, phase, failProne>>

(* With the storage faulted, HEAD is undecidable: NO sweep action is
   enabled, so the row survives every sweep pass — "cannot tell" is never
   resolved by guessing. (Modeled by omission; the liveness assumptions
   below require fault periods to end.) *)

(* ------------------------------------------------------ the gc executor *)

(* The intent enters the EXISTING gc_deletions pipeline; the executor
   deletes remotely on a healthy storage and finalizes. A live key is
   structurally impossible here (IntentSound), and the guard is kept
   explicit: the executor never deletes a live key. *)
GcDrain ==
    /\ intent = "Pending"
    /\ ~live
    /\ StorageHealthy
    /\ obj' = "Absent"
    /\ intent' = "Done"
    /\ UNCHANGED <<cand, live, phase, failProne>>

(* A gc delete attempt under faults is undecided: the intent stays
   pending (the durable row is the retry budget). *)
GcRetry ==
    /\ intent = "Pending"
    /\ failProne
    /\ obj' \in {"Present", "Absent"}
    /\ intent' = "Pending"
    /\ UNCHANGED <<cand, live, phase, failProne>>

(* ------------------------------------------------ the process lifecycle *)

(* A crash loses NOTHING durable: only the phase flips. This action is
   the whole "故障重启不丢追踪" story — the candidate row is in SQLite,
   not in the process. *)
Crash ==
    /\ phase = "Serving"
    /\ phase' = "Down"
    /\ UNCHANGED <<cand, obj, live, intent, failProne>>

(* Restart: engine lock acquired, SQLite opened, startup sweep runs. The
   sweep pass itself is the atomic Sweep* actions enabled once serving
   again (build_core runs it before the server accepts traffic; modeling
   it as an immediately-enabled interleaved action is equivalent because
   the pass is idempotent). *)
Recover ==
    /\ phase = "Down"
    /\ phase' = "Serving"
    /\ UNCHANGED <<cand, obj, live, intent, failProne>>

(* Fault periods begin and end — the environment, not the protocol. *)
FaultOn ==
    /\ ~failProne
    /\ failProne' = TRUE
    /\ UNCHANGED <<cand, obj, live, intent, phase>>

FaultOff ==
    /\ failProne
    /\ failProne' = FALSE
    /\ UNCHANGED <<cand, obj, live, intent, phase>>

(* ---------------------------------------------------------------- spec *)

Next ==
    \/ Register
    \/ PutOk
    \/ PutUncertain
    \/ CasCommit
    \/ CleanupConfirmed
    \/ CleanupUncertain
    \/ SweepIntentEnsured
    \/ SweepConfirmedAbsent
    \/ GcDrain
    \/ GcRetry
    \/ Crash
    \/ Recover
    \/ FaultOn
    \/ FaultOff

Spec == Init /\ [][Next]_vars

(* Fair behaviors: storage fault periods eventually end for good
   (stability assumption, same shape as the pack model); every enabled
   resolution/gc step eventually fires. Because the sweep and gc actions
   carry no phase guard, their ENABLED periods are not cancelled by
   Crash/Recover alternation — weak fairness suffices (the S4.12
   lesson: environment alternation cancels ENABLED periods when the
   action is phase-gated). *)
FairSpec ==
    /\ Spec
    /\ WF_vars(Recover)
    /\ WF_vars(SweepIntentEnsured)
    /\ WF_vars(SweepConfirmedAbsent)
    /\ WF_vars(GcDrain)
    /\ <>[] StorageHealthy

(* ------------------------------------------------------------ invariants *)

TypeOK ==
    /\ cand \in {"None", "Tracked"}
    /\ obj \in {"Absent", "Present"}
    /\ intent \in {"None", "Pending", "Done"}
    /\ live \in BOOLEAN
    /\ phase \in {"Down", "Serving"}
    /\ failProne \in BOOLEAN

(* THE core property: an object that exists, is not live, and has no
   intent MUST still be tracked by its candidate row. No uploaded
   per-file object is ever untracked (spec §1). *)
ObjectTracked ==
    (obj = "Present" /\ ~live /\ intent = "None") => cand = "Tracked"

(* A live key is never tracked as a candidate and never carries a
   deletion intent (spec §6: 不删 live 引用 key). *)
LiveSafe ==
    live => (cand = "None" /\ intent = "None")

(* An intent exists only for keys with no live reference (the retire
   transaction re-checks the predicate; the CAS path consumed its own
   candidate first). *)
IntentSound ==
    intent # "None" => ~live

(* ------------------------------------------------------------- liveness *)

(* Every candidate row eventually leaves the table — consumed by its CAS,
   dropped as LiveOwned, dropped on confirmed absence, or handed to the
   durable intent pipeline. This obligation survives crashes (leads-to,
   not <>[]): each restart re-arms the sweep. *)
CandidateResolves == [](cand = "Tracked" ~> cand = "None")

(* A pending intent is eventually executed by the gc executor. *)
IntentCompletes == [](intent = "Pending" ~> intent = "Done")

(* The three-way terminal choice (spec §6): with the candidate gone and
   the object present, the key is EITHER live-owned OR covered by a
   durable intent — never in limbo. (ObjectTracked states the same thing
   invariantly; this is its temporal reading.) *)
TerminalTrichotomy == [](cand = "None" /\ obj = "Present" /\ ~live => intent # "None")

(* ============================================================ *)
(* Action-coverage note (checked with `tlc2.TLC -coverage 1`): every    *)
(* action above must show total > 0. A coverage run where Register or   *)
(* SweepIntentEnsured shows 0:0 is a MODEL error (the S4.12 lesson: a   *)
(* string-constant typo once produced an all-zero member machine that   *)
(* looked green).                                                       *)
(* ============================================================ *)
====================================================================================================
