------------------------------- MODULE PackLifecycle -------------------------------
(***************************************************************************)
(* S4.12 / S4.12.1 — pack lifecycle formal model (LightFS, branch          *)
(* s31-pack-proto). S4.12.1 fidelity revision:                             *)
(*                                                                         *)
(*   * NodeUnlink now performs the reference decrement AND the pack state  *)
(*     transition inside ONE action, exactly pack_ref_gone_tx (sqlite.rs:  *)
(*     active+refs==0 -> unreferenced; committing re-runs the idempotent   *)
(*     finalize decision; other states untouched).                         *)
(*   * The engine process has a phase (Down/Recovering/Serving): the       *)
(*     stale-pack sweep runs ONLY in the startup-recovery pass or in      *)
(*     gc's stale-pack phase — never "whenever a lock holder feels like   *)
(*     it" during normal serving. A recover pass is ATOMIC (see below).   *)
(*   * The gc process has its own phase (Idle/Sweeping/Draining). Failing *)
(*     to take the engine lock (GcStartBusy) skips the sweep but STILL    *)
(*     drains deletion intents — matching cmd::gc.                         *)
(*                                                                         *)
(* One pack instance is modeled end-to-end through the six persistent row  *)
(* states, the real object behind it, the durable deletion intent, two     *)
(* batch members, the engine lock, backup protection and storage-fault     *)
(* periods. Strict and Write-Back are NOT one merged machine: they are     *)
(* two parameter instances of this module (CONSTANT Mode), each enabling   *)
(* only its own actions — mirroring PackEngine::seal (strict: no inline    *)
(* delete since S4.10) and WriteBackEngine::seal_batch (wb: delete-        *)
(* confirmed row drop since S4.11).                                        *)
(*                                                                         *)
(* Abstractions (declared, not hidden):                                    *)
(*   * One pack at a time.                                                 *)
(*   * SQLite is atomic at transaction granularity: every Rust single-     *)
(*     transaction step is ONE TLA+ action. This means TLC checks the     *)
(*     REACHABLE STATES AFTER each multi-row transaction — it does NOT    *)
(*     verify that real SQLite transactions are atomic; that property is  *)
(*     an implementation obligation mapped to the SQLite layer (I9).      *)
(*   * ONE recover/gc-sweep PASS is atomic. The real recover_stale_packs  *)
(*     iterates row by row and a crash can interrupt a pass; because the  *)
(*     pass is idempotent and the NEXT startup/gc pass redoes the same     *)
(*     adjudication, modeling the pass atomically preserves the           *)
(*     convergence claims checked here.                                   *)
(*   * Crash durability of committed transactions (synchronous=FULL) and   *)
(*     host power loss are OUT OF SCOPE.                                   *)
(*   * Retention time is abstracted: `Unreferenced` is directly enrollable.*)
(*   * Object-store results are modeled independently of the real remote   *)
(*     state: PUT-fail and DELETE-fail may leave the object EITHER state;  *)
(*     only DELETE-confirmed (2xx or idempotent 404) forces Absent; HEAD   *)
(*     is decidable only when the storage is healthy.                      *)
(*   * The engine lock is one enum variable, not the kernel lock file:     *)
(*     mutual exclusion of HOLDERS is structural here; that the OS lock    *)
(*     really excludes a second process and dies with the process is      *)
(*     delegated to the fslock.rs tests (S4.3.4b), not checked by TLC.     *)
(***************************************************************************)
EXTENDS Integers, Naturals

CONSTANT Mode   (* "strict" or "wb" — two parameter instances, never merged *)

MaxRefs == 2

EngineLock == IF Mode = "strict" THEN "Strict" ELSE "Wb"

VARIABLES pack,      (* pack row: Absent|Building|Committing|Active|Unreferenced|Deleting *)
          obj,       (* REAL remote object: Absent|Present (model-omniscient) *)
          refs,      (* live node references to the pack key: 0..MaxRefs *)
          intent,    (* durable deletion intent: None|Pending|Claimed|Failed|Done *)
          mem,       (* wb batch members as a function: {a,b} -> Unbound|Bound|Committed|Dead *)
          lock,      (* engine lock holder: NoLock|Strict|Wb|GC *)
          phase,     (* ENGINE process phase: Down|Recovering|Serving *)
          gcp,       (* GC process phase: Idle|Sweeping|Draining *)
          prot,      (* backup manifest protection: Unprotected|Protected *)
          failProne  (* storage fault period: PUT/DELETE/HEAD unreliable *)
vars == <<pack, obj, refs, intent, mem, lock, phase, gcp, prot, failProne>>

(* Two batch members (wb only), function-valued so actions can quantify. *)
Members == {"a", "b"}
NoBound == \A x \in Members : mem[x] # "Bound"
AllTerminal == \A x \in Members : mem[x] \in {"Committed", "Dead"}

(* finalize decision legality (finalize_batch_tx / wb_finalize_batch_     *)
(* if_terminal): strict has no binding rows, so ANY committing pack can   *)
(* be finalized; wb requires no live (Bound) member. *)
CanFinalize == \/ Mode = "strict"
               \/ NoBound

Init == /\ pack = "Absent"
        /\ obj  = "Absent"
        /\ refs = 0
        /\ intent = "None"
        /\ mem = [x \in Members |-> "Unbound"]
        /\ lock = "NoLock"
        /\ phase = "Down"
        /\ gcp = "Idle"
        /\ prot = "Unprotected"
        /\ failProne = FALSE

(***************************************************************************)
(* The shared sweep decision (recover_stale_packs), factored out because   *)
(* BOTH the startup pass and gc's stale-pack phase apply exactly it.       *)
(* HEAD three-state: exists -> deleting+intent (one tx); confirmed 404 ->  *)
(* row dropped; undecidable (failProne) -> untouched.                      *)
(***************************************************************************)
SweepPackNext ==
    CASE pack = "Building" /\ ~failProne /\ obj = "Present"  -> "Deleting"
      [] pack = "Building" /\ ~failProne /\ obj = "Absent"   -> "Absent"
      [] pack = "Committing" /\ CanFinalize
            -> IF refs > 0 THEN "Active" ELSE "Unreferenced"
      [] OTHER -> pack

SweepIntentNext == IF pack = "Building" /\ ~failProne /\ obj = "Present"
                   THEN "Pending"
                   ELSE intent

(***************************************************************************)
(* Engine lifecycle (engine.rs): build_core acquires the engine lock, runs *)
(* recover_stale_packs ONCE as the startup pass, then serves. The kernel   *)
(* lock dies with the process (S4.3.4b), so EngineCrash releases it and    *)
(* drops the process back to Down; persistent state is untouched. A live   *)
(* engine NEVER re-runs the sweep: Serving has no sweep actions.           *)
(***************************************************************************)
EngineStart == /\ lock = "NoLock" /\ phase = "Down"
               /\ lock' = EngineLock /\ phase' = "Recovering"
               /\ UNCHANGED <<pack, obj, refs, intent, mem, gcp, prot, failProne>>

(* One ATOMIC startup pass (see header): adjudicate the row if HEAD is
   decidable, then serve. An undecidable row is KEPT and is NOT revisited
   by this engine — only the next startup or gc pass looks at it again. *)
RecoverSweep == /\ phase = "Recovering"
                /\ pack' = SweepPackNext
                /\ intent' = SweepIntentNext
                /\ phase' = "Serving"
                /\ UNCHANGED <<obj, refs, mem, lock, gcp, prot, failProne>>

EngineCrash == /\ lock = EngineLock
               /\ lock' = "NoLock" /\ phase' = "Down"
               /\ UNCHANGED <<pack, obj, refs, intent, mem, gcp, prot, failProne>>

(***************************************************************************)
(* GC process lifecycle (cmd::gc): try the engine lock for the stale-pack  *)
(* phase; win -> sweep (atomic pass) then release and drain; lose ->       *)
(* SKIP the sweep and STILL drain deletion intents. Draining = enroll +    *)
(* claim + execute + complete; a run ends (GcEnd) or dies (GcCrash) with   *)
(* intents left for the NEXT explicit gc invocation — there is no daemon.  *)
(***************************************************************************)
GcStart == /\ gcp = "Idle" /\ lock = "NoLock"
           /\ gcp' = "Sweeping" /\ lock' = "GC"
           /\ UNCHANGED <<pack, obj, refs, intent, mem, phase, prot, failProne>>

GcStartBusy == /\ gcp = "Idle" /\ lock # "NoLock"
               /\ gcp' = "Draining"      (* sweep skipped: a live engine owns *)
                                            (* the lock; its own startup recovery *)
                                            (* and the next gc pass cover the rows *)
               /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, phase, prot, failProne>>

GcSweepDone == /\ gcp = "Sweeping"
               /\ pack' = SweepPackNext
               /\ intent' = SweepIntentNext
               /\ gcp' = "Draining" /\ lock' = "NoLock"
                  (* the engine lock is released when the stale-pack phase
                     ends; gc_run's drain needs no engine lock *)
               /\ UNCHANGED <<obj, refs, mem, phase, prot, failProne>>

GcEnd == /\ gcp = "Draining"
         /\ gcp' = "Idle"
         /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, phase, prot, failProne>>
            (* the explicit run exits; unfinished intents wait for the next
               invocation — nothing retries by itself *)

GcCrash == /\ gcp # "Idle"
           /\ gcp' = "Idle"
           /\ lock' = IF lock = "GC" THEN "NoLock" ELSE lock
              (* a gc process crash mid-sweep = that pass never happened;
                 the row stays exactly as it was *)
           /\ UNCHANGED <<pack, obj, refs, intent, mem, phase, prot, failProne>>

(***************************************************************************)
(* Strict seal path (PackEngine::seal). NO inline delete on upload failure *)
(* (S4.10): the building row always survives as the durable tracker.       *)
(* All serving actions premise phase = "Serving": the engine only does     *)
(* client work while serving, and it only sweeps during startup recovery.  *)
(***************************************************************************)
StrictRegister == /\ Mode = "strict" /\ lock = "Strict" /\ phase = "Serving"
                  /\ pack = "Absent"
                  /\ pack' = "Building"
                  /\ intent' = "None" /\ mem' = [x \in Members |-> "Unbound"]
                     (* register_pack: row exists BEFORE the bytes. A fresh
                        pack key starts a fresh lifecycle: the previous
                        instance's Done intent and dead members belonged to
                        a DIFFERENT key and are not this pack's state (the
                        single-instance abstraction resets them here). *)
                  /\ UNCHANGED <<obj, refs, lock, phase, gcp, prot, failProne>>

StrictPutOk == /\ phase = "Serving" /\ pack = "Building" /\ ~failProne
               /\ obj' = "Present"
               /\ UNCHANGED <<pack, refs, intent, mem, lock, phase, gcp, prot, failProne>>

StrictPutUncertain == /\ phase = "Serving" /\ pack = "Building" /\ failProne
                      /\ obj' \in {"Absent", "Present"}
                         (* PUT reported failure; the server may have
                            persisted it. The row is KEPT either way. *)
                      /\ UNCHANGED <<pack, refs, intent, mem, lock, phase, gcp, prot, failProne>>

StrictMark == /\ phase = "Serving" /\ pack = "Building" /\ obj = "Present"
              /\ pack' = "Committing"
                 (* mark_pack_uploaded: single UPDATE, atomic *)
              /\ UNCHANGED <<obj, refs, intent, mem, lock, phase, gcp, prot, failProne>>

StrictCAS == /\ phase = "Serving"
             /\ pack \in {"Committing", "Active"}
             /\ obj = "Present"                 (* bytes are up *)
             /\ refs < MaxRefs
             /\ refs' = refs + 1
             /\ pack' = "Active"
                (* commit_content_cas_pack is ONE statement: the node flip,
                   ref_new and the synchronous finalize (refs>0 -> Active via
                   pack_ref_new_tx/finalize_batch_tx) are a single SQLite
                   transaction. Refusing states other than Committing/Active
                   is what keeps a Deleting pack unreferencable. Strict has
                   no wb binding rows — mem is not a strict concept and
                   stays untouched (this is the Mode difference, kept
                   explicit rather than merged). *)
             /\ UNCHANGED <<obj, intent, mem, lock, phase, gcp, prot, failProne>>

StrictCASConflict == /\ phase = "Serving"
                     /\ pack \in {"Committing", "Active"}
                     /\ obj = "Present"
                     /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, phase, gcp, prot, failProne>>
                        (* CAS loser: dead bytes, no ref. The pack stays
                           Committing at zero refs until a sweep finalizes
                           it (Unreferenced). *)

(***************************************************************************)
(* Write-back batch path (WriteBackEngine::seal_batch). Inline delete, and *)
(* the row is dropped ONLY after the delete is CONFIRMED (S4.11).          *)
(***************************************************************************)
WbRegister == /\ Mode = "wb" /\ lock = "Wb" /\ phase = "Serving"
              /\ pack = "Absent"
              /\ pack' = "Building"
              /\ intent' = "None" /\ mem' = [x \in Members |-> "Unbound"]
                 (* fresh pack key: fresh lifecycle (see StrictRegister) *)
              /\ UNCHANGED <<obj, refs, lock, phase, gcp, prot, failProne>>

WbPutOk == /\ phase = "Serving" /\ pack = "Building" /\ ~failProne
           /\ obj' = "Present"
           /\ UNCHANGED <<pack, refs, intent, mem, lock, phase, gcp, prot, failProne>>

WbPutUncertain == /\ phase = "Serving" /\ pack = "Building" /\ failProne
                  /\ obj' \in {"Absent", "Present"}
                  /\ UNCHANGED <<pack, refs, intent, mem, lock, phase, gcp, prot, failProne>>

WbInlineDeleteConfirmed == /\ phase = "Serving" /\ pack = "Building" /\ failProne
                           /\ obj' = "Absent"
                              (* the inline delete exists ONLY on the upload-
                                 failure arm of seal_batch (S4.11): after a
                                 failed PUT, delete the partial object.
                                 delete contract: 2xx or idempotent 404.
                                 Gating on failProne excludes the bogus
                                 healthy-path interleaving where a confirmed
                                 upload is immediately deleted. *)
                           /\ UNCHANGED <<pack, refs, intent, mem, lock, phase, gcp, prot, failProne>>

WbDropRow == /\ phase = "Serving" /\ pack = "Building" /\ obj = "Absent"
             /\ pack' = "Absent"
                (* S4.11: drop_pack ONLY after a confirmed delete. A failed
                   row-drop is modeled by simply not taking this action —
                   the row waits for the sweep's HEAD 404. *)
             /\ UNCHANGED <<obj, refs, intent, mem, lock, phase, gcp, prot, failProne>>

WbInlineDeleteFailed == /\ phase = "Serving" /\ pack = "Building" /\ failProne
                        /\ obj' \in {"Absent", "Present"}
                           (* S4.11: the building row is KEPT — the tracker
                              must not be dropped on an uncertain delete. *)
                        /\ UNCHANGED <<pack, refs, intent, mem, lock, phase, gcp, prot, failProne>>

WbMark == /\ phase = "Serving" /\ pack = "Building" /\ obj = "Present"
          /\ pack' = "Committing"
          /\ UNCHANGED <<obj, refs, intent, mem, lock, phase, gcp, prot, failProne>>

WbBind == /\ phase = "Serving" /\ lock = "Wb" /\ pack = "Committing"
          /\ \E mm \in Members :
               /\ mem[mm] = "Unbound"
               /\ mem' = [mem EXCEPT ![mm] = "Bound"]
                (* wb_bind_pack_keys: members bind AFTER the PUT succeeded *)
          /\ UNCHANGED <<pack, obj, refs, intent, lock, phase, gcp, prot, failProne>>

WbCAS == /\ phase = "Serving" /\ lock = "Wb"
         /\ pack \in {"Committing", "Active"}
         /\ obj = "Present"
         /\ refs < MaxRefs
         /\ \E mm \in Members :
              /\ mem[mm] = "Bound"
                 (* finalize_batch_if_terminal fires inside the CAS
                    transaction only when no non-terminal member remains:
                    mm becomes Committed HERE, and every other member is
                    UNCHANGED by this action, so the primed check is
                    equivalent to checking the current values of the
                    others. *)
              /\ \A x \in Members : (x # mm) => mem[x] # "Bound"
              /\ mem' = [mem EXCEPT ![mm] = "Committed"]
              /\ refs' = refs + 1
              /\ pack' = IF pack = "Active" THEN "Active"
                         ELSE IF \A x \in Members : mem'[x] # "Bound"
                              THEN "Active"
                              ELSE "Committing"
         /\ UNCHANGED <<obj, intent, lock, phase, gcp, prot, failProne>>

WbCASConflict == /\ phase = "Serving" /\ lock = "Wb"
                 /\ pack \in {"Committing", "Active"}
                 /\ obj = "Present"
                 /\ \E mm \in Members :
                      /\ mem[mm] = "Bound"
                      /\ \A x \in Members : (x # mm) => mem[x] # "Bound"
                      /\ mem' = [mem EXCEPT ![mm] = "Dead"]
                      /\ pack' = IF pack = "Active" THEN "Active"
                                 ELSE IF \A x \in Members : mem'[x] # "Bound"
                                      THEN (IF refs = 0 THEN "Unreferenced"
                                            ELSE "Active")
                                      ELSE "Committing"
                    (* all members terminal at zero refs -> Unreferenced,
                       exactly finalize_batch_tx's decision. *)
                 /\ UNCHANGED <<obj, refs, intent, lock, phase, gcp, prot, failProne>>

(* wb_requeue_draining runs in WriteBackEngine::recover — i.e. in the
   STARTUP-RECOVERY phase, not while serving. A requeued task re-seals
   into a NEW pack; for THIS pack the member is gone for good (Dead is
   the faithful endpoint: this pack never waits on that task again). *)
RecoverRequeue == /\ phase = "Recovering" /\ lock = "Wb"
                  /\ \E mm \in Members :
                       /\ mem[mm] = "Bound"
                       /\ mem' = [mem EXCEPT ![mm] = "Dead"]
                  /\ UNCHANGED <<pack, obj, refs, intent, lock, phase, gcp, prot, failProne>>

(* Environment (wb only — strict has no member state machine at all):
   the requeued task eventually lands in another pack or dead-letters. *)
MemberAbandon == /\ Mode = "wb"
                 /\ \E mm \in Members :
                      /\ mem[mm] = "Unbound"
                      /\ mem' = [mem EXCEPT ![mm] = "Dead"]
                 /\ UNCHANGED <<pack, obj, refs, intent, lock, phase, gcp, prot, failProne>>

(***************************************************************************)
(* NodeUnlink — namespace delete retiring ONE reference, modeled exactly   *)
(* as pack_ref_gone_tx (sqlite.rs:185), which runs INSIDE the caller's     *)
(* transaction: the reference decrement and the pack transition are ONE    *)
(* action, never two independently interleavable steps.                   *)
(*   active + refs hits 0  -> unreferenced (with unref_since)              *)
(*   committing            -> re-run the idempotent finalize decision:     *)
(*                            non-terminal wb members keep it committing;  *)
(*                            otherwise refs>0 -> active, refs==0 ->       *)
(*                            unreferenced — regardless of which member    *)
(*                            event triggered it.                          *)
(*   any other state       -> untouched (building can hold no ref, I2).    *)
(***************************************************************************)
NodeUnlink == /\ refs > 0
              /\ refs' = refs - 1
              /\ pack' = IF pack = "Active" /\ refs = 1
                         THEN "Unreferenced"
                         ELSE IF pack = "Committing" /\ CanFinalize
                              THEN IF refs > 1 THEN "Active" ELSE "Unreferenced"
                              ELSE pack
              /\ UNCHANGED <<obj, intent, mem, lock, phase, gcp, prot, failProne>>

(***************************************************************************)
(* gc drain pipeline (gc_run) — only while a gc process is Draining.       *)
(* The gc pipeline needs NO engine lock for these (state machine guards:   *)
(* Unreferenced packs have no engine-side claimant, Deleting refuses new   *)
(* references), but it only ever runs inside an explicit `lightfs gc`.     *)
(***************************************************************************)
GcEnrollUnreferenced == /\ gcp = "Draining"
                        /\ pack = "Unreferenced" /\ refs = 0
                        /\ prot = "Unprotected"
                        /\ pack' = "Deleting" /\ intent' = "Pending"
                           (* gc_register_pack_deletion: tx re-checks refs=0
                              and flips unreferenced->deleting; a protected
                              key is skipped (guarded by the prot premise) *)
                        /\ UNCHANGED <<obj, refs, mem, lock, phase, gcp, prot, failProne>>

GcClaim == /\ gcp = "Draining"
           /\ intent \in {"Pending", "Failed", "Claimed"}
           /\ intent' = "Claimed"
              (* gc_claim_deletions SELECTs pending/failed and re-queues a
                 crashed executor's deleting rows — so a previously FAILED
                 intent is retried within this run or the next *)
           /\ UNCHANGED <<pack, obj, refs, mem, lock, phase, gcp, prot, failProne>>

GcExecuteOk == /\ gcp = "Draining" /\ intent = "Claimed"
               /\ ~failProne /\ prot = "Unprotected"
               /\ obj' = "Absent"
                  (* protected keys are skipped by the executor — modeled by
                     the prot premise making this action disabled *)
               /\ UNCHANGED <<pack, refs, intent, mem, lock, phase, gcp, prot, failProne>>

GcExecuteFail == /\ gcp = "Draining" /\ intent = "Claimed"
                 /\ failProne /\ prot = "Unprotected"
                 /\ obj' \in {"Absent", "Present"}
                 /\ intent' = "Failed"
                    (* a failed DELETE must NOT be read as absence: the
                       remote object may be in either state; the intent
                       stays durable for the retry *)
                 /\ UNCHANGED <<pack, refs, mem, lock, phase, gcp, prot, failProne>>

GcComplete == /\ gcp = "Draining" /\ intent = "Claimed" /\ obj = "Absent"
              /\ prot = "Unprotected"
              /\ intent' = "Done" /\ pack' = "Absent"
                 (* gc_complete_deletion: the kind='pack' row drop and the
                    'done' flip are ONE transaction. The executor re-checks
                    protection on every pass — a key protected between the
                    delete and the finalize is left claimed for the next
                    pass, never completed (modeled by the prot premise). *)
              /\ UNCHANGED <<obj, refs, mem, lock, phase, gcp, prot, failProne>>

(***************************************************************************)
(* Environment.                                                            *)
(***************************************************************************)
StoreFail == /\ ~failProne /\ failProne' = TRUE
             /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, phase, gcp, prot>>

StoreRecover == /\ failProne /\ failProne' = FALSE
                /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, phase, gcp, prot>>

Protect == /\ prot = "Unprotected"
           /\ intent \in {"None", "Pending", "Claimed", "Failed"}
           /\ prot' = "Protected"
              (* a fresh backup manifest lists keys whose deletion loop has
                 not fully closed; a Done key is in nobody's manifest *)
           /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, phase, gcp, failProne>>

Unprotect == /\ prot = "Protected" /\ prot' = "Unprotected"
                (* backup expiry / prune *)
             /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, phase, gcp, failProne>>

Next == \/ EngineStart \/ RecoverSweep \/ EngineCrash
        \/ GcStart \/ GcStartBusy \/ GcSweepDone \/ GcEnd \/ GcCrash
        \/ StrictRegister \/ StrictPutOk \/ StrictPutUncertain
        \/ StrictMark \/ StrictCAS \/ StrictCASConflict
        \/ WbRegister \/ WbPutOk \/ WbPutUncertain
        \/ WbInlineDeleteConfirmed \/ WbDropRow \/ WbInlineDeleteFailed
        \/ WbMark \/ WbBind
        \/ WbCAS \/ WbCASConflict
        \/ RecoverRequeue \/ MemberAbandon
        \/ NodeUnlink
        \/ GcEnrollUnreferenced \/ GcClaim \/ GcExecuteOk \/ GcExecuteFail
        \/ GcComplete
        \/ StoreFail \/ StoreRecover \/ Protect \/ Unprotect

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety invariants. I1–I5 and I8 (plus the process/lock consistency      *)
(* part of I7) are STATE predicates TLC exhaustively checks; I6/I9 and     *)
(* the kernel-lock part of I7 are recorded as structural constraints or    *)
(* assumptions with implementation obligations (see README — TLC does not  *)
(* check those).                                                           *)
(***************************************************************************)
(* I1 reference safety: a live node reference never points at a deleted,    *)
(* deleting or absent pack object.                                          *)
RefsSafe == (refs > 0) => (obj = "Present" /\ pack \in {"Committing", "Active"})

(* I2 deletion admission: a Deleting pack has zero refs (registration       *)
(* guards); Building rows can never gain a reference (ref_new refuses).     *)
DeletionAdmission == /\ pack = "Deleting" => refs = 0
                     /\ pack = "Building" => refs = 0

(* I3 object tracking (model-scoped packs only): if the object may exist,   *)
(* either the pack row or a durable deletion intent exists.                 *)
ObjectTracked == obj = "Present" => pack # "Absent" \/ intent # "None"

(* I4 intent consistency: deleting <-> non-terminal intent, transactionally  *)
(* coupled; Done implies the row went with it.                               *)
IntentConsistent == /\ intent \in {"Pending", "Claimed", "Failed"} => pack = "Deleting"
                    /\ pack = "Deleting" => intent # "None"
                    /\ intent = "Done" => pack = "Absent"

(* I5 unknown is not missing: the only way to reach (no row, no intent) is   *)
(* through a state where the object was already confirmed absent.            *)
UnknownNotMissing == (pack = "Absent" /\ intent = "None") => obj = "Absent"

(* I8 backup protection: while protected, no deletion loop has completed.    *)
BackupGuard == prot = "Protected" => intent # "Done"

(* Process/lock consistency: the phase variables faithfully encode WHO is    *)
(* doing WHAT — the part of I7 that IS checkable as a state predicate.       *)
ProcessLockInv == /\ (phase # "Down") => (lock = EngineLock)
                  /\ (phase = "Down") => (lock # EngineLock)
                  /\ (lock = "GC") <=> (gcp = "Sweeping")
                  /\ (gcp = "Draining") => (lock # "GC")

SafeInv == /\ RefsSafe /\ DeletionAdmission /\ ObjectTracked
           /\ IntentConsistent /\ UnknownNotMissing /\ BackupGuard
           /\ ProcessLockInv

(***************************************************************************)
(* Liveness variant (checked from PackLifecycle-liveness.cfg only).          *)
(*                                                                           *)
(* Liveness assumptions — each one is an ENVIRONMENT/OPERATIONS assumption,  *)
(* stated explicitly because the implementation does NOT provide the         *)
(* corresponding behavior by itself:                                         *)
(*   * FaultsEventuallyEnd: storage faults are finite. WF alone is not      *)
(*     enough: an infinite StoreFail/StoreRecover alternation interrupts    *)
(*     every ENABLED period, so weak fairness never fires.                   *)
(*   * ProtectionEventuallyEnds: backup retention ends for good.             *)
(*   * SweepOpportunitiesInfinitelyOften: startup-recovery passes or gc     *)
(*     stale-pack phases occur infinitely often. THIS IS LOAD-BEARING: a    *)
(*     live engine NEVER sweeps, so convergence of a stale row requires    *)
(*     the server to (re)start or `lightfs gc` to win the lock — forever.  *)
(*     The implementation's real promise is "the NEXT startup or gc pass    *)
(*     adjudicates the row"; this assumption says there always IS a next.  *)
(*   * NoStuckRecovery: no process stays in its sweep phase forever         *)
(*     (start-crash loops that always interrupt a pass are excluded).       *)
(*   * GcRunsInfinitelyOften: `lightfs gc` is invoked infinitely often.     *)
(*     Explicit CLI path — NOTHING retries deletion automatically between   *)
(*     invocations; a single run leaves failed intents for the next run.    *)
(*   * WF inside phases: a started pass completes; a draining gc advances;  *)
(*     a serving engine progresses on uploads/marks. CAS stays UNFAIR on    *)
(*     purpose: it is client-close driven.                                  *)
(***************************************************************************)
FaultsEventuallyEnd == <>[]~failProne
ProtectionEventuallyEnds == <>[](prot = "Unprotected")
SweepOpportunitiesInfinitelyOften == []<>(phase = "Recovering" \/ gcp = "Sweeping")
NoStuckRecovery == <>[]((phase # "Recovering") /\ (gcp # "Sweeping"))
GcRunsInfinitelyOften == []<>(gcp = "Draining")

FairSpec == Init
         /\ [][Next]_vars
         /\ FaultsEventuallyEnd
         /\ ProtectionEventuallyEnds
         /\ SweepOpportunitiesInfinitelyOften
         /\ NoStuckRecovery
         /\ GcRunsInfinitelyOften
         /\ WF_vars(RecoverSweep) /\ WF_vars(GcSweepDone)
         /\ WF_vars(GcClaim)
         /\ WF_vars(GcExecuteOk)
         /\ WF_vars(GcExecuteFail)
         /\ WF_vars(GcComplete)
         /\ WF_vars(StrictPutOk) /\ WF_vars(StrictMark)
         /\ WF_vars(WbPutOk) /\ WF_vars(WbMark)
         /\ WF_vars(WbInlineDeleteConfirmed) /\ WF_vars(WbDropRow)

(* L1: in a world where sweep opportunities keep coming (see above), a
   building row whose HEAD becomes decidable does not stay building
   forever: the NEXT startup or gc pass adjudicates it. Between passes a
   live engine legitimately leaves the row alone — that is the
   implementation's actual promise, and this property mirrors it. *)
BuildingConverges == (pack = "Building" /\ ~failProne) ~> (pack # "Building")

(* L2: a recorded deletion intent reaches Done — given gc keeps being
   invoked, storage recovers and protection ends. Within ONE gc run a
   failed intent may stay failed; only the next invocation retries it. *)
IntentCompletes == (intent \in {"Pending", "Claimed", "Failed"}) ~> (intent = "Done")

(* L4: a Committing pack eventually leaves Committing — strict: any such
   pack is finalized by the next sweep pass; wb: only once every member is
   terminal, because an engine never finalizes a batch with a live member
   (a Bound member waits for its client's close-driven CAS, and the client
   is an environment the model does not owe fairness). *)
CommittingResolves == (pack = "Committing" /\ (Mode = "strict" \/ AllTerminal))
                      ~> (pack # "Committing")

=============================================================================
