------------------------------- MODULE PackLifecycle -------------------------------
(***************************************************************************)
(* S4.12 — pack lifecycle formal model (LightFS, branch s31-pack-proto).   *)
(*                                                                         *)
(* One pack instance is modeled end-to-end through the six persistent row  *)
(* states, the real object behind it, the durable deletion intent, two      *)
(* batch members, the engine lock, backup protection and storage-fault     *)
(* periods. Strict and Write-Back are NOT one merged machine: they are two  *)
(* parameter instances of this module (CONSTANT Mode), each enabling only   *)
(* its own actions — mirroring PackEngine::seal (strict: no inline delete  *)
(* since S4.10) and WriteBackEngine::seal_batch (wb: delete-confirmed row  *)
(* drop since S4.11).                                                      *)
(*                                                                         *)
(* Abstractions (declared, not hidden):                                    *)
(*   * One pack at a time. Packs never interact except through the node    *)
(*     refs counted here; a second concurrent pack is the same machine     *)
(*     with fresh variables and cannot violate a per-pack invariant more   *)
(*     strongly.                                                           *)
(*   * SQLite is atomic at transaction granularity: every Rust single-     *)
(*     transaction step (CAS+refcount+finalize; building->deleting flip    *)
(*     +intent insert; complete = row drop + done) is ONE TLA+ action.     *)
(*     Crash durability of committed transactions (synchronous=FULL) and   *)
(*     host power loss are OUT OF SCOPE.                                   *)
(*   * Retention time is abstracted: `Unreferenced` is directly enrollable *)
(*     (the retention cutoff is a pure time gate, verified by unit tests,  *)
(*     not a correctness mechanism).                                       *)
(*   * Object-store results are modeled independently of the real remote   *)
(*     state: PUT-fail and DELETE-fail may leave the object EITHER state;  *)
(*     only DELETE-confirmed (2xx or idempotent 404) forces Absent; HEAD   *)
(*     is decidable only when the storage is healthy.                      *)
(*                                                                         *)
(* `failProne` models a storage-fault period: PUT/DELETE return errors     *)
(* (with uncertain remote effects) and HEAD is undecidable. StoreFail /    *)
(* StoreRecover are environment actions — liveness properties assume       *)
(* storage eventually recovers (SF).                                       *)
(***************************************************************************)
EXTENDS Integers, Naturals

CONSTANT Mode   (* "strict" or "wb" — two parameter instances, never merged *)

MaxRefs == 2

EngineLock == IF Mode = "strict" THEN "Strict" ELSE "Wb"

VARIABLES pack,      (* pack row: Absent|Building|Committing|Active|Unreferenced|Deleting *)
          obj,       (* REAL remote object: Absent|Present (model-omniscient) *)
          refs,      (* live node references to the pack key: 0..MaxRefs *)
          intent,    (* durable deletion intent: None|Pending|Claimed|Failed|Done *)
          mem,       (* batch members as a function: {a,b} -> Unbound|Bound|Committed|Dead *)
          lock,      (* engine lock: NoLock|Strict|Wb|GC *)
          prot,      (* backup manifest protection: Unprotected|Protected *)
          failProne  (* storage fault period: PUT/DELETE/HEAD unreliable *)
vars == <<pack, obj, refs, intent, mem, lock, prot, failProne>>

(* Two batch members, function-valued so actions can quantify over them. *)
Members == {"a", "b"}
NoBound == \A x \in Members : mem[x] # "Bound"

(* finalize_batch_if_terminal legality: strict has no wb binding rows, so a
   sweep may finalize any `Committing` pack; wb needs no Bound member left. *)
CanFinalize == \/ Mode = "strict"
               \/ NoBound

Init == /\ pack = "Absent"
        /\ obj  = "Absent"
        /\ refs = 0
        /\ intent = "None"
        /\ mem = [x \in Members |-> "Unbound"]
        /\ lock = "NoLock"
        /\ prot = "Unprotected"
        /\ failProne = FALSE

(***************************************************************************)
(* Engine lifecycle. The kernel lock dies with the process (S4.3.4b), so    *)
(* EngineCrash only releases the lock; persistent state is untouched.       *)
(***************************************************************************)
EngineStart == /\ lock = "NoLock"
               /\ lock' = EngineLock
               /\ UNCHANGED <<pack, obj, refs, intent, mem, prot, failProne>>

EngineCrash == /\ lock = EngineLock
               /\ lock' = "NoLock"
               /\ UNCHANGED <<pack, obj, refs, intent, mem, prot, failProne>>

(* cmd::gc's stale-pack phase: FileLock::try_acquire — succeeds only when
   the engine lock is free (S4.10). Losing the race is modeled by the action
   being disabled; the gc pipeline continues WITHOUT the sweep. *)
GcLock == /\ lock = "NoLock"
          /\ lock' = "GC"
          /\ UNCHANGED <<pack, obj, refs, intent, mem, prot, failProne>>

GcUnlock == /\ lock = "GC"
            /\ lock' = "NoLock"
            /\ UNCHANGED <<pack, obj, refs, intent, mem, prot, failProne>>

(***************************************************************************)
(* Strict seal path (PackEngine::seal). NO inline delete on upload failure  *)
(* (S4.10): the building row always survives as the durable tracker.        *)
(***************************************************************************)
StrictRegister == /\ Mode = "strict" /\ lock = "Strict"
                  /\ pack = "Absent"
                  /\ pack' = "Building"
                  /\ intent' = "None" /\ mem' = [x \in Members |-> "Unbound"]
                     (* register_pack: row exists BEFORE the bytes. A fresh
                        pack key starts a fresh lifecycle: the previous
                        instance's Done intent and dead members belonged to
                        a DIFFERENT key and are not this pack's state (the
                        single-instance abstraction resets them here). *)
                  /\ UNCHANGED <<obj, refs, lock, prot, failProne>>

StrictPutOk == /\ lock = "Strict" /\ pack = "Building" /\ ~failProne
               /\ obj' = "Present"
               /\ UNCHANGED <<pack, refs, intent, mem, lock, prot, failProne>>

StrictPutUncertain == /\ lock = "Strict" /\ pack = "Building" /\ failProne
                      /\ obj' \in {"Absent", "Present"}
                         (* PUT reported failure; the server may have
                            persisted it. The row is KEPT either way. *)
                      /\ UNCHANGED <<pack, refs, intent, mem, lock, prot, failProne>>

StrictMark == /\ lock = "Strict" /\ pack = "Building" /\ obj = "Present"
              /\ pack' = "Committing"
                 (* mark_pack_uploaded: single UPDATE, atomic *)
              /\ UNCHANGED <<obj, refs, intent, mem, lock, prot, failProne>>

StrictCAS == /\ lock = "Strict"
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
             /\ UNCHANGED <<obj, intent, mem, lock, prot, failProne>>

StrictCASConflict == /\ lock = "Strict"
                     /\ pack \in {"Committing", "Active"}
                     /\ obj = "Present"
                     /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, prot, failProne>>
                        (* CAS loser: dead bytes, no ref. The pack stays
                           Committing at zero refs until a sweep finalizes
                           it (Unreferenced). *)

(***************************************************************************)
(* Write-back batch path (WriteBackEngine::seal_batch). Inline delete, and  *)
(* the row is dropped ONLY after the delete is CONFIRMED (S4.11).           *)
(***************************************************************************)
WbRegister == /\ Mode = "wb" /\ lock = "Wb"
              /\ pack = "Absent"
              /\ pack' = "Building"
              /\ intent' = "None" /\ mem' = [x \in Members |-> "Unbound"]
                 (* fresh pack key: fresh lifecycle (see StrictRegister) *)
              /\ UNCHANGED <<obj, refs, lock, prot, failProne>>

WbPutOk == /\ lock = "Wb" /\ pack = "Building" /\ ~failProne
           /\ obj' = "Present"
           /\ UNCHANGED <<pack, refs, intent, mem, lock, prot, failProne>>

WbPutUncertain == /\ lock = "Wb" /\ pack = "Building" /\ failProne
                  /\ obj' \in {"Absent", "Present"}
                  /\ UNCHANGED <<pack, refs, intent, mem, lock, prot, failProne>>

WbInlineDeleteConfirmed == /\ lock = "Wb" /\ pack = "Building" /\ failProne
                           /\ obj' = "Absent"
                              (* the inline delete exists ONLY on the upload-
                                 failure arm of seal_batch (S4.11): after a
                                 failed PUT, delete the partial object.
                                 delete contract: 2xx or idempotent 404.
                                 Gating on failProne excludes the bogus
                                 healthy-path interleaving where a confirmed
                                 upload is immediately deleted, which would
                                 starve WbMark's fairness period forever. *)
                           /\ UNCHANGED <<pack, refs, intent, mem, lock, prot, failProne>>

WbDropRow == /\ lock = "Wb" /\ pack = "Building" /\ obj = "Absent"
             /\ pack' = "Absent"
                (* S4.11: drop_pack ONLY after a confirmed delete. A failed
                   row-drop is modeled by simply not taking this action —
                   the row waits for the sweep's HEAD 404. *)
             /\ UNCHANGED <<obj, refs, intent, mem, lock, prot, failProne>>

WbInlineDeleteFailed == /\ lock = "Wb" /\ pack = "Building" /\ failProne
                        /\ obj' \in {"Absent", "Present"}
                           (* S4.11: the building row is KEPT — the tracker
                              must not be dropped on an uncertain delete. *)
                        /\ UNCHANGED <<pack, refs, intent, mem, lock, prot, failProne>>

WbMark == /\ lock = "Wb" /\ pack = "Building" /\ obj = "Present"
          /\ pack' = "Committing"
          /\ UNCHANGED <<obj, refs, intent, mem, lock, prot, failProne>>

WbBind == /\ lock = "Wb" /\ pack = "Committing"
             /\ \E mm \in Members :
                  /\ mem[mm] = "Unbound"
                  /\ mem' = [mem EXCEPT ![mm] = "Bound"]
                (* wb_bind_pack_keys: members bind AFTER the PUT succeeded *)
             /\ UNCHANGED <<pack, obj, refs, intent, lock, prot, failProne>>

WbCAS == /\ lock = "Wb"
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
         /\ UNCHANGED <<obj, intent, lock, prot, failProne>>

WbCASConflict == /\ lock = "Wb"
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
                       exactly finalize_batch_tx's decision; the other
                       members are unchanged by this action. *)
                 /\ UNCHANGED <<obj, refs, intent, lock, prot, failProne>>

RecoverRequeue == /\ lock = "Wb"
                  /\ \E mm \in Members :
                       /\ mem[mm] = "Bound"
                       /\ mem' = [mem EXCEPT ![mm] = "Dead"]
                        (* wb_requeue_draining: the task row goes back to
                           queued and will re-seal into a NEW pack. For THIS
                           pack the member is gone for good — modeling it as
                           Unbound (re-bindable) would allow an infinite
                           Bind/Requeue alternation that starves the CAS's
                           fairness period; Dead is the faithful endpoint:
                           this pack never waits on that task again. *)
                     /\ UNCHANGED <<pack, obj, refs, intent, lock, prot, failProne>>

MemberAbandon == /\ \E mm \in Members :
                       /\ mem[mm] = "Unbound"
                       /\ mem' = [mem EXCEPT ![mm] = "Dead"]
                       (* the requeued task eventually lands in another pack
                          or dead-letters; from this pack's perspective the
                          member is gone (environment, no lock needed) *)
                    /\ UNCHANGED <<pack, obj, refs, intent, lock, prot, failProne>>

(***************************************************************************)
(* Shared stale-pack sweep — recover_stale_packs, invoked under the engine  *)
(* lock by a starting/recovering engine (Strict/Wb) AND by `lightfs gc`'s   *)
(* stale-pack phase, which try-acquires the SAME lock first (S4.10). HEAD   *)
(* three-state: exists -> deleting+intent (one transaction); confirmed      *)
(* 404 -> row dropped; undecidable -> untouched.                            *)
(***************************************************************************)
SweepBuildingExists == /\ lock \in {"Strict", "Wb", "GC"}
                       /\ pack = "Building"
                       /\ ~failProne          (* HEAD decidable *)
                       /\ obj = "Present"
                       /\ pack' = "Deleting" /\ intent' = "Pending"
                          (* gc_register_stale_pack_deletion: predicate
                             re-check + flip + intent insert, one tx *)
                       /\ UNCHANGED <<obj, refs, mem, lock, prot, failProne>>

SweepBuildingMissing == /\ lock \in {"Strict", "Wb", "GC"}
                        /\ pack = "Building"
                        /\ ~failProne
                        /\ obj = "Absent"
                        /\ pack' = "Absent"
                           (* definitive 404: drop the row, no intent *)
                        /\ UNCHANGED <<obj, refs, intent, mem, lock, prot, failProne>>

SweepFinalize == /\ lock \in {"Strict", "Wb", "GC"}
                 /\ pack = "Committing" /\ CanFinalize
                 /\ pack' = IF refs > 0 THEN "Active" ELSE "Unreferenced"
                    (* committing_packs() -> wb_finalize_batch_if_terminal *)
                 /\ UNCHANGED <<obj, refs, intent, mem, lock, prot, failProne>>

(***************************************************************************)
(* gc enroll / claim / execute — the cmd::gc pipeline needs NO engine lock   *)
(* for these (the state machine guards them: Unreferenced packs have no     *)
(* engine-side claimant, Deleting refuses new references).                  *)
(***************************************************************************)
GcEnrollUnreferenced == /\ pack = "Unreferenced" /\ refs = 0
                        /\ prot = "Unprotected"
                        /\ pack' = "Deleting" /\ intent' = "Pending"
                           (* gc_register_pack_deletion: tx re-checks refs=0
                              and flips unreferenced->deleting; a protected
                              key is skipped (guarded by the prot premise) *)
                        /\ UNCHANGED <<obj, refs, mem, lock, prot, failProne>>

GcClaim == /\ intent \in {"Pending", "Failed", "Claimed"}
           /\ intent' = "Claimed"
              (* gc_claim_deletions: pending/failed claimed; a crashed
                 executor's 'deleting' row re-enters the queue — idempotent *)
           /\ UNCHANGED <<pack, obj, refs, mem, lock, prot, failProne>>

GcExecuteOk == /\ intent = "Claimed" /\ ~failProne /\ prot = "Unprotected"
               /\ obj' = "Absent"
                  (* protected keys are skipped by the executor — modeled by
                     the prot premise making this action disabled *)
               /\ UNCHANGED <<pack, refs, intent, mem, lock, prot, failProne>>

GcExecuteFail == /\ intent = "Claimed" /\ failProne /\ prot = "Unprotected"
                 /\ obj' \in {"Absent", "Present"}
                 /\ intent' = "Failed"
                    (* a failed DELETE must NOT be read as absence: the
                       remote object may be in either state; the intent
                       stays durable for the retry *)
                 /\ UNCHANGED <<pack, refs, mem, lock, prot, failProne>>

GcComplete == /\ intent = "Claimed" /\ obj = "Absent"
              /\ prot = "Unprotected"
              /\ intent' = "Done" /\ pack' = "Absent"
                 (* gc_complete_deletion: the kind='pack' row drop and the
                    'done' flip are ONE transaction. The executor re-checks
                    protection on every pass — a key protected between the
                    delete and the finalize is left claimed for the next
                    pass, never completed (modeled by the prot premise). If
                    the process dies before it, GcClaim re-enters and
                    GcExecuteOk's 404 converges. *)
              /\ UNCHANGED <<obj, refs, mem, lock, prot, failProne>>

(***************************************************************************)
(* Environment.                                                              *)
(***************************************************************************)
StoreFail == /\ ~failProne /\ failProne' = TRUE
             /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, prot>>

StoreRecover == /\ failProne /\ failProne' = FALSE
                /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, prot>>

Protect == /\ prot = "Unprotected"
           /\ intent \in {"None", "Pending", "Claimed", "Failed"}
           /\ prot' = "Protected"
              (* a fresh backup manifest lists keys whose deletion loop has
                 not fully closed; a Done key is in nobody's manifest *)
           /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, failProne>>

Unprotect == /\ prot = "Protected" /\ prot' = "Unprotected"
                (* backup expiry / prune *)
             /\ UNCHANGED <<pack, obj, refs, intent, mem, lock, failProne>>

NodeUnlink == /\ refs > 0 /\ refs' = refs - 1
              /\ UNCHANGED <<pack, obj, intent, mem, lock, prot, failProne>>
                 (* namespace delete retires this pack's last reference *)

Next == \/ EngineStart \/ EngineCrash \/ GcLock \/ GcUnlock
        \/ StrictRegister \/ StrictPutOk \/ StrictPutUncertain
        \/ StrictMark \/ StrictCAS \/ StrictCASConflict
        \/ WbRegister \/ WbPutOk \/ WbPutUncertain
        \/ WbInlineDeleteConfirmed \/ WbDropRow \/ WbInlineDeleteFailed
        \/ WbMark \/ WbBind
        \/ WbCAS \/ WbCASConflict
        \/ RecoverRequeue \/ MemberAbandon
        \/ SweepBuildingExists \/ SweepBuildingMissing \/ SweepFinalize
        \/ GcEnrollUnreferenced \/ GcClaim \/ GcExecuteOk \/ GcExecuteFail
        \/ GcComplete
        \/ StoreFail \/ StoreRecover \/ Protect \/ Unprotect \/ NodeUnlink

Spec == Init /\ [][Next]_vars

(***************************************************************************)
(* Safety invariants (S4.12 §4, I1..I9).                                    *)
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

(* I5 unknown is not missing: encoded as action premises (no sweep decision  *)
(* without a decidable HEAD; no row drop after a FAILED delete). Its         *)
(* contrapositive is checkable: the only way to reach (no row, no intent)    *)
(* is through a state where the object was already confirmed absent.         *)
UnknownNotMissing == (pack = "Absent" /\ intent = "None") => obj = "Absent"

(* I8 backup protection: while protected, no deletion loop has completed.    *)
BackupGuard == prot = "Protected" => intent # "Done"

SafeInv == /\ RefsSafe /\ DeletionAdmission /\ ObjectTracked
           /\ IntentConsistent /\ UnknownNotMissing /\ BackupGuard

(***************************************************************************)
(* Liveness variant (checked from PackLifecycle-liveness.cfg only).          *)
(*                                                                           *)
(* Fairness / stability assumptions, stated EXPLICITLY (no unconditional     *)
(* convergence is claimed without them). Recoverability is an ASSUMPTION,    *)
(* not a default:                                                            *)
(*   * storage faults are finite: eventually the store is healthy forever    *)
(*     (FaultsEventuallyEnd). WF alone is NOT enough — an infinite           *)
(*     StoreFail/StoreRecover alternation keeps interrupting every           *)
(*     ENABLED period of the work actions, so weak fairness never fires      *)
(*     (TLC counter-example in examples/L1-unguarded-counterexample.trace).  *)
(*   * backup protection eventually ends for good                            *)
(*     (ProtectionEventuallyEnds).                                           *)
(*   * the gc deletion loop eventually runs (claim/execute/complete) — WF.   *)
(*   * a sweeper that HOLDS the lock eventually performs its sweep/put/      *)
(*     mark/drop steps — WF (all guarded by lock # "NoLock" in their         *)
(*     premises, so a crashed system owes nothing).                          *)
(*   * crashes are finite: eventually one sweeper stays alive for good       *)
(*     (CrashEventuallyStops). Even with WF everywhere, an infinite          *)
(*     restart-crash alternation advances at most one step per epoch and     *)
(*     can park a building row forever — a liveness lead-to obligation,     *)
(*     once triggered, survives the crash that cancels the fairness debt.    *)
(*                                                                           *)
(* Known NON-properties (documented counter-example, TLC trace saved):       *)
(*   `pack = "Building" ~> pack # "Building"` is FALSE in this model: after  *)
(*   EngineCrash with no restart and no gc run, a stale building row legal-  *)
(*   ly stays forever — the implementation promises convergence only when a  *)
(*   sweeper runs (engine recover / gc stale-pack phase). L1 below is the    *)
(*   faithful, guarded version. A committed member also legitimately loses   *)
(*   its reference via NodeUnlink, so "Committed => refs >= 1" holds only    *)
(*   at commit instants and is checked as safety (I1), not liveness.         *)
(***************************************************************************)
FaultsEventuallyEnd == <>[]~failProne
ProtectionEventuallyEnds == <>[](prot = "Unprotected")
CrashEventuallyStops == <>[](lock # "NoLock")

FairSpec == Init
         /\ [][Next]_vars
         /\ FaultsEventuallyEnd
         /\ ProtectionEventuallyEnds
         /\ CrashEventuallyStops
         /\ WF_vars(GcClaim)
         /\ WF_vars(GcExecuteOk)
         /\ WF_vars(GcExecuteFail)
         /\ WF_vars(GcComplete)
         /\ WF_vars(SweepBuildingMissing)
         /\ WF_vars(SweepBuildingExists)
         /\ WF_vars(SweepFinalize)
         /\ WF_vars(StrictPutOk) /\ WF_vars(StrictMark)
         /\ WF_vars(WbPutOk) /\ WF_vars(WbMark)
         /\ WF_vars(WbInlineDeleteConfirmed) /\ WF_vars(WbDropRow)

(* L1 (guarded): while a sweeper holds the lock and storage is decidable, a
   building row does not stay building. *)
BuildingConverges == (pack = "Building" /\ lock # "NoLock" /\ ~failProne)
                     ~> (pack # "Building")

(* L2: a recorded deletion intent completes once storage recovers and the
   key is no longer protected. The gc loop needs no engine lock. *)
IntentCompletes == (intent \in {"Pending", "Claimed", "Failed"}) ~> (intent = "Done")

(* ARCHIVED (documented counter-example; not part of FairSpec checks):
   the unguarded version of L1 that TLC refuted. *)
BuildingConvergesUnconditional == (pack = "Building") ~> (pack # "Building")

(* L4 (guarded): under a LIVE sweeper, a Committing pack eventually leaves
   Committing. The premise is Mode-sensitive BY DESIGN:
   * strict: the sweep may finalize any Committing pack (no binding rows).
   * wb: an engine never finalizes a batch with a live member — a Bound
     member waits for its client's close-driven CAS, and the client is an
     environment the model does not owe fairness. Convergence is promised
     only once every member is terminal (AllTerminal). A TLC counterexample
     with a stuttering Bound member (examples/L4-wb-counterexample.trace)
     pinned this premise down. *)
AllTerminal == \A x \in Members : mem[x] \in {"Committed", "Dead"}
CommittingResolves == (pack = "Committing" /\ lock # "NoLock"
                       /\ (Mode = "strict" \/ AllTerminal))
                      ~> (pack # "Committing")

=============================================================================
