# Pack Lifecycle — TLA+ Formal Model (S4.12)

An executable TLA+ model of the LightFS pack lifecycle (branch `s31-pack-proto`,
baseline `d3b7acd`), checked with TLC. It encodes the six persistent pack-row
states, the real remote object, the durable deletion intent, two batch
members, the engine lock, backup protection, and storage-fault periods — and
checks the S4.12 invariants I1–I9 against both engine paths (Strict Pack and
Write-Back Batch-Pack) as two separate model instances.

**The model found real gaps during its own construction** — see
[Counter-examples](#counter-examples). None of them required a production-code
change (all were model-fidelity bugs, verified against the code), and the audit
conclusion for the implementation itself is in the S4.12 report (internal docs).

## Tool versions

| Tool    | Version |
|---------|---------|
| TLC     | tla2tools 2.0, build 2026.10.15.145915 (rev 932d35e), from the `tlaplus/tlaplus` GitHub releases (`tla2tools.jar`; not bundled in this repo — place it next to the model or adjust `-cp`) |
| Java    | Eclipse Adoptium JRE 21.0.12.1 (aarch64) |
| Checker | breadth-first search, fingerprint set MSBDiskFPSet, 1 worker, -Xmx4g |

## How to run

```sh
export JAVA_HOME=/path/to/jre-21
# Safety (both modes; ~1 s each)
java -Xmx4g -cp tla2tools.jar tlc2.TLC -cleanup -config PackLifecycle.cfg PackLifecycle.tla
sed 's/Mode = "strict"/Mode = "wb"/' PackLifecycle.cfg > /tmp/pack-wb.cfg
java -Xmx4g -cp tla2tools.jar tlc2.TLC -cleanup -config /tmp/pack-wb.cfg PackLifecycle.tla
# Liveness (both modes; ~1 s each)
java -Xmx4g -cp tla2tools.jar tlc2.TLC -cleanup -config PackLifecycle-liveness.cfg PackLifecycle.tla
sed 's/Mode = "strict"/Mode = "wb"/' PackLifecycle-liveness.cfg > /tmp/liveness-wb.cfg
java -Xmx4g -cp tla2tools.jar tlc2.TLC -cleanup -config /tmp/liveness-wb.cfg PackLifecycle.tla
# Action-coverage sanity (every in-mode action must have total > 0)
java -Xmx4g -cp tla2tools.jar tlc2.TLC -coverage 1 -cleanup -config PackLifecycle.cfg PackLifecycle.tla
```

Syntax-only check: `java -cp tla2tools.jar tla2sany.SANY PackLifecycle.tla`.

The model is pure TLA+; it does not compile or load any Rust.

## Scope and declared abstractions

Declared in the module header, repeated here because they bound every claim
below:

- **One pack at a time.** Packs do not interact except through node refcounts.
- **SQLite is atomic at transaction granularity.** Every single-transaction
  Rust step (CAS + refcount + finalize; building→deleting flip + intent
  insert; completion = row drop + done) is ONE TLA+ action. `synchronous=FULL`
  crash durability of committed transactions and host power loss are **out of
  scope** (they are verified by the R15/R16/R17 gates and S4.4 tests, not
  here).
- **Retention time is abstracted**: `Unreferenced` is directly enrollable.
- **Object-store results are modeled independently of the real remote state**:
  a failed PUT or failed DELETE may leave the object in EITHER state; only a
  *confirmed* delete (2xx or idempotent 404) forces `Absent`; HEAD is
  decidable only when storage is healthy.
- **Strict and Write-Back are never merged**: `CONSTANT Mode` picks one
  engine; the other's actions are structurally disabled (verified: 0
  coverage for the off-mode actions).
- **Two batch members**, function-valued `mem: {a,b} -> Unbound|Bound|Committed|Dead`.
  Strict does not touch `mem` at all (strict has no binding rows — S4.10 audit).
- **Model-scoped**: I3/I5 cover *this* pack instance only. Historical
  row-less orphans from before S4.10/S4.11 are out of scope by definition.
- **Not a proof of the Rust implementation**: this is a finite-state model of
  the *design*, mechanically checked by TLC within these bounds. Unit tests
  are not state-space checks and this is not a mathematical proof of the code.

## Results

All four runs completed with **no error found** (~1 s each):

| Run | Mode | Property kind | States generated | Distinct states | Result |
|-----|------|---------------|------------------|-----------------|--------|
| 1 | strict | safety (I1–I6, I8, I9) | 3 537 | 648 | pass |
| 2 | wb | safety | 3 457 | 660 | pass |
| 3 | strict | liveness (L1, L2, L4) | 3 537 | 648 | pass |
| 4 | wb | liveness | 3 457 | 660 | pass |

Model parameters: `MaxRefs = 2`, members = {a, b}, one pack instance,
`Mode ∈ {"strict", "wb"}` (two separate runs).

### Invariants (safety), both modes

| # | Invariant | Statement (model operator) | Result |
|---|-----------|---------------------------|--------|
| I1 | RefsSafe | `refs > 0 => obj = Present /\ pack ∈ {Committing, Active}` — a live reference never points at a deleted/deleting/absent object | pass |
| I2 | DeletionAdmission | `pack = Deleting => refs = 0` and `pack = Building => refs = 0` (ref_new refuses Building — `pack_ref_new_tx` accepts only committing/active) | pass |
| I3 | ObjectTracked | `obj = Present => pack # Absent \/ intent # None` — a possibly-live object always has a row or a durable intent (model-scoped; historical row-less orphans excluded) | pass |
| I4 | IntentConsistent | Pending/Claimed/Failed ⇒ Deleting; Deleting ⇒ intent # None; Done ⇒ pack = Absent | pass |
| I5 | UnknownNotMissing | `(pack = Absent /\ intent = None) => obj = Absent` — the only way to lose both trackers is a confirmed absence; encoded by action premises (no sweep decision without decidable HEAD; no row drop after a failed delete) | pass |
| I6 | recovery idempotence | modeled by construction: Sweep actions are re-executable, GcClaim accepts Pending/Failed/Claimed (a crashed executor's row re-enters the queue), Register resets to a fresh lifecycle | pass |
| I7 | engine-lock mutual exclusion | single `lock` variable; `GcLock` (gc's `FileLock::try_acquire`) succeeds only from NoLock; EngineStart likewise; a second holder is structurally impossible in the model, matching fslock semantics | pass |
| I8 | BackupGuard | `prot = Protected => intent # Done` — no deletion completes while protected; GcComplete/GcExecute/GcEnroll all premise `prot = Unprotected` (the executor re-checks protection every pass) | pass |
| I9 | SQLite atomicity boundary | multi-row couplings (CAS+ref+finalize, flip+intent, complete=row+done) are single actions; no action interleaves inside them | pass |

### Liveness, and the assumptions it needs

`PackLifecycle-liveness.cfg` checks `FairSpec` with properties:

- **L1 `BuildingConverges` (guarded)**: while a sweeper holds the lock and
  storage is decidable, a building row does not stay building.
- **L2 `IntentCompletes`**: a recorded deletion intent eventually reaches Done.
- **L4 `CommittingResolves` (Mode-sensitive)**: under a live sweeper, a
  Committing pack eventually leaves Committing — for any pack in strict; in
  wb only once every member is terminal (`AllTerminal`).

Assumptions, all **explicit and necessary** (each was proven necessary by a
TLC counter-example — see below):

1. `FaultsEventuallyEnd == <>[]~failProne` — storage faults are finite.
   Weak fairness alone is NOT enough: an infinite StoreFail/StoreRecover
   alternation keeps interrupting every ENABLED period of the work actions, so
   weak fairness never fires. Recoverability is an assumption, not a default.
2. `ProtectionEventuallyEnds == <>[](prot = "Unprotected")` — backup
   retention is finite.
3. `CrashEventuallyStops == <>[](lock # "NoLock")` — eventually one sweeper
   stays alive for good. A lead-to obligation, once triggered, survives the
   crash that cancels the fairness debt; infinite restart-crash alternation
   can park a building row forever.
4. WF on the gc loop (claim/execute/complete), on sweeps, and on the
   engine-side steps guarded by `lock # "NoLock"`. The engine owes nothing
   while crashed. **No fairness is assumed for CAS**: it is client-close
   driven (see L4's wb premise).

Known NON-properties (asserted false on purpose, documented with traces):

- `pack = "Building" ~> pack # "Building"` (unguarded) is **false**: after a
  crash with no restart and no gc run, a stale building row legally stays
  forever. Trace: `examples/L1-unguarded-counterexample.trace`.
- `mem[x] = "Committed" => refs ≥ 1` holds only at commit instants; a
  namespace unlink legitimately retires the reference. It is a safety fact
  (I1), not a liveness property, and is checked as such.

### Action coverage (model fidelity gate)

`-coverage 1` was used to require that every action reachable in a mode is
actually exercised — after fixing the bugs below, every in-mode action has
`total > 0` in both modes; the only `0:0` entries are the other mode's actions
and `RecoverRequeue` under strict (premised on `lock = "Wb"`). A "all green"
result before this gate was **meaningless**: the first coverage run exposed a
broken member state machine (below).

## Counter-examples (all resolved; model corrected, code unchanged)

Each counter-example was saved, mapped to the design, and adjudicated. None
exposed an implementation defect; all were model bugs or over-strong
property statements. Production code was **not** modified for the model to
pass (S4.12 ground rule).

| # | What TLC refuted | Root cause | Adjudication / fix |
|---|------------------|-----------|--------------------|
| 1 | `IntentConsistent` (safety): `pack = Building /\ intent = Done` | Single-instance model reuses its variables across pack keys: a fresh Register inherited the previous instance's Done intent | Fresh key = fresh lifecycle: Register resets `intent' = None` and members to Unbound. Matches the code: a new key gets new rows; the old Done intent belongs to the old key. |
| 2 | `BackupGuard` (safety): `prot = Protected /\ intent = Done` | `GcComplete` lacked a protection premise | The real executor re-checks protection every pass and leaves a protected key claimed. Added `prot = "Unprotected"` premise to GcComplete (and to GcExecuteOk/GcExecuteFail/GcEnrollUnreferenced). |
| 3 | Coverage gate: `WbBind/WbCAS/WbCASConflict/RecoverRequeue/MemberAbandon/NodeUnlink/GcUnlock` all `0:0` | Model bug: member actions quantified a STRING parameter (`m = "Unbound"` is `"m1" = "Unbound"`, always FALSE) — the member state machine never ran and the 84-state "all green" was vacuous | Rewrote members as a function variable `mem ∈ [{a,b} -> MemberState]`; actions quantify over `Members`. Distinct states went 84 → 648/660. Also added the missing `GcLock` action (gc's try-acquire) which made the GC-held sweep branch reachable. |
| 4 | TLC crash: "Successor state is not completely specified" in `WbCAS` | `pack'` expression referenced `NoBoundPr` (a bare `mem'` read) — the successor variable was read before being defined | The other member is UNCHANGED by a one-member CAS, so the "no remaining Bound member" check is equivalent on CURRENT values. Rewrote with `\A x \in Members : (x # mm) => mem[x] # "Bound"`. |
| 5 | `BuildingConverges` (unguarded) liveness refuted; then guarded version still refuted | (a) fault-period alternation cancels every WF debt; (b) a lead-to obligation survives a crash that cancels the fairness debt | Added stability assumptions 1–3 above; saved the trace as `examples/L1-unguarded-counterexample.trace`; L1 now premises `lock # "NoLock" /\ ~failProne`. |
| 6 | wb `BuildingConverges` refuted: loop `WbPutOk → WbInlineDeleteConfirmed` starves `WbMark` forever | The inline delete is an upload-FAILURE-arm action; gating it only on `pack = Building` allowed the bogus healthy-path interleaving "upload OK, immediately delete" | Premised `WbInlineDeleteConfirmed`/`WbInlineDeleteFailed` on `failProne` (the failure arm exists only after a failed PUT). |
| 7 | wb `CommittingResolves` refuted twice: (a) `WbBind → RecoverRequeue → WbBind` alternation starves CAS; (b) stuttering with a `Bound` member | (a) a requeued task never comes back to THIS pack — Unbound made it re-bindable forever; (b) wb CAS is client-close driven and the model owes no fairness to clients | (a) `RecoverRequeue` now lands on `Dead` (terminal), like `MemberAbandon`; (b) L4's wb premise requires `AllTerminal` — an engine never finalizes a batch with a live member. Trace: `examples/L4-wb-counterexample.trace`. |

## Mapping: model actions ↔ implementation (Phase 1)

Code anchors verified at baseline `d3b7acd` (public repo).

| Model action | Rust entry | Transaction / storage boundary | Tests |
|--------------|-----------|-------------------------------|-------|
| `StrictRegister` / `WbRegister` | `MetadataStore::register_pack` (metadata.rs:188); strict `PackEngine::seal` (pack.rs:259), wb `WriteBackEngine::seal_batch` (wb.rs:1102) | one INSERT; row exists BEFORE bytes | p0a suite; `s410_*` |
| `StrictPutOk` / `StrictPutUncertain` / `WbPutOk` / `WbPutUncertain` | `objects.put_stream` in seal paths; FlakyObjects/S411Objects inject `uncertain_put` | non-transactional; result independent of remote state | `s410_uncertain_put_keeps_tracker_row_sweep_reclaims` (pack.rs:863), `s411_*` |
| `StrictMark` / `WbMark` | `mark_pack_uploaded` (metadata.rs:194) — single UPDATE; `uploaded_at NULL ⇔ building` | one UPDATE | p0a |
| `WbInlineDeleteConfirmed` + `WbDropRow` | seal_batch failure arm: `objects.delete` 2xx/404 THEN `drop_pack` (wb.rs:1102) | delete is remote; drop is one DELETE | `s411_plain_failure_confirmed_delete_drops_object_and_row` (wb.rs:4921) |
| `WbInlineDeleteFailed` | failure arm keeps the building row as durable tracker | — | `s411_uncertain_delete_fails_tracker_survives_then_gc_closes` (wb.rs:4957) |
| `WbBind` | `wb_bind_pack_keys` (metadata.rs:232) — members bind AFTER the PUT succeeds | one tx | s411 scenarios |
| `StrictCAS` / `WbCAS` | `commit_content_cas_pack` (metadata.rs:162) — node flip + `pack_ref_new_tx` (sqlite.rs:221) + `finalize_batch_tx` (sqlite.rs:255) in ONE tx (refs>0 → Active) | single SQLite tx | `s410_partial_cas_crash_finalizes_to_active` (pack.rs:963) |
| `StrictCASConflict` / `WbCASConflict` | CAS loser at zero refs → finalize_batch_tx → Unreferenced | same tx | p0a |
| `RecoverRequeue` | `wb_requeue_draining` (metadata.rs:383) — requeued tasks re-seal into a NEW pack | one UPDATE | wb recover tests |
| `SweepBuildingExists` | `gc_register_stale_pack_deletion` (metadata.rs:275): predicate re-check + building→deleting flip + intent insert | single Immediate tx | `s410_head_unavailable_keeps_row_next_pass_converges` (pack.rs:932) |
| `SweepBuildingMissing` / `SweepFinalize` | `recover_stale_packs` (core.rs:1457) HEAD three-state; committing finalize loop | HEAD then per-row tx | same + `s410_committing_zero_refs_flows_through_gc` (pack.rs:1038) |
| `GcEnrollUnreferenced` | `gc_register_pack_deletion` (metadata.rs:256): refs=0 re-check + flip + intent | single tx | p0a crash contract |
| `GcClaim` / `GcExecuteOk` / `GcExecuteFail` / `GcComplete` | `gc_claim_deletions` (metadata.rs:294) / cmd::gc executor / `gc_complete_deletion` (metadata.rs:300): intent flip + row drop in one tx | per-key txs | s431-gc-e2e.sh; p0b |
| `GcLock` / `GcUnlock` | cmd.rs:148 `gc_stale_pack_phase` — `FileLock::try_acquire` (fslock.rs:30); lock dies with process | flock per-ofd / LockFileEx | S4.3.4b engine-lock tests |
| `EngineStart` / `EngineCrash` | `build_core` recover + `hold_engine_lock` (engine.rs); crash = process death | lock released by kernel | S4.3.4b subproc SIGKILL test |
| `Protect` / `Unprotect` | backup manifest (src/backup.rs, S4.3): protected = live refs ∪ unexpired manifests; `backup prune --id` | manifest file + gc guard set | s431-gc-e2e.sh protection stages |
| `NodeUnlink` | namespace delete retiring the last reference (unlink → refcount) | one tx | p0a |

## Model → Rust test matrix (Phase 6)

| Model fact | Rust surface where it lives | Existing test evidence | Gap |
|------------|----------------------------|------------------------|-----|
| I1 refs never point at dead objects | CAS refuses non-committing/active | p0a; `s410_partial_cas_*` | none material |
| I2 Building unreferencable | `pack_ref_new_tx` accepts {committing, active} only | S4.10 structural invariant tests | none |
| I3 every live object tracked | building rows survive failure arms | `s410_uncertain_put_*`, `s411_uncertain_delete_*` | pre-S4.10 historical orphans have no repair path (fsck reports only — S4.10 §6.2 backlog) |
| I4 intent/state coupling | flip+insert in one tx | p0a crash matrix | none |
| I5 unknown ≠ missing | HEAD three-state sweep; delete-outcome mapping | `s410_head_unavailable_*`, `s411_head_unavailable_*`, S4.2.2 delete_outcome tests | MinIO NoSuchBucket vs NotFound asymmetry is covered by S4.2.2 unit tests, not the model (object-store specific) |
| I6 recovery idempotence | recover_stale_packs re-runs; GcClaim re-queues | repeated-recover scenarios in s410/s411 tests | none |
| I7 lock mutual exclusion | fslock + engine-lock + gc try-acquire | S4.3.4b three-axis tests; SIGKILL test | gc lock-failure path (Some(None) sweep skip) has no dedicated test — minor |
| I8 backup guard | executor per-pass protection re-check | s431-gc-e2e.sh protection-then-prune stages | crash between delete and complete while protected: covered by design (claimed re-check) but not by a dedicated test — minor |
| L1 guarded convergence | recover on engine start + gc stale-pack phase | s410/s411 "next pass converges" tests | none |
| L2 intent completion | gc executor retry on Failed | s431 e2e corruption-interrupt stage | none |
| L4 Mode-sensitive resolution | strict sweep finalize vs wb AllTerminal | `s410_committing_zero_refs_*`; wb bind/finalize tests | wb "live Bound member parks Committing indefinitely" is by-design and untested as such — documented, low priority |

**Uncovered properties / out of scope** (stated, not hidden): power loss;
SQLite `synchronous=FULL` durability mechanics; multi-pack interactions;
unbounded member counts (>2) and MaxRefs > 2; object-store quirks (MinIO
directory-object list anomalies — S4.6 contract suite territory); the retention
time gate; historical pre-S4.10 row-less orphans; liveness of the *client*
(close-driven CAS is deliberately unfair in the model).
