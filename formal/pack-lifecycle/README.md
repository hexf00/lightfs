# Pack Lifecycle — TLA+ Formal Model (S4.12 / S4.12.1)

An executable TLA+ model of the LightFS pack lifecycle (branch `s31-pack-proto`,
S4.12 baseline `146dc2c`), checked with TLC. It encodes the six persistent
pack-row states, the real remote object, the durable deletion intent, two
batch members, the engine lock, backup protection, and storage-fault periods.

**S4.12.1 fidelity revision** (this version) corrected three fidelity gaps
found in the first round and re-scoped what the checks actually cover:

1. `NodeUnlink` now decrements the reference AND applies the pack transition
   in ONE action, exactly as `pack_ref_gone_tx` does inside the caller's
   transaction (active + refs→0 ⇒ unreferenced; committing re-runs the
   idempotent finalize decision; other states untouched). Before the
   revision the "last reference gone → reclaimable → GC-enrolled" chain was
   NOT reachable — active packs at zero refs were stuck.
2. The engine has a process phase (`Down/Recovering/Serving`): the
   stale-pack sweep runs ONLY in the one-shot startup-recovery pass or in
   gc's stale-pack phase — never spontaneously while serving. A live
   engine that keeps running does NOT rediscover stale rows.
3. The gc process has its own phase (`Idle/Sweeping/Draining`). Losing the
   engine-lock race (`GcStartBusy`) skips the stale-pack sweep but STILL
   drains deletion intents, matching `cmd::gc`.

**The model found real gaps during its own construction** — see
[Counter-examples](#counter-examples). None required a production-code
change; the S4.12.1 revision changed only the model.

## Tool versions

| Tool    | Version |
|---------|---------|
| TLC     | tla2tools 2.0, build 2026.10.15.145915 (rev 932d35e), from the `tlaplus/tlaplus` GitHub releases (`tla2tools.jar`; not bundled in this repo — place it next to the model or adjust `-cp`) |
| Java    | Eclipse Adoptium JRE 21.0.12.1 (aarch64) |
| Checker | breadth-first search, MSBDiskFPSet, 1 worker, -Xmx4g |

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
# Action-coverage gate: every in-mode action must show total > 0
java -Xmx4g -cp tla2tools.jar tlc2.TLC -coverage 1 -cleanup -config PackLifecycle.cfg PackLifecycle.tla
```

Syntax-only check: `java -cp tla2tools.jar tla2sany.SANY PackLifecycle.tla`.

The model is pure TLA+; it does not compile or load any Rust.

## Scope and declared abstractions

- **One pack at a time.**
- **SQLite is atomic at transaction granularity — an ASSUMPTION, not a
  result.** Every single-transaction Rust step is ONE TLA+ action; TLC
  explores the reachable states AFTER each multi-row transaction. This does
  NOT verify that real SQLite transactions are atomic (I9 below); it
  verifies the state machine GIVEN that atomicity. `synchronous=FULL`
  durability and host power loss are OUT OF SCOPE.
- **ONE recover/gc-sweep pass is atomic.** The real `recover_stale_packs`
  iterates row by row and a crash can interrupt a pass; the pass is
  idempotent and the NEXT pass redoes the same adjudication, so the atomic
  pass preserves the convergence claims. Declared, not hidden.
- **Retention time is abstracted**: `Unreferenced` is directly enrollable.
- **Object-store results are independent of the real remote state**: failed
  PUT/DELETE may leave the object in EITHER state; only a confirmed delete
  (2xx or idempotent 404) forces `Absent`; HEAD is decidable only when
  storage is healthy.
- **Strict and Write-Back are never merged** (`CONSTANT Mode`). Strict does
  not touch the member variable `mem` at all, and after S4.12.1 the wb-only
  environment action `MemberAbandon` is premised on `Mode = "wb"` — in the
  strict instance the member machine does not exist (coverage confirms:
  `mem` takes 1 value under strict, 12 under wb).
- **Model-scoped**: I3/I5 cover *this* pack instance; historical row-less
  orphans from before S4.10/S4.11 are out of scope.
- **Engine lock = one enum variable.** Holder-level mutual exclusion is
  structural; that the kernel lock actually excludes a second process and
  is released when the holder dies is delegated to the `fslock.rs` tests —
  TLC does not check it.
- **Not a proof of the Rust implementation**: a finite-state check of the
  design within these bounds. Unit tests are not state-space checks.

## Results (S4.12.2, all runs completed with no error found — AND non-vacuous, see below)

| Run | Mode | Kind | States generated | Distinct | Result |
|-----|------|------|------------------|----------|--------|
| 1 | strict | safety | 1 895 | 350 | pass |
| 2 | wb | safety | 22 603 | 3 682 | pass |
| 3 | strict | liveness | 1 895 | 350 | pass |
| 4 | wb | liveness | 22 603 | 3 682 | pass |

Action-coverage gate (S4.12.1): every in-mode action has `total > 0` in
both modes; the only `0:0` entries are the other mode's actions and
`RecoverRequeue` under strict (premised on `lock = "Wb"`). The wb chain
"upload-failure arm", the new `NodeUnlink` transitions and the
"GcEnrollUnreferenced after last-unlink" chain are all exercised.

Model parameters: `MaxRefs = 2`, members = {a, b}, one pack instance,
`Mode ∈ {"strict", "wb"}` (two separate runs).

### What TLC actually checks vs structural constraints

This distinction is the point of the S4.12.1 re-scoping. Only the first
group is exhaustively checked by TLC.

**Group A — state predicates exhaustively checked (INVARIANT, both modes):**

| # | Invariant | Statement | Result |
|---|-----------|-----------|--------|
| I1 | RefsSafe | `refs > 0 => obj = Present /\ pack ∈ {Committing, Active}` | pass |
| I2 | DeletionAdmission | `pack = Deleting => refs = 0`, `pack = Building => refs = 0` | pass |
| I3 | ObjectTracked | `obj = Present => pack # Absent \/ intent # None` (model-scoped) | pass |
| I4 | IntentConsistent | non-terminal intent ⇔ Deleting; `Done => pack = Absent` | pass |
| I5 | UnknownNotMissing | `(pack = Absent /\ intent = None) => obj = Absent` | pass |
| I8 | BackupGuard | `prot = Protected => intent # Done` | pass |
| I7a | ProcessLockInv | `(phase # Down) <=> lock = EngineLock`; `lock = GC <=> gcp = Sweeping`; `gcp = Draining => lock # GC` | pass |

**Group B — structural constraints / assumptions, NOT independently checked
by TLC (each names its real evidence):**

| # | Status | What it means | Where the real evidence lives |
|---|--------|---------------|-------------------------------|
| I6 recovery idempotence | model CONSTRUCTION constraint (sweeps are re-executable; `GcClaim` accepts Pending/Failed/Claimed) — not a separate checkable property, and no hollow invariant was invented for it | repeated passes converge instead of corrupting | Rust: repeated-recover scenarios in `s410_*` / `s411_*` tests; `s431-gc-e2e.sh` re-run stages |
| I7b kernel-lock semantics | ASSUMPTION at model level (single `lock` enum) | the OS lock excludes a second process and dies with the holder | Rust: `src/fslock.rs` S4.3.4b tests (three-axis rejection, real child SIGKILL), 4b.2 alias tests |
| I9 SQLite atomicity | ASSUMPTION at model level; becomes an IMPLEMENTATION OBLIGATION: every action marked "one tx" in the mapping table must really be a single SQLite transaction | multi-row couplings (CAS+ref+finalize; flip+intent; complete=row+done) do not interleave | Rust: single `Connection` + `Transaction` blocks per mapping row; S4.2.2 p0a22 crash-window tests |

### Liveness: consistency, non-vacuity, and what each property needs

**S4.12.2 audit finding (the reason this section exists):** the S4.12.1
FairSpec was **unsatisfiable**. `SweepOpportunitiesInfinitelyOften`
(`[]<>(phase="Recovering" \/ gcp="Sweeping")` — sweep phases occur infinitely
often) directly contradicted the old
`NoStuckRecovery` (`<>[]((phase # "Recovering") /\ (gcp # "Sweeping"))` —
eventually sweep phases never occur again). Under a contradiction every
property is vacuously true, and TLC indeed printed "no error" for everything —
including a deliberately-false probe. **None of the S4.12.1 liveness results
were evidence.** The probe that exposed this is archived:
`examples/` (the fixed-spec probes below flip to VIOLATED, which is the
non-vacuity proof).

The fixed liveness spec:

- **Environment/operations assumptions (the implementation does NOT provide
  these by itself):**
  1. `FaultsEventuallyEnd == <>[]~failProne` — storage faults are finite.
  2. `ProtectionEventuallyEnds == <>[](prot = "Unprotected")` — backup
     retention ends for good.
  3. `SweepOpportunitiesInfinitelyOften == []<>(phase = "Recovering" \/ gcp = "Sweeping")`
     — restarts and gc stale-pack phases keep happening. A live engine NEVER
     sweeps (by design, still modeled), so stale-row convergence rides on
     there always being a NEXT pass.
  4. `GcRunsInfinitelyOften == []<>(gcp = "Draining")` — `lightfs gc` keeps
     being invoked. Explicit CLI path; NO daemon; NO automatic retry between
     invocations.
- **Pass-finish properties (the S4.12.2 task's candidate form, kept
  explicitly; each is implied by the SF clause below):**
  5. `RecoveryPassesFinish == [](phase = "Recovering" => <>(phase # "Recovering"))`
  6. `GcSweepPassesFinish == [](gcp = "Sweeping" => <>(gcp # "Sweeping"))`
- **Strong fairness where weak fairness is provably insufficient:** a
  start-crash loop (or gc always dying mid-run) interrupts every ENABLED
  period, cancelling WF debts forever; only SF ("enabled infinitely often ⇒
  happens infinitely often") encodes the operations assumption that repeated
  restarts / invocations eventually succeed:
  `SF_vars(RecoverSweep)`, `SF_vars(GcSweepDone)`, `SF_vars(GcClaim)`,
  `SF_vars(GcExecuteOk)`, `SF_vars(GcExecuteFail)`, `SF_vars(GcComplete)`.
  CAS stays UNFAIR on purpose (client-close driven).
- **`GcEnd` premise fix:** `gc_run` is a synchronous pipeline, so a run
  cannot exit with a Pending/Claimed intent (`GcEnd` now requires
  `intent ∈ {None, Done, Failed}`); a crash mid-run is `GcCrash`, unchanged.
  Without this, a legal "always call gc, always exit before executing"
  behavior starves the drain fairness forever — something the real
  implementation never does.

**Non-vacuity evidence (S4.12.2 §3 method: deliberately-expected-failure
probes, isolated, never acceptance properties):**

| Probe | PROPERTY | Expected | Result |
|-------|----------|----------|--------|
| `VacuityProbe` under the OLD (contradictory) FairSpec | `~<>(intent = "Done")` | "No error" ⇒ empty behavior graph | **No error — vacuity confirmed** |
| `~BuildingConverges` under the fixed spec (strict) | negation of an accepted property | VIOLATED ⇒ a legal infinite behavior exists | **violated**; lasso `examples/nonvacuity-probe-strict.trace`: `GcStart → GcSweepDone → GcEnd` loop |
| full-pipeline probe (strict) | `~(<>(phase="Serving") /\ <>(intent="Done"))` | VIOLATED | **violated**; lasso `examples/nonvacuity-probe-full-pipeline.trace`: `Init → EngineStart → RecoverSweep → StrictRegister → StrictPutOk → EngineCrash → GcStart → GcSweepDone → GcClaim → GcExecuteOk → GcComplete → GcEnd → (GcStart → GcSweepDone → GcEnd)*` |
| full-pipeline probe (wb) | same | VIOLATED | **violated**; `examples/nonvacuity-probe-full-pipeline-wb.trace` |

Each lasso satisfies `Init`, `Next`, all environment assumptions, the SF/WF
clauses and the Finish properties — TLC guarantees this for any
counter-example; the loop segments above were re-checked by hand against each
clause.

**The three properties, per mode, with exactly what each depends on:**

| Property (both modes pass) | Implementation guarantees by itself | Depends on faults ending | Depends on protection ending | Depends on next sweep pass existing | Depends on gc being re-invoked | Depends on fairness |
|---|---|---|---|---|---|---|
| `BuildingConverges` strict | the next startup/gc pass adjudicates any decidable stale row | yes (undecidable HEAD keeps the row) | no | **yes — load-bearing** | no | yes (SF on passes) |
| `BuildingConverges` wb | same + failure-arm rows keep the tracker | yes | no | **yes — load-bearing** | no | yes |
| `IntentCompletes` strict | a failed DELETE leaves a retryable durable intent; a single run drains synchronously | yes | yes | no | **yes — load-bearing** | yes (SF on the drain chain) |
| `IntentCompletes` wb | same | yes | yes | no | **yes — load-bearing** | yes |
| `CommittingResolves` strict | the sweep finalizes ANY committing pack (no HEAD needed) | no | no | **yes — load-bearing** | no | yes |
| `CommittingResolves` wb | an engine never finalizes a batch with a live member; once all members are terminal the next pass resolves it | no | no | **yes — load-bearing** | no | yes (premise `AllTerminal` is a client-side condition the model does not force) |

None of the "depends on" cells is provided automatically by the production
implementation; each is an environment/operations premise stated in FairSpec.
A permanently-running server whose lock gc never wins, and an operator who
never re-runs gc, are legal worlds with NO convergence — the properties do
not apply there, by construction of the assumptions.

### Counter-examples

S4.12.2's finding is not a TLC counter-example but an audit result: the
S4.12.1 FairSpec was self-contradictory (sweep phases infinitely often vs
eventually never again), making every liveness check vacuous. The fix
(replace the <>[]-shaped assumption with pass-finish properties + SF where
WF is provably insufficient; align GcEnd with the synchronous gc_run
pipeline) was verified non-vacuous by expected-failure probes before any
property was allowed to count as passing.

S4.12.1 produced **no new TLC counter-example**: after the fidelity fixes
all five runs passed. The adjudication records below are from S4.12 and
remain the audit trail (all were model bugs or over-strong properties;
production code was never modified).

| # | What TLC refuted | Root cause | Adjudication / fix |
|---|------------------|-----------|--------------------|
| 1 | `IntentConsistent`: `pack = Building /\ intent = Done` | single-instance variables reused across pack keys | fresh key = fresh lifecycle; Register resets intent/members (matches code: new key gets new rows) |
| 2 | `BackupGuard`: `prot = Protected /\ intent = Done` | `GcComplete` lacked a protection premise | executor re-checks protection every pass; premise added to GcComplete/GcExecute/Enroll |
| 3 | coverage gate: member actions all `0:0` | string parameter compared against a state ("m1" = "Unbound" always false); missing `GcLock` | members function-valued; states 84 → 648/660 (S4.12 numbers) |
| 4 | "Successor not completely specified" in `WbCAS` | `pack'` read the bare primed variable `mem'` | one-member CAS leaves the other UNCHANGED → check current values |
| 5 | guarded L1 still refuted | fault alternation cancels WF debts; lead-to obligations survive crashes | stability assumptions 1–3 (S4.12 set); trace `examples/L1-unguarded-counterexample.trace` |
| 6 | wb L1: `WbPutOk → WbInlineDeleteConfirmed` loop starves `WbMark` | inline delete is an upload-FAILURE-arm action; bogus healthy-path interleaving | premised the delete actions on `failProne`; trace `examples/L1-wb-counterexample.trace` |
| 7 | wb L4 twice | (a) Bind/Requeue alternation starves CAS; (b) stuttering with a Bound member | (a) requeue lands on Dead (terminal); (b) wb L4 premise = `AllTerminal`; traces `examples/L4-wb-*.trace` |

## Mapping: model actions ↔ implementation (anchors at S4.12.1 baseline `146dc2c`)

| Model action | Rust entry | Transaction boundary | Tests |
|--------------|-----------|---------------------|-------|
| `EngineStart`/`RecoverSweep`/`EngineCrash` | engine.rs `build_core`: acquire lock → `recover_stale_packs()` once → serve; hold via `hold_engine_lock` | startup pass = several txs, modeled atomic (declared) | S4.3.4b lock tests; `s410_*` |
| `GcStart`/`GcStartBusy`/`GcSweepDone`/`GcEnd`/`GcCrash` | cmd.rs `gc`: `gc_stale_pack_phase` (try-acquire; None ⇒ skip sweep, drain anyway) then `gc_run` | lock held only for the sweep phase | `s431-gc-e2e.sh` |
| `SweepPackNext`/`SweepIntentNext` (shared decision) | `recover_stale_packs` (core.rs) — HEAD three-state | per-row tx | `s410_head_unavailable_keeps_row_next_pass_converges` |
| `StrictRegister`/`WbRegister` | `register_pack` (metadata.rs:188); `PackEngine::seal` (pack.rs:259); `seal_batch` (wb.rs:1102) | one INSERT; row before bytes | p0a; `s410_*` |
| strict/wb PUT actions | `objects.put_stream` in seal paths | non-transactional | `s410_uncertain_put_*`, `s411_*` |
| `StrictMark`/`WbMark` | `mark_pack_uploaded` (metadata.rs:194) | one UPDATE | p0a |
| `WbInlineDeleteConfirmed`+`WbDropRow` / `WbInlineDeleteFailed` | seal_batch failure arm: delete 2xx/404 THEN `drop_pack`; failure keeps the row | remote delete + one DELETE | `s411_plain_failure_*` (wb.rs:4921), `s411_uncertain_delete_*` (wb.rs:4957) |
| `WbBind` | `wb_bind_pack_keys` (metadata.rs:232) | one tx | s411 scenarios |
| `StrictCAS`/`WbCAS`/conflicts | `commit_content_cas_pack` (metadata.rs:162) = node flip + `pack_ref_new_tx` (sqlite.rs:221) + `finalize_batch_tx` (sqlite.rs:255), ONE tx | single tx | `s410_partial_cas_crash_finalizes_to_active` (pack.rs:963) |
| `RecoverRequeue` | `wb_requeue_draining` (metadata.rs:383), called from `WriteBackEngine::recover` (wb.rs:437) — startup phase | one UPDATE | wb recover tests |
| `MemberAbandon` | environment: requeued task lands elsewhere / dead-letters | — | wb dead-letter tests |
| `NodeUnlink` | `pack_ref_gone_tx` (sqlite.rs:185) inside the caller's tx: active+refs0→unreferenced; committing→`finalize_batch_tx` re-run (non-terminal members keep it committing; else refs decide); other states untouched | ONE tx with the ref delete | p0a retire tests |
| `GcEnrollUnreferenced` | `gc_register_pack_deletion` via `gc_enroll_candidates` (core.rs:341, inside gc_run) | single tx | p0a crash contract |
| `GcClaim`/`GcExecuteOk`/`GcExecuteFail`/`GcComplete` | `gc_claim_deletions` (metadata.rs:294, SELECT pending/failed + re-queue deleting), executor, `gc_complete_deletion` (metadata.rs:300) | per-key txs | s431-gc-e2e.sh; p0b |
| `Protect`/`Unprotect` | backup manifest (src/backup.rs, S4.3) | manifest + gc guard set | s431-gc-e2e.sh |

## Model → Rust test matrix

Group A rows are checked by TLC on the model; the "Rust evidence" column
says what additionally backs the same claim in the code. Group B rows
appear in the structural-constraints table above and are NOT TLC results.

| Model fact | Rust surface | Existing test evidence | Gap |
|------------|--------------|------------------------|-----|
| I1 refs never point at dead objects | CAS refuses non-committing/active | p0a; `s410_partial_cas_*` | none material |
| I2 Building unreferencable | `pack_ref_new_tx` accepts {committing, active} only | S4.10 structural tests | none |
| I3 every live object tracked | building rows survive failure arms | `s410_uncertain_put_*`, `s411_uncertain_delete_*` | pre-S4.10 historical orphans: report-only (S4.10 §6.2 backlog) |
| I4 intent/state coupling | flip+insert in one tx | p0a crash matrix | none |
| I5 unknown ≠ missing | HEAD three-state sweep; delete-outcome mapping | `s410/s411_head_unavailable_*`; S4.2.2 delete_outcome tests | MinIO NoSuchBucket asymmetry is unit-tested, not modeled |
| I6 repeated passes converge | recover_stale_packs re-runs; claim re-queues | repeated-recover scenarios; s431 e2e | not a TLC property — construction constraint (see Group B) |
| I7 phase/lock bookkeeping | ProcessLockInv (checked) + kernel-lock semantics (fslock tests) | S4.3.4b/4b.2 | kernel-level mutual exclusion is NOT a TLC result |
| I8 backup guard | executor per-pass re-check | s431 protection-then-prune | crash between delete and complete while protected: by design, no dedicated test — minor |
| L1 next-pass convergence | startup recover + gc stale-pack phase | "next pass converges" tests | live-engine non-healing is by design and now modeled as such |
| L2 next-invocation retry | gc executor leaves failed intents | s431 corruption-interrupt stage | no daemon retry: by design, documented |
| L4 Mode-sensitive resolution | strict sweep finalize vs wb AllTerminal | `s410_committing_zero_refs_*`; wb bind/finalize tests | wb "live Bound member parks Committing" is by-design, untested as such — documented |

### S4.13 audit: the three candidate gaps, and the layered evidence matrix

Candidate gaps from the S4.13 task, audited against the code:

- **A. GC losing the engine-lock race — REAL GAP, closed.** The old test
  (`s410_gc_stale_phase_respects_engine_lock`) only asserted the
  `Some/None` return of `gc_stale_pack_phase`. New test
  `s413_gc_lock_skip_keeps_stale_row_and_still_drains_intents` (src/engine.rs)
  pins the full behavior with row/object/intent assertions, and
  distinguishes the TWO sources of drain work (S4.13.1 revision): an intent
  durably registered BEFORE `gc_run` is invoked (the unreferenced pack is
  flipped to `deleting` with its pending intent in one transaction before
  the test takes the lock) AND a candidate the run enrolls itself
  (`enrolled.0 = 1, done = 2`). Under a held engine lock the sweep is
  skipped AND the stale building row (and its object) survive untouched
  while BOTH intents execute; after the lock is released the next sweep
  adjudicates the stale row (building → deleting + pending intent) and the
  following gc run converges it completely. Corresponds to model actions
  `GcStartBusy` (skip) + `Draining` work + `GcStart`/`GcSweepDone` (later
  pass).
- **B. Backup protection vs deletion lifecycle — NO gap.**
  `p0b_protected_object_survives_gc_until_prune`,
  `p0b_gc_end_to_end_enroll_execute_and_protection_filter` and
  `p0b_expired_backup_releases_protection_but_still_fails_closed_when_corrupt`
  already assert row/object state: a protected key is never enrolled nor
  executed, and after prune/expiry the durable state converges via gc.
  Reality check on the model: the protection set is a SNAPSHOT taken once
  per `gc_run` under the backup `MaintenanceLock`, which also serializes
  backup publish/prune — so "protection changes between delete and
  complete" is NOT reachable in the real system. The model's per-action
  `prot = "Unprotected"` premises therefore OVER-approximate the possible
  interleavings (conservative direction: the model checks a larger world
  than reality and the invariants still hold). The model is kept as-is;
  this paragraph is the declared mapping note.
- **C. WB Bound member waits for client close — NO gap.**
  `p0a_committing_zero_refs_survives_until_batch_members_terminal`
  (sqlite.rs) already pins: with a member still queued (non-terminal), a
  committing pack stays committing even at zero references and across an
  unlink; once the member reaches a terminal state the batch resolves
  (unreferenced, retention starts). Path C/D rename-overwrite and unlink
  tests cover the reference-event side (`pack_ref_gone_tx`), and the S4.7
  three-outcome tests cover member success/conflict/terminal-failure.

Layered evidence matrix (a pass at one layer NEVER claims another layer):

| Property | TLC exhaustive (model) | Rust deterministic unit | SQLite file/integration | Real MinIO | Real child-process crash | Power loss / physical durability |
|---|---|---|---|---|---|---|
| I1 refs safe | ✔ (350/3682 states) | p0a path C/D; `s410_partial_cas_*` | — | `concurrent_commit_race_minio` | — | not verified |
| I2 building unreferencable | ✔ | `pack_ref_new_tx` refusal tests | schema contract tests | — | — | not verified |
| I3 object tracked | ✔ | `s410_uncertain_put_*`, `s411_uncertain_delete_*` | s431-gc-e2e.sh stages | symlinks+fsck+reopen | — | not verified |
| I4 intent coupling | ✔ | p0a crash matrix (p0a22) | — | — | — | not verified |
| I5 unknown ≠ missing | ✔ | `s410/s411_head_unavailable_*`, delete_outcome | — | contract suite (etag/404 semantics) | — | not verified |
| I8 backup guard | ✔ (over-approximated interleavings, see B) | p0b protection tests | s431-gc-e2e.sh protection stages | — | — | not verified |
| ProcessLockInv (model part) | ✔ | — | — | — | — | — |
| I7b kernel-lock semantics | model assumption | fslock unit tests | 4b.2 alias tests | — | S4.3.4b child SIGKILL test | not verified |
| I9 SQLite tx atomicity | model assumption + obligation | single-transaction code paths | REQUIRED_TABLE_COLUMNS contract; reopen tests | minio_integration reopen | — | `synchronous=FULL` configured (S4.4); physical durability NOT verified |
| I6 recovery idempotence | construction constraint | repeated-recover s410/s411 | s431 e2e re-run stages | minio reopen | — | not verified |
| L1 next-pass convergence | ✔ under FairSpec | `s413_gc_lock_skip_*` (new), "next pass converges" tests | s431 e2e | — | — | not verified |
| L2 next-invocation retry | ✔ under FairSpec | gc retry/failed-keep tests | s431 corruption-interrupt stage | — | — | not verified |
| L4 Mode-sensitive resolve | ✔ under FairSpec | p0a committing-survival; `s410_committing_zero_refs_*` | — | — | — | not verified |

**Out of scope / uncovered** (stated, not hidden): power loss; SQLite
`synchronous=FULL` mechanics; real kernel-lock semantics (fslock-test
territory); multi-pack interactions; members/refs > 2; MinIO object-store
quirks; the retention time gate; historical pre-S4.10 orphans; client
CAS liveness.
