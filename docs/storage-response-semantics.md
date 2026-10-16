# Storage Response Semantics — the authoritative contract

How an object-store response becomes a domain decision and a durable state
transition. One page to answer: "why does this HTTP result delete a tracking
row, that one keep it, and that other one kill a task?"

Scope: the six audited paths — S3 `HEAD` (pack-lifecycle recovery), S3
`DELETE`, strict-pack upload failure, write-back batch upload-failure
cleanup, write-back upload-failure classification, and GC deletion-intent
execution. Everything below was traced through the current code on branch
`s31-pack-proto` (S4.15 audit); every function and test named here exists.

The goal is NOT to unify the verbs or the callers. Some asymmetries below
are load-bearing correctness, and this document says which and why.

---

## 1. Who interprets what (layer responsibilities)

```
S3 endpoint ──HTTP──▶ L0 adapter (src/object_store/s3.rs)
                        status + XML error code  ──▶  Error variant
                      ObjectStore trait (src/object_store.rs)
                          │
                          ▼
                      L1 domain callers (core.rs / pack.rs / wb.rs)
                        Error variant + operation context  ──▶  domain judgment
                        (side-effect certainty × retry class  ──▶  SQLite state
                         → disposition; see the three axes below)
```

- **L0 (the adapter)** maps ONE response to an error category. It never
  decides lifecycle, never touches SQLite, and retries only one narrow
  class (503/SlowDown, via `with_retry` — `src/object_store/s3.rs:1140`).
- **L1 (the caller)** owns every state decision. It never parses HTTP; it
  reasons only about the `Error` variants the adapter produced, plus its
  own operation context (what a PUT error means for an object that may or
  may not exist).
- **The database** (SQLite `packs`, `wb_tasks`, `gc_deletions`,
  `superseded_objects`) is the only durable memory. Every "keep tracking"
  decision below is a row that a later pass re-adjudicates.

The vocabulary used throughout this document — three ORTHOGONAL axes, never
collapsed into one:

1. **Remote side-effect certainty** (axis 1) — a domain judgment about
   THIS key, in THIS operation's context: **confirmed-present**,
   **confirmed-absent**, or **unresolved** (the remote state is
   unspecified). It is never a function of the status code alone. For
   `put`/`put_stream` the trait contract is explicit: after ANY `Err` the
   remote state is UNSPECIFIED (`object_store.rs:54-61`) — including 403
   and 5xx; a multipart upload can fail after parts were already stored.
   No error classification may be read as "the object does not exist".
2. **Retry classification** (axis 2) — `transient` / `permanent` /
   `unknown`, consumed ONLY by task dispositions (row 5). It judges
   whether RETRYING can fix the cause. It says nothing about axis 1: an
   `upload-permanent` 403 on a PUT classifies the retry decision, and the
   object's presence stays UNSPECIFIED regardless.
3. **Disposition** (axis 3) — the business decision: keep the tracking
   row, requeue, dead-letter, or complete the deletion. Derived from axes
   1 and 2 plus the operation context. The standing dispositions for
   unresolved results: an undecidable HEAD keeps the `building` packs row
   (1c); an undecidable DELETE keeps the deletion intent (6b).

The same `Error::Storage` maps to different domain results per operation:
a transport error on a pack PUT leaves presence unresolved and the row
kept (row 3); the same error on a DELETE leaves the deletion unresolved
and the intent kept (6b); on a HEAD it is "undecidable", again keep the
row (1c). One error variant, three operations, three dispositions — the
operation context, not the variant, decides.

Bridge to the shorthand used in earlier rounds and in some table rows:
**CONFIRMED** meant axis 1 decided (present or absent); **UNCERTAIN**
meant axis 1 unresolved; **FAILED** meant a definite negative about the
operation's EFFECT — never about the object's presence. The canonical
example of that last point: a 403 on a DELETE definitively did not delete
(effect negative → intent retained, 2d/6b), while a 403 on a PUT is
axis-2 `upload-permanent` AND axis-1 unresolved — the object may still
have been stored (multipart parts). Wherever those three words appear
below they mean the axis judgments above, never a raw status-code
mapping.

## 2. L0 baseline: response → `Error` (read this before the table)

| Response shape | Adapter mapping | Resulting variant | Code |
|---|---|---|---|
| any BODY-bearing verb (GET/PUT/DELETE…), a 404 status, or a body containing `NoSuchKey`/`NoSuchBucket` | `map_status` | `Error::ObjectMissing(key)` | `s3.rs:311` |
| any other non-2xx (403, 400, 5xx, …) | `map_status` | `Error::Storage("s3 {status} on {key}: …")` | `s3.rs:311` |
| transport failure (connect/reset/timeout) | `map_transport` | `Error::Storage("s3 transport: …")` | `s3.rs:1159` |
| `Storage` message containing `503`/`SlowDown` | `with_retry` | retried ×4 (100ms·2^attempt backoff), then the error surfaces | `s3.rs:1140` |

Two L0 subtleties the contract depends on:

1. **`map_status` deliberately conflates `NoSuchKey` and `NoSuchBucket`
   into `ObjectMissing`** (the trait speaks only "object missing", not
   "bucket missing"). Callers that MUST tell them apart re-check the body
   before `map_status` runs — that is exactly what `delete_outcome` does
   below. A new backend must reproduce the body inspection, not just the
   status code.
2. **`HEAD` has no response body.** Its 404 cannot distinguish a missing
   key from a missing bucket — both surface as `ObjectMissing`, decided by
   the status code alone (GET and DELETE carry an XML error-code body;
   HEAD carries nothing). This is a documented limit of the HEAD
   adjudication — gap **G1**.

### The two DELETE mappings are different on purpose

- `ObjectStore::delete` → `delete_outcome` (`s3.rs:324`):
  `NoSuchBucket` in the body → **`Error::Storage`** (a namespace failure
  must NOT be swallowed as "already gone"); 2xx **or** 404 → **Ok**
  (deletion is idempotent at the KEY level: a missing key is success);
  everything else → `map_status`.
- `head` (`s3.rs:981`): 404 → `ObjectMissing` ("absent" as a FIRST-CLASS
  answer used to adjudicate a lifecycle), 2xx → `ObjectInfo`, any other
  status → `Storage` ("undecidable").

So "404" means three different things depending on the verb: on DELETE it
is idempotent success; on HEAD it is a confirmed-absent verdict, status
code only (no body to inspect — gap **G1**); in `map_status` bodies it is
folded into `ObjectMissing` for the read path, which is also where the
missing-BUCKET case loses its identity on the read path (gap **G2** — a
trait-semantics limit, distinct from G1). The domain meaning lives at the
caller, keyed by operation — never at the status code alone.

---

## 3. The master table

| # | Operation / call path | Raw response / error class | Domain semantics | Durable state transition | Retry / termination | Implementation | Tests | Future-backend obligation |
|---|---|---|---|---|---|---|---|---|
| 1a | `recover_stale_packs` adjudicates a `building` packs row — HEAD says the object EXISTS (`Ok`) | HEAD 2xx | CONFIRMED present: an upload we thought failed (or a crash mid-seal) actually landed | one tx: `packs.state building→deleting` + `gc_deletions(kind='pack', state='pending')` (`gc_register_stale_pack_deletion`, predicate re-checked inside the tx; live node refs make it a no-op) | the intent is drained by gc's executor (row 6); the sweep itself does not delete | `core.rs:1457 recover_stale_packs` (arm at `core.rs:1481`) | `pack.rs s410_uncertain_put_keeps_tracker_row_sweep_reclaims`, `engine.rs s410_strict_engine_lock_and_startup_sweep` | HEAD must be a real metadata probe; 2xx means observable-by-GET |
| 1b | same — HEAD answers 404 | `ObjectMissing` (from the bare status code; HEAD has no body — gap G1) | confirmed-ABSENT: object never materialized (or was already removed) | `drop_pack` — the `building` row is deleted outright | terminal for the row; nothing to retry | `core.rs:1494` | `pack.rs s410_absent_object_row_dropped_without_intent` | 404 from HEAD = confirmed absent (a missing KEY and a missing BUCKET are indistinguishable here — gap G1) |
| 1c | same — HEAD errors otherwise (403, 5xx, transport, timeout) | `Error::Storage` | UNDECIDABLE — fail-closed | row KEPT as-is (`head_unavailable` counter); re-adjudicated by the next startup pass or `lightfs gc` | passive: wait for a pass where HEAD is decidable | `core.rs:1503` | `pack.rs s410_head_unavailable_keeps_row_next_pass_converges` | undecidable probes must ERROR, never synthesize "absent" |
| 2a | `ObjectStore::delete` on an existing key | 204 (or any 2xx) | confirmed deleted | (caller-side transitions: rows 6, 4b) | — | `s3.rs:1007 delete` | `contract.rs delete_and_listing` (mem + MinIO) | 2xx must mean gone-for-later-GETs |
| 2b | `delete` on a key that never existed | 404 / body `NoSuchKey` | idempotent success — a missing key IS a deleted key | same as 2a | no retry needed; a retried delete cannot resurrect anything | `delete_outcome` `s3.rs:324` | `s3.rs p0a22_delete_outcome_missing_key_is_idempotent_success` | 404-with-missing-key must be Ok, never an error |
| 2c | `delete` with the bucket missing | body `NoSuchBucket` | namespace failure — fail-closed, NOT "already gone" | caller keeps the deletion intent (row 6f) | retained and retried by the next gc pass | `delete_outcome` checks the body BEFORE `map_status` | `s3.rs p0a22_delete_outcome_bucket_permission_and_server_errors_stay_errors` | MUST re-check the XML error code; a bare 404 must NOT be treated as bucket-missing |
| 2d | `delete` — permissions / 5xx / transport | 403 etc. → `Storage` | FAILED about the EFFECT: the deletion definitively did NOT happen; the object's presence is not the question (a transport error would leave even the deletion unresolved) | intent retained | next gc pass retries; transport 503s retried by `with_retry` | `s3.rs:1007` | `contract.rs delete_and_listing` | non-absence failures must ERROR |
| 3 | Strict pack seal: PUT fails after `register_pack` (`pack.rs seal`, error arm) | any PUT error — transport, 5xx, even 403: per the trait contract the remote state is UNSPECIFIED after ANY Err (axis 1 = unresolved, regardless of axis 2 classification) | unresolved presence: the object MAY exist. The row (registered BEFORE the bytes) is the pack's durable lifecycle tracker | `packs` row KEPT in `building` (S4.10). fsck would still SEE a rowless `packs/` object (it reports "pack without row"), but a report is not an owner: the HEAD three-state sweep iterates ROWS (`all_packs()`), so without the row nothing would ever adjudicate or reclaim the object (mechanism detailed in G3) | NO inline delete; the row waits for the HEAD three-state (1a/1b/1c) at the next startup or gc pass. Members get their flush errors; per-entry CAS failures are local dead bytes and never touch the pack | `pack.rs:277-344` | `pack.rs s410_uncertain_put_keeps_tracker_row_sweep_reclaims` | `put`/`put_stream` failure semantics: after Ok, atomic overwrite + non-empty etag; after Err, remote state UNSPECIFIED (`object_store.rs:54-61`) |
| 4a | WB `seal_batch`: PUT fails, inline delete CONFIRMED (`Ok` via `delete_outcome`) | PUT error + DELETE 2xx/404 | uncertain PUT, then confirmed cleanup | `drop_pack` — the `building` row is removed ONLY now (S4.11); members requeued or dead-lettered per row 5 | members: transient → requeue (budget 3, `wb_retry_or_dead`), else dead-letter | `wb.rs:1117-1154` | `wb.rs s411_plain_failure_confirmed_delete_drops_object_and_row` | delete confirmation uses the SAME `delete_outcome` contract (2c included: NoSuchBucket keeps the row) |
| 4b | WB `seal_batch`: PUT fails AND inline delete fails | PUT error + DELETE `Storage` | uncertain PUT, uncertain cleanup — never widen uncertainty into absence | `building` row KEPT as the durable tracker; converges via the stale-pack sweep (1a/1b/1c) "potentially much later under a lasting outage — the accepted safety-first cost" | same passive sweep as row 3 | `wb.rs:1149-1153` | `wb.rs s411_uncertain_delete_fails_tracker_survives_then_gc_closes` | a failed cleanup must leave the tracker row intact |
| 4c | WB `seal_batch`: delete confirmed but `drop_pack` fails | DELETE Ok + SQLite error | object gone; row is now stale-but-harmless | row KEPT (log); next sweep HEADs it → 404 → dropped (1b) | passive convergence | `wb.rs:1143-1147` | `wb.rs s411_delete_confirmed_drop_pack_fails_row_waits_for_sweep` | local DB failure must not be reported as remote failure |
| 4d | WB `seal_batch`: HEAD unavailable later | see 1c | undecidable | row kept | `wb.rs s411_head_unavailable_keeps_tracker_until_head_recovers` | same as 1c | | |
| 5a | WB upload-failure classification — `s3 transport: …` | transport error | axis 1: presence unresolved; axis 2: transient | `wb_tasks.attempts++`, `error_class='upload-transient'` | auto-requeue: `wb_retry_or_dead(id, 3)` → `state='queued'` (50ms backoff) or `state='dead'` past budget | `wb.rs:216 classify_object_upload_failure`, `wb.rs:262 upload_failure_disposition` | `wb.rs p0d47_upload_disposition_decision_table`, `p0d_r17_upload_transient_requeues_with_class_then_recovers` | transport-shaped errors must be classifiable WITHOUT parsing provider-specific bodies |
| 5b | — HTTP 5xx / 429 / 408 | `s3 {status} …` | remote overload/retryable verdict; side effect presence UNKNOWN but class transient | same as 5a | same budget-3 auto-requeue | `wb.rs:229-231` | `p0d47_…decision_table` (statuses swept), `p0d47_batch_upload_transient_budget_exhaustion_dead_letters_with_class` | 5xx/429/408 must classify transient |
| 5c | — other HTTP verdicts (403, 400, bucket policy…) | `s3 {status} …` | PERMANENT: retrying cannot fix the cause | `error_class='upload-permanent'`, `state='dead'` | dead-letter immediately; future operator retry MAY admit the class (`retry_admissible`), auto-retry never | `wb.rs:232-234` | `p0d_r17_upload_permanent_dead_letters_immediately_with_class` | non-retryable remote verdicts must classify permanent |
| 5d | — anything unjudgeable (protocol/parse leftovers, non-`Storage` errors) | other | UNKNOWN: never masquerade as retryable | `error_class='unknown'`, `state='dead'` | dead-letter; excluded from `retry_admissible` by construction | `wb.rs:236-238` | `p0d_r17_unknown_error_never_requeues`, `p0d_r17_error_class_origin_and_admission_table` | unparseable outcomes must classify unknown, never transient |
| 5e | per-file drain (`drain_task`) upload failure — the SAME disposition, different cleanup | any upload error | classification as 5a-5d; the version key is single-file scoped | best-effort inline delete of the version key (`let _`), then requeue/dead-letter. On a dead task whose PUT had SUCCEEDED, `wb_set_uploaded_key` makes `recover()` enroll the orphan candidate | requeue budget 3, shared wording via `log_upload_dead(Task \| BatchMember)` | `wb.rs:950-969` | `p0d_r17_*` series (per-file arms) | see gap G4: an uncertain PUT whose cleanup delete ALSO fails leaves an fsck-visible, untracked orphan — accepted for single-file keys, never for packs |
| 6a | gc executor: claimed deletion intent, remote delete succeeds (2xx or 404-missing-key; `NotFound`/`ObjectMissing` accepted defensively) | `delete_outcome` = Ok | CONFIRMED gone (or already gone) | one tx (`gc_complete_deletion`): kind=`pack` → `DELETE FROM packs`; kind=`object` → `DELETE FROM superseded_objects`; intent → `done` | terminal | `core.rs:382-419`, `sqlite.rs:1245` | `core.rs p0a22_object_missing_from_delete_completes_the_task`, `p0a22_remote_deleted_before_finalize_converges_on_recover`, `p0a_deletion_failure_persists_then_retry_succeeds` | same idempotent-delete contract as 2a/2b |
| 6b | gc executor: remote delete fails (NoSuchBucket, 403, transport) | `Storage` | FAILED about the EFFECT — the deletion did not happen; the intent must never be lost (a transport error leaves even that unresolved, same disposition) | `gc_fail_deletion`: intent stays, `state='failed'`, error text persisted | next gc pass re-claims (`gc_claim_deletions` re-queues `pending` AND `failed`; a crash left `deleting` re-enters as `pending` — the remote delete is idempotent) | `core.rs:411-414`, `sqlite.rs:1211` | `p0a_deletion_failure_persists_then_retry_succeeds` | failures must persist for retry, not abort the pass |
| 6c | gc executor: key is in the protected set | (before any call) | manifest protection beats reclamation | intent left in `deleting`, `skipped_protected` counted; "a protected intent is never completed"; the next claim re-queues it | passive: protection expires via backup `prune`/expiry, then 6a proceeds | `core.rs:390-394` | `backup/tests.rs p0b_protected_object_survives_gc_until_prune`, `s431_retention_off_gc_keeps_protected_object_and_task`; end-to-end: `scripts/backup-restore-drill.sh` (asserts `skipped-protected 2` / `deleted 1`) | the protection check MUST precede the delete call |
| 6d | gc entry: protection-set derivation | backup manifests | unexpired manifest keys are protected; live refs are guarded UPSTREAM (retire's live-reference guard, `gc_register_*`'s `NOT EXISTS (nodes …)` predicates) — the executor only arbitrates intents | verification is fail-closed: one unverifiable manifest → zero deletes (`p0b_scan_fails_closed_on_any_unverifiable_backup`), missing `[backup]` root → gc refuses entirely | `cmd.rs:54 gc_run`, `backup.rs protected_keys` | `p0b_gc_fail_closed_makes_zero_delete_calls`, `p0b_gc_end_to_end_enroll_execute_and_protection_filter`, `s431_retention_off_corrupt_manifest_fails_closed_zero_deletes` | manifest integrity (sha256 anchor) must be checked before any delete |

### Strict vs write-back: the real difference (do not "unify" this)

Both engines obey the SAME invariant — *an uncertain PUT must leave a
durable tracker that a later CONFIRMED probe can adjudicate* — and the SAME
HEAD three-state sweep converges both. What differs is the failure-arm
strategy, and each side has a pinned test:

- **Strict** keeps the row and does NOT attempt an inline delete
  (`pack.rs:329`). The foreground flush fails fast; cleanup is entirely
  the background sweep's job.
- **WB** attempts a confirmed-cleanup delete first and drops the row only
  on confirmation (`wb.rs:1117`). Its failure arm can do this safely
  because members bind to the pack only AFTER a successful PUT, so a
  failed batch upload is unreferenced by construction.

The difference is behavioral, pinned by tests on both sides (row 3 vs rows
4a-4c); the shared part (classification + wording + sweep) was already
converged where it was one decision — `upload_failure_disposition` and
`log_upload_dead` exist precisely so the two arms cannot drift again (S4.7).

---

## 4. Proof boundary — what is proved where

| Layer | What it actually proves | Where |
|---|---|---|
| TLA+ model (`formal/pack-lifecycle/`) | The pack/task lifecycle state machine over an ABSTRACTION: an object oracle `obj ∈ {Present, Absent}` and a boolean `failProne`. `StrictPutUncertain`/`WbPutUncertain` let a failed PUT land in EITHER remote state, and safety (I1–I9) + liveness hold in both. `SweepPackNext` IS the HEAD three-state (decidable-present → Deleting+intent; Absent → row dropped; `failProne` → untouched); `WbInlineDeleteConfirmed` is the row-4a gate. The model does NOT talk about HTTP status codes or S3 error codes — a green model run says nothing about, e.g., 403 vs NoSuchBucket handling. That mapping is Rust-level only. In the axis vocabulary of §1: the model covers axis 1 (the `obj` oracle IS side-effect certainty, with `failProne` making HEAD undecidable) and axis 3 (the dispositions); axis 2 (retry classification) has no model counterpart at all. | `PackLifecycle.tla:96-115, 196-263` |
| Rust unit tests (pure logic, no I/O) | The response→error mapping and the pure decision functions: `delete_outcome` table (2xx/404/NoSuchBucket/403), `upload_failure_disposition` decision table (every class, auto_requeue), `with_retry` semantics, `retry_admissible` admission | `s3.rs p0a22_delete_outcome_*`, `s3.rs with_retry_retries_slowdown_then_succeeds`, `wb.rs p0d47_upload_disposition_decision_table`, `wb.rs p0d_r17_error_class_origin_and_admission_table` |
| SQLite integration tests | The durable state machine: intent persistence across a failed remote delete, deleting→pending re-queue, finalize transactions, protected-skip accounting, retention-off enrollment — over real SQLite files reopened in-process | `core.rs p0a_*` / `p0a22_*`, `backup/tests.rs p0b_*` / `s431_*`, `wb.rs s411_*` (with injectable failing stores) |
| MinIO integration tests (`LIGHTFS_TEST_MINIO=1`) | The REAL adapter against a REAL S3 server: the full `run_contract_suite` (delete/listing behavior, etag non-empty, get/range contract), commit races, point-in-time restore with manifest protection | `tests/minio_integration.rs object_store_contract_suite_minio`, `concurrent_commit_race_minio`, `point_in_time_restore_minio`; the suite also runs against the in-memory store (`contract.rs mem_object_store_passes_the_contract_suite`) so both backends answer the same assertions |
| Subprocess / crash tests | Process-death behavior: engine-lock takeover after SIGKILL, first-open races, bidirectional lock refusal, cross-process maintenance lock | `engine.rs s4141_concurrent_first_open_yields_exactly_one_lock_holder`, `s414_*` refusal tests; `backup/tests.rs p0b_lock_is_cross_process_and_released_on_exit_and_death`; `wb.rs p0c2_*` takeover |
| NOT verified anywhere | Host power loss / physical fsync durability (crash tests stop at process kill); S3-compatible implementations other than MinIO (known real differences exist — e.g. flat-namespace objects-are-directories list behavior, documented on the `ObjectStore` trait and avoided by fixtures); HTTP 408/429 classification is unit-tested as strings, not behavior-tested against a live throttling endpoint | — |

## 5. Contract for future backend implementers

Answer key, derived from the table:

1. **A request errored but the side effect may have happened — what must
   the metadata look like?**
   Exactly as it looked before the call, PLUS whatever tracker the
   operation's pre-registered row provides (a `building` packs row exists
   before the bytes by design). You must never write a state that asserts
   the remote outcome of an uncertain call. The later CONFIRMED probe
   (HEAD three-state / idempotent delete) converges it.
2. **Which results may be retried idempotently?**
   `delete` (key-level idempotent, 404 = success) and any op whose retry
   writes an IMMUTABLE key (strict/WB pack PUTs and per-file version-key
   PUTs are only ever retried onto a FRESH key, never the same one — that
   is why requeue in row 5 is safe even though the PUT was uncertain).
3. **Which failures must fail closed?**
   `NoSuchBucket`, permissions (403), 5xx-after-retries, transport errors
   on HEAD, an unverifiable backup manifest, a missing `[backup]` root.
   None of these may be read as "object absent"; none may complete a
   deletion intent.
4. **When may a durable tracking record be cleared?**
   `building` packs row: only on a CONFIRMED-absent HEAD (1b) or a
   CONFIRMED delete (4a). Deletion intent: only via `gc_complete_deletion`
   after a confirmed idempotent delete (6a) — never while protected (6c).
   Dead task: never auto-cleared; `error_class` is the operator interface.
5. **What must a new backend reuse vs add?**
   Reuse: `run_contract_suite` (same assertions on mem + your backend),
   the L1 state machines unchanged (they only consume `Error` variants),
   and `delete_outcome`'s shape (body-aware mapping). Add: the L0 mapping
   for your provider's error codes (which bodies mean missing-key vs
   missing-bucket vs permission), and provider-specific integration tests
   for anything the contract suite cannot express (throttling behavior,
   versioning interactions).

## 6. Evidence gaps (honest limits of this contract)

- **G1 — HEAD cannot distinguish NoSuchKey from NoSuchBucket** (no body).
  The building-row sweep would read a missing-BUCKET 404 as "object never
  existed" and drop rows. Accepted: if the bucket is gone, the deployment
  has lost its data plane entirely; there is no safe alternative verdict
  for HEAD today. Documented, not fixed.
- **G2 — `map_status` folds NoSuchBucket into `ObjectMissing`** for every
  verb except `delete` (which re-checks the body). Read-path callers
  cannot currently tell "key gone" from "bucket gone"; the trait has no
  dedicated variant. Changing it is a trait evolution, not a bug fix.
- **G3 — fsck SEES every pack anomaly but reclaims nothing.** Current
  fsck scans BOTH prefixes and cross-checks rows against objects
  (`fsck.rs:178-237`): a bucket `packs/` object with a live row is live
  or `unreferenced` (gc-managed); with a pending row it is reported as
  "pending row, object present — sweep adjudicates"; with NO row it is
  reported as an orphan "pack without row" (external tampering or a
  deleted row). Symmetrically, a row whose object is absent is reported
  ("registered, object absent" / "row only, object absent"). Pinned by
  `s410_fsck_reports_pack_rows_and_pending_labels`. What fsck deliberately
  does NOT do is delete: it is a read-only reporter. Reclamation requires
  the HEAD three-state adjudication, and that sweep iterates ROWS
  (`all_packs()`) — which is the real reason rows 3/4b keep the
  `building` row: the row is the durable lifecycle tracker that gives an
  uncertain-PUT object an OWNER with an adjudication path. A rowless pack
  would remain visible-but-ownerless forever: reported every fsck run,
  reclaimed by no pass, because deleting on the suspicion "no row means
  abandoned" would race external tampering diagnostics and has no
  confirmed-absent probe behind it.
- **G4 — per-file uncertain orphan (row 5e).** A `drain_task` PUT that
  uncertainly succeeded AND whose best-effort cleanup delete also failed
  leaves an untracked `objects/…` orphan (visible in fsck's
  `orphan_objects`, no auto-reclamation). Accepted for single-file keys;
  the pack path must never do this — a pack's tracker row is its only
  lifecycle owner, and dropping it without a confirmed delete would
  create exactly the visible-but-ownerless state G3 describes.
- **G5 — physical durability.** Everything above is process-crash tested
  at best. Power-loss behavior of the local SQLite file and staging
  directory is asserted by construction (fsync gates, S4.4) but never
  experimentally verified.

— S4.15, branch `s31-pack-proto`. Trace it yourself: every symbol in this
document is `grep`-able in `src/`, `tests/`, and `formal/` at the pinned
commit.
