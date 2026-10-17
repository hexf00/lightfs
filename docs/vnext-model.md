# The vnext model — SQLite authority, payload-only object store

Status: S4.22 (branch `vnext`; S4.20 removed per-commit meta/ publication,
S4.21 merged the new-file write into one transaction, split the three
object identities and made the namespace claim atomic; S4.22 moved
object registration INTO that transaction and backend-qualified the
ledger end to end).
This document is the authoritative description: what is authoritative,
what the object store holds, how crashes decompose, what disaster
recovery guarantees (and explicitly does NOT guarantee), and where the
backend-neutrality boundaries sit.

## 1. Authority and storage boundary

- **SQLite is the ONLY logical metadata and version-state authority.**
  refs, commits, COW trees, file_versions and the object ledger update
  atomically inside one `BEGIN IMMEDIATE` transaction per commit.
  Optimistic per-file CAS (`(file_id, version)` unique) makes writer
  conflicts explicit; ref heads advance by CAS.
- **The object store is PAYLOAD-ONLY**: immutable data objects
  (`objects/{object-id}.data`, envelope v1: magic, format version,
  kind, encoder, cipher, logical length, sha256) plus backend-internal
  physical structures. Normal commits write NOTHING else — no
  `meta/refs`, no `meta/commits`, no `meta/trees` (the S4.19
  per-commit recovery copies are gone, not deferred or batched).
- **Three identities, never conflated (S4.21)**:
  - **File ID** — the logical file's stable identity (SQLite `files`
    AUTOINCREMENT, carried in tree entries as `fid`). Allocated inside
    the commit transaction.
  - **Object ID** — the immutable data object's own identity: a fresh
    random token per write, embedded in the locator `objects/{oid}.data`
    and integrity-checked via the envelope. Independent of the File ID:
    one file has many objects over its history; an object outlives path
    and identity changes; the PUT happens BEFORE the File ID exists on
    the merged create path.
  - **Storage Locator** — the backend's physical reference for the
    object (for S3, the key), persisted inside a `StorageRef
    {backend, locator}` (tree entries) and `file_versions
    (object_backend, object_key)`. The persisted reference NAMES the
    backend instance it was written through (`ObjectStore::backend_id`,
    stable per configuration).
  - The core NEVER parses a locator to recover logical identity — not
    in the read path, not in gc (reachability keys on locators), and
    not in salvage (the locator is echoed as diagnostics only; a key
    that once embedded a file id conferred no authority even then).
    Every read checks the reference's backend against the bound store
    and refuses fail-closed on mismatch (routing changed without
    migration → loud error, never a silent cross-backend read).
- The ONLY non-payload object is `namespace/marker` (envelope kind
  `Namespace`): written ONCE at initialization, re-verified on every
  open. It carries a random namespace id that SQLite holds in its
  `model` table. Rules (S4.21: the claim is ATOMIC):
  - fresh database + no marker → write SQLite id, then ATOMICALLY
    claim the marker via `ObjectStore::put_if_absent` (S3: conditional
    PUT `If-None-Match: *`). Two databases racing on one unclaimed
    bucket yield exactly one winner; the loser reads the winner's
    marker and refuses. A failure between the local id and the claim
    fails the NEXT open closed (marker missing, database owns an id).
  - a backend WITHOUT the atomic-claim capability (`supports_atomic_
    claim() == false`) refuses namespace initialization outright — the
    core never degrades to check-then-put, whose double-claim race is
    exactly what this closes.
  - fresh database + existing marker → REFUSED (a different namespace
    owns this bucket; never co-rooted).
  - known database + matching marker → normal open.
  - known database + missing marker → REFUSED fail-closed (bucket may
    have been cleared; restoring or re-rooting is an explicit operator
    decision).
  - id mismatch → REFUSED.
  Scope: this is a per-bucket ownership declaration checked at open —
  NOT multi-server coordination (there is none, by design; a second
  live server on the same database is impossible — kernel-held engine
  lock — and a second DATABASE on the same bucket is refused at ITS
  open, not policed on the wire).
- Object-store access goes through the `ObjectStore` trait
  (put/put_if_absent/get/get_stream/head/list/delete/presign +
  backend_id + supports_atomic_claim). The core holds logical
  references (`StorageRef` + etag), never S3 semantics: no LIST
  consistency, no multipart state, no bucket addressing inside the
  commit protocol. S4.21 runs ONE configured backend; nothing here
  precludes per-class backend routing later — the reference already
  names its backend, so routing would be a write-time decision
  recorded in the reference and a read-time backend dispatch on the
  named name (refusing when absent), never a recomputation.
  Capability boundaries are explicit: enumeration is a salvage-path
  capability, presign is optional, atomic claim is REQUIRED for
  namespace initialization and absent-capability backends refuse that
  path instead of degrading.

## 2. Commit protocol

```
1. PUT data object   (envelope v1, immutable Object-ID key) — NO SQLite
                     write lock is held across the upload
2. BEGIN IMMEDIATE   walk base trees INSIDE the tx (no stale-snapshot
                     lost updates); REGISTER the uploaded object
                     (ledger row, S4.22: inside the tx, not a separate
                     autocommit); for a NEW file, allocate the File ID
                     and create the directory entry HERE (S4.21: no
                     separate empty-file create commit); for an
                     existing file, judge the caller's CAS against the
                     in-transaction entry; insert file_version, bind
                     the StorageRef, COW the touched chain, append
                     commit, advance ref — one transaction
3. COMMIT            ← the commit is durable HERE; registration and
                        file reference took effect atomically
```

ONE SQLite write transaction per successful content commit (S4.22:
the pre-transaction ledger autocommit is gone).

**Failure contract (S4.22).** A PUT that succeeded but whose logical
commit failed — confirmed Conflict/kind/path failure, uncertain commit,
any error — is re-registered in an INDEPENDENT transaction so reachability
gc can prove-and-reclaim it. The original error is ALWAYS returned
unchanged; cleanup never masks the caller's truth. A failed or
interrupted re-registration leaves the object UNREGISTERED, and gc
provably never deletes unregistered objects: the residue is safe by
construction. SQLite commit-error semantics are classified, not assumed:
a failed COMMIT is probed with ROLLBACK — a succeeding ROLLBACK proves
certain rollback (transaction still open, e.g. BUSY); a ROLLBACK refused
because no transaction is active proves the commit DID apply. The two
cases surface as distinct errors; re-registration is safe in both worlds
because the ledger alone never makes an object a deletion candidate —
reachability does.

Every decision (path conflict, CAS, kind) is judged INSIDE the
transaction; the pre-transaction lookup is advisory only (it saves
wasted uploads). The independent POSIX `create` keeps its own
semantics: it still creates an empty file in its own commit, and
`create=true` meeting an existing file still means overwrite (last
committed write wins, versions advance, no metadata lost).

Crash decomposition:

| crash window | state after restart |
|---|---|
| during 1 (the PUT) | no confirmed upload, or a raw object with no ledger row: gc can NOT prove it dead → kept (fail-safe; salvage can read it) |
| killed with the tx OPEN | SQLite rolls back on restart: neither the file version NOR the in-tx registration exists (one atomic unit) — cross-process kill-window tested |
| confirmed logical failure (conflict/kind/path) | main tx rolled back; the upload is re-registered in an independent tx → ledgered, unreferenced → gc reclaims; re-registration failure/crash leaves it unregistered → kept (fail-safe) |
| uncertain COMMIT error | classified by ROLLBACK probe (certain rollback vs applied); object re-registered either way — safe in both worlds |
| after COMMIT | done — registration, reference and commit took effect together; there is no post-commit step to lose |

## 3. Recovery

**A. Normal transaction recovery (SQLite intact or recoverable).**
WAL + synchronous=FULL; a killed or crashed process recovers the full,
consistent namespace from SQLite alone (covered by graceful-restart and
SIGKILL integration tests). GC keeps its fail-closed gate: the model
marker row AND at least one ref row must be readable IN SQLITE, or gc
refuses; unreachable-provable objects only, judged on full (backend,
locator) pairs; objects without a ledger row are NEVER deleted (they
cannot be proven dead); rows bound to a backend this engine cannot see
are never deleted and never reinterpreted (S4.22). Corrupted or
missing SQLite fails the store open, so gc has no entry point at all.

**B. Database lost: content salvage, READ-ONLY.**
`vnext::recover::salvage_all_data` enumerates the data prefix (a
DECLARED capability of this salvage path — a backend without
enumeration cannot offer it, explicitly), verifies each object's
envelope (magic, format version, kind, encoder/cipher, logical length,
sha256) and exports byte-exact payloads with their original physical
LOCATORS — diagnostic information only, never parsed into a logical
identity (S4.21 rule: no File ID is claimed from key shape) — and
per-object status:
corrupt, unknown-format, unknown-type and unreadable objects are
REPORTED individually, never served, never fatal to the pass. The pass
writes/deletes NOTHING. It does NOT recover paths, directory
structure, deletion state or latest-version identity, and does NOT
pretend to distinguish committed from orphan objects — everything
verified is a restoration CANDIDATE. Salvage and GC are separate,
non-composable operations. Historical S4.19 `meta/` objects, where
they exist, are legacy: never scanned for deletion by this task;
removal belongs to a future, separately-owned cleanup flow that must
first prove ownership.

What vnext does NOT have: a tool that adopts salvaged content as a new
writable database (`adopt` is future work); namespace reconstruction
is explicitly out of the guarantee.

## 4. Known limitations (deliberate, this slice)

- Write-back (commit.mode = "writeback") is refused at config time.
- No FUSE/WinFsp kernel-level mount test in this slice's suites
  (adapters compile for both; the restart check exercises the server).
- Per-commit metadata cost is SQLite-only now; the COW chain still
  serializes every touched directory (measured separately in the S4.20
  performance report — the removal of per-commit meta/ PUTs is
  reported separately from the remaining COW cost).
- One-database-per-bucket is enforced by the ATOMIC CLAIM at
  initialization (S4.21) and re-verified at every open; it is not
  policed continuously on the wire (no leases, no heartbeats — that
  would be multi-server coordination, which stays out of scope).
  Single-writer-per-database remains the kernel lock.
- The namespace-init crash window (local id committed, claim lost)
  leaves a database that refuses closed until an operator either
  restores/deletes the state; this is fail-closed by design, not
  self-healing.
- Power-loss durability (as opposed to process crash) remains untested.

## 5. Relation to earlier rounds

S4.18 (design) asked whether a mutable node projection should be
authority, view or migration aid — S4.19 answered by elimination and
additionally published per-commit meta/ recovery copies; S4.20 removes
those copies (the commit chain lives only in SQLite) and narrows the
database-loss guarantee to content salvage. The COW tree, CAS, ref and
envelope model is unchanged. Legacy `meta/` objects from S4.19-era
deployments may exist in old buckets; nothing in vnext reads them
anymore, and nothing deletes them automatically.
