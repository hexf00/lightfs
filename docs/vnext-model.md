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
  (`objects/{object-id}.data`, envelope v2 — format, checksum scope and
  length semantics are specified in §6) plus backend-internal
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
1. PUT data object   (envelope v2, immutable Object-ID key) — NO SQLite
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
envelope (magic, format version, kind, encoder/cipher, checksum id,
payload length, footer checksum — §6) and exports byte-exact payloads
with their original physical LOCATORS — diagnostic information only, never parsed into a logical
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

## 6. Object encoding layer (S4.23)

**Responsibilities.** The CORE owns files, versions, CAS, commits and
logical references — it contains no envelope byte arithmetic (no header
size, no footer offset); it speaks only the encoding layer's API:
`encoded_len`, `prefix_range`, `payload_range`, `payload_slice`,
`seal`/`open`, `seal_reader`. The ENCODING LAYER (`vnext::envelope`)
owns the wire format and integrity. The PHYSICAL BACKEND persists,
reads back (whole or ranged) and honestly declares capabilities; it
never interprets envelope bytes (S3-side checksums/multipart/ETag are
the backend's own concerns, not contract terms for other backends).

**Wire format v2** (replaces v1; v1 objects are refused by name, no
migration branch — pre-v2 buckets fail closed at first read):

```
0    4   magic "LF2\n"
4    1   format version = 2
5    1   object type (1 data, 2 tree, 3 commit, 4 ref, 5 namespace)
6    1   encoder id (0 = raw; unknown → explicit refusal)
7    1   cipher id (0 = none; unknown → explicit refusal)
8    1   checksum algorithm id (1 = SHA-256; unknown → refusal)
9    3   reserved (must be zero)
12   8   payload length (little-endian)
20   ..  payload
end-32  FOOTER: SHA-256( header[0..20] || payload[0..len] )
```

Total overhead is 52 bytes per object — identical to v1. The checksum
covers the canonical header AND the full payload (v1 covered the
payload only), so metadata tampering is detected on a full read. The
footer is computable in one streaming pass — the v1 header digest
required hashing before writing, hence a second pass or a full in-memory
copy; v2 emits header, streams payload through the hasher, then emits
the footer.

**Length vocabulary (do not collapse these).** LOGICAL FILE LENGTH —
how long the application's file is; owned by the caller/core metadata,
UNKNOWN to the envelope. PAYLOAD LENGTH — the 8-byte header field, the
only length this format defines and the checksum's data scope.
PHYSICAL ENCODED LENGTH — `header + payload + footer`, known before any
upload. Under `enc=0` (raw) all three coincide; that identity belongs
to the raw encoder id, NOT to the format. A future trusted-client
encoder (compression or AEAD applied before the RPC) delivers
ciphertext payloads: the server still verifies what it sees (header +
ciphertext), learns nothing about plaintext digests, keys or decoders,
and the logical length becomes a property of that encoder's format.
`enc`/`cipher`/checksum ids are refused explicitly when unknown; no
envelope rule assumes payload bytes are readable plaintext.

**Hash inventory (purposes are distinct; none of these is content
deduplication):**

| hash | where | purpose / invariant | owner |
|---|---|---|---|
| envelope SHA-256 footer | `vnext::envelope` | integrity of the bytes the encoding layer stores (header+payload); verified on FULL reads, pinned reads and salvage; never a lookup key | encoding layer |
| Object ID digest | `core::new_object_id` = sha256(uuid)[:32hex] | UNIQUE key material only (`objects/{oid}.data`); randomness folding, not content addressing | core |
| etag | object store responses | BACKEND VALIDATOR: recorded-vs-observed object identity on reads (and cache revalidation); for multipart uploads it is NOT an MD5 of content and is never described as one | physical backend |
| SigV4 payload hash + HMAC-SHA256 | `sigv4.rs` / HTTP client | request signing (transport), per request | HTTP layer |
| tree/commit content hashes | — | do not exist: SQLite is the sole metadata authority (S4.20); `Tree/Commit/Ref` kinds decode as legacy identifiers only | — |

**Read guarantees by shape.** FULL read (whole file from offset 0,
server/RPC entry): ONE full-object GET whose own response carries the
validator; the object is stream-decoded (header verified, payload
through a bounded buffer, incremental `SHA-256(header||payload)`) into
a PRIVATE staging file and PUBLISHED to the caller only after the
footer verified — the served reader IS the validated file. No extra
HEAD; the etag comparison uses the same GET response (never a second
request, never a two-version assumption). Range read (partial window
or non-zero offset): ONE ranged GET from offset 0 (header + requested
window; the footer is not fetched and adds NO request), deeper ranges:
header GET + window GET; payload integrity rests on the recorded etag
comparison, unchanged. Pinned/past-commit reads (`read_at`): ALWAYS
full-verified — the range is carved out of the verified staging file
AFTER validation; memory is O(range) but the FULL object still
travels the network because the checksum needs every byte (downgrading
pinned reads to ranged GET + validator would be a separate correctness
contract decision, deliberately not made). Empty objects decode and
verify like any other. Unknown format/encoder/cipher/checksum id,
non-canonical header, truncated payload, missing or wrong footer,
trailing bytes, forged/overflowing lengths: all fail closed with named
errors (checked arithmetic in the decoder; nothing allocates by
declared length).

**The staging trade (S4.24, measured).** Full reads trade heap for
disk: the verified object occupies a temp file of `payload_len` bytes
in `std::env::temp_dir` for the lifetime of the served reader (max
concurrent full reads × file size), then it is deleted — on drop, on
any verification failure, on short read, on IO error, on cancellation;
failed verification never publishes bytes and never leaves residue.
Wall-clock cost is real and reported, not hidden: local full-read
benchmarks pay ~0.7 ms per small read (file lifecycle) and roughly
double for a 256 MiB read, with request counts byte-identical to the
pre-staging build. Peak server RSS for a full 256 MiB read drops from
~333 MB (whole-object Vec + payload copy) to ~5.4 MB flat, and salvage
of 384 MiB across many objects peaks at ~50 MB with a hashing consumer.

**Salvage output (S4.24).** `salvage_all_data` is per-object streaming:
each object is decoded and verified into a private staging file and
handed once to a consumer sink (`SalvageSink::verified(locator, len,
reader)`) — only metadata survives in the report (locator, payload
length, rejection reasons); payload bytes are never accumulated.
Consumer IO failures are recorded per-object; the staging file is
deleted on every exit path; nothing is ever written back to the store.
The pass still recovers CONTENT candidates only — no paths, no
directory structure, no commit history, no adopt tool.

**Memory bounds (measured).** `write_stream` (the server RPC path):
staging file on disk → single-pass streamed encode → `put_stream`
(single PUT ≤8 MiB objects, 8 MiB parts above) → server upload-window
peak RSS is FLAT in file size (~31-40 MB at 256 MiB, vs ~277 MB linear
on the pre-streaming build). `commit_write` (interactive session path)
still holds the session buffer: O(file size), documented, NOT claimed
bounded. Full reads and salvage are bounded as described above.

## 7. The mounted-write boundary (S4.25)

**Staging budget.** Every process that stages bytes to disk holds ONE
`StagingBudget`: a flock-guarded `lightfs-staging-<pid>-<nanos>` directory
under the configured root (`[staging].dir`, default the system temp
directory) with arithmetic admission (`budget_bytes`, `max_object_bytes`;
refusal is `Error::Capacity` = ENOSPC, never eviction, never spin).
`budget_bytes` default is 2 GiB. Dead processes' directories are swept
provably-dead at startup by sweep participants (server boot, engine
open): a directory whose `staging.lock` is not flocked is removed.
Short-lived CLIENTS open their budget lazily (first write session) and
without sweeping — a per-process temp-root scan was a measurable
per-operation cost (every CLI invocation is a process); orphans are
reclaimed by the next sweep participant sharing the same root.

**Write sessions spool to disk.** A mounted write session (FUSE or
WinFsp) buffers dirty bytes in a `WriteSpool`: one private 0600 file,
random writes, sparse holes, two-sided truncate, read-your-writes over
a base, dirty-range overlay. The commit streams the envelope straight
out of the spool around the base content — the session never holds the
whole file in the heap. Server-side write sessions (`WriteHandle`) and
client-side sessions (`ClientWriteSession`) share the same spool. The
session's contribution to the visible size is its highest dirty byte
end; `close` on a session with no writes commits nothing.

**The mounted-write commit over the wire.** `FsApi::commit_write_merged`
is the mount flush: the file's CURRENT committed content streams in as
the base (a never-committed file has NO base — `ObjectMissing` on the
base read means "merge over nothing"), the spool's dirty ranges win,
and the merged whole uploads through the same `/data/write`
(Node-Id + Expected-Version + Length) any streamed write takes. The
CORE implements this in-process; the network `Client` overrides it —
the trait DEFAULT propagates the base-read error, which is exactly why
every first flush of a new file failed on a real mount while in-process
tests passed. Adapters must go through the override, not the default.

**Reads at EOF never touch content.** An explicitly zero-length
request returns empty without any object I/O. A file created but never
written has NO object at all, and ANY read of it is end-of-file (POSIX)
— never `ObjectMissing`. WinFsp accept A9 falsified the earlier
"nonzero reads keep ObjectMissing" boundary: real filesystem reads
always request >= 32 bytes (std::fs::read drives read_to_end), so a
request-size-only short-circuit never fires for them. The precise
contract, identical on all four read entries (`read` / `open_read` /
`read_by_id` / `open_read_by_id`): zero-length REQUEST = success with
no object I/O (even if the object is gone); a no-object file reading
at/past EOF = empty; a file WITH an object always takes the verified
path (an empty object is verified, never assumed). Whole-file
`read_stream_by_id` stays strict — "no base" is a signal the commit
merge consumes, not an error to erase.

**Wire codes.** `ObjectMissing` maps to the distinct `OBJECT_MISSING`
code (404), NOT to `NOT_FOUND` — collapsing it destroys the "no base"
semantics above on every wire client.

**FUSE inode namespace.** Kernel inos are adapter-allocated (a
monotonic counter plus a path↔ino map), never node ids: vnext
directories have tree ids from a separate rowid sequence, and the root
tree id changes on every commit (COW). Handle-based operations resolve
the REAL node id through the adapter's map (`node_id_of`); getattr is
path-anchored because a directory's id is meaningless across commits.
The root directory is mode 0755 — 0555 makes `default_permissions`
(and WinFsp) reject every create inside the mount.

**Measured (local, five rounds × 300 files, shell RPC).** Baseline
5d8ca74 vs S4.25: small-file write ~10.1 → ~11.5 ms/op, read ~7.0 →
~7.5 ms/op (within run-to-run drift; the residual ~1 ms is the
per-session spool file lifecycle — the bounded-memory contract, paid
where it is owed). An intermediate build paid 21.5/17.9 ms — the
eager-sweep defect above, since fixed. Mounted-write RSS on the Linux
FUSE gate: 2668 KB (hwm 3008) for a 64 MiB mount write, 2112 KB for
256 MiB — bounded by the spool, not the file.
