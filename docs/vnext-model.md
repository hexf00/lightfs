# The vnext model — SQLite authority, payload-only object store

Status: S4.20 (branch `vnext`, supersedes the S4.19 recovery-copy design).
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
  (`objects/{fid}/w-{uniq}.data`, envelope v1: magic, format version,
  kind, encoder, cipher, logical length, sha256) plus backend-internal
  physical structures. Normal commits write NOTHING else — no
  `meta/refs`, no `meta/commits`, no `meta/trees` (the S4.19
  per-commit recovery copies are gone, not deferred or batched).
- The ONLY non-payload object is `namespace/marker` (envelope kind
  `Namespace`): written ONCE at initialization, re-verified on every
  open. It carries a random namespace id that SQLite holds in its
  `model` table. Rules:
  - fresh database + no marker → write SQLite id, then the marker;
    a failure between the two steps fails the NEXT open closed.
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
  (put/put_stream/get/get_stream/head/list/delete/presign). The core
  holds logical references (the persisted `o`/`e` entry fields carry
  the physical locator + etag), never S3 semantics: no LIST
  consistency, no multipart state, no bucket addressing inside the
  commit protocol. S4.20 runs ONE configured backend; nothing here
  precludes per-class backend routing later, because routing would be
  a write-time decision recorded in the reference — never a read-time
  recomputation.

## 2. Commit protocol

```
1. PUT data object   (envelope v1, immutable key)
2. ledger the key    (durable SQLite row)
3. BEGIN IMMEDIATE   walk base trees INSIDE the tx (no stale-snapshot
                     lost updates), COW the touched chain, insert
                     file_version, CAS entry version, append commit,
                     advance ref — one transaction
4. COMMIT            ← the commit is durable HERE; nothing follows
```

Crash decomposition:

| crash window | state after restart |
|---|---|
| during 1 / before 2 | raw object, no ledger row: gc can NOT prove it dead → kept (fail-safe; salvage can read it) |
| during 2 / before 3 | ledgered, unreferenced → reachability gc reclaims |
| during 3 | one transaction → SQLite rolls back; ledgered-orphan as above |
| after 4 | done — there IS no post-commit publication step to lose |

## 3. Recovery

**A. Normal transaction recovery (SQLite intact or recoverable).**
WAL + synchronous=FULL; a killed or crashed process recovers the full,
consistent namespace from SQLite alone (covered by graceful-restart and
SIGKILL integration tests). GC keeps its fail-closed gate: the model
marker row AND at least one ref row must be readable IN SQLITE, or gc
refuses; unreachable-provable objects only; `objects/` keys without a
ledger row are NEVER deleted (they cannot be proven dead). Corrupted or
missing SQLite fails the store open, so gc has no entry point at all.

**B. Database lost: content salvage, READ-ONLY.**
`vnext::recover::salvage_all_data` enumerates the data prefix (a
DECLARED capability of this salvage path — a backend without
enumeration cannot offer it, explicitly), verifies each object's
envelope (magic, format version, kind, encoder/cipher, logical length,
sha256) and exports byte-exact payloads with their original keys, the
stable file id parsed from the key layout, and per-object status:
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
- The namespace marker is checked at open. Two databases initialized
  CONCURRENTLY on the same empty bucket is a deployment violation the
  marker cannot referee after the fact (last writer wins the marker;
  the loser refuses at its NEXT open). Single-writer-per-database is
  enforced by the kernel lock; one-database-per-bucket is enforced at
  open, not continuously.
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
