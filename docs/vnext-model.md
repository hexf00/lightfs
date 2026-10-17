# The vnext model — immutable metadata authority over object storage

Status: S4.19 first integration slice (branch `vnext`). This document is
the authoritative description of the new core: what is authoritative,
what is rebuildable, how crashes decompose, and what is deliberately out
of scope.

## 1. Authority

Five object classes, all immutable except refs:

```
refs        (mutable, one row per branch: name → commit sha, seq)
  └─ commits        {parent, root_tree, seq}            meta/commits/<sha>
       └─ trees     COW directories, entries JSON,
                    content-addressed (sha256 of canonical JSON)
                                                       meta/trees/<sha>
             └─ file_versions  (file_id, version) unique — the optimistic
                               concurrency anchor per FILE
                    └─ data objects (envelope v1)    objects/<fid>/w-*.data
```

- The LOGICAL authority is the object store: `meta/refs/{name}` names a
  commit; walking parent links folds every tree the namespace ever had.
  `vnext::recover::namespace_view` does exactly this and needs no
  database (covered by tests against real MinIO).
- SQLite (`src/vnext/store.rs`, seven tables) is the EXECUTION layer:
  current tree row ids, the per-file version table that makes conflicts
  explicit, the object ledger. Every tree/commit row carries the
  content hash it publishes; the store refuses to open for writing when
  the initial commit's hash does not match its canonical record.
- **No second mutable projection exists.** The S4.18 "mutable nodes
  table" idea was NOT carried into vnext: there is no mutable node-row
  authority anywhere. What SQLite holds is either (a) the immutable
  content-addressed rows themselves (trees/commits — verifiable against
  the objects they publish), (b) per-file version rows (the concurrency
  anchor), or (c) best-effort recovery COPIES of `meta/` objects
  (published after COMMIT; losing them loses nothing — the object store
  is the recovery source, never the reverse). One correctness authority,
  one rebuildable cache.

## 2. Commit protocol

```
1. PUT data object            envelope v1: "LF1\n" magic, format version,
                              kind, encoder=raw, cipher=none, logical len,
                              sha256 digest over the whole object
2. ledger the object key      (durable "this key exists" row)
3. BEGIN IMMEDIATE            walk base trees INSIDE the tx (a stale walk
                              cannot revert another writer's commit),
                              COW the touched directory chain, insert
                              file_version, CAS entry version, append
                              commit, advance ref — all in ONE tx
4. COMMIT
5. publish_recovery           trees + commit + ref projections to meta/
                              keys, best-effort (their loss is invisible)
```

Crash decomposition:

| crash window | state after restart |
|---|---|
| during 1 / before 2 | raw object, no ledger row: gc can NOT prove it dead → kept (fail-safe; salvage can read it) |
| during 2 / before 3 | ledgered, unreferenced → reachability gc reclaims |
| during 3 | one `BEGIN IMMEDIATE` tx → SQLite rolls back; the object is ledgered-orphaned as above |
| after 4, during 5 | commit is durable; projections re-publishable; recovery view unaffected |

Concurrent semantics: per-FILE optimistic concurrency
(`(file_id, version)` unique — a stale base gets an explicit
`Conflict{expected, actual}`), explicit `advance_ref` CAS for branch
heads, and one engine per database (kernel-held `fslock`, cross-process).
A fresh store that meets a bucket already carrying a DIFFERENT root
fails closed (`publish_initial_commit`); there is no cross-server
coordination, by design (S4.18).

## 3. Recovery layers (metadata disaster recovery is a first-class goal)

| layer | owner | mechanism |
|---|---|---|
| data-block salvage | objects/ prefix | list + envelope verification (`salvage_all_data`) |
| file reassembly | vnext slice | one object = one file version (chunking is a FUTURE encoding layer; the core holds stable `(file_id, version)` references and never parses payload formats) |
| namespace folding | meta/ trees+commits | fold trees via entries' `w` (child content hash) down the parent chain of the ref (`namespace_view`) |
| latest trusted commit | meta/ refs projection | highest seq reachable ref; commits present but unreachable from any ref = unpublished transactions (reported, not applied) |

Failure boundaries are explicit: SQLite lost → layers 3–4 rebuild
everything; a meta/ object corrupted → the envelope digest refuses it
(`ObjectCorrupt`, never silent wrong bytes); an unknown format version,
encoder or cipher is refused, never interpreted; the object store being
unreachable is reported as `Storage`. What vnext does NOT yet have: a
tool that ADOPTS a recovered view as a new writable database (recovery
today is a read-only view + salvage listing).

## 4. Object envelope (format boundary)

`envelope v1`: 52-byte header — magic `LF1\n`, u32 format version, u8
object kind (Data/Tree/Commit/Ref), u8 encoder (0 = raw), u8 cipher
(0 = none), u64 logical length, sha256 over the whole object. The core
validates ONLY the header (kind + version + length + digest presence);
encoder/cipher interpretation belongs to a future encoding layer.
Compression (Zstandard per object) and AEAD encryption are RESERVED
FIELDS, not implemented; nothing assumes "payload = raw bytes forever".

## 5. Known limitations (deliberate, this slice)

- Write-back (commit.mode = "writeback") is refused at config time; the
  v0.1 WB machinery is frozen on main. Strict commit only.
- Offline `gc` / `fsck` / `backup` CLIs are gone with the v0.1 core;
  reachability GC exists as a library call (`vnext::gc::collect`,
  fail-closed gate: refuses to run without the model marker and at least
  one readable ref; `meta/` keys are never candidates).
- FUSE/WinFsp adapters are wired to the new core but not exercised on a
  real mount in this slice's tests (kernel-level gate only).
- Per-commit cost is O(tree depth) small writes + O(width of each
  touched directory) serialization — measured +20% on a 300×8 KiB
  serial import vs v0.1 (same machine, same MinIO,
  `scripts/s419-perf-compare.sh`); reads are at parity (±10%, n=1 runs).
- Recovery is read-only (see §3); no branch-management UI, no merge,
  no leases, no notifications — out of scope by task definition.

## 6. Relation to S4.18

S4.18 (design) named the durable commit chain as the single next task
and asked the question "is the mutable nodes projection an authority, a
view, or a migration aid?" This slice answers it by ELIMINATION: the
projection does not exist; the ref→commit→tree chain is the only
authority and SQLite is its anchored execution layer. The five S4.18
concepts map 1:1: Namespace = the `main` ref's folded tree, Branch Ref =
a named ref row, Immutable Commit = commits table + meta/commits
object, Immutable Object = data objects, Local Workspace = not
implemented (no client-local state in this slice).
