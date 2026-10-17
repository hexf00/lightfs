# Consistency & Workspace Design (S4.18)

This document defines LightFS's unified model for caching, commits,
offline workspaces and branching, against the code that actually exists
today (branch `s31-pack-proto`, post `v0.1.0-experimental`). It is a
design-convergence document: it changes no engine semantics, adds no
configuration system, and implements nothing. The last section names
the one recommended next implementation task.

Every claim below is anchored to real code (`file:line` at the time of
writing). The status vocabulary is strict:

- **implemented+verified** — capability exists and has passing tests or
  end-to-end runs.
- **prototype, limited** — code exists but is deliberately narrow.
- **not implemented** — no code path provides it.
- **unverified** — claimed by code/docs but never exercised.

## 1. Ground truth: what exists today

| Capability | Status | Evidence |
| --- | --- | --- |
| Strict sync commit: data PUT + metadata CAS both durable before ack | implemented+verified | `core.rs:1048-1120`; single-statement CAS `metadata/sqlite.rs:741-786`; MinIO contract + integration suites |
| In-flight version registration (`version_candidates`, consumed in the CAS transaction) | implemented+verified | `metadata/sqlite.rs:1344-1354`, `:776-786` (S4.16 series) |
| Server-side Write-Back staging: reserve → fsync file → fsync dir → ledger tx → ack | implemented+verified | `wb.rs:596-733` (ordering `:672-687`), real-child crash tests (p0c/p0d) |
| WB background commit state machine + capacity ledger + recover | implemented+verified | states/errors `wb.rs:194-200`; recover `wb.rs:418-503`; ledger `metadata/sqlite.rs:1864-1952`; `drain_workers` forced to 1 `engine.rs:74-95` |
| Immutable data objects + `(id, version)` CAS protocol end to end | implemented+verified | keys `core.rs:100-114`; CAS `core.rs:1225-1242`; RPC/HTTP/FUSE anchors in §1.1 |
| Client content read-cache: `(node_id, offset, len)` slots, every hit revalidated against live `(version, etag)`; commit path structurally bypasses it | implemented+verified | `readcache.rs:213-278`, `:521-556`; authoritative bypass `:560-568`; `tests/client_integration.rs:174-198` |
| Client metadata cache (path-keyed TTL; only self-mutations invalidate) | implemented+verified | `client.rs:29-85`; stale-within-TTL contract `tests/client_integration.rs:113-155` |
| Pack storage with refcounted 5-state lifecycle | implemented+verified | `metadata/sqlite.rs` packs schema `:104-167`; S4.10–S4.16 series |
| Backup manifests as GC protection set (fail-closed) | implemented+verified | `backup.rs:269-295`, `:359-398`; `scripts/backup-restore-drill.sh` |
| Offline GC: registered-intent only, never lists the bucket | implemented+verified | `core.rs:343-419`; `scripts/s431-gc-e2e.sh` |
| Policy-based client routing (per-path `PolicyView`) | prototype, limited | `policy.rs:110-121`; client-side only (`cmd.rs:203-210`); `commit` dimension accepts `StrictSync` only — `writeback` parses as reserved and is rejected `policy.rs:67-68` |
| Client-local (negative-id) backend | prototype, limited | `localfs.rs:10-19`, `LOCAL_FLAG 1<<63 :41-43`; ids stable only within one mount; `TaskScoped` lifecycle; cross-rule rename = `EXDEV` `routing.rs:367-387` |
| FUSE mount client (per-handle buffers, detached inodes, read overlay) | implemented+verified (in-container; native gate not run this release) | `fuse.rs:1239-1361`, `:325-432`; container fuse-gate history; native `/dev/fuse` gate **unverified** this release |
| WinFsp mount adapter | prototype, limited (experimental; path-form only, no by-id) | `winfsp.rs` (no `read_by_id` usage); CI mount-smoke green |
| Disk data cache (`[cache.data]`) | not implemented | config exists, enabling only warns `client.rs:160-169` |
| Client offline writes (durable local journal/queue) | not implemented | offline client = transport error `client.rs:186`; no retry queue, no client-side on-disk staging |
| Leases / invalidation push / cross-client eviction | not implemented | no lease/notify/watch in `client.rs`/`server.rs`/`rpc.rs` |
| Branch refs, snapshot read views, merge | not implemented | no root/HEAD record at all: current tree = whole `nodes` table, root = `id=1` (`metadata/sqlite.rs:430-434`) |
| Immutable metadata root / commit records | not implemented | commits mutate `nodes` rows in place (`version` column) |
| Server-side subtree authorization | not implemented | policy `shadow` is client-side probing (`routing.rs:185-204`), not an authority boundary |
| Multi-server coordination (same bucket, two DBs) | not implemented | only single-host flock per DB path `engine.rs:97-109`; second server's objects are "unknown" to the first (fsck reports them orphan) |
| Power-loss durability (`synchronous=FULL`+WAL) | unverified | process-death is tested everywhere; no host has lost power on this code |
| Real cloud endpoints (AWS S3, OSS, …) | unverified | MinIO only (see `docs/storage-response-semantics.md`) |

### 1.1 The one identity rule that already works

Everything durable is anchored by `(node_id, version)`:

- objects are content-immutable under `objects/<id>/…` (`core.rs:100-114`);
- commits are a single-statement CAS on `nodes.version` (`metadata/sqlite.rs:741-786`);
- the client cache keys slots by `node_id` and revalidates `(version, etag)` on every hit (`readcache.rs:105-130`, `:521-556`);
- RPC/HTTP carry `Node-Id`/`Expected-Version` (`server.rs:371-392`);
- deleted ids answer `IDENTITY_GONE`, never silently alias (`core.rs:551-557`).

This is the asset the workspace model must keep. The liability is the
other half: **there is no immutable commit**, and "the tree" is
defined only as *whatever the mutable rows say right now*.

## 2. Unified core model

Five concepts, mapped to what exists:

| Concept | Today | Target |
| --- | --- | --- |
| **Namespace** — logical tree + permission boundary | one namespace per server (`nodes` table, root `id=1`); policy prefixes are a *client-side* illusion; no server-side authz | Namespace becomes a server-side first-class scope: a named tree with its own authority; mounts are granted a namespace (or subtree), `shadow` becomes subject to that grant |
| **Branch Ref** — mutable pointer to a commit | absent; no HEAD record exists | a named, atomically-updated pointer to a commit id |
| **Immutable Commit (metadata root)** | absent; `nodes` rows are mutated in place | an immutable record of "tree state after these changes": parent commit + delta (changed/removed paths → node records). Reading a commit = fold deltas from a base |
| **Immutable Object** | exists: content keys are write-once, refcounted, GC'd by registered intent | unchanged; commits *add* referents (commit records themselves) that GC must respect |
| **Local Workspace / Journal** | client buffers are memory-only (`ClientWriteSession.chunks` `client.rs:106-143`, FUSE `PendingWrite`); server WB staging is *server-local*, not client offline | a client-side durable journal of uncommitted changes, anchored on `(id, version)` like everything else |

**Evolution path from the mutable-SQLite present** (no big-bang rewrite):

1. **Introduce a durable root pointer and commit records while SQLite
   stays the execution layer.** Each successful mainline commit (strict
   CAS, or WB drain CAS) additionally appends an immutable commit record
   (object or table) holding `{parent, delta}`, and advances a singleton
   root ref in the same transaction that today consumes the
   `version_candidates` row. The `nodes` table remains the hot read
   path; the commit chain becomes the *history*, not a replacement for
   it. Nothing about existing client protocol changes.
2. **Branches = second root refs.** A branch is a named row in the same
   root-ref table pointing at any commit. The single-writer engine-lock
   remains the *fork-head* arbitrator per namespace; cross-machine
   branch-head updates must reuse the proven pattern: register-intent
   (`version_candidates`-style durable pre-registration) + conditional
   consume — never bare last-writer-wins on S3.
3. **Workspace journal = persist what FUSE already buffers.** The
   FUSE write path already delays commit until flush/close and keeps an
   overlay for read-your-own-writes (`fuse.rs:325-352`, `:1239-1361`);
   the journal is that state spilled to disk under `(id, base_version)`
   anchors, so a mount restart can replay or discard explicitly. It is
   a **new component**, not a config flag on existing buffers.

**Assumptions to keep**: ids are never reused (AUTOINCREMENT marker
`metadata/sqlite.rs:534-576`); object keys are immutable; writes CAS on
version; GC is registration-based and never bucket-scans. **Assumptions
to shed**: single implicit HEAD; `nodes` rows as the *only* truth;
"tree" being undefinable at a past point in time.

## 3. Operation semantics and the guarantee matrix

**What write/flush/fsync/close mean today**

- `write` (FUSE or shell): appends into a **memory buffer** in the
  client process. Nothing is sent, nothing is durable anywhere.
- `flush`/`fsync`/`close` (FUSE) and `commit` (RPC clients) all
  converge to one atomic submission: strict = data PUT + metadata CAS
  both completed → ack (`core.rs:1048-1120`); WB server = staging file
  fsync + dir fsync + ledger tx → ack with a **projected** version
  (`wb.rs:672-687`, `:727-732`).
- FUSE gates close on prior flush failure: a session that once failed
  may not report "error then committed anyway" (`fuse.rs:1264-1328`).

**Status lattice** (the words used consistently below):

| State | Data lives in | Authority | Survives process crash | Can continue offline | Visible to other clients |
| --- | --- | --- | --- | --- | --- |
| **Local-visible** | client process memory | client (tentative) | no — lost on client death | n/a (nothing to continue) | never |
| **Client-durable** *(not implemented)* | client journal on local disk | client (tentative) | yes | yes | never |
| **Server-staged** (WB ack) | server-local staging file, fsynced file+dir | server (durable, not mainline) | yes — server restart replays via `recover` (`wb.rs:418-503`) | no — client already needed the network to reach the server | no — others read `nodes`, which still says the old version |
| **Server-accepted / Mainline-committed** (strict ack) | immutable object + `nodes` row (SQLite `FULL`+WAL) | server (authoritative) | yes (power-loss separately **unverified**) | no | next read that bypasses or outlives their TTL: metadata ≤ `metadata_ttl` (default 5 s), data reads always live-validated |
| **WB background committed** | immutable object + `nodes` row, written by the drain worker | server | yes (recover requeues) | no | same as strict, from the moment of the background CAS |

**Forbidden conflations** (violated by none of the current docs, and by
this one on purpose): client `write` returning is not any kind of
durability; WB ack is not mainline — a WB task can later die with
`error_class=conflict` and the projected version then names content
that will never be at that path (`wb.rs:194-200`); server staging is
not client-offline — the client was online to reach it.

**Sync & conflict resolution today**: CAS `expected_version`, conflict →
`CONFLICT`; deleted id → `IDENTITY_GONE`; clients avoid false conflicts
by observing the live version at `open_write` (`client.rs:474-477`).
There is no merge: conflicts are terminal per write.

## 4. Orthogonal configuration dimensions

Five independent axes. Exposed profiles should pin all five; arbitrary
cross-products are not offered.

| Axis | Values |
| --- | --- |
| **Read freshness** | `latest` (default) · `leased` (online view held under a lease — not implemented) · `pinned` (fixed snapshot/branch version — not implemented) |
| **Cache** | `none` · `memory` (exists, validated) · `disk` (not implemented) · `memory+disk` (not implemented) |
| **Write durability** | `memory` (today's pre-commit buffer) · `client-durable` (not implemented) · `server-staged` (WB) · `remote-committed` (strict) |
| **Publication** | `immediate` (strict/WB drain) · `background` (WB) · `manual` (workspace publish — not implemented) |
| **Conflict handling** | `reject` (today, everywhere) · `keep-both` (needs branches) · `explicit-merge` (needs branches) |

**Profiles** (the only shapes users should see):

1. **`strict`** = latest + cache none/memory + remote-committed +
   immediate + reject. *Today's server default; fully implemented.*
2. **`writeback`** = latest + memory + server-staged + background +
   reject. *Implemented; the honest caveat is that `reject` here is
   server-side dead-lettering, invisible to an already-acked client —
   acceptable only because WB is default-OFF and experimental.*
3. **`workspace`** (future) = pinned/leased + memory+disk +
   client-durable + manual + keep-both/merge. *Not implemented; the
   journal, branches and merge are its prerequisites.*

**Forbidden combinations** (and why):

- `client-durable` + `latest` without a lease — an offline writer
  cannot also promise latest-view reads; lies or races by construction.
- `remote-committed` + `manual` — contradiction: the commit *is* the
  publication.
- any disk cache without per-hit version revalidation — the cache
  would become a correctness oracle; the current memory cache is only
  admissible because of live `(version, etag)` checks and the
  authoritative-path bypass (`readcache.rs:521-568`).
- `keep-both`/`explicit-merge` without the commit model — there is
  nowhere to put the second version.
- `leased` before leases exist — no code may advertise it (the same
  discipline that keeps `cache.data` honest today).

The existing `PolicyView` (`authority / read_cache / commit /
lifecycle`, `policy.rs:110-121`) is the natural home for the first
three axes once their values exist; publication and conflict are
namespace/branch-level, not per-path, and should NOT be folded into
per-path policy.

## 5. Minimal branch & conflict model

- **Create branch**: record `{name → commit-id C}`; zero data copied —
  the branch head *is* C. Reads through the branch fold C's chain.
- **Common ancestor**: commits form a parent chain per first version
  (linear per branch); merge-base = last commit reachable from both
  heads by parent links. First version deliberately has no DAG merges
  (a merge commit has two parents; that's the only extension needed
  later).
- **Advance head**: conditional update of the branch-ref record, using
  the register-intent + conditional-consume pattern — the same shape
  the CAS path already trusts for `version_candidates`
  (`metadata/sqlite.rs:1344-1354`, `:776-786`). Never bare S3
  last-writer-wins.
- **Three-way merge, first version**: tree-level diff of
  `base → ours` and `base → theirs` keyed by path. Changes to
  disjoint paths merge cleanly; overlapping changes fall to the rules
  below. No textual merge, no CRDT, ever, in v1.

**Conflict rules v1** — in every case both sides' data survives:

| Case | Rule |
| --- | --- |
| Same file concurrently modified (both ≠ base) | keep `ours` at the path; copy `theirs` to the conflict area as a full file; mark both in the merge report |
| Delete vs modify | deletion is NOT applied; modified file kept at the path and flagged — the delete side's intent is recorded in the report for explicit resolution |
| Directory deleted vs child added beneath it | the added child wins (rescued into the conflict area under its path); the directory removal still proceeds — no data silently vanishes |
| Same path created on both sides | merge issuer keeps the path; the other side's object is copied to the conflict area; deterministic tie-break if both are merges: lexicographically smaller branch name keeps the path |

Conflict area: `.lightfs-conflicts/<branch>/<commit-id>/…` inside the
same namespace — ordinary immutable objects, visible, gc-reachable via
the merge commit. Resolution is explicit (choose/overwrite), never
implicit. A merge that cannot complete records a merge-commit with
conflict entries; it never drops a side.

## 6. Safety, backend and GC boundaries

- **Mounting a subtree ≠ being granted it.** `allow_shadow` lets a
  client adopt an existing remote subtree (`routing.rs:185-204`); the
  server has no notion of scoped credentials. Until namespace-level
  authz exists (§2), shadow must be documented as a convenience for a
  trusted client, never a security boundary; the design keeps
  Namespace as *the* place where server-side grants will live.
- **Caches are never correctness oracles.** Already structural for the
  memory cache (live revalidation + authoritative bypass + empty-etag
  exclusion, `readcache.rs:225-229`, `:316-319`, `:521-568`); any
  future disk cache must satisfy the identical contract or it does not
  get built. Metadata TTL staleness is bounded and documented
  (`client.rs:1-10`), never unbounded.
- **GC invariants (target form)**: an object may be deleted only if it
  is unreachable from *every* live root: current mainline state, every
  branch head, every pinned snapshot, every unexpired backup manifest —
  plus the registered-intent machinery that already works
  (`core.rs:343-419`). Concretely, the existing rule
  ("candidate sources + NOT EXISTS live reference + cutoff + backup
  protection") stays, but the reference set grows: commit records,
  branch heads and conflict-area objects become referents, and branch
  deletion becomes a new *intent-registration site*. A branch whose
  head commit is pruned without unregistering the branch is a GC bug
  by definition.
- **Backends declare capabilities.** The object-store trait must not
  assume S3-complete semantics: conditional writes, listing
  consistency, batch delete are *capabilities* a backend reports.
  `docs/storage-response-semantics.md` already records which S3 error
  paths are vouched for; the design adds: the branch-head CAS must be
  implementable on top of the register-intent pattern precisely so it
  does NOT require a compare-and-swap primitive from the backend.

## 7. Verification results that change architecture decisions

Only findings from this pass that alter what gets built next:

1. **No root/HEAD record exists anywhere** (`metadata/sqlite.rs:430-434`
   — tree = whole table). Every snapshot/branch/merge feature is
   blocked on this single primitive; nothing else can precede it.
2. **Two servers, two DBs, one bucket have zero coordination** —
   engine-lock is per-DB-path and single-host (`engine.rs:97-109`), and
   each server's GC only ever deletes keys its own DB registered.
   Correctness for that topology must come from object-storage-level
   protocol (register-intent + conditional consume), not from assuming
   one server. The branch design already assumes this.
3. **The client has no durable state whatsoever** — offline workspace
   is a new component (journal), not a configuration of existing
   buffers. Any plan that "turns on offline" is fabricating a feature.
4. **The register-intent + conditional-consume pattern is proven** in
   production paths (`version_candidates`) and in the TLA+ models
   (S4.16 series) — it is the right primitive to reuse for branch-head
   updates on backends without conditional writes.
5. **WB's ack-then-die gap is real and documented** (`error_class`
   conflict after a projected-version ack) — acceptable while WB is
   experimental and reject-only; the `workspace` profile's `manual`
   publication is what eventually closes it.

## 8. Recommended next task (single) and success criteria

**Task: durable commit chain, first slice** — every successful
mainline commit (strict CAS path and WB drain CAS) additionally
appends one immutable commit record `{parent, delta}` and advances a
singleton root ref inside the existing atomic window; SQLite stays the
execution layer; client protocol unchanged.

Success criteria (all measurable):

1. After N≥100 mixed strict commits, folding the commit chain from the
   initial root reproduces the `nodes` table exactly: same path set,
   same per-path `(id, version, object_key, etag)`; a byte-for-byte
   tree dump comparison passes.
2. Crash injection between "commit record written" and "root ref
   advanced" (real child-process kill, existing p0-style harness)
   converges on restart: replay reaches a state consistent with the
   advanced-or-not root ref, and no commit object is left untracked by
   GC (chain objects are reachable from the root ref, torn ones get
   registered for deletion).
3. WB parity: drain-path commits produce the same chain; recover
   replays do not duplicate records (requeue is idempotent at the
   chain level).
4. Performance guard: strict serial throughput within 5% of the
   `v0.1.0-experimental` baseline (container 737 files/s class, CI
   1220 files/s class — same-harness comparison only).
5. GC coexistence: with the chain populated, `lightfs gc` still deletes
   only registered keys, chain objects survive gc, and fsck reports the
   chain clean; a deliberately orphaned commit record (simulated torn
   write) is collected via the existing intent path.

This slice is chosen because §7.1 makes it the blocker for *every*
workspace/branch feature, it reuses only proven patterns, and its
success is checkable without building any user-facing feature on top.

*Scope guard: this document implements none of §5's merge rules and
none of §4's `workspace` profile. It only fixes the vocabulary and the
first slice of ground work.*
