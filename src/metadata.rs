//! MetadataStore abstraction (control plane) and the SQLite backend.
//! One process owns one metadata store; all namespace mutations happen here.

pub mod sqlite;

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Dir,
    File,
    Symlink,
}

impl NodeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeKind::Dir => "dir",
            NodeKind::File => "file",
            NodeKind::Symlink => "symlink",
        }
    }
    /// Parse the stored string form ("dir"/"file"/"symlink"). Named to avoid
    /// confusion with std::str::FromStr.
    pub fn parse_kind(s: &str) -> Option<Self> {
        match s {
            "dir" => Some(NodeKind::Dir),
            "file" => Some(NodeKind::File),
            "symlink" => Some(NodeKind::Symlink),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub name: String,
    pub kind: NodeKind,
    pub size: i64,
    /// Unix seconds.
    pub mtime: i64,
    /// Permission bits (u32 as stored in sqlite INTEGER).
    pub mode: u32,
    /// Data plane key (`objects/<id>/data` for files, None for dirs).
    pub object_key: Option<String>,
    /// Content version, increments on every committed write (stored as TEXT).
    pub version: i64,
    /// ETag of the last committed object.
    pub etag: String,
    /// Symlink target, stored VERBATIM (relative or absolute); None for
    /// every other kind. Resolution semantics live in Core, not here.
    pub target: Option<String>,
    /// S3.1 pack prototype: when `pack_off` is set, the file's bytes live
    /// INSIDE the immutable pack object `object_key` at [pack_off,
    /// pack_off+pack_len). The pack's ETag is NOT this file's etag (which
    /// is empty for pack-backed rows — the per-file content digest rides
    /// in `content_hash` instead).
    pub pack_off: Option<i64>,
    pub pack_len: Option<i64>,
    pub content_hash: Option<String>,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        self.kind == NodeKind::Dir
    }
    pub fn is_file(&self) -> bool {
        self.kind == NodeKind::File
    }
    pub fn is_symlink(&self) -> bool {
        self.kind == NodeKind::Symlink
    }
    /// True when this node's content lives inside a shared immutable pack
    /// object (S3.1 prototype). Such rows must never be retired/deleted as
    /// if `object_key` were their own — the pack is shared.
    pub fn is_pack_backed(&self) -> bool {
        self.pack_off.is_some()
    }
}

pub trait MetadataStore: Send + Sync {
    /// Create a node; fails with AlreadyExists on duplicate (parent_id, name).
    fn create_node(
        &self,
        parent_id: i64,
        name: &str,
        kind: NodeKind,
        mode: u32,
        object_key: Option<&str>,
    ) -> Result<Node>;
    /// Create a symlink node whose `target` is stored verbatim (never
    /// resolved at write time). size records target.len() for lstat.
    fn create_symlink(&self, parent_id: i64, name: &str, target: &str, mode: u32) -> Result<Node>;
    fn get_node(&self, id: i64) -> Result<Option<Node>>;
    fn get_child(&self, parent_id: i64, name: &str) -> Result<Option<Node>>;
    fn list_children(&self, parent_id: i64) -> Result<Vec<Node>>;
    fn count_children(&self, parent_id: i64) -> Result<i64>;
    /// Commit new content state for a file with optimistic concurrency:
    /// succeeds only if the node's current version is still `expected_version`
    /// (compare-and-swap inside one statement). On mismatch returns
    /// Error::Conflict and changes nothing — a losing writer can never
    /// overwrite the winner's metadata or bytes.
    fn commit_content_cas(
        &self,
        id: i64,
        expected_version: i64,
        size: i64,
        mtime: i64,
        new_object_key: &str,
        etag: &str,
    ) -> Result<()>;
    /// Update permissions/mtime only.
    fn update_attrs(&self, id: i64, mode: u32, mtime: i64) -> Result<()>;
    /// Move a node within the namespace. Transactionally re-checks that the
    /// destination slot is free.
    fn rename_node(&self, id: i64, new_parent_id: i64, new_name: &str) -> Result<()>;
    /// File-over-file rename-overwrite in ONE transaction: move `source_id`
    /// to (new_parent_id, new_name) and delete the node `target_id`, or
    /// change nothing. The target must still live exactly where the caller
    /// resolved it (same parent, same name) or the call fails without
    /// mutating anything. Returns the replaced target row AS READ INSIDE
    /// the transaction — the caller retires its object only AFTER this
    /// returns Ok, so a failed rename can never leave a live metadata
    /// pointer to a removed object, and neither file can vanish.
    fn rename_node_overwrite(
        &self,
        source_id: i64,
        target_id: i64,
        new_parent_id: i64,
        new_name: &str,
    ) -> Result<Node>;
    /// Remove a node. Caller guarantees no children (rmdir check lives in Core).
    fn delete_node(&self, id: i64) -> Result<()>;
    /// Full table scan for fsck/reconciliation.
    fn all_nodes(&self) -> Result<Vec<Node>>;

    /// Record a COMMITTED object as superseded: it is no longer the live
    /// pointer, but its bytes must stay in the object store until retention
    /// expires, so a metadata snapshot (lightfs backup) can restore
    /// point-in-time content. Idempotent.
    fn record_superseded(&self, object_key: &str, at: i64) -> Result<()>;
    /// Superseded rows recorded at or before `cutoff` whose object_key is
    /// NOT referenced by any live node (crash windows can leave a row while
    /// the node still points at the key — those must never be purged).
    fn gc_candidates(&self, cutoff: i64, limit: usize) -> Result<Vec<String>>;
    /// Remove one superseded row (after its object was purged).
    fn drop_superseded(&self, object_key: &str) -> Result<()>;
    /// All superseded keys (fsck classification).
    fn all_superseded(&self) -> Result<Vec<String>>;

    // ---- S3.1 pack prototype (branch-local schema) ----

    /// Pack-backed CAS: same single-statement optimistic gate as
    /// commit_content_cas, but the new content lives at (pack_key,
    /// [pack_off, pack_off+pack_len)) inside a shared immutable object and
    /// the per-file digest is `content_hash` (the pack's ETag is the
    /// pack's, never the file's). The row's etag is cleared: empty etag
    /// rows are structurally excluded from the read cache (R4.3 guard).
    #[allow(clippy::too_many_arguments)]
    fn commit_content_cas_pack(
        &self,
        id: i64,
        expected_version: i64,
        size: i64,
        mtime: i64,
        pack_key: &str,
        pack_off: i64,
        pack_len: i64,
        content_hash: &str,
    ) -> Result<()> {
        let _ = (
            id,
            expected_version,
            size,
            mtime,
            pack_key,
            pack_off,
            pack_len,
            content_hash,
        );
        Err(Error::Storage("packs unsupported".into()))
    }
    /// Register a pack BEFORE its bytes are uploaded (uploaded_at NULL =
    /// pending; crash before upload leaves a pending row and no object,
    /// which pack gc and fsck can both see). Idempotent per key.
    fn register_pack(&self, key: &str, bytes: i64) -> Result<()> {
        let _ = (key, bytes);
        Err(Error::Storage("packs unsupported".into()))
    }
    /// Mark a pack uploaded (immutable from now on; gc-eligible once no
    /// live node references it and the retention window has passed).
    fn mark_pack_uploaded(&self, key: &str, at: i64) -> Result<()> {
        let _ = (key, at);
        Err(Error::Storage("packs unsupported".into()))
    }
    /// Uploaded packs past the retention cutoff that NO live node
    /// references anymore, with their byte sizes (gc + fragmentation
    /// accounting). Pending (never-uploaded) packs are never returned.
    fn gc_pack_candidates(&self, cutoff: i64, limit: usize) -> Result<Vec<(String, i64)>> {
        let _ = (cutoff, limit);
        Ok(Vec::new())
    }
    /// Remove one pack row (after its object was purged).
    fn drop_pack(&self, key: &str) -> Result<()> {
        let _ = key;
        Err(Error::Storage("packs unsupported".into()))
    }
    /// All pack rows (key, bytes, uploaded_at): fsck + fragmentation trend.
    fn all_packs(&self) -> Result<Vec<(String, i64, Option<i64>)>> {
        Ok(Vec::new())
    }

    // ---- S4.2 P0-a: pack lifecycle + durable deletion tasks. Defaults
    // report the empty/unsupported outcome; only the SQLite backend
    // implements them (the only backend this branch ships).

    /// (state, unref_since) of one pack, or None when no row exists.
    /// Test/gate surface for the lifecycle state machine.
    fn pack_state(&self, key: &str) -> Result<Option<(String, Option<i64>)>> {
        let _ = key;
        Ok(None)
    }
    /// Pack keys currently in `committing` (recover's batch-finalize sweep).
    fn committing_packs(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    /// Bind claimed tasks to a batch's pack key (or clear it for per-file
    /// drains). Batches abandoned by the move finalize in the same
    /// transaction once no non-terminal task remains for them.
    fn wb_bind_pack_keys(&self, ids: &[i64], pack_key: Option<&str>, now: i64) -> Result<()> {
        let _ = (ids, pack_key, now);
        Ok(())
    }
    /// Batch termination decision: only when no non-terminal task still
    /// carries `pack_key` is the final reference count verified in one
    /// transaction — refs>0 → `active`, refs==0 → `unreferenced` from now.
    fn wb_finalize_batch_if_terminal(&self, pack_key: &str, now: i64) -> Result<()> {
        let _ = (pack_key, now);
        Ok(())
    }
    /// Record the object key a task already uploaded (orphan provenance for
    /// dead-lettered tasks).
    fn wb_set_uploaded_key(&self, id: i64, key: &str, now: i64) -> Result<()> {
        let _ = (id, key, now);
        Ok(())
    }
    /// Object keys uploaded by dead tasks and referenced by no live node.
    fn dead_task_orphans(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    /// Register deletion intent for a gc-eligible pack: transaction re-checks
    /// "no live references AND still `unreferenced`" and flips the pack to
    /// `deleting` (from which new references are refused). False = not eligible.
    fn gc_register_pack_deletion(&self, key: &str, now: i64) -> Result<bool> {
        let _ = (key, now);
        Ok(false)
    }
    /// Register deletion intent for a superseded per-file object: re-checks
    /// the superseded row exists AND no live node references the key.
    fn gc_register_superseded_deletion(&self, key: &str, now: i64) -> Result<bool> {
        let _ = (key, now);
        Ok(false)
    }
    /// Register deletion intent for an orphan object (uploaded by a dead
    /// task, referenced by nobody). Guards run in the registration transaction.
    fn gc_enqueue_orphan_object(&self, key: &str, now: i64) -> Result<bool> {
        let _ = (key, now);
        Ok(false)
    }
    /// Claim deletion work: `deleting` rows from a crashed executor re-enter
    /// the queue (the remote delete is idempotent), then pending/failed rows
    /// move to `deleting` and are returned.
    fn gc_claim_deletions(&self, limit: usize, now: i64) -> Result<Vec<GcDeletion>> {
        let _ = (limit, now);
        Ok(Vec::new())
    }
    /// Finalize a succeeded deletion: `done` plus the kind-specific local
    /// cleanup (pack row / superseded row), one transaction.
    fn gc_complete_deletion(&self, id: i64, now: i64) -> Result<()> {
        let _ = (id, now);
        Ok(())
    }
    /// Record a failed deletion attempt (state back to `failed`, retried by
    /// the next pass; the intent is never lost).
    fn gc_fail_deletion(&self, id: i64, err: &str, now: i64) -> Result<()> {
        let _ = (id, err, now);
        Ok(())
    }

    // ---- S3.1 write-back prototype: the durable task queue lives in the
    // metadata store so staging + task commit share one durability story
    // (WAL SQLite). Default implementations report unsupported; the SQLite
    // backend implements them (the only backend this branch ships).

    /// Record a staged write for `node_id`. Coalesces into the node's
    /// QUEUED task when `expected` is strictly newer than the task's
    /// (same-writer lineage); otherwise queues a new task. Returns the
    /// projected version (committed + pending tasks AFTER this call) and,
    /// when a coalesce replaced one, the (staging path, size) the row
    /// abandoned — the caller releases those `staged_bytes` ONLY after the
    /// physical delete of the old file succeeds (P0-c invariant 4).
    /// Also converts this write's reservation to staged accounting in the
    /// SAME transaction (P0-c invariant 3).
    fn wb_enqueue(
        &self,
        node_id: i64,
        staging: &str,
        size: i64,
        expected: i64,
        now: i64,
    ) -> Result<(i64, Option<(String, i64)>)> {
        let _ = (node_id, staging, size, expected, now);
        Err(Error::Storage("write-back unsupported".into()))
    }
    /// Atomically claim the oldest QUEUED task for draining.
    fn wb_claim(&self, now: i64) -> Result<Option<WbTask>> {
        let _ = now;
        Ok(None)
    }
    /// Like [`Self::wb_claim`] but only tasks whose declared size fits
    /// `max_bytes` (batch-pack assembly). None does NOT imply an empty
    /// queue: oversized tasks remain for the unconditional [`Self::wb_claim`].
    fn wb_claim_fit(&self, max_bytes: i64, now: i64) -> Result<Option<WbTask>> {
        let _ = (max_bytes, now);
        Ok(None)
    }
    /// A claimed task finished successfully: drop the row.
    fn wb_finish(&self, id: i64) -> Result<()> {
        let _ = id;
        Ok(())
    }
    /// Bump the retry counter and record the S4.4 error class; dead-letter
    /// at `max_attempts`. Returns true when the task was requeued for
    /// another attempt.
    fn wb_retry_or_dead(&self, id: i64, max_attempts: u32, now: i64, class: &str) -> Result<bool> {
        let _ = (id, max_attempts, now, class);
        Err(Error::Storage("write-back unsupported".into()))
    }
    /// Dead-letter a task with its terminal S4.4 error class (persisted on
    /// the row: `conflict` / `upload-transient` / `upload-permanent` /
    /// `staging-lost` / `unknown`).
    fn wb_mark_dead(&self, id: i64, class: &str, now: i64) -> Result<()> {
        let _ = (id, class, now);
        Err(Error::Storage("write-back unsupported".into()))
    }
    /// Test-only readback of every task's observable fields.
    #[cfg(test)]
    fn wb_task_rows(&self) -> Result<Vec<WbTaskRowDebug>> {
        Ok(Vec::new())
    }
    /// Test-only readback of pack rows: (key, state).
    #[cfg(test)]
    fn pack_rows_debug(&self) -> Result<Vec<(String, String)>> {
        Ok(Vec::new())
    }
    fn wb_set_state(&self, id: i64, state: &str, now: i64) -> Result<()> {
        let _ = (id, state, now);
        Err(Error::Storage("write-back unsupported".into()))
    }
    /// Startup recovery: draining tasks go back to queued (their staging
    /// file was renamed under them; the recorded path still points at it).
    fn wb_requeue_draining(&self) -> Result<()> {
        Ok(())
    }
    fn wb_active_tasks(&self) -> Result<Vec<WbTask>> {
        Ok(Vec::new())
    }
    fn wb_all_stagings(&self) -> Result<Vec<String>> {
        Ok(Vec::new())
    }
    /// Latest staged write for a node: (staging path, size) or None.
    fn wb_latest_staging(&self, node_id: i64) -> Result<Option<(String, i64)>> {
        let _ = node_id;
        Ok(None)
    }
    /// (latest staged size, pending task count) for the projection.
    fn wb_projection(&self, node_id: i64) -> Result<Option<(i64, i64)>> {
        let _ = node_id;
        Ok(None)
    }
    /// Cancel queued tasks for a node; returns their (staging path, size)
    /// so the caller releases staged bytes only after the physical delete
    /// succeeds (P0-c invariant 4).
    fn wb_cancel_queued(&self, node_id: i64) -> Result<Vec<(String, i64)>> {
        let _ = node_id;
        Ok(Vec::new())
    }
    /// (queued, draining, dead) counts.
    fn wb_counts(&self) -> Result<(u64, u64, u64)> {
        Ok((0, 0, 0))
    }

    // ---- P0-c staging capacity accounting (staged_bytes / reserved_bytes)
    // Both counters persist in SQLite and every change happens in the SAME
    // transaction as the task-state change it belongs to (S4 §3.D). The
    // defaults are inert no-ops for stores without the accounting table.

    /// Atomically reserve `bytes` of staging capacity for a write that is
    /// about to create its staging file. The admission predicate
    /// `staged + reserved + bytes <= budget` is evaluated INSIDE the same
    /// immediate transaction that bumps `reserved_bytes`, so concurrent
    /// writers cannot overbook (P0-c invariant 2). `budget <= 0` means
    /// unbounded (accounting only). Exhaustion is `Error::Capacity`.
    fn wb_capacity_reserve(&self, bytes: i64, budget: i64) -> Result<()> {
        let _ = (bytes, budget);
        Ok(())
    }
    /// Return an unused reservation (the staging file could not be
    /// written): reserved_bytes decreases, nothing was ever staged.
    fn wb_release_reserved(&self, bytes: i64) -> Result<()> {
        let _ = bytes;
        Ok(())
    }
    /// Release `bytes` of STAGED accounting — callers MUST have physically
    /// deleted the backing file first (P0-c invariants 4/5/6: a file that
    /// still exists keeps occupying capacity, dead tasks included).
    fn wb_release_staged(&self, bytes: i64) -> Result<()> {
        let _ = bytes;
        Ok(())
    }
    /// Startup reconciliation (P0-c invariant 8): rebuild the ledger from
    /// the REAL staging files (`(path, size)` pairs enumerated by the
    /// caller), voiding every leftover reservation. The rebuild runs in
    /// one immediate transaction at engine construction, before any
    /// worker or front-stage write can reserve — single-authority by
    /// architecture, so two processes cannot race two rebuilds.
    /// Returns the new (staged, reserved) ledger.
    fn wb_reconcile_capacity(&self, on_disk: &[(String, i64)]) -> Result<(i64, i64)> {
        let _ = on_disk;
        Ok((0, 0))
    }
    /// Current (staged_bytes, reserved_bytes) ledger — tests and metrics.
    fn wb_capacity_snapshot(&self) -> Result<(i64, i64)> {
        Ok((0, 0))
    }
}

/// One deferred-commit task row (write-back prototype).
#[derive(Debug, Clone)]
pub struct WbTask {
    pub id: i64,
    pub node_id: i64,
    pub staging: String,
    pub size: i64,
    pub expected: i64,
}

/// Test-only readback row for [`MetadataStore::wb_task_rows`].
#[cfg(test)]
pub type WbTaskRowDebug = (i64, String, i64, Option<String>);

/// One durable deletion task claimed for execution (P0-a).
#[derive(Debug, Clone)]
pub struct GcDeletion {
    pub id: i64,
    pub key: String,
    /// 'pack' | 'object' — drives the local cleanup on completion.
    pub kind: String,
}
