//! Filesystem Core: the single source of filesystem semantics.
//! Embedded mode, the server, and (through the client protocol) FUSE all
//! go through this module. Adapters never touch SQLite or S3 directly.
//!
//! Concurrency model (see the design notes, concurrency section):
//! - metadata mutations are serialized by the metadata store's transactions
//! - file content uses optimistic version checking; stale writes get CONFLICT,
//!   never a silent overwrite

use std::collections::BTreeMap;
use std::io::Read as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::config::PackSection;
use crate::error::{Error, Result};
use crate::metadata::{MetadataStore, Node, NodeKind};
use crate::object_store::ObjectStore;
use crate::pack::{PackEngine, PackReadCache};
use crate::wb::{WbOptions, WriteBackEngine};

/// Counters per the spec notes. Cheap atomics; the benchmark reads them.
#[derive(Debug, Default)]
pub struct Metrics {
    pub s3_get: AtomicU64,
    pub s3_put: AtomicU64,
    pub s3_head: AtomicU64,
    pub s3_delete: AtomicU64,
    pub s3_list: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub conflicts: AtomicU64,
    pub metadata_tx: AtomicU64,
    // ---- S3.1 prototype counters ----
    /// Immutable pack objects uploaded (one PUT each).
    pub pack_uploads: AtomicU64,
    /// Files routed into packs (per-entry, not per-PUT).
    pub pack_entries: AtomicU64,
    /// Dead bytes inside packs: CAS losers and unlinked-before-seal entries.
    pub pack_dead_bytes: AtomicU64,
    /// Pack-read-cache accounting (hits serve from RAM, zero GETs).
    pub pack_cache_hits: AtomicU64,
    pub pack_cache_misses: AtomicU64,
    pub pack_cache_evictions: AtomicU64,
    /// Pack bytes transferred from the store (amplification denominator:
    /// logical consumption is bytes_read).
    pub pack_bytes_loaded: AtomicU64,
    /// Write-back deferred-commit lifecycle.
    pub wb_enqueued: AtomicU64,
    pub wb_drained: AtomicU64,
    pub wb_conflicts: AtomicU64,
    /// S3.2 batch-pack drain: packs uploaded, and flushes that degraded
    /// to a synchronous commit because the queue was full.
    pub wb_packs: AtomicU64,
    pub wb_overflow: AtomicU64,
    /// S3.5 evidence chain: every task must be accountable through exactly
    /// one terminal outcome. flushes = enqueued = new_tasks + coalesced;
    /// new_tasks land as drained (CAS ok, incl. crash-recovery finishes),
    /// conflicts (dead), discarded (node gone mid-drain), cancelled
    /// (unlink of queued tasks), dead (transient failures past retries).
    pub wb_coalesced: AtomicU64,
    pub wb_cancelled: AtomicU64,
    pub wb_discarded: AtomicU64,
    pub wb_cas_ok: AtomicU64,
}

pub struct Core {
    meta: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    pub metrics: Arc<Metrics>,
    /// When true, superseded committed objects are RECORDED (not deleted):
    /// their bytes stay in the object store so a metadata snapshot can
    /// restore point-in-time content. `lightfs gc` purges them after the
    /// retention window. false restores the old immediate-delete behavior.
    retention_supersede: bool,
    /// S3.1 prototype engines; None = today's behavior exactly.
    pack: Option<Arc<PackEngine>>,
    pack_cache: Option<Arc<PackReadCache>>,
    wb: Option<Arc<WriteBackEngine>>,
}

/// Default retention window for superseded objects: 7 days.
pub const DEFAULT_SUPERSEDED_SECS: u64 = 7 * 24 * 3600;

/// Buffered write session. Adapters accumulate `write` calls and commit once
/// (FUSE: on flush/release; shell/server: when the full content is known).
pub struct WriteHandle {
    pub file_id: i64,
    pub path: String,
    /// Version the writer observed at open; commit fails with Conflict if the
    /// node has moved on since.
    pub expected_version: i64,
    chunks: Mutex<BTreeMap<u64, Vec<u8>>>,
}

pub fn object_key_for(file_id: i64) -> String {
    format!("objects/{file_id}/data")
}

/// Unique content key per write, rooted in the file id (AD-010: derived
/// from identity, never from the path — rename stays metadata-only).
pub(crate) fn new_version_key_for(file_id: i64) -> String {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("objects/{file_id}/w-{nanos:x}-{seq:x}.data")
}

/// Split an absolute path into normalized components.
/// Rejects relative paths, empty components, ".", ".." (traversal).
pub fn path_components(path: &str) -> Result<Vec<&str>> {
    if !path.starts_with('/') {
        return Err(Error::InvalidPath(path.to_string()));
    }
    let mut out = Vec::new();
    for comp in path.split('/') {
        match comp {
            "" | "." => {}
            ".." => return Err(Error::InvalidPath(path.to_string())),
            c => {
                if c.contains('\0') {
                    return Err(Error::InvalidPath(path.to_string()));
                }
                out.push(c);
            }
        }
    }
    Ok(out)
}

/// POSIX SYMLOOP_MAX: total symlink resolutions allowed during one path
/// walk (the kernel uses the same bound; loops fail instead of hanging).
pub const MAX_SYMLINK_RESOLUTIONS: usize = 40;

/// Join a symlink target onto the directory the link lives in, producing
/// the next candidate path. Pure string logic, unit-tested as the escape
/// contract:
/// - relative targets resolve against the link's directory
/// - absolute targets restart at the mount root (mount-relative, chroot
///   semantics - a link can never point outside this filesystem)
/// - ".." pops one component and CLAMPS at the root instead of escaping
pub fn resolve_target(link_dir: &str, target: &str) -> Result<String> {
    if target.is_empty() || target.contains('\0') {
        return Err(Error::InvalidArgument("empty symlink target".into()));
    }
    let mut out: Vec<&str> = if target.starts_with('/') {
        Vec::new()
    } else {
        path_components(link_dir)?
    };
    for comp in target.split('/') {
        match comp {
            "" | "." => {}
            ".." => {
                out.pop(); // clamp at root: pop on empty stays empty
            }
            c => out.push(c),
        }
    }
    Ok(format!("/{}", out.join("/")))
}

impl Core {
    pub fn new(meta: Arc<dyn MetadataStore>, objects: Arc<dyn ObjectStore>) -> Self {
        Self::with_retention(meta, objects, DEFAULT_SUPERSEDED_SECS)
    }

    pub fn with_retention(
        meta: Arc<dyn MetadataStore>,
        objects: Arc<dyn ObjectStore>,
        superseded_secs: u64,
    ) -> Self {
        Self {
            meta,
            objects,
            metrics: Arc::new(Metrics::default()),
            retention_supersede: superseded_secs > 0,
            pack: None,
            pack_cache: None,
            wb: None,
        }
    }

    /// S3.1 prototype constructor: wires the pack engine and/or the
    /// write-back engine. Either None keeps today's behavior byte for byte.
    pub fn with_options(
        meta: Arc<dyn MetadataStore>,
        objects: Arc<dyn ObjectStore>,
        superseded_secs: u64,
        pack: Option<PackSection>,
        wb: Option<WbOptions>,
    ) -> Result<Self> {
        let metrics = Arc::new(Metrics::default());
        let retention_supersede = superseded_secs > 0;
        let pack_engine = pack.map(|cfg| {
            let cache = if cfg.read_cache_bytes > 0 {
                Some(Arc::new(PackReadCache::new(cfg.read_cache_bytes)))
            } else {
                None
            };
            (
                Arc::new(PackEngine::new(
                    meta.clone(),
                    objects.clone(),
                    metrics.clone(),
                    cfg,
                )),
                cache,
            )
        });
        let wb_engine = match wb {
            Some(opts) => Some(Arc::new(WriteBackEngine::new(
                meta.clone(),
                objects.clone(),
                metrics.clone(),
                &opts,
                retention_supersede,
            )?)),
            None => None,
        };
        Ok(Self {
            meta,
            objects,
            metrics,
            retention_supersede,
            pack: pack_engine.as_ref().map(|(e, _)| e.clone()),
            pack_cache: pack_engine.and_then(|(_, c)| c),
            wb: wb_engine,
        })
    }

    /// The write-back engine, when the config enabled it.
    pub fn wb(&self) -> Option<&Arc<WriteBackEngine>> {
        self.wb.as_ref()
    }

    /// The pack engine, when the config enabled it.
    pub fn pack_engine(&self) -> Option<&Arc<PackEngine>> {
        self.pack.as_ref()
    }

    /// Start the write-back drain workers (engine/server startup). Workers
    /// are daemon-style: they end with the process or at `stop_wb()`. The
    /// count comes from engine startup's `resolve_wb_workers` (config +
    /// LIGHTFS_WB_WORKERS override, validated there); this takes it
    /// verbatim so there is exactly ONE wiring path, not an env side channel.
    pub fn start_wb_workers(&self, workers: usize) {
        if let Some(wb) = &self.wb {
            wb.spawn_workers(workers);
        }
    }

    /// Project a node through the write-back overlay: a node with staged
    /// pending writes reports the STAGED size and the projected version
    /// (committed + pending tasks). The client's commit chain assembles
    /// its overlay base from exactly this view, so consecutive flushes of
    /// a write-back session compose correctly before any drain lands.
    /// Errors propagate (S3.5): a metadata failure must never silently
    /// degrade the lookup to the un-projected (stale-size) node.
    fn finish_lookup(&self, node: Node) -> Result<Node> {
        if let Some(wb) = &self.wb {
            if let Some((size, version)) = wb.projected(&node)? {
                let mut node = node;
                node.size = size;
                node.version = version;
                return Ok(node);
            }
        }
        Ok(node)
    }

    /// Retire a committed object: record it for retention (point-in-time
    /// restore) or, when retention is disabled, park a durable deletion
    /// intent. Best effort — a leftover is a stale/retained object for
    /// fsck, never a correctness issue. No inline remote delete here: a
    /// committed object may be referenced by historical metadata backups,
    /// so its bytes only ever leave through the P0-b-gated deletion
    /// pipeline (the gc dependency gate), never synchronously.
    fn retire_object(&self, key: &str) {
        // A pack object is shared by construction; no per-file path may
        // retire (let alone delete) it. Its lifecycle is reference-driven:
        // pack gc reclaims it when no live node references it anymore.
        if key.starts_with(crate::pack::PACK_KEY_PREFIX) {
            return;
        }
        if self.retention_supersede && self.meta.record_superseded(key, self.now()).is_ok() {
            return;
        }
        // Retention disabled, or the supersede record failed: enroll the
        // object as a deletion intent. The caller's metadata transaction
        // already committed, so the registration's live-reference guard
        // holds; executing the intent stays behind the gc gate.
        match self.meta.gc_enqueue_orphan_object(key, self.now()) {
            Ok(true) => {}
            Ok(false) => {
                eprintln!("lightfs: retire of {key} skipped: a live reference appeared")
            }
            Err(e) => eprintln!(
                "lightfs: retire of {key} could not record a deletion intent: {e} (left for fsck)"
            ),
        }
    }

    pub fn metadata(&self) -> &Arc<dyn MetadataStore> {
        &self.meta
    }

    // ---- S4.2 P0-a / S4.3 P0-b: durable object deletion (gc). Candidates
    // are enrolled as deletion intents and executed against the object
    // store with transactional guards. The production CLI is the ONLY
    // caller, behind the P0-b gates: [backup] root configured, maintenance
    // lock held, every published manifest verified fail-closed.

    /// Enroll gc candidates as durable deletion intents: unreferenced packs
    /// past their retention window and superseded per-file objects. Keys in
    /// `protected` (the verified, unexpired backup manifests) are skipped —
    /// they stay reclaimable candidates for a later pass, after the backup
    /// expires or is pruned. Each registration re-checks its guards inside
    /// its own transaction and returns false when the predicate no longer
    /// holds.
    ///
    /// Candidate eligibility is per-source and time-based only: packs via
    /// `unref_since <= cutoff`, superseded via `superseded_at <= cutoff`.
    /// `cutoff = now` (retention off) means a candidate qualifies the moment
    /// it exists — under retention-off, retirement registers durable
    /// deletion tasks and writes NO new superseded rows, so superseded
    /// candidates can only be leftovers from a retention-on era; deploying
    /// retention-off declares there is no retention window, and those
    /// leftovers are reclaimable immediately (manifest protection still
    /// applies on top).
    ///
    /// Production callers: ONLY `lightfs gc` (cmd::gc), which holds the
    /// maintenance lock and has verified every published manifest first.
    pub(crate) fn gc_enroll_candidates(
        &self,
        cutoff: i64,
        protected: &std::collections::HashSet<String>,
    ) -> Result<(usize, usize)> {
        let mut packs = 0usize;
        let mut objects = 0usize;
        for (key, _) in self.meta.gc_pack_candidates(cutoff, 1000)? {
            if protected.contains(&key) {
                continue;
            }
            if self.meta.gc_register_pack_deletion(&key, self.now())? {
                packs += 1;
            }
        }
        for key in self.meta.gc_candidates(cutoff, 1000)? {
            if protected.contains(&key) {
                continue;
            }
            if self
                .meta
                .gc_register_superseded_deletion(&key, self.now())?
            {
                objects += 1;
            }
        }
        Ok((packs, objects))
    }

    /// Execute claimed deletion tasks: remote delete (idempotent — a key
    /// already gone, e.g. a crash after the remote delete but before the
    /// local finalize, counts as success) then the transactional finalize.
    /// Failures persist as retryable rows; the intent is never lost. Keys
    /// in `protected` are left untouched in the `deleting` stage (the next
    /// gc's claim re-queues them) — a protected intent is never completed.
    ///
    /// Production callers: ONLY `lightfs gc` (cmd::gc), behind the P0-b
    /// gate: maintenance lock held, every published manifest verified, and
    /// [backup] root configured. There is no other entry to real deletion.
    pub(crate) fn gc_execute_deletions(
        &self,
        limit: usize,
        protected: &std::collections::HashSet<String>,
    ) -> Result<(u64, u64, u64)> {
        let mut done = 0u64;
        let mut failed = 0u64;
        let mut skipped_protected = 0u64;
        for d in self.meta.gc_claim_deletions(limit, self.now())? {
            if protected.contains(&d.key) {
                skipped_protected += 1;
                continue;
            }
            // "Object confirmed absent" is deletion success at every layer
            // that can report it (S4.2.2 contract audit): the S3 adapter
            // normalizes 2xx/404-key-missing to Ok itself, NotFound and
            // ObjectMissing stay defensive here. Namespace (NoSuchBucket →
            // Storage), permission and transport errors are NOT in this
            // arm — the task retains and retries.
            let res = match self.objects.delete(&d.key) {
                Ok(()) => Ok(()),
                Err(Error::NotFound(_)) | Err(Error::ObjectMissing(_)) => Ok(()),
                Err(e) => Err(e),
            };
            match res {
                Ok(()) => {
                    self.meta.gc_complete_deletion(d.id, self.now())?;
                    done += 1;
                }
                Err(e) => {
                    self.meta
                        .gc_fail_deletion(d.id, &format!("{e}"), self.now())?;
                    failed += 1;
                }
            }
        }
        Ok((done, failed, skipped_protected))
    }

    pub fn objects(&self) -> &Arc<dyn ObjectStore> {
        &self.objects
    }

    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// Walk `path` from the root. Intermediate symlink components are
    /// RESOLVED (POSIX path resolution); the final component is returned
    /// as-is - lstat semantics. NotFound names the full path of the first
    /// missing component; more than MAX_SYMLINK_RESOLUTIONS resolutions
    /// in one walk fail with InvalidArgument (ELOOP analogue).
    pub fn lookup(&self, path: &str) -> Result<Node> {
        let mut comps: Vec<String> = path_components(path)?
            .into_iter()
            .map(String::from)
            .collect();
        let mut budget = MAX_SYMLINK_RESOLUTIONS;
        let mut cur = String::new();
        loop {
            let mut node = self
                .meta
                .get_node(1)?
                .ok_or_else(|| Error::NotFound("/".to_string()))?;
            cur.clear();
            let mut i = 0usize;
            while i < comps.len() {
                cur.push('/');
                cur.push_str(&comps[i]);
                let parent = node.id;
                node = self
                    .meta
                    .get_child(parent, &comps[i])?
                    .ok_or_else(|| Error::NotFound(cur.clone()))?;
                // Intermediate component that is a link: splice its target
                // in front of the remaining components and restart the walk
                // from the root. The loop budget bounds the WHOLE walk, so
                // chains and cycles both terminate with the same error.
                if node.is_symlink() && i + 1 < comps.len() {
                    let target = node.target.clone().unwrap_or_default();
                    let link_dir = if i == 0 {
                        "/"
                    } else {
                        // cur currently ends with "/<comps[i]>"; strip it.
                        &cur[..cur.len() - comps[i].len() - 1]
                    };
                    let next = resolve_target(link_dir, &target)?;
                    let mut new_comps: Vec<String> = path_components(&next)?
                        .into_iter()
                        .map(String::from)
                        .collect();
                    new_comps.extend_from_slice(&comps[i + 1..]);
                    comps = new_comps;
                    budget -= 1;
                    if budget == 0 {
                        return Err(Error::InvalidArgument(format!(
                            "too many symlink resolutions: {path}"
                        )));
                    }
                    break;
                }
                i += 1;
            }
            if i == comps.len() {
                return self.finish_lookup(node);
            }
        }
    }

    /// stat semantics: like lookup (lstat), but a final symlink resolves
    /// to its target, recursively. Budget-bounded like lookup.
    pub fn lookup_follow(&self, path: &str) -> Result<Node> {
        let mut cur = path.to_string();
        let mut budget = MAX_SYMLINK_RESOLUTIONS;
        loop {
            let node = self.lookup(&cur)?;
            if !node.is_symlink() {
                return Ok(node);
            }
            budget -= 1;
            if budget == 0 {
                return Err(Error::InvalidArgument(format!(
                    "too many symlink resolutions: {path}"
                )));
            }
            let target = node.target.clone().unwrap_or_default();
            let dir = match cur.rfind('/') {
                Some(0) | None => "/",
                Some(i) => &cur[..i],
            };
            cur = resolve_target(dir, &target)?;
        }
    }

    /// By-id lookup: stat a node by its STABLE id. A rename on another
    /// client is invisible here by construction (rename only moves
    /// parent/name - the id never changes); a deleted node reports
    /// IdentityGone, never the new occupant of the old path.
    pub fn lookup_by_id(&self, node_id: i64) -> Result<Node> {
        let node = self
            .meta
            .get_node(node_id)?
            .ok_or_else(|| Error::IdentityGone(node_id.to_string()))?;
        self.finish_lookup(node)
    }

    /// symlink(target, link_path): store the target VERBATIM. Fails with
    /// AlreadyExists if ANY node occupies the final name (POSIX EEXIST).
    pub fn symlink(&self, target: &str, link_path: &str) -> Result<Node> {
        if target.is_empty() || target.contains('\0') || target.len() > 4096 {
            return Err(Error::InvalidArgument(format!(
                "symlink target length {} invalid",
                target.len()
            )));
        }
        let (parent, name) = self.parent_and_name(link_path)?;
        if self.meta.get_child(parent.id, &name)?.is_some() {
            return Err(Error::AlreadyExists(link_path.to_string()));
        }
        self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
        self.meta.create_symlink(parent.id, &name, target, 0o777)
    }

    /// readlink: return the stored target of the symlink at `path`.
    /// Not-following is the whole point; a non-symlink fails with
    /// KindMismatch (EINVAL analogue).
    pub fn readlink(&self, path: &str) -> Result<String> {
        let node = self.lookup(path)?;
        match node.target {
            Some(t) => Ok(t),
            None => Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "symlink",
                actual: node.kind.as_str().to_string(),
            }),
        }
    }

    /// Node for `path`'s parent, plus the final component name.
    fn parent_and_name(&self, path: &str) -> Result<(Node, String)> {
        let comps = path_components(path)?;
        let Some(name) = comps.last() else {
            return Err(Error::InvalidArgument("root has no parent".into()));
        };
        let parent_path = format!("/{}", comps[..comps.len() - 1].join("/"));
        // POSIX resolution of the dirname follows symlinks; only the FINAL
        // component of the original path is treated no-follow by callers.
        let parent = self.lookup_follow(&parent_path)?;
        if !parent.is_dir() {
            return Err(Error::KindMismatch {
                path: parent_path,
                expected: "dir",
                actual: "file".into(),
            });
        }
        Ok((parent, name.to_string()))
    }

    pub fn getattr(&self, path: &str) -> Result<Node> {
        // stat(2) semantics: follow a final symlink.
        self.lookup_follow(path)
    }

    pub fn readdir(&self, path: &str) -> Result<Vec<Node>> {
        // opendir(3) semantics: follow a final symlink to the directory.
        let node = self.lookup_follow(path)?;
        if !node.is_dir() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "dir",
                actual: node.kind.as_str().to_string(),
            });
        }
        // Same write-back projection as lookup: the mount seeds its attr
        // cache from this listing, so a child with staged pending writes
        // must report the projected size/version here too — the committed
        // view (size 0 for a just-written file) would blind every stat
        // within the attr_ttl window.
        self.meta
            .list_children(node.id)?
            .into_iter()
            .map(|child| self.finish_lookup(child))
            .collect()
    }

    pub fn mkdir(&self, path: &str, mode: u32) -> Result<Node> {
        let (parent, name) = self.parent_and_name(path)?;
        if let Some(_existing) = self.meta.get_child(parent.id, &name)? {
            return Err(Error::AlreadyExists(path.to_string()));
        }
        self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
        self.meta
            .create_node(parent.id, &name, NodeKind::Dir, mode, None)
    }

    /// Create an empty file. The create is METADATA-ONLY: no content
    /// object is written (version 0, no object key). See the in-body
    /// comment for the full contract and why the old eager placeholder
    /// object was removed.
    pub fn create(&self, path: &str, mode: u32) -> Result<Node> {
        let (parent, name) = self.parent_and_name(path)?;
        if let Some(_existing) = self.meta.get_child(parent.id, &name)? {
            return Err(Error::AlreadyExists(path.to_string()));
        }
        // Metadata-only create: an empty file needs NO content object.
        // Size 0 reads as empty (both read paths short-circuit on size),
        // fsck treats a size-0 file as healthy without an object, and the
        // first real write commit uploads the content and CASes the
        // version - one PUT per new non-empty file instead of two (the
        // old 0-byte placeholder was pure overhead: ~0.7ms and one object
        // per create in the small-file storms). Crash between create and
        // the first flush leaves exactly an empty file - the same contract
        // observers had with the placeholder.
        let node = self
            .meta
            .create_node(parent.id, &name, NodeKind::File, mode, None)?;
        self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
        Ok(node)
    }

    /// Check a file exists and return it (FUSE open / shell read entry).
    pub fn open(&self, path: &str) -> Result<Node> {
        let node = self.lookup_follow(path)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        Ok(node)
    }

    /// Read up to `size` bytes at `offset`. Read past EOF yields empty data.
    pub fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        let node = self.lookup_follow(path)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        self.read_node_by_id(node.id, offset, size)
    }

    /// Stream a byte range of `path` without buffering the whole slice:
    /// returns a reader + the exact number of bytes it yields. Reads past
    /// EOF yield a zero-length reader.
    pub fn open_read(
        &self,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let node = self.lookup_follow(path)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        self.open_read_node(node.id, offset, size)
    }

    /// By-id read: the node is located by its STABLE id, so a rename by
    /// another client cannot detach an open handle from its bytes, and a
    /// deleted node reports IdentityGone instead of whatever now lives on
    /// the recycled path.
    pub fn open_read_by_id(
        &self,
        node_id: i64,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        self.open_read_node(node_id, offset, size)
    }

    /// Byte-range reader for a node located by STABLE id. Content state is
    /// resolved here (see read_node_by_id), never from a caller-held
    /// snapshot.
    fn open_read_node(
        &self,
        node_id: i64,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        // Write-back overlay and pack-backed content both materialize a
        // bounded slice (pack files are small by definition, staged writes
        // are bounded by the staging budget); per-file objects keep the
        // zero-copy stream.
        if self.wb.is_some() {
            let data = self.read_node_by_id(node_id, offset, size)?;
            let n = data.len() as u64;
            return Ok((Box::new(std::io::Cursor::new(data)), n));
        }
        let node = self
            .meta
            .get_node(node_id)?
            .ok_or_else(|| Error::IdentityGone(node_id.to_string()))?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: node_id.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        if offset >= node.size as u64 || size == 0 {
            return Ok((Box::new(std::io::empty()), 0));
        }
        let len = size.min(node.size as u64 - offset);
        if node.is_pack_backed() {
            let data = self.read_committed_bytes(&node, offset, len)?;
            let n = data.len() as u64;
            return Ok((Box::new(std::io::Cursor::new(data)), n));
        }
        let key = node
            .object_key
            .clone()
            .unwrap_or_else(|| object_key_for(node.id));
        let data = self.objects.get_stream(
            &key,
            Some(crate::object_store::Range { start: offset, len }),
        )?;
        self.metrics.s3_get.fetch_add(1, Ordering::Relaxed);
        Ok(data)
    }

    /// One read for a node, resolved AUTHORITATIVELY at read time (S3.5).
    /// `node` is only a hint: the async drain can commit, finish the task
    /// and unlink the staging file at ANY moment after a snapshot was
    /// taken, so neither the snapshot's object_key nor its pending state
    /// may be spliced into the read. Resolution order per attempt:
    /// 1. staged bytes (read_pending is open-before-validate and
    ///    re-resolves internally when the drain lands in its window);
    /// 2. the committed object of a FRESHLY fetched node, validated
    ///    afterwards against the committed view - a transition during the
    ///    read (enqueue, coalesce, commit) retries, so a retired key can
    ///    never serve a previous generation.
    ///
    /// The byte range is clamped against whichever view is authoritative
    /// for the branch taken.
    fn read_node_by_id(&self, node_id: i64, offset: u64, size: u64) -> Result<Vec<u8>> {
        let wb_mode = self.wb.is_some();
        let attempts = if wb_mode { 4 } else { 1 };
        let mut best: Option<Vec<u8>> = None;
        let mut pending_raced = false;
        for _ in 0..attempts {
            // Staged bytes first, by node id: read_pending clamps against
            // the TASK's staged size and re-resolves internally when the
            // drain lands inside its query/open window. Retry means the
            // probe kept losing a coalesce race - the whole snapshot must
            // be re-resolved; treating that as "no pending" would serve a
            // previous generation or a spurious EOF.
            if let Some(wb) = &self.wb {
                match wb.read_pending(node_id, offset, size)? {
                    crate::wb::PendingRead::Bytes(staged) => {
                        self.metrics
                            .bytes_read
                            .fetch_add(staged.len() as u64, Ordering::Relaxed);
                        return Ok(staged);
                    }
                    crate::wb::PendingRead::None => {}
                    crate::wb::PendingRead::Retry => {
                        pending_raced = true;
                        continue;
                    }
                }
            }
            // Committed branch: fetch the node AT THIS INSTANT - it may
            // have been committed/finished since the pending check above.
            let raw = self
                .meta
                .get_node(node_id)?
                .ok_or_else(|| Error::IdentityGone(node_id.to_string()))?;
            if !raw.is_file() {
                return Err(Error::KindMismatch {
                    path: node_id.to_string(),
                    expected: "file",
                    actual: "dir".into(),
                });
            }
            if offset >= raw.size as u64 || size == 0 {
                return Ok(Vec::new());
            }
            let len = size.min(raw.size as u64 - offset);
            match self.read_committed_bytes(&raw, offset, len) {
                Ok(bytes) => {
                    if !wb_mode {
                        return Ok(bytes);
                    }
                    // Validate against the COMMITTED views actually read,
                    // never the projected ones: the projection folds
                    // pending count into the version, so a commit landing
                    // mid-read (committed +1, pending -1) keeps the
                    // projected version AND size identical and would bless
                    // the previous generation's bytes.
                    let unchanged = match self.meta.get_node(node_id)? {
                        Some(now) => now.version == raw.version && now.size == raw.size,
                        None => false,
                    };
                    if unchanged {
                        return Ok(bytes);
                    }
                    best = Some(bytes); // raced: retry, but keep as a valid instant
                    pending_raced = false; // the committed branch made progress
                }
                Err(Error::NotFound(_)) if wb_mode => {
                    // the node's previous object was retired mid-read; the
                    // fresh fetch at the top of the next attempt sees the
                    // new generation
                }
                Err(e) => return Err(e),
            }
        }
        if pending_raced {
            // The last failure was a pending-layer race: any `best` here is
            // several generations behind the staged truth. An explicit error
            // beats silently serving stale bytes.
            return Err(Error::Storage(format!(
                "write-back read of node {node_id} kept racing pending updates; retries exhausted"
            )));
        }
        // Every attempt raced a transition; `best` was still read from a
        // valid source at one instant, which is a legal linearized read.
        best.ok_or_else(|| Error::Storage(format!("wb read raced node {node_id}")))
    }

    /// Committed-content read for a RAW node: a pack range (optionally
    /// through the pack read cache) or the plain per-file object. No
    /// write-back awareness - callers resolve staged bytes first.
    fn read_committed_bytes(&self, node: &Node, offset: u64, len: u64) -> Result<Vec<u8>> {
        let key = node
            .object_key
            .clone()
            .unwrap_or_else(|| object_key_for(node.id));
        if node.is_pack_backed() {
            let pack_off = node.pack_off.unwrap_or(0) as u64;
            let start = pack_off + offset;
            if let Some(cache) = &self.pack_cache {
                let (pack, hit) = cache.get_or_load(&key, || {
                    self.metrics.s3_get.fetch_add(1, Ordering::Relaxed);
                    Ok(self.objects.get(&key, None)?.data)
                })?;
                // Mirror the cache's own counters into the core metrics the
                // RPC surfaces (they only ever grow; store is race-loose).
                self.metrics
                    .pack_cache_hits
                    .store(cache.hits.load(Ordering::Relaxed), Ordering::Relaxed);
                self.metrics
                    .pack_cache_misses
                    .store(cache.misses.load(Ordering::Relaxed), Ordering::Relaxed);
                self.metrics
                    .pack_cache_evictions
                    .store(cache.evictions.load(Ordering::Relaxed), Ordering::Relaxed);
                self.metrics.pack_bytes_loaded.store(
                    cache.loaded_bytes.load(Ordering::Relaxed),
                    Ordering::Relaxed,
                );
                let _ = hit;
                let end = (start + len).min(pack.len() as u64);
                let out = pack[start as usize..end as usize].to_vec();
                self.metrics
                    .bytes_read
                    .fetch_add(out.len() as u64, Ordering::Relaxed);
                return Ok(out);
            }
            let data = self
                .objects
                .get(&key, Some(crate::object_store::Range { start, len }))?;
            self.metrics.s3_get.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .bytes_read
                .fetch_add(data.data.len() as u64, Ordering::Relaxed);
            return Ok(data.data);
        }
        let data = self.objects.get(
            &key,
            Some(crate::object_store::Range { start: offset, len }),
        )?;
        self.metrics.s3_get.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .bytes_read
            .fetch_add(data.data.len() as u64, Ordering::Relaxed);
        Ok(data.data)
    }

    /// Single-pass streaming write: bytes flow from `reader` (exactly `len`)
    /// into a fresh version object, then the same CAS commit as
    /// commit_write. No whole-file buffering on this side; callers pay for
    /// it only if they buffered anyway. Optimistic version semantics are
    /// identical.
    pub fn write_stream(
        &self,
        path: &str,
        expected_version: Option<i64>,
        create_if_missing: bool,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<Node> {
        if create_if_missing {
            match self.create(path, 0o644) {
                Ok(_) => {}
                Err(Error::AlreadyExists(_)) => {}
                Err(e) => return Err(e),
            }
        }
        let node = self.lookup_follow(path)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        let expected = expected_version.unwrap_or(node.version);
        self.write_stream_node(
            &node,
            expected,
            reader,
            len,
            create_if_missing && node.version == 1,
        )
    }

    /// By-id streaming write: the node is located by its STABLE id (never
    /// re-resolved through the path), so a rename by another client cannot
    /// detach an open handle from its own commit, and a deleted node fails
    /// with IdentityGone instead of writing onto whatever took its place.
    /// `create_if_missing` is meaningless for an id anchor: the caller MUST
    /// pass false (the protocol surface enforces this).
    pub fn write_stream_by_id(
        &self,
        node_id: i64,
        expected_version: i64,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<Node> {
        let node = self
            .meta
            .get_node(node_id)?
            .ok_or_else(|| Error::IdentityGone(node_id.to_string()))?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: node_id.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        self.write_stream_node(&node, expected_version, reader, len, false)
    }

    /// CAS commit shared by both anchors. `drop_created_node` mirrors the
    /// path-anchored create-if-missing cleanup (no phantom empty file).
    fn write_stream_node(
        &self,
        node: &Node,
        expected: i64,
        reader: &mut dyn std::io::Read,
        len: u64,
        drop_created_node: bool,
    ) -> Result<Node> {
        // Write-back prototype: stage durably, queue, ack. Remote commit
        // happens on the drain with the same PUT + CAS chain as below.
        if let Some(wb) = &self.wb {
            let r = wb.write_deferred(node, expected, reader, len);
            if r.is_err() && drop_created_node {
                let _ = self.meta.delete_node(node.id);
            }
            return r;
        }
        // Pack prototype: small files aggregate into immutable packs.
        if let Some(pack) = &self.pack {
            if len <= pack.config().large_file_bytes {
                let mut content = Vec::with_capacity(len as usize);
                if let Err(e) = reader.take(len).read_to_end(&mut content) {
                    if drop_created_node {
                        let _ = self.meta.delete_node(node.id);
                    }
                    return Err(e.into());
                }
                if content.len() as u64 != len {
                    let e = Error::InvalidArgument(format!(
                        "declared length {len} != body length {}",
                        content.len()
                    ));
                    if drop_created_node {
                        let _ = self.meta.delete_node(node.id);
                    }
                    return Err(e);
                }
                return self.commit_via_pack(node, expected, content, drop_created_node);
            }
        }
        let version_key = self.new_version_key(node.id);
        let info = match self.objects.put_stream(&version_key, reader, len) {
            Ok(info) => info,
            Err(e) => {
                // If we created the node for this write, don't leave a
                // phantom empty file behind.
                if drop_created_node {
                    let _ = self.meta.delete_node(node.id);
                }
                let _ = self.objects.delete(&version_key);
                return Err(e);
            }
        };
        self.metrics.s3_put.fetch_add(1, Ordering::Relaxed);
        self.metrics.bytes_written.fetch_add(len, Ordering::Relaxed);
        match self.meta.commit_content_cas(
            node.id,
            expected,
            len as i64,
            self.now(),
            &version_key,
            &info.etag,
        ) {
            Ok(()) => {
                self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
                if let Some(old_key) = &node.object_key {
                    self.retire_object(old_key);
                }
                let mut node = node.clone();
                node.size = len as i64;
                node.version += 1;
                node.etag = info.etag;
                node.object_key = Some(version_key);
                Ok(node)
            }
            Err(e @ Error::Conflict { .. }) => {
                self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
                let _ = self.objects.delete(&version_key);
                Err(e)
            }
            Err(e) => {
                let _ = self.objects.delete(&version_key);
                Err(e)
            }
        }
    }

    /// Pack-routed commit for in-memory content (shared by the streaming
    /// and buffered-session paths). The pack engine blocks until THIS
    /// file's CAS resolved — strict per-file semantics, aggregated layout.
    fn commit_via_pack(
        &self,
        node: &Node,
        expected: i64,
        content: Vec<u8>,
        drop_created_node: bool,
    ) -> Result<Node> {
        let pack = self.pack.as_ref().unwrap();
        self.metrics
            .bytes_written
            .fetch_add(content.len() as u64, Ordering::Relaxed);
        match pack.commit(node, expected, content) {
            Ok(n) => {
                self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
                // retire of the previous object respects the pack guard
                // (pack keys are never retired from a per-file path).
                if let Some(old_key) = &node.object_key {
                    self.retire_object(old_key);
                }
                Ok(n)
            }
            Err(e) => {
                if drop_created_node {
                    let _ = self.meta.delete_node(node.id);
                }
                Err(e)
            }
        }
    }

    /// Start a buffered write session on `path`. `expected_version = None`
    /// means "whatever the node holds now" (single-writer convenience).
    pub fn open_write(&self, path: &str, expected_version: Option<i64>) -> Result<WriteHandle> {
        let node = self.lookup_follow(path)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        let expected = expected_version.unwrap_or(node.version);
        Ok(WriteHandle {
            file_id: node.id,
            path: path.to_string(),
            expected_version: expected,
            chunks: Mutex::new(BTreeMap::new()),
        })
    }

    /// Start a buffered write session anchored on the STABLE node id: the
    /// handle is located by id (never re-resolving the path), and the
    /// existing by-id CAS in commit_write gates versions.
    pub fn open_write_id(
        &self,
        node_id: i64,
        path_hint: &str,
        expected_version: Option<i64>,
    ) -> Result<WriteHandle> {
        let node = self.lookup_by_id(node_id)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path_hint.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        let expected = expected_version.unwrap_or(node.version);
        Ok(WriteHandle {
            file_id: node.id,
            path: path_hint.to_string(),
            expected_version: expected,
            chunks: Mutex::new(BTreeMap::new()),
        })
    }

    /// Buffer bytes at `offset` for a later commit_write.
    pub fn write(&self, handle: &WriteHandle, offset: u64, data: &[u8]) -> Result<usize> {
        if data.is_empty() {
            return Ok(0);
        }
        handle.chunks.lock().unwrap().insert(offset, data.to_vec());
        self.metrics
            .bytes_written
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(data.len())
    }

    /// Assemble buffered chunks and publish them with optimistic concurrency:
    ///
    /// 1. upload to a per-write version key `objects/<id>/w-<uniq>.data`
    ///    (immutable once written — a losing writer can never overwrite the
    ///    winner's bytes - the design notes, concurrency section)
    /// 2. flip the node's object pointer with a single-statement CAS
    ///    (`WHERE version = expected`); only the CAS winner becomes current
    /// 3. on conflict, delete our version key and report CONFLICT
    /// 4. on success, best-effort delete the superseded object; until that
    ///    runs (or after a crash) the old key is a stale version that fsck
    ///    reports (the spec notes, fsck section)
    pub fn commit_write(&self, handle: &WriteHandle) -> Result<Node> {
        // Fast-fail before any network I/O; the CAS below is the real gate.
        // The lookup passes through the write-back projection so a session
        // rebased onto a projected version does not spuriously conflict
        // while its earlier flush is still queued.
        let node = self
            .meta
            .get_node(handle.file_id)?
            .ok_or_else(|| Error::NotFound(handle.path.clone()))?;
        let node = self.finish_lookup(node)?;
        if node.version != handle.expected_version {
            self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Conflict {
                path: handle.path.clone(),
                expected: handle.expected_version,
                actual: node.version,
            });
        }
        let chunks = handle.chunks.lock().unwrap();
        let mut content: Vec<u8> = Vec::new();
        for (&off, data) in chunks.iter() {
            if off > content.len() as u64 {
                content.resize(off as usize, 0);
            }
            let end = (off as usize + data.len()).max(content.len());
            content.resize(end, 0);
            content[off as usize..off as usize + data.len()].copy_from_slice(data);
        }
        drop(chunks);

        // Write-back prototype (buffered-session path): defer the whole
        // assembled content; the drain replays it through the same PUT +
        // CAS chain as the streaming path below.
        if let Some(wb) = &self.wb {
            let len = content.len() as u64;
            let mut cursor = std::io::Cursor::new(content);
            return wb.write_deferred(&node, handle.expected_version, &mut cursor, len);
        }
        // Pack prototype (buffered-session path): small content aggregates
        // into the shared immutable pack buffer.
        if let Some(pack) = &self.pack {
            if content.len() as u64 <= pack.config().large_file_bytes {
                return self.commit_via_pack(&node, handle.expected_version, content, false);
            }
        }

        let version_key = self.new_version_key(node.id);
        let info = self.objects.put(&version_key, &content)?;
        self.metrics.s3_put.fetch_add(1, Ordering::Relaxed);

        let cas = self.meta.commit_content_cas(
            node.id,
            handle.expected_version,
            content.len() as i64,
            self.now(),
            &version_key,
            &info.etag,
        );
        match cas {
            Ok(()) => {
                self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
                // Superseded object is no longer the live pointer: retain
                // (record) or delete it per policy (best effort — a leftover
                // is a stale/retained object for fsck, never a correctness
                // issue).
                if let Some(old_key) = &node.object_key {
                    self.retire_object(old_key);
                }
                let mut node = node;
                node.size = content.len() as i64;
                node.version += 1;
                node.etag = info.etag;
                node.object_key = Some(version_key);
                Ok(node)
            }
            Err(e @ Error::Conflict { .. }) => {
                self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
                // Losing writer cleans up after itself; the winner's object
                // was never touched (different key).
                if self.objects.delete(&version_key).is_ok() {
                    self.metrics.s3_delete.fetch_add(1, Ordering::Relaxed);
                }
                Err(e)
            }
            Err(e) => {
                // CAS could not run (node vanished, storage/db error): drop
                // our orphan-in-waiting.
                let _ = self.objects.delete(&version_key);
                Err(e)
            }
        }
    }

    /// Unique content key per write, still rooted in the file id (AD-010:
    /// derived from identity, never from the path — rename stays
    /// metadata-only).
    fn new_version_key(&self, file_id: i64) -> String {
        new_version_key_for(file_id)
    }

    /// rename/move: metadata-only. Object bytes never move (AD-010).
    /// A file may overwrite an existing file; anything else must not exist.
    pub fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        let src = self.lookup(old_path)?;
        let (dest_parent, dest_name) = self.parent_and_name(new_path)?;
        let dest = self.meta.get_child(dest_parent.id, &dest_name)?;

        if let Some(dest) = &dest {
            // POSIX: renaming a node onto itself succeeds and changes
            // nothing — no delete, no move, no retire. Equivalent path
            // forms (`//f`, `/./f`) resolve to this same check.
            if dest.id == src.id {
                return Ok(());
            }
            // Overwriting requires same-kind file->file; a directory
            // target must not exist (POSIX allows empty-dir overwrite,
            // which v1 does not need).
            if src.is_dir() || dest.is_dir() {
                return Err(Error::AlreadyExists(new_path.to_string()));
            }
        }

        // Moving a directory below itself would orphan the subtree. The
        // destination parent itself is the first candidate ancestor. This
        // runs BEFORE any mutation: a refused rename must leave both nodes
        // and both objects exactly as they were.
        if src.is_dir() {
            let mut anc = Some(dest_parent.id);
            while let Some(a) = anc {
                if a == src.id {
                    return Err(Error::Cycle(format!("{old_path} -> {new_path}")));
                }
                anc = self.meta.get_node(a)?.and_then(|n| n.parent_id);
            }
        }

        self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
        match dest {
            Some(dest) => {
                // File-over-file overwrite in ONE metadata transaction:
                // move the source and remove the target, or leave both
                // standing. The old target object is retired only AFTER
                // the transaction committed, using the key the target had
                // at replacement time — a failed rename can therefore
                // never retire (let alone delete) an object a live row
                // still points at, and retire itself stays best-effort:
                // its failure leaves a leftover, not a broken pointer.
                let replaced =
                    self.meta
                        .rename_node_overwrite(src.id, dest.id, dest_parent.id, &dest_name)?;
                if let Some(key) = replaced.object_key {
                    self.retire_object(&key);
                }
                Ok(())
            }
            None => self.meta.rename_node(src.id, dest_parent.id, &dest_name),
        }
    }

    /// Remove a file. The metadata transaction is the atomic truth: the
    /// node row goes (and a pack-backed file's reference-gone event fires
    /// inside the same transaction). Only AFTER it commits is the old
    /// object's reclamation booked — a superseded record under retention,
    /// a durable deletion intent otherwise — and both are best effort:
    /// a failure leaks an unreferenced object for fsck, it can never
    /// dangle a live row against deleted bytes, and no bytes are ever
    /// deleted inline (the gc dependency gate owns real deletion).
    pub fn unlink(&self, path: &str) -> Result<()> {
        let node = self.lookup(path)?;
        // POSIX: unlink removes files AND symlinks (never following the
        // link); only directories are refused (that is rmdir's job).
        if !node.is_file() && !node.is_symlink() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: node.kind.as_str().to_string(),
            });
        }
        // Write-back: queued staged writes for this node never reach the
        // store (the delete-before-commit cancel). A draining task rides
        // to its CAS, which fails NotFound and is discarded.
        if let Some(wb) = &self.wb {
            wb.cancel(node.id);
        }
        self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
        self.meta.delete_node(node.id)?;
        if let Some(key) = &node.object_key {
            if !node.is_pack_backed() {
                if self.retention_supersede {
                    if let Err(e) = self.meta.record_superseded(key, self.now()) {
                        eprintln!("lightfs: unlinked object {key} left unrecorded: {e} (fsck)");
                    }
                } else {
                    match self.meta.gc_enqueue_orphan_object(key, self.now()) {
                        Ok(true) => {}
                        Ok(false) => eprintln!(
                            "lightfs: unlinked object {key} not enrolled: a live reference exists"
                        ),
                        Err(e) => eprintln!(
                            "lightfs: unlinked object {key} could not record a deletion intent: {e} (fsck)"
                        ),
                    }
                }
            }
        }
        Ok(())
    }

    pub fn rmdir(&self, path: &str) -> Result<()> {
        let node = self.lookup(path)?;
        // A symlink is NOT a directory and rmdir never follows it (POSIX
        // rmdir on a link fails ENOTDIR, it does not remove the target).
        if !node.is_dir() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "dir",
                actual: node.kind.as_str().to_string(),
            });
        }
        if node.id == 1 {
            return Err(Error::InvalidArgument("cannot remove root".into()));
        }
        if self.meta.count_children(node.id)? > 0 {
            return Err(Error::DirectoryNotEmpty(path.to_string()));
        }
        self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
        self.meta.delete_node(node.id)
    }

    /// Adjust mode/mtime (used by FUSE setattr).
    pub fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
        let node = self.lookup(path)?;
        let mode = mode.unwrap_or(node.mode);
        let mtime = mtime.unwrap_or(node.mtime);
        self.meta.update_attrs(node.id, mode, mtime)?;
        let mut node = node;
        node.mode = mode;
        node.mtime = mtime;
        Ok(node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::object_store::mem::MemObjectStore;

    fn core() -> Core {
        Core::new(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(MemObjectStore::new()),
        )
    }

    // ---- S4.2 P0-a: durable deletion tasks over a fake ObjectStore.

    /// Red-test 10: a remote delete failure persists as a retryable
    /// deletion task (intent never lost), and the next pass converges.
    #[test]
    fn p0a_deletion_failure_persists_then_retry_succeeds() {
        let mem = Arc::new(MemObjectStore::new());
        let c = Core::new(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            mem.clone(),
        );
        let p = "packs/p0a-fail.data";
        c.metadata().register_pack(p, 100).unwrap();
        c.metadata().mark_pack_uploaded(p, 1).unwrap();
        c.metadata().wb_finalize_batch_if_terminal(p, 1).unwrap();
        // Past the retention window: the pack enrolls as a deletion intent.
        let (packs, objects) = c
            .gc_enroll_candidates(i64::MAX, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((packs, objects), (1, 0));
        // First execution fails on the remote: the task survives as failed
        // and the pack row stays `deleting` (no refs can be created).
        mem.inject_delete_failure(p);
        let (done, failed, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((done, failed), (0, 1));
        assert!(c.metadata().pack_state(p).unwrap().is_some());
        assert!(
            c.metadata()
                .gc_pack_candidates(i64::MAX, 10)
                .unwrap()
                .is_empty()
        );
        // Second execution succeeds: pack row finalized away.
        let (done, failed, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((done, failed), (1, 0));
        assert!(c.metadata().pack_state(p).unwrap().is_none());
    }

    /// Red-test 11, rewritten in S4.2.2 as a REAL deterministic timeline
    /// (the old version never created a store object and never deleted one,
    /// so "remote delete succeeded, local finalize crashed" was fiction):
    ///
    /// 1-2. pack metadata + a REAL object; pack advanced to unreferenced;
    ///      deletion intent enrolled — and the metadata DB is PERSISTED and
    ///      dropped, proving the intent is durable.
    /// 3.   claim: the task and the pack sit in the deleting stage.
    /// 4.   the object is ACTUALLY deleted from the store and verified
    ///      absent; `gc_complete_deletion` is deliberately skipped.
    /// 5-6. that is the crash window; recovery reopens the persisted DB.
    /// 7.   the re-queued task runs against an absent object and still
    ///      finalizes (idempotent remote delete).
    /// 8.   task terminal, pack row cleaned, repeat pass is a no-op.
    #[test]
    fn p0a22_remote_deleted_before_finalize_converges_on_recover() {
        let dir = std::env::temp_dir().join(format!("lightfs-p0a22-crash-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("m.db");
        let mem = Arc::new(MemObjectStore::new());
        let p = "packs/p0a22-crash.data";
        // 1-2 (and drop the Core: the intent must live in the DB, not in
        // any process state).
        {
            let c = Core::with_retention(
                Arc::new(SqliteMetadataStore::open(db.to_str().unwrap()).unwrap()),
                mem.clone(),
                1,
            );
            c.metadata().register_pack(p, 100).unwrap();
            c.metadata().mark_pack_uploaded(p, 1).unwrap();
            c.metadata().wb_finalize_batch_if_terminal(p, 1).unwrap();
            mem.put(p, b"real-pack-bytes").unwrap();
            assert!(mem.head(p).is_ok(), "the pack object really exists");
            let (packs, objects) = c
                .gc_enroll_candidates(i64::MAX, &std::collections::HashSet::new())
                .unwrap();
            assert_eq!((packs, objects), (1, 0));
        }
        // 3. claim — the durable task and the pack are in `deleting`.
        {
            let c = Core::with_retention(
                Arc::new(SqliteMetadataStore::open(db.to_str().unwrap()).unwrap()),
                mem.clone(),
                1,
            );
            let claimed = c.metadata().gc_claim_deletions(10, 1).unwrap();
            assert_eq!(claimed.len(), 1);
            assert_eq!(
                (claimed[0].key.as_str(), claimed[0].kind.as_str()),
                (p, "pack")
            );
            assert!(
                c.metadata().pack_state(p).unwrap().is_some(),
                "pack row retained while deleting"
            );
            // A claim with no live executor around re-queues the stale
            // `deleting` row by design (crash residue): same task again,
            // never lost.
            let reclaimed = c.metadata().gc_claim_deletions(10, 1).unwrap();
            assert_eq!(reclaimed.len(), 1);
            assert_eq!(reclaimed[0].id, claimed[0].id, "the task is retained");
            // 4. the remote delete REALLY happens; no local finalize.
            mem.delete(p).unwrap();
            assert!(mem.head(p).is_err(), "object verified absent");
        }
        // 5-6. crash window over; recovery reopens the persisted metadata.
        let c = Core::with_retention(
            Arc::new(SqliteMetadataStore::open(db.to_str().unwrap()).unwrap()),
            mem.clone(),
            1,
        );
        // 7. the stale deleting claim re-enters; the absent object does not
        // block the local finalize.
        let (done, failed, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((done, failed), (1, 0), "missing key counts as deleted");
        // 8. terminal states, and a repeat pass is an idempotent no-op.
        assert!(c.metadata().pack_state(p).unwrap().is_none());
        assert!(c.metadata().gc_claim_deletions(10, 1).unwrap().is_empty());
        let (done, failed, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((done, failed), (0, 0));
        assert!(mem.head(p).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An object whose key is ALREADY absent when its deletion task first
    /// executes converges on that first pass (the idempotent-success
    /// contract), and the superseded ledger is finalized away.
    #[test]
    fn p0a22_absent_object_finalizes_on_first_execution() {
        let c = core();
        c.create("/f", 0o644).unwrap();
        let v1 = c
            .write_stream(
                "/f",
                Some(0),
                false,
                &mut std::io::Cursor::new(b"one".to_vec()),
                3,
            )
            .unwrap();
        let old_key = v1.object_key.clone().unwrap();
        c.write_stream(
            "/f",
            Some(1),
            false,
            &mut std::io::Cursor::new(b"two!".to_vec()),
            4,
        )
        .unwrap();
        let (packs, objects) = c
            .gc_enroll_candidates(i64::MAX, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((packs, objects), (0, 1));
        // The object vanishes out-of-band before the executor runs.
        c.objects().delete(&old_key).unwrap();
        assert!(c.objects().head(&old_key).is_err());
        let (done, failed, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((done, failed), (1, 0));
        assert!(c.metadata().all_superseded().unwrap().is_empty());
        assert!(c.metadata().gc_claim_deletions(10, 1).unwrap().is_empty());
    }

    /// A store that REPORTS "object does not exist" from its own delete()
    /// (Error::ObjectMissing — unlike MemObjectStore's S3-normalized Ok)
    /// must hit the executor's absent-object branch: the task completes,
    /// the pack metadata finalizes away, no retry residue. Baseline test:
    /// the branch is the S4.2.2 defensive arm and this is its first direct
    /// end-to-end coverage, so there is no old-behavior red phase.
    #[test]
    fn p0a22_object_missing_from_delete_completes_the_task() {
        let mem = Arc::new(MemObjectStore::new());
        let c = Core::new(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            mem.clone(),
        );
        let p = "packs/p0a22-om.data";
        c.metadata().register_pack(p, 100).unwrap();
        c.metadata().mark_pack_uploaded(p, 1).unwrap();
        c.metadata().wb_finalize_batch_if_terminal(p, 1).unwrap();
        let (packs, objects) = c
            .gc_enroll_candidates(i64::MAX, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((packs, objects), (1, 0));
        // The store reports ObjectMissing for this key's delete.
        mem.inject_object_missing_delete(p);

        let (done, failed, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!(
            (done, failed),
            (1, 0),
            "ObjectMissing = already gone, not a failure"
        );
        assert!(
            c.metadata().pack_state(p).unwrap().is_none(),
            "the pack row is finalized away"
        );
        assert!(
            c.metadata().gc_claim_deletions(10, 1).unwrap().is_empty(),
            "no retry residue"
        );
        let (done, failed, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((done, failed), (0, 0), "idempotent repeat");
    }

    /// Superseded per-file objects ride the same durable deletion task flow.
    #[test]
    fn p0a_superseded_object_deletion_task_flow() {
        let c = core();
        c.create("/f", 0o644).unwrap();
        let v1 = c
            .write_stream(
                "/f",
                Some(0),
                false,
                &mut std::io::Cursor::new(b"one".to_vec()),
                3,
            )
            .unwrap();
        let old_key = v1.object_key.clone().unwrap();
        c.write_stream(
            "/f",
            Some(1),
            false,
            &mut std::io::Cursor::new(b"two!".to_vec()),
            4,
        )
        .unwrap();
        let (packs, objects) = c
            .gc_enroll_candidates(i64::MAX, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!((packs, objects), (0, 1), "the superseded v1 key enrolls");
        let (done, _, _) = c
            .gc_execute_deletions(10, &std::collections::HashSet::new())
            .unwrap();
        assert_eq!(done, 1);
        assert!(c.objects().get(&old_key, None).is_err(), "v1 bytes gone");
        assert!(c.metadata().all_superseded().unwrap().is_empty());
    }

    #[test]
    fn path_parsing() {
        assert_eq!(path_components("/").unwrap(), Vec::<&str>::new());
        assert_eq!(path_components("/a/b/c").unwrap(), vec!["a", "b", "c"]);
        assert_eq!(path_components("/a//b/.").unwrap(), vec!["a", "b"]);
        assert!(path_components("rel").is_err());
        assert!(path_components("/a/../b").is_err());
        assert!(path_components("/a/./../c").is_err());
    }

    #[test]
    fn root_and_lookup() {
        let c = core();
        let root = c.lookup("/").unwrap();
        assert_eq!(root.id, 1);
        assert!(root.is_dir());
        c.mkdir("/a", 0o755).unwrap();
        c.mkdir("/a/b", 0o755).unwrap();
        c.create("/a/b/f.txt", 0o644).unwrap();
        let f = c.lookup("/a/b/f.txt").unwrap();
        assert!(f.is_file());
        // Metadata-only create: a brand-new empty file sits at version 0
        // with NO object key - the first write commit installs it.
        assert_eq!(f.version, 0);
        assert!(f.object_key.is_none(), "v0 file must have no object key");
        assert!(matches!(c.lookup("/a/missing"), Err(Error::NotFound(p)) if p == "/a/missing"));
        assert!(matches!(
            c.lookup("/a/b/f.txt/deeper"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn create_mkdir_errors() {
        let c = core();
        c.mkdir("/d", 0o755).unwrap();
        assert!(matches!(c.mkdir("/d", 0o755), Err(Error::AlreadyExists(_))));
        c.create("/d/f", 0o644).unwrap();
        assert!(matches!(
            c.create("/d/f", 0o644),
            Err(Error::AlreadyExists(_))
        ));
        // parent is a file
        assert!(matches!(
            c.create("/d/f/child", 0o644),
            Err(Error::KindMismatch { .. })
        ));
        assert!(matches!(
            c.mkdir("/missing/child", 0o755),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn write_read_range_and_versions() {
        let c = core();
        c.create("/f", 0o644).unwrap();

        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"hello world").unwrap();
        let node = c.commit_write(&h).unwrap();
        assert_eq!(node.size, 11);
        assert_eq!(node.version, 1);

        assert_eq!(c.read("/f", 0, 100).unwrap(), b"hello world");
        assert_eq!(c.read("/f", 6, 5).unwrap(), b"world");
        assert_eq!(c.read("/f", 100, 10).unwrap(), Vec::<u8>::new());

        // stale writer conflicts
        let h1 = c.open_write("/f", Some(1)).unwrap();
        let h2 = c.open_write("/f", None).unwrap();
        c.write(&h2, 0, b"second").unwrap();
        c.commit_write(&h2).unwrap(); // version -> 2
        c.write(&h1, 0, b"stale").unwrap();
        let err = c.commit_write(&h1).unwrap_err();
        assert!(matches!(
            err,
            Error::Conflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));
        assert_eq!(c.read("/f", 0, 100).unwrap(), b"second"); // no silent overwrite

        // sparse chunks assemble with zero fill
        let h3 = c.open_write("/f", None).unwrap();
        c.write(&h3, 4, b"AB").unwrap();
        let node = c.commit_write(&h3).unwrap();
        assert_eq!(node.size, 6);
        assert_eq!(c.read("/f", 0, 10).unwrap(), vec![0, 0, 0, 0, b'A', b'B']);
    }

    #[test]
    fn rename_is_metadata_only() {
        let c = core();
        c.mkdir("/src", 0o755).unwrap();
        c.mkdir("/dst", 0o755).unwrap();
        c.create("/src/f", 0o644).unwrap();
        let h = c.open_write("/src/f", None).unwrap();
        c.write(&h, 0, b"payload").unwrap();
        let before = c.commit_write(&h).unwrap();

        c.rename("/src/f", "/dst/g").unwrap();
        let after = c.lookup("/dst/g").unwrap();
        assert_eq!(after.id, before.id); // same inode
        assert_eq!(after.object_key, before.object_key); // same object key
        assert_eq!(c.read("/dst/g", 0, 100).unwrap(), b"payload");
        assert!(c.lookup("/src/f").is_err());

        // dir rename moves children
        c.rename("/dst", "/dst2").unwrap();
        assert!(c.lookup("/dst2/g").is_ok());

        // cycle rejection
        c.rename("/dst2", "/dst2/sub").unwrap_err();
    }

    /// The exact interleaving a reviewer should worry about: both writers
    /// observe the same version BEFORE either commits. With the CAS commit
    /// there is exactly one winner, the winner's bytes are what the file
    /// contains, and the loser's upload is removed.
    #[test]
    fn racing_writers_single_winner_data_intact() {
        let c = core();
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"version one").unwrap();
        c.commit_write(&h).unwrap(); // v2
        let v = c.lookup("/f").unwrap().version;

        // Both writers open at v and buffer content while neither committed.
        let ha = c.open_write("/f", Some(v)).unwrap();
        let hb = c.open_write("/f", Some(v)).unwrap();
        c.write(&ha, 0, b"content from writer A").unwrap();
        c.write(&hb, 0, b"content from writer B").unwrap();

        let ra = c.commit_write(&ha).unwrap();
        let rb = c.commit_write(&hb);
        let err = rb.expect_err("second commit must conflict");
        assert!(matches!(err, Error::Conflict { expected, actual: 2, .. } if expected == v));

        // Winner's data is exactly what the file serves; metadata agrees.
        assert_eq!(c.read("/f", 0, 100).unwrap(), b"content from writer A");
        let after = c.lookup("/f").unwrap();
        assert_eq!(after.version, ra.version);
        assert_eq!(after.etag, ra.etag);
        assert_eq!(after.size, ra.size);
        let live = c
            .objects()
            .head(after.object_key.as_deref().unwrap())
            .unwrap();
        assert_eq!(live.etag, ra.etag);
        assert_eq!(live.size, ra.size as u64);

        // The loser's version key was cleaned up (conflict path deletes its
        // own object). The create committed NO object (metadata-only), so
        // exactly TWO objects remain: the live winner plus the pre-race
        // commit retained by the retention window (one superseded record),
        // and the loser's key is gone (assert by set).
        let mut leftovers = Vec::new();
        let mut token = None;
        loop {
            let page = c
                .objects()
                .list(&format!("objects/{}/", after.id), token.as_deref(), 100)
                .unwrap();
            for item in &page.items {
                leftovers.push(item.key.clone());
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        assert_eq!(leftovers.len(), 2, "loser gone; no placeholder existed");
        let mut superseded = c.metadata().all_superseded().unwrap();
        superseded.sort();
        assert_eq!(superseded.len(), 1, "{superseded:?}");
        assert!(
            !superseded.contains(&after.object_key.clone().unwrap()),
            "live key must not be in the retained set"
        );
        for k in &superseded {
            assert!(leftovers.contains(k), "recorded key must exist: {k}");
            assert!(k.starts_with(&format!("objects/{}/", after.id)));
        }
    }

    /// True thread race: N writers commit simultaneously many times. Every
    /// round must end with metadata and object agreeing on exactly one
    /// winner's content (no silent overwrite, no torn state).
    #[test]
    fn concurrent_commit_stress_single_winner() {
        for round in 0..25 {
            let c = Arc::new(core());
            c.create("/race", 0o644).unwrap();
            let v = c.lookup("/race").unwrap().version;

            let mut joins = Vec::new();
            for w in 0..4 {
                let c = c.clone();
                joins.push(std::thread::spawn(move || {
                    let h = c.open_write("/race", Some(v)).unwrap();
                    let payload = format!("writer-{w}-payload-{round}");
                    c.write(&h, 0, payload.as_bytes()).unwrap();
                    c.commit_write(&h)
                }));
            }
            let mut wins = 0;
            let mut winner_node = None;
            for j in joins {
                match j.join().unwrap() {
                    Ok(node) => {
                        wins += 1;
                        winner_node = Some(node);
                    }
                    Err(Error::Conflict { .. }) => {}
                    Err(e) => panic!("unexpected error: {e}"),
                }
            }
            assert_eq!(wins, 1, "round {round}: exactly one winner");
            let node = winner_node.unwrap();
            let served = c.read("/race", 0, 1000).unwrap();
            // The served bytes must be SOME writer's full payload — never a
            // mix, never the loser's after the winner committed.
            let candidates: Vec<String> = (0..4)
                .map(|w| format!("writer-{w}-payload-{round}"))
                .collect();
            assert!(
                candidates.iter().any(|p| p.as_bytes() == served.as_slice()),
                "served bytes must equal exactly one writer's payload: {served:?}"
            );
            let live = c
                .objects()
                .head(node.object_key.as_deref().unwrap())
                .unwrap();
            assert_eq!(live.etag, node.etag);
            assert_eq!(live.size as i64, node.size);
            assert_eq!(live.size, served.len() as u64);
        }
    }

    /// Declared v1 semantics for open-handle vs namespace-mutation interplay
    /// (pinned so future changes are conscious decisions, not accidents):
    /// - rename is metadata-only; a write session opened before the rename
    ///   commits onto the file under its NEW path (same inode, same CAS).
    /// - unlink while a write session is open: the commit fails NotFound —
    ///   it must NOT resurrect the file.
    #[test]
    fn open_handle_interacts_with_rename_and_unlink() {
        let c = core();
        c.mkdir("/d", 0o755).unwrap();
        c.create("/d/f", 0o644).unwrap();
        let v = c.lookup("/d/f").unwrap().version;

        // open a session, then rename the file underneath it
        let h = c.open_write("/d/f", Some(v)).unwrap();
        c.write(&h, 0, b"written after rename").unwrap();
        c.rename("/d/f", "/d/g").unwrap();
        let node = c.commit_write(&h).unwrap();
        assert_eq!(node.id, c.lookup("/d/g").unwrap().id);
        assert_eq!(c.read("/d/g", 0, 100).unwrap(), b"written after rename");

        // open a session, then unlink the file underneath it
        let h2 = c.open_write("/d/g", None).unwrap();
        c.write(&h2, 0, b"doomed").unwrap();
        c.unlink("/d/g").unwrap();
        let err = c.commit_write(&h2).unwrap_err();
        assert!(
            matches!(err, Error::NotFound(_)),
            "commit after unlink must fail, got {err}"
        );
        assert!(
            c.lookup("/d/g").is_err(),
            "unlinked file must not resurrect"
        );
    }

    #[test]
    fn rename_overwrite_rules() {
        let c = core();
        c.create("/a", 0o644).unwrap();
        c.create("/b", 0o644).unwrap();
        let h = c.open_write("/a", None).unwrap();
        c.write(&h, 0, b"AAA").unwrap();
        c.commit_write(&h).unwrap();

        // file -> file overwrite is allowed
        c.rename("/a", "/b").unwrap();
        assert!(c.lookup("/a").is_err());
        assert_eq!(c.read("/b", 0, 10).unwrap(), b"AAA");

        // file -> dir is rejected
        c.mkdir("/dir", 0o755).unwrap();
        c.create("/f2", 0o644).unwrap();
        assert!(matches!(
            c.rename("/f2", "/dir"),
            Err(Error::AlreadyExists(_))
        ));
    }

    /// S1b: renaming a file onto ITS OWN path is a successful no-op —
    /// node id, content, version, object_key and superseded records all
    /// stand, and the object is neither retired nor deleted.
    #[test]
    fn rename_same_path_is_a_noop() {
        let c = core();
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"self").unwrap();
        c.commit_write(&h).unwrap();
        let before = c.lookup("/f").unwrap();

        c.rename("/f", "/f").unwrap();

        let after = c.lookup("/f").unwrap();
        assert_eq!(after.id, before.id);
        assert_eq!(after.version, before.version);
        assert_eq!(after.size, before.size);
        assert_eq!(after.object_key, before.object_key);
        assert_eq!(c.read("/f", 0, 10).unwrap(), b"self");
        assert!(c.metadata().all_superseded().unwrap().is_empty());
    }

    /// Equivalent path forms (`//f`, `/./f`) resolve to the same node and
    /// must not be misjudged as overwriting a different file.
    #[test]
    fn rename_equivalent_paths_are_a_noop() {
        let c = core();
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"same").unwrap();
        c.commit_write(&h).unwrap();
        let before = c.lookup("/f").unwrap();

        c.rename("/f", "//f").unwrap();
        c.rename("/f", "/./f").unwrap();

        let after = c.lookup("/f").unwrap();
        assert_eq!(after.id, before.id);
        assert_eq!(after.version, before.version);
        assert_eq!(after.object_key, before.object_key);
        assert_eq!(c.read("/f", 0, 10).unwrap(), b"same");
        assert!(c.metadata().all_superseded().unwrap().is_empty());
    }

    /// A directory renamed onto itself is also a no-op (POSIX). A
    /// file-over-file overwrite moves the SOURCE node: its id and
    /// object_key are stable, the old target disappears, and the old
    /// target object is retired only AFTER the namespace operation
    /// succeeded.
    #[test]
    fn rename_file_over_file_moves_the_source_node() {
        let c = core();
        c.mkdir("/d", 0o755).unwrap();
        c.rename("/d", "/d").unwrap();
        assert!(c.lookup("/d").is_ok());

        for (name, body) in [("/src", &b"SRC"[..]), ("/dst", &b"DST"[..])] {
            c.create(name, 0o644).unwrap();
            let h = c.open_write(name, None).unwrap();
            c.write(&h, 0, body).unwrap();
            c.commit_write(&h).unwrap();
        }
        let src = c.lookup("/src").unwrap();
        let key_src = src.object_key.clone().unwrap();
        let key_dst = c.lookup("/dst").unwrap().object_key.clone().unwrap();
        assert_ne!(src.id, c.lookup("/dst").unwrap().id);

        c.rename("/src", "/dst").unwrap();

        let moved = c.lookup("/dst").unwrap();
        assert_eq!(moved.id, src.id, "the source node itself moved");
        assert_eq!(moved.object_key.as_deref(), Some(key_src.as_str()));
        assert_eq!(c.read("/dst", 0, 10).unwrap(), b"SRC");
        assert!(c.lookup("/src").is_err());
        // the replaced object is recorded only after the successful rename
        assert!(c.metadata().all_superseded().unwrap().contains(&key_dst));
        // the moved live pointer targets an existing object
        c.objects.head(&key_src).unwrap();
    }

    /// Directory semantics must not regress: overwriting a directory (from
    /// either kind) is refused, both files still stand after the refusals,
    /// plain moves work, and moving a directory below itself is refused.
    #[test]
    fn rename_dir_semantics_do_not_regress() {
        let c = core();
        c.mkdir("/d1", 0o755).unwrap();
        c.mkdir("/d2", 0o755).unwrap();
        c.create("/f", 0o644).unwrap();

        assert!(matches!(
            c.rename("/d1", "/d2"),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            c.rename("/d1", "/f"),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            c.rename("/f", "/d2"),
            Err(Error::AlreadyExists(_))
        ));
        assert!(c.lookup("/d1").is_ok());
        assert!(c.lookup("/d2").is_ok());
        assert!(c.lookup("/f").is_ok());

        c.rename("/d1", "/d3").unwrap();
        assert!(c.lookup("/d3").is_ok());
        assert!(c.lookup("/d1").is_err());

        assert!(matches!(c.rename("/d3", "/d3/sub"), Err(Error::Cycle(_))));
        assert!(c.lookup("/d3").is_ok());
        assert!(c.lookup("/d3/sub").is_err());
    }

    #[test]
    fn unlink_and_rmdir() {
        let c = core();
        c.mkdir("/d", 0o755).unwrap();
        c.create("/d/f", 0o644).unwrap();
        // commit content so the retention path has an object to keep
        let h0 = c.open_write("/d/f", None).unwrap();
        c.write(&h0, 0, b"payload").unwrap();
        c.commit_write(&h0).unwrap();
        let f = c.lookup("/d/f").unwrap();
        let key = f.object_key.clone().unwrap();

        assert!(matches!(c.rmdir("/d"), Err(Error::DirectoryNotEmpty(_))));
        c.unlink("/d/f").unwrap();
        assert!(c.lookup("/d/f").is_err());
        // retention: the object stays restorable and is recorded
        assert!(c.objects.head(&key).is_ok());
        assert!(c.metadata().all_superseded().unwrap().contains(&key));

        c.rmdir("/d").unwrap();
        assert!(c.lookup("/d").is_err());
        assert!(matches!(c.rmdir("/"), Err(Error::InvalidArgument(_))));
        assert!(matches!(c.unlink("/d"), Err(Error::NotFound(_))));
    }

    // --- S4.2.1: no unprotected destructive delete -------------------------

    /// Retention-off unlink must not delete bytes inline: the object stays
    /// in the store and a durable deletion intent is parked for the gated
    /// pipeline. (Old behavior: `objects.delete(key)?` ran BEFORE the
    /// metadata delete — a failed unlink could dangle a live row on
    /// already-deleted bytes.)
    #[test]
    fn p0a1_unlink_retention_off_leaves_object_to_gated_intent() {
        let objs = Arc::new(MemObjectStore::new());
        let c = Core::with_retention(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            objs.clone(),
            0,
        );
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"x").unwrap();
        c.commit_write(&h).unwrap();
        let key = c.lookup("/f").unwrap().object_key.clone().unwrap();

        c.unlink("/f").unwrap();
        assert!(c.lookup("/f").is_err());
        assert!(
            objs.head(&key).is_ok(),
            "unlink must not physically delete: only the gated executor may"
        );
        let claimed = c.metadata().gc_claim_deletions(10, 1).unwrap();
        assert!(
            claimed.iter().any(|d| d.key == key && d.kind == "object"),
            "the unlinked object must carry a durable deletion intent: {claimed:?}"
        );
    }

    /// Even with the object store unable to serve a delete, unlink keeps
    /// its metadata contract: the node goes, nothing is deleted inline,
    /// the intent is recorded (old behavior: unlink propagated the store
    /// error and left the node row behind).
    #[test]
    fn p0a1_unlink_succeeds_without_inline_object_delete() {
        let objs = Arc::new(MemObjectStore::new());
        let c = Core::with_retention(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            objs.clone(),
            0,
        );
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"x").unwrap();
        c.commit_write(&h).unwrap();
        let key = c.lookup("/f").unwrap().object_key.clone().unwrap();

        objs.inject_delete_failure(&key);
        c.unlink("/f")
            .expect("unlink must not depend on an inline remote delete");
        assert!(
            c.lookup("/f").is_err(),
            "the metadata delete is the unlink contract"
        );
        assert!(objs.head(&key).is_ok());
    }

    /// Overwrite with retention off: the replaced version object is parked
    /// as a deletion intent, never deleted inline by the commit path.
    #[test]
    fn p0a1_retire_overwrite_enqueues_intent_instead_of_inline_delete() {
        let objs = Arc::new(MemObjectStore::new());
        let c = Core::with_retention(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            objs.clone(),
            0,
        );
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"one").unwrap();
        c.commit_write(&h).unwrap();
        let old_key = c.lookup("/f").unwrap().object_key.clone().unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"two").unwrap();
        c.commit_write(&h).unwrap();

        assert!(
            objs.head(&old_key).is_ok(),
            "the replaced version must not be deleted inline on overwrite"
        );
        assert!(c.metadata().all_superseded().unwrap().is_empty());
        let claimed = c.metadata().gc_claim_deletions(10, 1).unwrap();
        assert!(
            claimed
                .iter()
                .any(|d| d.key == old_key && d.kind == "object"),
            "the replaced version must carry a durable deletion intent: {claimed:?}"
        );
    }

    /// Retention-disabled unlink parks a deletion intent instead of the
    /// old inline delete: the object survives until the gated gc executor
    /// runs, and the superseded ledger stays untouched. This replaces the
    /// pre-S4.2.1 `unlink_deletes_immediately_when_retention_disabled`,
    /// which encoded exactly the unprotected destructive path the audit
    /// retired (a committed object could leave through an ungated delete).
    #[test]
    fn unlink_with_retention_disabled_parks_intent_not_inline_delete() {
        let c = Core::with_retention(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(MemObjectStore::new()),
            0,
        );
        c.create("/f", 0o644).unwrap();
        // commit content so there is an object to reclaim later
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"x").unwrap();
        c.commit_write(&h).unwrap();
        let key = c.lookup("/f").unwrap().object_key.clone().unwrap();
        c.unlink("/f").unwrap();
        assert!(
            c.objects.head(&key).is_ok(),
            "no inline delete: the gated executor owns reclamation"
        );
        assert!(c.metadata().all_superseded().unwrap().is_empty());
        let claimed = c.metadata().gc_claim_deletions(10, 1).unwrap();
        assert!(claimed.iter().any(|d| d.key == key && d.kind == "object"));
    }

    /// The point of retention: overwrite a file, GC nothing — the metadata
    /// snapshot's key must still be readable, and gc_candidates must NOT
    /// return a key a live node still references (crash-window guard).
    #[test]
    fn retention_keeps_superseded_bytes_and_gc_respects_live_refs() {
        let c = core();
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"version one").unwrap();
        c.commit_write(&h).unwrap();
        let old_key = {
            // after the NEXT commit the v2 key becomes superseded
            let v = c.lookup("/f").unwrap().version;
            let h = c.open_write("/f", Some(v)).unwrap();
            let old = c.lookup("/f").unwrap().object_key.clone().unwrap();
            c.write(&h, 0, b"version two").unwrap();
            c.commit_write(&h).unwrap();
            old
        };
        // bytes restorable from the retained object
        assert_eq!(c.objects.get(&old_key, None).unwrap().data, b"version one");
        // crash-window guard: record a LIVE node's key and check that gc
        // never returns it as a candidate.
        let live_key = c.lookup("/f").unwrap().object_key.clone().unwrap();
        c.metadata().record_superseded(&live_key, 0).unwrap();
        let candidates = c.metadata().gc_candidates(i64::MAX, 100).unwrap();
        assert!(
            !candidates.contains(&live_key),
            "gc must never purge a key a live node references"
        );
        // the genuinely superseded key IS a candidate at +inf cutoff
        assert!(candidates.contains(&old_key));
    }

    #[test]
    fn read_missing_object_reports_missing() {
        let c = core();
        c.create("/f", 0o644).unwrap();
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"content").unwrap();
        c.commit_write(&h).unwrap();
        // simulate external deletion of the object behind metadata's back
        let node = c.lookup("/f").unwrap();
        c.objects.delete(&node.object_key.unwrap()).unwrap();
        assert!(matches!(c.read("/f", 0, 10), Err(Error::ObjectMissing(_))));
    }

    #[test]
    fn concurrent_core_use() {
        let c = Arc::new(core());
        let mut joins = Vec::new();
        for i in 0..4 {
            let c = c.clone();
            joins.push(std::thread::spawn(move || {
                c.mkdir(&format!("/t{i}"), 0o755).unwrap();
                c.create(&format!("/t{i}/f"), 0o644).unwrap();
                let h = c.open_write(&format!("/t{i}/f"), None).unwrap();
                c.write(&h, 0, format!("data-{i}").as_bytes()).unwrap();
                c.commit_write(&h).unwrap();
            }));
        }
        for j in joins {
            j.join().unwrap();
        }
        assert_eq!(c.readdir("/").unwrap().len(), 4);
        assert_eq!(c.read("/t2/f", 0, 100).unwrap(), b"data-2");
    }
    // ------------------------------------------------------------------
    // S3.5 red tests: the read path must never splice a node snapshot with
    // content state that the async drain invalidated afterwards. Both tests
    // hand read_node_bytes a node captured BEFORE the drain - the exact
    // shape a concurrent reader holds in production - and demand the
    // CURRENT content.
    // ------------------------------------------------------------------
    fn wb_core(tag: &str) -> Core {
        use crate::wb::WbOptions;
        let dir = std::env::temp_dir().join(format!(
            "lightfs-core-wb-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Core::with_options(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(MemObjectStore::new()),
            DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 1 << 20,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 0,
            }),
        )
        .unwrap()
    }

    fn flush(c: &Core, path: &str, body: &[u8], expected: Option<i64>) {
        c.write_stream(
            path,
            expected,
            false,
            &mut std::io::Cursor::new(body.to_vec()),
            body.len() as u64,
        )
        .unwrap();
    }

    /// First write of a file: the stale snapshot has NO object_key at all,
    /// so splicing it after the drain must not fall back to a default key
    /// that was never uploaded (old code: read error).
    #[test]
    fn red_read_with_pre_drain_snapshot_first_generation() {
        let c = wb_core("splice1");
        c.create("/f", 0o644).unwrap();
        flush(&c, "/f", b"generation one", Some(0));
        let snapshot = c.lookup("/f").unwrap(); // projected: size 14, v1, no key
        c.wb().unwrap().drain_once().unwrap(); // commit + finish + remove staging
        let got = c.read_node_by_id(snapshot.id, 0, 14).unwrap();
        assert_eq!(
            got, b"generation one",
            "stale snapshot must resolve to the committed content"
        );
    }

    /// Overwrite: the stale snapshot still points at the PREVIOUS
    /// generation's object; splicing it after the drain would serve the old
    /// version. The read must return the current content instead.
    #[test]
    fn red_read_with_pre_drain_snapshot_second_generation() {
        let c = wb_core("splice2");
        c.create("/f", 0o644).unwrap();
        flush(&c, "/f", b"old", Some(0));
        c.wb().unwrap().drain_once().unwrap();
        flush(&c, "/f", b"new-content", Some(1));
        let snapshot = c.lookup("/f").unwrap(); // projected v2, object_key still gen-1
        c.wb().unwrap().drain_once().unwrap();
        let got = c.read_node_by_id(snapshot.id, 0, 11).unwrap();
        assert_eq!(
            got, b"new-content",
            "must not serve the previous generation through the stale key"
        );
    }

    /// readdir feeds the mount's readdirplus seed: a child with staged
    /// pending writes must carry the PROJECTED size there too, or one
    /// directory listing poisons the attr cache with the committed size 0
    /// and every stat within the attr_ttl window goes blind. git pack
    /// verification read its just-written 11 MB pack as an empty file
    /// through exactly this hole.
    #[test]
    fn red_readdir_projects_pending_writes() {
        let c = wb_core("rdproj");
        c.create("/f", 0o644).unwrap();
        flush(&c, "/f", b"staged-bytes", Some(0)); // queued: drain_workers=0
        let listed = c.readdir("/").unwrap();
        let f = listed
            .iter()
            .find(|n| n.name == "f")
            .expect("child f listed");
        assert_eq!(
            f.size, 12,
            "readdir must serve the staged size, not the committed 0"
        );
        assert_eq!(f.version, 1, "readdir must serve the projected version");
    }

    // ------------------------------------------------------------------
    // S3.5.1 red tests: read_pending's internal retry loop can exhaust
    // under a persistent coalescer. Exhaustion must surface as an explicit
    // retry to Core (which re-resolves the WHOLE snapshot), never as "no
    // pending" — that disguise makes a brand-new file read as EOF and an
    // overwritten file read as its previous committed generation.
    // ------------------------------------------------------------------

    #[test]
    fn red_pending_coalesce_race_on_new_file_must_not_read_as_eof() {
        // A1: a file with NO committed object ever, staging rewritten
        // under the reader until the probe loop exhausts. The read must
        // return the FINAL generation's full content - not zero bytes,
        // not a spurious EOF from the committed size 0.
        let c = wb_core("pendcoalesce1");
        c.create("/f", 0o644).unwrap();
        flush(&c, "/f", b"gen-1", Some(0)); // queued, never drained
        let node = c.lookup("/f").unwrap();

        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let expected = Arc::new(std::sync::atomic::AtomicI64::new(1));
        let eng = Arc::clone(c.wb().unwrap());
        let node_for_hook = node.clone();
        let probes_for_hook = Arc::clone(&probes);
        let expected_for_hook = Arc::clone(&expected);
        *c.wb().unwrap().test_hook_before_open.lock().unwrap() = Some(Box::new(move || {
            let n = probes_for_hook.fetch_add(1, Ordering::Relaxed);
            if n >= 3 {
                return; // stop coalescing: the read may now succeed
            }
            let body = format!("gen-{}", n + 2); // gen-2, gen-3, gen-4
            let exp = expected_for_hook.fetch_add(1, Ordering::Relaxed) + 1;
            eng.write_deferred(
                &node_for_hook,
                exp,
                &mut std::io::Cursor::new(body.into_bytes()),
                5,
            )
            .unwrap();
        }));

        let got = c.read("/f", 0, 64).unwrap();
        assert_eq!(
            got, b"gen-4",
            "must serve the final staged generation, not EOF"
        );
        assert!(
            probes.load(Ordering::Relaxed) >= 3,
            "hook must have raced the probe loop"
        );

        *c.wb().unwrap().test_hook_before_open.lock().unwrap() = None;
    }

    #[test]
    fn red_pending_coalesce_race_on_committed_file_must_not_serve_base() {
        // A2: `base` is committed, a rewrite is queued, and the reader
        // keeps losing the coalesce race. The read must NOT fall back to
        // the committed `base` (the projected-view comparison made both
        // snapshots look identical and served it) - it must serve the
        // latest staged generation.
        let c = wb_core("pendcoalesce2");
        c.create("/f", 0o644).unwrap();
        flush(&c, "/f", b"base", Some(0));
        c.wb().unwrap().drain_once().unwrap(); // commit base
        flush(&c, "/f", b"gen-1", Some(1)); // queued rewrite
        let node = c.lookup("/f").unwrap();

        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let expected = Arc::new(std::sync::atomic::AtomicI64::new(1));
        let eng = Arc::clone(c.wb().unwrap());
        let node_for_hook = node.clone();
        let probes_for_hook = Arc::clone(&probes);
        let expected_for_hook = Arc::clone(&expected);
        *c.wb().unwrap().test_hook_before_open.lock().unwrap() = Some(Box::new(move || {
            let n = probes_for_hook.fetch_add(1, Ordering::Relaxed);
            if n >= 5 {
                return; // >3 internal retries: the loop MUST have exhausted
            }
            let body = format!("gen-{}", n + 2);
            let exp = expected_for_hook.fetch_add(1, Ordering::Relaxed) + 1;
            eng.write_deferred(
                &node_for_hook,
                exp,
                &mut std::io::Cursor::new(body.into_bytes()),
                5,
            )
            .unwrap();
        }));

        let got = c.read("/f", 0, 64).unwrap();
        assert_eq!(
            got, b"gen-6",
            "must serve the latest staged bytes, never committed base"
        );
        assert!(
            probes.load(Ordering::Relaxed) >= 3,
            "the internal retry loop must have been exhausted (5 coalesces)"
        );

        *c.wb().unwrap().test_hook_before_open.lock().unwrap() = None;
    }

    #[test]
    fn red_pending_permanent_coalesce_race_ends_in_explicit_error() {
        // A2 variant: the coalescer NEVER stops. Every retry loses; the
        // read must end in an explicit error - silently serving the
        // committed `base` (the old disguise) is forbidden.
        let c = wb_core("pendcoalesce3");
        c.create("/f", 0o644).unwrap();
        flush(&c, "/f", b"base", Some(0));
        c.wb().unwrap().drain_once().unwrap();
        flush(&c, "/f", b"gen-1", Some(1));
        let node = c.lookup("/f").unwrap();

        let probes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let expected = Arc::new(std::sync::atomic::AtomicI64::new(1));
        let eng = Arc::clone(c.wb().unwrap());
        let node_for_hook = node.clone();
        let probes_for_hook = Arc::clone(&probes);
        let expected_for_hook = Arc::clone(&expected);
        *c.wb().unwrap().test_hook_before_open.lock().unwrap() = Some(Box::new(move || {
            let n = probes_for_hook.fetch_add(1, Ordering::Relaxed);
            let body = format!("gen-{}", n + 2);
            let exp = expected_for_hook.fetch_add(1, Ordering::Relaxed) + 1;
            eng.write_deferred(
                &node_for_hook,
                exp,
                &mut std::io::Cursor::new(body.into_bytes()),
                5,
            )
            .unwrap();
        }));

        let got = c.read("/f", 0, 64);
        assert!(
            got.is_err(),
            "exhausted retries must error, not serve stale content"
        );
        let msg = format!("{}", got.unwrap_err());
        assert!(
            msg.contains("race") || msg.contains("racing") || msg.contains("retry"),
            "error must name the race/retry, got: {msg}"
        );
        assert!(
            probes.load(Ordering::Relaxed) > 3,
            "the outer loop must have retried past the internal exhaustion"
        );

        *c.wb().unwrap().test_hook_before_open.lock().unwrap() = None;
    }
}

#[cfg(test)]
mod symlink_tests {
    //! Symlink contract tests: these DEFINE the POSIX semantics we ship.
    //! Written first (contract), implementation second.

    use super::*;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::object_store::mem::MemObjectStore;

    fn core() -> Core {
        Core::new(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(MemObjectStore::new()),
        )
    }

    fn put(c: &Core, path: &str, data: &[u8]) {
        let _ = c.create(path, 0o644);
        let h = c.open_write(path, None).unwrap();
        c.write(&h, 0, data).unwrap();
        c.commit_write(&h).unwrap();
    }

    #[test]
    fn symlink_readlink_roundtrip() {
        let c = core();
        c.mkdir("/d", 0o755).unwrap();
        let n = c.symlink("/d/target", "/d/link").unwrap();
        assert!(n.is_symlink());
        assert_eq!(n.target.as_deref(), Some("/d/target"));
        assert_eq!(c.readlink("/d/link").unwrap(), "/d/target");
        // size records the target length for lstat
        assert_eq!(n.size, "/d/target".len() as i64);
    }

    #[test]
    fn lstat_vs_stat_on_the_link() {
        let c = core();
        put(&c, "/f", b"hello");
        c.symlink("/f", "/l").unwrap();
        // lstat: the link itself (kind symlink, size = target length)
        let st = c.lookup("/l").unwrap();
        assert!(st.is_symlink());
        assert_eq!(st.size, "/f".len() as i64);
        // stat: follows to the file
        let st = c.getattr("/l").unwrap();
        assert!(st.is_file());
        assert_eq!(st.size, 5);
        // reading THROUGH the link returns the target's bytes
        assert_eq!(c.read("/l", 0, 100).unwrap(), b"hello");
        assert_eq!(c.open("/l").unwrap().id, c.lookup("/f").unwrap().id);
    }

    #[test]
    fn relative_and_dir_links() {
        let c = core();
        c.mkdir("/data", 0o755).unwrap();
        put(&c, "/data/inner", b"x");
        // relative target resolves against the LINK's directory
        c.symlink("../data", "/alias").unwrap();
        // stat through the dir link
        let st = c.getattr("/alias/inner").unwrap();
        assert_eq!(st.size, 1);
        // readdir through the dir link lists the target dir
        let names: Vec<String> = c
            .readdir("/alias")
            .unwrap()
            .into_iter()
            .map(|n| n.name)
            .collect();
        assert_eq!(names, vec!["inner".to_string()]);
        // create THROUGH an intermediate dir link lands in the target dir
        put(&c, "/alias/created_via_link", b"y");
        assert!(c.lookup("/data/created_via_link").is_ok());
    }

    #[test]
    fn absolute_target_is_mount_relative() {
        let c = core();
        put(&c, "/etc-passwd-like", b"z");
        // An absolute target NEVER escapes the mount: it resolves from
        // this filesystem's root, not the host's.
        c.symlink("/etc-passwd-like", "/abs").unwrap();
        assert_eq!(c.read("/abs", 0, 100).unwrap(), b"z");
        // A target pointing at a nonexistent mount path is dangling.
        c.symlink("/no/such/path", "/dangling").unwrap();
        assert!(c.lookup("/dangling").unwrap().is_symlink());
        assert!(matches!(
            c.read("/dangling", 0, 10),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn dotdot_clamps_at_root_and_cannot_escape() {
        // resolve_target contract (pure string level)
        assert_eq!(resolve_target("/a", "../../etc").unwrap(), "/etc");
        assert_eq!(resolve_target("/", "../../etc").unwrap(), "/etc");
        assert_eq!(resolve_target("/a/b", "../../..").unwrap(), "/");
        // And through an actual walk: /up -> /../.. stays inside.
        let c = core();
        put(&c, "/top", b"t");
        c.mkdir("/sub", 0o755).unwrap();
        c.symlink("/../top", "/sub/up").unwrap();
        assert_eq!(c.read("/sub/up", 0, 10).unwrap(), b"t");
        // .. above the root is not an escape hatch to anything.
        c.symlink("../../../elsewhere", "/nowhere").unwrap();
        assert!(matches!(c.read("/nowhere", 0, 10), Err(Error::NotFound(_))));
    }

    #[test]
    fn cycles_fail_with_loop_error() {
        let c = core();
        c.symlink("/b", "/a").unwrap();
        c.symlink("/a", "/b").unwrap();
        let err = c.getattr("/a").unwrap_err();
        assert!(
            matches!(err, Error::InvalidArgument(ref m) if m.contains("too many symlink")),
            "expected loop error, got {err:?}"
        );
        // self-loop
        c.symlink("/self", "/self").unwrap();
        assert!(c.getattr("/self").is_err());
        // a long chain still terminates correctly (well under the cap)
        let c2 = core();
        put(&c2, "/end", b"e");
        for i in 0..30 {
            let next = format!("/chain{}", i + 1);
            let prev = if i == 0 {
                "/end".to_string()
            } else {
                format!("/chain{i}")
            };
            c2.symlink(&prev, &next).unwrap();
        }
        assert_eq!(c2.read("/chain30", 0, 10).unwrap(), b"e");
        // and a chain past the budget fails
        let c3 = core();
        for i in 0..45 {
            let next = format!("/long{}", i + 1);
            let prev = if i == 0 {
                "/end".to_string()
            } else {
                format!("/long{i}")
            };
            c3.symlink(&prev, &next).unwrap();
        }
        assert!(c3.getattr("/long45").is_err());
    }

    #[test]
    fn name_collisions_and_rename_unlink_semantics() {
        let c = core();
        put(&c, "/real", b"r");
        c.symlink("/real", "/same").unwrap();
        // the name is occupied by ANY kind
        assert!(matches!(
            c.symlink("/real", "/same"),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            c.create("/same", 0o644),
            Err(Error::AlreadyExists(_))
        ));
        assert!(matches!(
            c.mkdir("/same", 0o755),
            Err(Error::AlreadyExists(_))
        ));
        // renaming the TARGET leaves the (now dangling) link untouched
        c.rename("/real", "/moved").unwrap();
        assert!(c.lookup("/same").unwrap().is_symlink());
        assert!(matches!(c.read("/same", 0, 10), Err(Error::NotFound(_))));
        assert_eq!(c.readlink("/same").unwrap(), "/real");
        // renaming the LINK keeps its target verbatim
        c.rename("/same", "/same2").unwrap();
        assert_eq!(c.readlink("/same2").unwrap(), "/real");
        // deleting the link never touches the target
        put(&c, "/real2", b"r2");
        c.symlink("/real2", "/l2").unwrap();
        c.unlink("/l2").unwrap();
        assert!(matches!(c.readlink("/l2"), Err(Error::NotFound(_))));
        assert_eq!(c.read("/real2", 0, 10).unwrap(), b"r2");
        // rmdir on a link fails instead of removing the target
        c.symlink("/real2", "/l3").unwrap();
        assert!(c.rmdir("/l3").is_err());
        assert!(c.lookup("/real2").is_ok());
        // unlinking a symlink does NOT delete the target's object
        c.unlink("/real2").unwrap();
        assert!(matches!(c.read("/real2", 0, 10), Err(Error::NotFound(_))));
    }

    #[test]
    fn readlink_rejects_non_symlink() {
        let c = core();
        c.mkdir("/d", 0o755).unwrap();
        put(&c, "/f", b"x");
        for p in ["/d", "/f", "/"] {
            let err = c.readlink(p).unwrap_err();
            assert!(
                matches!(
                    err,
                    Error::KindMismatch {
                        expected: "symlink",
                        ..
                    }
                ),
                "{p}: {err:?}"
            );
        }
    }

    #[test]
    fn symlink_target_validation() {
        let c = core();
        assert!(matches!(
            c.symlink("", "/bad-empty"),
            Err(Error::InvalidArgument(_))
        ));
        // dangling targets ARE legal at creation time (checked only at use)
        c.symlink("/not-yet", "/lazy").unwrap();
        put(&c, "/not-yet", b"now");
        assert_eq!(c.read("/lazy", 0, 10).unwrap(), b"now");
    }

    #[test]
    fn fsck_ignores_symlink_nodes() {
        let c = core();
        put(&c, "/f", b"data");
        c.symlink("/f", "/l").unwrap();
        c.symlink("/nowhere", "/dangling").unwrap();
        let report = crate::fsck::fsck(&c).unwrap();
        assert_eq!(report.problems(), 0, "symlinks have no object");
    }

    #[test]
    fn symlink_survives_metadata_snapshot_roundtrip() {
        // backup = SQLite snapshot; the target column must ride along.
        let dir = std::env::temp_dir().join(format!("lightfs-sym-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("meta.db");
        let objects = Arc::new(MemObjectStore::new());
        let store = SqliteMetadataStore::open(db.to_str().unwrap()).unwrap();
        let c = Core::new(Arc::new(store), objects.clone());
        put(&c, "/f", b"data");
        c.symlink("../f", "/rel").unwrap();
        c.symlink("/f", "/abs").unwrap();
        drop(c);
        // reopen: the target column must still be there and populated
        let store = SqliteMetadataStore::open(db.to_str().unwrap()).unwrap();
        let c = Core::new(Arc::new(store), objects);
        assert_eq!(c.readlink("/rel").unwrap(), "../f");
        assert_eq!(c.readlink("/abs").unwrap(), "/f");
        assert_eq!(c.read("/rel", 0, 10).unwrap(), b"data");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- R4.2c: stable identity (AUTOINCREMENT from CREATE time) ------------

    /// R4.2c acceptance: a deleted file's id is NEVER handed to the next
    /// created file, so the old identity resolves to nothing — a read
    /// through it reports an error, never another file's bytes.
    #[test]
    fn recycled_id_read_must_report_identity_gone() {
        let c = core();
        c.create("/a", 0o644).unwrap();
        let h = c.open_write("/a", None).unwrap();
        c.write(&h, 0, b"A's bytes").unwrap();
        let a = c.commit_write(&h).unwrap();
        c.unlink("/a").unwrap();
        c.create("/b", 0o644).unwrap();
        let hb = c.open_write("/b", None).unwrap();
        c.write(&hb, 0, b"B's bytes").unwrap();
        let b = c.commit_write(&hb).unwrap();
        // AUTOINCREMENT: the recreated file got a fresh id.
        assert_ne!(b.id, a.id, "ids must never be reused");
        // and the old identity is precisely IdentityGone, not some other
        // error or another file's bytes
        assert!(
            matches!(c.lookup_by_id(a.id), Err(Error::IdentityGone(_))),
            "lookup through a recycled id must report IdentityGone"
        );
        assert!(
            matches!(c.open_read_by_id(a.id, 0, 64), Err(Error::IdentityGone(_))),
            "a read through a recycled id must report IdentityGone"
        );
        assert_eq!(c.read("/b", 0, 64).unwrap(), b"B's bytes");
    }

    /// R4.2c acceptance: a write session opened before the delete can never
    /// pass its CAS against a different file — the recreated file's bytes,
    /// version, and object key stay exactly as the new writer left them.
    #[test]
    fn stale_session_commit_must_not_touch_recreated_file() {
        let c = core();
        c.create("/a", 0o644).unwrap();
        let h = c.open_write("/a", None).unwrap();
        c.write(&h, 0, b"A's bytes").unwrap();
        let a = c.commit_write(&h).unwrap();
        let stale = c.open_write("/a", None).unwrap();
        c.write(&stale, 0, b"POISON").unwrap();
        c.unlink("/a").unwrap();
        c.create("/b", 0o644).unwrap();
        let hb = c.open_write("/b", None).unwrap();
        c.write(&hb, 0, b"B's bytes").unwrap();
        let b = c.commit_write(&hb).unwrap();
        assert_ne!(b.id, a.id);
        // The stale session's target no longer exists: the commit fails.
        assert!(c.commit_write(&stale).is_err());
        // B is untouched: bytes, version, object key.
        assert_eq!(c.read("/b", 0, 64).unwrap(), b"B's bytes");
        assert_eq!(b.version, 1);
        let live = c.lookup("/b").unwrap();
        assert_eq!(live.id, b.id);
        assert_eq!(live.object_key, b.object_key, "object key unchanged");
    }

    /// R4.2c: after deleting the highest allocated id, churn (create +
    /// commit + delete, repeatedly) never re-issues an id this database has
    /// already used; closing and reopening the store continues upward and
    /// the surviving data is intact. Plain restarts never reset anything.
    #[test]
    fn ids_never_repeat_within_or_across_restarts() {
        let dir = std::env::temp_dir().join(format!("lightfs-ra-churn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("meta.db");
        let open = |p: &std::path::Path| {
            Core::new(
                Arc::new(SqliteMetadataStore::open(p.to_str().unwrap()).unwrap()),
                Arc::new(MemObjectStore::new()),
            )
        };
        let mut seen = std::collections::BTreeSet::new();
        {
            let c = open(&db);
            // a survivor proves data integrity across the restart below
            c.create("/keep", 0o644).unwrap();
            let hk = c.open_write("/keep", None).unwrap();
            c.write(&hk, 0, b"kept bytes").unwrap();
            let keep = c.commit_write(&hk).unwrap();
            seen.insert(keep.id);
            // churn: every file is deleted at its highest allocated id
            for i in 0..40u32 {
                let name = format!("/f{i}");
                c.create(&name, 0o644).unwrap();
                let h = c.open_write(&name, None).unwrap();
                c.write(&h, 0, format!("bytes {i}").as_bytes()).unwrap();
                let n = c.commit_write(&h).unwrap();
                assert!(seen.insert(n.id), "id {id} reused", id = n.id);
                c.unlink(&name).unwrap();
            }
        }
        {
            // plain restart: guarantees continue, nothing is reset. The
            // in-memory object store starts empty here (real deployments
            // keep objects in S3), so integrity is asserted on metadata.
            let c = open(&db);
            let keep = c.lookup("/keep").unwrap();
            assert_eq!(keep.version, 1);
            assert_eq!(keep.size, 10);
            assert!(
                keep.object_key.is_some(),
                "committed object key survives the restart"
            );
            c.create("/after", 0o644).unwrap();
            let h = c.open_write("/after", None).unwrap();
            c.write(&h, 0, b"after restart").unwrap();
            let n = c.commit_write(&h).unwrap();
            let prev_max = *seen.iter().next_back().unwrap();
            assert!(seen.insert(n.id), "id reused across restart");
            assert!(
                n.id > prev_max,
                "allocation continues above everything seen before the restart"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
