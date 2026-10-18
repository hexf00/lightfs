//! vnext Core: the FsApi-facing engine over the commit/ref metadata
//! model. Strict-commit only: an ack means the data object is stored
//! (envelope-sealed, integrity verifiable) AND the metadata transaction
//! (file version + COW trees + commit + ref advance) has committed.
//!
//! SQLite is the execution layer; content-addressed copies of trees,
//! commits and refs are pushed to the object store AFTER COMMIT as
//! disaster-recovery assets. A failed copy is logged and never fails the
//! commit: SQLite stays authoritative, and the recovery story degrades
//! only for objects that were never copied.

use std::io::Read;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use rusqlite::Connection;

use crate::error::{Error, Result};
use crate::metadata::{Node, NodeKind};
use crate::object_store::{ListPage, ObjectData, ObjectInfo, ObjectStore, Range, StreamedGet};
use crate::vnext::envelope::{self, ObjKind};
use crate::vnext::store::{CommitRow, Entry, FileVersionRow, StorageRef, Store};

pub(crate) const MAIN_REF: &str = "main";

/// Object keys carry NO logical identity: `objects/<object-id>.data`.
/// The object id is a fresh random token per immutable object — it is
/// the Object ID, independent of the writing file's File ID (which does
/// not exist yet at PUT time on the merged create path) and independent
/// of any backend routing. The core never parses this key back.
fn data_key(oid: &str) -> String {
    format!("objects/{oid}.data")
}

/// A fresh Object ID: 32 hex chars folded from pid + wall nanos + a
/// process counter (uniqueness is the requirement, not secrecy).
fn new_object_id() -> String {
    crate::sigv4::sha256_hex(&uuid_bytes()).chars().take(32).collect()
}
/// Loop bound for trailing-symlink resolution (POSIX ELOOP shape).
const MAX_SYMLINK_RESOLUTIONS: usize = 40;

/// Counters the benchmark and /metrics read. vnext counts the data
/// plane plus the metadata transaction that is the consistency anchor.
#[derive(Debug, Default)]
pub struct Metrics {
    pub s3_get: std::sync::atomic::AtomicU64,
    pub s3_put: std::sync::atomic::AtomicU64,
    pub s3_head: std::sync::atomic::AtomicU64,
    pub s3_delete: std::sync::atomic::AtomicU64,
    pub s3_list: std::sync::atomic::AtomicU64,
    pub bytes_read: std::sync::atomic::AtomicU64,
    pub bytes_written: std::sync::atomic::AtomicU64,
    pub conflicts: std::sync::atomic::AtomicU64,
    pub metadata_tx: std::sync::atomic::AtomicU64,
}

/// Metrics choke point: every Core-mediated object-store operation bumps
/// the counters (same contract as the previous core — the in-memory
/// backend counts too; /metrics, the benchmark and read-cache attribution
/// all read these).
struct CountedObjects {
    inner: Arc<dyn ObjectStore>,
    m: Arc<Metrics>,
}

impl ObjectStore for CountedObjects {
    fn backend_id(&self) -> String {
        self.inner.backend_id()
    }
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectInfo> {
        self.m.s3_put.fetch_add(1, Ordering::Relaxed);
        self.m.bytes_written.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.put(key, data)
    }
    fn put_stream(&self, key: &str, reader: &mut dyn Read, len: u64) -> Result<ObjectInfo> {
        self.m.s3_put.fetch_add(1, Ordering::Relaxed);
        self.m.bytes_written.fetch_add(len, Ordering::Relaxed);
        self.inner.put_stream(key, reader, len)
    }
    fn put_if_absent(&self, key: &str, data: &[u8]) -> Result<bool> {
        self.m.s3_put.fetch_add(1, Ordering::Relaxed);
        self.m.bytes_written.fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.put_if_absent(key, data)
    }
    fn supports_atomic_claim(&self) -> bool {
        self.inner.supports_atomic_claim()
    }
    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData> {
        self.m.s3_get.fetch_add(1, Ordering::Relaxed);
        let out = self.inner.get(key, range)?;
        self.m.bytes_read.fetch_add(out.data.len() as u64, Ordering::Relaxed);
        Ok(out)
    }
    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<StreamedGet> {
        self.m.s3_get.fetch_add(1, Ordering::Relaxed);
        let out = self.inner.get_stream(key, range)?;
        self.m.bytes_read.fetch_add(out.len, Ordering::Relaxed);
        Ok(out)
    }
    fn head(&self, key: &str) -> Result<ObjectInfo> {
        self.m.s3_head.fetch_add(1, Ordering::Relaxed);
        self.inner.head(key)
    }
    fn presign_get(&self, key: &str, expires_secs: u64) -> Option<String> {
        self.inner.presign_get(key, expires_secs)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.m.s3_delete.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str, token: Option<&str>, max: usize) -> Result<ListPage> {
        self.m.s3_list.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix, token, max)
    }
    fn ping(&self) -> Result<()> {
        self.inner.ping()
    }
}

pub struct Core {
    pub(crate) store: Arc<Store>,
    pub(crate) objects: Arc<dyn ObjectStore>,
    pub metrics: Arc<Metrics>,
    /// The single-writer engine lock, held for the engine's lifetime.
    _lock: Option<crate::fslock::FileLock>,
}

/// One directory level along a walked path: the tree row id and a
/// working COPY of its entries (immutable rows are never edited in
/// place; the copy becomes the next COW generation).
#[derive(Clone)]
struct Level {
    tid: i64,
    entries: Vec<Entry>,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 16 random-ish bytes for the namespace id: pid + wall nanos + a process
/// counter, folded through sha256. Uniqueness is the requirement (the id
/// is a random ownership token, not a secret); no new dependency.
fn uuid_bytes() -> [u8; 16] {
    use std::sync::atomic::AtomicU64;
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut material = Vec::with_capacity(32);
    material.extend_from_slice(&std::process::id().to_le_bytes());
    material.extend_from_slice(
        &std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
            .to_le_bytes(),
    );
    material.extend_from_slice(&n.to_le_bytes());
    let digest = crate::sigv4::sha256_hex(&material);
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&digest[i * 2..i * 2 + 2], 16).unwrap_or(0);
    }
    out
}


impl Core {
    pub fn new(store: Store, objects: Arc<dyn ObjectStore>) -> Result<Core> {
        Self::new_shared(Arc::new(store), objects)
    }

    /// Core over an ALREADY-shared store (crash-simulation tests reopen
    /// over the same Arc a panicking engine dropped).
    pub fn new_shared(store: Arc<Store>, objects: Arc<dyn ObjectStore>) -> Result<Core> {
        objects.ping()?;
        let metrics = Arc::new(Metrics::default());
        let core = Core {
            store,
            objects: Arc::new(CountedObjects { inner: objects, m: metrics.clone() }),
            metrics,
            _lock: None,
        };
        core.init_namespace()?;
        Ok(core)
    }

    /// Take ownership of the engine lock acquired BEFORE this Core (and
    /// its SQLite store) existed — the S4.14.1 ownership order.
    pub(crate) fn hold_engine_lock(&mut self, lock: crate::fslock::FileLock) {
        self._lock = Some(lock);
    }

    /// Ownership marker, checked and enforced on EVERY open (S4.20): the
    /// object store is payload-only — no per-commit logical metadata — so
    /// bucket ownership rests on ONE namespace marker written at
    /// initialization and re-verified here. SQLite holds the same id.
    ///
    /// S4.21 atomic claim: the marker lands via a REAL create-if-absent
    /// (`ObjectStore::put_if_absent`) — two fresh databases initializing
    /// the same unclaimed namespace race, and exactly one put_if_absent
    /// returns `true`; the loser reads the winner's marker and refuses.
    /// Backends WITHOUT the atomic-claim capability refuse the unsafe
    /// initialization path outright — the core never degrades to
    /// check-then-put (its claim race is exactly what this closes).
    /// Order: SQLite row first, claim second — a crash between the two
    /// fails the NEXT open closed (marker missing, database owns an id)
    /// instead of silently re-rooting. A fresh store that meets a marker
    /// it does not hold is a DIFFERENT namespace: refused, never co-rooted.
    fn init_namespace(&self) -> Result<()> {
        const MARKER_KEY: &str = "namespace/marker";
        let local = self.store.namespace_id()?;
        let remote = match self.objects.get(MARKER_KEY, None) {
            Ok(obj) => {
                let (kind, body) = envelope::open(&obj.data)?;
                if kind != ObjKind::Namespace {
                    return Err(Error::Db(format!(
                        "{MARKER_KEY}: kind {kind:?} is not a namespace marker"
                    )));
                }
                Some(String::from_utf8(body).map_err(|_| {
                    Error::ObjectCorrupt(format!("{MARKER_KEY}: marker id is not UTF-8"))
                })?)
            }
            Err(Error::ObjectMissing(_)) => None,
            Err(e) => return Err(e),
        };
        match (local.as_deref(), remote.as_deref()) {
            (None, None) => {
                if !self.objects.supports_atomic_claim() {
                    return Err(Error::Storage(format!(
                        "vnext: refusing namespace initialization — backend {:?} cannot \
                         atomically claim a namespace (put-if-absent unsupported); an \
                         init there could double-claim under concurrency",
                        self.objects.backend_id()
                    )));
                }
                let id = crate::sigv4::sha256_hex(&uuid_bytes())
                    .chars()
                    .take(32)
                    .collect::<String>();
                self.store
                    .with_tx(|conn| Store::set_namespace_id_tx(conn, &id))?;
                // commit point reached: from here a failed CLAIM leaves
                // (Some(id), None) — the next open refuses closed rather
                // than silently re-rooting (documented crash window).
                let claimed = self.objects.put_if_absent(
                    MARKER_KEY,
                    &envelope::seal(ObjKind::Namespace, id.as_bytes()),
                )?;
                if claimed {
                    Ok(())
                } else {
                    Err(Error::Db(format!(
                        "vnext: namespace was claimed concurrently — the marker on {} \
                         belongs to another database; this store is not the namespace owner",
                        MARKER_KEY
                    )))
                }
            }
            (None, Some(_)) => Err(Error::Db(format!(
                "vnext: bucket already carries another namespace's marker ({MARKER_KEY}); \
                 a fresh store must not co-root with it"
            ))),
            (Some(_), Some(r)) if local.as_deref() == Some(r) => Ok(()),
            (Some(_), Some(r)) => Err(Error::Db(format!(
                "vnext: namespace mismatch — this database owns {}, the bucket marker says {r}",
                local.as_deref().unwrap_or("?")
            ))),
            (Some(l), None) => Err(Error::Db(format!(
                "vnext: namespace marker missing on the object store (database owns {l}). \
                 The bucket may have been cleared or the marker deleted; refusing to \
                 re-root fail-closed — restore the marker object or start a new namespace"
            ))),
        }
    }

    pub fn objects(&self) -> &Arc<dyn ObjectStore> {
        &self.objects
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Shared handle for crash-simulation tests that must keep the store
    /// alive across a panic boundary.
    pub fn store_arc(&self) -> Arc<Store> {
        self.store.clone()
    }

    // ---- path helpers ----

    fn split_path(path: &str) -> Result<Vec<String>> {
        if !path.starts_with('/') {
            return Err(Error::InvalidPath(format!("{path:?}: not absolute")));
        }
        let comps: Vec<String> = path[1..]
            .split('/')
            .filter(|c| !c.is_empty())
            .map(String::from)
            .collect();
        for c in &comps {
            if c == "." || c == ".." {
                return Err(Error::InvalidPath(format!("{path:?}: dot components")));
            }
        }
        Ok(comps)
    }

    fn entry_node(entry: &Entry, parent_tree: i64) -> Node {
        let kind = match entry.k.as_str() {
            "dir" => NodeKind::Dir,
            "symlink" => NodeKind::Symlink,
            _ => NodeKind::File,
        };
        Node {
            id: entry.tid.or(entry.fid).unwrap_or(0),
            parent_id: Some(parent_tree),
            name: entry.n.clone(),
            kind,
            size: entry.s,
            mtime: entry.mt,
            mode: entry.m,
            object_key: entry.o.as_ref().map(|r| r.locator.clone()),
            version: entry.fv.unwrap_or(0),
            etag: entry.e.clone().unwrap_or_default(),
            target: entry.tgt.clone(),
            pack_off: None,
            pack_len: None,
            content_hash: None,
        }
    }

    fn root_node(commit: &CommitRow, tree: i64) -> Node {
        Node {
            id: tree,
            parent_id: None,
            name: "/".into(),
            kind: NodeKind::Dir,
            size: 0,
            mtime: commit.created_at,
            mode: 0o555,
            object_key: None,
            version: 0,
            etag: String::new(),
            target: None,
            pack_off: None,
            pack_len: None,
            content_hash: None,
        }
    }

    fn head_commit(&self) -> Result<CommitRow> {
        let head = self
            .store
            .head(MAIN_REF)?
            .ok_or_else(|| Error::Db("main ref missing".into()))?;
        self.store.get_commit(head.commit_id)
    }

    /// Head commit on a CALLER-HELD connection: mutations must resolve
    /// their base INSIDE the BEGIN IMMEDIATE window, or two concurrent
    /// writers walk the same stale trees and the later commit silently
    /// reverts the earlier one (lost update).
    fn head_commit_tx(conn: &Connection) -> Result<CommitRow> {
        let head = Store::head_tx(conn, MAIN_REF)?
            .ok_or_else(|| Error::Db("main ref missing".into()))?;
        Store::get_commit_tx(conn, head.commit_id)
    }

    /// Walk the head tree root→…→parent-dir of `path` (every component
    /// resolved as a directory).
    fn walk_dirs(&self, comps: &[String]) -> Result<Vec<Level>> {
        let commit = self.head_commit()?;
        self.walk_dirs_from(commit.root_tree_id, comps)
    }

    /// The mutation-path walk: on a CALLER-HELD connection (inside the
    /// transaction), so the base trees a COW starts from are the ones the
    /// write lock observed — never a snapshot from before another writer
    /// committed.
    fn walk_dirs_tx(&self, conn: &Connection, comps: &[String]) -> Result<Vec<Level>> {
        let commit = Self::head_commit_tx(conn)?;
        self.walk_dirs_from_tx(conn, commit.root_tree_id, comps)
    }

    fn walk_dirs_from_tx(
        &self,
        conn: &Connection,
        root: i64,
        comps: &[String],
    ) -> Result<Vec<Level>> {
        let mut chain = vec![Level { tid: root, entries: Store::get_tree_tx(conn, root)? }];
        for comp in comps {
            let cur = chain.last().expect("≥1");
            let entry = cur
                .entries
                .iter()
                .find(|e| &e.n == comp)
                .ok_or_else(|| Error::NotFound(format!("/{}/", comps.join("/"))))?;
            let child_tid = entry.tid.ok_or_else(|| Error::KindMismatch {
                path: comp.clone(),
                expected: "dir",
                actual: entry.k.clone(),
            })?;
            chain.push(Level {
                tid: child_tid,
                entries: Store::get_tree_tx(conn, child_tid)?,
            });
        }
        Ok(chain)
    }

    /// Same walk from an EXPLICIT root tree — the pinned-read path: a
    /// commit's tree is immutable, so history never moves under a read.
    fn walk_dirs_from(&self, root_tree: i64, comps: &[String]) -> Result<Vec<Level>> {
        let mut chain = vec![Level {
            tid: root_tree,
            entries: self.store.get_tree(root_tree)?,
        }];
        for comp in comps {
            let cur = chain.last().expect("root present");
            let entry = cur
                .entries
                .iter()
                .find(|e| e.n == *comp)
                .ok_or_else(|| Error::NotFound(comp.clone()))?;
            let child_tid = entry.tid.ok_or_else(|| Error::KindMismatch {
                path: comp.clone(),
                expected: "dir",
                actual: entry.k.clone(),
            })?;
            chain.push(Level {
                tid: child_tid,
                entries: self.store.get_tree(child_tid)?,
            });
        }
        Ok(chain)
    }

    /// The shared mutation pipeline for single-subtree operations.
    /// `step` runs INSIDE the BEGIN IMMEDIATE transaction with a working
    /// copy of the target directory's entries; afterwards the COW chain
    /// is built bottom-up, the commit appended and `main` advanced —
    /// atomically. Recovery copies publish after COMMIT (best-effort).
    fn mutate<T>(
        &self,
        comps: &[String],
        step: impl FnOnce(&Connection, &mut Vec<Entry>) -> Result<T>,
    ) -> Result<T> {
        // S4.20: the transaction IS the whole commit — no object-store
        // metadata publication after it. The COW chain lives only in
        // SQLite; the data plane holds immutable payload objects, and the
        // namespace marker (checked at open) owns bucket identity.
        self.store.with_tx(|conn| {
            // Walk INSIDE the write transaction: base trees are read under
            // BEGIN IMMEDIATE, so serialized writers can never build on a
            // stale snapshot (no silent lost updates).
            let mut chain = self.walk_dirs_tx(conn, comps)?;
            let result = {
                let leaf = chain.last_mut().expect("walk yields ≥1");
                step(conn, &mut leaf.entries)?
            };
            Self::cow_and_commit(conn, &mut chain)?;
            self.metrics
                .metadata_tx
                .fetch_add(1, Ordering::Relaxed);
            Ok(result)
        })
    }

    /// Bottom-up COW over `chain` + commit + ref advance. Entries of the
    /// leaf (and of any level a caller adjusted) are already mutated;
    /// child pointers are rewired by matching the OLD tree id (within a
    /// directory, at most one entry references a given child tree).
    fn cow_and_commit(conn: &Connection, chain: &mut [Level]) -> Result<()> {
        let (last_id, last_sha, _) = Self::cow_branch(conn, chain)?;
        Store::commit_mainline_tx(conn, MAIN_REF, last_id, &last_sha)?;
        Ok(())
    }

    /// Bottom-up COW over `chain` WITHOUT appending the commit. Returns
    /// `(new_top_id, new_top_sha, old_top_tid)` so a caller splicing this
    /// branch under a shared prefix can rewire the parent entry by the old
    /// tree id. `chain` must be non-empty.
    fn cow_branch(conn: &Connection, chain: &mut [Level]) -> Result<(i64, String, i64)> {
        let top_old = chain[0].tid;
        let mut last_id = 0;
        let mut last_sha = String::new();
        for idx in (0..chain.len()).rev() {
            let json = serde_json::to_string(&chain[idx].entries)
                .map_err(|e| Error::Db(format!("serialize tree: {e}")))?;
            let (id, sha) = Store::insert_tree_json_tx(conn, &json)?;
            if idx > 0 {
                let old_child = chain[idx].tid;
                let rewired = chain[idx - 1]
                    .entries
                    .iter_mut()
                    .find(|e| e.tid == Some(old_child));
                match rewired {
                    Some(e) => {
                        e.tid = Some(id);
                        e.w = Some(sha.clone());
                    }
                    None => {
                        return Err(Error::Db(format!(
                            "cow: level {} lost the pointer to tree {old_child}",
                            idx - 1
                        )))
                    }
                }
            }
            last_id = id;
            last_sha = sha;
        }
        Ok((last_id, last_sha, top_old))
    }

    // ---- reads ----

    pub fn lookup(&self, path: &str) -> Result<Node> {
        let comps = Self::split_path(path)?;
        if comps.is_empty() {
            let commit = self.head_commit()?;
            return Ok(Self::root_node(&commit, commit.root_tree_id));
        }
        // LSTAT semantics: the final component is NOT followed (a
        // symlink's own node comes back, target riding along); only
        // open/read/readdir/write sessions follow it.
        let parent_chain = self.walk_dirs(&comps[..comps.len() - 1])?;
        let parent = parent_chain.last().expect("≥1");
        let name = comps.last().expect("non-empty");
        let entry = parent
            .entries
            .iter()
            .find(|e| e.n == *name)
            .ok_or_else(|| Error::NotFound(path.to_string()))?;
        Ok(Self::entry_node(entry, parent.tid))
    }

    pub fn open(&self, path: &str) -> Result<Node> {
        let (entry, parent_tid) = self.resolve_follow(path)?;
        Ok(Self::entry_node(&entry, parent_tid))
    }

    pub fn readdir(&self, path: &str) -> Result<Vec<Node>> {
        let comps = Self::split_path(path)?;
        let (tid, entries) = if comps.is_empty() {
            let commit = self.head_commit()?;
            (commit.root_tree_id, self.store.get_tree(commit.root_tree_id)?)
        } else {
            let (entry, _) = self.resolve_follow(path)?;
            let tid = entry.tid.ok_or_else(|| Error::KindMismatch {
                path: path.to_string(),
                expected: "dir",
                actual: entry.k.clone(),
            })?;
            (tid, self.store.get_tree(tid)?)
        };
        let mut out: Vec<Node> = entries.iter().map(|e| Self::entry_node(e, tid)).collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    fn resolve_entry(&self, path: &str) -> Result<Entry> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        let chain = self.walk_dirs(&comps[..comps.len() - 1])?;
        let parent = chain.last().expect("≥1");
        parent
            .entries
            .iter()
            .find(|e| e.n == *name)
            .cloned()
            .ok_or_else(|| Error::NotFound(path.to_string()))
    }

    /// Final-component symlink resolution (POSIX follow semantics):
    /// lookup/open/read/readdir and write sessions follow a trailing
    /// symlink; unlink/rename/readlink never do. Intermediate components
    /// are resolved literally (vnext slice edge, documented).
    fn resolve_follow(&self, path: &str) -> Result<(Entry, i64)> {
        let mut cur = path.to_string();
        let mut budget = MAX_SYMLINK_RESOLUTIONS;
        loop {
            let comps = Self::split_path(&cur)?;
            let name = comps
                .last()
                .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
            let chain = self.walk_dirs(&comps[..comps.len() - 1])?;
            let parent = chain.last().expect("≥1");
            let entry = parent
                .entries
                .iter()
                .find(|e| e.n == *name)
                .cloned()
                .ok_or_else(|| Error::NotFound(path.to_string()))?;
            if entry.k != "symlink" {
                return Ok((entry, parent.tid));
            }
            budget -= 1;
            if budget == 0 {
                return Err(Error::InvalidArgument(format!(
                    "too many symlink resolutions: {path}"
                )));
            }
            let dir = match cur.rfind('/') {
                Some(0) | None => "/".to_string(),
                Some(i) => cur[..i].to_string(),
            };
            cur = Self::resolve_link_target(&dir, entry.tgt.as_deref().unwrap_or(""))?;
        }
    }

    /// lexical symlink-target resolution against the link's directory
    /// (`..` clamps at root, never escapes the namespace).
    fn resolve_link_target(link_dir: &str, target: &str) -> Result<String> {
        if target.is_empty() || target.contains('\0') {
            return Err(Error::InvalidArgument("empty symlink target".into()));
        }
        let mut out: Vec<&str> = if target.starts_with('/') {
            Vec::new()
        } else {
            link_dir.split('/').filter(|c| !c.is_empty()).collect()
        };
        for comp in target.split('/') {
            match comp {
                "" | "." => {}
                ".." => {
                    out.pop(); // clamp at root: pop on empty stays empty
                }
                other => out.push(other),
            }
        }
        Ok(format!("/{}", out.join("/")))
    }

    /// Locate a file's CURRENT head entry by stable id: fast path via
    /// the path hint, then a full-tree DFS (the file may have been
    /// renamed since the handle was opened).
    fn locate_by_id(&self, file_id: i64, path_hint: &str) -> Result<Entry> {
        if let Ok(entry) = self.resolve_entry(path_hint) {
            if entry.fid == Some(file_id) {
                return Ok(entry);
            }
        }
        let commit = self.head_commit()?;
        let mut stack = vec![commit.root_tree_id];
        while let Some(tid) = stack.pop() {
            for e in self.store.get_tree(tid)? {
                match (e.k.as_str(), e.tid) {
                    ("dir", Some(child)) => stack.push(child),
                    _ if e.fid == Some(file_id) => return Ok(e),
                    _ => {}
                }
            }
        }
        Err(Error::IdentityGone(format!("file id {file_id} no longer exists")))
    }

    /// Reconstruct the head path of a file id (DFS).
    fn path_of(&self, file_id: i64) -> Result<Option<String>> {
        let commit = self.head_commit()?;
        let mut stack = vec![(commit.root_tree_id, String::new())];
        while let Some((tid, prefix)) = stack.pop() {
            for e in self.store.get_tree(tid)? {
                let path = if prefix.is_empty() {
                    format!("/{}", e.n)
                } else {
                    format!("{prefix}/{}", e.n)
                };
                match (e.k.as_str(), e.tid) {
                    ("dir", Some(child)) => stack.push((child, path)),
                    _ if e.fid == Some(file_id) => return Ok(Some(path)),
                    _ => {}
                }
            }
        }
        Ok(None)
    }

    pub fn lookup_by_id(&self, file_id: i64) -> Result<Node> {
        let entry = self.locate_by_id(file_id, "")?;
        Ok(Self::entry_node(&entry, 0))
    }

    // ---- content reads (envelope-verified) ----

    /// Range read: ONE ranged GET carries the envelope header and the
    /// payload together (cost contract: exactly one object GET per cold
    /// read), header verified, etag compared. The v2 checksum lives in the
    /// FOOTER, which a partial read does not fetch — payload integrity for
    /// a range read rests on the recorded etag comparison; full reads
    /// verify the checksum via `read_full_verified`.
    ///
    /// `offset`/`size` are in LOGICAL file space; the envelope layer owns
    /// the logical→payload window mapping, which is the identity because
    /// this build reads raw encoding only.
    fn read_range_verified(
        &self,
        key: &str,
        recorded_etag: &str,
        recorded_size: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        if offset >= recorded_size || size == 0 {
            return Ok(Vec::new());
        }
        let want = size.min(recorded_size - offset);
        // Cold reads start at 0 (sequential scan, open-head): one ranged
        // GET carries header+payload (cost contract: one object GET per
        // such read). Deeper ranges need the header first, then the
        // payload window — two small GETs, still O(range) bytes.
        if offset == 0 {
            let obj = self.objects.get(key, Some(envelope::prefix_range(want)))?;
            if !recorded_etag.is_empty() && obj.info.etag != recorded_etag {
                return Err(Error::ObjectCorrupt(format!(
                    "{key}: recorded etag {recorded_etag} != observed {}",
                    obj.info.etag
                )));
            }
            let (kind, logical) = envelope::open_header(&obj.data)?;
            if kind != ObjKind::Data {
                return Err(Error::ObjectCorrupt(format!("{key}: not a data object")));
            }
            if logical < want {
                return Err(Error::ObjectCorrupt(format!(
                    "{key}: logical length {logical} shorter than the requested read"
                )));
            }
            return Ok(envelope::payload_slice(&obj.data, want as usize).to_vec());
        }
        let head = self.objects.get(key, Some(envelope::prefix_range(0)))?;
        if !recorded_etag.is_empty() && head.info.etag != recorded_etag {
            return Err(Error::ObjectCorrupt(format!(
                "{key}: recorded etag {recorded_etag} != observed {}",
                head.info.etag
            )));
        }
        let (kind, logical) = envelope::open_header(&head.data)?;
        if kind != ObjKind::Data {
            return Err(Error::ObjectCorrupt(format!("{key}: not a data object")));
        }
        if offset >= logical {
            return Ok(Vec::new());
        }
        let len = size.min(logical - offset);
        if len == 0 {
            return Ok(Vec::new());
        }
        let body = self.objects.get(key, Some(envelope::payload_range(offset, len)))?;
        if !recorded_etag.is_empty() && body.info.etag != recorded_etag {
            return Err(Error::ObjectCorrupt(format!("{key}: etag changed mid-read")));
        }
        Ok(body.data)
    }

    /// Full read with full digest verification, streamed (S4.24): ONE
    /// GET whose own response metadata carries the validator, a
    /// single-pass decode into a caller-visible `Vec`. O(file) memory AT
    /// THIS BOUNDARY by explicit choice of the Vec return — the server's
    /// full-read path uses [`Self::read_full_verified_stream`] instead
    /// and never materializes the payload in the heap.
    pub fn read_full_verified(&self, key: &str, recorded_etag: &str) -> Result<Vec<u8>> {
        let mut vo = self.read_full_verified_stream(key, recorded_etag)?;
        let mut out = Vec::with_capacity(vo.payload_len as usize);
        vo.reader.read_to_end(&mut out)?;
        Ok(out)
    }

    /// Verified full read, bounded memory (S4.24): the object is
    /// stream-decoded into a PRIVATE temp file and published only after
    /// the footer verified — the returned reader is the validated file.
    /// The validator comparison uses the etag carried by the SAME GET
    /// response (no HEAD, no two-request version assumption); an empty
    /// recorded etag skips the comparison, exactly as before.
    ///
    /// Temp cost: one file of `payload_len` bytes in `std::env::temp_dir`
    /// for the lifetime of the returned value; deleted on drop, on any
    /// verification failure, on short read, on IO error, on cancellation.
    pub fn read_full_verified_stream(
        &self,
        key: &str,
        recorded_etag: &str,
    ) -> Result<VerifiedObjectRead> {
        let mut sg = self.objects.get_stream(key, None)?;
        if !recorded_etag.is_empty() && sg.etag != recorded_etag {
            return Err(Error::ObjectCorrupt(format!(
                "{key}: recorded etag {recorded_etag} != observed {}",
                sg.etag
            )));
        }
        let mut staged = TempFileGuard::create()
            .map_err(|e| Error::Storage(format!("staging file: {e}")))?;
        let decoded = {
            use std::io::Write as _;
            let mut file = staged.file.take().expect("fresh guard holds a file");
            let d = envelope::decode_stream(&mut sg.reader, &mut file, Some(ObjKind::Data));
            // hand the file back regardless; flush only on success
            let flushed = match &d {
                Ok(_) => file.flush().err(),
                Err(_) => None,
            };
            staged.file = Some(file);
            match (d, flushed) {
                (Ok(d), None) => Ok(d),
                (Err(e), _) => Err(e),
                (Ok(_), Some(io)) => Err(Error::Io(io.to_string())),
            }
        }?;
        let payload_len = decoded.payload_len;
        let mut file = staged.file.take().expect("guard still holds a file");
        use std::io::Seek;
        file.rewind()?;
        Ok(VerifiedObjectRead {
            reader: file,
            payload_len,
            guard: staged,
        })
    }

    pub fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        let (entry, _) = self.resolve_follow(path)?;
        if entry.k != "file" {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: entry.k,
            });
        }
        let r = entry
            .o
            .ok_or_else(|| Error::ObjectMissing(format!("{path}: file has no content yet")))?;
        let key = self.check_ref(&r)?;
        self.read_range_verified(
            key,
            entry.e.as_deref().unwrap_or(""),
            entry.s.max(0) as u64,
            offset,
            size,
        )
    }

    pub fn read_by_id(&self, file_id: i64, offset: u64, size: u64) -> Result<Vec<u8>> {
        let entry = self.locate_by_id(file_id, "")?;
        let r = entry
            .o
            .ok_or_else(|| Error::ObjectMissing(format!("file {file_id}: no content yet")))?;
        let key = self.check_ref(&r)?;
        self.read_range_verified(
            key,
            entry.e.as_deref().unwrap_or(""),
            entry.s.max(0) as u64,
            offset,
            size,
        )
    }

    /// Resolve a persisted reference against THIS engine's backend. A
    /// reference bound to a different backend is refused fail-closed —
    /// handing it to the wrong store would either miss or (worse) hit a
    /// same-named foreign locator. This is the seam a future router plugs
    /// into; today there is exactly one store, so any mismatch is an
    /// unrecoverable configuration change and must be loud.
    fn check_ref<'a>(&self, r: &'a StorageRef) -> Result<&'a str> {
        let bound = self.objects.backend_id();
        if r.backend != bound {
            return Err(Error::Storage(format!(
                "object reference belongs to backend {:?} but this engine is bound to \
                 {bound:?}; refusing to read across backends (routing changed without \
                 migration?)",
                r.backend
            )));
        }
        Ok(&r.locator)
    }

    /// Server read entry (S4.24 routing): a request that covers the
    /// WHOLE file from offset 0 is served from the footer-verified
    /// streamed decode (bounded memory, content published only after the
    /// checksum passed). Range requests keep the exact range contract —
    /// same GET shapes, same offset math, no footer fetch, O(range)
    /// bytes; their payload integrity rests on the recorded etag exactly
    /// as before.
    pub fn open_read(
        &self,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let (entry, _) = self.resolve_follow(path)?;
        if entry.k != "file" {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: entry.k,
            });
        }
        let r = entry
            .o
            .ok_or_else(|| Error::ObjectMissing(format!("{path}: file has no content yet")))?;
        let key = self.check_ref(&r)?;
        self.open_read_ref(
            key,
            entry.e.as_deref().unwrap_or(""),
            entry.s.max(0) as u64,
            offset,
            size,
        )
    }

    pub fn open_read_by_id(
        &self,
        file_id: i64,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let entry = self.locate_by_id(file_id, "")?;
        let r = entry
            .o
            .ok_or_else(|| Error::ObjectMissing(format!("file {file_id}: no content yet")))?;
        let key = self.check_ref(&r)?;
        self.open_read_ref(
            key,
            entry.e.as_deref().unwrap_or(""),
            entry.s.max(0) as u64,
            offset,
            size,
        )
    }

    /// Shared decision body for both open_read variants.
    fn open_read_ref(
        &self,
        key: &str,
        recorded_etag: &str,
        recorded_size: u64,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        if offset == 0 && recorded_size > 0 && size >= recorded_size {
            // WHOLE-file request: one full GET (the range path fetched the
            // same bytes anyway), footer verified BEFORE anything is
            // served, streamed out of the validated staging file. Same
            // request count as before; 32 more bytes on the wire.
            let vo = self.read_full_verified_stream(key, recorded_etag)?;
            let len = vo.payload_len;
            return Ok((Box::new(vo), len));
        }
        let data = self.read_range_verified(key, recorded_etag, recorded_size, offset, size)?;
        let len = data.len() as u64;
        Ok((Box::new(std::io::Cursor::new(data)), len))
    }

    // ---- namespace mutations ----

    pub fn mkdir(&self, path: &str, mode: u32) -> Result<Node> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        self.mutate(&comps[..comps.len() - 1], |conn, entries| {
            if entries.iter().any(|e| e.n == name) {
                return Err(Error::AlreadyExists(path.to_string()));
            }
            let (child_tid, child_sha) = Store::insert_tree_json_tx(conn, "[]")?;
            entries.push(Entry {
                n: name,
                k: "dir".into(),
                m: mode,
                s: 0,
                mt: now_secs(),
                tid: Some(child_tid),
                w: Some(child_sha),
                fid: None,
                fv: None,
                o: None,
                e: None,
                tgt: None,
            });
            Ok(())
        })?;
        self.lookup(path)
    }

    pub fn create(&self, path: &str, mode: u32) -> Result<Node> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        self.mutate(&comps[..comps.len() - 1], |conn, entries| {
            if entries.iter().any(|e| e.n == name) {
                return Err(Error::AlreadyExists(path.to_string()));
            }
            let file_id = Store::alloc_file_id_tx(conn)?;
            entries.push(Entry {
                n: name,
                k: "file".into(),
                m: mode,
                s: 0,
                mt: now_secs(),
                tid: None,
                w: None,
                fid: Some(file_id),
                fv: Some(0),
                o: None,
                e: None,
                tgt: None,
            });
            Ok(())
        })?;
        self.lookup(path)
    }

    pub fn symlink(&self, target: &str, path: &str) -> Result<Node> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        // The link is returned UNRESOLVED — a dangling target is legal
        // (POSIX), so a follow-lookup here would report NotFound.
        let entry = self.mutate(&comps[..comps.len() - 1], |conn, entries| {
            if entries.iter().any(|e| e.n == name) {
                return Err(Error::AlreadyExists(path.to_string()));
            }
            let file_id = Store::alloc_file_id_tx(conn)?;
            let e = Entry {
                n: name,
                k: "symlink".into(),
                m: 0o777,
                s: target.len() as i64,
                mt: now_secs(),
                tid: None,
                w: None,
                fid: Some(file_id),
                fv: None,
                o: None,
                e: None,
                tgt: Some(target.to_string()),
            };
            entries.push(e.clone());
            Ok(e)
        })?;
        Ok(Self::entry_node(&entry, 0))
    }

    pub fn readlink(&self, path: &str) -> Result<String> {
        let entry = self.resolve_entry(path)?;
        match entry.k.as_str() {
            "symlink" => entry
                .tgt
                .ok_or_else(|| Error::Db(format!("{path}: symlink without target"))),
            other => Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "symlink",
                actual: other.to_string(),
            }),
        }
    }

    pub fn unlink(&self, path: &str) -> Result<()> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        self.mutate(&comps[..comps.len() - 1], |_conn, entries| {
            let entry = entries
                .iter()
                .find(|e| e.n == name)
                .ok_or_else(|| Error::NotFound(path.to_string()))?;
            if entry.k == "dir" {
                return Err(Error::KindMismatch {
                    path: path.to_string(),
                    expected: "file",
                    actual: "dir".into(),
                });
            }
            entries.retain(|e| e.n != name);
            Ok(())
        })?;
        Ok(())
    }

    pub fn rmdir(&self, path: &str) -> Result<()> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        self.mutate(&comps[..comps.len() - 1], |conn, entries| {
            let entry = entries
                .iter()
                .find(|e| e.n == name)
                .ok_or_else(|| Error::NotFound(path.to_string()))?;
            if entry.k != "dir" {
                return Err(Error::KindMismatch {
                    path: path.to_string(),
                    expected: "dir",
                    actual: entry.k.clone(),
                });
            }
            let child = match entry.tid {
                // same-connection read: with_tx already holds the store
                // mutex, so Store::get_tree (which re-locks) would deadlock.
                Some(t) => Store::get_tree_tx(conn, t)?,
                None => Vec::new(),
            };
            if !child.is_empty() {
                return Err(Error::DirectoryNotEmpty(path.to_string()));
            }
            entries.retain(|e| e.n != name);
            Ok(())
        })?;
        Ok(())
    }

    pub fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        self.mutate(&comps[..comps.len() - 1], |_conn, entries| {
            let entry = entries
                .iter_mut()
                .find(|e| e.n == name)
                .ok_or_else(|| Error::NotFound(path.to_string()))?;
            if let Some(m) = mode {
                entry.m = m;
            }
            if let Some(t) = mtime {
                entry.mt = t;
            }
            Ok(())
        })?;
        self.lookup(path)
    }

    pub fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        let old_comps = Self::split_path(old_path)?;
        let new_comps = Self::split_path(new_path)?;
        let old_name = old_comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(old_path.to_string()))?;
        let new_name = new_comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(new_path.to_string()))?;
        // Parents only: the leaves are ENTRIES in the last level (same
        // contract as every other mutation — never walk a leaf as a dir).
        self.store.with_tx(|conn| {
            // Both chains are walked INSIDE the transaction (same
            // lost-update discipline as every other mutation), and the
            // cycle check runs on the freshly read chains.
            let mut old_chain =
                self.walk_dirs_tx(conn, &old_comps[..old_comps.len() - 1])?;
            let mut new_chain =
                self.walk_dirs_tx(conn, &new_comps[..new_comps.len() - 1])?;

            let old_entry = old_chain
                .last()
                .expect("≥1")
                .entries
                .iter()
                .find(|e| e.n == old_name)
                .ok_or_else(|| Error::NotFound(old_path.to_string()))?
                .clone();
            if let Some(old_dir_tid) = old_entry.tid {
                for level in &new_chain {
                    if level.tid == old_dir_tid {
                        return Err(Error::Cycle(format!(
                            "rename {old_path} -> {new_path}: target is inside the source directory"
                        )));
                    }
                }
            }

            // Branch point of the two PARENT chains (identical prefix,
            // then divergent levels). `k == len` on one side means that
            // side's leaf IS the shared prefix's last level.
            let split_at = old_chain
                .iter()
                .zip(new_chain.iter())
                .position(|(a, b)| a.tid != b.tid)
                .unwrap_or_else(|| old_chain.len().min(new_chain.len()));
            let same_leaf_dir = split_at == old_chain.len() && split_at == new_chain.len();

            // detach: remove the entry from the old leaf's parent
            let moved = {
                let leaf = old_chain.last_mut().expect("walk yields ≥1");
                let pos = leaf
                    .entries
                    .iter()
                    .position(|e| e.n == old_name)
                    .ok_or_else(|| Error::NotFound(old_path.to_string()))?;
                leaf.entries.remove(pos)
            };
            // attach: insert under the new name in the new leaf's parent
            {
                // when both leaves share one directory, that directory is
                // old_chain's last level — mutate THAT copy only.
                let target = if same_leaf_dir {
                    old_chain.last_mut().unwrap()
                } else {
                    new_chain.last_mut().unwrap()
                };
                if !same_leaf_dir || moved.n != new_name {
                    if let Some(existing) = target.entries.iter().find(|e| e.n == new_name) {
                        if existing.k == "dir" {
                            return Err(Error::KindMismatch {
                                path: new_path.to_string(),
                                expected: "file",
                                actual: "dir".into(),
                            });
                        }
                        // POSIX rename overwrites an existing FILE.
                        // Renaming a DIRECTORY over an existing file is
                        // ENOTDIR; over an empty dir is allowed by
                        // POSIX but v1 keeps it simple: dir-over-anything
                        // is refused (documented vnext edge).
                        if moved.k == "dir" {
                            return Err(Error::KindMismatch {
                                path: new_path.to_string(),
                                expected: "dir target empty/nonexistent",
                                actual: existing.k.clone(),
                            });
                        }
                    }
                    target.entries.retain(|e| e.n != new_name);
                }
                let mut m = moved;
                m.n = new_name;
                target.entries.push(m);
            }
            if same_leaf_dir {
                // one directory changed: plain linear cow
                return Self::cow_and_commit(conn, &mut old_chain);
            }
            // Two branches hanging off a shared prefix: cow each branch
            // bottom-up, splice both new tops into the prefix's last level
            // (the chain copies share tids, so either copy carries it),
            // then cow the prefix + commit.
            if old_chain.len() > split_at {
                let (id, sha, old_top) = Self::cow_branch(conn, &mut old_chain[split_at..])?;
                let top = &mut old_chain[split_at - 1];
                let slot = top
                    .entries
                    .iter_mut()
                    .find(|e| e.tid == Some(old_top))
                    .ok_or_else(|| Error::Db("rename: prefix lost the old branch".into()))?;
                slot.tid = Some(id);
                slot.w = Some(sha);
            }
            if new_chain.len() > split_at {
                let (id, sha, new_top) = Self::cow_branch(conn, &mut new_chain[split_at..])?;
                let top = &mut old_chain[split_at - 1];
                let slot = top
                    .entries
                    .iter_mut()
                    .find(|e| e.tid == Some(new_top))
                    .ok_or_else(|| Error::Db("rename: prefix lost the new branch".into()))?;
                slot.tid = Some(id);
                slot.w = Some(sha);
            }
            let out = Self::cow_and_commit(conn, &mut old_chain[..split_at]);
            self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
            out
        })
    }

    // ---- writes ----

    pub fn open_write(
        &self,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<WriteHandle> {
        let (entry, _) = self.resolve_follow(path)?;
        if entry.k != "file" {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: entry.k,
            });
        }
        let file_id = entry
            .fid
            .ok_or_else(|| Error::Db("file entry without identity".into()))?;
        let expected = expected_version.unwrap_or_else(|| entry.fv.unwrap_or(0));
        Ok(WriteHandle::new(file_id, path, expected))
    }

    pub fn open_write_id(
        &self,
        file_id: i64,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<WriteHandle> {
        let entry = self.locate_by_id(file_id, path)?;
        let expected = expected_version.unwrap_or_else(|| entry.fv.unwrap_or(0));
        Ok(WriteHandle::new(file_id, path, expected))
    }

    pub fn write(&self, handle: &WriteHandle, offset: u64, data: &[u8]) -> Result<usize> {
        handle.write(offset, data)
    }

    /// Commit a write session: data PUT first (envelope-sealed, integrity
    /// digest inside), then ONE metadata transaction — S4.22: the object
    /// REGISTRATION lives in that same transaction (the separate
    /// autocommit ledger write is gone; one SQLite write transaction per
    /// successful content commit). A file-level version conflict or any
    /// other confirmed logical failure rolls the transaction back AND
    /// re-registers the uploaded object in an INDEPENDENT transaction, so
    /// gc can prove-and-reclaim it; a failed or interrupted
    /// re-registration leaves the object UNREGISTERED, which gc never
    /// deletes (fail-safe residue, never risk-deleted for cleanup).
    pub fn commit_write(&self, handle: &WriteHandle) -> Result<Node> {
        // Stream-encode straight out of the locked session buffer: no
        // snapshot clone, no whole-envelope Vec. The buffer itself stays
        // O(file size) — this is the interactive-write path; the bounded
        // path is server spool + write_stream (documented, not hidden).
        let key = data_key(&new_object_id());
        let buf = handle.buf.lock().expect("write buffer");
        let len = buf.len();
        let mut src = std::io::Cursor::new(&buf[..]);
        let mut enc = envelope::seal_reader(ObjKind::Data, &mut src, len as u64);
        let info = self
            .objects
            .put_stream(&key, &mut enc, envelope::encoded_len(len as u64))?;
        drop(buf);

        let comps = Self::split_path(&handle.path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(handle.path.clone()))?;
        let file_id = handle.file_id;
        let expected = handle.expected_version;
        let version = expected + 1;
        let (mtime, size, etag) = (now_secs(), len as i64, info.etag.clone());
        let backend = self.objects.backend_id();
        let key2 = key.clone();
        let result = self.mutate(&comps[..comps.len() - 1], |conn, entries| {
            // Registration is INSIDE the commit transaction: ledger row
            // and file reference commit atomically or not at all.
            Store::ledger_object_tx(conn, &backend, &key)?;
            Store::insert_file_version_tx(conn, &FileVersionRow {
                file_id,
                version,
                size,
                mtime,
                mode: 0o644,
                object_backend: backend.clone(),
                object_key: key2.clone(),
                etag: etag.clone(),
            })?;
            let entry = entries
                .iter_mut()
                .find(|e| e.n == name);
            let entry = match entry {
                Some(e) if e.fid == Some(file_id) => e,
                _ => {
                    // Path changed hands or was renamed mid-session:
                    // relocate by stable id within the head tree.
                    let by_id = self.locate_by_id(file_id, &handle.path)?;
                    entries
                        .iter_mut()
                        .find(|e| e.n == by_id.n && e.fid == Some(file_id))
                        .ok_or_else(|| Error::IdentityGone(format!("file {file_id}")))?
                }
            };
            if entry.fv.unwrap_or(0) != expected {
                return Err(Error::Conflict {
                    path: handle.path.clone(),
                    expected,
                    actual: entry.fv.unwrap_or(0),
                });
            }
            entry.fv = Some(version);
            entry.o = Some(StorageRef::new(&backend, &key));
            entry.e = Some(info.etag);
            entry.s = size;
            entry.mt = mtime;
            Ok(())
        });
        Self::reregister_uploaded_object_on_failure(self, &backend, &key, &result);
        result?;
        self.lookup(&handle.path)
    }

    /// S4.22 failure contract: after a successful PUT whose logical
    /// commit did NOT succeed (confirmed conflict, uncertain commit,
    /// anything), the uploaded object is re-registered in an INDEPENDENT
    /// transaction so reachability gc can reclaim it. The ORIGINAL error
    /// is always returned unchanged — cleanup convenience never masks
    /// the caller's truth. If re-registration itself fails, that is
    /// logged as a diagnostic and the object stays UNREGISTERED: gc
    /// provably keeps objects without a ledger row, so the residue is
    /// safe, not destructive.
    fn reregister_uploaded_object_on_failure<T>(
        core: &Core,
        backend: &str,
        key: &str,
        result: &Result<T>,
    ) {
        if result.is_ok() {
            return;
        }
        if let Err(le) = core.store.ledger_object(backend, key) {
            eprintln!(
                "lightfs: WARNING orphan re-registration failed for {key} (object kept \
                 UNREGISTERED — gc never deletes without a ledger row): {le}"
            );
        }
    }

    /// Server streaming write (S4.21 merged path): a new-file write is
    /// ONE upload plus ONE SQLite transaction — create, File ID
    /// allocation, file version and object reference all land in the same
    /// BEGIN IMMEDIATE window as the COW commit. The old shape committed
    /// an empty-file `create` first (a second fsync'd transaction and an
    /// extra tree per logical write); that separate commit is gone.
    ///
    /// Ordering contract: the data PUT completes BEFORE the transaction
    /// opens, so SQLite never holds its write lock across an upload. A
    /// crash after the PUT leaves an unregistered (or ledgered) object
    /// that reachability gc handles fail-closed — never a half-created
    /// file. Path conflicts (the entry changed between the advisory
    /// pre-check and the transaction) are re-judged INSIDE the
    /// transaction; the pre-check only avoids wasted uploads.
    ///
    /// Memory shape (S4.23): the envelope is encoded in a SINGLE streaming
    /// pass — header, payload chunks (checksum updated in flight), footer —
    /// and handed straight to `put_stream`, which uploads in bounded
    /// buffers (single PUT ≤8 MiB objects, 8 MiB parts above). The full
    /// encoded object never exists in memory and the source is never
    /// rewound, so the reader no longer needs to be seekable (the server
    /// still spools to a temp file for HTTP framing and declared-length
    /// validation).
    ///
    /// `len` is the CALLER-declared logical file length and is stored as
    /// such (entry size, file_versions.size). This build's server writes
    /// raw encoding only, so it is also the envelope payload length; a
    /// future trusted-client encoder owns any other relationship.
    pub fn write_stream(
        &self,
        path: &str,
        expected_version: Option<i64>,
        create: bool,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<Node> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        // Advisory pre-check ONLY (fewer wasted uploads, early kind and
        // NotFound errors): every real decision is re-made inside the
        // transaction.
        match self.resolve_entry(path).ok().map(|e| e.k) {
            Some(k) if k != "file" => {
                return Err(Error::KindMismatch {
                    path: path.to_string(),
                    expected: "file",
                    actual: k,
                });
            }
            Some(_) => {}
            None if !create => return Err(Error::NotFound(path.to_string())),
            None => {}
        }
        let key = data_key(&new_object_id());
        // Upload completes BEFORE the transaction opens (no SQLite write
        // lock is ever held across an upload). The PUT response already
        // carries the etag — the old separate HEAD per write is gone.
        let mut enc = envelope::seal_reader(ObjKind::Data, reader, len);
        let info = self
            .objects
            .put_stream(&key, &mut enc, envelope::encoded_len(len))?;
        let backend = self.objects.backend_id();
        let etag = info.etag;
        let result = self.mutate(&comps[..comps.len() - 1], |conn, entries| {
            // Registration is INSIDE the commit transaction (S4.22): the
            // ledger row and the logical commit land atomically.
            Store::ledger_object_tx(conn, &backend, &key)?;
            let entry = entries.iter_mut().find(|e| e.n == name);
            let (file_id, base_version) = match entry {
                // EXISTING file: overwrite, caller's CAS condition judged
                // in-transaction against the CURRENT entry.
                Some(e) if e.k == "file" => {
                    let fid = e
                        .fid
                        .ok_or_else(|| Error::Db("file without identity".into()))?;
                    if let Some(exp) = expected_version {
                        if exp != e.fv.unwrap_or(0) {
                            return Err(Error::Conflict {
                                path: path.to_string(),
                                expected: exp,
                                actual: e.fv.unwrap_or(0),
                            });
                        }
                    }
                    (fid, e.fv.unwrap_or(0))
                }
                Some(e) => {
                    return Err(Error::KindMismatch {
                        path: path.to_string(),
                        expected: "file",
                        actual: e.k.clone(),
                    });
                }
                // NEW file: allocate the File ID inside THIS transaction —
                // the separate create commit no longer exists.
                None if create => (Store::alloc_file_id_tx(conn)?, 0),
                None => return Err(Error::NotFound(path.to_string())),
            };
            let version = base_version + 1;
            Store::insert_file_version_tx(conn, &FileVersionRow {
                file_id,
                version,
                size: len as i64,
                mtime: now_secs(),
                mode: 0o644,
                object_backend: backend.clone(),
                object_key: key.clone(),
                etag: etag.clone(),
            })?;
            // The entry was created (in-tx) or already existed (overwrite):
            // either way it is THIS file now, and every condition below is
            // judged against the transaction's own read.
            let e = match entries.iter_mut().find(|e| e.n == name) {
                Some(e) => e,
                None => {
                    entries.push(Entry {
                        n: name.clone(),
                        k: "file".into(),
                        m: 0o644,
                        s: 0,
                        mt: now_secs(),
                        tid: None,
                        w: None,
                        fid: Some(file_id),
                        fv: None,
                        o: None,
                        e: None,
                        tgt: None,
                    });
                    entries.last_mut().expect("just pushed")
                }
            };
            if e.fid != Some(file_id) {
                return Err(Error::IdentityGone(format!("file {file_id}")));
            }
            if e.fv.unwrap_or(0) != base_version {
                return Err(Error::Conflict {
                    path: path.to_string(),
                    expected: base_version,
                    actual: e.fv.unwrap_or(0),
                });
            }
            e.fv = Some(version);
            e.o = Some(StorageRef::new(&backend, &key));
            e.e = Some(etag);
            e.s = len as i64;
            e.mt = now_secs();
            Ok(())
        });
        Self::reregister_uploaded_object_on_failure(self, &backend, &key, &result);
        result?;
        self.lookup(path)
    }

    pub fn write_stream_by_id(
        &self,
        file_id: i64,
        expected_version: Option<i64>,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<Node> {
        let entry = self.locate_by_id(file_id, "")?;
        let path = self
            .path_of(file_id)?
            .ok_or_else(|| Error::IdentityGone(format!("file {file_id} has no head location")))?;
        self.write_stream(
            &path,
            expected_version.or(entry.fv),
            false,
            reader,
            len,
        )
    }
}

// ---- pinned (fixed-commit) reads ----

impl Core {
    /// stat a path AS OF a commit. The commit's tree is immutable: later
    /// mainline updates cannot change what this returns.
    pub fn lookup_at(&self, commit_id: i64, path: &str) -> Result<Node> {
        let commit = self.store.get_commit(commit_id)?;
        let comps = Self::split_path(path)?;
        if comps.is_empty() {
            return Ok(Self::root_node(&commit, commit.root_tree_id));
        }
        let chain = self.walk_dirs_from(commit.root_tree_id, &comps[..comps.len() - 1])?;
        let parent = chain.last().expect("≥1");
        let name = comps.last().expect("non-empty");
        let entry = parent
            .entries
            .iter()
            .find(|e| e.n == *name)
            .ok_or_else(|| Error::NotFound(path.to_string()))?;
        Ok(Self::entry_node(entry, parent.tid))
    }

    /// Read bytes AS OF a commit (full-content digest verification,
    /// because history must be exactly right or refused).
    pub fn read_at(&self, commit_id: i64, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        let commit = self.store.get_commit(commit_id)?;
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        let chain = self.walk_dirs_from(commit.root_tree_id, &comps[..comps.len() - 1])?;
        let parent = chain.last().expect("≥1");
        let entry = parent
            .entries
            .iter()
            .find(|e| e.n == *name)
            .ok_or_else(|| Error::NotFound(path.to_string()))?;
        if entry.k != "file" {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: entry.k.clone(),
            });
        }
        let r = entry
            .o
            .clone()
            .ok_or_else(|| Error::ObjectMissing(format!("{path}: no content at this commit")))?;
        let key = self.check_ref(&r)?;
        // Full verified read, then slice: pinned history must be
        // byte-exact, so the footer digest is ALWAYS checked here — the
        // range is carved out of the verified staging file AFTER
        // validation, never before (S4.24: same semantics, bounded
        // memory). Honest cost trade: memory is now O(range), but the
        // FULL object still travels over the network because the
        // checksum needs every byte. Whether pinned reads may downgrade
        // to ranged GET + backend validator is a separate correctness
        // contract decision, deliberately NOT made here.
        let mut vo = self.read_full_verified_stream(key, entry.e.as_deref().unwrap_or(""))?;
        let payload_len = vo.payload_len;
        let start = offset.min(payload_len);
        let end = start.saturating_add(size).min(payload_len);
        use std::io::{Read as _, Seek, SeekFrom};
        vo.reader.seek(SeekFrom::Start(start))?;
        let mut buf = vec![0u8; (end - start) as usize];
        vo.reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

pub struct WriteHandle {
    pub file_id: i64,
    pub path: String,
    pub(crate) expected_version: i64,
    buf: Mutex<Vec<u8>>,
}

impl WriteHandle {
    fn new(file_id: i64, path: &str, expected_version: i64) -> WriteHandle {
        WriteHandle {
            file_id,
            path: path.to_string(),
            expected_version,
            buf: Mutex::new(Vec::new()),
        }
    }

    pub fn write(&self, offset: u64, data: &[u8]) -> Result<usize> {
        let mut buf = self.buf.lock().unwrap();
        let end = (offset as usize).saturating_add(data.len());
        if end > buf.len() {
            buf.resize(end, 0);
        }
        buf[offset as usize..end].copy_from_slice(data);
        Ok(data.len())
    }

}

/// A footer-verified full object staged in a private temp file (S4.24).
/// The reader is the VALIDATED file (positioned at 0); the payload length
/// is the decoded logical size. Dropping this value deletes the file.
pub struct VerifiedObjectRead {
    pub reader: std::fs::File,
    pub payload_len: u64,
    /// Held for Drop: deletes the staging file when the read is consumed
    /// or cancelled. Never read directly.
    #[allow(dead_code)]
    pub(crate) guard: TempFileGuard,
}

impl std::io::Read for VerifiedObjectRead {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.reader.read(buf)
    }
}

impl std::io::Seek for VerifiedObjectRead {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.reader.seek(pos)
    }
}

/// Private staging file, deleted on EVERY exit path (success consumption,
/// verification failure, short read, IO error, cancellation-by-drop).
/// Failure to remove is surfaced, never silent (it holds user data).
pub(crate) struct TempFileGuard {
    path: std::path::PathBuf,
    pub file: Option<std::fs::File>,
}

impl TempFileGuard {
    pub(crate) fn create() -> std::io::Result<TempFileGuard> {
        let dir = std::env::temp_dir();
        for _ in 0..8 {
            let p = dir.join(format!(
                "lightfs-read-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_nanos())
                    .unwrap_or(0)
            ));
            let mut f = std::fs::OpenOptions::new();
            f.read(true).write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt as _;
                f.mode(0o600);
            }
            match f.open(&p) {
                Ok(file) => {
                    return Ok(TempFileGuard { path: p, file: Some(file) });
                }
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(std::io::Error::other("could not create staging file"))
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(&self.path) {
            if e.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "lightfs: WARNING failed to remove staging file {}: {e}",
                    self.path.display()
                );
            }
        }
    }
}
