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
use crate::object_store::{ListPage, ObjectData, ObjectInfo, ObjectStore, Range};
use crate::vnext::envelope::{self, ObjKind};
use crate::vnext::store::{CommitRow, Entry, FileVersionRow, Store};

pub(crate) const MAIN_REF: &str = "main";

fn data_key(file_id: i64, uniq: &str) -> String {
    format!("objects/{file_id}/w-{uniq}.data")
}
pub(crate) fn tree_key(sha: &str) -> String {
    format!("meta/trees/{sha}")
}
pub(crate) fn commit_key(sha: &str) -> String {
    format!("meta/commits/{sha}")
}
pub(crate) fn ref_key(name: &str) -> String {
    format!("meta/refs/{name}")
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
    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData> {
        self.m.s3_get.fetch_add(1, Ordering::Relaxed);
        let out = self.inner.get(key, range)?;
        self.m.bytes_read.fetch_add(out.data.len() as u64, Ordering::Relaxed);
        Ok(out)
    }
    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<(Box<dyn Read + Send>, u64)> {
        self.m.s3_get.fetch_add(1, Ordering::Relaxed);
        let (r, len) = self.inner.get_stream(key, range)?;
        self.m.bytes_read.fetch_add(len, Ordering::Relaxed);
        Ok((r, len))
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

/// Recovery artifacts collected inside a commit transaction.
#[derive(Default)]
struct Acc {
    new_trees: Vec<(String, Vec<u8>)>, // (sha, entries_json bytes)
    commit_record: Option<(String, Vec<u8>)>, // (sha, canonical record)
    commit_seq: Option<i64>,
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn uniq_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{}-{nanos:x}", std::process::id())
}

fn db_err(e: rusqlite::Error) -> Error {
    Error::Db(format!("vnext: {e}"))
}

fn record_root_tree(record: &[u8]) -> String {
    serde_json::from_slice::<serde_json::Value>(record)
        .ok()
        .and_then(|v| v.get("root_tree").and_then(|x| x.as_str().map(String::from)))
        .unwrap_or_default()
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
        core.publish_initial_commit()?;
        Ok(core)
    }

    /// Take ownership of the engine lock acquired BEFORE this Core (and
    /// its SQLite store) existed — the S4.14.1 ownership order.
    pub(crate) fn hold_engine_lock(&mut self, lock: crate::fslock::FileLock) {
        self._lock = Some(lock);
    }

    /// The INITIAL commit (created by Store::initialize, which has no
    /// object store) is the root of every parent chain. Its recovery
    /// object and empty root tree are published here, idempotently: a
    /// store opened a hundred times publishes exactly once.
    fn publish_initial_commit(&self) -> Result<()> {
        let (sha, record, tree_json): (String, Vec<u8>, Vec<u8>) = self.store.read(|conn| {
            let (sha, root_tree_sha): (String, String) = conn
                .query_row(
                    "SELECT content_sha, (SELECT content_sha FROM trees WHERE id = commits.root_tree_id)
                     FROM commits WHERE parent_id IS NULL",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .map_err(db_err)?;
            let record = serde_json::json!({
                "ref": MAIN_REF,
                "parent": serde_json::Value::Null,
                "root_tree": root_tree_sha,
                "seq": 0,
            })
            .to_string()
            .into_bytes();
            let entries_json: String = conn
                .query_row(
                    "SELECT entries_json FROM trees WHERE content_sha = ?1",
                    [root_tree_sha],
                    |r| r.get(0),
                )
                .map_err(db_err)?;
            Ok((sha, record, entries_json.into_bytes()))
        })?;
        if crate::sigv4::sha256_hex(&record) != sha {
            // The stored hash must match the canonical record we would
            // publish; if it doesn't, the store was written by different
            // logic and we refuse to invent a recovery object.
            return Err(Error::Db(
                "vnext: initial commit hash does not match its canonical record".into(),
            ));
        }
        match self.objects.head(&commit_key(&sha)) {
            Ok(_) => {} // already published
            Err(Error::ObjectMissing(_)) => {
                let _ = self.objects.put(
                    &tree_key(&record_root_tree(&record)),
                    &envelope::seal(ObjKind::Tree, &tree_json),
                );
                let _ = self
                    .objects
                    .put(&commit_key(&sha), &envelope::seal(ObjKind::Commit, &record));
            }
            Err(e) => return Err(e),
        }
        Ok(())
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
            object_key: entry.o.clone(),
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
        step: impl FnOnce(&Connection, &mut Vec<Entry>, &mut Acc) -> Result<T>,
    ) -> Result<T> {
        let acc = Mutex::new(Acc::default());
        let out = self.store.with_tx(|conn| {
            // Walk INSIDE the write transaction: base trees are read under
            // BEGIN IMMEDIATE, so serialized writers can never build on a
            // stale snapshot (no silent lost updates).
            let mut chain = self.walk_dirs_tx(conn, comps)?;
            let mut acc = acc.lock().unwrap();
            let result = {
                let leaf = chain.last_mut().expect("walk yields ≥1");
                step(conn, &mut leaf.entries, &mut acc)?
            };
            Self::cow_and_commit(conn, &mut chain, &mut acc)?;
            Ok(result)
        });
        match out {
            Ok(v) => {
                self.publish_recovery(&acc.into_inner().unwrap());
                Ok(v)
            }
            Err(e) => Err(e),
        }
    }

    /// Bottom-up COW over `chain` + commit + ref advance. Entries of the
    /// leaf (and of any level a caller adjusted) are already mutated;
    /// child pointers are rewired by matching the OLD tree id (within a
    /// directory, at most one entry references a given child tree).
    fn cow_and_commit(conn: &Connection, chain: &mut [Level], acc: &mut Acc) -> Result<()> {
        let (last_id, last_sha, _) = Self::cow_branch(conn, chain, acc)?;
        let (_, seq, record, record_sha) =
            Store::commit_mainline_tx(conn, MAIN_REF, last_id, &last_sha)?;
        acc.commit_record = Some((record_sha, record));
        acc.commit_seq = Some(seq);
        Ok(())
    }

    /// Bottom-up COW over `chain` WITHOUT appending the commit. Returns
    /// `(new_top_id, new_top_sha, old_top_tid)` so a caller splicing this
    /// branch under a shared prefix can rewire the parent entry by the old
    /// tree id. `chain` must be non-empty.
    fn cow_branch(
        conn: &Connection,
        chain: &mut [Level],
        acc: &mut Acc,
    ) -> Result<(i64, String, i64)> {
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
            acc.new_trees.push((sha.clone(), json.into_bytes()));
            last_id = id;
            last_sha = sha;
        }
        Ok((last_id, last_sha, top_old))
    }

    /// Best-effort recovery copies. Failures logged, never propagated.
    fn publish_recovery(&self, acc: &Acc) {
        for (sha, json) in &acc.new_trees {
            let _ = self
                .objects
                .put(&tree_key(sha), &envelope::seal(ObjKind::Tree, json));
        }
        if let Some((sha, record)) = &acc.commit_record {
            let _ = self
                .objects
                .put(&commit_key(sha), &envelope::seal(ObjKind::Commit, record));
            let proj = serde_json::json!({
                "ref": MAIN_REF,
                "commit": sha,
                "seq": acc.commit_seq.unwrap_or(0),
            })
            .to_string();
            let _ = self.objects.put(
                &ref_key(MAIN_REF),
                &envelope::seal(ObjKind::Ref, proj.as_bytes()),
            );
        }
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
    /// read), header verified, etag compared; the envelope digest covers
    /// the WHOLE object, so a partial read trusts the recorded etag.
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
            let obj = self
                .objects
                .get(key, Some(Range { start: 0, len: envelope::HEADER_LEN as u64 + want }))?;
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
            if (logical as u64) < want {
                return Err(Error::ObjectCorrupt(format!(
                    "{key}: logical length {logical} shorter than the requested read"
                )));
            }
            return Ok(obj.data[envelope::HEADER_LEN..envelope::HEADER_LEN + want as usize].to_vec());
        }
        let head =
            self.objects
                .get(key, Some(Range { start: 0, len: envelope::HEADER_LEN as u64 }))?;
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
        let body = self.objects.get(
            key,
            Some(Range {
                start: envelope::HEADER_LEN as u64 + offset,
                len,
            }),
        )?;
        if !recorded_etag.is_empty() && body.info.etag != recorded_etag {
            return Err(Error::ObjectCorrupt(format!("{key}: etag changed mid-read")));
        }
        Ok(body.data)
    }

    /// Full read with full digest verification: a damaged object is
    /// refused, never served (read-your-bytes integrity).
    pub fn read_full_verified(&self, key: &str, recorded_etag: &str) -> Result<Vec<u8>> {
        let obj = self.objects.get(key, None)?;
        if !recorded_etag.is_empty() && obj.info.etag != recorded_etag {
            return Err(Error::ObjectCorrupt(format!(
                "{key}: recorded etag {recorded_etag} != observed {}",
                obj.info.etag
            )));
        }
        let (kind, content) = envelope::open(&obj.data)?;
        if kind != ObjKind::Data {
            return Err(Error::ObjectCorrupt(format!("{key}: not a data object")));
        }
        Ok(content)
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
        let key = entry
            .o
            .ok_or_else(|| Error::ObjectMissing(format!("{path}: file has no content yet")))?;
        self.read_range_verified(
            &key,
            entry.e.as_deref().unwrap_or(""),
            entry.s.max(0) as u64,
            offset,
            size,
        )
    }

    pub fn read_by_id(&self, file_id: i64, offset: u64, size: u64) -> Result<Vec<u8>> {
        let entry = self.locate_by_id(file_id, "")?;
        let key = entry
            .o
            .ok_or_else(|| Error::ObjectMissing(format!("file {file_id}: no content yet")))?;
        self.read_range_verified(
            &key,
            entry.e.as_deref().unwrap_or(""),
            entry.s.max(0) as u64,
            offset,
            size,
        )
    }

    pub fn open_read(
        &self,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let data = self.read(path, offset, size)?;
        let len = data.len() as u64;
        Ok((Box::new(std::io::Cursor::new(data)), len))
    }

    pub fn open_read_by_id(
        &self,
        file_id: i64,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let data = self.read_by_id(file_id, offset, size)?;
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
        self.mutate(&comps[..comps.len() - 1], |conn, entries, acc| {
            if entries.iter().any(|e| e.n == name) {
                return Err(Error::AlreadyExists(path.to_string()));
            }
            let (child_tid, child_sha) = Store::insert_tree_json_tx(conn, "[]")?;
            acc.new_trees.push((child_sha.clone(), b"[]".to_vec()));
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
        self.mutate(&comps[..comps.len() - 1], |conn, entries, _acc| {
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
        let entry = self.mutate(&comps[..comps.len() - 1], |conn, entries, _acc| {
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
        self.mutate(&comps[..comps.len() - 1], |_conn, entries, _acc| {
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
        self.mutate(&comps[..comps.len() - 1], |conn, entries, _acc| {
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
        self.mutate(&comps[..comps.len() - 1], |_conn, entries, _acc| {
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
        let acc = Mutex::new(Acc::default());
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
            let mut a = acc.lock().unwrap();
            if same_leaf_dir {
                // one directory changed: plain linear cow
                return Self::cow_and_commit(conn, &mut old_chain, &mut a);
            }
            // Two branches hanging off a shared prefix: cow each branch
            // bottom-up, splice both new tops into the prefix's last level
            // (the chain copies share tids, so either copy carries it),
            // then cow the prefix + commit.
            if old_chain.len() > split_at {
                let (id, sha, old_top) = Self::cow_branch(conn, &mut old_chain[split_at..], &mut a)?;
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
                let (id, sha, new_top) = Self::cow_branch(conn, &mut new_chain[split_at..], &mut a)?;
                let top = &mut old_chain[split_at - 1];
                let slot = top
                    .entries
                    .iter_mut()
                    .find(|e| e.tid == Some(new_top))
                    .ok_or_else(|| Error::Db("rename: prefix lost the new branch".into()))?;
                slot.tid = Some(id);
                slot.w = Some(sha);
            }
            Self::cow_and_commit(conn, &mut old_chain[..split_at], &mut a)
        })?;
        self.publish_recovery(&acc.into_inner().unwrap());
        Ok(())
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
    /// digest inside), then ONE metadata transaction. A file-level
    /// version conflict (another writer committed first) is an explicit
    /// Conflict; the orphaned object stays ledgered for reachability gc.
    pub fn commit_write(&self, handle: &WriteHandle) -> Result<Node> {
        let content = handle.snapshot();
        let key = data_key(handle.file_id, &uniq_suffix());
        let sealed = envelope::seal(ObjKind::Data, &content);
        let info = self.objects.put(&key, &sealed)?;
        self.store.ledger_object(&key)?;

        let comps = Self::split_path(&handle.path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(handle.path.clone()))?;
        let file_id = handle.file_id;
        let expected = handle.expected_version;
        let version = expected + 1;
        let (mtime, size, etag) = (now_secs(), content.len() as i64, info.etag.clone());
        let key2 = key.clone();
        self.mutate(&comps[..comps.len() - 1], |conn, entries, _acc| {
            Store::insert_file_version_tx(conn, &FileVersionRow {
                file_id,
                version,
                size,
                mtime,
                mode: 0o644,
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
            entry.o = Some(key);
            entry.e = Some(info.etag);
            entry.s = size;
            entry.mt = mtime;
            Ok(())
        })?;
        self.lookup(&handle.path)
    }

    /// Server streaming write. `create=true` first-creates the identity
    /// via a real `create` commit when the file doesn't exist. The reader
    /// must be seekable (the server spools to a temp file first) so the
    /// envelope digest can be computed without buffering.
    pub fn write_stream(
        &self,
        path: &str,
        expected_version: Option<i64>,
        create: bool,
        reader: &mut dyn envelope::ReadSeek,
        len: u64,
    ) -> Result<Node> {
        let comps = Self::split_path(path)?;
        let name = comps
            .last()
            .cloned()
            .ok_or_else(|| Error::InvalidPath(path.to_string()))?;
        let entry = self.resolve_entry(path).ok();
        let (file_id, base_version) = match &entry {
            Some(e) if e.k == "file" => (
                e.fid.ok_or_else(|| Error::Db("file without identity".into()))?,
                e.fv.unwrap_or(0),
            ),
            Some(e) => {
                return Err(Error::KindMismatch {
                    path: path.to_string(),
                    expected: "file",
                    actual: e.k.clone(),
                })
            }
            None if create => {
                let node = self.create(path, 0o644)?;
                let fid = node.id;
                (fid, 0)
            }
            None => return Err(Error::NotFound(path.to_string())),
        };
        if let Some(exp) = expected_version {
            if exp != base_version {
                return Err(Error::Conflict {
                    path: path.to_string(),
                    expected: exp,
                    actual: base_version,
                });
            }
        }
        let key = data_key(file_id, &uniq_suffix());
        {
            let mut spooled = Vec::new();
            envelope::seal_stream(ObjKind::Data, reader, len, &mut spooled)
                .map_err(|e| Error::Storage(format!("seal stream: {e}")))?;
            self.objects.put(&key, &spooled)?;
        }
        self.store.ledger_object(&key)?;
        let version = base_version + 1;
        let etag = {
            self.objects
                .head(&key)?
                .etag
        };
        self.mutate(&comps[..comps.len() - 1], |conn, entries, _acc| {
            Store::insert_file_version_tx(conn, &FileVersionRow {
                file_id,
                version,
                size: len as i64,
                mtime: now_secs(),
                mode: 0o644,
                object_key: key.clone(),
                etag: etag.clone(),
            })?;
            let e = entries
                .iter_mut()
                .find(|e| e.n == name)
                .ok_or_else(|| Error::NotFound(path.to_string()))?;
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
            e.o = Some(key);
            e.e = Some(etag);
            e.s = len as i64;
            e.mt = now_secs();
            Ok(())
        })?;
        self.lookup(path)
    }

    pub fn write_stream_by_id(
        &self,
        file_id: i64,
        expected_version: Option<i64>,
        reader: &mut dyn envelope::ReadSeek,
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
        let key = entry
            .o
            .clone()
            .ok_or_else(|| Error::ObjectMissing(format!("{path}: no content at this commit")))?;
        // Full verified read, then slice: pinned history must be
        // byte-exact, so the digest is always checked here.
        let full = self.read_full_verified(&key, entry.e.as_deref().unwrap_or(""))?;
        let start = (offset as usize).min(full.len());
        let end = (start + size as usize).min(full.len());
        Ok(full[start..end].to_vec())
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

    fn snapshot(&self) -> Vec<u8> {
        self.buf.lock().unwrap().clone()
    }
}
