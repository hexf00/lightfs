//! Filesystem Core: the single source of filesystem semantics.
//! Embedded mode, the server, and (through the client protocol) FUSE all
//! go through this module. Adapters never touch SQLite or S3 directly.
//!
//! Concurrency model (see the design notes, concurrency section):
//! - metadata mutations are serialized by the metadata store's transactions
//! - file content uses optimistic version checking; stale writes get CONFLICT,
//!   never a silent overwrite

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{Error, Result};
use crate::metadata::{MetadataStore, Node, NodeKind};
use crate::object_store::ObjectStore;

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
        }
    }

    /// Retire a committed object: record it for retention (point-in-time
    /// restore) or, when retention is disabled, delete it immediately.
    /// Best effort — a leftover is a stale/retained object for fsck, never
    /// a correctness issue.
    fn retire_object(&self, key: &str) {
        // Metadata write failed: fall through to the plain delete so the
        // data plane still does not accumulate.
        if !(self.retention_supersede && self.meta.record_superseded(key, self.now()).is_ok())
            && self.objects.delete(key).is_ok()
        {
            self.metrics.s3_delete.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn metadata(&self) -> &Arc<dyn MetadataStore> {
        &self.meta
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
                return Ok(node);
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
        self.meta
            .get_node(node_id)?
            .ok_or_else(|| Error::IdentityGone(node_id.to_string()))
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
        self.meta.list_children(node.id)
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
        if offset >= node.size as u64 || size == 0 {
            return Ok(Vec::new());
        }
        let len = size.min(node.size as u64 - offset);
        let key = node
            .object_key
            .clone()
            .unwrap_or_else(|| object_key_for(node.id));
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
        self.open_read_node(&node, offset, size)
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
        self.open_read_node(&node, offset, size)
    }

    fn open_read_node(
        &self,
        node: &Node,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        if offset >= node.size as u64 || size == 0 {
            return Ok((Box::new(std::io::empty()), 0));
        }
        let len = size.min(node.size as u64 - offset);
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
        self.write_stream_node(&node, expected, reader, len, create_if_missing && node.version == 1)
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
        let node = self
            .meta
            .get_node(handle.file_id)?
            .ok_or_else(|| Error::NotFound(handle.path.clone()))?;
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
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("objects/{file_id}/w-{nanos:x}-{seq:x}.data")
    }

    /// rename/move: metadata-only. Object bytes never move (AD-010).
    /// A file may overwrite an existing file; anything else must not exist.
    pub fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        let src = self.lookup(old_path)?;
        let (dest_parent, dest_name) = self.parent_and_name(new_path)?;

        if let Some(dest) = self.meta.get_child(dest_parent.id, &dest_name)? {
            if src.is_dir() || dest.is_dir() {
                // Overwriting requires same-kind file->file; a directory
                // target must not exist (POSIX allows empty-dir overwrite,
                // which v1 does not need).
                return Err(Error::AlreadyExists(new_path.to_string()));
            }
            // file -> file overwrite: retire the destination first.
            if let Some(key) = &dest.object_key {
                self.retire_object(key);
            }
            self.meta.delete_node(dest.id)?;
        }

        // Moving a directory below itself would orphan the subtree. The
        // destination parent itself is the first candidate ancestor.
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
        self.meta.rename_node(src.id, dest_parent.id, &dest_name)
    }

    /// Remove a file. With retention the object is RECORDED (bytes stay for
    /// point-in-time restore) before the metadata row goes; without
    /// retention it is deleted first, and a failed delete keeps the metadata
    /// row so fsck retries later.
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
        if let Some(key) = &node.object_key {
            if self.retention_supersede {
                self.meta.record_superseded(key, self.now())?;
            } else {
                self.objects.delete(key)?;
                self.metrics.s3_delete.fetch_add(1, Ordering::Relaxed);
            }
        }
        self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
        self.meta.delete_node(node.id)
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

    /// With retention disabled the old immediate-delete semantics hold.
    #[test]
    fn unlink_deletes_immediately_when_retention_disabled() {
        let c = Core::with_retention(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(MemObjectStore::new()),
            0,
        );
        c.create("/f", 0o644).unwrap();
        // commit content so there is an object to delete immediately
        let h = c.open_write("/f", None).unwrap();
        c.write(&h, 0, b"x").unwrap();
        c.commit_write(&h).unwrap();
        let key = c.lookup("/f").unwrap().object_key.clone().unwrap();
        c.unlink("/f").unwrap();
        assert!(c.objects.head(&key).is_err());
        assert!(c.metadata().all_superseded().unwrap().is_empty());
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

    #[test]
    fn migration_adds_target_column_to_legacy_db() {
        let dir = std::env::temp_dir().join(format!("lightfs-sym-mig-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("legacy.db");
        // Build a PRE-symlink database with the old schema text.
        let conn = rusqlite::Connection::open(&db).unwrap();
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS nodes (
                id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT NOT NULL,
                kind TEXT NOT NULL, size INTEGER NOT NULL DEFAULT 0,
                mtime INTEGER NOT NULL, mode INTEGER, object_key TEXT,
                version TEXT, etag TEXT, UNIQUE(parent_id, name)
            );
            INSERT INTO nodes (id, parent_id, name, kind, size, mtime, mode)
            VALUES (1, NULL, '', 'dir', 0, 0, 493);",
        )
        .unwrap();
        drop(conn);
        // Opening migrates; the store then works with symlinks.
        let store = SqliteMetadataStore::open(db.to_str().unwrap()).unwrap();
        let c = Core::new(Arc::new(store), Arc::new(MemObjectStore::new()));
        c.symlink("/x", "/y").unwrap();
        assert_eq!(c.readlink("/y").unwrap(), "/x");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
