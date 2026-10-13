//! FUSE adapter (Phase 8, Linux only). Pure adapter: inodes map to paths,
//! all semantics come from FsApi (Core or network client). Compiles only on
//! Linux targets; core correctness tests never require it (AD-006).
//!
//! Write path: FUSE delivers arbitrary-offset writes per handle; the adapter
//! buffers them and commits the assembled content once on flush (POSIX close
//! durability), carrying the version observed at open. Concurrent external
//! writers therefore surface as EIO, never silent overwrite.
//!
//! Written against the fuser 0.18.0 API (verified against the published
//! crate source: &self methods, INodeNo/FileHandle/Errno newtypes,
//! Config instead of MountOption).

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    Errno, FileAttr, FileHandle, FileType, Filesystem, INodeNo, LockOwner, OpenFlags, RenameFlags,
    ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen,
    ReplyWrite, Request, TimeOrNow,
};

use crate::api::FsApi;
use crate::error::Error;
use crate::metadata::Node;
use crate::writeback::PendingWrite;

/// Paginated readdir snapshot (per ino+fh, stamped when fetched).
type DirCacheKey = (u64, u64);
type DirSnapshot = (Vec<(u64, FileType, String)>, std::time::Instant);

/// Cached full listing for a directory (offset==0 refetch suppression).
/// EXPERIMENT-ONLY (C1 TTL sweep): default OFF. Entries are dropped on
/// any LOCAL namespace mutation (create/mkdir/unlink/rmdir/rename), so
/// own writes stay POSIX-visible and only CROSS-client changes age out
/// with the TTL - that aging window is exactly what the sweep measures.
type DirentCache = HashMap<String, (Vec<Node>, std::time::Instant)>;

fn secs_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

/// A nameless inode kept alive by open handles (detached semantics): the
/// bytes live in THIS map until the last handle closes, then are reaped.
/// `base` is the content snapshot taken at detach time (read through the
/// name while it still exists); `chunks` hold every write from ALL
/// surviving handles - shared per-inode, so two fds on a detached file
/// see each other's writes, exactly like POSIX. Nothing here is ever
/// uploaded: the name is gone and must not resurrect, fsync succeeds
/// because the data is held (in memory, like any anonymous inode).
struct Detached {
    base: Vec<u8>,
    chunks: crate::writeback::ChunkMap,
    refs: usize,
}

pub struct FuseAdapter {
    fs: std::sync::Arc<dyn FsApi>,
    /// ino (== metadata node id) -> path within the filesystem.
    inodes: Mutex<HashMap<u64, PathBuf>>,
    /// file handle -> buffered write.
    writes: Mutex<HashMap<u64, PendingWrite>>,
    /// ino -> detached bytes (open handles holding a dead name).
    detached: Mutex<HashMap<u64, Detached>>,
    /// path -> ino reverse index: ino_of_path used to LINEAR-SCAN the
    /// whole inode map on every reverse lookup, growing with the number
    /// of dirs ever visited (O(n) per getattr on a cold name).
    paths: Mutex<HashMap<String, u64>>,
    /// Paginated readdir snapshots: the kernel walks a large directory
    /// through MANY readdir callbacks (one per reply buffer) and each
    /// callback used to refetch the whole list - an ~80x amplification
    /// (op metrics: 11.5k readdir callbacks cost 149s mount-side while
    /// the server handled the same calls in 1.7s). offset==0 fetches a
    /// fresh snapshot; offset>0 replays the cached one (getdents makes
    /// no coherence promises mid-walk).
    dir_cache: Mutex<HashMap<DirCacheKey, DirSnapshot>>,
    /// How long a readdir snapshot stays reusable for offset>0 pages.
    /// A walk that outlives it fails its next page with EINVAL rather
    /// than replaying a silently stale list; LIGHTFS_FUSE_DIR_SNAPSHOT_SECS
    /// overrides (A12 exercises eviction with a short TTL).
    dir_snapshot_ttl: std::time::Duration,
    /// How long lookup/attr replies may be cached by the kernel
    /// (LIGHTFS_FUSE_ATTR_TTL_SECS, default 1; 0 = ask every time).
    attr_ttl: Duration,
    /// Cross-walk listing cache (LIGHTFS_FUSE_DIRENT_CACHE_SECS, default
    /// 0 = off). See DirentCache docs - C1 experiment knob.
    dirent_cache: Mutex<DirentCache>,
    dirent_ttl: Duration,
    /// readdirplus-style lookup cache: full path -> node, seeded by every
    /// fresh directory listing (the listing already carries each child's
    /// metadata, so the lookup storm git sends right after a readdir is
    /// served without an RPC per file). Freshness is bounded by the SAME
    /// attr_ttl the kernel cache gets, and any local namespace mutation
    /// drops it entirely - so self-writes stay POSIX-visible and only
    /// cross-client changes can be observed up to attr_ttl late.
    attr_cache: Mutex<HashMap<String, (Node, std::time::Instant)>>,
    /// In-flight EOF per path (see writeback::PendingEof): appends derive
    /// their offset from our view of the file size, and stat coherence.
    pending: std::sync::LazyLock<crate::writeback::PendingEof>,
    next_fh: AtomicU64,
    uid: u32,
    gid: u32,
}

fn unix_to_systemtime(secs: i64) -> SystemTime {
    if secs >= 0 {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    } else {
        UNIX_EPOCH
    }
}

fn errno(e: &Error) -> Errno {
    match e {
        Error::NotFound(_) | Error::ParentNotFound(_) | Error::ObjectMissing(_) => Errno::ENOENT,
        // The identity an open handle was bound to was deleted by another
        // client. POSIX has no richer errno for this; the contract (M3) is
        // that it fails explicitly and observably instead of aliasing.
        Error::IdentityGone(_) => Errno::ENOENT,
        Error::AlreadyExists(_) => Errno::EEXIST,
        Error::KindMismatch {
            expected: "dir", ..
        } => Errno::ENOTDIR,
        // readlink on a non-symlink is EINVAL per POSIX (not EISDIR)
        Error::KindMismatch {
            expected: "symlink",
            ..
        } => Errno::EINVAL,
        Error::KindMismatch { .. } => Errno::EISDIR,
        Error::InvalidArgument(m) if m.contains("too many symlink") => Errno::ELOOP,
        Error::DirectoryNotEmpty(_) => Errno::ENOTEMPTY,
        Error::Cycle(_) | Error::InvalidPath(_) | Error::InvalidArgument(_) => Errno::EINVAL,
        Error::Conflict { .. } => Errno::EIO,
        Error::Storage(_) | Error::Db(_) | Error::Io(_) | Error::Config(_) => Errno::EIO,
    }
}

fn attr_for(node: &Node, uid: u32, gid: u32) -> FileAttr {
    FileAttr {
        ino: INodeNo(node.id as u64),
        size: node.size as u64,
        blocks: (node.size as u64).div_ceil(4096),
        atime: unix_to_systemtime(node.mtime),
        mtime: unix_to_systemtime(node.mtime),
        ctime: unix_to_systemtime(node.mtime),
        crtime: unix_to_systemtime(node.mtime),
        kind: if node.is_dir() {
            FileType::Directory
        } else if node.is_symlink() {
            FileType::Symlink
        } else {
            FileType::RegularFile
        },
        // Symlinks always appear as 0777 like on a real POSIX filesystem.
        perm: if node.is_symlink() {
            0o777
        } else {
            (node.mode & 0o7777) as u16
        },
        nlink: if node.is_dir() { 2 } else { 1 },
        uid,
        gid,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

impl FuseAdapter {
    pub fn new(fs: std::sync::Arc<dyn FsApi>) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(1u64, PathBuf::from("/"));
        // SAFETY: plain uid/gid queries; no invariants beyond Linux's own.
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        // Snapshot lifetime for paged readdir. Default 30s covers any
        // interactive walk by a wide margin; the env knob exists so the
        // slow-pagination regression (acceptance A12) can exercise the
        // eviction path in seconds instead of half a minute.
        let dir_snapshot_ttl =
            std::time::Duration::from_secs(secs_env("LIGHTFS_FUSE_DIR_SNAPSHOT_SECS", 30));
        Self {
            fs,
            inodes: Mutex::new(inodes),
            writes: Mutex::new(HashMap::new()),
            detached: Mutex::new(HashMap::new()),
            paths: Mutex::new(HashMap::new()),
            dir_cache: Mutex::new(HashMap::new()),
            dir_snapshot_ttl,
            attr_ttl: Duration::from_secs(secs_env("LIGHTFS_FUSE_ATTR_TTL_SECS", 1)),
            dirent_cache: Mutex::new(HashMap::new()),
            dirent_ttl: Duration::from_secs(secs_env("LIGHTFS_FUSE_DIRENT_CACHE_SECS", 0)),
            attr_cache: Mutex::new(HashMap::new()),
            pending: std::sync::LazyLock::new(crate::writeback::PendingEof::new),
            next_fh: AtomicU64::new(1),
            uid,
            gid,
        }
    }

    fn path_of(&self, ino: INodeNo) -> Result<String, Errno> {
        self.inodes
            .lock()
            .unwrap()
            .get(&ino.0)
            .map(|p| p.to_string_lossy().to_string())
            .ok_or(Errno::from_i32(libc::ENOENT))
    }

    fn ino_of_path(&self, path: &str) -> Option<u64> {
        self.paths.lock().unwrap().get(path).copied()
    }

    /// "." / ".." plus one entry per child, the reply-loop shape shared by
    /// fresh fetches and dirent-cache hits.
    fn build_dir_entries(
        &self,
        dir_ino: u64,
        path: &str,
        children: &[Node],
    ) -> Vec<(u64, FileType, String)> {
        let mut entries: Vec<(u64, FileType, String)> = Vec::with_capacity(children.len() + 2);
        entries.push((dir_ino, FileType::Directory, ".".to_string()));
        let parent_ino = match path.rfind('/') {
            Some(0) | None => 1u64,
            Some(i) => self.ino_of_path(&path[..i]).unwrap_or(1),
        };
        entries.push((parent_ino, FileType::Directory, "..".to_string()));
        for c in children {
            let kind = if c.is_dir() {
                FileType::Directory
            } else if c.is_symlink() {
                FileType::Symlink
            } else {
                FileType::RegularFile
            };
            entries.push((c.id as u64, kind, c.name.clone()));
        }
        entries
    }

    /// Drop every cached listing: any LOCAL namespace mutation invalidates
    /// (own writes must be immediately visible), so the dirent cache can
    /// only delay CROSS-client changes by at most the TTL.
    fn dirent_invalidate(&self) {
        if self.dirent_ttl > Duration::ZERO {
            self.dirent_cache.lock().unwrap().clear();
        }
        // The lookup cache is cheap enough to drop unconditionally: a
        // mutation means the next lookups must go to the server.
        self.attr_cache.lock().unwrap().clear();
    }

    /// Seed the lookup cache from a fresh listing (both the RPC fetch and
    /// a dirent-cache hit carry per-child metadata).
    fn seed_attrs(&self, dir_path: &str, children: &[Node]) {
        let now = std::time::Instant::now();
        let mut cache = self.attr_cache.lock().unwrap();
        for c in children {
            let p = if dir_path == "/" {
                format!("/{}", c.name)
            } else {
                format!("{dir_path}/{}", c.name)
            };
            cache.insert(p, (c.clone(), now));
        }
    }

    fn forget_path(&self, path: &str) {
        let ino = self.paths.lock().unwrap().remove(path);
        if let Some(ino) = ino {
            self.inodes.lock().unwrap().remove(&ino);
        }
    }

    fn remember(&self, ino: u64, parent_path: &str, name: &OsStr) {
        let mut p = PathBuf::from(parent_path);
        p.push(name);
        let ps = p.to_string_lossy().to_string();
        self.inodes.lock().unwrap().insert(ino, p);
        self.paths.lock().unwrap().insert(ps, ino);
    }

    fn child_path(parent_path: &str, name: &OsStr) -> String {
        let name = name.to_string_lossy();
        if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        }
    }

    /// Max over the committed size and the chunk ends of every open
    /// session for `path`. Callers must NOT hold the writes lock.
    fn visible_eof(&self, path: &str, committed_size: u64) -> u64 {
        let writes = self.writes.lock().unwrap();
        let mut end = committed_size;
        for st in writes.values() {
            if st.path == path {
                end = end.max(st.chunks.max_end());
            }
        }
        end
    }

    /// fs.read with same-mount write-back coherence: POSIX requires a read
    /// (or mmap fault) after write(2) to return the written bytes even
    /// before flush. The kernel page cache usually serves those reads, but
    /// any attribute refresh / page eviction sends it HERE - and committed-
    /// only reads truncated files (git pack verification hit exactly that).
    fn read_with_session(
        &self,
        node_id: u64,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, Error> {
        // by-id: a rename by another client must not detach the read from
        // this inode's bytes (the path only steers same-mount session
        // coherence below).
        let mut data = self.fs.read_by_id(node_id as i64, path, offset, size)?;
        let in_flight = self.pending.get(path);
        let want = if in_flight > offset {
            ((in_flight - offset) as usize).min(size as usize)
        } else {
            0
        };
        if want > data.len() {
            data.resize(want, 0);
        }
        let writes = self.writes.lock().unwrap();
        for st in writes.values() {
            if st.path == path {
                st.chunks.overlay_range(&mut data, offset);
            }
        }
        Ok(data)
    }

    fn start_write(&self, path: &str) -> Result<u64, Error> {
        // Bypass caches where possible: the version we capture now decides
        // optimistic concurrency at commit time, and the node id becomes
        // the session's stable anchor (the commit routes by id).
        let node = self.fs.lookup(path)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
        self.writes.lock().unwrap().insert(
            fh,
            PendingWrite::new(path.to_string(), node.id, node.version),
        );
        Ok(fh)
    }

    /// Detach every session opened on `path` to detached entry `ino`:
    /// snapshot the base content (read through the name while it still
    /// exists - callers MUST run this BEFORE the name dies), fold the
    /// sessions' unflushed chunks into the shared entry, and mark the
    /// sessions orphaned+detached. Subsequent reads/writes on those
    /// handles are served from the detached entry until the last close.
    fn detach_sessions(&self, path: &str, ino: u64) {
        // Take each victim's unflushed chunks NOW: after the name dies they
        // would never be committed, and dropping them would be exactly the
        // silent loss K2 forbids. fh order keeps the merge deterministic
        // (last write wins on an overlapping offset, matching fd order).
        let mut folded: Vec<std::collections::BTreeMap<u64, Vec<u8>>> = Vec::new();
        let victims = {
            let mut writes = self.writes.lock().unwrap();
            let mut fhs: Vec<u64> = writes
                .iter()
                .filter(|(_, st)| st.path == path && !st.orphaned)
                .map(|(fh, _)| *fh)
                .collect();
            fhs.sort_unstable();
            for fh in &fhs {
                if let Some(st) = writes.get_mut(fh) {
                    st.orphaned = true;
                    st.path.clear();
                    st.detached_ino = Some(ino);
                    folded.push(st.chunks.drain());
                }
            }
            fhs.len()
        };
        if victims == 0 {
            return;
        }
        // Entry exists before any detached write can arrive (write callbacks
        // lock this map); the base snapshot fills in below, outside both locks.
        self.detached
            .lock()
            .unwrap()
            .entry(ino)
            .or_insert_with(|| Detached {
                base: Vec::new(),
                chunks: crate::writeback::ChunkMap::new(),
                refs: 0,
            });
        let size = self.fs.lookup(path).map(|n| n.size as u64).unwrap_or(0);
        let base = self.fs.read(path, 0, size).unwrap_or_default();
        let mut det = self.detached.lock().unwrap();
        if let Some(d) = det.get_mut(&ino) {
            if d.base.is_empty() {
                d.base = base;
            }
            for batch in folded {
                for (off, data) in batch {
                    d.chunks.insert(off, data);
                }
            }
            d.refs += victims;
        }
    }

    /// Read from a detached inode (name gone, handles alive): base snapshot
    /// + every surviving handle's writes.
    fn detached_read(&self, ino: u64, offset: u64, size: u64) -> Option<Vec<u8>> {
        let det = self.detached.lock().unwrap();
        let d = det.get(&ino)?;
        let eof = (d.base.len() as u64).max(d.chunks.max_end());
        let want = if eof > offset {
            ((eof - offset) as usize).min(size as usize)
        } else {
            0
        };
        if want == 0 {
            return Some(Vec::new());
        }
        let mut full = d.base.clone();
        full.resize(eof as usize, 0);
        d.chunks.overlay_range(&mut full, offset);
        let from = (offset as usize).min(full.len());
        let to = (from + want).min(full.len());
        Some(full[from..to].to_vec())
    }

    /// Stat a detached inode: EOF = base snapshot size vs any surviving
    /// handle's write end, whichever is larger.
    fn detached_attr(&self, ino: INodeNo) -> Option<FileAttr> {
        let det = self.detached.lock().unwrap();
        let d = det.get(&ino.0)?;
        let size = (d.base.len() as u64).max(d.chunks.max_end());
        Some(FileAttr {
            ino,
            size,
            blocks: size.div_ceil(4096),
            atime: std::time::SystemTime::now(),
            mtime: std::time::SystemTime::now(),
            ctime: std::time::SystemTime::now(),
            crtime: std::time::SystemTime::now(),
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        })
    }

    /// Drop one reference on a detached inode; the last close reaps the
    /// bytes (POSIX: an anonymous inode lives until its last fd closes).
    fn release_detached(&self, ino: u64) {
        let mut det = self.detached.lock().unwrap();
        if let Some(d) = det.get_mut(&ino) {
            d.refs -= 1;
            if d.refs == 0 {
                det.remove(&ino);
            }
        }
    }

    /// Commit a buffered write (flush/release). Idempotent per handle.
    ///
    /// Chunk lifecycle (drain-on-commit): the pending chunks are TAKEN out
    /// of the session for the duration of the I/O, so chunks written by the
    /// kernel during the commit land in a fresh map instead of being
    /// silently merged with already-uploading data.
    ///
    /// Failure recovery: EVERY error after the drain puts the drained
    /// chunks back (writeback::ChunkMap::restore_under - chunks that
    /// arrived during the failed commit are newer and win) and re-bases
    /// the session's CAS anchor to the version observed at attempt start,
    /// so a retry flush can succeed. Without this a failed flush would
    /// drop the drained data and the next flush would "succeed" on an
    /// empty batch - silent data loss.
    ///
    /// Success: the session becomes committed ONLY when no new chunks
    /// arrived during the commit window; a write-during-commit batch is
    /// left for the next flush, CAS-anchored on the fresh version.
    fn commit_write(&self, fh: u64) -> Result<(), Error> {
        let snapshot = {
            let mut writes = self.writes.lock().unwrap();
            let Some(state) = writes.get_mut(&fh) else {
                return Ok(()); // read-only handle: nothing to do
            };
            if state.committed {
                return Ok(());
            }
            if state.orphaned {
                // Detached: the bytes are held in the shared detached entry
                // until the last handle closes - nothing to upload (the name
                // is gone and must not resurrect), nothing to drop. Draining
                // here would silently discard writes that already succeeded,
                // which K2 forbids. Idempotent like every flush.
                return Ok(());
            }
            let drained = state.chunks.drain();
            if drained.is_empty() {
                // A write-intent open that never wrote: commit nothing, or
                // every such open+close would re-upload the whole file.
                state.committed = true;
                return Ok(());
            }
            (
                state.path.clone(),
                state.node_id,
                state.expected_version,
                drained,
            )
        };
        let (path, node_id, expected, drained) = snapshot;
        let fail = |e: Error, observed: Option<i64>| -> Error {
            let mut writes = self.writes.lock().unwrap();
            if let Some(state) = writes.get_mut(&fh) {
                state.chunks.restore_under(&drained);
                if let Some(v) = observed {
                    state.rebase(v);
                }
                // The app now knows this flush failed. A later EXPLICIT
                // retry is the app's decision (A7 semantics); the implicit
                // release-time retry must not flip the reported outcome
                // (D4: a CAS loser must not win through close).
                state.flush_failed = true;
            }
            e
        };
        // Write-back overlay: final content = the file's CURRENT committed
        // content with the drained chunks overlaid at their offsets (the
        // kernel only sends dirty pages - chunks alone would zero-fill the
        // base and destroy existing data). Everything routes by the
        // session's STABLE node id: a rename by another client cannot
        // detach this commit from its file, and a deleted identity fails
        // with IdentityGone instead of touching the path successor.
        let node_now = match self.fs.lookup_by_id(node_id, &path) {
            Ok(n) => n,
            Err(e) => return Err(fail(e, None)),
        };
        let mut content = match self.fs.read_by_id(node_id, &path, 0, node_now.size as u64) {
            Ok(c) => c,
            Err(Error::ObjectMissing(_)) => Vec::new(),
            Err(e) => return Err(fail(e, Some(node_now.version))),
        };
        drained_overlay(&drained, &mut content);
        let mut session = match self.fs.open_write_by_id(node_id, &path, Some(expected)) {
            Ok(s) => s,
            Err(e) => return Err(fail(e, Some(node_now.version))),
        };
        if let Err(e) = session.write(0, &content) {
            return Err(fail(e, Some(node_now.version)));
        }
        match session.commit() {
            Ok(committed_node) => {
                let end = {
                    let mut writes = self.writes.lock().unwrap();
                    if let Some(state) = writes.get_mut(&fh) {
                        // chunks stay drained; a fresh kernel write starts a
                        // new batch - but if one ALREADY arrived during the
                        // commit, the session is NOT done (next flush commits
                        // it against the re-based version).
                        state.note_commit_success(committed_node.version);
                    }
                    // Inline scan: visible_eof would re-lock `writes` (a
                    // std Mutex is not reentrant) and deadlock this very
                    // flush - the exact bug fixed on the Windows adapter
                    // first; do not "simplify" this back into the helper.
                    let mut e = committed_node.size as u64;
                    for st in writes.values() {
                        if st.path == path {
                            e = e.max(st.chunks.max_end());
                        }
                    }
                    e
                };
                self.pending.rebase(&path, end);
                // The commit produced the freshest possible node: refresh
                // the readdirplus cache so the next stat does not pay an
                // RPC (and never sees the pre-write size).
                self.attr_cache
                    .lock()
                    .unwrap()
                    .insert(path.clone(), (committed_node, std::time::Instant::now()));
                Ok(())
            }
            Err(e) => Err(fail(e, Some(node_now.version))),
        }
    }
}

/// Overlay drained chunks onto the base content (zero-filling gaps).
fn drained_overlay(drained: &std::collections::BTreeMap<u64, Vec<u8>>, content: &mut Vec<u8>) {
    for (&off, data) in drained.iter() {
        let end = off as usize + data.len();
        if end > content.len() {
            content.resize(end, 0);
        }
        content[off as usize..end].copy_from_slice(data);
    }
}

impl Filesystem for FuseAdapter {
    fn destroy(&mut self) {
        // Unmount is the only reliable exit of the mount process - flush
        // the benchmark instrument to LIGHTFS_METRICS_FILE if configured.
        crate::metrics::dump_to_file_if_configured();
    }

    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let _g = crate::metrics::scope("fuse.lookup");
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        // readdirplus: a fresh listing already told us this child's
        // metadata; serve the kernel's lookup storm without one RPC per
        // file. Same freshness bound as the kernel attr cache; local
        // mutations drop the whole cache (see dirent_invalidate).
        let cached = self
            .attr_cache
            .lock()
            .unwrap()
            .get(&path)
            .filter(|(_, t)| t.elapsed() < self.attr_ttl)
            .map(|(n, _)| n.clone());
        if let Some(node) = cached {
            crate::metrics::observe("fuse.lookup.cache_hit", Duration::ZERO);
            self.remember(node.id as u64, &parent_path, name);
            reply.entry(
                &self.attr_ttl,
                &attr_for(&node, self.uid, self.gid),
                fuser::Generation(0),
            );
            return;
        }
        match self.fs.lookup(&path) {
            Ok(node) => {
                self.remember(node.id as u64, &parent_path, name);
                self.attr_cache
                    .lock()
                    .unwrap()
                    .insert(path, (node.clone(), std::time::Instant::now()));
                reply.entry(
                    &self.attr_ttl,
                    &attr_for(&node, self.uid, self.gid),
                    fuser::Generation(0),
                );
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let _g = crate::metrics::scope("fuse.getattr");
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => {
                // Nameless inode with surviving handles (detached): the
                // kernel revalidates attributes BEFORE a read and would
                // short-circuit the whole I/O with ENOENT otherwise - so
                // a detached entry must serve stat.
                if let Some(attr) = self.detached_attr(ino) {
                    return reply.attr(&self.attr_ttl, &attr);
                }
                return reply.error(e);
            }
        };
        // Same readdirplus cache as lookup (git's stat loop lands here
        // once the kernel dentry cache is warm). The in-flight fixup
        // below keeps dirty writes coherent; a completed write refreshes
        // the entry itself (see commit_write), and namespace mutations
        // drop the whole cache.
        let node = {
            let cached = self
                .attr_cache
                .lock()
                .unwrap()
                .get(&path)
                .filter(|(_, t)| t.elapsed() < self.attr_ttl)
                .map(|(n, _)| n.clone());
            match cached {
                Some(n) => {
                    crate::metrics::observe("fuse.getattr.cache_hit", Duration::ZERO);
                    n
                }
                None => match self.fs.lookup_by_id(ino.0 as i64, &path) {
                    Ok(n) => {
                        self.attr_cache
                            .lock()
                            .unwrap()
                            .insert(path.clone(), (n.clone(), std::time::Instant::now()));
                        n
                    }
                    Err(e) => return reply.error(errno(&e)),
                },
            }
        };
        let mut attr = attr_for(&node, self.uid, self.gid);
        let in_flight = self.pending.get(&path);
        if in_flight > node.size as u64 {
            // Write-back model: report the in-flight EOF like the
            // kernel page cache reports dirty size. Windows appends
            // derive offsets from this; stat coherence expects it.
            attr.size = in_flight;
        }
        reply.attr(&self.attr_ttl, &attr)
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let _g = crate::metrics::scope("fuse.setattr");
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        if let Some(new_size) = size {
            // truncate: EXTEND zero-fills beyond the current content; SHRINK
            // keeps the prefix. Either way the rewrite is committed with the
            // open write sessions re-based to the new version — otherwise
            // the data flush would conflict with the truncation commit and
            // lose the user's data.
            let node_now = match self.fs.lookup_by_id(ino.0 as i64, &path) {
                Ok(n) => n,
                Err(e) => return reply.error(errno(&e)),
            };
            let cur_len = (node_now.size as u64).min(new_size);
            let commit = self
                .fs
                .read_by_id(ino.0 as i64, &path, 0, cur_len)
                .and_then(|mut content| {
                    if new_size > content.len() as u64 {
                        // extension: zero-fill (ftruncate semantics)
                        content.resize(new_size as usize, 0);
                    }
                    // CAS on the version we just observed: a concurrent commit
                    // between the lookup and here fails the truncate instead of
                    // silently clobbering it.
                    let mut ws =
                        self.fs
                            .open_write_by_id(ino.0 as i64, &path, Some(node_now.version))?;
                    ws.write(0, &content)?;
                    ws.commit()
                });
            match commit {
                Ok(new_node) => {
                    let mut writes = self.writes.lock().unwrap();
                    for st in writes.values_mut() {
                        if st.path == path {
                            st.expected_version = new_node.version;
                            // ftruncate semantics: the kernel discards dirty
                            // pages beyond the new EOF and will NOT resend
                            // them. Pending chunks beyond new_size must be
                            // dropped, and chunks STRADDLING the new EOF
                            // trimmed to their pre-EOF prefix, or the next
                            // flush resurrects truncated-away data (review
                            // round 8 finding; straddle case review round 9).
                            st.chunks.trim_truncate(new_size);
                        }
                    }
                    // the committed size is authoritative from now on
                    self.pending.drop_path(&path);
                }
                Err(e) => return reply.error(errno(&e)),
            }
        }
        let mtime_secs = mtime.map(|t| match t {
            fuser::TimeOrNow::SpecificTime(s) => s
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            fuser::TimeOrNow::Now => SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        });
        match self.fs.setattr(&path, mode, mtime_secs) {
            Ok(node) => {
                self.attr_cache
                    .lock()
                    .unwrap()
                    .insert(path.clone(), (node.clone(), std::time::Instant::now()));
                reply.attr(&self.attr_ttl, &attr_for(&node, self.uid, self.gid))
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let _g = crate::metrics::scope("fuse.readdir");
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let cache_key = (ino.0, _fh.0);
        // Sub-phase accounting (closes the time ledger inside this
        // callback): lock wait, offset==0 fetch (client RPC + client-side
        // processing), snapshot assembly, offset>0 replay. Any residual
        // between the fuse.readdir total and these rows is kernel-side
        // queueing before the callback runs.
        let entries = {
            let lock_t0 = std::time::Instant::now();
            let mut cache = self.dir_cache.lock().unwrap();
            crate::metrics::observe("fuse.readdir.lock", lock_t0.elapsed());
            // evict stale snapshots lazily (a walk that outlives the TTL
            // gets a fresh list on its next offset==0 page anyway)
            cache.retain(|_, (_, t)| t.elapsed() < self.dir_snapshot_ttl);
            if offset == 0 {
                // C1 experiment knob: TTL>0 suppresses offset==0 refetches
                // across walks (default 0 = fetch every time). Stale
                // entries age out here; LOCAL mutations drop everything
                // (see dirent_invalidate), so only CROSS-client changes
                // can be observed late.
                let cached = if self.dirent_ttl > Duration::ZERO {
                    let mut listing = self.dirent_cache.lock().unwrap();
                    listing.retain(|_, (_, t)| t.elapsed() < self.dirent_ttl);
                    listing.get(&path).map(|(c, _)| {
                        crate::metrics::observe("fuse.readdir.dirent_hit", Duration::ZERO);
                        c.clone()
                    })
                } else {
                    None
                };
                let children = match cached {
                    Some(c) => c,
                    None => {
                        let fetch_t0 = std::time::Instant::now();
                        let c = match self.fs.readdir(&path) {
                            Ok(c) => c,
                            Err(e) => return reply.error(errno(&e)),
                        };
                        crate::metrics::observe("fuse.readdir.fetch", fetch_t0.elapsed());
                        if self.dirent_ttl > Duration::ZERO {
                            self.dirent_cache
                                .lock()
                                .unwrap()
                                .insert(path.clone(), (c.clone(), std::time::Instant::now()));
                        }
                        c
                    }
                };
                // The listing carries each child's metadata: seed the
                // lookup cache so the upcoming per-file lookups stay
                // client-side (readdirplus).
                self.seed_attrs(&path, &children);
                let build_t0 = std::time::Instant::now();
                let entries = self.build_dir_entries(ino.0, &path, &children);
                cache.insert(cache_key, (entries.clone(), std::time::Instant::now()));
                crate::metrics::observe("fuse.readdir.build", build_t0.elapsed());
                entries
            } else {
                let replay_t0 = std::time::Instant::now();
                match cache.get(&cache_key) {
                    Some((entries, _)) => {
                        let out = entries.clone();
                        crate::metrics::observe("fuse.readdir.replay", replay_t0.elapsed());
                        out
                    }
                    None => {
                        // page without a snapshot (kernel restart, cache
                        // eviction): refetch and replay from the top is
                        // WRONG for cookies - fail the walk instead
                        return reply.error(errno(&Error::InvalidArgument(
                            "readdir snapshot evicted".into(),
                        )));
                    }
                }
            }
        };
        for (i, (child_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*child_ino), (i + 1) as u64, *kind, name.as_str()) {
                break;
            }
        }
        reply.ok();
    }

    fn releasedir(
        &self,
        _req: &Request,
        ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        reply: ReplyEmpty,
    ) {
        // Drop this handle's snapshot immediately: the lazy 30s eviction
        // in readdir only runs on the next readdir call, so a closed
        // handle's snapshot (potentially thousands of entries) would
        // otherwise linger for up to the full TTL.
        self.dir_cache.lock().unwrap().remove(&(ino.0, fh.0));
        reply.ok();
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let _g = crate::metrics::scope("fuse.mkdir");
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        match self.fs.mkdir(&path, mode & 0o7777) {
            Ok(node) => {
                self.dirent_invalidate();
                self.remember(node.id as u64, &parent_path, name);
                reply.entry(
                    &self.attr_ttl,
                    &attr_for(&node, self.uid, self.gid),
                    fuser::Generation(0),
                );
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        link: &std::path::Path,
        reply: ReplyEntry,
    ) {
        let _g = crate::metrics::scope("fuse.symlink");
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        // The kernel hands us the raw target bytes; store verbatim. All
        // resolution happens kernel-side afterwards (it re-resolves and
        // comes back with lookups), so no Core-side following here.
        let target = link.to_string_lossy().to_string();
        match self.fs.symlink(&target, &path) {
            Ok(node) => {
                self.dirent_invalidate();
                self.remember(node.id as u64, &parent_path, name);
                reply.entry(
                    &self.attr_ttl,
                    &attr_for(&node, self.uid, self.gid),
                    fuser::Generation(0),
                );
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn readlink(&self, _req: &Request, ino: INodeNo, reply: ReplyData) {
        let _g = crate::metrics::scope("fuse.readlink");
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        match self.fs.readlink(&path) {
            // The kernel NUL-terminates; hand back the raw target bytes.
            Ok(target) => reply.data(target.as_bytes()),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        mode: u32,
        umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let _g = crate::metrics::scope("fuse.create");
        let _ = umask;
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        match self.fs.create(&path, mode & 0o7777).and_then(|node| {
            let fh = self.start_write(&path)?;
            Ok((node, fh))
        }) {
            Ok((node, fh)) => {
                self.dirent_invalidate();
                self.pending.drop_path(&path);
                self.remember(node.id as u64, &parent_path, name);
                reply.created(
                    &self.attr_ttl,
                    &attr_for(&node, self.uid, self.gid),
                    fuser::Generation(0),
                    fuser::FileHandle(fh),
                    fuser::FopenFlags::empty(),
                );
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, flags: OpenFlags, reply: ReplyOpen) {
        let _g = crate::metrics::scope("fuse.open");
        let write_intent = flags.acc_mode() != fuser::OpenAccMode::O_RDONLY;
        if write_intent {
            let path = match self.path_of(ino) {
                Ok(p) => p,
                Err(e) => return reply.error(e),
            };
            match self.start_write(&path) {
                Ok(fh) => reply.opened(fuser::FileHandle(fh), fuser::FopenFlags::empty()),
                Err(e) => reply.error(errno(&e)),
            }
        } else {
            reply.opened(fuser::FileHandle(0), fuser::FopenFlags::empty());
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let _g = crate::metrics::scope("fuse.read");
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => {
                // Nameless inode with surviving handles (detached): serve
                // the bytes we hold. Without an entry the inode is truly
                // gone - ENOENT stands.
                if let Some(data) = self.detached_read(ino.0, offset, size as u64) {
                    return reply.data(&data);
                }
                return reply.error(e);
            }
        };
        match self.read_with_session(ino.0, &path, offset, size as u64) {
            Ok(data) => reply.data(&data),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn write(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyWrite,
    ) {
        let _g = crate::metrics::scope("fuse.write");
        let mut writes = self.writes.lock().unwrap();
        let Some(state) = writes.get_mut(&fh.0) else {
            return reply.error(Errno::EIO);
        };
        // FUSE dup-close semantics: FLUSH arrives on close of ANY fd copy.
        // `echo x > f` in bash closes the redirect fd before the child
        // writes, so a committed session may receive further writes — re-arm
        // it by re-reading the current version (post-flush-commit).
        // The orphaned check MUST come first: a committed+orphaned session
        // carries a blanked path, and that lookup would turn into a
        // spurious ENOENT for a write POSIX says must succeed.
        if state.orphaned {
            // Detached inode (unlinked / renamed over): writes land in the
            // shared detached entry and STAY there until the last handle
            // closes. Success here means the bytes are actually held -
            // silently accepting and later dropping is exactly what K2
            // forbids. No entry = the last close already reaped the inode.
            match state.detached_ino {
                Some(dino) => {
                    let mut det = self.detached.lock().unwrap();
                    match det.get_mut(&dino) {
                        Some(d) => {
                            d.chunks.insert(offset, data.to_vec());
                            return reply.written(data.len() as u32);
                        }
                        None => return reply.error(Errno::EIO),
                    }
                }
                None => return reply.error(Errno::EIO),
            }
        }
        if state.committed {
            match self.fs.lookup_by_id(state.node_id, &state.path) {
                Ok(node) => state.expected_version = node.version,
                Err(e) => return reply.error(errno(&e)),
            }
            state.committed = false;
        }
        state.chunks.insert(offset, data.to_vec());
        self.pending
            .advance(&state.path, offset + data.len() as u64);
        reply.written(data.len() as u32);
    }

    fn flush(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _lock_owner: LockOwner,
        reply: ReplyEmpty,
    ) {
        let _g = crate::metrics::scope("fuse.flush");
        // Diagnostic (Phase F): how many flushes commit concurrently, sampled
        // once per flush as the observed level. Metrics-gated; the always-on
        // cost is one atomic add/sub and one histogram entry when enabled.
        static FLUSH_INFLIGHT: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let lvl = FLUSH_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        crate::metrics::observe(
            "fuse.flush.inflight",
            std::time::Duration::from_micros(lvl as u64),
        );
        // FLUSH is the kernel's CLOSE-side flush (fsync goes to the fsync
        // callback). A session whose earlier flush/fsync already reported
        // an error must not have that outcome silently flipped here: a
        // close-time commit would turn "error + not committed" into "error
        // + committed anyway" (D4: a CAS loser must not win through
        // close). An explicit retry remains available through fsync (A7).
        let gated = {
            let writes = self.writes.lock().unwrap();
            writes.get(&fh.0).map(|st| st.flush_failed).unwrap_or(false)
        };
        let r = if gated {
            Err(Error::Io(
                "earlier flush already reported failure; close-time retry refused (explicit fsync retry is still available)".into(),
            ))
        } else {
            self.commit_write(fh.0)
        };
        FLUSH_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        match r {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        let _g = crate::metrics::scope("fuse.release");
        // Anything flush did not commit commits here (best effort). The
        // kernel ignores release's return value, so a failed final commit
        // is only observable in this log - flush()/fsync() are the paths
        // that surface errors to applications.
        let (path, detached_ino) = {
            let writes = self.writes.lock().unwrap();
            writes
                .get(&fh.0)
                .map(|st| (st.path.clone(), st.detached_ino))
                .unwrap_or((String::new(), None))
        };
        // orphaned sessions carry a blank path: nothing left to recompute,
        // and the name may belong to a different file now
        let path = if path.is_empty() { None } else { Some(path) };
        let already_failed = {
            let writes = self.writes.lock().unwrap();
            writes.get(&fh.0).map(|st| st.flush_failed).unwrap_or(false)
        };
        if already_failed {
            // The application already received a flush/fsync error for this
            // session. Retrying here could commit data the app was told did
            // NOT commit (CAS conflict loser winning through close) - the
            // exact "error + committed anyway" lie M-phase forbids. An
            // explicit retry flush remains the app's escape hatch (A7).
            eprintln!(
                "lightfs-fuse: WARNING release: fh={} ({}) had a FAILED flush; close-time retry REFUSED - data not persisted",
                fh.0,
                path.as_deref().unwrap_or("?")
            );
        } else if let Err(e) = self.commit_write(fh.0) {
            eprintln!(
                "lightfs-fuse: WARNING release: final commit FAILED for fh={} ({}): {e} - data not persisted",
                fh.0,
                path.as_deref().unwrap_or("?")
            );
        }
        self.writes.lock().unwrap().remove(&fh.0);
        if let Some(dino) = detached_ino {
            // The handle is gone: drop its reference; the LAST close reaps
            // the detached bytes (POSIX anonymous-inode lifetime).
            self.release_detached(dino);
        }
        if let Some(path) = path {
            // recompute from remaining sessions: a failed final commit must
            // not leave a phantom in-flight EOF (readers would see a hole)
            let end = self.visible_eof(&path, 0);
            self.pending.recompute(&path, end);
        }
        reply.ok();
    }

    fn fsync(
        &self,
        _req: &Request,
        _ino: INodeNo,
        fh: FileHandle,
        datasync: bool,
        reply: ReplyEmpty,
    ) {
        let _g = crate::metrics::scope("fuse.fsync");
        // Our commit writes content AND bumps the metadata version in one
        // atomic CAS, so datasync and full fsync both reduce to committing
        // the session's pending chunks. Errors propagate to fsync(2).
        let _ = datasync;
        match self.commit_write(fh.0) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _g = crate::metrics::scope("fuse.unlink");
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        // Detach open handles BEFORE the name (and its object) die: the
        // base snapshot reads through the still-live name.
        if let Some(ino) = self.ino_of_path(&path) {
            self.detach_sessions(&path, ino);
        }
        match self.fs.unlink(&path) {
            Ok(()) => {
                self.dirent_invalidate();
                let victim = self.ino_of_path(&path);
                if let Some(ino) = victim {
                    self.inodes.lock().unwrap().remove(&ino);
                }
                // A handle opened before the unlink must not resurrect the
                // name at commit time: orphan its session (batch drops,
                // close(2) succeeds - POSIX unlink semantics for open fds).
                // `path` is blanked so path-scanning helpers cannot alias
                // a new file created under the recycled name.
                {
                    let mut writes = self.writes.lock().unwrap();
                    for st in writes.values_mut() {
                        if st.path == path {
                            st.orphaned = true;
                            st.path.clear();
                        }
                    }
                }
                self.pending.drop_path(&path);
                reply.ok()
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let _g = crate::metrics::scope("fuse.rmdir");
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        match self.fs.rmdir(&path) {
            Ok(()) => {
                self.dirent_invalidate();
                self.forget_path(&path);
                reply.ok()
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: ReplyEmpty,
    ) {
        let _g = crate::metrics::scope("fuse.rename");
        let old_parent = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let new_parent = match self.path_of(newparent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let old_path = Self::child_path(&old_parent, name);
        let new_path = Self::child_path(&new_parent, newname);
        // An overwritten destination loses its name with open handles
        // possibly held: detach BEFORE the rename moves the name away
        // (base snapshot + chunk fold happen through the live name).
        let dest_ino = self.ino_of_path(&new_path);
        if let Some(dino) = dest_ino {
            self.detach_sessions(&new_path, dino);
        }
        match self.fs.rename(&old_path, &new_path) {
            Ok(()) => {
                self.dirent_invalidate();
                self.pending.drop_path(&old_path);
                self.pending.drop_path(&new_path);
                // K1: the overwritten inode must NOT resolve to the new
                // name any more - its stale mapping would alias reads of
                // the incoming file (detached handles are served from the
                // detached entry, not from path lookups).
                if let Some(dino) = dest_ino {
                    self.inodes.lock().unwrap().remove(&dino);
                }
                // Open write handles ride along: repoint their paths so
                // the commit looks the file up under its CURRENT name
                // (rename_node only moves parent/name - the CAS anchor
                // version is untouched). A handle on an overwritten
                // destination just lost its inode's last name: orphan it
                // (batch drops, close succeeds) so it can neither error
                // the close nor alias the incoming file under the
                // recycled name. rename(a, a) is a no-op - never orphan
                // or repoint the mover itself.
                if new_path != old_path {
                    let mut writes = self.writes.lock().unwrap();
                    for st in writes.values_mut() {
                        if st.path == new_path {
                            st.orphaned = true;
                            st.path.clear();
                        }
                    }
                    let prefix = format!("{old_path}/");
                    for st in writes.values_mut() {
                        if st.path == old_path {
                            st.path = new_path.clone();
                        } else if st.path.starts_with(&prefix) {
                            st.path = format!("{new_path}{}", &st.path[old_path.len()..]);
                        }
                    }
                }
                // Repoint cached paths of the moved subtree (only entries the
                // kernel has looked up are in the map).
                let mut map = self.inodes.lock().unwrap();
                let prefix = if old_path.ends_with('/') {
                    old_path.clone()
                } else {
                    format!("{old_path}/")
                };
                let mut rev = self.paths.lock().unwrap();
                let updates: Vec<(u64, String, String)> = map
                    .iter()
                    .filter(|(_, p)| {
                        let ps = p.to_string_lossy();
                        ps == old_path || ps.starts_with(&prefix)
                    })
                    .map(|(ino, p)| {
                        let ps = p.to_string_lossy().to_string();
                        let new_ps = format!("{new_path}{}", &ps[old_path.len()..]);
                        (*ino, ps, new_ps)
                    })
                    .collect();
                for (ino, old_ps, new_ps) in updates {
                    rev.remove(&old_ps);
                    map.insert(ino, PathBuf::from(&new_ps));
                    rev.insert(new_ps, ino);
                }
                drop(rev);
                reply.ok()
            }
            Err(e) => reply.error(errno(&e)),
        }
    }
}

/// FUSE callback pool size. LIGHTFS_FUSE_THREADS overrides; unset keeps the
/// historical default (available_parallelism, clamped to 2..=16 — fuser
/// defaults to a single callback thread, which serializes the whole mount at
/// one in-flight op). Unlike LIGHTFS_SERVER_WORKERS, an invalid explicit
/// value is a hard error: silently falling back would make a measured
/// concurrency knob lie about what ran.
fn callback_threads() -> crate::error::Result<usize> {
    let Ok(v) = std::env::var("LIGHTFS_FUSE_THREADS") else {
        return Ok(std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .clamp(2, 16));
    };
    let n: usize = v.trim().parse().map_err(|_| {
        Error::Config(format!(
            "LIGHTFS_FUSE_THREADS: invalid value {v:?} (expected integer 2-16)"
        ))
    })?;
    if !(2..=16).contains(&n) {
        return Err(Error::Config(format!(
            "LIGHTFS_FUSE_THREADS: {n} out of range (supported: 2-16)"
        )));
    }
    Ok(n)
}

/// Mount entry point: `lightfs mount [--config PATH] [--allow-other] MOUNTPOINT`.
pub fn mount(
    fs: std::sync::Arc<dyn FsApi>,
    mountpoint: &str,
    allow_other: bool,
) -> crate::error::Result<()> {
    crate::metrics::init_from_env();
    let adapter = FuseAdapter::new(fs);
    // fuser 0.18: Config is non_exhaustive (mutate the default), and
    // allow_other moved from a mount option into SessionACL.
    let mut config = fuser::Config::default();
    config.mount_options = vec![
        fuser::MountOption::FSName("lightfs".into()),
        fuser::MountOption::DefaultPermissions,
    ];
    config.acl = if allow_other {
        fuser::SessionACL::All
    } else {
        fuser::SessionACL::Owner
    };
    // Scale the callback pool (see callback_threads); the adapter is Sync
    // (all mutable state sits behind Mutexes).
    config.n_threads = Some(callback_threads()?);
    let result = fuser::mount(adapter, std::path::Path::new(mountpoint), &config)
        .map_err(|e| crate::error::Error::Config(format!("mount failed: {e}")));
    // Session over (unmount or kernel abort): flush the benchmark
    // instrument even if the destroy callback did not run.
    crate::metrics::dump_to_file_if_configured();
    result
}
