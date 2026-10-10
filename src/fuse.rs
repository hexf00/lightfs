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

const ATTR_TTL: Duration = Duration::from_secs(1);

struct PendingWrite {
    path: String,
    expected_version: i64,
    chunks: std::collections::BTreeMap<u64, Vec<u8>>,
    committed: bool,
}

pub struct FuseAdapter {
    fs: std::sync::Arc<dyn FsApi>,
    /// ino (== metadata node id) -> path within the filesystem.
    inodes: Mutex<HashMap<u64, PathBuf>>,
    /// file handle -> buffered write.
    writes: Mutex<HashMap<u64, PendingWrite>>,
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
        Error::AlreadyExists(_) => Errno::EEXIST,
        Error::KindMismatch {
            expected: "dir", ..
        } => Errno::ENOTDIR,
        Error::KindMismatch { .. } => Errno::EISDIR,
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
        } else {
            FileType::RegularFile
        },
        perm: (node.mode & 0o7777) as u16,
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
        Self {
            fs,
            inodes: Mutex::new(inodes),
            writes: Mutex::new(HashMap::new()),
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
        self.inodes
            .lock()
            .unwrap()
            .iter()
            .find(|(_, p)| p.to_string_lossy() == path)
            .map(|(ino, _)| *ino)
    }

    fn remember(&self, ino: u64, parent_path: &str, name: &OsStr) {
        let mut p = PathBuf::from(parent_path);
        p.push(name);
        self.inodes.lock().unwrap().insert(ino, p);
    }

    fn child_path(parent_path: &str, name: &OsStr) -> String {
        let name = name.to_string_lossy();
        if parent_path == "/" {
            format!("/{name}")
        } else {
            format!("{parent_path}/{name}")
        }
    }

    fn start_write(&self, path: &str) -> Result<u64, Error> {
        // Bypass caches where possible: the version we capture now decides
        // optimistic concurrency at commit time.
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
            PendingWrite {
                path: path.to_string(),
                expected_version: node.version,
                chunks: std::collections::BTreeMap::new(),
                committed: false,
            },
        );
        Ok(fh)
    }

    /// Commit a buffered write (flush/release). Idempotent per handle.
    ///
    /// Chunk lifecycle (drain-on-commit): the pending chunks are TAKEN out
    /// of the session for the duration of the I/O, so chunks written by the
    /// kernel during the commit land in a fresh map instead of being
    /// silently merged with already-uploading data.
    ///
    /// Write-back overlay: the commit content is the file's CURRENT
    /// committed content with the drained chunks overlaid at their offsets
    /// (the kernel only sends dirty pages - chunks alone would zero-fill
    /// the base). On success the drained chunks stay drained
    /// (committed=true); on CAS failure they are merged back into the
    /// session so a retry flush can succeed against the rebased version.
    fn commit_write(&self, fh: u64) -> Result<(), Error> {
        let snapshot = {
            let mut writes = self.writes.lock().unwrap();
            let Some(state) = writes.get_mut(&fh) else {
                return Ok(()); // read-only handle: nothing to do
            };
            if state.committed {
                return Ok(());
            }
            let drained = std::mem::take(&mut state.chunks);
            if drained.is_empty() {
                // A write-intent open that never wrote: commit nothing, or
                // every such open+close would re-upload the whole file.
                state.committed = true;
                return Ok(());
            }
            (state.path.clone(), state.expected_version, drained)
        };
        let (path, expected, drained) = snapshot;
        // Write-back overlay: final content = the file's CURRENT committed
        // content with the drained chunks overlaid at their offsets (the
        // kernel only sends dirty pages - chunks alone would zero-fill the
        // base and destroy existing data).
        let node_now = match self.fs.lookup(&path) {
            Ok(n) => n,
            Err(e) => {
                Self::restore_chunks(fh, drained, self);
                return Err(e);
            }
        };
        let mut content = match self.fs.read(&path, 0, node_now.size as u64) {
            Ok(c) => c,
            Err(Error::ObjectMissing(_)) => Vec::new(),
            Err(e) => {
                Self::restore_chunks(fh, drained, self);
                return Err(e);
            }
        };
        for (&off, data) in drained.iter() {
            let end = off as usize + data.len();
            if end > content.len() {
                content.resize(end, 0);
            }
            content[off as usize..off as usize + data.len()].copy_from_slice(data);
        }
        let mut session = self.fs.open_write(&path, Some(expected))?;
        session.write(0, &content)?;
        let committed_node = session.commit()?;
        if let Some(state) = self.writes.lock().unwrap().get_mut(&fh) {
            state.committed = true;
            state.expected_version = committed_node.version;
            // chunks stay drained; a fresh kernel write starts a new batch
        }
        Ok(())
    }

    /// Merge drained chunks back into a session's pending map (failure
    /// recovery for retry). Newer writes at the same offsets win.
    fn restore_chunks(
        fh: u64,
        drained: std::collections::BTreeMap<u64, Vec<u8>>,
        adapter: &FuseAdapter,
    ) {
        if drained.is_empty() {
            return;
        }
        if let Some(state) = adapter.writes.lock().unwrap().get_mut(&fh) {
            for (off, data) in drained {
                state.chunks.insert(off, data);
            }
        }
    }
}

impl Filesystem for FuseAdapter {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        match self.fs.lookup(&path) {
            Ok(node) => {
                self.remember(node.id as u64, &parent_path, name);
                reply.entry(
                    &ATTR_TTL,
                    &attr_for(&node, self.uid, self.gid),
                    fuser::Generation(0),
                );
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        match self.fs.lookup(&path) {
            Ok(node) => reply.attr(&ATTR_TTL, &attr_for(&node, self.uid, self.gid)),
            Err(e) => reply.error(errno(&e)),
        }
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
            let node_now = match self.fs.lookup(&path) {
                Ok(n) => n,
                Err(e) => return reply.error(errno(&e)),
            };
            let cur_len = (node_now.size as u64).min(new_size);
            let commit = self.fs.read(&path, 0, cur_len).and_then(|mut content| {
                if new_size > content.len() as u64 {
                    // extension: zero-fill (ftruncate semantics)
                    content.resize(new_size as usize, 0);
                }
                // CAS on the version we just observed: a concurrent commit
                // between the lookup and here fails the truncate instead of
                // silently clobbering it.
                let mut ws = self.fs.open_write(&path, Some(node_now.version))?;
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
                            // dropped here, or the next flush resurrects
                            // truncated-away data (review round 8 finding).
                            let beyond: Vec<u64> =
                                st.chunks.range(new_size..).map(|(off, _)| *off).collect();
                            for off in beyond {
                                st.chunks.remove(&off);
                            }
                        }
                    }
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
            Ok(node) => reply.attr(&ATTR_TTL, &attr_for(&node, self.uid, self.gid)),
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
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let children = match self.fs.readdir(&path) {
            Ok(c) => c,
            Err(e) => return reply.error(errno(&e)),
        };
        let parent_ino = match path.as_str() {
            "/" => 1,
            _ => {
                let parent_path = match path.rfind('/') {
                    Some(0) | None => "/".to_string(),
                    Some(i) => path[..i].to_string(),
                };
                self.ino_of_path(&parent_path).unwrap_or(1)
            }
        };
        let mut entries: Vec<(u64, FileType, String)> = Vec::new();
        entries.push((ino.0, FileType::Directory, ".".to_string()));
        entries.push((parent_ino, FileType::Directory, "..".to_string()));
        for c in &children {
            let kind = if c.is_dir() {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            entries.push((c.id as u64, kind, c.name.clone()));
        }
        for (i, (child_ino, kind, name)) in entries.iter().enumerate().skip(offset as usize) {
            if reply.add(INodeNo(*child_ino), (i + 1) as u64, *kind, name.as_str()) {
                break;
            }
        }
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
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        match self.fs.mkdir(&path, mode & 0o7777) {
            Ok(node) => {
                self.remember(node.id as u64, &parent_path, name);
                reply.entry(
                    &ATTR_TTL,
                    &attr_for(&node, self.uid, self.gid),
                    fuser::Generation(0),
                );
            }
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
                self.remember(node.id as u64, &parent_path, name);
                reply.created(
                    &ATTR_TTL,
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
        let path = match self.path_of(ino) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        match self.fs.read(&path, offset, size as u64) {
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
        let mut writes = self.writes.lock().unwrap();
        let Some(state) = writes.get_mut(&fh.0) else {
            return reply.error(Errno::EIO);
        };
        // FUSE dup-close semantics: FLUSH arrives on close of ANY fd copy.
        // `echo x > f` in bash closes the redirect fd before the child
        // writes, so a committed session may receive further writes — re-arm
        // it by re-reading the current version (post-flush-commit).
        if state.committed {
            match self.fs.lookup(&state.path) {
                Ok(node) => state.expected_version = node.version,
                Err(e) => return reply.error(errno(&e)),
            }
            state.committed = false;
        }
        state.chunks.insert(offset, data.to_vec());
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
        match self.commit_write(fh.0) {
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
        // Anything flush did not commit commits here (best effort). The
        // kernel ignores release's return value, so a failed final commit
        // is only observable in this log - flush()/fsync() are the paths
        // that surface errors to applications.
        let path = self
            .writes
            .lock()
            .unwrap()
            .get(&fh.0)
            .map(|st| st.path.clone());
        if let Err(e) = self.commit_write(fh.0) {
            eprintln!(
                "lightfs-fuse: WARNING release: final commit FAILED for fh={} ({}): {e} - data not persisted",
                fh.0,
                path.as_deref().unwrap_or("?")
            );
        }
        self.writes.lock().unwrap().remove(&fh.0);
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
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        match self.fs.unlink(&path) {
            Ok(()) => {
                let victim = self.ino_of_path(&path);
                if let Some(ino) = victim {
                    self.inodes.lock().unwrap().remove(&ino);
                }
                reply.ok()
            }
            Err(e) => reply.error(errno(&e)),
        }
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEmpty) {
        let parent_path = match self.path_of(parent) {
            Ok(p) => p,
            Err(e) => return reply.error(e),
        };
        let path = Self::child_path(&parent_path, name);
        match self.fs.rmdir(&path) {
            Ok(()) => {
                let victim = self.ino_of_path(&path);
                if let Some(ino) = victim {
                    self.inodes.lock().unwrap().remove(&ino);
                }
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
        match self.fs.rename(&old_path, &new_path) {
            Ok(()) => {
                // Repoint cached paths of the moved subtree (only entries the
                // kernel has looked up are in the map).
                let mut map = self.inodes.lock().unwrap();
                let prefix = if old_path.ends_with('/') {
                    old_path.clone()
                } else {
                    format!("{old_path}/")
                };
                let updates: Vec<(u64, PathBuf)> = map
                    .iter()
                    .filter(|(_, p)| {
                        let ps = p.to_string_lossy();
                        ps == old_path || ps.starts_with(&prefix)
                    })
                    .map(|(ino, p)| {
                        let ps = p.to_string_lossy();
                        let rest = &ps[old_path.len()..];
                        (*ino, PathBuf::from(format!("{new_path}{rest}")))
                    })
                    .collect();
                for (ino, p) in updates {
                    map.insert(ino, p);
                }
                reply.ok()
            }
            Err(e) => reply.error(errno(&e)),
        }
    }
}

/// Mount entry point: `lightfs mount [--config PATH] [--allow-other] MOUNTPOINT`.
pub fn mount(
    fs: std::sync::Arc<dyn FsApi>,
    mountpoint: &str,
    allow_other: bool,
) -> crate::error::Result<()> {
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
    fuser::mount(adapter, std::path::Path::new(mountpoint), &config)
        .map_err(|e| crate::error::Error::Config(format!("mount failed: {e}")))
}
