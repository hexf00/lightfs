//! LocalFs: the client-local authority backend (Phase P).
//!
//! Owns a real directory on the client machine as the AUTHORITATIVE store
//! for one policy rule's subtree. Strict-sync: a write session's commit
//! fsyncs the file (and its parent directory) before returning, so a
//! successful commit is durable on local disk. No cache, no write-back,
//! no lifecycle cleanup - those stay unimplemented, and the policy parser
//! rejects their config on client-local rules outright.
//!
//! Identity: local node ids live in their own domain - negative i64 with
//! bit 63 set (`LOCAL_FLAG | seq`) - so they can never collide with remote
//! node ids, and the router dispatches BY DOMAIN: a negative id is never
//! forwarded to the remote backend. Ids are allocated on first sight and
//! stay stable across renames for the mount's lifetime; they are NOT
//! stable across remounts (only the remote domain persists identity that
//! long - D7 semantics remain remote-only). The id map is what makes
//! by-id operations real: rename updates the mapped path, unlink drops
//! the entry, so a handle bound to a deleted identity fails IdentityGone
//! instead of aliasing the path's new occupant (M3, enforced locally).
//!
//! Root safety (Phase P2, runtime half - shape checks live in the policy
//! parser): the root is canonicalized at build; every operation
//! canonicalizes the operation's parent directory and refuses if it
//! resolves outside the root (symlink escape), and file opens pass
//! O_NOFOLLOW so a symlink planted as a file's final component fails
//! loudly instead of reading through. The name `.lightfs-tmp` at the top
//! of the root is reserved for the commit scratch directory.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::api::WriteSession;
use crate::error::{Error, Result};
use crate::metadata::{Node, NodeKind};

/// Local ids: bit 63 set, so they are negative i64 and can never equal a
/// remote node id (positive SQLite rowids). The router tests the sign.
pub const LOCAL_FLAG: u64 = 1 << 63;

/// Scratch-directory name reserved at the top of every local root.
const RESERVED: &str = ".lightfs-tmp";

#[derive(Default)]
struct IdMap {
    by_id: HashMap<i64, String>,
    by_path: HashMap<String, i64>,
}

pub struct LocalFs {
    /// Canonical root (all symlink components resolved at build time).
    root: PathBuf,
    ids: Mutex<IdMap>,
    /// Shared across every LocalFs of one mount: ids stay unique across
    /// backends so the router can hand a negative id to each backend in
    /// turn ("do you know this id?") without ambiguity. Also names commit
    /// temp files (fetch_add - uniqueness, not identity).
    seq: Arc<AtomicU64>,
}

fn io_err(e: std::io::Error, rel: &str) -> Error {
    #[cfg(unix)]
    if let Some(code) = e.raw_os_error() {
        match code {
            x if x == libc::ENOENT => return Error::NotFound(rel.to_string()),
            x if x == libc::EEXIST => return Error::AlreadyExists(rel.to_string()),
            x if x == libc::ENOTEMPTY => return Error::DirectoryNotEmpty(rel.to_string()),
            x if x == libc::EISDIR => {
                return Error::KindMismatch {
                    path: rel.to_string(),
                    expected: "file",
                    actual: "dir".into(),
                };
            }
            x if x == libc::ENOTDIR => {
                return Error::KindMismatch {
                    path: rel.to_string(),
                    expected: "dir",
                    actual: "file".into(),
                };
            }
            x if x == libc::EINVAL => return Error::InvalidPath(rel.to_string()),
            x if x == libc::ELOOP => {
                return Error::InvalidArgument(format!(
                    "refusing to follow symlink at {rel}: symlink loop"
                ));
            }
            _ => {}
        }
    }
    let _ = rel;
    Error::Io(e.to_string())
}

impl LocalFs {
    /// Open (or lazily create) the authoritative root. Returns the backend
    /// and its canonical path (the caller runs the mount-tree containment
    /// checks on it before the backend goes live).
    pub fn new(root: &Path, seq: Arc<AtomicU64>) -> Result<(Self, PathBuf)> {
        let canon = match std::fs::canonicalize(root) {
            Ok(p) => p,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Lazy-create is allowed (P2); it happens HERE, at the
                // configured path - never by silently picking another one.
                std::fs::create_dir_all(root).map_err(|e| {
                    Error::Config(format!("local root {}: cannot create: {e}", root.display()))
                })?;
                std::fs::canonicalize(root)
                    .map_err(|e| Error::Config(format!("local root {}: {e}", root.display())))?
            }
            Err(e) => return Err(Error::Config(format!("local root {}: {e}", root.display()))),
        };
        std::fs::create_dir_all(canon.join(RESERVED)).map_err(|e| {
            Error::Config(format!("local root {}: scratch dir: {e}", canon.display()))
        })?;
        Ok((
            Self {
                root: canon.clone(),
                ids: Mutex::new(IdMap::default()),
                seq,
            },
            canon,
        ))
    }

    pub fn canonical_root(&self) -> &Path {
        &self.root
    }

    /// Does this backend own `id`? The router asks each backend in turn
    /// for negative ids; a miss is not an error (another backend may own
    /// it), only a miss from ALL of them is IdentityGone.
    pub fn knows(&self, id: i64) -> bool {
        self.ids.lock().unwrap().by_id.contains_key(&id)
    }

    // ------------------------------------------------------------------
    // path plumbing
    // ------------------------------------------------------------------

    fn abs(&self, rel: &str) -> PathBuf {
        if rel.is_empty() {
            self.root.clone()
        } else {
            self.root.join(rel)
        }
    }

    /// Reject relative paths that could leave the root by shape, plus the
    /// reserved scratch name. The router normalizes before stripping the
    /// prefix, so this is defense in depth, not the primary gate.
    fn rel_ok(rel: &str) -> Result<()> {
        if rel.starts_with('/') {
            return Err(Error::InvalidPath(rel.to_string()));
        }
        let mut parts = rel.split('/');
        if parts.next() == Some(RESERVED) {
            return Err(Error::InvalidArgument(format!(
                "{RESERVED} is reserved for commit scratch"
            )));
        }
        for c in parts {
            if c == ".." {
                return Err(Error::InvalidPath(rel.to_string()));
            }
        }
        Ok(())
    }

    fn name_of(rel: &str) -> &str {
        rel.rsplit('/').next().unwrap_or(rel)
    }

    /// The parent directory of `rel`, canonicalized, and verified to sit
    /// inside the root. This is the symlink-escape gate: a symlinked
    /// directory component that leads OUT of the root fails here, before
    /// any filesystem mutation. The rule root itself is always fine.
    fn checked_parent(&self, rel: &str) -> Result<PathBuf> {
        Self::rel_ok(rel)?;
        if rel.is_empty() {
            return Ok(self.root.clone());
        }
        let abs = self.abs(rel);
        let parent = match abs.parent() {
            Some(p) if p != self.root.as_path() => p.to_path_buf(),
            _ => return Ok(self.root.clone()),
        };
        let canon = std::fs::canonicalize(&parent).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::ParentNotFound(rel.to_string())
            } else {
                io_err(e, rel)
            }
        })?;
        if !canon.starts_with(&self.root) {
            return Err(Error::InvalidArgument(format!(
                "{rel}: parent directory resolves outside the local root (symlink escape)"
            )));
        }
        Ok(canon)
    }

    fn node_for(&self, rel: &str, id: i64, md: &std::fs::Metadata) -> Result<Node> {
        let ft = md.file_type();
        let (kind, size) = if ft.is_symlink() {
            let target = std::fs::read_link(self.abs(rel)).map_err(|e| io_err(e, rel))?;
            (NodeKind::Symlink, target.to_string_lossy().len() as i64)
        } else if ft.is_dir() {
            (NodeKind::Dir, md.len() as i64)
        } else {
            (NodeKind::File, md.len() as i64)
        };
        let mtime = md
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        #[cfg(unix)]
        let mode = std::os::unix::fs::PermissionsExt::mode(&md.permissions()) & 0o7777;
        #[cfg(not(unix))]
        let mode = if kind == NodeKind::Dir { 0o755 } else { 0o644 };
        Ok(Node {
            id,
            parent_id: None,
            name: Self::name_of(rel).to_string(),
            kind,
            size,
            mtime,
            mode,
            object_key: None,
            version: 1,
            etag: String::new(),
            target: None,
        })
    }

    /// lstat a relative path and return its (stable) node. Reuses the id
    /// already mapped for `rel`; assigns a fresh one on first sight. A
    /// stale map entry for a vanished file is purged and reported
    /// NotFound.
    fn stat_rel(&self, rel: &str) -> Result<Node> {
        Self::rel_ok(rel)?;
        let md = std::fs::symlink_metadata(self.abs(rel)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                let mut ids = self.ids.lock().unwrap();
                if let Some(old) = ids.by_path.remove(rel) {
                    ids.by_id.remove(&old);
                }
            }
            io_err(e, rel)
        })?;
        let id = self.id_for(rel);
        self.node_for(rel, id, &md)
    }

    /// The stable id for `rel`: the mapped one, or a fresh allocation.
    fn id_for(&self, rel: &str) -> i64 {
        let mut ids = self.ids.lock().unwrap();
        if let Some(id) = ids.by_path.get(rel) {
            return *id;
        }
        let id = (LOCAL_FLAG | self.seq.fetch_add(1, Ordering::Relaxed)) as i64;
        ids.by_path.insert(rel.to_string(), id);
        ids.by_id.insert(id, rel.to_string());
        id
    }

    fn purge(&self, rel: &str) {
        let mut ids = self.ids.lock().unwrap();
        if let Some(old) = ids.by_path.remove(rel) {
            ids.by_id.remove(&old);
        }
    }

    // ------------------------------------------------------------------
    // namespace operations (relative paths; the router strips the prefix)
    // ------------------------------------------------------------------

    pub fn lookup(&self, rel: &str) -> Result<Node> {
        self.stat_rel(rel)
    }

    pub fn readdir(&self, rel: &str) -> Result<Vec<Node>> {
        Self::rel_ok(rel)?;
        // The directory itself must be a real directory inside the root:
        // enumerating through a symlinked dir would read someone else's
        // tree (escape).
        if !rel.is_empty() {
            let md = std::fs::symlink_metadata(self.abs(rel)).map_err(|e| io_err(e, rel))?;
            if md.is_symlink() {
                return Err(Error::InvalidArgument(format!(
                    "{rel}: refusing to enumerate through a symlink"
                )));
            }
        }
        self.checked_parent(rel)?;
        let mut out = Vec::new();
        for entry in std::fs::read_dir(self.abs(rel)).map_err(|e| io_err(e, rel))? {
            let entry = entry.map_err(|e| io_err(e, rel))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == RESERVED {
                continue; // commit scratch, never part of the namespace
            }
            let child_rel = if rel.is_empty() {
                name.to_string()
            } else {
                format!("{rel}/{name}")
            };
            match std::fs::symlink_metadata(entry.path()) {
                Ok(md) => {
                    let id = self.id_for(&child_rel);
                    out.push(self.node_for(&child_rel, id, &md)?);
                }
                Err(_) => continue, // raced away mid-enumeration: skip
            }
        }
        Ok(out)
    }

    fn parent_must_exist(&self, rel: &str) -> Result<()> {
        let abs = self.abs(rel);
        let parent = match abs.parent() {
            Some(p) if p != self.root.as_path() => p.to_path_buf(),
            _ => return Ok(()),
        };
        match std::fs::symlink_metadata(&parent) {
            Ok(md) if md.is_dir() => Ok(()),
            Ok(_) => Err(Error::KindMismatch {
                path: rel.to_string(),
                expected: "dir",
                actual: "file".into(),
            }),
            Err(_) => Err(Error::ParentNotFound(rel.to_string())),
        }
    }

    pub fn mkdir(&self, rel: &str, _mode: u32) -> Result<Node> {
        Self::rel_ok(rel)?;
        if rel.is_empty() {
            return Err(Error::AlreadyExists("/".into()));
        }
        self.checked_parent(rel)?;
        self.parent_must_exist(rel)?;
        std::fs::create_dir(self.abs(rel)).map_err(|e| io_err(e, rel))?;
        let id = self.id_for(rel);
        self.stat_rel(rel).map(|mut n| {
            n.id = id;
            n
        })
    }

    pub fn create(&self, rel: &str, _mode: u32) -> Result<Node> {
        if rel.is_empty() {
            return Err(Error::InvalidPath("/".into()));
        }
        self.checked_parent(rel)?;
        self.parent_must_exist(rel)?;
        // create_new: an existing occupant (including a symlink) refuses;
        // no truncate-on-create, no following anything.
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.abs(rel))
            .map_err(|e| io_err(e, rel))?;
        drop(f);
        // Fresh identity, even if a stale mapping for this path existed
        // (the previous file is gone; old handles must see IdentityGone,
        // not alias the new file - M3 locally).
        let mut ids = self.ids.lock().unwrap();
        if let Some(old) = ids.by_path.remove(rel) {
            ids.by_id.remove(&old);
        }
        let id = (LOCAL_FLAG | self.seq.fetch_add(1, Ordering::Relaxed)) as i64;
        ids.by_path.insert(rel.to_string(), id);
        ids.by_id.insert(id, rel.to_string());
        drop(ids);
        self.stat_rel(rel).map(|mut n| {
            n.id = id;
            n
        })
    }

    pub fn open(&self, rel: &str) -> Result<Node> {
        self.lookup(rel)
    }

    pub fn read(&self, rel: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        Self::rel_ok(rel)?;
        self.checked_parent(rel)?; // symlink-escape gate on the parent chain
        self.read_impl(&self.abs(rel), rel, offset, size)
    }

    fn read_impl(&self, abs: &Path, disp: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        let mut f = self.open_nofollow_read(abs, disp)?;
        use std::io::{Read, Seek, SeekFrom};
        f.seek(SeekFrom::Start(offset))
            .map_err(|e| io_err(e, disp))?;
        let mut out = Vec::with_capacity(size.min(1 << 20) as usize);
        f.take(size)
            .read_to_end(&mut out)
            .map_err(|e| io_err(e, disp))?;
        Ok(out)
    }

    #[cfg(unix)]
    fn open_nofollow_read(&self, abs: &Path, disp: &str) -> Result<std::fs::File> {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(abs)
            .map_err(|e| io_err(e, disp))
    }

    #[cfg(not(unix))]
    fn open_nofollow_read(&self, abs: &Path, disp: &str) -> Result<std::fs::File> {
        std::fs::File::open(abs).map_err(|e| io_err(e, disp))
    }

    pub fn rename(&self, old_rel: &str, new_rel: &str) -> Result<()> {
        Self::rel_ok(old_rel)?;
        Self::rel_ok(new_rel)?;
        if old_rel.is_empty() {
            return Err(Error::InvalidPath("cannot rename the rule root".into()));
        }
        if new_rel.is_empty() {
            return Err(Error::InvalidPath(
                "cannot rename onto the rule root".into(),
            ));
        }
        self.checked_parent(old_rel)?;
        self.checked_parent(new_rel)?;
        // Source must exist (rename(2) would say ENOENT too, but an
        // explicit lstat keeps the error surface uniform).
        std::fs::symlink_metadata(self.abs(old_rel)).map_err(|e| io_err(e, old_rel))?;
        let overwritten = std::fs::symlink_metadata(self.abs(new_rel)).is_ok();
        std::fs::rename(self.abs(old_rel), self.abs(new_rel)).map_err(|e| {
            // rename(2) gives EINVAL for "dir into its own descendant" -
            // lightfs reports that as Cycle.
            if e.kind() == std::io::ErrorKind::InvalidInput {
                return Error::Cycle(format!("{old_rel} -> {new_rel}"));
            }
            io_err(e, old_rel)
        })?;
        // Identity follows the name change: same id, new mapped path. An
        // overwritten destination loses its mapping entirely - its open
        // handles now fail IdentityGone (M3, locally).
        let mut ids = self.ids.lock().unwrap();
        let id = ids.by_path.remove(old_rel);
        if overwritten {
            if let Some(victim) = ids.by_path.remove(new_rel) {
                ids.by_id.remove(&victim);
            }
        }
        if let Some(id) = id {
            ids.by_path.insert(new_rel.to_string(), id);
            ids.by_id.insert(id, new_rel.to_string());
        }
        Ok(())
    }

    pub fn unlink(&self, rel: &str) -> Result<()> {
        Self::rel_ok(rel)?;
        if rel.is_empty() {
            return Err(Error::InvalidPath("cannot unlink the rule root".into()));
        }
        self.checked_parent(rel)?;
        let md = std::fs::symlink_metadata(self.abs(rel)).map_err(|e| io_err(e, rel))?;
        if md.is_dir() {
            return Err(Error::KindMismatch {
                path: rel.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        std::fs::remove_file(self.abs(rel)).map_err(|e| io_err(e, rel))?;
        self.purge(rel);
        Ok(())
    }

    pub fn rmdir(&self, rel: &str) -> Result<()> {
        Self::rel_ok(rel)?;
        if rel.is_empty() {
            return Err(Error::InvalidPath("cannot remove the rule root".into()));
        }
        self.checked_parent(rel)?;
        std::fs::remove_dir(self.abs(rel)).map_err(|e| io_err(e, rel))?;
        self.purge(rel);
        Ok(())
    }

    pub fn setattr(&self, rel: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
        Self::rel_ok(rel)?;
        self.checked_parent(rel)?;
        let abs = self.abs(rel);
        let md = std::fs::symlink_metadata(&abs).map_err(|e| io_err(e, rel))?;
        if md.is_symlink() {
            // chmod/utimens on a symlink would follow it (escape); refuse
            // loudly instead.
            return Err(Error::InvalidArgument(format!(
                "{rel}: refusing to follow symlink for setattr"
            )));
        }
        if let Some(mode) = mode {
            self.set_mode(&abs, rel, mode)?;
        }
        if let Some(mtime) = mtime {
            let t = std::time::UNIX_EPOCH + std::time::Duration::from_secs(mtime.max(0) as u64);
            let f = self.open_nofollow_write(&abs, rel)?;
            f.set_modified(t).map_err(|e| io_err(e, rel))?;
        }
        self.stat_rel(rel)
    }

    #[cfg(unix)]
    fn set_mode(&self, abs: &Path, rel: &str, mode: u32) -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(abs, std::fs::Permissions::from_mode(mode))
            .map_err(|e| io_err(e, rel))
    }

    #[cfg(not(unix))]
    fn set_mode(&self, _abs: &Path, rel: &str, _mode: u32) -> Result<()> {
        Err(Error::InvalidArgument(format!(
            "{rel}: chmod is not supported on this platform"
        )))
    }

    #[cfg(unix)]
    fn open_nofollow_write(&self, abs: &Path, disp: &str) -> Result<std::fs::File> {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(abs)
            .map_err(|e| io_err(e, disp))
    }

    #[cfg(not(unix))]
    fn open_nofollow_write(&self, abs: &Path, disp: &str) -> Result<std::fs::File> {
        std::fs::OpenOptions::new()
            .write(true)
            .open(abs)
            .map_err(|e| io_err(e, disp))
    }

    pub fn symlink(&self, target: &str, rel: &str) -> Result<Node> {
        #[cfg(unix)]
        {
            Self::rel_ok(rel)?;
            if rel.is_empty() {
                return Err(Error::InvalidPath("/".into()));
            }
            self.checked_parent(rel)?;
            self.parent_must_exist(rel)?;
            std::os::unix::fs::symlink(target, self.abs(rel)).map_err(|e| io_err(e, rel))?;
            self.stat_rel(rel)
        }
        #[cfg(not(unix))]
        {
            let _ = (target, rel);
            Err(Error::Config("symlinks require a POSIX filesystem".into()))
        }
    }

    pub fn readlink(&self, rel: &str) -> Result<String> {
        Self::rel_ok(rel)?;
        let md = std::fs::symlink_metadata(self.abs(rel)).map_err(|e| io_err(e, rel))?;
        if !md.is_symlink() {
            return Err(Error::KindMismatch {
                path: rel.to_string(),
                expected: "symlink",
                actual: if md.is_dir() { "dir" } else { "file" }.into(),
            });
        }
        std::fs::read_link(self.abs(rel))
            .map_err(|e| io_err(e, rel))
            .map(|p| p.to_string_lossy().into_owned())
    }

    // ------------------------------------------------------------------
    // stable-identity operations (never re-resolve through any path)
    // ------------------------------------------------------------------

    fn path_of_id(&self, id: i64) -> Result<String> {
        self.ids
            .lock()
            .unwrap()
            .by_id
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::IdentityGone(format!("local id {id}")))
    }

    pub fn lookup_by_id(&self, id: i64) -> Result<Node> {
        let rel = self.path_of_id(id)?;
        match std::fs::symlink_metadata(self.abs(&rel)) {
            Ok(md) => self.node_for(&rel, id, &md),
            Err(_) => Err(Error::IdentityGone(format!("local id {id} ({rel})"))),
        }
    }

    pub fn read_by_id(&self, id: i64, offset: u64, size: u64) -> Result<Vec<u8>> {
        let rel = self.path_of_id(id)?;
        // Same escape gate as the path form: the file's directory must
        // still resolve inside the root.
        if let Some((dir, _)) = rel.rsplit_once('/') {
            self.checked_parent(dir)?;
        }
        self.read_impl(&self.abs(&rel), &rel, offset, size)
    }

    pub fn open_write_by_id(
        &self,
        id: i64,
        _path: &str,
        _expected_version: Option<i64>,
    ) -> Result<LocalWriteSession<'_>> {
        // The identity must be alive at open; the session re-resolves its
        // path FROM THE ID at commit, so a rename during the session
        // cannot detach the commit from the file (and an unlink turns the
        // commit into IdentityGone).
        let rel = self.path_of_id(id)?;
        std::fs::symlink_metadata(self.abs(&rel))
            .map_err(|_| Error::IdentityGone(format!("local id {id} ({rel})")))?;
        // Strict-sync local authority: there is no optimistic-concurrency
        // gate to arm (single-machine authority, local FS ordering rules),
        // so `expected_version` is accepted and ignored by design.
        Ok(LocalWriteSession {
            fs: self,
            id,
            chunks: Vec::new(),
        })
    }

    /// Path-form open (WinFsp/shell flows): stat first so an existing file
    /// gains its stable id, then anchor the session on it.
    pub fn open_write(
        &self,
        rel: &str,
        expected_version: Option<i64>,
    ) -> Result<LocalWriteSession<'_>> {
        let node = self.stat_rel(rel)?;
        self.open_write_by_id(node.id, rel, expected_version)
    }

    fn scratch(&self) -> PathBuf {
        self.root.join(RESERVED)
    }
}

// ---------------------------------------------------------------------------
// write session: strict sync, full-content commit
// ---------------------------------------------------------------------------

/// The adapter (FUSE/WinFsp/shell) assembles the FULL file content and
/// hands it over with one `write(0, bytes)`; commit replaces the file
/// atomically (temp + fsync + rename + parent fsync) under the id's
/// CURRENT mapped path. There is intentionally no partial-write path.
pub struct LocalWriteSession<'a> {
    fs: &'a LocalFs,
    id: i64,
    chunks: Vec<(u64, Vec<u8>)>,
}

impl LocalWriteSession<'_> {
    fn assemble(&self) -> Vec<u8> {
        let end = self
            .chunks
            .iter()
            .map(|(off, d)| off + d.len() as u64)
            .max()
            .unwrap_or(0);
        let mut out = vec![0u8; end as usize];
        for (off, data) in &self.chunks {
            let o = *off as usize;
            out[o..o + data.len()].copy_from_slice(data);
        }
        out
    }
}

impl WriteSession for LocalWriteSession<'_> {
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        self.chunks.push((offset, data.to_vec()));
        Ok(data.len())
    }

    fn commit(self: Box<Self>) -> Result<Node> {
        let content = self.assemble();
        let fs = self.fs;
        // Re-resolve the id -> path NOW: a rename during the session is
        // picked up here; an unlink removed the mapping and the commit
        // must fail IdentityGone, never resurrect or alias.
        let rel = fs.path_of_id(self.id)?;
        // The escape gate runs on every platform; the handle is only
        // needed for the unix parent-fsync below.
        #[cfg(unix)]
        let parent = fs.checked_parent(&rel)?;
        #[cfg(not(unix))]
        fs.checked_parent(&rel)?;
        // Atomic strict-sync commit: temp file inside the reserved scratch
        // dir, fsync, rename into place, fsync the parent directory. The
        // temp name draws from the shared id sequence: unique per commit.
        let tmp = fs.scratch().join(format!(
            "commit-{}-{}",
            std::process::id(),
            fs.seq.fetch_add(1, Ordering::Relaxed)
        ));
        {
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| Error::Io(format!("scratch {}: {e}", tmp.display())))?;
            f.write_all(&content)
                .map_err(|e| Error::Io(e.to_string()))?;
            f.sync_all().map_err(|e| Error::Io(e.to_string()))?;
        }
        let dest = fs.abs(&rel);
        std::fs::rename(&tmp, dest).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            io_err(e, &rel)
        })?;
        #[cfg(unix)]
        {
            if let Ok(dir) = std::fs::File::open(&parent) {
                let _ = dir.sync_all(); // file fsync already guarantees the bytes
            }
        }
        let md = std::fs::symlink_metadata(fs.abs(&rel))
            .map_err(|_| Error::IdentityGone(format!("local id {}", self.id)))?;
        fs.node_for(&rel, self.id, &md)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "lightfs-localfs-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn fs_at(root: &Path) -> LocalFs {
        // (the canonical root may differ textually from `root` on hosts
        // with symlinked temp dirs - macOS /var -> /private/var - which is
        // exactly what the canonicalization is for)
        let (fs, _) = LocalFs::new(root, Arc::new(AtomicU64::new(1))).unwrap();
        fs
    }

    #[test]
    fn planted_symlink_refused_on_open() {
        let t = tmpdir("symlink");
        let fs = fs_at(&t.join("root"));
        // an innocent-looking file outside the root...
        std::fs::write(t.join("secret"), b"top secret").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(t.join("secret"), t.join("root").join("lnk")).unwrap();

        // listing sees the symlink node (kind is honest, no follow)
        let n = fs.lookup("lnk").unwrap();
        assert_eq!(n.kind, NodeKind::Symlink);
        assert_eq!(
            fs.readlink("lnk").unwrap(),
            t.join("secret").to_string_lossy()
        );

        // ...but reading through it refuses loudly (O_NOFOLLOW)
        assert!(matches!(
            fs.read("lnk", 0, 100),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            fs.setattr("lnk", Some(0o600), None),
            Err(Error::InvalidArgument(_))
        ));
        // a symlink LOOP also fails instead of hanging
        #[cfg(unix)]
        std::os::unix::fs::symlink("loop", t.join("root").join("loop")).unwrap();
        assert!(matches!(
            fs.read("loop", 0, 1),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn parent_escape_refused() {
        let t = tmpdir("escape");
        let fs = fs_at(&t.join("root"));
        fs.mkdir("sub", 0o755).unwrap();
        // sub/out -> / (the whole machine): any child op under it must
        // refuse at the parent-canonicalization gate
        #[cfg(unix)]
        std::os::unix::fs::symlink("/", t.join("root").join("sub").join("out")).unwrap();
        assert!(matches!(
            fs.create("sub/out/evil", 0o644),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            fs.read("sub/out/etc/passwd", 0, 10),
            Err(Error::InvalidArgument(_))
        ));
        // enumerating THROUGH a symlinked dir refuses too
        assert!(matches!(
            fs.readdir("sub/out"),
            Err(Error::InvalidArgument(_))
        ));
        // nothing was created through the escape
        assert!(
            std::fs::symlink_metadata(t.join("root").join("sub").join("out").join("evil")).is_err()
        );
    }

    #[test]
    fn scratch_name_reserved() {
        let t = tmpdir("scratch");
        let fs = fs_at(&t.join("root"));
        assert!(matches!(
            fs.create(".lightfs-tmp/evil", 0o644),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            fs.lookup(".lightfs-tmp"),
            Err(Error::InvalidArgument(_))
        ));
        // readdir at the root hides the scratch dir (it physically exists)
        let entries = fs.readdir("").unwrap();
        assert!(entries.iter().all(|n| n.name != RESERVED));
    }

    #[test]
    fn strict_sync_commit_replaces_atomically() {
        let t = tmpdir("commit");
        let root = t.join("root");
        let fs = fs_at(&root);
        fs.create("f", 0o644).unwrap();
        // first commit: full replacement with one chunk
        let mut s = fs.open_write("f", None).unwrap();
        s.write(0, b"hello").unwrap();
        let n = Box::new(s).commit().unwrap();
        assert_eq!(n.size, 5);
        assert_eq!(std::fs::read(root.join("f")).unwrap(), b"hello".to_vec());
        // second (smaller) commit truncates: full-content semantics
        let mut s = fs.open_write("f", None).unwrap();
        s.write(0, b"hi").unwrap();
        Box::new(s).commit().unwrap();
        assert_eq!(std::fs::read(root.join("f")).unwrap(), b"hi".to_vec());
        // scratch dir is empty after commits (temp renamed away)
        let scratch: Vec<_> = std::fs::read_dir(root.join(RESERVED)).unwrap().collect();
        assert!(scratch.is_empty(), "scratch must not leak temp files");
    }

    #[test]
    fn identity_gone_on_unlink_during_session() {
        let t = tmpdir("gone");
        let fs = fs_at(&t.join("root"));
        fs.create("f", 0o644).unwrap();
        let id = fs.lookup("f").unwrap().id;
        let mut s = fs.open_write("f", None).unwrap();
        fs.unlink("f").unwrap();
        s.write(0, b"orphan").unwrap();
        // the commit must NOT resurrect or alias anything
        assert!(matches!(Box::new(s).commit(), Err(Error::IdentityGone(_))));
        assert!(matches!(
            fs.open_write_by_id(id, "f", None),
            Err(Error::IdentityGone(_))
        ));
        assert!(!t.join("root").join("f").exists());
    }
}
