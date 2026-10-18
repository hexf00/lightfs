//! RoutingFs: dispatch one mount across storage backends by path policy
//! (Phase P).
//!
//! Two routing axes, kept strictly apart:
//! - NAMESPACE operations (lookup/create/rename/...) route on the
//!   normalized path against the policy resolved once at mount time.
//! - OPEN-FILE operations (read_by_id/open_write_by_id/lookup_by_id)
//!   route on the STABLE backend identity. Local ids carry bit 63
//!   (negative i64, see [`crate::localfs::LOCAL_FLAG`]); remote node ids
//!   are positive rowids. The sign IS the routing decision: a negative id
//!   is structurally incapable of reaching the remote backend - it is
//!   never sent to the server, not even as a hint.
//!
//! rename keeps the file's backend identity (a local file renamed within
//! its rule stays local, same id); rename across two different backends
//! is refused with EXDEV - no implicit data migration.
//!
//! Mount-time rules (Phase P3): the policy is loaded once and frozen; a
//! client-local rule TAKES OVER its subtree, so if the remote backend has
//! content at the prefix, mounting refuses unless the config explicitly
//! opts in with `allow_shadow = true` (and then warns loudly). Nothing
//! ever moves between backends on mount or unmount.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::api::{FsApi, WriteSession};
use crate::error::{Error, Result};
use crate::localfs::LocalFs;
use crate::metadata::Node;
use crate::policy::{Authority, PolicySection, ResolvedPolicy, normalize_path};

struct LocalBackend {
    /// Normalized rule prefix ("/work"). Locals are longest-first because
    /// [`ResolvedPolicy::rules`] is.
    prefix: String,
    fs: LocalFs,
}

pub struct RoutingFs {
    /// The s3-authority backend: whatever the mount would have used
    /// without a policy (embedded Core or network client). When the
    /// policy enables the memory read cache (Phase Q), this is the
    /// CacheFs decorator in front of that backend.
    remote: Arc<dyn FsApi>,
    policy: ResolvedPolicy,
    locals: Vec<LocalBackend>,
}

/// Resolve `~` prefixes (the policy parser accepts absolute or `~/...`).
fn expand_root(root: &str) -> Result<PathBuf> {
    match root.strip_prefix("~/") {
        Some(rest) => {
            #[cfg(unix)]
            let home = std::env::var("HOME");
            #[cfg(not(unix))]
            let home = std::env::var("USERPROFILE");
            let home = home.map_err(|_| {
                Error::Config(format!(
                    "policy root {root:?}: cannot resolve home directory"
                ))
            })?;
            Ok(Path::new(&home).join(rest))
        }
        None => Ok(PathBuf::from(root)),
    }
}

/// Component-wise containment (true when equal).
fn contains(ancestor: &Path, child: &Path) -> bool {
    child.starts_with(ancestor)
}

/// Best-effort canonical form of a path that may not exist yet at routing
/// time (the FUSE layer creates the mountpoint after routing succeeds):
/// canonicalize the deepest existing ancestor, then re-append the missing
/// tail. Comparisons with other canonical roots must happen in the same
/// (symlink-resolved) space on BOTH sides, or containment silently passes.
fn canon_or_abs(p: &Path) -> PathBuf {
    if let Ok(c) = std::fs::canonicalize(p) {
        return c;
    }
    let mut missing: Vec<std::ffi::OsString> = Vec::new();
    let mut anc = p.to_path_buf();
    while let (Some(par), Some(name)) = (anc.parent(), anc.file_name()) {
        missing.push(name.to_os_string());
        anc = par.to_path_buf();
        if let Ok(c) = std::fs::canonicalize(&anc) {
            let mut c = c;
            for part in missing.iter().rev() {
                c.push(part);
            }
            return c;
        }
    }
    std::path::absolute(p).unwrap_or_else(|_| p.to_path_buf())
}

impl RoutingFs {
    /// Mount-time construction. `None` = no local rules configured: the
    /// caller should mount `remote` directly and skip this wrapper
    /// entirely (bit-for-bit today's behavior).
    ///
    /// Order matters: policy validation (parser) -> local roots opened ->
    /// mount-tree containment -> shadow probes. Any failure aborts the
    /// mount with a named error; nothing falls back silently.
    pub fn build(
        remote: Arc<dyn FsApi>,
        section: Option<&PolicySection>,
        mountpoint: &Path,
    ) -> Result<Option<RoutingFs>> {
        let Some(section) = section else {
            return Ok(None);
        };
        if section.rule.is_empty() {
            return Ok(None);
        }
        let policy =
            ResolvedPolicy::build(section).map_err(|e| Error::Config(format!("policy: {e}")))?;

        // Phase Q: install the single-tier memory read cache between the
        // router and the s3 backend when the policy asks for it. The policy
        // build gate guarantees at most one cached rule with a validated
        // budget, so this is a single unconditional wrap.
        let cached_rule = policy.memory_cache();
        let remote: Arc<dyn FsApi> = match cached_rule {
            Some((prefix, budget)) => Arc::new(crate::readcache::CacheFs::new(
                remote,
                prefix.to_string(),
                budget,
            )),
            None => remote,
        };

        let mnt = canon_or_abs(mountpoint);
        let seq = Arc::new(AtomicU64::new(1));
        let mut locals: Vec<LocalBackend> = Vec::new();
        let mut roots: Vec<PathBuf> = Vec::new();

        for (prefix, view) in policy.rules() {
            if view.authority != Authority::Local {
                continue;
            }
            // The resolved policy guarantees every Local rule has a root;
            // find the raw rule for root + allow_shadow (both mount-time
            // concerns the PolicyView deliberately does not carry).
            let raw = section
                .rule
                .iter()
                .find(|r| {
                    normalize_path(&r.prefix)
                        .map(|p| p == prefix)
                        .unwrap_or(false)
                })
                .expect("resolved rule has a raw rule");
            let root = expand_root(raw.root.as_deref().expect("local rule has a root"))?;
            let (fs, canon) = LocalFs::new(&root, seq.clone())?;

            // Recursion guards: the local root must be disjoint from the
            // mount tree and from every other local root - in BOTH
            // directions, on the CANONICAL path, so a symlink that points
            // into (or out of) the mount tree is caught by resolution,
            // not by string comparison.
            if canon == mnt || contains(&mnt, &canon) || contains(&canon, &mnt) {
                return Err(Error::Config(format!(
                    "policy rule {prefix}: root {} overlaps the mount tree at {} - \
                     recursive serving is refused",
                    canon.display(),
                    mnt.display()
                )));
            }
            for (b, other) in locals.iter().zip(roots.iter()) {
                if canon == *other || contains(other, &canon) || contains(&canon, other) {
                    return Err(Error::Config(format!(
                        "policy rule {prefix}: root {} overlaps rule {}'s root {}",
                        canon.display(),
                        b.prefix,
                        other.display()
                    )));
                }
            }
            roots.push(canon);

            // Shadow probe (P3): a client-local rule TAKES OVER its
            // subtree. Remote content at the prefix would become invisible
            // - refuse by default, warn when explicitly allowed. This is
            // the one remote metadata query a local-path mount makes.
            match remote.lookup(prefix) {
                Ok(node) => {
                    if raw.allow_shadow != Some(true) {
                        return Err(Error::Config(format!(
                            "policy rule {prefix}: remote {} exists there - mounting \
                             client-local would shadow it; set allow_shadow = true to accept",
                            if node.kind == crate::metadata::NodeKind::Dir {
                                "directory".to_string()
                            } else {
                                "file".to_string()
                            }
                        )));
                    }
                    eprintln!(
                        "lightfs: WARNING: policy rule {prefix}: client-local takes over a \
                         remote {} at {prefix} (allow_shadow = true) - remote content there \
                         is invisible and unreachable while mounted",
                        if node.kind == crate::metadata::NodeKind::Dir {
                            "directory"
                        } else {
                            "file"
                        }
                    );
                }
                Err(Error::NotFound(_)) | Err(Error::ParentNotFound(_)) => {}
                Err(e) => {
                    return Err(Error::Config(format!(
                        "policy rule {prefix}: shadow probe failed: {e} - refusing to mount \
                         without proof the prefix is free on the remote"
                    )));
                }
            }

            locals.push(LocalBackend {
                prefix: prefix.to_string(),
                fs,
            });
        }

        if locals.is_empty() {
            if cached_rule.is_none() {
                return Ok(None); // only s3 rules: the remote alone IS the policy
            }
            // Q: a memory-cached s3 policy has no local backends but still
            // needs this wrapper (it owns the CacheFs) - an all-remote
            // router with an empty locals list routes everything forward.
            return Ok(Some(Self {
                remote,
                policy,
                locals,
            }));
        }
        Ok(Some(Self {
            remote,
            policy,
            locals,
        }))
    }

    /// Namespace routing: `Some((backend, backend-relative path))` for a
    /// local rule, `None` for the remote. Paths are normalized once here;
    /// everything downstream sees clean shapes.
    fn route<'a>(&'a self, path: &str) -> Result<Option<(&'a LocalBackend, String)>> {
        let norm = normalize_path(path).map_err(|e| Error::InvalidPath(e.to_string()))?;
        let view = self
            .policy
            .resolve(&norm)
            .map_err(|e| Error::InvalidPath(e.to_string()))?;
        if view.authority != Authority::Local {
            return Ok(None);
        }
        let backend = self
            .locals
            .iter()
            .find(|b| b.prefix == view.rule_prefix)
            .expect("build created a backend for every local rule");
        let rel = if backend.prefix == "/" {
            norm.trim_start_matches('/').to_string()
        } else {
            norm[backend.prefix.len()..]
                .trim_start_matches('/')
                .to_string()
        };
        Ok(Some((backend, rel)))
    }

    /// Identity routing for the local domain. A negative id can NEVER
    /// reach `self.remote` - this function is the only by-id dispatcher
    /// and has no remote branch for negatives.
    fn local_by_id<T>(&self, node_id: i64, op: impl Fn(&LocalFs, i64) -> Result<T>) -> Result<T> {
        for b in &self.locals {
            if b.fs.knows(node_id) {
                return op(&b.fs, node_id);
            }
        }
        Err(Error::IdentityGone(format!("local id {node_id}")))
    }
}

impl FsApi for RoutingFs {
    // ---- namespace: route by normalized path ----

    fn lookup(&self, path: &str) -> Result<Node> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.lookup(&rel),
            None => self.remote.lookup(path),
        }
    }

    fn readdir(&self, path: &str) -> Result<Vec<Node>> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.readdir(&rel),
            None => self.remote.readdir(path),
        }
    }

    fn mkdir(&self, path: &str, mode: u32) -> Result<Node> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.mkdir(&rel, mode),
            None => self.remote.mkdir(path, mode),
        }
    }

    fn create(&self, path: &str, mode: u32) -> Result<Node> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.create(&rel, mode),
            None => self.remote.create(path, mode),
        }
    }

    fn open(&self, path: &str) -> Result<Node> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.open(&rel),
            None => self.remote.open(path),
        }
    }

    fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.read(&rel, offset, size),
            None => self.remote.read(path, offset, size),
        }
    }

    fn read_stream(
        &self,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        match self.route(path)? {
            // Local files are small by contract (client-local authority);
            // the buffered default read is the implementation.
            Some(_) => FsApi::read(self, path, offset, size).map(|data| {
                let len = data.len() as u64;
                (
                    Box::new(std::io::Cursor::new(data)) as Box<dyn std::io::Read + Send>,
                    len,
                )
            }),
            None => self.remote.read_stream(path, offset, size),
        }
    }

    fn open_write(
        &self,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        match self.route(path)? {
            Some((b, rel)) => {
                let sess = b.fs.open_write(&rel, expected_version)?;
                Ok(Box::new(sess))
            }
            None => self.remote.open_write(path, expected_version),
        }
    }

    fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        let old = self.route(old_path)?;
        let new = self.route(new_path)?;
        match (old, new) {
            (None, None) => self.remote.rename(old_path, new_path),
            (Some((a, ra)), Some((b, rb))) => {
                if a.prefix != b.prefix {
                    // Different local rules (or local vs remote): refused
                    // outright. No implicit migration, both sides intact.
                    return Err(Error::Exdev(format!(
                        "rename {} -> {} crosses policy rules ({} vs {})",
                        old_path, new_path, a.prefix, b.prefix
                    )));
                }
                a.fs.rename(&ra, &rb)
            }
            _ => Err(Error::Exdev(format!(
                "rename {old_path} -> {new_path} crosses policy backends"
            ))),
        }
    }

    fn unlink(&self, path: &str) -> Result<()> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.unlink(&rel),
            None => self.remote.unlink(path),
        }
    }

    fn rmdir(&self, path: &str) -> Result<()> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.rmdir(&rel),
            None => self.remote.rmdir(path),
        }
    }

    fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.setattr(&rel, mode, mtime),
            None => self.remote.setattr(path, mode, mtime),
        }
    }

    fn symlink(&self, target: &str, path: &str) -> Result<Node> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.symlink(target, &rel),
            None => self.remote.symlink(target, path),
        }
    }

    fn readlink(&self, path: &str) -> Result<String> {
        match self.route(path)? {
            Some((b, rel)) => b.fs.readlink(&rel),
            None => self.remote.readlink(path),
        }
    }

    // ---- open-file operations: route by STABLE backend identity ----

    fn lookup_by_id(&self, node_id: i64, path_hint: &str) -> Result<Node> {
        if node_id < 0 {
            return self.local_by_id(node_id, |fs, id| fs.lookup_by_id(id));
        }
        self.remote.lookup_by_id(node_id, path_hint)
    }

    fn read_by_id(&self, node_id: i64, path_hint: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        if node_id < 0 {
            return self.local_by_id(node_id, |fs, id| fs.read_by_id(id, offset, size));
        }
        self.remote.read_by_id(node_id, path_hint, offset, size)
    }

    /// The commit chain's base read: negatives have no cache anywhere
    /// (local backends don't cache), positives must NOT hit the CacheFs —
    /// delegate to its structural bypass.
    fn client_staging(&self) -> Option<std::sync::Arc<crate::staging::StagingBudget>> {
        self.remote.client_staging()
    }

    fn read_by_id_authoritative(
        &self,
        node_id: i64,
        path_hint: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        if node_id < 0 {
            return self.local_by_id(node_id, |fs, id| fs.read_by_id(id, offset, size));
        }
        self.remote
            .read_by_id_authoritative(node_id, path_hint, offset, size)
    }

    fn open_write_by_id(
        &self,
        node_id: i64,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        if node_id < 0 {
            // Inline (not local_by_id): the returned session borrows the
            // specific LocalFs, so the routing loop must hold that borrow.
            for b in &self.locals {
                if b.fs.knows(node_id) {
                    let sess = b.fs.open_write_by_id(node_id, path, expected_version)?;
                    return Ok(Box::new(sess));
                }
            }
            return Err(Error::IdentityGone(format!("local id {node_id}")));
        }
        self.remote
            .open_write_by_id(node_id, path, expected_version)
    }
}

// ---------------------------------------------------------------------------
// Phase P acceptance (host-level: the FsApi contract is mount-independent;
// the FUSE-level E-section on ECS exercises the same paths through a real
// kernel mount)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, MetadataSection};
    use crate::vnext::Core;
    use crate::policy::RawRule;
    // Only CountingFs (unix-gated) uses these.
    #[cfg(unix)]
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "lightfs-p-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn mem_core(tag: &str) -> Arc<Core> {
        let cfg = Config {
            metadata: MetadataSection {
                path: tmpdir(tag).join("meta.db").to_string_lossy().into_owned(),
            },
            ..Default::default()
        };
        crate::engine::open_engine_mem(&cfg).unwrap().core
    }

    fn local_rule(prefix: &str, root: &Path) -> RawRule {
        RawRule {
            prefix: prefix.into(),
            storage: "client-local".into(),
            root: Some(root.to_string_lossy().into_owned()),
            ..Default::default()
        }
    }

    fn section(rules: Vec<RawRule>) -> PolicySection {
        PolicySection {
            limits: None,
            rule: rules,
        }
    }

    fn write_via(fs: &dyn FsApi, path: &str, data: &[u8]) -> Node {
        let mut s = fs.open_write(path, None).unwrap();
        s.write(0, data).unwrap();
        s.commit().unwrap()
    }

    /// Remote stand-in that counts every call: mount-time probes are the
    /// only traffic a local-prefix workload may cause (P4-6).
    /// Unix-only: its single user (`local_ops_never_reach_remote`) mounts
    /// a local rule through LocalFs, which refuses to open on Windows.
    #[cfg(unix)]
    struct CountingFs {
        inner: Arc<dyn FsApi>,
        calls: AtomicUsize,
    }

    #[cfg(unix)]
    impl CountingFs {
        fn new(inner: Arc<dyn FsApi>) -> Self {
            Self {
                inner,
                calls: AtomicUsize::new(0),
            }
        }
        fn count(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[cfg(unix)]
    impl FsApi for CountingFs {
        fn lookup(&self, path: &str) -> Result<Node> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.lookup(path)
        }
        fn readdir(&self, path: &str) -> Result<Vec<Node>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.readdir(path)
        }
        fn mkdir(&self, path: &str, mode: u32) -> Result<Node> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.mkdir(path, mode)
        }
        fn create(&self, path: &str, mode: u32) -> Result<Node> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.create(path, mode)
        }
        fn open(&self, path: &str) -> Result<Node> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.open(path)
        }
        fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.read(path, offset, size)
        }
        fn read_stream(
            &self,
            path: &str,
            offset: u64,
            size: u64,
        ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.read_stream(path, offset, size)
        }
        fn open_write(
            &self,
            path: &str,
            expected_version: Option<i64>,
        ) -> Result<Box<dyn WriteSession + '_>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.open_write(path, expected_version)
        }
        fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.rename(old_path, new_path)
        }
        fn unlink(&self, path: &str) -> Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.unlink(path)
        }
        fn rmdir(&self, path: &str) -> Result<()> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.rmdir(path)
        }
        fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.setattr(path, mode, mtime)
        }
        fn symlink(&self, target: &str, path: &str) -> Result<Node> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.symlink(target, path)
        }
        fn readlink(&self, path: &str) -> Result<String> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.readlink(path)
        }
        fn lookup_by_id(&self, node_id: i64, path_hint: &str) -> Result<Node> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.lookup_by_id(node_id, path_hint)
        }
        fn read_by_id(
            &self,
            node_id: i64,
            path_hint: &str,
            offset: u64,
            size: u64,
        ) -> Result<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.read_by_id(node_id, path_hint, offset, size)
        }
        fn open_write_by_id(
            &self,
            node_id: i64,
            path: &str,
            expected_version: Option<i64>,
        ) -> Result<Box<dyn WriteSession + '_>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.inner.open_write_by_id(node_id, path, expected_version)
        }
    }

    // P4-1: no policy = exactly the baseline (no router at all).
    #[test]
    fn no_policy_is_passthrough() {
        let core = mem_core("p1");
        assert!(
            RoutingFs::build(core.clone(), None, Path::new("/tmp/lightfs-p-nonexistent"))
                .unwrap()
                .is_none()
        );
        let empty = section(vec![]);
        assert!(
            RoutingFs::build(core, Some(&empty), Path::new("/tmp/lightfs-p-nonexistent"))
                .unwrap()
                .is_none()
        );
    }

    // P4-2: full lifecycle under one client-local rule, byte-accurate,
    // including the on-disk authority and a remount.
    #[test]
    #[cfg(unix)]
    fn local_rule_full_lifecycle() {
        let t = tmpdir("p2");
        let root = t.join("root");
        let core = mem_core("p2");
        let sec = section(vec![local_rule("/p", &root)]);
        let rf = RoutingFs::build(core.clone(), Some(&sec), &t.join("mnt"))
            .unwrap()
            .unwrap();

        let node = rf.create("/p/a.txt", 0o644).unwrap();
        assert!(node.id < 0, "local ids live in the negative domain");
        write_via(&rf, "/p/a.txt", b"hello world");
        assert_eq!(rf.read("/p/a.txt", 0, 100).unwrap(), b"hello world");

        let entries = rf.readdir("/p").unwrap();
        assert_eq!(entries.iter().filter(|n| n.name == "a.txt").count(), 1);
        // the reserved scratch dir is never part of the namespace
        assert!(!entries.iter().any(|n| n.name == ".lightfs-tmp"));

        rf.mkdir("/p/d", 0o755).unwrap();
        rf.create("/p/d/nested.bin", 0o644).unwrap();
        write_via(&rf, "/p/d/nested.bin", &[1, 2, 3]);
        assert_eq!(rf.read("/p/d/nested.bin", 0, 10).unwrap(), &[1, 2, 3]);

        // rename keeps identity and bytes; the LOCAL DIR is the authority
        let id = rf.lookup("/p/a.txt").unwrap().id;
        rf.rename("/p/a.txt", "/p/b.txt").unwrap();
        assert_eq!(rf.lookup_by_id(id, "").unwrap().name, "b.txt");
        assert_eq!(rf.read_by_id(id, "", 0, 100).unwrap(), b"hello world");
        assert_eq!(
            std::fs::read(root.join("b.txt")).unwrap(),
            b"hello world".to_vec()
        );
        assert!(!root.join("a.txt").exists());

        let n = rf
            .setattr("/p/b.txt", Some(0o600), Some(1_700_000_000))
            .unwrap();
        assert_eq!(n.mode & 0o777, 0o600);
        assert_eq!(n.mtime, 1_700_000_000);

        // delete → old identity reports IdentityGone; a fresh create at
        // the same path gets a fresh identity (M3 locally)
        rf.unlink("/p/b.txt").unwrap();
        assert!(matches!(
            rf.lookup_by_id(id, ""),
            Err(Error::IdentityGone(_))
        ));
        let n2 = rf.create("/p/b.txt", 0o644).unwrap();
        assert_ne!(n2.id, id);

        assert!(matches!(rf.rmdir("/p/d"), Err(Error::DirectoryNotEmpty(_))));
        rf.unlink("/p/d/nested.bin").unwrap();
        rf.rmdir("/p/d").unwrap();

        // remount: bytes persist; identities are mount-scoped and re-armed
        drop(rf);
        let rf2 = RoutingFs::build(core, Some(&sec), &t.join("mnt"))
            .unwrap()
            .unwrap();
        let n3 = rf2.lookup("/p/b.txt").unwrap();
        assert_eq!(std::fs::read(root.join("b.txt")).unwrap(), Vec::<u8>::new());
        assert!(n3.id < 0 && n3.id != n2.id);
    }

    // P4-3: identity routing - a rename must not re-route old handles to
    // whatever now occupies the old path, and a session that spans a
    // rename commits through the file's NEW location.
    #[test]
    #[cfg(unix)]
    fn identity_routes_by_id_not_path() {
        let t = tmpdir("p3");
        let sec = section(vec![local_rule("/p", &t.join("root"))]);
        let rf = RoutingFs::build(mem_core("p3"), Some(&sec), &t.join("mnt"))
            .unwrap()
            .unwrap();

        rf.create("/p/f", 0o644).unwrap();
        write_via(&rf, "/p/f", b"AAA");
        let id = rf.lookup("/p/f").unwrap().id;

        rf.rename("/p/f", "/p/g").unwrap();
        // a DIFFERENT file now occupies the old path
        rf.create("/p/f", 0o644).unwrap();
        write_via(&rf, "/p/f", b"BBB");

        // the OLD identity still reads the RENAMED file's bytes
        assert_eq!(rf.read_by_id(id, "", 0, 10).unwrap(), b"AAA");
        assert_ne!(rf.read_by_id(id, "", 0, 10).unwrap(), b"BBB");

        // write session opened before a rename, committed after: the
        // commit lands on the renamed file (by identity), nowhere else.
        rf.create("/p/s", 0o644).unwrap();
        let mut sess = rf.open_write("/p/s", None).unwrap();
        rf.rename("/p/s", "/p/s2").unwrap();
        sess.write(0, b"moved").unwrap();
        sess.commit().unwrap();
        assert_eq!(rf.read("/p/s2", 0, 10).unwrap(), b"moved");
        assert!(matches!(rf.lookup("/p/s"), Err(Error::NotFound(_))));
    }

    // P4-4: cross-backend rename = EXDEV, both sides intact.
    #[test]
    #[cfg(unix)]
    fn exdev_cross_backend() {
        let t = tmpdir("p4");
        let core = mem_core("p4");
        core.mkdir("/r", 0o755).unwrap();
        let sec = section(vec![local_rule("/p", &t.join("root"))]);
        let rf = RoutingFs::build(core.clone(), Some(&sec), &t.join("mnt"))
            .unwrap()
            .unwrap();

        rf.create("/p/f", 0o644).unwrap();
        write_via(&rf, "/p/f", b"local");
        rf.create("/r/g", 0o644).unwrap();
        write_via(&rf, "/r/g", b"remote");

        assert!(matches!(rf.rename("/p/f", "/r/x"), Err(Error::Exdev(_))));
        assert!(matches!(rf.rename("/r/g", "/p/y"), Err(Error::Exdev(_))));
        assert_eq!(rf.read("/p/f", 0, 10).unwrap(), b"local");
        assert_eq!(rf.read("/r/g", 0, 10).unwrap(), b"remote");

        // two local rules, two roots: still EXDEV (different backends)
        let t2 = tmpdir("p4b");
        let sec2 = section(vec![
            local_rule("/a", &t2.join("ra")),
            local_rule("/b", &t2.join("rb")),
        ]);
        let rf2 = RoutingFs::build(mem_core("p4b"), Some(&sec2), &t2.join("mnt"))
            .unwrap()
            .unwrap();
        rf2.create("/a/x", 0o644).unwrap();
        assert!(matches!(rf2.rename("/a/x", "/b/y"), Err(Error::Exdev(_))));
    }

    // P4-5: shadowed remote content refuses the mount by default;
    // explicit allow_shadow accepts with a loud warning.
    #[test]
    #[cfg(unix)]
    fn shadow_refuse_by_default_allow_warns() {
        let t = tmpdir("p5");
        let core = mem_core("p5");
        core.mkdir("/work", 0o755).unwrap();
        core.create("/work/keep.txt", 0o644).unwrap();

        let mut rule = local_rule("/work", &t.join("root"));
        let sec = section(vec![rule.clone()]);
        let err = match RoutingFs::build(core.clone(), Some(&sec), &t.join("mnt")) {
            Err(e) => e,
            Ok(_) => panic!("shadowed mount must be refused"),
        };
        assert!(err.to_string().contains("shadow"), "{err}");

        rule.allow_shadow = Some(true);
        let sec = section(vec![rule]);
        // prints the WARNING to stderr; accepting is the assert
        assert!(
            RoutingFs::build(core, Some(&sec), &t.join("mnt"))
                .unwrap()
                .is_some()
        );

        // a FREE prefix needs no opt-in
        let sec = section(vec![local_rule("/free", &t.join("root2"))]);
        assert!(
            RoutingFs::build(mem_core("p5b"), Some(&sec), &t.join("mnt"))
                .unwrap()
                .is_some()
        );
    }

    // P4-6 (+ by-id): local-prefix traffic never reaches the remote
    // backend - not namespace ops, not by-id ops. The ONLY remote call is
    // the mount-time shadow probe.
    #[test]
    #[cfg(unix)]
    fn local_ops_never_reach_remote() {
        let t = tmpdir("p6");
        let core = mem_core("p6");
        let counting = Arc::new(CountingFs::new(core.clone()));
        let sec = section(vec![local_rule("/p", &t.join("root"))]);
        let rf = RoutingFs::build(
            counting.clone() as Arc<dyn FsApi>,
            Some(&sec),
            &t.join("mnt"),
        )
        .unwrap()
        .unwrap();

        // exactly one remote call so far: the shadow probe
        let probe = counting.count();
        assert_eq!(probe, 1, "mount makes exactly one shadow probe");

        rf.create("/p/f", 0o644).unwrap();
        write_via(&rf, "/p/f", b"data");
        let _ = rf.read("/p/f", 0, 10);
        let _ = rf.readdir("/p");
        let id = rf.lookup("/p/f").unwrap().id;
        let _ = rf.lookup_by_id(id, "");
        let _ = rf.read_by_id(id, "", 0, 4);
        rf.rename("/p/f", "/p/g").unwrap();
        rf.unlink("/p/g").unwrap();
        // a negative id no backend knows: IdentityGone, not a remote call
        assert!(matches!(
            rf.lookup_by_id(-999_999, "hint"),
            Err(Error::IdentityGone(_))
        ));
        assert_eq!(
            counting.count(),
            probe,
            "zero remote traffic for the local workload"
        );

        // a remote-prefix op DOES reach the remote (sanity: the counter
        // counts, the router did not just swallow everything)
        core.mkdir("/r", 0o755).unwrap();
        assert!(rf.lookup("/r").is_ok());
        assert_eq!(counting.count(), probe + 1);
    }

    // P-R2/Q1: the mount entry refuses unimplemented features with the SAME
    // errors the parser produces - parser and mount gate are one surface.
    // Q1 opened `cache = "memory"` (default capacity LRU, one rule): that
    // config now MOUNTS and reads work through the cache; the refusals
    // below keep their exact shape.
    #[test]
    fn unimplemented_features_refuse_the_mount() {
        let t = tmpdir("pr2");
        let core = mem_core("pr2");

        // cache = disk: model-valid, feature-unbuilt -> named refusal
        let mut r = local_rule("/p", &t.join("root"));
        r.storage = "s3".into();
        r.root = None;
        r.cache = Some("disk".into());
        r.disk_budget = Some("1GiB".into());
        r.cache_root = Some("~/cache/p".into());
        let mut sec = section(vec![r]);
        sec.limits = Some(crate::policy::PolicyLimits {
            memory_budget: None,
            disk_budget: Some("2GiB".into()),
        });
        let err = match RoutingFs::build(core.clone(), Some(&sec), &t.join("mnt")) {
            Err(e) => e,
            Ok(_) => panic!("an unimplemented cache must refuse the mount"),
        };
        assert!(
            err.to_string().contains("no implementation"),
            "mount error must name the reason: {err}"
        );

        // two memory-cached rules: the one-cached-rule limit refuses
        let mk = |p: &str| crate::policy::RawRule {
            prefix: p.into(),
            storage: "s3".into(),
            cache: Some("memory".into()),
            memory_budget: Some("1MiB".into()),
            ..Default::default()
        };
        let sec = section(vec![mk("/a"), mk("/b")]);
        let mut sec_limits = sec;
        sec_limits.limits = Some(crate::policy::PolicyLimits {
            memory_budget: Some("8MiB".into()),
            disk_budget: None,
        });
        let err = match RoutingFs::build(core.clone(), Some(&sec_limits), &t.join("mnt")) {
            Err(e) => e,
            Ok(_) => panic!("two cached rules must refuse the mount"),
        };
        assert!(
            err.to_string().contains("at most one memory-cached rule"),
            "mount error must name the one-rule limit: {err}"
        );

        // authority_life = task on a local rule: same surface
        let mut r = local_rule("/jobs", &t.join("jobs-root"));
        r.authority_life = Some("task".into());
        let err = match RoutingFs::build(core, Some(&section(vec![r])), &t.join("mnt")) {
            Err(e) => e,
            Ok(_) => panic!("TaskScoped cleanup must refuse the mount until it exists"),
        };
        assert!(
            err.to_string().contains("TaskScoped"),
            "mount error must name the reason: {err}"
        );
    }

    // Q1/Q2: a memory-cached s3 policy with NO local rules still mounts
    // (the router exists to own the CacheFs) and reads work through it.
    #[test]
    fn memory_cached_s3_policy_mounts_and_reads() {
        let core = mem_core("q2");
        let mut r = crate::policy::RawRule {
            prefix: "/hot".into(),
            storage: "s3".into(),
            cache: Some("memory".into()),
            memory_budget: Some("1MiB".into()),
            ..Default::default()
        };
        r.lifecycle = Some("capacity".into());
        let mut sec = section(vec![r]);
        sec.limits = Some(crate::policy::PolicyLimits {
            memory_budget: Some("8MiB".into()),
            disk_budget: None,
        });
        let rf = RoutingFs::build(core.clone(), Some(&sec), &tmpdir("q2").join("mnt"))
            .unwrap()
            .expect("the cached policy mounts even without local rules");

        rf.mkdir("/hot", 0o755).unwrap();
        rf.create("/hot/f", 0o644).unwrap();
        write_via(&rf, "/hot/f", b"cached bytes");
        assert_eq!(rf.read("/hot/f", 0, 100).unwrap(), b"cached bytes");
        let id = rf.lookup("/hot/f").unwrap().id;
        // by-id twice: the read path under test (FUSE reads by id)
        assert_eq!(
            rf.read_by_id(id, "/hot/f", 0, 100).unwrap(),
            b"cached bytes"
        );
        assert_eq!(
            rf.read_by_id(id, "/hot/f", 0, 100).unwrap(),
            b"cached bytes"
        );

        // the commit chain keeps its authority guarantee through the router
        let mut ws = rf.open_write_by_id(id, "/hot/f", None).unwrap();
        ws.write(0, b"committed through the router").unwrap();
        ws.commit().unwrap();
        assert_eq!(
            rf.read_by_id(id, "/hot/f", 0, 100).unwrap(),
            b"committed through the router"
        );
        // a second rule prefix resolves to the default (no-cache) policy
        core.mkdir("/cold", 0o755).unwrap();
        core.create("/cold/g", 0o644).unwrap();
        write_via(&rf, "/cold/g", b"cold");
        assert_eq!(
            rf.read_by_id(rf.lookup("/cold/g").unwrap().id, "/cold/g", 0, 10)
                .unwrap(),
            b"cold"
        );
    }

    // P4-7: unsafe roots and unsupported config fail loudly.
    #[test]
    #[cfg(unix)]
    fn unsafe_roots_and_bad_config_refused() {
        let t = tmpdir("p7");
        let core = mem_core("p7");

        // root == mountpoint
        let sec = section(vec![local_rule("/p", &t)]);
        assert!(RoutingFs::build(core.clone(), Some(&sec), &t).is_err());

        // root inside the mount tree
        let sec = section(vec![local_rule("/p", &t.join("inside"))]);
        assert!(RoutingFs::build(core.clone(), Some(&sec), &t).is_err());

        // mount tree inside the root
        let outer = tmpdir("p7-outer");
        let sec = section(vec![local_rule("/p", &outer)]);
        assert!(RoutingFs::build(core.clone(), Some(&sec), &outer.join("mnt")).is_err());

        // symlink escape: the configured root resolves INTO the mount tree
        let link_dir = tmpdir("p7-links");
        std::fs::create_dir_all(t.join("sneaky")).unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(t.join("sneaky"), link_dir.join("link")).unwrap();
        let sec = section(vec![local_rule("/p", &link_dir.join("link"))]);
        assert!(RoutingFs::build(core.clone(), Some(&sec), &t).is_err());

        // two roots, one inside the other
        let t2 = tmpdir("p7-nest");
        let sec = section(vec![
            local_rule("/a", &t2.join("outer-root")),
            local_rule("/b", &t2.join("outer-root").join("inner")),
        ]);
        // mountpoint stays disjoint; the refusal must come from the
        // root-vs-root containment check
        match RoutingFs::build(core, Some(&sec), &t2.join("mnt")) {
            Err(e) => assert!(e.to_string().contains("overlaps rule"), "{e}"),
            Ok(_) => panic!("nested roots must be refused"),
        }
    }

    // P4-8: two rules, two roots, fully independent.
    #[test]
    #[cfg(unix)]
    fn two_roots_independent() {
        let t = tmpdir("p8");
        let sec = section(vec![
            local_rule("/a", &t.join("ra")),
            local_rule("/b", &t.join("rb")),
        ]);
        let rf = RoutingFs::build(mem_core("p8"), Some(&sec), &t.join("mnt"))
            .unwrap()
            .unwrap();
        rf.create("/a/x", 0o644).unwrap();
        rf.create("/b/x", 0o644).unwrap();
        write_via(&rf, "/a/x", b"AAA");
        write_via(&rf, "/b/x", b"BBB");
        assert_eq!(rf.read("/a/x", 0, 10).unwrap(), b"AAA");
        assert_eq!(rf.read("/b/x", 0, 10).unwrap(), b"BBB");
        assert_eq!(
            std::fs::read(t.join("ra").join("x")).unwrap(),
            b"AAA".to_vec()
        );
        assert_eq!(
            std::fs::read(t.join("rb").join("x")).unwrap(),
            b"BBB".to_vec()
        );
        rf.rename("/a/x", "/a/y").unwrap();
        assert!(matches!(rf.lookup("/a/x"), Err(Error::NotFound(_))));
        assert_eq!(rf.read("/a/y", 0, 10).unwrap(), b"AAA");
    }

    // P4-9: the remote path through the router keeps S3 semantics
    // (CAS conflicts, rename, error propagation), and the local rule
    // leaves the remote namespace untouched (P4-6's real assertion).
    #[test]
    #[cfg(unix)]
    fn remote_baseline_through_router() {
        let t = tmpdir("p9");
        let core = mem_core("p9");
        let sec = section(vec![local_rule("/p", &t.join("root"))]);
        let rf = RoutingFs::build(core.clone(), Some(&sec), &t.join("mnt"))
            .unwrap()
            .unwrap();

        // local ops touch nothing remote
        rf.create("/p/f", 0o644).unwrap();
        write_via(&rf, "/p/f", b"local");
        assert!(matches!(core.lookup("/p/f"), Err(Error::NotFound(_))));
        assert!(core.readdir("/").unwrap().is_empty());

        // remote ops work verbatim
        rf.mkdir("/r", 0o755).unwrap();
        rf.create("/r/f", 0o644).unwrap();
        write_via(&rf, "/r/f", b"v2");
        assert!(core.lookup("/r/f").is_ok(), "remote node exists");
        assert_eq!(rf.read("/r/f", 0, 10).unwrap(), b"v2");

        // stale CAS through the router still conflicts (error propagates)
        rf.create("/r/cas", 0o644).unwrap();
        write_via(&rf, "/r/cas", b"first");
        let now = rf.lookup("/r/cas").unwrap().version;
        let mut stale = rf.open_write("/r/cas", Some(now + 1)).unwrap();
        stale.write(0, b"stale").unwrap();
        assert!(matches!(stale.commit(), Err(Error::Conflict { .. })));

        rf.rename("/r/f", "/r/f2").unwrap();
        assert!(matches!(core.lookup("/r/f"), Err(Error::NotFound(_))));
        assert!(core.lookup("/r/f2").is_ok());
        assert!(matches!(
            rf.read("/r/missing", 0, 1),
            Err(Error::NotFound(_))
        ));
        rf.unlink("/r/f2").unwrap();
        rf.unlink("/r/cas").unwrap();
        rf.rmdir("/r").unwrap();
    }
}
