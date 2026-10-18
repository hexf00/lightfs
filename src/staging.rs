//! S4.25: bounded staging-disk admission, shared by the vnext core
//! (verified full-read staging files, §二) and the mounted-write spool
//! (server `WriteHandle` and client write sessions, §三).
//!
//! Contract:
//! - NOTHING enters a staging file before an admission reservation for
//!   its expected size succeeds. The expected size comes from the layer
//!   that can actually know it (a recorded logical size, or the object
//!   length reported by the backend response) and is CORRECTED to the
//!   verified payload length after decode — the reservation never
//!   permanently assumes logical length == payload length.
//! - Refusal is `Error::Capacity` (mapped to ENOSPC by the FUSE/WinFsp
//!   adapters); staged data is never evicted to make room and never
//!   partially admitted.
//! - A reservation lives exactly as long as its staging data: held while
//!   a consumer still owns the reader, grown only by explicit admission,
//!   released on drop/commit/cancel — so a slow consumer holds its bytes
//!   and a cancelled one gives them back.
//! - Staging files live in a per-process MANAGED subdirectory guarded by
//!   a kernel lock (`fslock`): a live process's directory is never
//!   touched; at startup, directories whose lock is free are provably
//!   owned by dead processes and are swept. Files are created 0600 with
//!   O_EXCL and deleted on every exit path.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::{Error, Result};

/// What a reservation is for — observation only; both kinds draw from
/// the SAME per-process budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StagingKind {
    /// A verified full-read staging file (server read path, salvage).
    Read,
    /// A mounted-write spool file (server WriteHandle / client session).
    Spool,
}

#[derive(Debug, Clone, Copy)]
pub struct StagingLimits {
    /// Ceiling on total staging bytes this process may hold at once.
    /// 0 = unbounded (explicit opt-out; discouraged).
    pub budget_bytes: u64,
    /// Ceiling per object/session. 0 = no per-object cap.
    pub max_object_bytes: u64,
}

impl Default for StagingLimits {
    fn default() -> Self {
        Self { budget_bytes: 2 * 1024 * 1024 * 1024, max_object_bytes: 0 }
    }
}

#[derive(Debug, Default, Clone)]
pub struct StagingStats {
    pub budget_bytes: u64,
    pub max_object_bytes: u64,
    pub used: u64,
    pub read_bytes: u64,
    pub spool_bytes: u64,
    pub active_reads: u64,
    pub active_write_sessions: u64,
    pub admitted_total: u64,
    pub refused_total: u64,
}

#[derive(Debug)]
pub struct StagingBudget {
    limits: StagingLimits,
    /// This process's managed staging directory (created 0700).
    dir: PathBuf,
    /// Kernel lock on `<dir>/staging.lock`, held for the budget's (=
    /// process's) lifetime. Its presence marks the directory LIVE.
    _lock: Option<crate::fslock::FileLock>,
    used: AtomicU64,
    read_bytes: AtomicU64,
    spool_bytes: AtomicU64,
    active_reads: AtomicU64,
    active_write_sessions: AtomicU64,
    admitted_total: AtomicU64,
    refused_total: AtomicU64,
}

impl StagingBudget {
    /// Sweep provably-dead staging directories under the temp root, then
    /// create this process's own directory and lock it.
    pub fn open(limits: StagingLimits) -> std::io::Result<Arc<StagingBudget>> {
        Self::open_at(limits, std::env::temp_dir())
    }

    /// Test seam: same, under an explicit root.
    pub fn open_at(limits: StagingLimits, root: PathBuf) -> std::io::Result<Arc<StagingBudget>> {
        sweep_stale_staging(&root);
        let dir = root.join(format!(
            "lightfs-staging-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir(&dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
        }
        let lock = crate::fslock::FileLock::try_acquire(&dir.join("staging.lock")).map_err(|e| {
            std::io::Error::other(format!("staging dir lock: {e}"))
        })?;
        Ok(Arc::new(StagingBudget {
            limits,
            dir,
            _lock: Some(lock),
            used: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            spool_bytes: AtomicU64::new(0),
            active_reads: AtomicU64::new(0),
            active_write_sessions: AtomicU64::new(0),
            admitted_total: AtomicU64::new(0),
            refused_total: AtomicU64::new(0),
        }))
    }

    /// A no-filesystem-governance budget for tests that only exercise
    /// admission arithmetic or create files directly in the temp root
    /// (no managed directory, no lock, no sweep participation).
    pub fn headless(limits: StagingLimits) -> Arc<StagingBudget> {
        Arc::new(StagingBudget {
            limits,
            dir: std::env::temp_dir(),
            _lock: None,
            used: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
            spool_bytes: AtomicU64::new(0),
            active_reads: AtomicU64::new(0),
            active_write_sessions: AtomicU64::new(0),
            admitted_total: AtomicU64::new(0),
            refused_total: AtomicU64::new(0),
        })
    }

    /// The managed staging directory (files MUST be created here, so the
    /// sweep's ownership rule covers them).
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn limits(&self) -> StagingLimits {
        self.limits
    }

    /// Pre-flight check for a specific object/session size without
    /// taking a reservation (used to refuse BEFORE the network fetch).
    pub fn admits(&self, want: u64) -> Result<()> {
        if self.limits.max_object_bytes != 0 && want > self.limits.max_object_bytes {
            self.refused_total.fetch_add(1, Ordering::Relaxed);
            return Err(Error::Capacity(format!(
                "staging admission refused: object needs {want} bytes, per-object cap is {}",
                self.limits.max_object_bytes
            )));
        }
        Ok(())
    }

    /// Reserve `want` bytes. Refusal = `Error::Capacity`; nothing is
    /// partially admitted.
    pub fn try_reserve(
        self: &Arc<Self>,
        want: u64,
        kind: StagingKind,
        what: &str,
    ) -> Result<StagingReservation> {
        self.admits(want)?;
        if want == 0 {
            // zero-byte reservations still observe session liveness
            return Ok(StagingReservation::new(self.clone(), 0, kind));
        }
        loop {
            let cur = self.used.load(Ordering::Relaxed);
            if self.limits.budget_bytes != 0 && cur.saturating_add(want) > self.limits.budget_bytes {
                self.refused_total.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Capacity(format!(
                    "staging budget exhausted: {} bytes in use, {want} more needed for {what}, \
                     budget is {} bytes — refuse, never evict",
                    cur, self.limits.budget_bytes
                )));
            }
            if self
                .used
                .compare_exchange(cur, cur + want, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                self.admitted_total.fetch_add(1, Ordering::Relaxed);
                return Ok(self.materialize(want, kind));
            }
        }
    }

    fn materialize(self: &Arc<Self>, bytes: u64, kind: StagingKind) -> StagingReservation {
        match kind {
            StagingKind::Read => {
                self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
                self.active_reads.fetch_add(1, Ordering::Relaxed);
            }
            StagingKind::Spool => {
                self.spool_bytes.fetch_add(bytes, Ordering::Relaxed);
                self.active_write_sessions.fetch_add(1, Ordering::Relaxed);
            }
        }
        StagingReservation::new(self.clone(), bytes, kind)
    }

    pub fn stats(&self) -> StagingStats {
        StagingStats {
            budget_bytes: self.limits.budget_bytes,
            max_object_bytes: self.limits.max_object_bytes,
            used: self.used.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            spool_bytes: self.spool_bytes.load(Ordering::Relaxed),
            active_reads: self.active_reads.load(Ordering::Relaxed),
            active_write_sessions: self.active_write_sessions.load(Ordering::Relaxed),
            admitted_total: self.admitted_total.load(Ordering::Relaxed),
            refused_total: self.refused_total.load(Ordering::Relaxed),
        }
    }

    fn release(&self, bytes: u64, kind: StagingKind) {
        self.used.fetch_sub(bytes, Ordering::Relaxed);
        match kind {
            StagingKind::Read => {
                self.read_bytes.fetch_sub(bytes, Ordering::Relaxed);
                self.active_reads.fetch_sub(1, Ordering::Relaxed);
            }
            StagingKind::Spool => {
                self.spool_bytes.fetch_sub(bytes, Ordering::Relaxed);
                self.active_write_sessions.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    fn grow(&self, res_bytes: &mut u64, extra: u64, kind: StagingKind, what: &str) -> Result<()> {
        if extra == 0 {
            return Ok(());
        }
        loop {
            let cur = self.used.load(Ordering::Relaxed);
            let want = *res_bytes + extra;
            if self.limits.max_object_bytes != 0 && want > self.limits.max_object_bytes {
                self.refused_total.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Capacity(format!(
                    "staging admission refused: object grew to {want} bytes, per-object cap is {}",
                    self.limits.max_object_bytes
                )));
            }
            if self.limits.budget_bytes != 0 && cur.saturating_add(extra) > self.limits.budget_bytes {
                self.refused_total.fetch_add(1, Ordering::Relaxed);
                return Err(Error::Capacity(format!(
                    "staging budget exhausted: {} bytes in use, {extra} more needed for {what}, \
                     budget is {} bytes — refuse, never evict",
                    cur, self.limits.budget_bytes
                )));
            }
            if self
                .used
                .compare_exchange(cur, cur + extra, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                match kind {
                    StagingKind::Read => self.read_bytes.fetch_add(extra, Ordering::Relaxed),
                    StagingKind::Spool => self.spool_bytes.fetch_add(extra, Ordering::Relaxed),
                };
                *res_bytes = want;
                self.admitted_total.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
        }
    }

    fn shrink(&self, res_bytes: &mut u64, to: u64, kind: StagingKind) {
        let to = to.min(*res_bytes);
        let drop_by = *res_bytes - to;
        if drop_by == 0 {
            return;
        }
        self.used.fetch_sub(drop_by, Ordering::Relaxed);
        match kind {
            StagingKind::Read => self.read_bytes.fetch_sub(drop_by, Ordering::Relaxed),
            StagingKind::Spool => self.spool_bytes.fetch_sub(drop_by, Ordering::Relaxed),
        };
        *res_bytes = to;
    }
}

/// Bytes held against the budget for as long as this value exists. Drop
/// releases them (read consumed/cancelled, spool committed/abandoned).
#[derive(Debug)]
pub struct StagingReservation {
    budget: Arc<StagingBudget>,
    bytes: u64,
    kind: StagingKind,
}

impl StagingReservation {
    pub(crate) fn new(budget: Arc<StagingBudget>, bytes: u64, kind: StagingKind) -> Self {
        Self { budget, bytes, kind }
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn budget(&self) -> &Arc<StagingBudget> {
        &self.budget
    }
    /// Admit `extra` more bytes (expected size turned out bigger).
    pub fn grow(&mut self, extra: u64, what: &str) -> Result<()> {
        self.budget.grow(&mut self.bytes, extra, self.kind, what)
    }
    /// Correct down to the verified size (decode succeeded smaller).
    pub fn shrink(&mut self, to: u64) {
        self.budget.shrink(&mut self.bytes, to, self.kind)
    }
}

impl Drop for StagingReservation {
    fn drop(&mut self) {
        self.budget.release(self.bytes, self.kind);
    }
}

/// Remove staging directories whose kernel lock is free — provably left
/// behind by dead processes. A LIVE process's directory is never touched:
/// its lock is held, and flock fails immediately. A directory without a
/// lock file at all is a half-created remnant, also safe to remove.
pub fn sweep_stale_staging(root: &Path) {
    let Ok(entries) = std::fs::read_dir(root) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !name.starts_with("lightfs-staging-") {
            continue;
        }
        let dir = entry.path();
        if !dir.is_dir() {
            continue;
        }
        let lock = dir.join("staging.lock");
        if !lock.exists() {
            let _ = std::fs::remove_dir_all(&dir);
            continue;
        }
        // try_acquire succeeds only when no live process holds the lock
        match crate::fslock::FileLock::try_acquire(&lock) {
            Ok(held) => {
                drop(held);
                let _ = std::fs::remove_dir_all(&dir);
            }
            Err(_) => { /* live process: keep */ }
        }
    }
}

/// Private staging file, deleted on EVERY exit path (success consumption,
/// verification failure, short read, IO error, cancellation-by-drop).
/// Failure to remove is surfaced, never silent (it holds user data).
/// Created 0600 O_EXCL inside the budget's managed directory.
pub(crate) struct TempFileGuard {
    path: PathBuf,
    pub file: Option<std::fs::File>,
}

impl TempFileGuard {
    pub(crate) fn create_in(dir: &Path) -> std::io::Result<TempFileGuard> {
        for _ in 0..8 {
            let p = dir.join(format!(
                "lightfs-staging-file-{}-{}",
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
                Ok(file) => return Ok(TempFileGuard { path: p, file: Some(file) }),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
        Err(std::io::Error::other("could not create staging file"))
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "lightfs-staging-test-{}-{}",
            tag,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|t| t.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn admission_reserves_grows_shrinks_and_releases() {
        let b = StagingBudget::headless(StagingLimits { budget_bytes: 100, max_object_bytes: 60 });
        let mut r = b.try_reserve(40, StagingKind::Read, "t").unwrap();
        assert_eq!(b.stats().used, 40);
        r.grow(20, "t").unwrap();
        assert_eq!(b.stats().used, 60);
        assert!(r.grow(1, "t").is_err(), "per-object cap");
        assert_eq!(b.stats().used, 60, "failed growth must not leak budget");
        r.shrink(50); // correct down to the verified size (50 bytes total)
        assert_eq!(b.stats().used, 50);
        drop(r);
        assert_eq!(b.stats().used, 0);
        assert_eq!(b.stats().refused_total, 1);
    }

    #[test]
    fn budget_refusal_is_named_capacity_and_never_partial() {
        let b = StagingBudget::headless(StagingLimits { budget_bytes: 10, max_object_bytes: 0 });
        let r = b.try_reserve(8, StagingKind::Read, "t").unwrap();
        let err = b
            .try_reserve(8, StagingKind::Read, "second")
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("capacity"), "{err}");
        assert_eq!(b.stats().used, 8, "refused request admitted nothing");
        drop(r);
        assert_eq!(b.stats().used, 0);
    }

    #[test]
    fn slow_consumer_holds_bytes_until_drop() {
        let b = StagingBudget::headless(StagingLimits { budget_bytes: 10, max_object_bytes: 0 });
        let held = b.try_reserve(10, StagingKind::Read, "slow").unwrap();
        assert!(b.try_reserve(1, StagingKind::Read, "fast").is_err());
        drop(held);
        assert!(b.try_reserve(1, StagingKind::Read, "fast").is_ok());
    }

    #[test]
    fn concurrent_admission_stays_within_budget() {
        let b = StagingBudget::headless(StagingLimits { budget_bytes: 40, max_object_bytes: 0 });
        std::thread::scope(|s| {
            for _ in 0..8 {
                let b = &b;
                s.spawn(move || {
                    for _ in 0..50 {
                        match b.try_reserve(10, StagingKind::Read, "race") {
                            Ok(r) => {
                                assert!(b.stats().used <= 40);
                                drop(r);
                            }
                            Err(e) => assert!(e.to_string().contains("capacity")),
                        }
                    }
                });
            }
        });
        assert_eq!(b.stats().used, 0);
        assert!(b.stats().admitted_total > 0);
    }

    #[test]
    fn stale_sweep_removes_dead_dirs_and_keeps_live() {
        let root = tmp_root("sweep");
        // dead: dir with a lock nobody holds
        let dead = root.join("lightfs-staging-999-dead");
        std::fs::create_dir_all(&dead).unwrap();
        std::fs::write(dead.join("staging.lock"), b"").unwrap();
        std::fs::write(dead.join("lightfs-tmp-orphan"), b"data").unwrap();
        // dead: half-created dir without a lock file
        let half = root.join("lightfs-staging-998-half");
        std::fs::create_dir_all(&half).unwrap();
        // live: hold the lock in this process
        let live = root.join("lightfs-staging-997-live");
        std::fs::create_dir_all(&live).unwrap();
        let _guard = crate::fslock::FileLock::try_acquire(&live.join("staging.lock")).unwrap();
        // unrelated dir is never touched
        let other = root.join("not-lightfs-staging");
        std::fs::create_dir_all(&other).unwrap();

        sweep_stale_staging(&root);
        assert!(!dead.exists(), "dead dir must be swept");
        assert!(!half.exists(), "half-created dir must be swept");
        assert!(live.exists(), "live dir must be kept");
        assert!(other.exists(), "unrelated dirs untouched");
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn open_creates_managed_dir_and_tempfile_inside() {
        let root = tmp_root("open");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        assert!(b.dir().is_dir());
        let g = TempFileGuard::create_in(b.dir()).unwrap();
        assert!(g.path().starts_with(b.dir()));
        drop(g);
        assert!(b.dir().join("staging.lock").is_file());
        drop(b);
        let _ = std::fs::remove_dir_all(&root);
    }
}
