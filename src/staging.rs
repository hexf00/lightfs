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
        Self::open_at_impl(limits, root)
    }

    /// No dead-directory sweep. For short-lived client processes, where a
    /// full temp-root scan per process is a per-operation cost (every CLI
    /// invocation is a process): orphaned staging dirs are reclaimed by
    /// the next sweep participant (server boot, mount adapters), which
    /// share the same temp root.
    pub fn open_fresh(limits: StagingLimits) -> std::io::Result<Arc<StagingBudget>> {
        Self::open_at_impl(limits, std::env::temp_dir())
    }

    fn open_at_impl(limits: StagingLimits, root: PathBuf) -> std::io::Result<Arc<StagingBudget>> {
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
            self.admitted_total.fetch_add(1, Ordering::Relaxed);
            return Ok(self.materialize(0, kind));
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
    #[cfg(test)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

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

/// A mounted-write session spool (S4.25 §三): the session's bytes live in
/// ONE private file (0600, managed staging dir), never in the heap. Random
/// writes, sparse extension (holes read as zero), truncate in both
/// directions, range reads (read-your-writes), and a base-content overlay
/// for the commit merge (committed bytes come from the authoritative base
/// stream; dirty ranges keep the spool's own bytes — always newer than
/// the base). The reservation tracks the spool's high-water mark and
/// releases on reset/drop. Crash residue: the file is inside the managed
/// directory and dies with the sweep; the reservation dies with the
/// process either way.
pub struct WriteSpool {
    file: std::fs::File,
    _guard: TempFileGuard,
    /// High-water logical size (POSIX size of the uncommitted file).
    size: u64,
    res: StagingReservation,
    /// Merged, sorted dirty ranges — the bytes this session wrote. The
    /// commit overlay never overwrites these from the base (spool bytes
    /// are strictly newer than the base snapshot being merged).
    dirty: Vec<(u64, u64)>,
    /// Total writes since open/reset; the commit window uses it to tell
    /// "nothing new arrived" from "new writes need another commit".
    write_ops: u64,
}

impl WriteSpool {
    /// Open an empty spool under `budget` (Spool kind: counted in the
    /// active-write-session gauges). Admission for the first write is
    /// taken lazily — an opened-but-never-written session reserves 0.
    pub fn open(budget: &Arc<StagingBudget>) -> Result<WriteSpool> {
        let res = budget.try_reserve(0, StagingKind::Spool, "write session")?;
        let mut guard = TempFileGuard::create_in(budget.dir()).map_err(|e| {
            eprintln!("lightfs: write spool file create failed in {:?}: {e}", budget.dir());
            Error::Storage(format!("write spool file: {e}"))
        })?;
        let file = guard
            .file
            .take()
            .expect("fresh guard holds a file")
            .try_clone()
            .map_err(|e| Error::Storage(format!("write spool handle: {e}")))?;
        Ok(WriteSpool { file, _guard: guard, size: 0, res, dirty: Vec::new(), write_ops: 0 })
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    /// Highest dirty byte end — the session's contribution to the visible
    /// EOF (same role ChunkMap::max_end played).
    pub fn dirty_end(&self) -> u64 {
        self.dirty.last().map(|(_, e)| *e).unwrap_or(0)
    }

    pub fn write_ops(&self) -> u64 {
        self.write_ops
    }

    /// Is any byte dirty (uncommitted session content present)?
    pub fn is_dirty(&self) -> bool {
        !self.dirty.is_empty()
    }

    /// Random write with POSIX sparse semantics: a seek past EOF creates
    /// a hole (reads return zeros there), growth admits the new high-water
    /// mark BEFORE the file grows (Error::Capacity refusal = nothing
    /// written, no growth, no eviction of existing bytes).
    pub fn write(&mut self, offset: u64, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let end = offset.saturating_add(data.len() as u64);
        if end > self.size {
            self.grow_to(end)?;
        }
        use std::io::{Seek, SeekFrom, Write as _};
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(data)?;
        self.mark_dirty(offset, end);
        self.write_ops += 1;
        Ok(())
    }

    /// Truncate/extend to `new_size` (zero-filling an extension), with the
    /// same grow-admission contract as [`Self::write`]; shrinking releases
    /// the difference back to the budget and discards dirty ranges past
    /// the new end.
    pub fn truncate(&mut self, new_size: u64) -> Result<()> {
        if new_size > self.size {
            self.grow_to(new_size)?;
            self.write_ops += 1;
            return Ok(());
        }
        if new_size < self.size {
            self.res.shrink(new_size);
            self.file.set_len(new_size)?;
            self.size = new_size;
            // A range STRADDLING the new EOF keeps its pre-EOF prefix
            // (ftruncate semantics: pages beyond the new EOF are discarded
            // and never resent, but the prefix below the EOF is user
            // data). Dropping the whole range let the base stream clobber
            // live dirty bytes during the commit overlay (WinFsp accept
            // A5: shrink a straddling chunk, then append — byte 0 came
            // back as base zero instead of the written pattern).
            self.dirty.retain(|r| r.0 < new_size);
            for r in self.dirty.iter_mut() {
                r.1 = r.1.min(new_size);
            }
            self.write_ops += 1;
        }
        Ok(())
    }

    fn grow_to(&mut self, end: u64) -> Result<()> {
        if end > self.res.bytes() {
            self.res.grow(end - self.res.bytes(), "write session")?;
        } else {
            self.res.shrink(end);
        }
        self.file.set_len(end)?;
        self.size = end;
        Ok(())
    }

    fn mark_dirty(&mut self, from: u64, to: u64) {
        // merge into the sorted, disjoint range list
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(self.dirty.len() + 1);
        let mut s = from;
        let mut e = to;
        for &(rs, re) in &self.dirty {
            if re < s {
                merged.push((rs, re));
            } else if e < rs {
                merged.push((s, e));
                s = rs;
                e = re;
            } else {
                s = s.min(rs);
                e = e.max(re);
            }
        }
        merged.push((s, e));
        self.dirty = merged;
    }

    /// Read-your-writes range read: holes (never-written bytes below the
    /// size) read as zeros, exactly like the old in-memory overlay.
    /// Positioned reads (pread semantics) — readers never disturb the
    /// writer's cursor, so concurrent commit overlay and app reads are
    /// safe on one spool.
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        buf.fill(0);
        if offset >= self.size || buf.is_empty() {
            return Ok(());
        }
        let mut filled = 0usize;
        while filled < buf.len() {
            let n = {
                #[cfg(unix)]
                {
                    use std::os::unix::fs::FileExt as _;
                    self.file.read_at(&mut buf[filled..], offset + filled as u64)?
                }
                #[cfg(windows)]
                {
                    use std::os::windows::fs::FileExt as _;
                    self.file.seek_read(&mut buf[filled..], offset + filled as u64)?
                }
                #[cfg(not(any(unix, windows)))]
                {
                    let _ = offset + filled as u64;
                    return Err(Error::Storage(
                        "write spool positioned reads unsupported on this platform".into(),
                    ));
                }
            };
            if n == 0 {
                break;
            }
            filled += n;
        }
        Ok(())
    }

    /// Merge the authoritative BASE content into the spool (commit prep):
    /// every non-dirty byte below `base_len` is streamed in from `base`
    /// (a 64 KiB-bounded copy loop — the base is never materialized);
    /// dirty ranges keep the spool's own bytes, and the base stream is
    /// consumed (discarded) across them so a single sequential pass works
    /// on any backend. Extends the spool when the base is longer.
    pub fn overlay_base(&mut self, base: &mut dyn std::io::Read, base_len: u64) -> Result<()> {
        if base_len > self.size {
            self.grow_to(base_len)?;
        }
        let mut buf = vec![0u8; 64 * 1024];
        let mut base_pos: u64 = 0;
        let mut next_dirty = self.dirty.iter().copied();
        let mut pending = next_dirty.next();
        while base_pos < base_len {
            let (gap_end, skip_to) = match pending {
                Some((ds, de)) if ds < base_len => (ds, de.min(base_len)),
                _ => (base_len, base_len),
            };
            if base_pos < gap_end {
                copy_stream_range(base, &mut self.file, base_pos, gap_end, &mut buf)?;
                base_pos = gap_end;
            }
            if let Some(_) = pending {
                // consume (discard) the base's bytes under the dirty span
                let mut left = skip_to.saturating_sub(base_pos);
                while left > 0 {
                    let want = (left as usize).min(buf.len());
                    let n = read_full(base, &mut buf[..want])?;
                    if n == 0 {
                        return Err(Error::Storage(format!(
                            "base stream ended {} bytes short during commit overlay",
                            left
                        )));
                    }
                    left -= n as u64;
                    base_pos += n as u64;
                }
                pending = next_dirty.next();
                if pending.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        if base_pos < base_len {
            copy_stream_range(base, &mut self.file, base_pos, base_len, &mut buf)?;
        }
        Ok(())
    }

    /// Overlay the session's dirty bytes onto a read buffer at
    /// `read_offset` — the exact role ChunkMap::overlay_range played in
    /// the same-mount read-coherence path. Only dirty ranges touch the
    /// buffer (holes are NOT zeros here: unwritten spool bytes must not
    /// mask committed base content).
    pub fn overlay_range(&self, buf: &mut [u8], read_offset: u64) -> Result<()> {
        let end = read_offset + buf.len() as u64;
        for &(rs, re) in &self.dirty {
            let s = rs.max(read_offset);
            let e = re.min(end);
            if s >= e {
                continue;
            }
            let mut tmp = vec![0u8; (e - s) as usize];
            self.read_at(s, &mut tmp)?;
            let from = (s - read_offset) as usize;
            buf[from..from + tmp.len()].copy_from_slice(&tmp);
        }
        Ok(())
    }

    /// Merge `other` INTO `self`, other's bytes WINNING on overlap
    /// (deterministic fd-order fold for detached sessions). Only other's
    /// dirty ranges are copied — its holes never mask self's bytes.
    pub fn merge_from(&mut self, other: WriteSpool) -> Result<()> {
        if other.size > self.size {
            self.grow_to(other.size)?;
        }
        for (rs, re) in other.dirty.clone() {
            let mut tmp = vec![0u8; (re - rs) as usize];
            other.read_at(rs, &mut tmp)?;
            self.write(rs, &tmp)?;
        }
        Ok(())
    }

    /// Merge `other` INTO `self`, SELF's bytes WINNING on overlap — the
    /// failed-commit recovery path (ChunkMap::restore_under semantics):
    /// writes that arrived during the failed commit (self) are newer and
    /// keep their bytes; the drained batch (other) fills the gaps so
    /// nothing is lost for the retry.
    pub fn merge_under(&mut self, mut other: WriteSpool) -> Result<()> {
        if other.size > self.size {
            self.grow_to(other.size)?;
        }
        let self_dirty = self.dirty.clone();
        for (os, oe) in other.dirty.clone() {
            let mut cur = os;
            for &(ss, se) in &self_dirty {
                if se <= cur {
                    continue;
                }
                if ss >= oe {
                    break;
                }
                if ss > cur {
                    let to = ss.min(oe);
                    Self::copy_between(&mut other, self, cur, to)?;
                    // copied bytes are session data: without the dirty
                    // mark the retry flush takes the !is_dirty() no-op
                    // shortcut and the commit overlay lets the base
                    // clobber them (WinFsp accept A7)
                    self.mark_dirty(cur, to);
                }
                cur = cur.max(se);
                if cur >= oe {
                    break;
                }
            }
            if cur < oe {
                Self::copy_between(&mut other, self, cur, oe)?;
                self.mark_dirty(cur, oe);
            }
        }
        Ok(())
    }

    fn copy_between(
        src: &mut WriteSpool,
        dst: &mut WriteSpool,
        from: u64,
        to: u64,
    ) -> Result<()> {
        let mut pos = from;
        while pos < to {
            let want = ((to - pos) as usize).min(64 * 1024);
            let mut tmp = vec![0u8; want];
            src.read_at(pos, &mut tmp)?;
            dst.write(pos, &tmp)?;
            pos += want as u64;
        }
        Ok(())
    }

    /// The commit succeeded with no concurrent writes: release everything
    /// (bytes AND budget) — the committed content now lives in the object
    /// store, the spool is an empty session again.
    pub fn reset(&mut self) -> Result<()> {
        self.res.shrink(0);
        self.file.set_len(0)?;
        self.size = 0;
        self.dirty.clear();
        self.write_ops = 0;
        Ok(())
    }

    /// Rewind the spool file for a streaming read-out (commit upload).
    pub fn rewind(&mut self) -> Result<()> {
        use std::io::Seek;
        self.file.seek(std::io::SeekFrom::Start(0))?;
        Ok(())
    }

    /// Streaming read-out handle for the commit upload (call after
    /// [`Self::rewind`]); the caller streams exactly [`Self::size`] bytes.
    pub fn reader(&mut self) -> &mut std::fs::File {
        &mut self.file
    }
}

fn read_full(src: &mut dyn std::io::Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match src.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

fn copy_stream_range(
    src: &mut dyn std::io::Read,
    dst: &mut std::fs::File,
    from: u64,
    to: u64,
    buf: &mut [u8],
) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write as _};
    let mut pos = from;
    while pos < to {
        let want = ((to - pos) as usize).min(buf.len());
        let n = read_full(src, &mut buf[..want])?;
        if n == 0 {
            return Err(Error::Storage(format!(
                "base stream ended early during commit overlay (at {pos}, wanted {to})"
            )));
        }
        dst.seek(SeekFrom::Start(pos))?;
        dst.write_all(&buf[..n])?;
        pos += n as u64;
    }
    Ok(())
}

impl std::fmt::Debug for WriteSpool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteSpool")
            .field("size", &self.size)
            .field("dirty_ranges", &self.dirty.len())
            .field("reserved", &self.res.bytes())
            .finish()
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

    fn test_spool(budget: &Arc<StagingBudget>) -> WriteSpool {
        WriteSpool::open(budget).unwrap()
    }

    #[test]
    fn spool_random_writes_holes_and_read_your_writes() {
        let root = tmp_root("spool1");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        let mut sp = test_spool(&b);
        assert_eq!(b.stats().active_write_sessions, 1);
        // sparse write at 5000: everything before reads as zero
        sp.write(5000, b"hello").unwrap();
        assert_eq!(sp.size(), 5005);
        let mut buf = vec![0u8; 5010];
        sp.read_at(0, &mut buf).unwrap();
        assert!(buf[..5000].iter().all(|&b| b == 0), "hole must read as zero");
        assert_eq!(&buf[5000..5005], b"hello");
        // overwrite in place
        sp.write(5002, b"XX").unwrap();
        sp.read_at(5000, &mut buf[..5]).unwrap();
        assert_eq!(&buf[..5], b"heXXo");
        // dirty_end tracks the last write
        assert_eq!(sp.dirty_end(), 5005);
        assert_eq!(b.stats().spool_bytes, 5005);
        drop(sp);
        assert_eq!(b.stats().used, 0);
        assert_eq!(b.stats().active_write_sessions, 0);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn spool_truncate_extends_zero_fills_and_shrinks_release() {
        let root = tmp_root("spool2");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        let mut sp = test_spool(&b);
        sp.write(0, b"abcdef").unwrap();
        sp.truncate(10).unwrap();
        assert_eq!(sp.size(), 10);
        let mut buf = [0u8; 10];
        sp.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf[..6], b"abcdef");
        assert_eq!(&buf[6..], &[0; 4]);
        sp.truncate(3).unwrap();
        assert_eq!(sp.size(), 3);
        assert_eq!(b.stats().spool_bytes, 3, "shrinking releases the difference");
        let mut small = [0u8; 3];
        sp.read_at(0, &mut small).unwrap();
        assert_eq!(&small, b"abc");
        drop(sp);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn spool_truncate_keeps_straddling_dirty_prefix_under_base_overlay() {
        // WinFsp accept A5: write 8192, shrink to 4096 (the dirty range
        // straddles the new EOF), then a commit overlays the committed
        // base — the base (zeros) must NOT clobber the kept prefix, and
        // bytes appended past the new EOF must survive.
        let root = tmp_root("spool-straddle");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        let mut sp = test_spool(&b);
        sp.write(0, &[7u8; 8192]).unwrap();
        sp.truncate(4096).unwrap();
        sp.write(4096, b"XYZ").unwrap();
        let mut empty_base = std::io::empty();
        sp.overlay_base(&mut empty_base, 0).unwrap();
        let mut buf = vec![0u8; 4099];
        sp.read_at(0, &mut buf).unwrap();
        assert!(buf[..4096].iter().all(|&x| x == 7), "straddling prefix survives");
        assert_eq!(&buf[4096..], b"XYZ");
        // a real base under the holes: bytes the session never touched
        // come from the base, dirty bytes still win
        let mut sp2 = test_spool(&b);
        sp2.write(4096, &[9u8; 10]).unwrap();
        let base = vec![5u8; 8192];
        let mut rd = std::io::Cursor::new(base);
        sp2.overlay_base(&mut rd, 8192).unwrap();
        let mut got = vec![0u8; 8192];
        sp2.read_at(0, &mut got).unwrap();
        assert_eq!(&got[..4096], &[5u8; 4096]);
        assert_eq!(&got[4096..4106], &[9u8; 10]);
        assert_eq!(&got[4106..], &[5u8; 4086]);
        drop(sp);
        drop(sp2);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn spool_merge_under_registers_copied_holes_as_dirty() {
        // WinFsp accept A7 (multi-handle CAS conflict + retry): the
        // drained spool merges into a FRESH session spool — the copied
        // bytes MUST be dirty, or the retry flush takes the !is_dirty()
        // no-op shortcut and silently drops the session's writes.
        let root = tmp_root("spool-merge");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        let mut a = test_spool(&b); // the surviving (fresh) session spool
        let mut d = test_spool(&b); // the drained attempt spool
        d.write(4096, b"BBBB").unwrap();
        a.merge_under(d).unwrap();
        assert!(a.is_dirty(), "merged bytes must be dirty");
        assert_eq!(a.size(), 4100);
        let mut buf = [0u8; 4];
        a.read_at(4096, &mut buf).unwrap();
        assert_eq!(&buf, b"BBBB");
        // the commit overlay must treat them dirty-wins against a base
        let base = vec![0u8; 4100];
        let mut rd = std::io::Cursor::new(base);
        a.overlay_base(&mut rd, 4100).unwrap();
        a.read_at(4096, &mut buf).unwrap();
        assert_eq!(&buf, b"BBBB", "base overlay must not clobber merged bytes");
        drop(a);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn spool_growth_admission_refuses_without_writing() {
        let root = tmp_root("spool3");
        let b = StagingBudget::open_at(
            StagingLimits { budget_bytes: 100, max_object_bytes: 0 },
            root.clone(),
        )
        .unwrap();
        let mut sp = test_spool(&b);
        sp.write(0, &[7u8; 50]).unwrap();
        let err = sp.write(80, &[9u8; 30]).err().unwrap().to_string();
        assert!(err.contains("capacity"), "{err}");
        assert_eq!(sp.size(), 50, "refused write must not grow the spool");
        let mut buf = [0u8; 50];
        sp.read_at(0, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 7));
        // truncate past the budget refuses the same way
        assert!(sp.truncate(200).is_err());
        assert_eq!(sp.size(), 50);
        drop(sp);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn spool_overlay_base_merges_around_dirty_and_resets() {
        let root = tmp_root("spool4");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        let mut sp = test_spool(&b);
        // base: 0..100 of 'A'; session wrote 'X' at 10..20 and 90..95
        sp.write(10, &[b'X'; 10]).unwrap();
        sp.write(90, &[b'Y'; 5]).unwrap();
        let base = vec![b'A'; 100];
        let mut base_r = std::io::Cursor::new(base);
        sp.overlay_base(&mut base_r, 100).unwrap();
        assert_eq!(sp.size(), 100);
        let mut out = vec![0u8; 100];
        sp.read_at(0, &mut out).unwrap();
        assert!(out[..10].iter().all(|&b| b == b'A'));
        assert_eq!(&out[10..20], &[b'X'; 10]);
        assert!(out[20..90].iter().all(|&b| b == b'A'));
        assert_eq!(&out[90..95], &[b'Y'; 5]);
        assert!(out[95..].iter().all(|&b| b == b'A'));
        // commit success with no concurrent writes: full reset
        assert_eq!(sp.write_ops(), 2);
        sp.reset().unwrap();
        assert_eq!((sp.size(), sp.dirty_end(), sp.write_ops()), (0, 0, 0));
        assert_eq!(b.stats().used, 0, "reset releases the reservation");
        drop(sp);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn spool_overlay_base_extends_and_handles_dirty_past_base() {
        let root = tmp_root("spool5");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        let mut sp = test_spool(&b);
        // session extended far past the base (append without flush)
        sp.write(40, &[b'Z'; 10]).unwrap();
        let base = vec![b'B'; 20];
        let mut base_r = std::io::Cursor::new(base);
        sp.overlay_base(&mut base_r, 20).unwrap();
        assert_eq!(sp.size(), 50);
        let mut out = vec![0u8; 50];
        sp.read_at(0, &mut out).unwrap();
        assert!(out[..20].iter().all(|&b| b == b'B'));
        assert!(out[20..40].iter().all(|&b| b == 0), "gap between base and first write");
        assert_eq!(&out[40..50], &[b'Z'; 10]);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn spool_overlay_base_short_stream_is_an_error() {
        let root = tmp_root("spool6");
        let b = StagingBudget::open_at(StagingLimits::default(), root.clone()).unwrap();
        let mut sp = test_spool(&b);
        sp.write(0, b"dirty").unwrap();
        let base = vec![b'A'; 10];
        let mut base_r = std::io::Cursor::new(base);
        // claim base_len 100 but supply only 10 dirty-free bytes after the
        // dirty span — the overlay must fail, never silently zero-fill
        let r = sp.overlay_base(&mut base_r, 100);
        assert!(r.is_err());
        drop(sp);
        let _ = std::fs::remove_dir_all(&root);
    }
}
