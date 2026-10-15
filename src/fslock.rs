//! Cross-platform exclusive file lock: advisory, kernel-owned, released by
//! the OS the moment the holding process dies (SIGKILL included) — no stale
//! lock files, no retry protocol needed. Unix uses `flock(LOCK_EX|LOCK_NB)`
//! (per open file description, so two descriptors in ONE process also
//! conflict); Windows uses `LockFileEx(LOCKFILE_EXCLUSIVE_LOCK|
//! LOCKFILE_FAIL_IMMEDIATELY)` on one byte at offset 1024 (per handle,
//! same rule; the offset keeps the holder diagnostics at 0..16 readable).
//!
//! Consumers: the write-back engine's single-writer locks (staging dir +
//! capacity ledger, see `wb::WriteBackEngine::new`) and the backup
//! maintenance lock (`backup::MaintenanceLock`). Hand-declared FFI only —
//! no new external dependency.

use std::fs::File;
use std::path::Path;

use crate::error::{Error, Result};

/// An acquired exclusive lock. Holding the struct holds the lock; dropping
/// it (or process death) releases.
#[derive(Debug)]
pub(crate) struct FileLock {
    _file: File,
}

impl FileLock {
    /// Open (or create) the lock file and try to take the exclusive lock
    /// WITHOUT blocking. `Err` when another live holder exists — the error
    /// text names the holder recorded in the file when readable.
    pub(crate) fn try_acquire(path: &Path) -> Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(path)
            .map_err(|e| Error::Config(format!("cannot open lock {}: {e}", path.display())))?;
        if !try_lock_raw(&file) {
            let holder = std::fs::read_to_string(path).unwrap_or_default();
            let holder = holder.trim();
            let who = if holder.is_empty() {
                "another live process".to_string()
            } else {
                format!("another live process ({holder})")
            };
            return Err(Error::Config(format!(
                "{} is locked by {}: kernel locks die with the holder, so this \
                 error means a holder is alive right now",
                path.display(),
                who
            )));
        }
        // Best-effort diagnostics for the NEXT contender; never fatal.
        // Fixed-width, written at offset 0 through the SAME handle: the
        // lock byte sits at offset 1024, far outside this area, so the
        // next contender can READ it (a byte-range lock blocks other
        // handles from touching the locked bytes — a lock at offset 0
        // made the holder line unreadable on Windows) and no truncation
        // is ever needed.
        let _ = (|| -> std::io::Result<()> {
            use std::io::{Seek, SeekFrom, Write};
            let line = format!("pid {}", std::process::id());
            let mut buf = [b' '; 16];
            let n = line.len().min(15);
            buf[..n].copy_from_slice(&line.as_bytes()[..n]);
            buf[15] = b'\n';
            let mut f = &file;
            f.seek(SeekFrom::Start(0))?;
            f.write_all(&buf)
        })();
        Ok(Self { _file: file })
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // The kernel releases byte-range locks when the handle closes
        // anyway; the explicit unlock makes release immediate and
        // independent of close ordering (belt and braces — the unix
        // arm has nothing to do, flock dies with the fd).
        #[cfg(windows)]
        unsafe {
            use std::os::windows::io::AsRawHandle;
            let mut ov = range_overlapped();
            let _ = UnlockFileEx(
                self._file.as_raw_handle() as *mut core::ffi::c_void,
                0,
                1,
                0,
                &mut ov,
            );
        }
        #[cfg(unix)]
        {
            // no explicit unlock exists for flock; the fd closes below
        }
    }
}

/// One non-blocking exclusive-lock attempt on an already-open file.
/// `false` = a live holder exists.
pub(crate) fn try_lock_raw(file: &File) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe { libc::flock(AsRawFd::as_raw_fd(file), libc::LOCK_EX | libc::LOCK_NB) == 0 }
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        let mut ov = range_overlapped();
        unsafe {
            LockFileEx(
                AsRawHandle::as_raw_handle(file) as *mut core::ffi::c_void,
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                1,
                0,
                &mut ov,
            ) != 0
        }
    }
}

#[cfg(windows)]
const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x1;
#[cfg(windows)]
const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x2;

// Offset 1024: byte-range locks block OTHER handles from the locked
// bytes, so the lock must sit outside the diagnostics area at offset
// 0..16 (a lock at offset 0 made the "pid …" holder line unreadable on
// Windows). Locking beyond EOF is valid and standard on Windows.
#[cfg(windows)]
const LOCK_RANGE_OFFSET: u32 = 1024;

#[cfg(windows)]
#[repr(C)]
struct Overlapped {
    internal: usize,
    internal_high: usize,
    offset: u32,
    offset_high: u32,
    event: *mut core::ffi::c_void,
}

#[cfg(windows)]
fn range_overlapped() -> Overlapped {
    Overlapped {
        internal: 0,
        internal_high: 0,
        offset: LOCK_RANGE_OFFSET,
        offset_high: 0,
        event: core::ptr::null_mut(),
    }
}

#[cfg(windows)]
#[link(name = "kernel32")]
unsafe extern "system" {
    fn LockFileEx(
        file: *mut core::ffi::c_void,
        flags: u32,
        reserved: u32,
        count_low: u32,
        count_high: u32,
        overlapped: *mut Overlapped,
    ) -> i32;
    fn UnlockFileEx(
        file: *mut core::ffi::c_void,
        reserved: u32,
        count_low: u32,
        count_high: u32,
        overlapped: *mut Overlapped,
    ) -> i32;
}

/// Resolve a resource (staging directory, database file, backup root) to
/// its canonical filesystem identity: absolute, symlink-free, spelling-
/// independent. Every lock that guards the resource is derived from THIS
/// path, so two spellings of the same resource — relative vs absolute, or
/// through a symlink — compute the same lock and cannot both be taken.
///
/// `fs::canonicalize` fails when the path does not exist; callers therefore
/// create the resource first (staging dir, db file, backup root all are),
/// and any failure is a hard error — never a fall back to raw string
/// identities.
pub(crate) fn canonical_identity(path: &Path, what: &str) -> Result<std::path::PathBuf> {
    std::fs::canonicalize(path).map_err(|e| {
        Error::Config(format!(
            "cannot resolve the identity of {what} {}: {e} — refusing to \
             derive a lock from an ambiguous path",
            path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_conflicts_within_one_process_and_releases_on_drop() {
        // flock is per open file description: a SECOND descriptor in the
        // same process conflicts — exactly the dual-engine shape.
        let dir = std::env::temp_dir().join(format!("lightfs-fslock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("l.lock");
        let l1 = FileLock::try_acquire(&path).unwrap();
        let err = FileLock::try_acquire(&path).unwrap_err();
        assert!(
            format!("{err}").contains("locked by another live process"),
            "{err}"
        );
        assert!(format!("{err}").contains("pid "), "{err}");
        drop(l1);
        // Bounded retry, not a single shot: Darwin can keep a closed flock
        // visible to the next flock for a few tens of microseconds under
        // thread contention; a genuinely leaked lock still fails the bound.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match FileLock::try_acquire(&path) {
                Ok(_) => break,
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        panic!("lock not reacquired after release: {e}");
                    }
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
