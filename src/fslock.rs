//! Cross-platform exclusive file lock: advisory, kernel-owned, released by
//! the OS the moment the holding process dies (SIGKILL included) — no stale
//! lock files, no retry protocol needed. Unix uses `flock(LOCK_EX|LOCK_NB)`
//! (per open file description, so two descriptors in ONE process also
//! conflict); Windows uses `LockFileEx(LOCKFILE_EXCLUSIVE_LOCK|
//! LOCKFILE_FAIL_IMMEDIATELY)` on a 1-byte range (per handle, same rule).
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
        let _ = std::fs::write(path, format!("pid {}\n", std::process::id()));
        Ok(Self { _file: file })
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
        #[repr(C)]
        struct Overlapped {
            internal: usize,
            internal_high: usize,
            offset: u32,
            offset_high: u32,
            event: *mut core::ffi::c_void,
        }
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
        }
        const LOCKFILE_FAIL_IMMEDIATELY: u32 = 0x1;
        const LOCKFILE_EXCLUSIVE_LOCK: u32 = 0x2;
        let mut ov = Overlapped {
            internal: 0,
            internal_high: 0,
            offset: 0,
            offset_high: 0,
            event: core::ptr::null_mut(),
        };
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
        FileLock::try_acquire(&path).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
