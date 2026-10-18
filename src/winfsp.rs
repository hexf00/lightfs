//! WinFsp FUSE-compat adapter (Windows only). Hand-declared bindings: no
//! wrapper crates, `LoadLibraryW("winfsp-x64.dll")` + `GetProcAddress` for
//! the single export we need (`fsp_fuse_main_real`), the rest is callbacks
//! we own. Layouts are pinned by const assertions against the published
//! WinFsp 2.x ABI (verified field-by-field against winfsp v2.0 headers).
//!
//! How it works: winfsp-x64.dll embeds a libfuse-2.8-style FUSE layer that
//! talks to the signed winfsp.sys kernel driver. We hand it a
//! `fuse_operations` table; it constructs the kernel volume parameters,
//! drives the mount, dispatches worker threads, and unmounts on exit (when
//! the process dies, the kernel tears the drive down).
//!
//! Semantics mirror src/fuse.rs: write-intent opens capture the version,
//! writes buffer per handle, flush/fsync commits with optimistic CAS on
//! that version, release commits best-effort. Concurrency contract: all
//! callbacks arrive on WinFsp worker threads; FsApi is Sync.
//!
//! Windows-CRT errno values (NOT the Linux ones): ENOSYS=40, ENOTEMPTY=41.

#![allow(clippy::missing_safety_doc)]

use std::ffi::{CStr, c_char, c_int, c_void};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

use crate::api::FsApi;
use crate::error::Error;
use crate::metadata::Node;
use crate::writeback::PendingWrite;

// ---------------------------------------------------------------------------
// ABI constants (winfsp v2.x / libfuse 2.8 surface)
// ---------------------------------------------------------------------------

/// Windows CRT errno codes returned as `-err` from callbacks.
const ENOENT: c_int = 2;
const EIO: c_int = 5;
const EACCES: c_int = 13;
const EEXIST: c_int = 17;
const ENOTDIR: c_int = 20;
const EISDIR: c_int = 21;
const EINVAL: c_int = 22;
const ENOSYS: c_int = 40;
const ENOTEMPTY: c_int = 41;
/// POSIX ENOSPC (28): WB staging budget exhausted (capacity-exhausted).
const ENOSPC: c_int = 28;
/// POSIX EXDEV (18): rename across filesystems/policy backends.
const EXDEV: c_int = 18;

/// fuse_common.h capability bits negotiated through `init(conn)`.
const FSP_FUSE_CAP_READDIR_PLUS: u32 = 1 << 21;
const FSP_FUSE_CAP_STAT_EX: u32 = 1 << 23;

/// POSIX file type bits (st_mode & 0o170000).
const S_IFDIR: u32 = 0o040000;
const S_IFREG: u32 = 0o100000;
const S_IFLNK: u32 = 0o120000;

// ---------------------------------------------------------------------------
// Struct layouts. Field offsets verified with a compile-time probe against
// the winfsp 2.0 headers on x86_64-windows; const asserts pin them here.
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
struct FuseTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

/// `struct fuse_stat` with STAT_EX negotiated (160 bytes). If the DLL ever
/// declines STAT_EX it fills only the first 128 bytes; the tail we own stays
/// zeroed, so one layout serves both.
#[repr(C)]
struct FuseStat {
    st_dev: u32,               // 0
    st_ino: u64,               // 8
    st_mode: u32,              // 16
    st_nlink: u16,             // 20
    st_uid: u32,               // 24
    st_gid: u32,               // 28
    st_rdev: u32,              // 32
    st_size: i64,              // 40
    st_atim: FuseTimespec,     // 48
    st_mtim: FuseTimespec,     // 64
    st_ctim: FuseTimespec,     // 80
    st_blksize: i32,           // 96
    st_blocks: i64,            // 104
    st_birthtim: FuseTimespec, // 112
    st_flags: u32,             // 128
    st_reserved32: [u32; 3],   // 132
    st_reserved64: [u64; 2],   // 144
}

/// `struct fuse_file_info` (32 bytes). The `flags2` word is the C bitfield
/// unit (direct_io:1, keep_cache:1, flush:1, nonseekable:1, rest padding);
/// we always hand back zeroed bits.
#[repr(C)]
struct FuseFileInfo {
    flags: c_int,     // 0
    fh_old: u32,      // 4
    writepage: c_int, // 8
    flags2: u32,      // 12 (bitfield word)
    fh: u64,          // 16
    lock_owner: u64,  // 24
}

/// `struct fuse_conn_info` (128 bytes).
#[repr(C)]
struct FuseConnInfo {
    proto_major: c_int,  // 0
    proto_minor: c_int,  // 4
    async_read: c_int,   // 8
    max_write: u32,      // 12
    max_readahead: u32,  // 16
    capable: u32,        // 20
    want: u32,           // 24
    reserved: [u32; 25], // 28
}

/// `struct fuse_statvfs` (96 bytes, all u64 on _WIN64).
#[repr(C)]
struct FuseStatvfs {
    f_bsize: u64,
    f_frsize: u64,
    f_blocks: u64,
    f_bfree: u64,
    f_bavail: u64,
    f_files: u64,
    f_ffree: u64,
    f_favail: u64,
    f_fsid: u64,
    f_flag: u64,
    f_namemax: u64,
}

/// `struct fuse_args` (24 bytes).
#[repr(C)]
struct FuseArgs {
    argc: c_int,
    argv: *mut *mut c_char,
    allocated: c_int,
}

/// `struct fsp_fuse_env` (72 bytes). WinFsp calls daemonize and
/// set_signal_handlers unconditionally, so both must be real functions;
/// we no-op them (the process runs foreground, and dying unmounts the
/// drive via the kernel handle).
#[repr(C)]
struct FuseEnv {
    environment: u32, // 'W'
    memalloc: unsafe extern "system" fn(usize) -> *mut c_void,
    memfree: unsafe extern "system" fn(*mut c_void),
    daemonize: unsafe extern "system" fn(c_int) -> c_int,
    set_signal_handlers: unsafe extern "system" fn(*mut c_void) -> c_int,
    conv_to_win_path: *mut c_void, // unused in 'W' env
    winpid_to_pid: *mut c_void,    // unused in 'W' env
    reserved: [*mut c_void; 2],
}

/// readdir filler handed to us by the DLL.
type FuseFillDir = unsafe extern "system" fn(
    buf: *mut c_void,
    name: *const c_char,
    stbuf: *const FuseStat,
    off: i64,
) -> c_int;

/// `struct fuse_operations` (464 bytes, libfuse 2.8 order + WinFsp/OSXFUSE
/// tail). 38 callbacks, the flag bitfield word, 7 more callbacks, 12 tail
/// slots we never use. `None`/null = absent (the DLL checks before calling).
#[repr(C)]
struct FuseOperations {
    getattr: Option<unsafe extern "system" fn(*const c_char, *mut FuseStat) -> c_int>,
    getdir: *mut c_void,
    readlink: Option<unsafe extern "system" fn(*const c_char, *mut c_char, usize) -> c_int>,
    mknod: *mut c_void,
    mkdir: Option<unsafe extern "system" fn(*const c_char, u32) -> c_int>,
    unlink: Option<unsafe extern "system" fn(*const c_char) -> c_int>,
    rmdir: Option<unsafe extern "system" fn(*const c_char) -> c_int>,
    symlink: *mut c_void,
    rename: Option<unsafe extern "system" fn(*const c_char, *const c_char) -> c_int>,
    link: *mut c_void,
    chmod: Option<unsafe extern "system" fn(*const c_char, u32) -> c_int>,
    chown: Option<unsafe extern "system" fn(*const c_char, u32, u32) -> c_int>,
    truncate: Option<unsafe extern "system" fn(*const c_char, i64) -> c_int>,
    utime: *mut c_void,
    open: Option<unsafe extern "system" fn(*const c_char, *mut FuseFileInfo) -> c_int>,
    read: Option<
        unsafe extern "system" fn(
            *const c_char,
            *mut c_char,
            usize,
            i64,
            *mut FuseFileInfo,
        ) -> c_int,
    >,
    write: Option<
        unsafe extern "system" fn(
            *const c_char,
            *const c_char,
            usize,
            i64,
            *mut FuseFileInfo,
        ) -> c_int,
    >,
    statfs: Option<unsafe extern "system" fn(*const c_char, *mut FuseStatvfs) -> c_int>,
    flush: Option<unsafe extern "system" fn(*const c_char, *mut FuseFileInfo) -> c_int>,
    release: Option<unsafe extern "system" fn(*const c_char, *mut FuseFileInfo) -> c_int>,
    fsync: Option<unsafe extern "system" fn(*const c_char, c_int, *mut FuseFileInfo) -> c_int>,
    setxattr: Option<
        unsafe extern "system" fn(
            *const c_char,
            *const c_char,
            *const c_char,
            usize,
            c_int,
        ) -> c_int,
    >,
    getxattr: Option<
        unsafe extern "system" fn(*const c_char, *const c_char, *mut c_char, usize) -> c_int,
    >,
    listxattr: Option<unsafe extern "system" fn(*const c_char, *mut c_char, usize) -> c_int>,
    removexattr: Option<unsafe extern "system" fn(*const c_char, *const c_char) -> c_int>,
    opendir: Option<unsafe extern "system" fn(*const c_char, *mut FuseFileInfo) -> c_int>,
    readdir: Option<
        unsafe extern "system" fn(
            *const c_char,
            *mut c_void,
            FuseFillDir,
            i64,
            *mut FuseFileInfo,
        ) -> c_int,
    >,
    releasedir: Option<unsafe extern "system" fn(*const c_char, *mut FuseFileInfo) -> c_int>,
    fsyncdir: *mut c_void,
    init: Option<unsafe extern "system" fn(*mut FuseConnInfo) -> *mut c_void>,
    destroy: Option<unsafe extern "system" fn(*mut c_void)>,
    access: Option<unsafe extern "system" fn(*const c_char, c_int) -> c_int>,
    create: Option<unsafe extern "system" fn(*const c_char, u32, *mut FuseFileInfo) -> c_int>,
    ftruncate: Option<unsafe extern "system" fn(*const c_char, i64, *mut FuseFileInfo) -> c_int>,
    fgetattr:
        Option<unsafe extern "system" fn(*const c_char, *mut FuseStat, *mut FuseFileInfo) -> c_int>,
    lock: *mut c_void,
    utimens: Option<unsafe extern "system" fn(*const c_char, *const FuseTimespec) -> c_int>,
    bmap: *mut c_void,
    // flag_nullpath_ok / flag_nopath / flag_utime_omit_ok / reserved bitfield
    flag_bits: u32,
    ioctl: *mut c_void,
    poll: *mut c_void,
    write_buf: *mut c_void,
    read_buf: *mut c_void,
    flock: *mut c_void,
    fallocate: *mut c_void,
    getpath: *mut c_void,    // 360
    tail: [*mut c_void; 12], // 368: OSXFUSE-era slots statfs_x .. fsetattr_x
}

const _: () = {
    use std::mem::{offset_of, size_of};
    assert!(size_of::<FuseTimespec>() == 16);
    assert!(size_of::<FuseStat>() == 160);
    assert!(offset_of!(FuseStat, st_ino) == 8);
    assert!(offset_of!(FuseStat, st_mode) == 16);
    assert!(offset_of!(FuseStat, st_nlink) == 20);
    assert!(offset_of!(FuseStat, st_uid) == 24);
    assert!(offset_of!(FuseStat, st_gid) == 28);
    assert!(offset_of!(FuseStat, st_rdev) == 32);
    assert!(offset_of!(FuseStat, st_size) == 40);
    assert!(offset_of!(FuseStat, st_atim) == 48);
    assert!(offset_of!(FuseStat, st_mtim) == 64);
    assert!(offset_of!(FuseStat, st_ctim) == 80);
    assert!(offset_of!(FuseStat, st_blksize) == 96);
    assert!(offset_of!(FuseStat, st_blocks) == 104);
    assert!(offset_of!(FuseStat, st_birthtim) == 112);
    assert!(offset_of!(FuseStat, st_flags) == 128);
    assert!(offset_of!(FuseStat, st_reserved64) == 144);
    assert!(size_of::<FuseFileInfo>() == 32);
    assert!(offset_of!(FuseFileInfo, fh) == 16);
    assert!(offset_of!(FuseFileInfo, lock_owner) == 24);
    assert!(size_of::<FuseConnInfo>() == 128);
    assert!(offset_of!(FuseConnInfo, capable) == 20);
    assert!(offset_of!(FuseConnInfo, want) == 24);
    assert!(size_of::<FuseStatvfs>() == 88);
    assert!(size_of::<FuseArgs>() == 24);
    assert!(size_of::<FuseEnv>() == 72);
    assert!(size_of::<FuseOperations>() == 464);
    assert!(offset_of!(FuseOperations, flag_bits) == 304);
    assert!(offset_of!(FuseOperations, ioctl) == 312);
    assert!(offset_of!(FuseOperations, fallocate) == 352);
    assert!(offset_of!(FuseOperations, tail) == 368);
};

type MainRealFn = unsafe extern "system" fn(
    env: *mut FuseEnv,
    argc: c_int,
    argv: *mut *mut c_char,
    ops: *const FuseOperations,
    opsize: usize,
    data: *mut c_void,
) -> c_int;

// ---------------------------------------------------------------------------
// Adapter state. One process mounts one filesystem (product decision), so a
// process-global handle + a handle->session map is the whole world.
// ---------------------------------------------------------------------------

static FS: Mutex<Option<Arc<dyn FsApi>>> = Mutex::new(None);

fn state() -> Arc<dyn FsApi> {
    FS.lock()
        .unwrap()
        .clone()
        .expect("lightfs-winfsp: state not initialized")
}

static WRITES: LazyLock<Mutex<std::collections::HashMap<u64, PendingWrite>>> =
    LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));
/// S4.25 §三: the WinFsp process's staging budget — write sessions spool
/// under it (bounded like every other process).
static STAGING: LazyLock<std::sync::Arc<crate::staging::StagingBudget>> =
    LazyLock::new(|| {
        match crate::staging::StagingBudget::open(crate::staging::StagingLimits::default()) {
            Ok(b) => b,
            Err(e) => {
                eprintln!(
                    "lightfs: WARNING managed staging directory unavailable ({e}); \
                     write sessions fall back to the raw temp dir (budget still enforced)"
                );
                crate::staging::StagingBudget::headless(crate::staging::StagingLimits::default())
            }
        }
    });
static PENDING: std::sync::LazyLock<crate::writeback::PendingEof> =
    std::sync::LazyLock::new(crate::writeback::PendingEof::new);
static NEXT_FH: AtomicU64 = AtomicU64::new(1);

/// Max over the committed size and the chunk ends of every open session
/// for `path`. Callers must NOT hold WRITES (we take it).
fn visible_eof(path: &str, committed_size: u64) -> u64 {
    let writes = WRITES.lock().unwrap();
    let mut end = committed_size;
    for sess in writes.values() {
        if sess.path == path {
            end = end.max(sess.spool.dirty_end());
        }
    }
    end
}

/// Map a core error onto Windows-CRT errno (mirrors fuse.rs).
fn errno(e: &Error) -> c_int {
    match e {
        Error::NotFound(_) | Error::ParentNotFound(_) | Error::ObjectMissing(_) => ENOENT,
        Error::IdentityGone(_) => ENOENT,
        Error::AlreadyExists(_) => EEXIST,
        Error::KindMismatch {
            expected: "dir", ..
        } => ENOTDIR,
        Error::KindMismatch { .. } => EISDIR,
        Error::DirectoryNotEmpty(_) => ENOTEMPTY,
        Error::Exdev(_) => EXDEV,
        Error::Cycle(_) | Error::InvalidPath(_) | Error::InvalidArgument(_) => EINVAL,
        // Staging budget exhausted: ENOSPC is the POSIX-honest signal (the
        // budget guards disk space); never a silent retry or eviction.
        Error::Capacity(_) => ENOSPC,
        Error::Conflict { .. } => EIO,
        Error::Storage(_) | Error::Db(_) | Error::Io(_) | Error::Config(_) | Error::ObjectCorrupt(_) => EIO,
    }
}

/// WinFsp hands us UTF-8 POSIX paths ("/", "/dir/file").
unsafe fn path_of<'a>(p: *const c_char) -> Result<&'a str, c_int> {
    if p.is_null() {
        return Err(EINVAL);
    }
    // SAFETY: caller guarantees the pointer points to a NUL-terminated C
    // string owned by the kernel request.
    unsafe { CStr::from_ptr(p) }.to_str().map_err(|_| EACCES) // ill-formed UTF-8: reject, not crash
}

fn fill_stat(st: &mut FuseStat, node: &Node) {
    // EXPERIMENTAL: symlink nodes report S_IFLNK. WinFsp FUSE resolves
    // far less of the symlink machinery than Linux VFS does - see the
    // documented platform limitation before relying on links on Windows.
    let mode_type = if node.is_dir() {
        S_IFDIR
    } else if node.is_symlink() {
        S_IFLNK
    } else {
        S_IFREG
    };
    st.st_dev = 0;
    st.st_ino = node.id as u64;
    st.st_mode = mode_type | (node.mode & 0o7777);
    st.st_nlink = if node.is_dir() { 2 } else { 1 };
    // Overridden by -o uid/gid on the winfsp side (mounting user).
    st.st_uid = 0;
    st.st_gid = 0;
    st.st_rdev = 0;
    st.st_size = node.size;
    let ts = FuseTimespec {
        tv_sec: node.mtime,
        tv_nsec: 0,
    };
    st.st_atim = ts;
    st.st_mtim = ts;
    st.st_ctim = ts;
    st.st_birthtim = ts;
    st.st_blksize = 4096;
    st.st_blocks = (node.size as u64).div_ceil(512) as i64;
    st.st_flags = 0;
    st.st_reserved32 = [0; 3];
    st.st_reserved64 = [0; 2];
}

/// Capture the version for a write-intent session (fuse.rs::start_write).
fn start_write(fs: &dyn FsApi, path: &str) -> Result<u64, Error> {
    let node = fs.lookup(path)?;
    if !node.is_file() {
        return Err(Error::KindMismatch {
            path: path.to_string(),
            expected: "file",
            actual: "dir".into(),
        });
    }
    let fh = NEXT_FH.fetch_add(1, Ordering::Relaxed);
    WRITES.lock().unwrap().insert(
        fh,
        PendingWrite::new(path.to_string(), node.id, node.version, &STAGING)?,
    );
    Ok(fh)
}

/// Commit a buffered write (shared semantics with fuse.rs::commit_write;
/// the state machine lives in crate::writeback so both adapters get failure
/// recovery, write-during-commit handling and truncate trimming at once).
/// A session with no buffered data commits nothing (a write-intent open
/// that never wrote must not re-upload the file).
fn commit_write(fh: u64) -> Result<(), Error> {
    // S4.25 §三: the session spool is swapped out WHOLESALE for the
    // commit (writes during the commit land in a FRESH spool); the base
    // streams in around the drained dirty ranges and the merged whole
    // uploads straight out of the spool file — no whole-file heap buffer
    // on the Windows adapter either.
    let (path, expected, mut drained) = {
        let mut writes = WRITES.lock().unwrap();
        let Some(sess) = writes.get_mut(&fh) else {
            eprintln!("lightfs-winfsp: commit_write fh={fh}: NO SESSION (read-only handle)");
            return Ok(()); // read-only handle
        };
        eprintln!(
            "lightfs-winfsp: commit_write fh={fh} path={} dirty={} committed={} orphaned={} expected={}",
            sess.path, sess.spool.is_dirty(), sess.committed, sess.orphaned, sess.expected_version
        );
        if sess.committed {
            return Ok(());
        }
        if sess.orphaned {
            // The name died while the handle was open (unlink, or the
            // rename that overwrote it). K2-B: writes to this handle were
            // already REFUSED at the write boundary, so the batch is empty
            // by construction; resetting is defensive. Success - nothing
            // was ever accepted that could be silently lost. Mirrors
            // fuse.rs::commit_write (which implements full detached
            // semantics instead).
            sess.spool.reset()?;
            sess.committed = true;
            return Ok(());
        }
        if !sess.spool.is_dirty() {
            sess.committed = true;
            return Ok(());
        }
        let fresh = crate::staging::WriteSpool::open(&STAGING)?;
        let drained = std::mem::replace(&mut sess.spool, fresh);
        (sess.path.clone(), sess.expected_version, drained)
    };
    let fs = state();
    // Every error after the swap merges the drained spool back (window
    // writes stay newer — WriteSpool::merge_under) and re-bases the CAS
    // anchor to the version observed at attempt start, so a retry flush
    // can succeed instead of silently dropping the drained data.
    let fail = |e: Error, observed: Option<i64>| -> Error {
        let mut writes = WRITES.lock().unwrap();
        if let Some(sess) = writes.get_mut(&fh) {
            let fresh = match crate::staging::WriteSpool::open(&STAGING) {
                Ok(f) => f,
                Err(e2) => return e2,
            };
            let drained = std::mem::replace(&mut sess.spool, fresh);
            if let Err(e3) = sess.spool.merge_under(drained) {
                return e3;
            }
            if let Some(v) = observed {
                sess.rebase(v);
            }
            eprintln!(
                "lightfs-winfsp: commit_write fh={fh} FAILED ({e}); merged back dirty={} size={} expected={}",
                sess.spool.is_dirty(),
                sess.spool.size(),
                sess.expected_version
            );
            // mirror fuse.rs: the app saw this failure; release must not
            // retry it behind its back (flush_failed gates cb_release)
            sess.flush_failed = true;
        } else {
            eprintln!("lightfs-winfsp: commit_write fh={fh} FAILED ({e}); NO SESSION to merge back");
        }
        e
    };
    let node_now = match fs.lookup(&path) {
        Ok(n) => n,
        Err(e) => {
            drop(drained);
            return Err(fail(e, None));
        }
    };
    let committed_node =
        match fs.commit_write_merged(node_now.id, &path, expected, &mut drained) {
            Ok(n) => n,
            Err(e) => {
                drop(drained);
                return Err(fail(e, Some(node_now.version)));
            }
        };
    drop(drained);
    eprintln!(
        "lightfs-winfsp: commit_write fh={fh} COMMITTED version={} size={}",
        committed_node.version, committed_node.size
    );
    let end = {
        let mut writes = WRITES.lock().unwrap();
        if let Some(sess) = writes.get_mut(&fh) {
            // Fully done only when no new writes arrived during the commit
            // window; a write-during-commit batch is committed by the next
            // flush against the re-based version.
            sess.note_commit_success(committed_node.version);
        }
        // Inline scan: visible_eof would re-lock WRITES (not reentrant)
        // and deadlock the flush that is holding it.
        let mut e = committed_node.size as u64;
        for sess in writes.values() {
            if sess.path == path {
                e = e.max(sess.spool.dirty_end());
            }
        }
        e
    };
    PENDING.rebase(&path, end);
    Ok(())
}

// ---------------------------------------------------------------------------
// Callbacks (unsafe: every one is called from WinFsp worker threads with
// pointers into the kernel request; we validate before use).
// ---------------------------------------------------------------------------

unsafe extern "system" fn cb_getattr(path: *const c_char, st: *mut FuseStat) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        match state().lookup(path) {
            Ok(node) => {
                *st = std::mem::zeroed();
                fill_stat(&mut *st, &node);
                let in_flight = PENDING.get(path);
                if in_flight > (*st).st_size as u64 {
                    // Write-back model: report the in-flight EOF exactly
                    // like the Linux page cache reports dirty size, so
                    // WinFsp's WriteToEndOfFile handler (which derives the
                    // append offset from our getattr) does not fold two
                    // appends onto the same offset between flushes.
                    (*st).st_size = in_flight as i64;
                }
                0
            }
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_fgetattr(
    path: *const c_char,
    st: *mut FuseStat,
    _fi: *mut FuseFileInfo,
) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe { cb_getattr(path, st) }
}

unsafe extern "system" fn cb_open(path: *const c_char, fi: *mut FuseFileInfo) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        let write_intent = (*fi).flags & 3 != 0; // O_WRONLY | O_RDWR
        eprintln!("lightfs-winfsp: cb_open path={} write_intent={} flags={:#x}", path, write_intent, (*fi).flags);
        if write_intent {
            match start_write(state().as_ref(), path) {
                Ok(fh) => {
                    (*fi).fh = fh;
                    0
                }
                Err(e) => -errno(&e),
            }
        } else {
            (*fi).fh = 0; // read-only: no session state
            0
        }
    }
}

unsafe extern "system" fn cb_create(
    path: *const c_char,
    mode: u32,
    fi: *mut FuseFileInfo,
) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        let fs = state();
        let mode = if mode == 0 { 0o644 } else { mode & 0o777 };
        match fs
            .create(path, mode)
            .and_then(|_| start_write(fs.as_ref(), path))
        {
            Ok(fh) => {
                PENDING.drop_path(path);
                (*fi).fh = fh;
                0
            }
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_read(
    path: *const c_char,
    buf: *mut c_char,
    size: usize,
    offset: i64,
    _fi: *mut FuseFileInfo,
) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        if buf.is_null() || offset < 0 {
            return -EINVAL;
        }
        if size == 0 {
            return 0;
        }
        // K2-B: this adapter has no detached-inode store (no stable inode
        // id to hold a dead name's bytes). A handle whose name was unlinked
        // or renamed over must NOT resolve through the recycled path - that
        // would alias the NEW file's bytes. Refuse at the boundary.
        {
            let fh = (*_fi).fh;
            let writes = WRITES.lock().unwrap();
            if writes.get(&fh).is_some_and(|sess| sess.orphaned) {
                return -ENOENT;
            }
        }
        match state().read(path, offset as u64, size as u64) {
            Ok(mut data) => {
                // Read-your-writes: serve session chunks still in flight
                // (same rationale as the Linux adapter; see writeback
                // ::ChunkMap::overlay_range).
                let in_flight = PENDING.get(path);
                let want = if in_flight > offset as u64 {
                    ((in_flight - offset as u64) as usize).min(size)
                } else {
                    0
                };
                if want > data.len() {
                    data.resize(want, 0);
                }
                {
                    let writes = WRITES.lock().unwrap();
                    for sess in writes.values() {
                        if sess.path == path {
                            // spool overlay failure is an IO error at the
                            // FFI boundary (a dirty range read cannot
                            // happen — the range exists if marked dirty)
                            if sess.spool.overlay_range(&mut data, offset as u64).is_err() {
                                return -EIO;
                            }
                        }
                    }
                }
                std::ptr::copy_nonoverlapping(data.as_ptr() as *const c_char, buf, data.len());
                data.len() as c_int
            }
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_write(
    path: *const c_char,
    buf: *const c_char,
    size: usize,
    offset: i64,
    fi: *mut FuseFileInfo,
) -> c_int {
    unsafe {
        eprintln!("lightfs-winfsp: cb_write fh={} off={} size={}", (*fi).fh, offset, size);
    }
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        if buf.is_null() || size == 0 || offset < 0 {
            return -EINVAL;
        }
        let fh = (*fi).fh;
        let data = std::slice::from_raw_parts(buf as *const u8, size).to_vec();
        let mut writes = WRITES.lock().unwrap();
        let Some(sess) = writes.get_mut(&fh) else {
            return -EIO;
        };
        if sess.orphaned {
            // K2-B: no detached-inode store on this adapter (no stable
            // inode id), so a write it cannot honestly persist is REFUSED
            // here, at the write boundary - never accepted and silently
            // dropped later. (The FUSE adapter implements full detached
            // semantics instead; see fuse.rs.)
            return -EIO;
        }
        if sess.committed {
            // New writes after a flush (Windows may flush and keep the handle
            // open): re-arm against the current committed version.
            match state().lookup(path) {
                Ok(node) => sess.expected_version = node.version,
                Err(e) => return -errno(&e),
            }
            sess.committed = false;
        }
        if sess.spool.write(offset as u64, &data).is_err() {
            return -ENOSPC;
        }
        PENDING.advance(path, offset as u64 + size as u64);
        size as c_int
    }
}

/// Truncate shared by truncate/ftruncate: read-keep-or-zero-fill, commit
/// with CAS on the version just observed, then re-base open sessions and
/// drop their pending chunks beyond the new EOF (kernel discards those
/// dirty pages and will not resend them).
fn truncate_impl(path: Result<&str, c_int>, new_size: i64) -> c_int {
    let Ok(path) = path else { return -EINVAL };
    if new_size < 0 {
        return -EINVAL;
    }
    eprintln!("lightfs-winfsp: truncate {path} -> {new_size}");
    let fs = state();
    let node_now = match fs.lookup(path) {
        Ok(n) => n,
        Err(e) => {
            eprintln!("lightfs-winfsp: truncate lookup failed for {path}: {e}");
            return -errno(&e);
        }
    };
    let cur_len = (node_now.size as u64).min(new_size as u64);
    let commit = fs.read(path, 0, cur_len).and_then(|mut content| {
        if new_size as u64 > content.len() as u64 {
            content.resize(new_size as usize, 0);
        }
        let mut ws = fs.open_write(path, Some(node_now.version))?;
        ws.write(0, &content)?;
        ws.commit()
    });
    match commit {
        Ok(new_node) => {
            {
                let mut writes = WRITES.lock().unwrap();
                for st in writes.values_mut() {
                    if st.path == path {
                        st.expected_version = new_node.version;
                        // Drop pending writes beyond the new EOF, trim
                        // dirty ranges straddling it to their pre-EOF
                        // prefix (the kernel discards those dirty pages
                        // and will not resend them; a straddling range
                        // left whole would re-extend the file on the
                        // next flush).
                        let _ = st.spool.truncate(new_size as u64);
                    }
                }
            }
            PENDING.drop_path(path);
            0
        }
        Err(e) => {
            eprintln!("lightfs-winfsp: truncate commit failed for {path}: {e}");
            -errno(&e)
        }
    }
}

unsafe extern "system" fn cb_truncate(path: *const c_char, new_size: i64) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe { truncate_impl(path_of(path), new_size) }
}

unsafe extern "system" fn cb_ftruncate(
    path: *const c_char,
    new_size: i64,
    _fi: *mut FuseFileInfo,
) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe { truncate_impl(path_of(path), new_size) }
}

unsafe extern "system" fn cb_flush(path: *const c_char, fi: *mut FuseFileInfo) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            eprintln!("lightfs-winfsp: cb_flush path_of REJECTED");
            return -EINVAL;
        };
        eprintln!("lightfs-winfsp: cb_flush path={} fh={}", path, (*fi).fh);
        match commit_write((*fi).fh) {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("lightfs-winfsp: flush FAILED for {path}: {e}");
                -errno(&e)
            }
        }
    }
}

unsafe extern "system" fn cb_release(path: *const c_char, fi: *mut FuseFileInfo) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        let fh = (*fi).fh;
        // Same gate as fuse.rs release: a session whose flush already
        // reported an error to the application must not flip the outcome
        // through a close-time retry (the app was told it did NOT commit).
        let already_failed = WRITES
            .lock()
            .unwrap()
            .get(&fh)
            .map(|st| st.flush_failed)
            .unwrap_or(false);
        if already_failed {
            eprintln!(
                "lightfs-winfsp: WARNING release: fh={fh} ({path}) had a FAILED flush; close-time retry REFUSED - data not persisted"
            );
        } else if let Err(e) = commit_write(fh) {
            // Release's result is ignored by the DLL; flush/fsync surface errors.
            eprintln!(
                "lightfs-winfsp: WARNING release: final commit FAILED for {path} (fh={fh}): {e} - data not persisted"
            );
        }
        WRITES.lock().unwrap().remove(&fh);
        let end = visible_eof(path, 0);
        PENDING.recompute(path, end);
        0
    }
}

unsafe extern "system" fn cb_fsync(
    path: *const c_char,
    _datasync: c_int,
    fi: *mut FuseFileInfo,
) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(p) = path_of(path) else {
            eprintln!("lightfs-winfsp: cb_fsync path_of REJECTED");
            return -EINVAL;
        };
        eprintln!("lightfs-winfsp: cb_fsync path={} fh={}", p, (*fi).fh);
        match commit_write((*fi).fh) {
            Ok(()) => 0,
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_mkdir(path: *const c_char, mode: u32) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        let mode = if mode == 0 { 0o755 } else { mode & 0o777 };
        match state().mkdir(path, mode) {
            Ok(_) => 0,
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_unlink(path: *const c_char) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        match state().unlink(path) {
            Ok(()) => {
                // A handle opened before the unlink must not resurrect the
                // name at commit time: orphan its session (batch drops,
                // close succeeds - POSIX unlink semantics for open fds).
                // Mirrors fuse.rs::unlink.
                {
                    let mut writes = WRITES.lock().unwrap();
                    for sess in writes.values_mut() {
                        if sess.path == path {
                            sess.orphaned = true;
                            sess.path.clear();
                        }
                    }
                }
                PENDING.drop_path(path);
                0
            }
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_rmdir(path: *const c_char) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        match state().rmdir(path) {
            Ok(()) => 0,
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_rename(old: *const c_char, new: *const c_char) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let (Ok(old), Ok(new)) = (path_of(old), path_of(new)) else {
            return -EINVAL;
        };
        match state().rename(old, new) {
            Ok(()) => {
                // Open write handles ride along: repoint their paths so the
                // commit looks the file up under its CURRENT name; a handle
                // on an overwritten destination just lost its inode's last
                // name - orphan it. Mirrors fuse.rs::rename.
                if new != old {
                    let mut writes = WRITES.lock().unwrap();
                    for sess in writes.values_mut() {
                        if sess.path == new {
                            sess.orphaned = true;
                            sess.path.clear();
                        }
                    }
                    let prefix = format!("{old}/");
                    for sess in writes.values_mut() {
                        if sess.path == old {
                            sess.path = new.to_string();
                        } else if sess.path.starts_with(&prefix) {
                            sess.path = format!("{new}{}", &sess.path[old.len()..]);
                        }
                    }
                }
                PENDING.drop_path(old);
                PENDING.drop_path(new);
                0
            }
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_chmod(path: *const c_char, mode: u32) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        match state().setattr(path, Some(mode & 0o7777), None) {
            Ok(_) => 0,
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_chown(_path: *const c_char, _uid: u32, _gid: u32) -> c_int {
    // Ownership is a mount-level concept here (-o uid=-1): per-node chown
    // has no storage representation, so report success.
    0
}

/// Windows attribute changes arrive as utimens with concrete times for
/// both fields; we persist mtime and ignore atime.
unsafe extern "system" fn cb_utimens(path: *const c_char, tv: *const FuseTimespec) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        if tv.is_null() {
            return -EINVAL;
        }
        let mtime = (*tv.add(1)).tv_sec;
        match state().setattr(path, None, Some(mtime)) {
            Ok(_) => 0,
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_statfs(_path: *const c_char, st: *mut FuseStatvfs) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        *st = std::mem::zeroed();
        // No quota concept: report a large fixed pool (1 PiB in 4 KiB blocks).
        let blocks = (1u64 << 50) / 4096;
        (*st).f_bsize = 4096;
        (*st).f_frsize = 4096;
        (*st).f_blocks = blocks;
        (*st).f_bfree = blocks;
        (*st).f_bavail = blocks;
        (*st).f_files = u32::MAX as u64;
        (*st).f_ffree = u32::MAX as u64;
        (*st).f_favail = u32::MAX as u64;
        (*st).f_fsid = 0;
        (*st).f_flag = 0;
        (*st).f_namemax = 255;
        0
    }
}

unsafe extern "system" fn cb_opendir(path: *const c_char, _fi: *mut FuseFileInfo) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        match state().lookup(path) {
            Ok(node) if node.is_dir() => 0,
            Ok(_) => -ENOTDIR,
            Err(e) => -errno(&e),
        }
    }
}

unsafe extern "system" fn cb_readdir(
    path: *const c_char,
    buf: *mut c_void,
    filler: FuseFillDir,
    _offset: i64,
    _fi: *mut FuseFileInfo,
) -> c_int {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        let Ok(path) = path_of(path) else {
            return -EINVAL;
        };
        let children = match state().readdir(path) {
            Ok(c) => c,
            Err(e) => return -errno(&e),
        };
        for child in &children {
            let Ok(name) = std::ffi::CString::new(child.name.as_str()) else {
                continue; // interior NUL cannot appear in a Windows name
            };
            let mut st: FuseStat = std::mem::zeroed();
            fill_stat(&mut st, child); // READDIR_PLUS: sizes/dates in one pass
            if filler(buf, name.as_ptr(), &st, 0) != 0 {
                break;
            }
        }
        0
    }
}

unsafe extern "system" fn cb_releasedir(_path: *const c_char, _fi: *mut FuseFileInfo) -> c_int {
    0
}

unsafe extern "system" fn cb_access(_path: *const c_char, _mask: c_int) -> c_int {
    // Enforcement happens on the winfsp side from mode + owner; we do not
    // second-guess it here.
    0
}

unsafe extern "system" fn cb_init(conn: *mut FuseConnInfo) -> *mut c_void {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        if !conn.is_null() {
            // Negotiate extended stat layout (explicit 160-byte fuse_stat) and
            // stat-carrying readdir.
            (*conn).want |= FSP_FUSE_CAP_STAT_EX | FSP_FUSE_CAP_READDIR_PLUS;
        }
        std::ptr::null_mut()
    }
}

unsafe extern "system" fn cb_destroy(_data: *mut c_void) {}

unsafe extern "system" fn cb_readlink(
    _path: *const c_char,
    _buf: *mut c_char,
    _size: usize,
) -> c_int {
    -ENOSYS
}

unsafe extern "system" fn cb_setxattr(
    _path: *const c_char,
    _name: *const c_char,
    _value: *const c_char,
    _size: usize,
    _flags: c_int,
) -> c_int {
    -ENOSYS
}

unsafe extern "system" fn cb_getxattr(
    _path: *const c_char,
    _name: *const c_char,
    _value: *mut c_char,
    _size: usize,
) -> c_int {
    -ENOSYS
}

unsafe extern "system" fn cb_listxattr(
    _path: *const c_char,
    _buf: *mut c_char,
    _size: usize,
) -> c_int {
    -ENOSYS
}

unsafe extern "system" fn cb_removexattr(_path: *const c_char, _name: *const c_char) -> c_int {
    -ENOSYS
}

// ---------------------------------------------------------------------------
// env plumbing: the DLL calls these unconditionally; alloc/free must form a
// self-consistent pair across the boundary (size header trick, 16-byte
// alignment).
// ---------------------------------------------------------------------------

unsafe extern "system" fn env_memalloc(size: usize) -> *mut c_void {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        if size == 0 {
            return std::ptr::null_mut();
        }
        let Some(total) = size.checked_add(16) else {
            return std::ptr::null_mut();
        };
        let Ok(layout) = std::alloc::Layout::from_size_align(total, 16) else {
            return std::ptr::null_mut();
        };
        let base = std::alloc::alloc(layout) as *mut u64;
        if base.is_null() {
            return std::ptr::null_mut();
        }
        *base = size as u64;
        base.add(1) as *mut c_void
    }
}

unsafe extern "system" fn env_memfree(p: *mut c_void) {
    // SAFETY: inputs validated above; raw FFI surface isolated here.
    unsafe {
        if p.is_null() {
            return;
        }
        // SAFETY: every pointer handed out by env_memalloc has this 16-byte
        // size header in front of it.
        let base = (p as *mut u64).sub(1);
        let total = (*base as usize) + 16;
        if let Ok(layout) = std::alloc::Layout::from_size_align(total, 16) {
            std::alloc::dealloc(base as *mut u8, layout);
        }
    }
}

unsafe extern "system" fn env_daemonize(_foreground: c_int) -> c_int {
    0 // foreground process; Task Scheduler handles restarts
}

unsafe extern "system" fn env_set_signal_handlers(_f: *mut c_void) -> c_int {
    0 // process death (Ctrl+C, task kill) unmounts the drive via the kernel
}

// ---------------------------------------------------------------------------
// Mount entry
// ---------------------------------------------------------------------------

/// Normalize a Windows mount target: a bare letter, "Z:", or "Z:\" all
/// become "Z:". Drive letters only for v1 (directory mounts are a WinFsp
/// feature we do not exercise yet).
fn normalize_mountpoint(mountpoint: &str) -> Result<String, Error> {
    let t = mountpoint.trim().trim_end_matches(['\\', '/']);
    let bytes = t.as_bytes();
    if bytes.len() == 1 && bytes[0].is_ascii_alphabetic() {
        return Ok(format!("{}:", bytes[0].to_ascii_uppercase() as char));
    }
    if bytes.len() == 2 && bytes[1] == b':' && bytes[0].is_ascii_alphabetic() {
        return Ok(format!("{}:", bytes[0].to_ascii_uppercase() as char));
    }
    Err(Error::Config(format!(
        "invalid Windows mountpoint {mountpoint:?}: pass a drive letter such as Z: \
         (WinFsp mounts the filesystem as a drive)"
    )))
}

fn ops_table() -> FuseOperations {
    FuseOperations {
        getattr: Some(cb_getattr),
        getdir: std::ptr::null_mut(),
        readlink: Some(cb_readlink),
        mknod: std::ptr::null_mut(),
        mkdir: Some(cb_mkdir),
        unlink: Some(cb_unlink),
        rmdir: Some(cb_rmdir),
        symlink: std::ptr::null_mut(),
        rename: Some(cb_rename),
        link: std::ptr::null_mut(),
        chmod: Some(cb_chmod),
        chown: Some(cb_chown),
        truncate: Some(cb_truncate),
        utime: std::ptr::null_mut(),
        open: Some(cb_open),
        read: Some(cb_read),
        write: Some(cb_write),
        statfs: Some(cb_statfs),
        flush: Some(cb_flush),
        release: Some(cb_release),
        fsync: Some(cb_fsync),
        setxattr: Some(cb_setxattr),
        getxattr: Some(cb_getxattr),
        listxattr: Some(cb_listxattr),
        removexattr: Some(cb_removexattr),
        opendir: Some(cb_opendir),
        readdir: Some(cb_readdir),
        releasedir: Some(cb_releasedir),
        fsyncdir: std::ptr::null_mut(),
        init: Some(cb_init),
        destroy: Some(cb_destroy),
        access: Some(cb_access),
        create: Some(cb_create),
        ftruncate: Some(cb_ftruncate),
        fgetattr: Some(cb_fgetattr),
        lock: std::ptr::null_mut(),
        utimens: Some(cb_utimens),
        bmap: std::ptr::null_mut(),
        flag_bits: 0,
        ioctl: std::ptr::null_mut(),
        poll: std::ptr::null_mut(),
        write_buf: std::ptr::null_mut(),
        read_buf: std::ptr::null_mut(),
        flock: std::ptr::null_mut(),
        fallocate: std::ptr::null_mut(),
        getpath: std::ptr::null_mut(),
        tail: [std::ptr::null_mut(); 12],
    }
}

fn wide(s: &str) -> Vec<u16> {
    s.encode_utf16().chain(std::iter::once(0)).collect()
}

/// Mount entry point: `lightfs mount [--config PATH] DRIVE_LETTER`. Blocks
/// until the process is killed; killing it unmounts the drive.
pub fn mount(fs: Arc<dyn FsApi>, mountpoint: &str) -> crate::error::Result<()> {
    let target = normalize_mountpoint(mountpoint)?;

    // SAFETY: FFI into winfsp-x64.dll; LoadLibraryW/GetProcAddress are
    // plain Win32 calls. The DLL either exists (WinFsp installed) or we
    // fail with an actionable message.
    let dll_name = wide("winfsp-x64.dll");
    let dll = unsafe { LoadLibraryW(dll_name.as_ptr()) };
    if dll.is_null() {
        return Err(Error::Config(
            "WinFsp not found: install WinFsp (https://winfsp.dev/rel) \
             so that winfsp-x64.dll is available, then retry"
                .to_string(),
        ));
    }
    let proc_addr = unsafe { GetProcAddress(dll, c"fsp_fuse_main_real".as_ptr()) };
    if proc_addr.is_null() {
        return Err(Error::Config(
            "winfsp-x64.dll loaded but fsp_fuse_main_real is missing \
             (unsupported WinFsp version?)"
                .to_string(),
        ));
    }
    // SAFETY: the address comes from the DLL by name; the signature is the
    // documented fsp_fuse_main_real(env, argc, argv, ops, opsize, data).
    let main_real: MainRealFn = unsafe { std::mem::transmute(proc_addr) };

    // Mount options: owner/group = the mounting user (uid=-1 resolves via
    // the process token), create modes land at 0644/0755, and
    // FileInfoTimeout=0 disables the kernel's metadata cache - our metadata
    // is a SQLite lookup away (~0.1ms) and a 1s stale-size window breaks
    // close-open consistency (app writes, closes, immediately reads back
    // and sees the pre-write size through the cached attributes).
    let opts = c"uid=-1,gid=-1,create_file_umask=133,create_dir_umask=022,FileInfoTimeout=0"
        .to_bytes_with_nul();
    let prog = c"lightfs".to_bytes_with_nul();
    let mnt = std::ffi::CString::new(target)
        .map_err(|e| Error::Config(format!("invalid mountpoint: {e}")))?;
    let mut argv_storage: [*mut c_char; 4] = [
        prog.as_ptr() as *mut c_char,
        c"-o".as_ptr() as *mut c_char,
        opts.as_ptr() as *mut c_char,
        mnt.as_ptr() as *mut c_char,
    ];

    // Publish state before the loop starts dispatching worker threads.
    *FS.lock().unwrap() = Some(fs);

    let mut env = FuseEnv {
        environment: b'W' as u32,
        memalloc: env_memalloc,
        memfree: env_memfree,
        daemonize: env_daemonize,
        set_signal_handlers: env_set_signal_handlers,
        conv_to_win_path: std::ptr::null_mut(),
        winpid_to_pid: std::ptr::null_mut(),
        reserved: [std::ptr::null_mut(); 2],
    };
    let args = FuseArgs {
        argc: 4,
        argv: argv_storage.as_mut_ptr(),
        allocated: 0,
    };
    let ops = ops_table();

    // Blocks until unmount (process kill). 0 success, 1 error (main-style).
    let rc = unsafe {
        main_real(
            &mut env,
            args.argc,
            args.argv,
            &ops,
            std::mem::size_of::<FuseOperations>(),
            std::ptr::null_mut(),
        )
    };
    *FS.lock().unwrap() = None;
    if rc != 0 {
        return Err(Error::Config(format!(
            "WinFsp mount of {mnt:?} failed; check that the drive letter is free \
             and the WinFsp service is installed"
        )));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Raw Win32 imports (no win32 crate - hand-declared, matching the project's
// no-heavy-dependency rule).
// ---------------------------------------------------------------------------

#[link(name = "kernel32")]
unsafe extern "system" {
    fn LoadLibraryW(filename: *const u16) -> *mut c_void;
    fn GetProcAddress(module: *mut c_void, name: *const c_char) -> *mut c_void;
}
