//! lightfs-accept: cross-platform mount acceptance suite. ONE set of
//! byte-exact assertions shared by the Linux FUSE gate and the Windows
//! WinFsp (experimental) acceptance - replacing per-platform shell/PS
//! harnesses that inevitably drift apart.
//!
//! The suite owns the server and mount lifecycles (spawn, health-wait,
//! kill, restart): failure injection (server outage, mount crash, server
//! crash) is deterministic product-path behavior, not scripting.
//!
//! Sections (every assertion reads back and compares BYTES):
//!   A  state-machine boundaries through the mount (offsets, overlap,
//!      truncate classes incl. straddle, multi-handle CAS conflict+retry,
//!      dup-close first-write preservation)
//!   B  flush paths: strong coherence (sync_all must surface commit
//!      errors), committed-session re-arm, server-outage flush failure +
//!      retry, plain-close eventual visibility (measured, NOT strong)
//!   C  crash/persistence: dirty-data mount kill, server crash + remount,
//!      rename/delete/enum, shell-client cross-check, offline fsck,
//!      unmount-on-death
//!
//! Usage (caller provides MinIO and a server-mode config):
//!   lightfs-accept --lightfs ./lightfs --config cfg.toml --mount-target /mnt/x
//!   lightfs-accept --lightfs lightfs.exe --config cfg.toml --mount-target Q:
//! Optional:
//!   --shell ./lightfs-shell   (client cross-check, section C)
//!   --section a,b,c           (default: all)
//!   --selftest                (no mounts: run the local-only assertions
//!                              against a temp dir - harness sanity check)

use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

// ---------------------------------------------------------------- helpers

struct Ctx {
    lightfs: PathBuf,
    shell: Option<PathBuf>,
    config: PathBuf,
    mount_target: String,
    server_url: String,
    server: Option<Child>,
    mount: Option<Child>,
    section_list: Vec<String>,
    #[allow(dead_code)] // printed in the env header only on the mount path
    workdir: PathBuf,
    /// false in --selftest mode (assertions against a plain local dir;
    /// adapter-only scenarios like CAS conflict are skipped there).
    adapter: bool,
}

impl Drop for Ctx {
    fn drop(&mut self) {
        if let Some(c) = self.mount.as_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
        if let Some(c) = self.server.as_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

fn sha256_file(p: &Path) -> String {
    let data = std::fs::read(p).unwrap_or_else(|e| panic!("read {p:?}: {e}"));
    hex(&data)
}

fn hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    format!("{:x}", h.finalize())
}

/// Deterministic pattern (identical construction on every platform).
fn pattern(n: usize, seed: u8) -> Vec<u8> {
    (0..n)
        .map(|i| ((i as u32 * 31 + seed as u32) % 251) as u8)
        .collect()
}

/// Byte-exact assertion: length, then content; diagnostics include the
/// first differing offset/byte and both SHA-256s.
fn assert_bytes(what: &str, path: &Path, want: &[u8]) {
    let got = std::fs::read(path).unwrap_or_else(|e| panic!("{what}: read {path:?}: {e}"));
    if got.len() != want.len() {
        panic!(
            "{what}: {path:?} length {}, want {} (sha got {} want {})",
            got.len(),
            want.len(),
            hex(&got),
            hex(want)
        );
    }
    for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        if g != w {
            panic!(
                "{what}: {path:?} first diff at offset {i}: got {g} want {w} (sha got {} want {})",
                hex(&got),
                hex(want)
            );
        }
    }
    println!("  OK  {what} ({} bytes byte-exact)", want.len());
}

fn wait_health(url: &str, tries: usize, what: &str) {
    let agent = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .build()
        .new_agent();
    for _ in 0..tries {
        if let Ok(r) = agent.get(url).call() {
            if r.status().as_u16() == 200 {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    panic!("{what} did not become healthy ({url})");
}

fn open_create(path: &Path) -> std::fs::File {
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .unwrap_or_else(|e| panic!("open {path:?}: {e}"))
}

fn write_at(f: &mut std::fs::File, off: u64, data: &[u8]) {
    f.seek(SeekFrom::Start(off)).unwrap();
    f.write_all(data).unwrap();
}

/// The strong-coherence primitive on both platforms: fsync on Linux (the
/// adapter commits on fsync), FlushFileBuffers via sync_all on Windows. A
/// failed commit surfaces HERE - never swallowed.
fn flush_strong(f: &mut std::fs::File, what: &str) {
    f.sync_all()
        .unwrap_or_else(|e| panic!("{what}: strong flush failed (must be observable): {e}"));
}

/// Strong-commit a whole file from scratch. A plain close on Windows defers
/// the commit to WinFsp's asynchronous Close callback (the B4 window,
/// ~235ms), so any test that writes and then reads back through a NEW
/// handle must take the FlushFileBuffers path first or it races that window
/// (observed on real WinFsp: reopen + sync_all after a plain-close seed
/// fails with ERROR_IO_DEVICE 1117 - the deferred commit conflicts with the
/// version the new handle captured).
#[cfg_attr(not(windows), allow(dead_code))]
fn write_strong(path: &Path, data: &[u8], what: &str) {
    let mut f = open_create(path);
    f.write_all(data)
        .unwrap_or_else(|e| panic!("{what}: write failed: {e}"));
    flush_strong(&mut f, what);
}

impl Ctx {
    fn child_stderr(&self, name: &str) -> Stdio {
        let path = self.workdir.join(format!("{name}.err"));
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .expect("open child stderr log");
        Stdio::from(f)
    }

    /// True while the kernel still lists a mount at `target` (Linux).
    /// Reads /proc/self/mounts - unlike mountpoint(1) this does not lie
    /// about dead mounts.
    fn unix_mounted(target: &str) -> bool {
        match std::fs::read_to_string("/proc/self/mounts") {
            Ok(mtab) => mtab
                .lines()
                .any(|l| l.split_whitespace().nth(1) == Some(target)),
            Err(_) => false,
        }
    }

    fn start_server(&mut self) {
        let mut cmd = Command::new(&self.lightfs);
        cmd.args(["server", "--config"]).arg(&self.config);
        cmd.stdout(Stdio::null())
            .stderr(self.child_stderr("server"));
        self.server = Some(cmd.spawn().expect("spawn lightfs server"));
        wait_health(
            &format!("{}/health", self.server_url),
            100,
            "lightfs server",
        );
    }

    fn kill_server(&mut self) {
        if let Some(c) = self.server.as_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.server = None;
        std::thread::sleep(Duration::from_millis(500));
    }

    /// Sections C4/C5 end with the mount AND the server torn down by design
    /// (process death = unmount; fsck wants no SQLite contention). Any
    /// section that runs AFTER c must re-establish both. Idempotent: a w-only
    /// run arrives here with a fresh live pair and does nothing.
    #[cfg_attr(not(windows), allow(dead_code))]
    fn ensure_running(&mut self) {
        if let Some(c) = self.server.as_mut() {
            if c.try_wait().map(|s| s.is_some()).unwrap_or(false) {
                self.server = None;
            }
        }
        if self.server.is_none() {
            self.start_server();
        }
        let appeared = if cfg!(unix) {
            Self::unix_mounted(&self.mount_target)
        } else {
            Path::new(&self.mount_target).exists()
        };
        if !appeared {
            self.start_mount();
        }
    }

    /// Mount authenticity check: on Linux the mountpoint directory exists
    /// BEFORE the mount, so "path exists" proves nothing - verify with
    /// mountpoint(1); on Windows a drive letter only exists once mounted.
    fn start_mount(&mut self) {
        let root = PathBuf::from(&self.mount_target);
        // A mount still listed for the target is a stale (possibly DEAD)
        // leftover: mounting over it stacks and fails weirdly on old
        // kernels (seen on CentOS 7 / 3.10: fresh conn dies with
        // ENOTCONN, leaving another corpse). Detect via /proc/mounts -
        // mountpoint(1) LIES on dead mounts (exit 0 with "Transport
        // endpoint is not connected").
        if cfg!(unix) && Self::unix_mounted(&self.mount_target) {
            panic!(
                "mount target {} already has a mount listed - a stale mount was not cleaned up",
                self.mount_target
            );
        }
        let mut cmd = Command::new(&self.lightfs);
        cmd.args(["mount", "--config"])
            .arg(&self.config)
            .arg(&self.mount_target);
        // Short dir-snapshot TTL (fuse-only, default 30): A12 exercises the
        // expired-snapshot path in seconds. No other test leaves a walk
        // open anywhere near this long, so nothing else can hit it.
        cmd.env("LIGHTFS_FUSE_DIR_SNAPSHOT_SECS", "2");
        cmd.stdout(Stdio::null()).stderr(self.child_stderr("mount"));
        self.mount = Some(cmd.spawn().expect("spawn lightfs mount"));

        for _ in 0..120 {
            let appeared = if cfg!(unix) {
                Self::unix_mounted(&self.mount_target)
            } else {
                root.exists()
            };
            if appeared {
                return;
            }
            if let Some(c) = self.mount.as_mut() {
                if c.try_wait().map(|s| s.is_some()).unwrap_or(false) {
                    panic!(
                        "mount process exited before {} was mounted",
                        self.mount_target
                    );
                }
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        panic!("mount target {} never appeared", self.mount_target);
    }

    /// Kill the mount daemon. Does NOT unmount: an unmount while fds may
    /// still pin the fs degrades to lazy detachment (see unmount_target).
    fn kill_mount_only(&mut self) {
        if let Some(c) = self.mount.as_mut() {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.mount = None;
    }

    /// Kill daemon AND unmount, for paths that hold no open files on the
    /// mount (C2 server-crash restart, suite teardown).
    fn kill_mount(&mut self) {
        self.kill_mount_only();
        self.unmount_target();
    }

    /// Detach the mount at mount_target and WAIT until it is really gone.
    /// Linux: fusermount(1) is the setuid helper that MOUNTED the fs, so
    /// it may unmount it; umount(8) often is NOT permitted for a plain
    /// user (seen on CentOS 7: "Operation not permitted" even with -l).
    /// -z is fusermount's lazy variant - only reachable when fds still pin
    /// the fs; lazy detachment leaves the entry listed until the last
    /// reference dies, which is why callers should close theirs first.
    /// Windows: the kernel unmounts a drive when its process dies; wait
    /// for the letter to vanish.
    fn unmount_target(&self) {
        if cfg!(unix) {
            for um in [
                ["fusermount3", "-u"],
                ["fusermount", "-u"],
                ["fusermount3", "-z"],
                ["fusermount", "-z"],
                ["umount", "-l"],
            ] {
                let _ = Command::new(um[0])
                    .args([um[1], &self.mount_target])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status();
            }
            // Truth source is /proc/mounts: mountpoint(1) reports success
            // on DEAD mounts ("Transport endpoint is not connected").
            for _ in 0..40 {
                if !Self::unix_mounted(&self.mount_target) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(250));
            }
            panic!(
                "mount target {} still listed after unmount attempts",
                self.mount_target
            );
        } else {
            let root = PathBuf::from(&self.mount_target);
            for _ in 0..60 {
                if !root.exists() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
            panic!(
                "mount target {} still present after mount process death",
                self.mount_target
            );
        }
    }
}

// ---------------------------------------------------------------- sections

fn section_a(ctx: &Ctx, root: &Path) {
    println!("--- A: state-machine boundaries ---");
    let sm = root.join("sm");
    std::fs::create_dir_all(&sm).unwrap();

    // A1 same-offset rewrite: the later write owns its range; earlier
    // pending bytes beyond it survive (pwrite semantics).
    let p = sm.join("a1.bin");
    let mut f = open_create(&p);
    write_at(&mut f, 0, &pattern(8, 1));
    write_at(&mut f, 0, &[0x42, 0x42]);
    // Strong close: A-section asserts FINAL SEMANTICS; a plain close on
    // Windows has the async-commit visibility window (B4 measures it).
    flush_strong(&mut f, "A1 commit");
    drop(f);
    let mut want = vec![0x42u8, 0x42];
    want.extend_from_slice(&pattern(8, 1)[2..]);
    assert_bytes("A1 same-offset rewrite", &p, &want);

    // A2 two-commit overlay: commit a high range, then a lower range that
    // overlaps it - the later commit owns the overlap.
    let p = sm.join("a2.bin");
    let mut f = open_create(&p);
    write_at(&mut f, 4096, &pattern(8192, 2));
    flush_strong(&mut f, "A2 first commit");
    write_at(&mut f, 0, &pattern(8192, 3));
    flush_strong(&mut f, "A2 second commit");
    drop(f);
    let mut want = pattern(8192, 3);
    want.extend_from_slice(&pattern(8192, 2)[4096..]);
    assert_bytes("A2 high-then-low overlap across commits", &p, &want);

    // A3 one-commit multi-chunk with gaps: gaps zero-fill.
    let p = sm.join("a3.bin");
    let mut f = open_create(&p);
    write_at(&mut f, 0, &pattern(4096, 4));
    write_at(&mut f, 8192, &pattern(4096, 5));
    write_at(&mut f, 16384, &pattern(4096, 6));
    flush_strong(&mut f, "A3 commit");
    drop(f);
    let mut want = pattern(4096, 4);
    want.extend(std::iter::repeat_n(0u8, 4096));
    want.extend_from_slice(&pattern(4096, 5));
    want.extend(std::iter::repeat_n(0u8, 4096));
    want.extend_from_slice(&pattern(4096, 6));
    assert_bytes("A3 multi-chunk gaps zero-fill", &p, &want);

    // A4 truncate-extend then write past the new EOF.
    let p = sm.join("a4.bin");
    let mut f = open_create(&p);
    write_at(&mut f, 0, b"abc");
    f.set_len(8192).unwrap();
    write_at(&mut f, 8192, b"XYZ");
    flush_strong(&mut f, "A4 commit");
    drop(f);
    let mut want = b"abc".to_vec();
    want.extend(std::iter::repeat_n(0u8, 8189));
    want.extend_from_slice(b"XYZ");
    assert_bytes("A4 extend + write past EOF", &p, &want);

    // A5 shrink a dirty chunk STRADDLING the new EOF, then append at the
    // new EOF. Deterministic in both kernel timings: pages beyond the new
    // EOF are invalidated (never resent) or already delivered (the adapter
    // trims the straddling chunk).
    let p = sm.join("a5.bin");
    let mut f = open_create(&p);
    write_at(&mut f, 0, &pattern(8192, 7));
    f.set_len(4096).unwrap();
    write_at(&mut f, 4096, b"END");
    flush_strong(&mut f, "A5 commit");
    drop(f);
    let mut want = pattern(8192, 7)[..4096].to_vec();
    want.extend_from_slice(b"END");
    assert_bytes("A5 straddle shrink + append", &p, &want);

    // A6 truncate to zero inside a session, then rewrite.
    let p = sm.join("a6.bin");
    let mut f = open_create(&p);
    write_at(&mut f, 0, &pattern(4096, 8));
    f.set_len(0).unwrap();
    write_at(&mut f, 0, b"fresh");
    flush_strong(&mut f, "A6 commit");
    drop(f);
    assert_bytes("A6 truncate-to-zero + rewrite", &p, b"fresh");

    // A7 two handles, conflict + retry: h2 opened before h1 commits, so
    // h2's first strong flush MUST fail the CAS (observable error); its
    // retry succeeds on the re-based version with both handles' data.
    // (Adapter-only: a plain local dir has no CAS.)
    if ctx.adapter {
        let p = sm.join("a7.bin");
        let mut h1 = open_create(&p);
        let mut h2 = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&p)
            .unwrap_or_else(|e| panic!("A7: second open failed: {e}"));
        write_at(&mut h1, 0, b"AAAA");
        write_at(&mut h2, 4096, b"BBBB");
        flush_strong(&mut h1, "A7 h1 commit");
        drop(h1);
        let conflicted = h2.sync_all().is_err();
        if !conflicted {
            panic!("A7: expected h2 flush to fail with CAS conflict (error must be observable)");
        }
        println!("  OK  A7 CAS conflict observed on h2 flush");
        h2.sync_all()
            .unwrap_or_else(|e| panic!("A7: retry flush failed: {e}"));
        drop(h2);
        let mut want = b"AAAA".to_vec();
        want.extend(std::iter::repeat_n(0u8, 4092));
        want.extend_from_slice(b"BBBB");
        assert_bytes("A7 multi-handle CAS conflict + retry flush", &p, &want);
    } else {
        println!("  SKIP A7 (selftest: CAS conflict needs the mount adapter)");
    }

    // A8 append sequence.
    let p = sm.join("a8.txt");
    {
        let mut f = open_create(&p);
        f.write_all(b"one\n").unwrap();
        flush_strong(&mut f, "A8 first append");
    }
    {
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(&p)
            .unwrap_or_else(|e| panic!("A8: append open failed: {e}"));
        f.write_all(b"two\n").unwrap();
        f.write_all(b"three\n").unwrap();
        flush_strong(&mut f, "A8 second append");
    }
    assert_bytes("A8 append sequence", &p, b"one\ntwo\nthree\n");

    // A9 empty file lifecycle: create empty, strong flush, still empty.
    let p = sm.join("a9.bin");
    {
        let mut f = open_create(&p);
        flush_strong(&mut f, "A9 flush");
    }
    assert_bytes("A9 empty create+flush stays empty", &p, b"");

    // A10 dup-close first-write preservation: fd1 closed (FLUSH fires
    // BEFORE any data - bash's `cmd > file` does exactly this via
    // dup2+close), then fd2 writes. The first (empty) commit must not
    // destroy the subsequent write.
    let p = sm.join("a10.bin");
    let f1 = open_create(&p);
    let mut f2 = f1.try_clone().expect("A10: dup fd");
    drop(f1); // first close -> empty flush (the dup-close ordering)
    write_at(&mut f2, 0, b"first write content\n");
    // Strong close on the SECOND fd: the write lands after the first
    // flush, and the session re-arm must preserve it.
    flush_strong(&mut f2, "A10 commit");
    drop(f2);
    assert_bytes(
        "A10 dup-close first-write preserved",
        &p,
        b"first write content\n",
    );

    // A11 read-your-writes BEFORE flush: POSIX requires a read (or mmap
    // fault) after write(2) to return the written bytes even while the
    // data is still in the write-back session. Found missing when git
    // verified a pack file mid-write: a committed-only read truncated the
    // file. POSIX_FADV_DONTNEED evicts the kernel's clean pages so the
    // read-back REALLY reaches the adapter instead of the page cache.
    let p = sm.join("a11.bin");
    let data = pattern(1_572_864, 21); // 1.5MB - many write-back chunks
    let mut f = open_create(&p);
    let mut off = 0usize;
    for chunk in data.chunks(4096) {
        write_at(&mut f, off as u64, chunk);
        off += chunk.len();
    }
    #[cfg(target_os = "linux")]
    unsafe {
        use std::os::unix::io::AsRawFd;
        if libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) != 0 {
            eprintln!("A11: posix_fadvise failed, page cache may mask the check");
        }
    }
    assert_bytes("A11 read-your-writes before flush", &p, &data);
    flush_strong(&mut f, "A11 commit");
    drop(f);
    assert_bytes("A11 read-your-writes after flush", &p, &data);

    // A12 readdir snapshot lifecycle: a paged directory walk that outlives
    // the snapshot TTL must fail LOUDLY (EINVAL on the next page), never
    // replay a silently stale list; a fresh walk afterwards must be clean
    // and complete. The suite's mount sets LIGHTFS_FUSE_DIR_SNAPSHOT_SECS=2
    // (fuse-only knob, default 30) so the eviction path costs seconds; the
    // 250-char names force real pagination - 1200 of them exceed one FUSE
    // reply buffer on every kernel this suite runs on. Raw getdents64 with
    // a 512-byte buffer: std's ReadDir buffers a whole kernel batch in
    // userspace, so ReadDir::next() is NOT one readdir callback and a mid
    // -iteration sleep would land inside a buffered page, never reaching
    // the adapter (first A12 draft tripped on exactly that).
    #[cfg(target_os = "linux")]
    {
        fn getdents_page(fd: std::os::unix::io::RawFd) -> std::io::Result<usize> {
            let mut buf = [0u8; 512]; // ~1 entry per call: every call is its own FUSE readdir
            let n = unsafe { libc::syscall(libc::SYS_getdents64, fd, buf.as_mut_ptr(), buf.len()) };
            if n < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(n as usize)
            }
        }

        let dir = sm.join("a12");
        std::fs::create_dir_all(&dir).expect("A12 mkdir");
        let mut names: Vec<String> = Vec::with_capacity(1200);
        for i in 0..1200 {
            // lexicographic == numeric order (4-digit suffix), so the vec
            // stays sorted for the membership check below
            let n = format!("a12-{i:04}-{}", "x".repeat(240));
            std::fs::write(dir.join(&n), b"").expect("A12 create");
            names.push(n);
        }
        let cpath = std::ffi::CString::new(dir.to_str().unwrap()).unwrap();
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
        assert!(fd >= 0, "A12: open dir fd");
        // page 1 (offset==0): builds the snapshot
        assert!(
            getdents_page(fd).expect("A12: first page read") > 0,
            "A12: first page must list entries"
        );
        // page 2 (offset>0): snapshot still fresh, paged reads work
        assert!(
            getdents_page(fd).expect("A12: second page read") > 0,
            "A12: second page must list entries"
        );
        std::thread::sleep(Duration::from_secs(3)); // > snapshot TTL (2s)
        // next page: snapshot gone - EINVAL, never a stale list
        let err = getdents_page(fd).expect_err("A12: expired snapshot must fail");
        assert_eq!(
            err.raw_os_error(),
            Some(libc::EINVAL),
            "A12: expired snapshot must fail with EINVAL, got {err}"
        );
        unsafe { libc::close(fd) }; // releasedir drops the dead snapshot

        // fresh walk: complete listing, nothing stale, nothing missing
        let mut count = 0usize;
        for e in std::fs::read_dir(&dir).expect("A12 reopen dir") {
            let e = e.expect("A12: fresh walk entry");
            assert!(
                names
                    .binary_search(&e.file_name().to_string_lossy().to_string())
                    .is_ok(),
                "A12: unexpected entry {:?}",
                e.file_name()
            );
            count += 1;
        }
        assert_eq!(count, 1200, "A12: fresh walk must list every entry");
    }

    // A13 readdirplus lookup cache: post-listing lookups are served from
    // the listing snapshot (attr_ttl bounded). Regression: cross-client
    // mutations must converge within the TTL, local ones immediately.
    // The mount runs with the default LIGHTFS_FUSE_ATTR_TTL_SECS=1, so
    // every convergence bound below is TTL + generous slack.
    // adapter-only: the cross-client side needs the second RPC client,
    // which --selftest (no server) does not have.
    if ctx.adapter && ctx.sections_want("a") && cfg!(unix) {
        println!("  A13 readdirplus lookup cache (cross-client bounds)");
        let dir = root.join("a13");
        std::fs::create_dir_all(&dir).expect("A13 mkdir");
        std::fs::write(dir.join("a13-one"), b"1").expect("A13 seed one");
        std::fs::write(dir.join("a13-two"), b"2").expect("A13 seed two");
        // seed the lookup cache with a listing
        let listed: Vec<String> = std::fs::read_dir(&dir)
            .expect("A13 listing")
            .map(|e| {
                e.expect("A13 entry")
                    .file_name()
                    .to_string_lossy()
                    .to_string()
            })
            .collect();
        assert!(listed.contains(&"a13-one".to_string()));

        // remote CREATE of an unlisted name: fresh (never-cached) lookup
        // must see it immediately - negative lookups are not cached
        rpc_post(
            &ctx.rpc_url(),
            "write",
            r#"{"path":"/a13/a13-remote","data":"cg=="}"#,
        );
        let t0 = Instant::now();
        loop {
            if dir.join("a13-remote").exists() {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_millis(2500),
                "A13: remote create visible within attr_ttl+slack, took {:?}",
                t0.elapsed()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(
            t0.elapsed() < Duration::from_millis(2500),
            "A13: remote create must converge quickly, took {:?}",
            t0.elapsed()
        );

        // remote DELETE of a LISTED file: the lookup cache knows it as
        // existing; the deletion must surface within attr_ttl + slack
        rpc_post(&ctx.rpc_url(), "unlink", r#"{"path":"/a13/a13-two"}"#);
        let t0 = Instant::now();
        loop {
            if !dir.join("a13-two").exists() {
                break;
            }
            assert!(
                t0.elapsed() < Duration::from_millis(2500),
                "A13: remote delete of a listed file must converge within attr_ttl+slack, took {:?}",
                t0.elapsed()
            );
            std::thread::sleep(Duration::from_millis(20));
        }

        // LOCAL create: mutation drops the cache, visibility is immediate
        std::fs::write(dir.join("a13-local"), b"3").expect("A13 local create");
        assert!(
            dir.join("a13-local").exists(),
            "A13: local create must be visible immediately"
        );
        // LOCAL delete of a listed file: immediate, no stale existence
        std::fs::remove_file(dir.join("a13-one")).expect("A13 local delete");
        assert!(
            !dir.join("a13-one").exists(),
            "A13: local delete must be visible immediately"
        );
        println!("    OK A13 readdirplus bounds (create/delete converge, local immediate)");
    }

    // A14 symlink semantics on the real mount: the kernel resolves links;
    // the filesystem stores targets verbatim and answers readlink.
    // Covers: lstat vs stat, relative/absolute/dir/dangling links, loops,
    // rename/unlink interactions, and the mount-root escape clamp.
    // Compile-time gated: libc and unix extensions do not exist on the
    // Windows build of this binary (A13-style runtime guards are not enough).
    #[cfg(unix)]
    if ctx.sections_want("a") {
        use std::os::unix::fs::{MetadataExt, symlink};
        println!("  A14 symlinks on the mount (lstat/stat/resolve/loops)");
        let dir = root.join("a14");
        std::fs::create_dir_all(&dir).expect("A14 mkdir");
        std::fs::write(dir.join("real.txt"), b"payload").expect("A14 seed");

        // relative + absolute targets; readlink returns them verbatim
        symlink("real.txt", dir.join("rel")).expect("A14 rel link");
        symlink(dir.join("real.txt"), dir.join("abs")).expect("A14 abs link");
        assert_eq!(
            std::fs::read_link(dir.join("rel"))
                .unwrap()
                .to_string_lossy(),
            "real.txt"
        );
        assert_eq!(
            std::fs::read_link(dir.join("abs")).unwrap(),
            dir.join("real.txt")
        );

        // lstat vs stat
        let lst = std::fs::symlink_metadata(dir.join("rel")).expect("A14 lstat");
        assert!(lst.file_type().is_symlink(), "lstat must see the link");
        assert_eq!(lst.len(), "real.txt".len() as u64, "link size = target len");
        let st = std::fs::metadata(dir.join("rel")).expect("A14 stat follows");
        assert!(!st.file_type().is_symlink());
        assert_eq!(st.len(), 7, "stat follows to the file content");
        // read through the link
        assert_eq!(std::fs::read(dir.join("rel")).unwrap(), b"payload");

        // directory link: listing and creation land in the target dir
        symlink("sub", dir.join("dirlink")).expect("A14 dir link");
        std::fs::create_dir(dir.join("sub")).expect("A14 sub");
        std::fs::write(dir.join("dirlink").join("via-link"), b"v").expect("A14 create via link");
        assert!(
            dir.join("sub").join("via-link").exists(),
            "create through a dir link must land in the target dir"
        );
        let mut listed = std::fs::read_dir(dir.join("dirlink"))
            .expect("A14 readdir via link")
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect::<Vec<_>>();
        listed.sort();
        assert!(listed.contains(&"via-link".to_string()));

        // dangling: lstat sees the link, stat and read fail ENOENT
        symlink("nowhere-here", dir.join("dangling")).expect("A14 dangling");
        assert!(
            std::fs::symlink_metadata(dir.join("dangling"))
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(std::fs::metadata(dir.join("dangling")).is_err());
        assert!(std::fs::read(dir.join("dangling")).is_err());

        // loop: the kernel must fail with ELOOP, lstat still works
        symlink("b", dir.join("a")).expect("A14 loop a");
        symlink("a", dir.join("b")).expect("A14 loop b");
        let err = std::fs::metadata(dir.join("a")).unwrap_err();
        assert_eq!(
            err.raw_os_error(),
            Some(libc::ELOOP),
            "loop must be ELOOP, got {err}"
        );

        // rename the LINK: target rides along verbatim
        std::fs::rename(dir.join("rel"), dir.join("rel2")).expect("A14 rename link");
        assert_eq!(
            std::fs::read_link(dir.join("rel2"))
                .unwrap()
                .to_string_lossy(),
            "real.txt"
        );
        assert_eq!(std::fs::read(dir.join("rel2")).unwrap(), b"payload");

        // rename the TARGET: the link goes dangling, target string unchanged
        std::fs::rename(dir.join("real.txt"), dir.join("moved.txt")).expect("A14 rename target");
        assert!(std::fs::metadata(dir.join("rel2")).is_err());
        assert_eq!(
            std::fs::read_link(dir.join("rel2"))
                .unwrap()
                .to_string_lossy(),
            "real.txt"
        );
        // ...and back: the link heals itself (resolution is by name)
        std::fs::rename(dir.join("moved.txt"), dir.join("real.txt")).expect("A14 rename back");
        assert_eq!(std::fs::read(dir.join("rel2")).unwrap(), b"payload");

        // deleting a link never touches the target
        std::fs::remove_file(dir.join("rel2")).expect("A14 unlink link");
        assert!(dir.join("real.txt").exists());
        assert!(std::fs::read_link(dir.join("rel2")).is_err());

        // an occupied name rejects link creation (any kind)
        let e = symlink("real.txt", dir.join("real.txt")).unwrap_err();
        assert_eq!(e.raw_os_error(), Some(libc::EEXIST));

        // Absolute-target semantics (review decision E1-F1a, NFS-style):
        // under a real FUSE mount the KERNEL resolves absolute link targets
        // against the CALLING PROCESS root - they can and do land OUTSIDE
        // the LightFS mount. The Core/RPC path instead clamps at the
        // virtual root (chroot). Both sides are asserted here against a
        // marker this test creates and removes, never against host state.
        let marker_host =
            std::env::temp_dir().join(format!("lightfs-a14-marker-{}", std::process::id()));
        std::fs::write(&marker_host, b"HOST-MARKER").expect("A14 host marker");
        symlink(&marker_host, dir.join("abs-host")).expect("A14 abs-host link");
        assert_eq!(
            std::fs::read_link(dir.join("abs-host")).unwrap(),
            marker_host,
            "readlink must return the stored target verbatim"
        );
        // kernel-side resolution reaches the host file: the selected
        // semantics, asserted on purpose
        assert_eq!(
            std::fs::read(dir.join("abs-host")).unwrap(),
            b"HOST-MARKER",
            "FUSE resolves absolute targets against the caller root (NFS-style)"
        );
        // Core/RPC-side resolution clamps at the virtual root: the same
        // link read through the RPC client must NOT reach the host file
        // (adapter-only: --selftest has no server to ask)
        if ctx.adapter {
            let (ok, body) = rpc_call(
                &ctx.rpc_url(),
                "read",
                "{\"path\":\"/a14/abs-host\",\"offset\":0,\"size\":64}",
            );
            assert!(
                !ok && body.contains("NOT_FOUND"),
                "Core/RPC must clamp absolute targets at the virtual root, got ok={ok} {body}"
            );
        }
        let _ = std::fs::remove_file(&marker_host);

        // same name file+link coexistence: distinct inodes, distinct kinds
        symlink("real.txt", dir.join("twin")).expect("A14 twin link");
        let l = std::fs::symlink_metadata(dir.join("twin")).unwrap();
        let f = std::fs::symlink_metadata(dir.join("real.txt")).unwrap();
        assert!(l.file_type().is_symlink() && f.file_type().is_file());
        assert_ne!(l.ino(), f.ino(), "link and file are distinct nodes");

        println!("    OK A14 symlinks (all 10 behaviors)");
    }

    // A15 symlink + double-rename + open-fd read observation (E1 finding 3).
    // A single unexplained anomaly (a 5-byte read where 4 were written) was
    // observed once in manual testing and never reproduced. This case pins
    // the EXACT original operation order and fails LOUDLY on the first
    // mismatch - no retries, no tolerance - with a diagnostics dump written
    // into the workdir. Limited rounds run in the gate; the longer sweep
    // lives behind the "stress" section (see stress_a15 below).
    #[cfg(unix)]
    if ctx.adapter && ctx.sections_want("a") {
        println!("  A15 symlink+rename+fd observation (10 rounds)");
        run_a15_rounds(ctx, root, 10, "A15");
    }
}

/// A15 rounds: the observation case from E1 finding 3, repeated. Strict
/// byte+length assertions; the first mismatch appends diagnostics to the
/// workdir and panics - a retry loop here would be masking, not testing.
#[cfg(unix)]
fn run_a15_rounds(ctx: &Ctx, root: &Path, rounds: usize, label: &str) {
    let dir = root.join("a15");
    std::fs::create_dir_all(&dir).expect("A15 mkdir");
    let data = b"Race";
    let data_b64 = "UmFjZQ==";
    for i in 0..rounds {
        let tag = format!("{i}");
        let link = dir.join(format!("l{tag}"));
        let link2 = dir.join(format!("l2-{tag}"));
        let file = dir.join(format!("t{tag}"));
        let file2 = dir.join(format!("t2-{tag}"));

        rpc_post(
            &ctx.rpc_url(),
            "write",
            &format!("{{\"path\":\"/a15/t{tag}\",\"data\":\"{data_b64}\"}}"),
        );
        rpc_post(
            &ctx.rpc_url(),
            "symlink",
            &format!("{{\"path\":\"/a15/l{tag}\",\"target\":\"t{tag}\"}}"),
        );
        // The original reproduction order: open THROUGH the link, rename
        // the link, rename the target, then read from the still-open fd.
        let cpath = std::ffi::CString::new(link.to_str().unwrap()).unwrap();
        let fd = unsafe { libc::open(cpath.as_ptr(), libc::O_RDONLY) };
        assert!(fd >= 0, "{label} iter {i}: open via link failed");
        let msg = format!("{label} iter {i}: link rename");
        std::fs::rename(&link, &link2).expect(&msg);
        let msg = format!("{label} iter {i}: target rename");
        std::fs::rename(&file, &file2).expect(&msg);
        let mut buf = [0u8; 100];
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        unsafe { libc::close(fd) };
        let got = &buf[..n.max(0) as usize];
        if n != data.len() as isize || got != data {
            let diag = format!(
                "{label} MISMATCH iter {i}: expected {:?} (4 bytes), read n={n} bytes {:?}\n\
                 dir state: l={:?} l2={:?} t={:?} t2={:?}\n",
                data,
                got,
                std::fs::read_link(&link).is_ok(),
                std::fs::read_link(&link2).is_ok(),
                file.exists(),
                file2.exists(),
            );
            let diag_path = ctx.workdir.join(format!("{label}-mismatch.log"));
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&diag_path)
                .and_then(|mut f| f.write_all(diag.as_bytes()))
                .ok();
            eprintln!("{diag}diagnostics appended to {}", diag_path.display());
            let p = diag_path.display();
            panic!("{label} iter {i}: fd read returned wrong bytes - see {p}");
        }
        // cleanup for the next round
        let _ = std::fs::remove_file(&link2);
        let _ = std::fs::remove_file(&file2);
    }
    println!("    OK {label} ({rounds} rounds, all byte-exact)");
}

/// Long repetition entry for the A15 observation case: run with
/// `lightfs-accept --sections stress ...` against a real mount.
#[cfg(unix)]
fn stress_a15(ctx: &Ctx, root: &Path) {
    println!("--- stress: A15 observation x200 ---");
    run_a15_rounds(ctx, root, 200, "STRESS-A15");
}

/// Minimal raw-HTTP RPC used by A13/A14/A15 to act as a second (out-of-band)
/// client while the mount under test stays connected. Returns
/// (ok, full response text) so callers can assert on failures too.
fn rpc_call(server_url: &str, method: &str, params: &str) -> (bool, String) {
    use std::io::{Read, Write};
    let addr = server_url.trim_start_matches("http://");
    let mut s = std::net::TcpStream::connect(addr).expect("rpc connect");
    let body = format!("{{\"id\":1,\"method\":\"{method}\",\"params\":{params}}}");
    let req = format!(
        "POST /rpc HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    s.write_all(req.as_bytes()).expect("rpc write");
    let mut resp = String::new();
    s.read_to_string(&mut resp).expect("rpc read");
    (resp.contains("\"ok\":true"), resp)
}

fn rpc_post(server_url: &str, method: &str, params: &str) {
    let (ok, resp) = rpc_call(server_url, method, params);
    assert!(ok, "rpc {method} failed: {resp}");
}

fn section_b(ctx: &mut Ctx, root: &Path) {
    println!("--- B: flush paths ---");
    let sm = root.join("sm");

    // B1 strong flush coherence. The no-retry guarantee IS flush_strong
    // not panicking - a failed commit surfaces there.
    let p = sm.join("b1.bin");
    let mut f = open_create(&p);
    let big = pattern(1 << 20, 9);
    write_at(&mut f, 0, &big);
    flush_strong(&mut f, "B1 commit");
    drop(f);
    assert_bytes("B1 strong flush coherence", &p, &big);

    // B2 flush -> more writes -> flush: committed-session re-arm.
    let p = sm.join("b2.bin");
    let mut f = open_create(&p);
    let first = pattern(65536, 10);
    let second = pattern(65536, 11);
    write_at(&mut f, 0, &first);
    flush_strong(&mut f, "B2 first commit");
    write_at(&mut f, 65536, &second);
    flush_strong(&mut f, "B2 second commit");
    drop(f);
    let mut want = first.clone();
    want.extend_from_slice(&second);
    assert_bytes("B2 flush-write-flush (re-arm)", &p, &want);

    // B3 flush FAILURE injection: server down -> strong flush MUST fail;
    // after recovery the retry commits the SAME data (the drained-chunks
    // recovery path). A swallowed error fails this test.
    let p = sm.join("b3.bin");
    let mut f = open_create(&p);
    let b3 = pattern(65536, 12);
    write_at(&mut f, 0, &b3);
    ctx.kill_server();
    let observed = f.sync_all().is_err();
    if !observed {
        panic!("B3: flush during server outage did NOT surface an error");
    }
    println!("  OK  B3 flush failure observable during server outage");
    ctx.start_server();
    f.sync_all()
        .unwrap_or_else(|e| panic!("B3: retry flush failed: {e}"));
    drop(f);
    assert_bytes("B3 retry flush after recovery commits the data", &p, &b3);

    // B4 plain-close visibility: MEASURED and reported as eventual,
    // explicitly NOT strong. Close without the strong flush - the commit
    // runs in the platform's async close path (on Windows the WinFsp FUSE
    // Close callback; on Linux FUSE flush is synchronous so ~0 window).
    let p = sm.join("b4.bin");
    let b4 = pattern(5 << 20, 13);
    {
        let mut f = open_create(&p);
        write_at(&mut f, 0, &b4);
    } // plain close, no sync_all
    let want_sha = hex(&b4);
    let t0 = Instant::now();
    loop {
        if sha256_file(&p) == want_sha {
            break;
        }
        if t0.elapsed() > Duration::from_secs(10) {
            panic!("B4: plain-close content not correct within 10s (eventual visibility violated)");
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    println!(
        "  OK  B4 plain-close eventual visibility after {}ms (measured; NOT strong)",
        t0.elapsed().as_millis()
    );
}

fn section_c(ctx: &mut Ctx, root: &Path) {
    println!("--- C: crash and persistence ---");
    let persist_dir = root.join("persist");
    std::fs::create_dir_all(&persist_dir).unwrap();

    // Seed the persistence set with EXPLICIT strong flushes, then record
    // the manifest (plain-close writes could still be inside their
    // visibility window - do not build guarantees on them).
    let mut manifest: Vec<(String, String)> = Vec::new();
    for (name, n, seed) in [("p1.bin", 65536usize, 14u8), ("p2.bin", 300000, 15u8)] {
        let data = pattern(n, seed);
        let pp = persist_dir.join(name);
        let mut f = open_create(&pp);
        write_at(&mut f, 0, &data);
        flush_strong(&mut f, "C seed");
        drop(f);
        manifest.push((name.to_string(), hex(&data)));
    }
    {
        let pp = persist_dir.join("p3.txt");
        let mut f = open_create(&pp);
        write_at(&mut f, 0, b"persist-me-p3");
        flush_strong(&mut f, "C seed");
        drop(f);
        manifest.push(("p3.txt".to_string(), hex(b"persist-me-p3")));
    }
    println!("  manifest: {} persisted files recorded", manifest.len());

    // C1 kill the mount with DIRTY (unflushed) data: the create committed
    // an empty placeholder, the unflushed chunks die with the process -
    // the file must come back EMPTY (documented loss of unconfirmed data)
    // and committed files must be untouched.
    let p = root.join("sm").join("c1.bin");
    // The fd stays OPEN across the crash: dropping it would close(2) the
    // file and commit the data before the mount dies.
    let mut stale = open_create(&p);
    write_at(&mut stale, 0, &pattern(65536, 16));
    ctx.kill_mount_only();
    // The daemon is dead, so close(2)/fsync here CANNOT commit anything
    // (the chunks lived in the killed process). Close BEFORE unmounting:
    // an open fd pins the dead fuse mount - fusermount -u gets EBUSY and
    // even the lazy variants leave the entry listed (seen on CentOS 7),
    // and mounting over that corpse fails with ENOTCONN on kernel 3.10.
    let _ = stale.sync_all();
    drop(stale);
    ctx.unmount_target();
    ctx.start_mount();
    println!("MOUNTED (after C1 crash)");
    let len = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(u64::MAX);
    if len != 0 {
        panic!("C1: unflushed file length {len} after crash, want 0 (empty placeholder)");
    }
    println!("  OK  C1 dirty data died with the mount (file empty, no corruption)");

    // C2 server crash + remount: committed data must be byte-exact.
    ctx.kill_server();
    ctx.start_server();
    ctx.kill_mount();
    ctx.start_mount();
    println!("MOUNTED (after C2 server crash)");
    for (name, sha) in &manifest {
        let pp = persist_dir.join(name);
        if !pp.exists() {
            panic!("C2: persisted file {pp:?} missing after server crash");
        }
        let got = sha256_file(&pp);
        if &got != sha {
            panic!("C2: {pp:?} sha mismatch after server crash ({got} != {sha})");
        }
    }
    println!(
        "  OK  C2 {} persisted files byte-exact after server crash + remount",
        manifest.len()
    );

    // C3 rename / delete / enum on the rebuilt mount.
    let a = persist_dir.join("p3.txt");
    let b = persist_dir.join("p3-renamed.txt");
    std::fs::rename(&a, &b).unwrap_or_else(|e| panic!("C3: rename failed: {e}"));
    if !b.exists() {
        panic!("C3: rename failed (target missing)");
    }
    std::fs::remove_file(&b).unwrap_or_else(|e| panic!("C3: delete failed: {e}"));
    if b.exists() {
        panic!("C3: delete failed (target still present)");
    }
    let n = std::fs::read_dir(root.join("sm")).unwrap().count();
    if n < 8 {
        panic!("C3: expected >=8 entries in sm, got {n}");
    }
    println!("  OK  C3 rename/delete/enum ({n} files in sm)");

    // C3b shell client (server mode) sees the same namespace.
    if let Some(shell) = &ctx.shell {
        let out = Command::new(shell)
            .args(["--config"])
            .arg(&ctx.config)
            .args(["--server", &ctx.server_url, "ls", "/persist"])
            .output()
            .expect("C3b: shell spawn");
        let text = String::from_utf8_lossy(&out.stdout);
        if !text.contains("p1.bin") {
            panic!("C3b: shell ls misses p1.bin: {text}");
        }
        println!("  OK  C3b shell client sees the same namespace");
    }

    // C6 rename/unlink with open write handles (J1). A handle must keep
    // writing to the SAME inode the kernel renamed, committing under the
    // CURRENT name (rename moves parent/name only - the CAS anchor version
    // is untouched); a handle whose name died (unlink, or the rename that
    // overwrote it) must close cleanly and must never resurrect or alias
    // a recycled name.
    #[cfg(unix)]
    if ctx.adapter && ctx.sections_want("c") {
        println!("  C6 rename/unlink with open write handles");
        let d = root.join("c6");
        let d2 = root.join("c6d2");
        std::fs::create_dir_all(&d).expect("C6 mkdir");
        std::fs::create_dir_all(&d2).expect("C6 mkdir d2");

        // a) same-dir rename, keep writing, strong fsync: the commit must
        //    look the inode up under the NEW name and succeed there.
        let want_a = b"AAAA-j1-same-dir+after-rename";
        let fa = d.join("a.txt");
        let mut h = open_create(&fa);
        write_at(&mut h, 0, b"AAAA-j1-same-dir");
        std::fs::rename(&fa, d.join("a2.txt")).expect("C6a: rename with open write handle failed");
        write_at(&mut h, 16, b"+after-rename");
        flush_strong(&mut h, "C6a fsync after rename");
        drop(h);
        if fa.exists() {
            panic!("C6a: old name still present after rename");
        }
        assert_bytes(
            "C6a renamed-handle content under new name",
            &d.join("a2.txt"),
            want_a,
        );

        // b) cross-directory rename, same contract.
        let want_b = b"BBBB-j1-cross-dir+tail";
        let fb = d.join("b.txt");
        let mut h = open_create(&fb);
        write_at(&mut h, 0, b"BBBB-j1-cross-dir");
        std::fs::rename(&fb, d2.join("b2.txt")).expect("C6b: cross-dir rename failed");
        write_at(&mut h, 17, b"+tail");
        flush_strong(&mut h, "C6b fsync after cross-dir rename");
        drop(h);
        if fb.exists() {
            panic!("C6b: old name still present after cross-dir rename");
        }
        assert_bytes(
            "C6b cross-dir renamed-handle content",
            &d2.join("b2.txt"),
            want_b,
        );

        // c) plain close after rename (no fsync): commit rides close's
        //    flush on Linux (synchronous) and the async close path on
        //    Windows - assert the SHARED contract: eventual visibility.
        let fc = d.join("plain.txt");
        {
            let mut h = open_create(&fc);
            write_at(&mut h, 0, b"PLAIN-j1-close-after-rename");
            std::fs::rename(&fc, d.join("plain2.txt")).expect("C6c rename failed");
        } // plain close
        let t0 = Instant::now();
        loop {
            if std::fs::read(d.join("plain2.txt")).ok().as_deref()
                == Some(b"PLAIN-j1-close-after-rename".as_slice())
            {
                break;
            }
            if t0.elapsed() > Duration::from_secs(10) {
                panic!("C6c: close-after-rename data not visible within 10s");
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        if fc.exists() {
            panic!("C6c: old name reappeared");
        }

        // d) rename OVER an open destination: the overwritten handle lost
        //    its inode's last name - it must close cleanly and its
        //    buffered data must NOT land in (or alias) the recycled name.
        let ft = d.join("ovr-target.txt");
        let fsrc = d.join("ovr-src.txt");
        let mut ht = open_create(&ft);
        write_at(&mut ht, 0, b"TARGET-OLD+stray-into-dead-inode");
        let mut hs = open_create(&fsrc);
        write_at(&mut hs, 0, b"SRC-NEW-DATA");
        std::fs::rename(&fsrc, &ft).expect("C6d: rename over open destination failed");
        drop(hs); // commits SRC-NEW-DATA under ovr-target.txt
        write_at(&mut ht, 11, b"-STRAY"); // disembodied inode: accepted, dropped
        drop(ht); // must close cleanly (commit is a no-op drop)
        let t0 = Instant::now();
        loop {
            if std::fs::read(&ft).ok().as_deref() == Some(b"SRC-NEW-DATA".as_slice()) {
                break;
            }
            if t0.elapsed() > Duration::from_secs(10) {
                panic!("C6d: moved content not visible under recycled name within 10s");
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        // Give a WRONGLY-routed stray commit time to surface before judging.
        std::thread::sleep(Duration::from_millis(300));
        let got = std::fs::read(&ft).unwrap_or_default();
        if got != b"SRC-NEW-DATA" {
            panic!("C6d: overwritten handle's data leaked into the recycled name: {got:?}");
        }

        // e) unlink with an open write handle: writes keep succeeding
        //    (POSIX), close(2) returns 0, the name must stay gone.
        let fe = d.join("gone.txt");
        let path_c = std::ffi::CString::new(fe.to_str().unwrap()).unwrap();
        let fd = unsafe {
            libc::open(
                path_c.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
                0o644,
            )
        };
        assert!(fd >= 0, "C6e: open failed");
        let w = b"NEVER-COMMITTED-j1";
        let n = unsafe { libc::pwrite(fd, w.as_ptr() as *const libc::c_void, w.len(), 0) };
        assert_eq!(n, w.len() as isize, "C6e: pwrite failed");
        std::fs::remove_file(&fe).expect("C6e: unlink failed");
        let w2 = b"-still-open";
        let n = unsafe {
            libc::pwrite(
                fd,
                w2.as_ptr() as *const libc::c_void,
                w2.len(),
                w.len() as i64,
            )
        };
        assert_eq!(
            n,
            w2.len() as isize,
            "C6e: write to unlinked handle must succeed (POSIX)"
        );
        let rc = unsafe { libc::close(fd) };
        assert_eq!(
            rc, 0,
            "C6e: close after unlink must succeed (POSIX open-fd semantics)"
        );
        if fe.exists() {
            panic!("C6e: unlinked name resurrected by the open handle's commit");
        }

        // f) existing error semantics survive: file->dir and dir->file
        //    renames still fail.
        std::fs::create_dir(d.join("adir")).expect("C6f mkdir");
        if std::fs::rename(d.join("a2.txt"), d.join("adir")).is_ok() {
            panic!("C6f: rename file onto directory must fail");
        }
        if std::fs::rename(d.join("adir"), d.join("a2.txt")).is_ok() {
            panic!("C6f: rename directory onto file must fail");
        }

        // g) unmount + remount: every C6 outcome survives restart byte-exact.
        ctx.kill_mount();
        ctx.start_mount();
        println!("MOUNTED (after C6 restart)");
        assert_bytes("C6g a2.txt after remount", &d.join("a2.txt"), want_a);
        assert_bytes("C6g b2.txt after remount", &d2.join("b2.txt"), want_b);
        assert_bytes(
            "C6g plain2.txt after remount",
            &d.join("plain2.txt"),
            b"PLAIN-j1-close-after-rename",
        );
        assert_bytes("C6g ovr-target.txt after remount", &ft, b"SRC-NEW-DATA");
        if fe.exists() {
            panic!("C6g: unlinked name appeared after remount");
        }
        println!("  OK  C6 rename/unlink with open write handles (a-g)");
    }

    // C7 detached-handle semantics, two-handle and read-path coverage.
    // Since K the FUSE adapter implements FULL detached semantics (K2-A):
    // a nameless inode's bytes live in memory until the last handle closes;
    // reads and writes on surviving handles are served and shared per-inode.
    // (WinFsp implements K2-B instead: refused at the write boundary,
    // ENOENT reads - those paths have no runtime assertions here.)
    #[cfg(unix)]
    if ctx.adapter && ctx.sections_want("c") {
        println!("  C7 detached-handle read/write semantics");
        let d = root.join("c7");
        std::fs::create_dir_all(&d).expect("C7 mkdir");

        // a) unlink with two open handles: bytes stay readable AND shared
        //    across the surviving handles until the last close.
        let fe = d.join("two-handles.txt");
        let mut h1 = open_create(&fe);
        write_at(&mut h1, 0, b"AAA-H1-UNCOMMITTED");
        let mut h2 = std::fs::File::open(&fe).expect("C7a second independent handle");
        std::fs::remove_file(&fe).expect("C7a unlink");
        if fe.exists() {
            panic!("C7a: unlinked name reappeared");
        }
        // Fresh open of the dead name must fail ENOENT (POSIX agrees).
        match std::fs::File::open(&fe) {
            Ok(_) => panic!("C7a: open of unlinked name must fail"),
            Err(e) => {
                if e.raw_os_error() != Some(libc::ENOENT) {
                    panic!("C7a: open after unlink: {e}, want ENOENT");
                }
            }
        }
        // Detached read through h2: the unflushed bytes survive (POSIX).
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut h2, &mut buf).expect("C7a detached read via h2");
        if buf != b"AAA-H1-UNCOMMITTED" {
            panic!("C7a: detached read via h2 got {buf:?}, want the h1 bytes");
        }
        // Writes through h1 keep succeeding, are really HELD, and are
        // visible through h2 (per-inode sharing, POSIX).
        write_at(&mut h1, 18, b"+H1-CONTINUES"); // 18 = EOF of AAA-H1-UNCOMMITTED
        flush_strong(&mut h1, "C7a fsync on detached handle");
        buf.clear();
        std::io::Seek::seek(&mut h2, std::io::SeekFrom::Start(0)).expect("C7a rewind h2");
        std::io::Read::read_to_end(&mut h2, &mut buf).expect("C7a second detached read");
        if buf != b"AAA-H1-UNCOMMITTED+H1-CONTINUES" {
            panic!("C7a: detached read after h1 write got {buf:?} (sharing broken)");
        }
        drop(h1);
        // h1 closed but h2 still open: the inode must still be alive.
        buf.clear();
        std::io::Seek::seek(&mut h2, std::io::SeekFrom::Start(0)).expect("C7a rewind h2 (2)");
        std::io::Read::read_to_end(&mut h2, &mut buf).expect("C7a read after h1 close");
        if buf != b"AAA-H1-UNCOMMITTED+H1-CONTINUES" {
            panic!("C7a: read via h2 after h1 close got {buf:?} (reaped too early)");
        }
        drop(h2); // last close reaps the anonymous inode
        println!("    OK  C7a unlink: two handles, detached bytes shared and alive");

        // b) rename OVER an open target: the stale handle keeps reading its
        //    OWN bytes (K1: never alias the incoming file), writes stay
        //    isolated, and the recycled name carries only the source data.
        let fsrc = d.join("r-src.txt");
        let ftgt = d.join("r-tgt.txt");
        {
            let mut f = open_create(&fsrc);
            write_at(&mut f, 0, b"SSS-SRC-DATA");
            flush_strong(&mut f, "C7b src seed");
        }
        {
            let mut f = open_create(&ftgt);
            write_at(&mut f, 0, b"TTT-OLD-TARGET");
            flush_strong(&mut f, "C7b tgt seed");
        }
        let mut ht = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&ftgt)
            .expect("C7b open target rw (no truncate - the seed must survive)");
        write_at(&mut ht, 14, b"+UNCOMMITTED-T2"); // 14 = EOF of TTT-OLD-TARGET
        std::fs::rename(&fsrc, &ftgt).expect("C7b rename over open target");
        // K1 PRIMARY: the stale handle reads its own inode, not the new file.
        std::io::Seek::seek(&mut ht, std::io::SeekFrom::Start(0)).expect("C7b rewind ht");
        let mut buf = Vec::new();
        std::io::Read::read_to_end(&mut ht, &mut buf).expect("C7b stale-handle read");
        if buf != b"TTT-OLD-TARGET+UNCOMMITTED-T2" {
            panic!("C7b: stale handle read {buf:?} - cross-inode aliasing (K1 violation)");
        }
        // The recycled name reads the incoming file's bytes.
        assert_bytes("C7b new name content", &ftgt, b"SSS-SRC-DATA");
        // Stray writes via the stale handle stay isolated from the new file.
        write_at(&mut ht, 29, b"+STRAY");
        flush_strong(&mut ht, "C7b fsync on overwritten handle");
        buf.clear();
        std::io::Seek::seek(&mut ht, std::io::SeekFrom::Start(0)).expect("C7b rewind ht (2)");
        std::io::Read::read_to_end(&mut ht, &mut buf).expect("C7b stale read after write");
        if buf != b"TTT-OLD-TARGET+UNCOMMITTED-T2+STRAY" {
            panic!("C7b: stale-handle content after write got {buf:?}");
        }
        drop(ht);
        // The stray data must not leak into the recycled name.
        std::thread::sleep(Duration::from_millis(300));
        assert_bytes("C7b target after stale close", &ftgt, b"SSS-SRC-DATA");
        if fsrc.exists() {
            panic!("C7b: source name still present");
        }
        println!("    OK  C7b rename-over-open-target: own bytes, no aliasing, no leak");

        // c) remount: detached bytes are memory-only (documented contract),
        //    the recycled name keeps the source data, nothing resurrects.
        ctx.kill_mount();
        ctx.start_mount();
        println!("MOUNTED (after C7 restart)");
        if fe.exists() {
            panic!("C7c: unlinked name appeared after remount");
        }
        assert_bytes("C7c target after remount", &ftgt, b"SSS-SRC-DATA");
        println!("  OK  C7 detached-handle semantics (a-c)");
    }

    // C4 clean shutdown: the drive must disappear with the mount process.
    ctx.kill_mount();
    println!("  OK  C4 unmount on process death");

    // C5 offline fsck (server stopped: no SQLite contention).
    ctx.kill_server();
    let out = Command::new(&ctx.lightfs)
        .args(["fsck", "--config"])
        .arg(&ctx.config)
        .output()
        .expect("C5: fsck spawn");
    let code = out.status.code().unwrap_or(-1);
    if code != 0 {
        panic!(
            "C5: fsck reported problems (exit {code}): {}",
            String::from_utf8_lossy(&out.stdout)
        );
    }
    println!("  OK  C5 offline fsck clean");
}

// ---------------------------------------------------------------- entry

fn main() {
    let mut args = std::env::args().skip(1);
    let mut lightfs = PathBuf::from("lightfs");
    let mut shell = None;
    let mut config = PathBuf::from("cfg.toml");
    let mut mount_target = String::new();
    let mut sections = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let mut selftest = false;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--lightfs" => lightfs = PathBuf::from(args.next().expect("--lightfs value")),
            "--shell" => shell = Some(PathBuf::from(args.next().expect("--shell value"))),
            "--config" => config = PathBuf::from(args.next().expect("--config value")),
            "--mount-target" => {
                mount_target = args.next().expect("--mount-target value");
                // A bare drive letter ("R:") is a RELATIVE path root on
                // Windows: Path::new("R:").join("sm") yields "R:sm" (the
                // drive's current directory). Normalize to "R:\\"
                let b = mount_target.as_bytes();
                if b.len() == 2 && b[1] == b':' && b[0].is_ascii_alphabetic() {
                    mount_target.push('\\');
                }
            }
            "--section" => {
                sections = args
                    .next()
                    .expect("--section value")
                    .split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .collect();
            }
            "--selftest" => selftest = true,
            other => panic!("unknown argument {other:?}"),
        }
    }

    if selftest {
        println!("selftest: local assertions against a temp dir (harness sanity)");
        let dir = std::env::temp_dir().join(format!("lightfs-accept-self-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut ctx = Ctx {
            lightfs: PathBuf::new(),
            shell: None,
            config: PathBuf::new(),
            mount_target: dir.to_string_lossy().to_string(),
            server_url: String::new(),
            server: None,
            mount: None,
            section_list: vec!["a".into()],
            workdir: dir.clone(),
            adapter: false,
        };
        section_a(&ctx, &dir);
        let _ = &mut ctx; // Drop cleans nothing here
        std::fs::remove_dir_all(&dir).ok();
        println!("SELFTEST PASSED");
        return;
    }

    if mount_target.is_empty() {
        panic!("--mount-target is required (a path on Linux, a drive letter like Q: on Windows)");
    }

    // server URL + workdir from the config (server bind / metadata path)
    let cfg_text = std::fs::read_to_string(&config).expect("read config");
    let server_bind =
        extract_toml_string(&cfg_text, "bind").unwrap_or_else(|| "127.0.0.1:18085".into());
    let meta_path = extract_toml_string(&cfg_text, "path").unwrap_or_else(|| ".".into());
    let server_url = format!("http://{server_bind}");
    let workdir = Path::new(&meta_path)
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    println!(
        "=== LIGHTFS MOUNT ACCEPTANCE ({} adapter, EXPERIMENTAL on windows) ===",
        std::env::consts::OS
    );
    println!("lightfs sha256: {}", sha256_file(&lightfs));
    println!("config: {:?}  mount-target: {}", config, mount_target);
    println!("workdir: {}", workdir.display());

    let mut ctx = Ctx {
        lightfs,
        shell,
        config,
        mount_target,
        server_url,
        server: None,
        mount: None,
        section_list: sections,
        workdir,
        adapter: true,
    };

    ctx.start_server();
    ctx.start_mount();
    println!("MOUNTED");

    let root = PathBuf::from(&ctx.mount_target);
    let t0 = Instant::now();
    if ctx.sections_want("a") {
        section_a(&ctx, &root);
    }
    if ctx.sections_want("b") {
        section_b(&mut ctx, &root);
    }
    if ctx.sections_want("c") {
        section_c(&mut ctx, &root);
    }
    #[cfg(unix)]
    if ctx.sections_want("stress") {
        stress_a15(&ctx, &root);
    }
    #[cfg(windows)]
    if ctx.sections_want("w") {
        // c ends with the mount and the server torn down (C4/C5); w needs
        // both back before it can probe drive semantics.
        ctx.ensure_running();
        section_w(&ctx, &root);
    }

    println!(
        "=== ACCEPTANCE PASSED in {:.1}s ===",
        t0.elapsed().as_secs_f64()
    );
    // Drop kills remaining children.
}

// ---------------------------------------------------------------------------
// W: Windows/WinFsp-specific semantics (E3 capability matrix, test-only).
// Linux never compiles this section; Windows CI runs it with --section w
// against the REAL WinFsp mount. Where a behavior is a SELECTED/documented
// platform difference it is asserted on purpose; where WinFsp semantics
// were previously unobserved the run RECORDS the observation and asserts
// only data consistency (no corruption, no hang).
#[cfg(windows)]
fn dir_names(p: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(p)
        .expect("W read_dir")
        .map(|e| {
            e.expect("W entry")
                .file_name()
                .to_string_lossy()
                .to_string()
        })
        .collect();
    v.sort();
    v
}

#[cfg(windows)]
fn section_w(ctx: &Ctx, root: &Path) {
    println!("--- W: Windows/WinFsp semantics (E3) ---");
    let dir = root.join("w");
    std::fs::create_dir_all(&dir).expect("W mkdir");

    // W1 overwrite-in-place: reopen, overwrite a middle region, sync
    // (FlushFileBuffers = the strong path), read back byte-exact. The seed
    // is strong-committed too: a plain close defers its commit (B4 window).
    let p = dir.join("w1.bin");
    write_strong(&p, &[0xAAu8; 65536], "W1 seed");
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(&p)
            .expect("W1 open");
        f.seek(SeekFrom::Start(1024)).expect("W1 seek");
        f.write_all(&vec![0x55u8; 1024]).expect("W1 write");
        f.sync_all().expect("W1 flush");
    }
    let got = std::fs::read(&p).expect("W1 read");
    let mut want = vec![0xAAu8; 65536];
    want[1024..2048].copy_from_slice(&[0x55u8; 1024]);
    assert_eq!(got, want, "W1: in-place overwrite must be byte-exact");
    println!("  OK   W1 overwrite-in-place (64KiB, middle 1KiB, flush-strong)");

    // W2 directory rename: children reference the parent by id, so the
    // whole subtree must follow.
    let a = dir.join("w2dir");
    std::fs::create_dir_all(a.join("sub")).expect("W2 mkdir");
    write_strong(&a.join("f.txt"), b"dir-content", "W2 seed");
    write_strong(&a.join("sub/g.txt"), b"nested", "W2 seed nested");
    std::fs::rename(&a, dir.join("w2dir-moved")).expect("W2 rename dir");
    assert_eq!(
        std::fs::read(dir.join("w2dir-moved/f.txt")).unwrap(),
        b"dir-content",
        "W2: child must follow the renamed directory"
    );
    assert_eq!(
        std::fs::read(dir.join("w2dir-moved/sub/g.txt")).unwrap(),
        b"nested"
    );
    assert!(!a.exists(), "W2: old name must be gone");
    println!("  OK   W2 directory rename (subtree follows, byte-exact)");

    // W2b: renaming ONTO an existing directory is rejected (v1 does not
    // implement the POSIX empty-dir overwrite). Recorded deviation.
    std::fs::create_dir_all(dir.join("w2b-target")).expect("W2b mkdir");
    let e = std::fs::rename(dir.join("w2dir-moved"), dir.join("w2b-target")).unwrap_err();
    assert!(
        e.raw_os_error() != Some(0),
        "W2b: directory overwrite must not succeed"
    );
    println!(
        "  OK   W2b rename onto existing dir rejected ({}), documented v1 deviation",
        e
    );

    // W3 rename over an existing FILE replaces it (destination retired).
    write_strong(&dir.join("w3-old"), b"OLD", "W3 seed");
    write_strong(&dir.join("w3-new"), b"NEWDATA", "W3 seed");
    std::fs::rename(dir.join("w3-new"), dir.join("w3-old")).expect("W3 rename over");
    assert_eq!(
        std::fs::read(dir.join("w3-old")).unwrap(),
        b"NEWDATA",
        "W3: destination must carry the source content"
    );
    assert!(
        !dir_names(&dir).iter().any(|n| n == "w3-new"),
        "W3: source name must be gone"
    );
    println!("  OK   W3 rename replaces existing file (dest retired)");

    // W4 open handle across rename/unlink. POSIX keeps the fd valid; on
    // Windows this goes through WinFsp sharing semantics. The rename and
    // content integrity are asserted; the unlink-while-open outcome is
    // RECORDED (delete-pending vs sharing violation) and only consistency
    // is asserted after close.
    write_strong(&dir.join("w4.txt"), b"HANDLE", "W4 seed");
    let f = std::fs::File::open(dir.join("w4.txt")).expect("W4 open");
    std::fs::rename(dir.join("w4.txt"), dir.join("w4-renamed")).expect("W4 rename while open");
    let del = std::fs::remove_file(dir.join("w4-renamed"));
    println!("  INFO W4 unlink while handle open: {:?}", del);
    match &del {
        Ok(()) => {
            drop(f);
            // delete-pending: the name disappears once the last handle
            // closes (the WinFsp close path is asynchronous - bounded wait,
            // this is the B4 "eventual visibility" pattern, not a retry of
            // a failed operation).
            let mut gone = false;
            for _ in 0..100 {
                if !dir.join("w4-renamed").exists() {
                    gone = true;
                    break;
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            assert!(
                gone,
                "W4: deleted-while-open name must disappear after close"
            );
            println!("  OK   W4 rename while open; unlink delete-pending, gone after close");
        }
        Err(e) => {
            drop(f);
            assert_eq!(
                std::fs::read(dir.join("w4-renamed")).unwrap(),
                b"HANDLE",
                "W4: refused unlink must keep the content intact"
            );
            let _ = std::fs::remove_file(dir.join("w4-renamed"));
            println!(
                "  OK   W4 rename while open ok; unlink refused by sharing ({}), content intact",
                e
            );
        }
    }

    // W5 case sensitivity: the mount is case-sensitive (WinFsp
    // CaseSensitiveSearch, the design notes section 5). Opening a different
    // case must fail, and the two spellings must coexist as two nodes.
    write_strong(&dir.join("CaseFile.txt"), b"upper", "W5 seed");
    match std::fs::File::open(dir.join("casefile.txt")) {
        Err(e) => println!("  OK   W5 case-sensitive lookup ('casefile.txt' -> {})", e),
        Ok(_) => panic!("W5: mount behaved case-INSENSITIVELY - docs claim case-sensitive"),
    }
    write_strong(&dir.join("casefile.txt"), b"lower", "W5 second");
    let names = dir_names(&dir);
    assert!(
        names.iter().any(|n| n == "CaseFile.txt") && names.iter().any(|n| n == "casefile.txt"),
        "W5: both spellings must coexist, got {names:?}"
    );
    assert_eq!(std::fs::read(dir.join("CaseFile.txt")).unwrap(), b"upper");
    println!("  OK   W5 case-sensitive names coexist (CaseFile.txt / casefile.txt)");

    // W6 timestamps: set mtime through the mount (cb_utimens -> setattr),
    // read it back through cb_getattr; sub-second precision is not kept
    // (node mtime is seconds), so compare at second granularity.
    let p6 = dir.join("w6.txt");
    write_strong(&p6, b"ts", "W6 seed");
    let stamp = std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000);
    std::fs::File::options()
        .write(true)
        .open(&p6)
        .expect("W6 open")
        .set_modified(stamp)
        .expect("W6 set_modified");
    let mt = std::fs::metadata(&p6).expect("W6 stat").modified().unwrap();
    let secs = mt
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert_eq!(
        secs, 1_700_000_000,
        "W6: mtime must round-trip at second granularity"
    );
    // cross-check the same node via the RPC client (server-side truth)
    let (ok, body) = rpc_call(&ctx.rpc_url(), "getattr", "{\"path\":\"/w/w6.txt\"}");
    assert!(ok, "W6: RPC getattr failed: {body}");
    assert!(
        body.contains("\"mtime\":1700000000"),
        "W6: RPC mtime must match the value set through the mount: {body}"
    );
    println!("  OK   W6 mtime round-trips mount->setattr->getattr (mount and RPC agree)");

    // W7 names Win32 pre-normalizes or reserves (trailing dot/space,
    // device names). What the FS actually sees/does is RECORDED; only
    // non-corruption (the directory still lists consistently) is asserted.
    for name in ["w7-dot.", "w7-sp ", "CON"] {
        match std::fs::write(dir.join(name), b"x") {
            Ok(()) => println!("  INFO W7 create {name:?} -> ok"),
            Err(e) => println!("  INFO W7 create {name:?} -> {}", e),
        }
    }
    let names = dir_names(&dir);
    assert!(
        names.contains(&"w6.txt".to_string()),
        "W7: directory must stay consistent"
    );
    println!(
        "  INFO W7 visible names after attempts: {:?}",
        names
            .iter()
            .filter(|n| n.starts_with("w7") || n.eq_ignore_ascii_case("con"))
            .collect::<Vec<_>>()
    );

    // W8 symlinks on Windows: creation from the mount is ENOSYS (the
    // symlink callback is not wired - selected limitation). A link node
    // created OUT OF BAND via RPC is then observed from Windows: readlink
    // is an ENOSYS stub and WinFsp resolves far less of the link machinery
    // than Linux VFS, so the observations are RECORDED as the capability-
    // matrix evidence; only namespace consistency is asserted.
    let (ok, body) = rpc_call(
        &ctx.rpc_url(),
        "symlink",
        "{\"path\":\"/w/w-link\",\"target\":\"w1.bin\"}",
    );
    assert!(ok, "W8: RPC symlink create failed: {body}");
    let (ok, body) = rpc_call(
        &ctx.rpc_url(),
        "symlink",
        "{\"path\":\"/w/w-dangling\",\"target\":\"no-such-target\"}",
    );
    assert!(ok, "W8: RPC dangling symlink create failed: {body}");
    let link = dir.join("w-link");
    match std::fs::symlink_metadata(&link) {
        Ok(m) => println!(
            "  INFO W8 symlink_metadata(w-link): len={} is_symlink={}",
            m.len(),
            m.file_type().is_symlink()
        ),
        Err(e) => println!("  INFO W8 symlink_metadata(w-link) -> {e}"),
    }
    match std::fs::read_link(&link) {
        Ok(t) => println!("  INFO W8 read_link -> {:?}", t.display().to_string()),
        Err(e) => println!("  INFO W8 read_link -> {e} (cb_readlink is an ENOSYS stub)"),
    }
    match std::fs::read(&link) {
        Ok(bytes) => println!(
            "  INFO W8 open through link -> {} bytes (WinFsp resolved it)",
            bytes.len()
        ),
        Err(e) => println!("  INFO W8 open through link -> {e}"),
    }
    match std::fs::metadata(dir.join("w-dangling")) {
        Ok(_) => println!("  INFO W8 dangling link followed to something (unexpected)"),
        Err(e) => println!("  INFO W8 dangling link open -> {e}"),
    }
    // create-link from the mount itself: Win32 CreateSymbolicLinkW (the
    // only way std cannot do this - the windows fs::symlink API was never
    // stabilized). WinFsp's FUSE layer should translate this to our symlink
    // callback, which is deliberately not wired (ENOSYS). Whichever error
    // comes back - ENOSYS from the FS or the privilege error - is recorded.
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateSymbolicLinkW(
            lpsymlinkfilename: *const u16,
            lptargetfilename: *const u16,
            dwflags: u32,
        ) -> u8;
    }
    fn to_wide(s: &str) -> Vec<u16> {
        s.encode_utf16().chain(std::iter::once(0)).collect()
    }
    let rc = unsafe {
        CreateSymbolicLinkW(
            to_wide(dir.join("w-local-link").to_str().unwrap()).as_ptr(),
            to_wide("w1.bin").as_ptr(),
            0,
        )
    };
    if rc == 0 {
        println!(
            "  INFO W8 mount-side symlink create -> {} (last_os_error)",
            std::io::Error::last_os_error()
        );
    } else {
        println!("  INFO W8 mount-side symlink create -> succeeded (Win32-level)");
    }
    let _ = std::fs::remove_file(dir.join("w-local-link"));
    assert!(
        dir_names(&dir).contains(&"w6.txt".to_string()),
        "W8: directory must stay consistent after link observations"
    );
    println!("  OK   W8 out-of-band link nodes observed and recorded (see INFO lines)");

    // W9 rmdir: empty / nonexistent / non-empty / nested chain, plus the
    // post-delete namespace state. Win32 error translation differs from
    // POSIX errno, so exact codes are RECORDED; success/failure and
    // namespace consistency are asserted. The one file seed goes through
    // the strong path (plain close defers its commit on Windows).
    let e9 = dir.join("w9-empty");
    std::fs::create_dir(&e9).expect("W9 mkdir empty target");
    std::fs::remove_dir(&e9).expect("W9 rmdir empty dir");
    assert!(!e9.exists(), "W9: removed empty dir must be gone");
    println!("  OK   W9 rmdir empty dir succeeds, name gone");
    match std::fs::remove_dir(dir.join("w9-never-existed")) {
        Err(e) => println!(
            "  INFO W9 rmdir nonexistent -> {} (raw={:?})",
            e,
            e.raw_os_error()
        ),
        Ok(()) => panic!("W9: rmdir of a nonexistent dir must fail"),
    }
    let ne = dir.join("w9-nonempty");
    std::fs::create_dir(&ne).expect("W9 mkdir nonempty target");
    write_strong(&ne.join("child.txt"), b"keep", "W9 seed");
    match std::fs::remove_dir(&ne) {
        Err(e) => println!(
            "  INFO W9 rmdir non-empty -> {} (raw={:?})",
            e,
            e.raw_os_error()
        ),
        Ok(()) => panic!("W9: rmdir of a non-empty dir must fail"),
    }
    let child = std::fs::read(ne.join("child.txt")).expect("W9 child read-back");
    assert!(
        child == b"keep",
        "W9: refused rmdir must keep the child intact"
    );
    std::fs::remove_file(ne.join("child.txt")).expect("W9 rm child");
    std::fs::remove_dir(&ne).expect("W9 rmdir after emptying");
    println!("  OK   W9 rmdir non-empty refused; emptied dir then removes");
    let na = dir.join("w9-n");
    std::fs::create_dir_all(na.join("b").join("c")).expect("W9 mkdir nested");
    std::fs::remove_dir(na.join("b").join("c")).expect("W9 rmdir c");
    std::fs::remove_dir(na.join("b")).expect("W9 rmdir b");
    std::fs::remove_dir(&na).expect("W9 rmdir a");
    let names = dir_names(&dir);
    assert!(
        !names.iter().any(|n| n.starts_with("w9-")),
        "W9: every w9 name must be gone after the nested teardown, got {names:?}"
    );
    println!("  OK   W9 nested chain (a/b/c) tears down leaf-first, namespace clean");

    println!("    OK W: Windows semantics section complete");
}

impl Ctx {
    fn sections_want(&self, s: &str) -> bool {
        // main owns the section list; kept on Ctx via the constructor above
        self.section_list.iter().any(|x| x == s)
    }

    /// Base URL of the running server (A13 second-client RPC).
    fn rpc_url(&self) -> String {
        self.server_url.clone()
    }
}

fn extract_toml_string(text: &str, key: &str) -> Option<String> {
    for line in text.lines() {
        let t = line.trim();
        if let Some(rest) = t.strip_prefix(&format!("{key} = \"")) {
            return rest.strip_suffix('"').map(|s| s.to_string());
        }
    }
    None
}
