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
    if ctx.sections_want("a") && cfg!(unix) {
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
}

/// Minimal raw-HTTP RPC used by A13 to act as a second (out-of-band)
/// client while the mount under test stays connected.
fn rpc_post(server_url: &str, method: &str, params: &str) {
    use std::io::{Read, Write};
    let addr = server_url.trim_start_matches("http://");
    let mut s = std::net::TcpStream::connect(addr).expect("A13 rpc connect");
    let body = format!("{{\"id\":1,\"method\":\"{method}\",\"params\":{params}}}");
    let req = format!(
        "POST /rpc HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    s.write_all(req.as_bytes()).expect("A13 rpc write");
    let mut resp = String::new();
    s.read_to_string(&mut resp).expect("A13 rpc read");
    assert!(
        resp.contains("\"ok\":true"),
        "A13 rpc {method} failed: {resp}"
    );
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

    println!(
        "=== ACCEPTANCE PASSED in {:.1}s ===",
        t0.elapsed().as_secs_f64()
    );
    // Drop kills remaining children.
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
