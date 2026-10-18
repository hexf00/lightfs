//! vnext cross-process acceptance (S4.19 §六) against a real object store.
//! Requires MinIO: LIGHTFS_TEST_MINIO=1 (bucket `lightfs-vnext-test` is
//! created if missing; the DB is a fresh temp file per run).
//!
//! Covers: reopen persistence with fixed-version history, branch fork
//! isolation, a crash (exit(9)) between the data plane and the metadata
//! commit with full recovery, the engine-lock single-writer contract,
//! reachability GC, and namespace recovery from objects alone.

use lightfs::config::Config;
use lightfs::error::Result;

const BUCKET: &str = "lightfs-vnext-test";

fn minio_config(db_tag: &str) -> Result<Config> {
    if std::env::var("LIGHTFS_TEST_MINIO").ok().as_deref() != Some("1") {
        return Err(lightfs::error::Error::Config(
            "set LIGHTFS_TEST_MINIO=1".into(),
        ));
    }
    let mut cfg = Config::load_or_defaults(None)?;
    cfg.s3.endpoint =
        std::env::var("LIGHTFS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    cfg.s3.bucket = std::env::var("LIGHTFS_S3_BUCKET").unwrap_or_else(|_| BUCKET.into());
    cfg.metadata.path = std::env::temp_dir()
        .join(format!("lightfs-vnext-{db_tag}-{}.db", std::process::id()))
        .to_string_lossy()
        .to_string();
    Ok(cfg)
}

fn fresh(tag: &str) -> Config {
    let mut cfg = minio_config(tag).expect("MinIO gate");
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", cfg.metadata.path));
    }
    // one bucket per test per run: meta/ is bucket-global, so a leftover
    // root from an earlier run must never meet a fresh DB (the engine
    // fails closed on that today — see init_namespace)
    cfg.s3.bucket = format!("{}-{}-{}", BUCKET, tag, std::process::id());
    lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config())
        .expect("s3 store")
        .ensure_bucket()
        .expect("ensure bucket");
    cfg
}

/// The same stable string S3ObjectStore::backend_id reports (endpoint +
/// bucket) — ledger rows in child modes must name the real backend.
fn backend_id(cfg: &Config) -> String {
    format!(
        "s3://{}/{}",
        cfg.s3.endpoint.trim_end_matches('/'),
        cfg.s3.bucket
    )
}

fn write_file(core: &lightfs::vnext::Core, path: &str, bytes: &[u8]) {
    core.create(path, 0o644).unwrap();
    let h = core.open_write(path, None).unwrap();
    core.write(&h, 0, bytes).unwrap();
    core.commit_write(&h).unwrap();
}

// ---- child entry points for the crash test ----
// The child re-execs THIS test binary with LIGHTFS_VNEXT_CHILD set; each
// mode writes committed state, then dies without cleanup (exit 9).

fn child_main(mode: &str, db: String, bucket: String) -> ! {
    let mut cfg = Config::load_or_defaults(None).expect("cfg");
    cfg.s3.endpoint =
        std::env::var("LIGHTFS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    cfg.s3.bucket = bucket;
    cfg.metadata.path = db;
    // The claim racer must open the engine only AFTER the barrier: the
    // namespace claim runs inside open_engine, and that claim IS the race.
    if mode == "claim" {
        let go = std::env::var("LIGHTFS_VNEXT_GO").expect("go file");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !std::path::Path::new(&go).exists() {
            assert!(std::time::Instant::now() < deadline, "go file never appeared");
            std::thread::sleep(std::time::Duration::from_millis(2));
        }
        match lightfs::engine::open_engine(&cfg) {
            Ok(_eng) => std::process::exit(0),
            Err(e) => {
                eprintln!("claim racer refused: {e}");
                std::process::exit(9);
            }
        }
    }
    let eng = lightfs::engine::open_engine(&cfg).expect("child engine");
    let core = &eng.core;
    match mode {
        // several durable commits, an uncommitted session, and one raw
        // object PUT that never reaches metadata (crashed data plane);
        // then die hard — no drop, no flush, no graceful anything.
        "crash" => {
            for i in 0..5 {
                write_file(core, &format!("/keep-{i}"), format!("committed-{i}").as_bytes());
            }
            core.create("/inflight", 0o644).unwrap();
            let h = core.open_write("/inflight", None).unwrap();
            core.write(&h, 0, b"never-published").unwrap();
            // Two crash flavors of the data plane: (a) a PUT that got its
            // ledger row and died before the metadata transaction —
            // provably unreachable, gc reclaims it; (b) a raw PUT the
            // ledger never saw — gc can NOT prove anything about it and
            // must keep it (fail-safe: never delete the unprovable).
            let ledgered = "objects/999/orphan-ledgered.data";
            core.objects().put(ledgered, b"orphan bytes").unwrap();
            core.store().ledger_object(&backend_id(&cfg), ledgered).unwrap();
            core.objects()
                .put("objects/999/orphan-raw.data", b"raw bytes")
                .unwrap();
            std::process::exit(9);
        }
        // S4.22 kill-window: SIGKILL AFTER a raw PUT, BEFORE any ledger
        // row — the object is unregistered residue; gc must never delete
        // it (the ledger is the deletion domain, not the bucket).
        // S4.24: many large objects, then streaming salvage with a
        // hashing consumer. The parent samples this process's RSS the
        // whole time; accumulation of recovered payloads would show up
        // as RSS tracking the total bytes.
        "salvage_rss" => {
            let total_env = std::env::var("LIGHTFS_SALVAGE_RSS_TOTAL")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(384 * 1024 * 1024);
            let obj_len = 24u64 * 1024 * 1024;
            let n = (total_env / obj_len) as u32;
            let spool = std::env::temp_dir().join(format!("lightfs-salvage-rss-src-{}", std::process::id()));
            {
                use std::io::Write as _;
                let mut f = std::fs::File::create(&spool).unwrap();
                let mut buf = vec![0u8; 1024 * 1024];
                let blen = buf.len();
                for (i, b) in buf.iter_mut().enumerate() {
                    *b = (i % 251) as u8;
                }
                let _ = blen;
                let mut left = obj_len;
                while left > 0 {
                    let chunk = buf.len().min(left as usize);
                    f.write_all(&buf[..chunk]).unwrap();
                    left -= chunk as u64;
                }
            }
            for i in 0..n {
                let mut f = std::fs::File::open(&spool).unwrap();
                core.write_stream(&format!("/rss-{i}.bin"), None, true, &mut f, obj_len)
                    .unwrap();
            }
            let _ = std::fs::remove_file(&spool);
            use sha2::Digest as _;
            let rep = lightfs::vnext::recover::salvage_all_data(
                core.objects().as_ref(),
                &mut |loc: &str, len: u64, r: &mut dyn std::io::Read| {
                    let mut h = sha2::Sha256::new();
                    std::io::copy(r, &mut h)?;
                    assert_eq!(len % obj_len, 0, "unexpected length for {loc}");
                    Ok(())
                },
            )
            .unwrap();
            assert_eq!(rep.recovered.len(), n as usize, "{:?}", rep.rejected);
            // linger briefly so the parent's sampler sees the settled RSS
            std::thread::sleep(std::time::Duration::from_millis(300));
            std::process::exit(0);
        }
        "kill_after_put" => {
            write_file(core, "/keep-0", b"committed-0");
            core.objects()
                .put("objects/raw-kill.data", b"unregistered-residue")
                .unwrap();
            std::process::exit(9);
        }
        // S4.22 kill-window: SIGKILL INSIDE the metadata transaction —
        // SQLite rolls back on restart: neither the ledger row nor the
        // file version may exist, and the engine opens cleanly.
        "kill_in_tx" => {
            write_file(core, "/keep-0", b"committed-0");
            let fid = core.lookup("/keep-0").unwrap().id;
            let be = backend_id(&cfg);
            let _ = core.store().with_tx(|conn| -> lightfs::error::Result<()> {
                lightfs::vnext::store::Store::ledger_object_tx(
                    conn,
                    &be,
                    "objects/in-tx-kill.data",
                )?;
                lightfs::vnext::store::Store::insert_file_version_tx(
                    conn,
                    &lightfs::vnext::store::FileVersionRow {
                        file_id: fid,
                        version: 99,
                        size: 1,
                        mtime: 0,
                        mode: 0o644,
                        object_backend: be.clone(),
                        object_key: "objects/in-tx-kill.data".into(),
                        etag: "never".into(),
                    },
                )?;
                // die with the transaction open
                std::process::exit(9);
            });
            unreachable!();
        }
        // S4.22 kill-window: SIGKILL immediately AFTER a committed write
        // was acked — the commit must survive (no lost committed files).
        "kill_after_commit" => {
            write_file(core, "/durable", b"durable-bytes");
            std::process::exit(9);
        }
        // normal completion: the restart workflow's first act
        "write" => {
            write_file(core, "/wf/a.txt", b"alpha");
            write_file(core, "/wf/b.txt", b"beta");
        }
        // second act: append a new version on top of what act one left
        "rewrite" => {
            assert_eq!(core.read("/wf/a.txt", 0, 64).unwrap(), b"alpha");
            let h = core.open_write("/wf/a.txt", Some(1)).unwrap();
            core.write(&h, 0, b"alpha-v2").unwrap();
            core.commit_write(&h).unwrap();
        }

        other => panic!("unknown child mode {other}"),
    }
    std::process::exit(0);
}

fn run_child(mode: &str, cfg: &Config) -> std::process::ExitStatus {
    let exe = std::env::current_exe().unwrap();
    let mut cmd = std::process::Command::new(exe);
    cmd.env("LIGHTFS_VNEXT_CHILD", format!("{mode}:{}:{}", cfg.metadata.path, cfg.s3.bucket))
        .arg("--exact")
        .arg("zz_child_harness")
        .arg("--nocapture")
        .stdout(std::process::Stdio::null())
        // child panics must be diagnosable: stderr lands in a temp file
        .stderr(std::process::Stdio::from(
            std::fs::File::create(std::env::temp_dir().join(format!("lightfs-vnext-child-{mode}.log")))
                .expect("child log"),
        ));
    cmd.status().expect("spawn child")
}

fn list_all_keys(objects: &dyn lightfs::object_store::ObjectStore) -> Vec<String> {
    let mut out = Vec::new();
    let mut token = None;
    loop {
        let page = objects.list("", token.as_deref(), 1000).unwrap();
        for i in &page.items {
            out.push(i.key.clone());
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    out.sort();
    out
}

#[test]
fn zz_child_harness() {
    // never runs as a test; dispatches to child_main when re-exec'd
    if let Ok(payload) = std::env::var("LIGHTFS_VNEXT_CHILD") {
        let mut it = payload.splitn(3, ':');
        let mode = it.next().unwrap();
        let db = it.next().unwrap().to_string();
        let bucket = it.next().unwrap().to_string();
        child_main(mode, db, bucket);
    }
}

/// S4.20 §五.2: normal writes produce ONLY payload objects — no per-commit
/// meta/ anything (verified against a REAL backend, not just in-memory).
#[test]
fn normal_writes_leave_meta_prefix_empty() {
    let cfg = fresh("payload");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    eng.core.mkdir("/w", 0o755).unwrap();
    for i in 0..20 {
        write_file(&eng.core, &format!("/w/f{i}"), format!("payload-{i}").as_bytes());
    }
    // overwrite one: still only a new payload object
    let h = eng.core.open_write("/w/f0", Some(1)).unwrap();
    eng.core.write(&h, 0, b"payload-0-v2").unwrap();
    eng.core.commit_write(&h).unwrap();

    let objects = eng.core.objects();
    let meta = list_all_keys(objects.as_ref())
        .into_iter()
        .filter(|k| k.starts_with("meta/"))
        .count();
    assert_eq!(meta, 0, "per-commit meta/ objects are gone (S4.20)");
    let ns: Vec<_> = list_all_keys(objects.as_ref())
        .into_iter()
        .filter(|k| k.starts_with("namespace/"))
        .collect();
    assert_eq!(ns.len(), 1, "exactly the ownership marker: {ns:?}");
    // 21 payloads (20 files + 1 overwritten v2)
    let payloads = list_all_keys(objects.as_ref())
        .into_iter()
        .filter(|k| k.starts_with("objects/"))
        .count();
    assert_eq!(payloads, 21, "one immutable payload per committed version");
}

/// S4.20 §五.8: a DIFFERENT database opening the same bucket is refused
/// at the namespace marker — never co-rooted, never silent.
#[test]
fn second_database_on_same_bucket_is_refused() {
    let cfg = fresh("shared");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    write_file(&eng.core, "/owned", b"by the first database");
    drop(eng);

    // a fresh DB in a fresh file, same bucket: refused at open
    let mut intruder = cfg.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        intruder.metadata.path = std::env::temp_dir()
            .join(format!("lightfs-vnext-intruder-{}.db", std::process::id()))
            .to_string_lossy()
            .to_string();
        let _ = std::fs::remove_file(&intruder.metadata.path);
        let _ = tx.send(lightfs::engine::open_engine(&intruder));
    });
    // the intruder must fail, and it must be the namespace refusal
    for _ in 0..600 {
        if let Ok(res) = rx.try_recv() {
            let err = res.err().expect("intruder engine must be refused");
            let msg = format!("{err}");
            assert!(msg.contains("another namespace") || msg.contains("co-root"),
                    "unexpected error: {msg}");
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    panic!("intruder open neither succeeded nor failed within 30s");
}

/// S4.20 §五.6: GC cannot run without SQLite — a corrupted database file
/// fails the store open, so there is no Core to collect with. The fail-
/// closed direction: no database, no reachability proof, no deletions.
#[test]
fn corrupted_sqlite_refuses_gc_entry() {
    let cfg = fresh("gcdeny");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    write_file(&eng.core, "/g", b"guard");
    drop(eng);
    for suffix in ["-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", cfg.metadata.path));
    }
    std::fs::write(&cfg.metadata.path, b"this is not a sqlite database at all").unwrap();
    // the engine lock file survives but the DB is garbage: opening the
    // store must fail loudly (engine lock + marker checks never reached)
    let err = lightfs::engine::open_engine(&cfg).err().expect("corrupt db must refuse");
    let _ = err; // message shape asserted loosely: any refusal is fine
}

/// S4.20 §五.5 (network leg): wrong credentials surface as an explicit
/// error from the salvage entry point, not an empty report.
#[test]
fn salvage_with_unusable_backend_errors_loudly() {
    let cfg = fresh("perms");
    let mut bad = cfg.clone();
    bad.s3.access_key = "not-a-valid-key".into();
    let store = lightfs::object_store::s3::S3ObjectStore::new(bad.s3_config()).unwrap();
    let err = match lightfs::vnext::recover::salvage_all_data(
        &store,
        &mut |_: &str, _: u64, _: &mut dyn std::io::Read| Ok(()),
    ) {
        Err(e) => e,
        Ok(_) => panic!("list with bad credentials must error, not return empty"),
    };
    let _ = format!("{err}"); // explicit failure; shape owned by the backend
}

#[test]
fn restart_and_fixed_version_history() {
    let mut cfg = match minio_config("restart") {
        Ok(c) => c,
        Err(_) => return,
    };
    // per-run bucket for the same reason as fresh(); DB starts fresh
    cfg.s3.bucket = format!("{}-restart-{}", BUCKET, std::process::id());
    lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config())
        .unwrap()
        .ensure_bucket()
        .unwrap();
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", cfg.metadata.path));
    }

    // act one (this process): 120 commits, remember every version's bytes
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    eng.core.mkdir("/hot", 0o755).unwrap();
    let mut history: Vec<(i64, Vec<u8>)> = Vec::new();
    for i in 0..120u32 {
        let path = "/hot/file";
        if i == 0 {
            eng.core.create(path, 0o644).unwrap();
        }
        let bytes = format!("generation-{i:03}").into_bytes();
        let expected = if i == 0 { None } else { Some(i as i64) };
        let h = eng.core.open_write(path, expected).unwrap();
        eng.core.write(&h, 0, &bytes).unwrap();
        let node = eng.core.commit_write(&h).unwrap();
        assert_eq!(node.version, i as i64 + 1);
        history.push((node.version, bytes));
    }
    drop(eng);

    // act two: a FRESH process sees every version, byte-exact (§六.1):
    // each commit's pinned tree still returns the exact generation
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    let commits = eng.core.store().all_commits().unwrap();
    let mut checked = 0usize;
    for commit in &commits {
        if let Ok(node) = eng.core.lookup_at(commit.id, "/hot/file") {
            if node.version == 0 {
                continue; // the create commit: an empty v0, no content yet
            }
            let bytes = &history[(node.version - 1) as usize].1;
            assert_eq!(
                eng.core.read_at(commit.id, "/hot/file", 0, 256).unwrap(),
                *bytes,
                "version {} byte-drifted across restart",
                node.version
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 120, "every commit carried one version of /hot/file");
    assert_eq!(
        eng.core.read("/hot/file", 0, 256).unwrap(),
        b"generation-119"
    );
}

#[test]
fn crash_between_data_and_metadata_recovers() {
    let cfg = fresh("crash");
    // child: 5 durable commits + 1 uncommitted session + 1 raw orphan; exit(9)
    let status = run_child("crash", &cfg);
    assert_eq!(status.code(), Some(9), "child must die hard");

    // restart: every COMMITTED write survived atomically, the uncommitted
    // version is nowhere, and the engine lock (released by the dead
    // process's exit) re-acquires cleanly.
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    for i in 0..5 {
        let path = format!("/keep-{i}");
        assert_eq!(
            eng.core.read(&path, 0, 64).unwrap(),
            format!("committed-{i}").into_bytes(),
            "{path} lost by the crash"
        );
    }
    // The create itself committed (an empty v0 is legal metadata); what
    // must NOT be visible is the buffered CONTENT: read refuses (no
    // object was ever referenced) — no half-published version.
    let node = eng.core.lookup("/inflight").unwrap();
    assert_eq!(node.version, 0, "no content version may exist");
    assert!(eng.core.read("/inflight", 0, 64).is_err(),
        "uncommitted content must not be visible after restart");

    // reachability GC: the LEDGERED orphan (provably unreachable) goes,
    // the raw one stays (gc never deletes what it cannot prove dead),
    // every referenced object stays
    let objects = eng.core.objects();
    assert!(objects.head("objects/999/orphan-ledgered.data").is_ok());
    let report = lightfs::vnext::gc::collect(&eng.core).unwrap();
    assert_eq!(report.deleted, 1, "exactly the ledgered orphan is reclaimed");
    assert!(objects.head("objects/999/orphan-ledgered.data").is_err());
    assert!(
        objects.head("objects/999/orphan-raw.data").is_ok(),
        "unledgered objects must survive gc (fail-safe)"
    );
    for i in 0..5 {
        assert_eq!(eng.core.read(&format!("/keep-{i}"), 0, 64).unwrap(),
            format!("committed-{i}").into_bytes());
    }

    // SQLite-independent content salvage (read-only): the five keeps are
    // restoration candidates with byte-exact payloads; both raw orphans
    // (written WITHOUT envelopes) are explicitly rejected, never served.
    let snapshot_before = list_all_keys(objects.as_ref());
    let mut keep_payloads: Vec<Vec<u8>> = Vec::new();
    let report = lightfs::vnext::recover::salvage_all_data(objects.as_ref(), &mut |loc: &str, _len: u64, r: &mut dyn std::io::Read| {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(r, &mut buf)?;
        if !loc.starts_with("objects/999/") {
            keep_payloads.push(buf);
        }
        Ok(())
    })
    .unwrap();
    let snapshot_after = list_all_keys(objects.as_ref());
    assert_eq!(snapshot_before, snapshot_after, "salvage must not mutate OSS");
    keep_payloads.sort();
    let expected: Vec<Vec<u8>> = (0..5).map(|i| format!("committed-{i}").into_bytes()).collect();
    assert_eq!(keep_payloads, expected, "byte-exact salvage of the keeps");
    // exactly one rejection: the RAW orphan (gc already reclaimed the
    // ledgered one — envelopes it never had are reported, never served)
    assert_eq!(report.rejected.len(), 1, "{:?}", report.rejected);
    assert_eq!(report.rejected[0].locator, "objects/999/orphan-raw.data");
    assert!(report.rejected[0].reason.contains("unreadable"));
    // salvage does NOT judge committed vs orphan: everything verified is
    // a candidate (the five keeps + nothing else — inflight never PUT)
    assert_eq!(report.recovered.len(), 5);
}

#[test]
fn single_writer_engine_lock_is_cross_process() {
    let cfg = fresh("single");
    let _eng = lightfs::engine::open_engine(&cfg).unwrap();
    // a SECOND process on the same DB must be refused, loudly
    let status = run_child("write", &cfg);
    assert_ne!(status.code(), Some(0), "second engine must fail, not co-write");
}

#[test]
fn branch_isolation_and_full_restart_workflow() {
    let cfg = fresh("wf");
    // act one: write, fork a branch at the head
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    eng.core.mkdir("/wf", 0o755).unwrap();
    write_file(&eng.core, "/wf/a.txt", b"alpha");
    let fork = eng.core.store().head("main").unwrap().unwrap().commit_id;
    eng.core.store().create_ref("review", fork).unwrap();
    drop(eng);

    // act two (fresh process): mainline moves; branch must not
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    let h = eng.core.open_write("/wf/a.txt", Some(1)).unwrap();
    eng.core.write(&h, 0, b"alpha-v2").unwrap();
    eng.core.commit_write(&h).unwrap();
    write_file(&eng.core, "/wf/only-main.txt", b"main-only");
    assert_eq!(
        eng.core.read_at(fork, "/wf/a.txt", 0, 64).unwrap(),
        b"alpha",
        "branch fork is zero-copy and frozen"
    );
    assert!(eng.core.lookup_at(fork, "/wf/only-main.txt").is_err());
    drop(eng);

    // act three (fresh process): everything above still holds
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    assert_eq!(eng.core.read("/wf/a.txt", 0, 64).unwrap(), b"alpha-v2");
    assert_eq!(eng.core.read("/wf/only-main.txt", 0, 64).unwrap(), b"main-only");
    assert_eq!(eng.core.read_at(fork, "/wf/a.txt", 0, 64).unwrap(), b"alpha");

    // conflict is EXPLICIT at file level: a stale-base write is refused
    let h = eng.core.open_write("/wf/a.txt", Some(1)).unwrap();
    eng.core.write(&h, 0, b"stale-base").unwrap();
    let err = eng.core.commit_write(&h).unwrap_err();
    assert!(matches!(err, lightfs::error::Error::Conflict { .. }), "{err}");

    // cleanup
    for p in ["/wf/a.txt", "/wf/only-main.txt"] {
        eng.core.unlink(p).unwrap();
    }
    eng.core.rmdir("/wf").unwrap();
}

/// S4.21 §四: TWO real processes, TWO independent SQLite databases, ONE
/// unclaimed bucket, released simultaneously through a file barrier. The
/// atomic namespace claim (S3 conditional PUT, If-None-Match: *) must
/// yield exactly one owner; the loser is refused, never co-rooted.
#[test]
fn namespace_claim_race_two_processes_exactly_one_winner() {
    let cfg = fresh("claimrace");
    let endpoint =
        std::env::var("LIGHTFS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let go = std::env::temp_dir().join(format!(
        "lightfs-go-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&go);
    let dbs: Vec<String> = (0..2)
        .map(|i| format!("{}-claim{i}", cfg.metadata.path))
        .collect();
    for d in &dbs {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{d}{suffix}"));
        }
    }
    let exe = std::env::current_exe().unwrap();
    let mut kids: Vec<(std::process::Child, usize)> = Vec::new();
    for (i, db) in dbs.iter().enumerate() {
        let errlog = std::fs::File::create(
            std::env::temp_dir().join(format!("lightfs-claim-{}-{i}.log", std::process::id())),
        )
        .expect("racer log");
        let mut cmd = std::process::Command::new(&exe);
        cmd.env("LIGHTFS_VNEXT_CHILD", format!("claim:{db}:{}", cfg.s3.bucket))
            .env("LIGHTFS_VNEXT_GO", &go)
            .env("LIGHTFS_S3_ENDPOINT", &endpoint)
            .arg("--exact")
            .arg("zz_child_harness")
            .arg("--nocapture")
            .stdout(std::process::Stdio::null())
            .stderr(errlog);
        kids.push((cmd.spawn().expect("spawn racer"), i));
    }
    // Release both racers at the same instant.
    std::fs::write(&go, b"go").unwrap();
    let mut outcome: Vec<(usize, i32)> = Vec::new();
    for (kid, i) in &mut kids {
        let st = kid.wait().expect("racer wait");
        let code = st.code().unwrap_or(-1);
        outcome.push((*i, code));
        assert!(
            code == 0 || code == 9,
            "racer {i} exited {code}: neither winner nor clean loser (see lightfs-claim log)"
        );
    }
    let winners: Vec<usize> = outcome.iter().filter(|(_, c)| *c == 0).map(|(i, _)| *i).collect();
    assert_eq!(
        winners.len(),
        1,
        "exactly one process may claim the namespace: {outcome:?}"
    );
    let winner_db = &dbs[winners[0]];
    let loser_db = &dbs[1 - winners[0]];

    // The winner's database reopens against the same bucket and writes.
    let mut wcfg = cfg.clone();
    wcfg.metadata.path = winner_db.clone();
    let eng = lightfs::engine::open_engine(&wcfg).expect("winner reopens");
    write_file(&eng.core, "/claimed", b"by-the-winner");
    assert_eq!(eng.core.read("/claimed", 0, 64).unwrap(), b"by-the-winner");
    drop(eng);
    // The loser's database stays refused (marker mismatch or foreign
    // marker), on EVERY subsequent open.
    let mut lcfg = cfg.clone();
    lcfg.metadata.path = loser_db.clone();
    let err = lightfs::engine::open_engine(&lcfg)
        .map(|_| ())
        .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("namespace mismatch") || msg.contains("another namespace's marker"),
        "{msg}"
    );
}

/// S4.22 §四.5: SIGKILL in each failure window (after PUT / inside the
/// transaction / after commit) loses NO committed file and lets gc
/// reclaim NOTHING that was not provably dead.
#[test]
fn s422_kill_windows_no_loss_no_wrong_reclaim() {
    // Window A: killed after the PUT, before any registration.
    {
        let cfg = fresh("kput");
        let st = run_child("kill_after_put", &cfg);
        assert_eq!(st.code(), Some(9), "child must die hard");
        let eng = lightfs::engine::open_engine(&cfg).unwrap();
        assert_eq!(eng.core.read("/keep-0", 0, 64).unwrap(), b"committed-0");
        // Unregistered residue: gc keeps it (deletion domain = ledger).
        let objects = eng.core.objects();
        let before = list_all_keys(objects.as_ref());
        let rep = lightfs::vnext::gc::collect(&eng.core).unwrap();
        assert_eq!(rep.deleted, 0, "unregistered object must NOT be reclaimed");
        assert_eq!(
            list_all_keys(objects.as_ref()),
            before,
            "gc changed nothing in window A"
        );
        // Register it (as the crash WOULD have, had the process lived) —
        // now it is provably unreachable and gc reclaims exactly it.
        eng.core
            .store()
            .ledger_object(&backend_id(&cfg), "objects/raw-kill.data")
            .unwrap();
        let rep = lightfs::vnext::gc::collect(&eng.core).unwrap();
        assert_eq!(rep.deleted, 1);
        assert!(objects.get("objects/raw-kill.data", None).is_err());
    }
    // Window B: killed INSIDE the metadata transaction.
    {
        let cfg = fresh("ktx");
        let st = run_child("kill_in_tx", &cfg);
        assert_eq!(st.code(), Some(9));
        let eng = lightfs::engine::open_engine(&cfg).unwrap();
        // Transaction rolled back: no ledger row, no phantom version.
        let known = eng.core.store().known_objects().unwrap();
        assert!(
            !known.iter().any(|(_, l)| l == "objects/in-tx-kill.data"),
            "in-transaction registration must not survive: {known:?}"
        );
        let node = eng.core.lookup("/keep-0").unwrap();
        assert_eq!(eng.core.read("/keep-0", 0, 64).unwrap(), b"committed-0");
        assert_eq!(node.version, 1, "no phantom version 99");
    }
    // Window C: killed immediately after a committed write was acked.
    {
        let cfg = fresh("kcommit");
        let st = run_child("kill_after_commit", &cfg);
        assert_eq!(st.code(), Some(9));
        let eng = lightfs::engine::open_engine(&cfg).unwrap();
        assert_eq!(eng.core.read("/durable", 0, 64).unwrap(), b"durable-bytes");
        // gc is safe right after restart: the committed object stays.
        lightfs::vnext::gc::collect(&eng.core).unwrap();
        assert_eq!(eng.core.read("/durable", 0, 64).unwrap(), b"durable-bytes");
    }
}

// ---- S4.23: streamed envelope v2, bounded-memory upload, integrity ----

/// Deterministic patterned spool file on DISK (the test never holds the
/// whole payload in memory while writing it). Byte at position i is
/// `(i % 251) as u8` — the same law the verifiers below re-check.
fn spool_file(tag: &str, len: u64) -> std::path::PathBuf {
    use std::io::Write as _;
    let p = std::env::temp_dir().join(format!("lightfs-s423-{tag}-{}.bin", std::process::id()));
    let mut f = std::fs::File::create(&p).unwrap();
    let mut written = 0u64;
    let mut buf = vec![0u8; 1024 * 1024];
    while written < len {
        let n = buf.len().min((len - written) as usize);
        for (i, b) in buf[..n].iter_mut().enumerate() {
            *b = ((written as usize + i) % 251) as u8;
        }
        f.write_all(&buf[..n]).unwrap();
        written += n as u64;
    }
    p
}

fn pattern_byte(pos: u64) -> u8 {
    (pos % 251) as u8
}

fn check_pattern(bytes: &[u8], base: u64) {
    for (i, b) in bytes.iter().enumerate() {
        assert_eq!(*b, pattern_byte(base + i as u64), "byte at {}", base + i as u64);
    }
}

fn write_spooled(core: &lightfs::vnext::Core, path: &str, src: &std::path::Path, create: bool, expected: Option<i64>) -> lightfs::metadata::Node {
    let mut f = std::fs::File::open(src).unwrap();
    let len = f.metadata().unwrap().len();
    core.write_stream(path, expected, create, &mut f, len).unwrap()
}

#[test]
fn s423_streamed_large_files_roundtrip_ranges_restart() {
    let cfg = fresh("s423large");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();

    // empty and 1-byte files through the SAME streamed server path
    let e0 = spool_file("s423e0", 0);
    write_spooled(&eng.core, "/empty", &e0, true, None);
    assert!(eng.core.read("/empty", 0, 16).unwrap().is_empty());
    let e1 = spool_file("s423e1", 1);
    write_spooled(&eng.core, "/one", &e1, true, None);
    assert_eq!(eng.core.read("/one", 0, 16).unwrap(), vec![pattern_byte(0)]);

    // 64 MiB: single streaming pass, disk spool -> put_stream, multipart
    // parts on the S3 side (encoded > SMALL_PUT_MAX).
    let big = spool_file("s423big", 64 * 1024 * 1024);
    let node = write_spooled(&eng.core, "/big.bin", &big, true, None);
    assert_eq!(node.size, 64 * 1024 * 1024);
    assert_eq!(node.version, 1);

    // full read: footer checksum verified inside the product
    let full = eng.core.read("/big.bin", 0, 64 * 1024 * 1024).unwrap();
    assert_eq!(full.len(), 64 * 1024 * 1024);
    for base in [0u64, 1, 8 * 1024 * 1024 - 1, 8 * 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024 - 1024] {
        check_pattern(&full[base as usize..base as usize + 512], base);
    }
    drop(full);

    // range reads: cold (offset 0, one GET) and deep (offset > 0, header
    // GET + window GET); last byte; clip at end; past-the-end empty.
    assert_eq!(eng.core.read("/big.bin", 0, 4096).unwrap().len(), 4096);
    let mid = eng.core.read("/big.bin", 33 * 1024 * 1024 + 7, 1000).unwrap();
    check_pattern(&mid, 33 * 1024 * 1024 + 7);
    let last = eng.core.read("/big.bin", 64 * 1024 * 1024 - 1, 10).unwrap();
    assert_eq!(last, vec![pattern_byte(64 * 1024 * 1024 - 1)]);
    assert!(eng.core.read("/big.bin", 64 * 1024 * 1024, 10).unwrap().is_empty());

    // overwrite through the same streamed path (CAS on version 1)
    let one_mib = spool_file("s423mib", 1024 * 1024);
    let n2 = write_spooled(&eng.core, "/big.bin", &one_mib, false, Some(1));
    assert_eq!(n2.version, 2);
    assert_eq!(n2.size, 1024 * 1024);
    let over = eng.core.read("/big.bin", 0, 1024 * 1024).unwrap();
    check_pattern(&over, 0);

    // restart: a fresh engine reads the streamed object back
    drop(eng);
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    let back = eng.core.read("/big.bin", 0, 1024 * 1024).unwrap();
    check_pattern(&back, 0);
    assert_eq!(eng.core.read("/one", 0, 16).unwrap(), vec![pattern_byte(0)]);

    // multipart boundary: encoded 8 MiB exactly (single PUT) vs one byte
    // more (multipart lifecycle) both round-trip byte-exactly
    for (tag, len) in [("s423b8m", 8 * 1024 * 1024u64), ("s423b8m1", 8 * 1024 * 1024 + 1)] {
        let p = spool_file(tag, len);
        let path = format!("/{tag}.bin");
        write_spooled(&eng.core, &path, &p, true, None);
        let mid = eng.core.read(&path, len / 2, 4096).unwrap();
        check_pattern(&mid, len / 2);
        let tail = eng.core.read(&path, len - 3, 100).unwrap();
        check_pattern(&tail, len - 3);
    }

    // salvage decodes and VERIFIES streamed objects; the big v2 objects
    // land in `recovered` with their payload lengths (bytes stream once
    // through the sink — nothing accumulates in the report)
    let mut salvaged_bytes = 0u64;
    let rep = lightfs::vnext::recover::salvage_all_data(
        eng.core.objects().as_ref(),
        &mut |_loc: &str, len: u64, r: &mut dyn std::io::Read| {
            std::io::copy(r, &mut std::io::sink())?;
            salvaged_bytes += len;
            Ok(())
        },
    )
    .unwrap();
    let locators: Vec<(String, u64)> = rep
        .recovered
        .iter()
        .map(|r| (r.locator.clone(), r.payload_len))
        .collect();
    assert!(
        locators.iter().any(|(l, n)| l.ends_with(".data") && *n == 64 * 1024 * 1024),
        "64 MiB object must salvage: {locators:?}"
    );
    assert!(locators.iter().any(|(_, n)| *n == 8 * 1024 * 1024 + 1));
    // the empty streamed object decodes and verifies too (logical_len 0)
    assert!(locators.iter().any(|(_, n)| *n == 0), "{locators:?}");
    // every recovered byte streamed exactly once through the sink
    assert_eq!(
        salvaged_bytes,
        locators.iter().map(|(_, n)| *n).sum::<u64>(),
        "sink must observe every verified payload exactly once"
    );

    for p in [&e0, &e1, &big, &one_mib] {
        let _ = std::fs::remove_file(p);
    }
}

#[test]
fn s423_damaged_and_legacy_objects_fail_closed() {
    let cfg = fresh("s423corrupt");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    write_file(&eng.core, "/good", b"good-bytes");

    use lightfs::object_store::ObjectStore as _;
    let raw = lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config()).unwrap();
    // a payload-damaged v2 object: refused with the checksum naming itself
    let mut damaged = lightfs::vnext::envelope::seal(lightfs::vnext::envelope::ObjKind::Data, b"corrupt-me");
    let last = damaged.len() - 1;
    damaged[last] ^= 0xff;
    raw.put("objects/zz-damaged.data", &damaged).unwrap();
    // a pre-v2 (LF1) object: named as unsupported, never parsed as bytes
    let mut legacy = vec![0u8; 64];
    legacy[0..4].copy_from_slice(b"LF1\n");
    raw.put("objects/zz-legacy.data", &legacy).unwrap();

    let e1 = eng
        .core
        .read_full_verified("objects/zz-damaged.data", "")
        .expect_err("damaged object refused")
        .to_string();
    assert!(e1.contains("sha256 footer mismatch"), "{e1}");
    let e2 = eng
        .core
        .read_full_verified("objects/zz-legacy.data", "")
        .expect_err("legacy object refused")
        .to_string();
    assert!(e2.contains("unsupported envelope format LF1"), "{e2}");

    // salvage reports both as rejected with the reason; the good object
    // is recovered — no forged successes
    let rep = lightfs::vnext::recover::salvage_all_data(
        eng.core.objects().as_ref(),
        &mut |_: &str, _: u64, r: &mut dyn std::io::Read| {
            std::io::copy(r, &mut std::io::sink())?;
            Ok(())
        },
    )
    .unwrap();
    let rejected: Vec<String> = rep.rejected.iter().map(|r| r.reason.clone()).collect();
    assert!(rejected.iter().any(|r| r.contains("sha256 footer mismatch")), "{rejected:?}");
    assert!(rejected.iter().any(|r| r.contains("unsupported envelope format LF1")), "{rejected:?}");
    assert!(
        rep.recovered.iter().any(|r| r.payload_len == 10),
        "{:?}",
        rep.recovered
    );
}

// ---- S4.24: bounded full reads, read_at, salvage streaming, temp hygiene ----

fn temp_residue() -> usize {
    std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_name().to_string_lossy().starts_with("lightfs-read-"))
        .count()
}

#[test]
fn s424_full_read_streamed_verified_and_fail_closed() {
    let cfg = fresh("s424full");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();

    // 64 MiB streamed write, then a WHOLE-file read through the server
    // entry (open_read): must route to the footer-verified streaming
    // decode and publish the staged file.
    let big = spool_file("s424big", 64 * 1024 * 1024);
    write_spooled(&eng.core, "/big.bin", &big, true, None);
    let cid = eng.core.store().head("main").unwrap().unwrap().commit_id;
    let (mut rd, len) = eng.core.open_read("/big.bin", 0, 64 * 1024 * 1024).unwrap();
    assert_eq!(len, 64 * 1024 * 1024);
    let mut got = std::io::BufReader::with_capacity(256 * 1024, rd.as_mut());
    let mut buf = Vec::new();
    std::io::Read::read_to_end(&mut got, &mut buf).unwrap();
    assert_eq!(buf.len(), 64 * 1024 * 1024);
    check_pattern(&buf[..1024], 0);
    check_pattern(&buf[32 * 1024 * 1024..32 * 1024 * 1024 + 512], 32 * 1024 * 1024);
    check_pattern(&buf[buf.len() - 512..], 64 * 1024 * 1024 - 512);
    drop(buf);

    // over-read clamp is still a whole-file request (same verified path)
    let (_, len2) = eng.core.open_read("/big.bin", 0, 128 * 1024 * 1024).unwrap();
    assert_eq!(len2, 64 * 1024 * 1024);

    // partial reads keep the range contract: exact window, no more
    let (mut rr, rlen) = eng.core.open_read("/big.bin", 33 * 1024 * 1024 + 7, 1000).unwrap();
    let mut win = Vec::new();
    std::io::Read::read_to_end(&mut rr, &mut win).unwrap();
    assert_eq!(rlen, 1000);
    check_pattern(&win, 33 * 1024 * 1024 + 7);

    // read_at (pinned history): full verification preserved, range out
    let at = eng.core.read_at(cid, "/big.bin", 1000, 512).unwrap();
    check_pattern(&at, 1000);

    // damage the stored object: whole-file reads must FAIL CLOSED with a
    // named corruption error, and no staged residue may survive
    use lightfs::object_store::ObjectStore as _;
    let key = {
        let known = eng.core.store().known_objects().unwrap();
        known.iter().find(|(_, l)| l.ends_with(".data")).unwrap().1.clone()
    };
    let raw = lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config()).unwrap();
    let cur = raw.get(&key, None).unwrap();
    let mut damaged = cur.data.clone();
    let mid = HEADER_OR_PAYLOAD_POS;
    damaged[mid] ^= 0xff;
    raw.put(&key, &damaged).unwrap();
    let before = temp_residue();
    // Re-PUT damage necessarily changes the backend validator, so the
    // SAME-RESPONSE etag check fires first — itself part of the required
    // fail-closed matrix (the footer-mismatch arm is pinned by the
    // envelope decode tests). Both names are hard failures.
    let err = eng
        .core
        .open_read("/big.bin", 0, 64 * 1024 * 1024)
        .err()
        .expect("damaged object must fail whole-file read")
        .to_string();
    assert!(
        err.contains("recorded etag") || err.contains("sha256 footer mismatch"),
        "{err}"
    );
    assert!(eng.core.read_at(cid, "/big.bin", 0, 16).is_err(), "pinned read must refuse too");
    assert_eq!(temp_residue(), before, "failed verification leaves no staging file");
}

const HEADER_OR_PAYLOAD_POS: usize = 20 + 4096;

#[test]
fn s424_salvage_rss_is_object_bounded_not_total_bounded() {
    let cfg = fresh("s424rss");
    let exe = std::env::current_exe().unwrap();
    let mut cmd = std::process::Command::new(exe);
    cmd.env("LIGHTFS_VNEXT_CHILD", format!("salvage_rss:{}:{}", cfg.metadata.path, cfg.s3.bucket))
        .env("LIGHTFS_S3_ENDPOINT", std::env::var("LIGHTFS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into()))
        .env("LIGHTFS_SALVAGE_RSS_TOTAL", (384u64 * 1024 * 1024).to_string())
        .arg("--exact")
        .arg("zz_child_harness")
        .arg("--nocapture")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut child = cmd.spawn().expect("spawn salvage child");
    // sample the child's RSS for its whole life
    let mut peak_kb = 0u64;
    loop {
        match child.try_wait().expect("wait") {
            Some(_) => break,
            None => {
                if let Ok(out) = std::process::Command::new("ps")
                    .args(["-o", "rss=", "-p", &child.id().to_string()])
                    .output()
                {
                    if let Ok(txt) = String::from_utf8(out.stdout) {
                        if let Ok(kb) = txt.trim().split_whitespace().next().unwrap_or("0").parse::<u64>() {
                            peak_kb = peak_kb.max(kb);
                        }
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(40));
            }
        }
    }
    let peak_mb = peak_kb as f64 / 1024.0;
    eprintln!("salvage child peak RSS: {peak_mb:.1} MB for 384 MiB salvaged");
    // 384 MiB of recovered payloads never exists in memory: the peak is
    // the child's baseline plus single-object buffers, not the total.
    assert!(peak_kb < 220 * 1024, "salvage accumulated: peak {peak_mb:.1} MB");
}

#[test]
fn s424_empty_file_and_dropped_reader_cleanup() {
    let cfg = fresh("s424empty");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    let e0 = spool_file("s424e0", 0);
    write_spooled(&eng.core, "/empty", &e0, true, None);
    // empty file: recorded_size == 0 -> range path, empty answer
    let (mut rd, len) = eng.core.open_read("/empty", 0, 16).unwrap();
    let mut buf = Vec::new();
    std::io::Read::read_to_end(&mut rd, &mut buf).unwrap();
    assert_eq!(len, 0);
    assert!(buf.is_empty());
    drop(rd);

    // dropping a published verified reader deletes its staging file
    let one = spool_file("s424one", 1);
    write_spooled(&eng.core, "/one", &one, true, None);
    let before = temp_residue();
    let (mut rd, _) = eng.core.open_read("/one", 0, 1).unwrap();
    std::io::Read::read_exact(&mut rd, &mut [0u8; 1]).unwrap();
    drop(rd);
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(temp_residue() <= before, "staging file must be deleted on drop");
}

#[test]
fn s424_salvage_streaming_many_objects_bounded_report() {
    let cfg = fresh("s424salvage");
    let eng = lightfs::engine::open_engine(&cfg).unwrap();
    // 12 files of 64 KiB each through the streamed write path
    for i in 0..12u32 {
        let p = spool_file("s424many", 64 * 1024);
        let name = format!("/m{i}.bin");
        write_spooled(&eng.core, &name, &p, true, None);
        let _ = std::fs::remove_file(&p);
    }
    // one raw unenveloped object: rejected, never served
    use lightfs::object_store::ObjectStore as _;
    lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config())
        .unwrap()
        .put("objects/999/raw.data", &[0u8; 64 * 1024])
        .unwrap();

    let mut hashed: Vec<(String, u64, u64)> = Vec::new();
    let before = temp_residue();
    let rep = lightfs::vnext::recover::salvage_all_data(
        eng.core.objects().as_ref(),
        &mut |loc: &str, len: u64, r: &mut dyn std::io::Read| {
            // consumer hashes instead of collecting: per-object memory only
            use sha2::{Digest as _, Sha256};
            let mut h = Sha256::new();
            std::io::copy(r, &mut h)?;
            hashed.push((loc.to_string(), len, h.finalize()[0] as u64));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(rep.scanned, 13);
    assert_eq!(rep.recovered.len(), 12, "{:?}", rep.rejected);
    assert_eq!(rep.rejected.len(), 1);
    assert!(rep.rejected[0].reason.contains("bad magic"), "{:?}", rep.rejected);
    assert_eq!(hashed.len(), 12);
    assert!(hashed.iter().all(|(_, l, _)| *l == 64 * 1024));
    assert!(rep.recovered.iter().all(|i| i.payload_len == 64 * 1024));
    std::thread::sleep(std::time::Duration::from_millis(50));
    assert!(temp_residue() <= before, "salvage staging files must be cleaned");
}
