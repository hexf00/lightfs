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
            core.store().ledger_object(ledgered).unwrap();
            core.objects()
                .put("objects/999/orphan-raw.data", b"raw bytes")
                .unwrap();
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

    use lightfs::object_store::ObjectStore as _;
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
    let err = lightfs::vnext::recover::salvage_all_data(&store)
        .err()
        .expect("list with bad credentials must error, not return empty");
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
    let report = lightfs::vnext::recover::salvage_all_data(objects.as_ref()).unwrap();
    let snapshot_after = list_all_keys(objects.as_ref());
    assert_eq!(snapshot_before, snapshot_after, "salvage must not mutate OSS");
    let mut keep_payloads: Vec<Vec<u8>> = report
        .recovered
        .iter()
        .filter(|o| o.key.starts_with("objects/999/") == false)
        .map(|o| o.content.clone())
        .collect();
    keep_payloads.sort();
    let expected: Vec<Vec<u8>> = (0..5).map(|i| format!("committed-{i}").into_bytes()).collect();
    assert_eq!(keep_payloads, expected, "byte-exact salvage of the keeps");
    // exactly one rejection: the RAW orphan (gc already reclaimed the
    // ledgered one — envelopes it never had are reported, never served)
    assert_eq!(report.rejected.len(), 1, "{:?}", report.rejected);
    assert_eq!(report.rejected[0].key, "objects/999/orphan-raw.data");
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
