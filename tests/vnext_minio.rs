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
    // fails closed on that today — see publish_initial_commit)
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

    // namespace recovery from objects alone still folds the full state
    let view = lightfs::vnext::recover::namespace_view(objects.as_ref()).unwrap();
    assert_eq!(view.refs.len(), 1);
    // the fold walks EVERY reachable tree, so historical versions appear
    // per commit; the recovered namespace's PATH SET is the assertion
    let mut keep_paths: Vec<&str> = view
        .files
        .iter()
        .map(|f| f.path.as_str())
        .filter(|p| p.starts_with("/keep-"))
        .collect();
    keep_paths.sort_unstable();
    keep_paths.dedup();
    assert_eq!(keep_paths.len(), 5, "recovered namespace carries the five keeps");
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
