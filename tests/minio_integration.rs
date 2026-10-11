//! Phase 4 integration: embedded engine (Core + SQLite + MinIO).
//! Requires MinIO; run via scripts/test-local.sh (LIGHTFS_TEST_MINIO=1).
//! Verifies TEST_PLAN §2: for every file -> node exists, object exists,
//! size matches.

use std::sync::Arc;

use lightfs::config::Config;
use lightfs::error::Result;
use lightfs::object_store::ObjectStore;

/// Ensure the test bucket exists (fresh MinIO instances have no buckets —
/// a fresh-CI-environment portability fix, not a production change).
fn ensure_test_bucket(cfg: &Config) {
    use lightfs::object_store::s3::S3ObjectStore;
    let store = S3ObjectStore::new(cfg.s3_config()).expect("s3 config");
    store.ensure_bucket().expect("ensure bucket");
}

fn minio_config(db_tag: &str) -> Result<Config> {
    if std::env::var("LIGHTFS_TEST_MINIO").ok().as_deref() != Some("1") {
        return Err(lightfs::error::Error::Config(
            "set LIGHTFS_TEST_MINIO=1".into(),
        ));
    }
    let mut cfg = Config::load_or_defaults(None)?;
    cfg.s3.endpoint =
        std::env::var("LIGHTFS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    cfg.s3.bucket = std::env::var("LIGHTFS_S3_BUCKET").unwrap_or_else(|_| "lightfs-test".into());
    cfg.metadata.path = std::env::temp_dir()
        .join(format!("lightfs-integ-{db_tag}-{}.db", std::process::id()))
        .to_string_lossy()
        .to_string();
    Ok(cfg)
}

#[test]
fn embedded_engine_end_to_end() {
    let cfg = match minio_config("e2e") {
        Ok(c) => c,
        Err(_) => return, // MinIO gate not set; unit tests cover the rest
    };
    let _ = std::fs::remove_file(&cfg.metadata.path);
    let eng = lightfs::engine::open_embedded(&cfg).expect("embedded engine opens");

    // namespace operations through the embedded Core
    eng.core.mkdir("/docs", 0o755).unwrap();
    eng.core.create("/docs/report.txt", 0o644).unwrap();
    let h = eng.core.open_write("/docs/report.txt", None).unwrap();
    eng.core.write(&h, 0, b"quarterly report contents").unwrap();
    eng.core.commit_write(&h).unwrap();

    // rename is metadata-only; bytes must survive at the same key
    eng.core.rename("/docs/report.txt", "/archive.txt").unwrap();
    assert_eq!(
        eng.core.read("/archive.txt", 0, 100).unwrap(),
        b"quarterly report contents"
    );

    // TEST_PLAN §2: metadata <-> object consistency for every file
    let mut files = 0;
    for node in eng.core.metadata().all_nodes().unwrap() {
        if node.is_file() {
            files += 1;
            let key = node.object_key.clone().unwrap();
            let head = eng.core.objects().head(&key).unwrap();
            assert_eq!(head.size as i64, node.size, "size mismatch for {key}");
            assert_eq!(head.etag, node.etag, "etag mismatch for {key}");
        }
    }
    assert_eq!(files, 1);

    // cleanup namespace
    eng.core.unlink("/archive.txt").unwrap();
    eng.core.rmdir("/docs").unwrap();
    let _ = std::fs::remove_file(&cfg.metadata.path);
    let _ = std::fs::remove_file(format!("{}-wal", cfg.metadata.path));
    let _ = std::fs::remove_file(format!("{}-shm", cfg.metadata.path));
}

#[test]
fn embedded_engine_restart_persists_namespace() {
    let cfg = match minio_config("restart") {
        Ok(c) => c,
        Err(_) => return,
    };
    ensure_test_bucket(&cfg);
    let _ = std::fs::remove_file(&cfg.metadata.path);

    {
        let eng = lightfs::engine::open_embedded(&cfg).unwrap();
        eng.core.mkdir("/keep", 0o755).unwrap();
        eng.core.create("/keep/data.bin", 0o644).unwrap();
        let h = eng.core.open_write("/keep/data.bin", None).unwrap();
        eng.core.write(&h, 0, b"survives restart").unwrap();
        eng.core.commit_write(&h).unwrap();
    } // engine dropped: "process restart"

    {
        let eng = lightfs::engine::open_embedded(&cfg).unwrap();
        let node = eng.core.lookup("/keep/data.bin").unwrap();
        assert_eq!(node.size, 16);
        assert_eq!(
            eng.core.read("/keep/data.bin", 0, 100).unwrap(),
            b"survives restart"
        );
        eng.core.unlink("/keep/data.bin").unwrap();
        eng.core.rmdir("/keep").unwrap();
    }
    let _ = std::fs::remove_file(&cfg.metadata.path);
    let _ = std::fs::remove_file(format!("{}-wal", cfg.metadata.path));
    let _ = std::fs::remove_file(format!("{}-shm", cfg.metadata.path));
}

#[test]
fn embedded_mem_engine_network_free() {
    // The Agent/CI fallback loop: same Core semantics, no MinIO needed.
    let mut cfg = Config::load_or_defaults(None).unwrap();
    cfg.metadata.path = std::env::temp_dir()
        .join(format!("lightfs-mem-{}.db", std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::remove_file(&cfg.metadata.path);
    let eng = lightfs::engine::open_embedded_mem(&cfg).unwrap();
    eng.core.mkdir("/x", 0o755).unwrap();
    eng.core.create("/x/y", 0o644).unwrap();
    let h = eng.core.open_write("/x/y", None).unwrap();
    eng.core.write(&h, 0, b"no network").unwrap();
    eng.core.commit_write(&h).unwrap();
    assert_eq!(eng.core.read("/x/y", 0, 50).unwrap(), b"no network");
    let _ = std::fs::remove_file(&cfg.metadata.path);
}

// Silence unused import when MinIO gate is off.
#[allow(unused)]
fn _touch(_: Arc<()>) {}

/// The race that matters, against real S3: several threads commit writes to
/// one file at the same observed version; network latency widens the
/// check-vs-commit window. Exactly one winner, winner's bytes intact.
#[test]
fn concurrent_commit_race_minio() {
    let mut cfg = match minio_config("race") {
        Ok(c) => c,
        Err(_) => return,
    };
    // Dedicated bucket: the "only the winner's object remains" assertion is
    // prefix-scoped per node id, and fresh DBs reuse small ids across runs.
    // Retention is DISABLED here: this test pins CAS-race cleanup, not the
    // retention feature (which intentionally keeps superseded history).
    cfg.s3.bucket = "lightfs-race".into();
    cfg.retention.superseded_secs = 0;
    let _ = std::fs::remove_file(&cfg.metadata.path);
    {
        let store = lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config()).unwrap();
        store.ensure_bucket().unwrap();
    }
    {
        let store = lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config()).unwrap();
        store.ensure_bucket().unwrap();
        // wipe leftovers from previous runs of this test
        let mut token = None;
        loop {
            let page = store.list("objects/", token.as_deref(), 1000).unwrap();
            for item in &page.items {
                store.delete(&item.key).unwrap();
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
    }
    let eng = lightfs::engine::open_embedded(&cfg).expect("engine opens");
    eng.core.create("/race", 0o644).unwrap();
    let v = eng.core.lookup("/race").unwrap().version;

    for round in 0..3 {
        let core = eng.core.clone();
        let mut joins = Vec::new();
        for w in 0..4u64 {
            let core = core.clone();
            joins.push(std::thread::spawn(move || {
                let h = core.open_write("/race", Some(v + round)).unwrap();
                let payload = format!("winner-is-writer-{w}-round-{round}");
                core.write(&h, 0, payload.as_bytes()).unwrap();
                (payload, core.commit_write(&h))
            }));
        }
        let mut winner: Option<(String, lightfs::metadata::Node)> = None;
        let mut wins = 0;
        for j in joins {
            let (payload, res) = j.join().unwrap();
            match res {
                Ok(node) => {
                    wins += 1;
                    winner = Some((payload, node));
                }
                Err(lightfs::error::Error::Conflict { .. }) => {}
                Err(e) => panic!("unexpected: {e}"),
            }
        }
        assert_eq!(wins, 1, "round {round}");
        let (payload, node) = winner.unwrap();
        // served bytes are exactly the winner's payload
        assert_eq!(eng.core.read("/race", 0, 1000).unwrap(), payload.as_bytes());
        // live object agrees with metadata
        let head = eng
            .core
            .objects()
            .head(node.object_key.as_deref().unwrap())
            .unwrap();
        assert_eq!(head.etag, node.etag);
        assert_eq!(head.size as i64, node.size);
        // the loser version keys were cleaned: exactly one object for the node
        let mut count = 0;
        let mut token = None;
        loop {
            let page = eng
                .core
                .objects()
                .list(&format!("objects/{}/", node.id), token.as_deref(), 100)
                .unwrap();
            count += page.items.len();
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        assert_eq!(
            count, 1,
            "only the winner's object may remain (round {round})"
        );
    }
    eng.core.unlink("/race").unwrap();
    let _ = std::fs::remove_file(&cfg.metadata.path);
    let _ = std::fs::remove_file(format!("{}-wal", cfg.metadata.path));
    let _ = std::fs::remove_file(format!("{}-shm", cfg.metadata.path));
}

/// P0-3 acceptance (review round 8): a metadata snapshot + the SAME bucket
/// restores point-in-time CONTENT. Retention keeps superseded objects in
/// the bucket, so the restored snapshot's object keys still resolve.
/// Sequence:
///   write /keep=V1, /zap  -> backup
///   overwrite /keep=V2, unlink /zap, create /post   (AFTER the backup)
///   restore: fresh engine on a COPY of the backup db, same bucket
///   asserts: byte-exact V1 content on /keep and /zap, /post invisible,
///            fsck missing_objects == 0; the post-backup leftovers appear
///            as exactly the expected orphan/stale classifications.
#[test]
fn point_in_time_restore_minio() {
    let mut cfg = match minio_config("pit") {
        Ok(c) => c,
        Err(_) => return,
    };
    cfg.s3.bucket = "lightfs-pit".into();
    let _ = std::fs::remove_file(&cfg.metadata.path);
    {
        let store = lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config()).unwrap();
        store.ensure_bucket().unwrap();
        let mut token = None;
        loop {
            let page = store.list("objects/", token.as_deref(), 1000).unwrap();
            for item in &page.items {
                store.delete(&item.key).unwrap();
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
    }

    let keep_v1 = b"point-in-time original content";
    let zap_old = b"deleted after backup, must come back";
    {
        let eng = lightfs::engine::open_embedded(&cfg).unwrap();
        for (path, content) in [("/keep", &keep_v1[..]), ("/zap", &zap_old[..])] {
            eng.core.create(path, 0o644).unwrap();
            let h = eng.core.open_write(path, None).unwrap();
            eng.core.write(&h, 0, content).unwrap();
            eng.core.commit_write(&h).unwrap();
        }
    }
    let backup_path = std::env::temp_dir()
        .join(format!("lightfs-pit-backup-{}.db", std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::remove_file(&backup_path);
    lightfs::cmd::backup(&cfg, &backup_path).unwrap();

    // Damage everything recoverable: overwrite, delete, and add.
    {
        let eng = lightfs::engine::open_embedded(&cfg).unwrap();
        let v = eng.core.lookup("/keep").unwrap().version;
        let h = eng.core.open_write("/keep", Some(v)).unwrap();
        eng.core
            .write(&h, 0, b"overwritten AFTER the backup")
            .unwrap();
        eng.core.commit_write(&h).unwrap();
        eng.core.unlink("/zap").unwrap();
        eng.core.create("/post", 0o644).unwrap();
        let h = eng.core.open_write("/post", None).unwrap();
        eng.core.write(&h, 0, b"created after the backup").unwrap();
        eng.core.commit_write(&h).unwrap();
    }

    // Restore = metadata snapshot + the same (current) bucket.
    let mut restore_cfg = cfg.clone();
    restore_cfg.metadata.path = std::env::temp_dir()
        .join(format!("lightfs-pit-restore-{}.db", std::process::id()))
        .to_string_lossy()
        .to_string();
    let _ = std::fs::remove_file(&restore_cfg.metadata.path);
    std::fs::copy(&backup_path, &restore_cfg.metadata.path).unwrap();
    {
        let eng = lightfs::engine::open_embedded(&restore_cfg).unwrap();
        assert_eq!(eng.core.read("/keep", 0, 100).unwrap(), keep_v1);
        assert_eq!(eng.core.read("/zap", 0, 100).unwrap(), zap_old);
        assert!(
            eng.core.lookup("/post").is_err(),
            "/post was created after the backup"
        );

        let report = lightfs::fsck::fsck(&eng.core).unwrap();
        assert_eq!(
            report.missing_objects.len(),
            0,
            "retention must keep every object the snapshot references: {}",
            report.render()
        );
        // Post-backup leftovers are expected and classified (not missing).
        // Node ids are reused after unlink (plain INTEGER PRIMARY KEY), so
        // /post's commits (id 3, reused from the deleted /zap) show up as
        // STALE versions of the restored /zap rather than orphans. The
        // metadata-only create adds no object of its own, so exactly the
        // two content objects remain stale. All inert: nothing references
        // them and restored-db gc only touches recorded retained rows.
        assert_eq!(report.orphan_objects.len(), 0, "{}", report.render());
        assert_eq!(report.stale_versions.len(), 2, "{}", report.render());
        assert_eq!(report.retained_objects.len(), 0, "{}", report.render());
    }
    for f in [
        cfg.metadata.path.clone(),
        restore_cfg.metadata.path.clone(),
        backup_path.clone(),
        format!("{}-wal", cfg.metadata.path),
        format!("{}-shm", cfg.metadata.path),
        format!("{}-wal", restore_cfg.metadata.path),
        format!("{}-shm", restore_cfg.metadata.path),
        format!("{}-wal", backup_path),
        format!("{}-shm", backup_path),
    ] {
        let _ = std::fs::remove_file(f);
    }
}

#[test]
fn symlinks_with_minio_fsck_and_reopen() {
    let cfg = match minio_config("symlink") {
        Ok(c) => c,
        Err(_) => return,
    };
    let _ = std::fs::remove_file(&cfg.metadata.path);
    let eng = lightfs::engine::open_embedded(&cfg).expect("embedded engine opens");

    // a real file, a relative link, a dangling link, a dir link
    eng.core.mkdir("/app", 0o755).unwrap();
    eng.core.create("/app/cli.js", 0o644).unwrap();
    let h = eng.core.open_write("/app/cli.js", None).unwrap();
    eng.core.write(&h, 0, b"#!/usr/bin/env node\n").unwrap();
    eng.core.commit_write(&h).unwrap();
    eng.core.symlink("cli.js", "/app/run").unwrap();
    eng.core.symlink("../app/cli.js", "/shortcut").unwrap();
    eng.core.symlink("/app", "/appdir").unwrap();

    // stat vs lstat through the MinIO-backed store
    assert!(eng.core.lookup("/app/run").unwrap().is_symlink());
    assert_eq!(
        eng.core.read("/shortcut", 0, 100).unwrap(),
        b"#!/usr/bin/env node\n"
    );
    let names: Vec<String> = eng
        .core
        .readdir("/appdir")
        .unwrap()
        .into_iter()
        .map(|n| n.name)
        .collect();
    assert_eq!(names, vec!["cli.js".to_string(), "run".to_string()]);

    // fsck: symlinks carry no object, must not be problems
    let report = lightfs::fsck::fsck(&eng.core).unwrap();
    assert_eq!(report.problems(), 0, "{}", report.render());

    // reopen the SAME database: targets survive (persistence of the
    // metadata that backup snapshots would carry)
    let eng2 = lightfs::engine::open_embedded(&cfg).expect("reopen");
    assert_eq!(eng2.core.readlink("/app/run").unwrap(), "cli.js");
    assert_eq!(eng2.core.readlink("/shortcut").unwrap(), "../app/cli.js");
    assert_eq!(
        eng2.core.read("/shortcut", 0, 100).unwrap(),
        b"#!/usr/bin/env node\n"
    );

    let _ = std::fs::remove_file(&cfg.metadata.path);
    let _ = std::fs::remove_file(format!("{}-wal", cfg.metadata.path));
    let _ = std::fs::remove_file(format!("{}-shm", cfg.metadata.path));
}
