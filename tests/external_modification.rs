//! Phase 12: external S3 modification (TEST_PLAN §6). Someone bypasses
//! LightFS and rewrites the object directly in MinIO; the system must
//! eventually see the new content (no permanent stale cache) and `fsck`
//! must classify + reconcile the divergence.

use lightfs::config::Config;
use lightfs::engine;
use lightfs::error::Result;

fn minio_config(db: &str) -> Result<Config> {
    if std::env::var("LIGHTFS_TEST_MINIO").ok().as_deref() != Some("1") {
        return Err(lightfs::error::Error::Config(
            "set LIGHTFS_TEST_MINIO=1".into(),
        ));
    }
    let mut cfg = Config::load_or_defaults(None)?;
    cfg.s3.endpoint =
        std::env::var("LIGHTFS_S3_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    cfg.s3.bucket = std::env::var("LIGHTFS_S3_BUCKET").unwrap_or_else(|_| "lightfs-ext".into());
    cfg.metadata.path = std::env::temp_dir()
        .join(format!("lightfs-ext-{db}-{}.db", std::process::id()))
        .to_string_lossy()
        .to_string();
    Ok(cfg)
}

fn cleanup_db(cfg: &Config) {
    let _ = std::fs::remove_file(&cfg.metadata.path);
    let _ = std::fs::remove_file(format!("{}-wal", cfg.metadata.path));
    let _ = std::fs::remove_file(format!("{}-shm", cfg.metadata.path));
}

#[test]
fn external_s3_mutation_detected_and_reconciled() {
    let cfg = match minio_config("ext") {
        Ok(c) => c,
        Err(_) => return,
    };
    cleanup_db(&cfg);
    // fsck counts orphans bucket-wide, so this test needs its own bucket.
    {
        let store = lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config()).unwrap();
        store.ensure_bucket().unwrap();
    }
    let eng = engine::open_embedded(&cfg).unwrap();

    // normal path: create and write through Core
    eng.core.mkdir("/ext", 0o755).unwrap();
    eng.core.create("/ext/file", 0o644).unwrap();
    let h = eng.core.open_write("/ext/file", None).unwrap();
    eng.core.write(&h, 0, b"original data").unwrap();
    eng.core.commit_write(&h).unwrap();

    let node = eng.core.lookup("/ext/file").unwrap();
    let key = node.object_key.clone().unwrap();

    // === external mutation: direct MinIO write, bypassing LightFS ===
    let new_content = b"externally rewritten content that LightFS did not see";
    eng.core.objects().put(&key, new_content).unwrap();

    // Reads are bounded by metadata size (POSIX EOF semantics): the new
    // bytes are on the data plane but only min(size) of them surface until
    // fsck adopts the external modification.
    assert_eq!(
        eng.core.read("/ext/file", 0, 1000).unwrap(),
        new_content[..13].to_vec()
    );
    assert_eq!(eng.core.lookup("/ext/file").unwrap().size as usize, 13);

    // fsck classifies
    let report = lightfs::fsck::fsck(&eng.core).unwrap();
    assert_eq!(report.size_mismatches.len(), 1, "{}", report.render());
    assert_eq!(report.etag_mismatches.len(), 1, "{}", report.render());

    // --fix adopts: size/etag converge, version bumps
    let adopted = lightfs::fsck::fix(&eng.core, &report).unwrap();
    assert_eq!(adopted, 1);
    let after = eng.core.lookup("/ext/file").unwrap();
    assert_eq!(after.size as usize, new_content.len());
    assert_eq!(after.version, node.version + 1);

    // after fix the whole external content is visible
    assert_eq!(
        eng.core.read("/ext/file", 0, 1000).unwrap(),
        new_content.to_vec()
    );

    // re-scan is clean for the adopted node
    let report2 = lightfs::fsck::fsck(&eng.core).unwrap();
    assert_eq!(report2.size_mismatches.len(), 0);
    assert_eq!(report2.etag_mismatches.len(), 0);

    // cleanup
    eng.core.unlink("/ext/file").unwrap();
    eng.core.rmdir("/ext").unwrap();
    cleanup_db(&cfg);
}
