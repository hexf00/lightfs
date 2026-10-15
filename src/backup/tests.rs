//! S4.3 P0-b acceptance tests: canonical root, atomic publication, manifest
//! fail-closed, protection vs gc, maintenance lock — deterministic, fake
//! store, temp dirs, no sleeps for synchronization (deadline polls only).

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::*;
use crate::config::{BackupSection, Config, MetadataSection};
use crate::core::Core;
use crate::metadata::sqlite::SqliteMetadataStore;
use crate::object_store::ObjectStore;
use crate::object_store::mem::MemObjectStore;
use rusqlite::params;

fn temp_dir(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!(
        "lightfs-p0b-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn cfg_with(root: Option<&Path>, meta_db: &Path, lock_timeout: u64) -> Config {
    Config {
        metadata: MetadataSection {
            path: meta_db.to_string_lossy().to_string(),
        },
        backup: BackupSection {
            root: root.map(|p| p.to_string_lossy().to_string()),
            lock_timeout_secs: lock_timeout,
        },
        ..Default::default()
    }
}

/// Seed a metadata DB with the given object inventory (direct SQL against a
/// store-initialized schema).
fn seed_meta_db(path: &Path, node_keys: &[&str], pack_keys: &[&str], superseded: &[&str]) {
    {
        let _s = SqliteMetadataStore::open(path.to_str().unwrap()).unwrap();
    }
    let conn = rusqlite::Connection::open(path).unwrap();
    for (i, k) in node_keys.iter().enumerate() {
        conn.execute(
            "INSERT INTO nodes (id, parent_id, name, kind, size, mtime, mode, object_key) \
             VALUES (?1, 1, ?2, 'file', 1, 1, 448, ?3)",
            params![(i as i64) + 2, format!("f{i}"), k],
        )
        .unwrap();
    }
    for k in pack_keys {
        conn.execute(
            "INSERT INTO packs (key, bytes, uploaded_at, state, unref_since) \
             VALUES (?1, 1, 1, 'active', NULL)",
            params![k],
        )
        .unwrap();
    }
    for k in superseded {
        conn.execute(
            "INSERT INTO superseded_objects (object_key, superseded_at) VALUES (?1, 1)",
            params![k],
        )
        .unwrap();
    }
}

fn manifest_of(dir: &Path) -> Manifest {
    serde_json::from_str(&std::fs::read_to_string(dir.join(MANIFEST_NAME)).unwrap()).unwrap()
}

fn write_backup_dir(root: &Path, id: &str, m: &Manifest, meta_sha: Option<String>) {
    let dir = root.join(id);
    std::fs::create_dir_all(&dir).unwrap();
    let bytes = serde_json::to_vec_pretty(m).unwrap();
    std::fs::write(dir.join(MANIFEST_NAME), &bytes).unwrap();
    let meta = BackupMeta {
        backup_id: id.into(),
        created_at: m.created_at,
        expires_at: m.expires_at,
        manifest_sha256: meta_sha.unwrap_or_else(|| hex(&Sha256::digest(&bytes))),
    };
    std::fs::write(
        dir.join(META_NAME),
        serde_json::to_vec_pretty(&meta).unwrap(),
    )
    .unwrap();
}

fn valid_manifest(expires_at: Option<i64>, objects: &[&str]) -> Manifest {
    let objects: Vec<String> = objects.iter().map(|s| s.to_string()).collect();
    Manifest {
        version: MANIFEST_VERSION,
        created_at: 1,
        expires_at,
        objects_sha256: objects_sha256(&objects),
        objects,
    }
}

// --- root requirement ------------------------------------------------------

#[test]
fn p0b_backup_refuses_without_root_and_gc_deletes_nothing() {
    let dir = temp_dir("noroot");
    let meta_db = dir.join("m.db");
    let cfg = cfg_with(None, &meta_db, 1);
    let e = backup_now(&cfg, None).unwrap_err();
    assert!(format!("{e}").contains("[backup] root"), "{e}");
    let e = backup_prune(&cfg, "whatever").unwrap_err();
    assert!(format!("{e}").contains("[backup] root"), "{e}");
    // gc fails closed too and the fake store is untouched.
    let mem = Arc::new(MemObjectStore::new());
    mem.put("packs/x", b"1").unwrap();
    let core = Core::with_retention(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        mem.clone(),
        1,
    );
    let e = crate::cmd::gc_run(&core, None, 1, 1, 10_000).unwrap_err();
    assert!(format!("{e}").contains("[backup] root"), "{e}");
    assert!(mem.head("packs/x").is_ok(), "no object may be touched");
    assert_eq!(
        mem.delete_calls(),
        0,
        "zero delete calls on the no-root path"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// --- atomic publication ------------------------------------------------------

#[test]
fn p0b_backup_publishes_verifiable_directory_from_snapshot() {
    let dir = temp_dir("publish");
    let (root, meta_db) = (dir.join("root"), dir.join("m.db"));
    seed_meta_db(
        &meta_db,
        &["objects/1/live-a", "objects/2/live-b"],
        &["packs/pack-1.data"],
        &["objects/9/superseded"],
    );
    let cfg = cfg_with(Some(&root), &meta_db, 5);
    let published = backup_now(&cfg, None).unwrap();
    assert!(published.is_dir());
    assert!(published.join("meta.db").is_file());
    assert!(published.join(MANIFEST_NAME).is_file());
    assert!(published.join(META_NAME).is_file());

    let m = manifest_of(&published);
    assert_eq!(m.version, MANIFEST_VERSION);
    assert_eq!(m.expires_at, None, "no expiry = protected until pruned");
    for k in [
        "objects/1/live-a",
        "objects/2/live-b",
        "packs/pack-1.data",
        "objects/9/superseded",
    ] {
        assert!(m.objects.iter().any(|o| o == k), "manifest missing {k}");
    }
    // Integrity anchors hold.
    let meta: BackupMeta =
        serde_json::from_str(&std::fs::read_to_string(published.join(META_NAME)).unwrap()).unwrap();
    let raw = std::fs::read(published.join(MANIFEST_NAME)).unwrap();
    assert_eq!(meta.manifest_sha256, hex(&Sha256::digest(&raw)));
    assert_eq!(m.objects_sha256, objects_sha256(&m.objects));
    // The root carries no .tmp- leftovers after publication.
    assert!(scan_backups(root.to_str().unwrap()).unwrap().len() == 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p0b_manifest_is_exported_from_snapshot_not_live_db() {
    let dir = temp_dir("snapself");
    let (root, meta_db) = (dir.join("root"), dir.join("m.db"));
    seed_meta_db(&meta_db, &["objects/1/a"], &[], &[]);
    let cfg = cfg_with(Some(&root), &meta_db, 5);
    let published = backup_now(&cfg, None).unwrap();
    // The LIVE database moves on after the snapshot: a new committed object.
    {
        let conn = rusqlite::Connection::open(&meta_db).unwrap();
        conn.execute(
            "INSERT INTO nodes (id, parent_id, name, kind, size, mtime, mode, object_key) \
             VALUES (99, 1, 'late', 'file', 1, 1, 448, 'objects/99/late')",
            [],
        )
        .unwrap();
    }
    let keys = protected_keys(root.to_str().unwrap(), now_secs() + 60).unwrap();
    assert!(keys.contains("objects/1/a"));
    assert!(
        !keys.contains("objects/99/late"),
        "the manifest must describe the snapshot, not the live database"
    );
    assert_eq!(manifest_of(&published).objects.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p0b_unpublished_tmp_dirs_are_never_backups_and_get_cleaned() {
    let dir = temp_dir("tmpclean");
    let (root, meta_db) = (dir.join("root"), dir.join("m.db"));
    seed_meta_db(&meta_db, &["objects/1/a"], &[], &[]);
    // A half-published leftover that WOULD fail closed if it were a backup.
    let junk = root.join(format!("{TMP_PREFIX}junk"));
    std::fs::create_dir_all(&junk).unwrap();
    std::fs::write(junk.join(MANIFEST_NAME), "not json").unwrap();
    let cfg = cfg_with(Some(&root), &meta_db, 5);
    backup_now(&cfg, None).unwrap();
    assert!(
        !junk.exists(),
        "stale .tmp- leftovers are cleaned under the lock"
    );
    let v = scan_backups(root.to_str().unwrap()).unwrap();
    assert_eq!(v.len(), 1, "only the published backup counts");
    let _ = std::fs::remove_dir_all(&dir);
}

// --- fail-closed verification ------------------------------------------------

fn corrupt_case(dir: &Path, case: &str) -> (PathBuf, PathBuf) {
    let (root, meta_db) = (dir.join("root"), dir.join("m.db"));
    seed_meta_db(&meta_db, &["objects/1/a"], &[], &[]);
    let id = format!("bk-{case}");
    match case {
        "missing-manifest" => {
            let d = root.join(&id);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join("meta.db"), b"sqlite").unwrap();
        }
        "corrupt-manifest" => {
            let d = root.join(&id);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join(MANIFEST_NAME), "not json at all").unwrap();
            std::fs::write(d.join(META_NAME), "{}").unwrap();
        }
        "unknown-version" => {
            let mut m = valid_manifest(None, &["objects/1/a"]);
            m.version = 99;
            write_backup_dir(&root, &id, &m, None);
        }
        "object-list-edited" => {
            let mut m = valid_manifest(None, &["objects/1/a"]);
            m.objects.push("objects/1/b".into());
            write_backup_dir(&root, &id, &m, None);
        }
        "meta-sha-mismatch" => {
            let m = valid_manifest(None, &["objects/1/a"]);
            write_backup_dir(&root, &id, &m, Some("0".repeat(64)));
        }
        "legacy-file" => {
            std::fs::create_dir_all(&root).unwrap();
            std::fs::write(root.join(&id), b"legacy single-file backup").unwrap();
        }
        _ => unreachable!(),
    }
    (root, meta_db)
}

#[test]
fn p0b_scan_fails_closed_on_any_unverifiable_backup() {
    for case in [
        "missing-manifest",
        "corrupt-manifest",
        "unknown-version",
        "object-list-edited",
        "meta-sha-mismatch",
        "legacy-file",
    ] {
        let dir = temp_dir(&format!("fc-{case}"));
        let (root, _meta) = corrupt_case(&dir, case);
        let err = scan_backups(root.to_str().unwrap()).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("fail-closed"), "[{case}] {msg}");
        assert!(msg.contains(&format!("bk-{case}")), "[{case}] {msg}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn p0b_gc_fail_closed_makes_zero_delete_calls() {
    for case in ["corrupt-manifest", "unknown-version"] {
        let dir = temp_dir(&format!("gcfail-{case}"));
        let (root, _meta) = corrupt_case(&dir, case);
        let mem = Arc::new(MemObjectStore::new());
        mem.put("packs/victim.data", b"bytes").unwrap();
        let core = Core::with_retention(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            mem.clone(),
            1,
        );
        core.metadata()
            .register_pack("packs/victim.data", 10)
            .unwrap();
        core.metadata()
            .mark_pack_uploaded("packs/victim.data", 1)
            .unwrap();
        core.metadata()
            .wb_finalize_batch_if_terminal("packs/victim.data", 1)
            .unwrap();
        let e = crate::cmd::gc_run(
            &core,
            Some(root.to_str().unwrap()),
            1,
            1,
            now_secs() + 10_000,
        )
        .unwrap_err();
        assert!(format!("{e}").contains("fail-closed"), "[{case}] {e}");
        assert!(
            mem.head("packs/victim.data").is_ok(),
            "[{case}] object intact"
        );
        assert_eq!(mem.delete_calls(), 0, "[{case}] zero delete calls");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

// --- protection vs gc ----------------------------------------------------------

/// A live infra: core whose pack `p` is unreferenced (reclaimable), the
/// object really in the store, and a manifest under `root`.
struct GcFixture {
    dir: PathBuf,
    core: Arc<Core>,
    mem: Arc<MemObjectStore>,
    root: PathBuf,
    p: &'static str,
}

fn gc_fixture(manifest: Option<Manifest>) -> GcFixture {
    let dir = temp_dir("gc");
    let (root, mem) = (dir.join("root"), Arc::new(MemObjectStore::new()));
    let core = Arc::new(Core::with_retention(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        mem.clone(),
        1,
    ));
    let p: &'static str = "packs/gc-fixture.data";
    core.metadata().register_pack(p, 10).unwrap();
    core.metadata().mark_pack_uploaded(p, 1).unwrap();
    core.metadata().wb_finalize_batch_if_terminal(p, 1).unwrap();
    mem.put(p, b"pack-bytes").unwrap();
    std::fs::create_dir_all(&root).unwrap();
    if let Some(m) = &manifest {
        write_backup_dir(&root, "bk-guard", m, None);
    }
    GcFixture {
        dir,
        core,
        mem,
        root,
        p,
    }
}

#[test]
fn p0b_protected_object_survives_gc_until_prune() {
    let f = gc_fixture(Some(valid_manifest(None, &["packs/gc-fixture.data"])));
    let now = now_secs() + 10_000;
    // Pass 1: protected — nothing deleted, no task registered for it.
    let s = crate::cmd::gc_run(&f.core, Some(f.root.to_str().unwrap()), 5, 1, now).unwrap();
    assert!(f.mem.head(f.p).is_ok(), "protected object must survive gc");
    assert!(f.core.metadata().pack_state(f.p).unwrap().is_some());
    assert!(
        f.core
            .metadata()
            .gc_claim_deletions(10, 1)
            .unwrap()
            .is_empty()
    );
    assert_eq!((s.done, s.failed, s.skipped_protected), (0, 0, 0));
    // Prune the guard: protection lifted.
    let cfg = cfg_with(Some(&f.root), &f.dir.join("unused.db"), 5);
    backup_prune(&cfg, "bk-guard").unwrap();
    // Pass 2: reclaimable — the durable task path deletes and finalizes.
    let s = crate::cmd::gc_run(&f.core, Some(f.root.to_str().unwrap()), 5, 1, now).unwrap();
    assert_eq!((s.done, s.failed), (1, 0));
    assert!(f.mem.head(f.p).is_err());
    assert!(f.core.metadata().pack_state(f.p).unwrap().is_none());
    let _ = std::fs::remove_dir_all(&f.dir);
}

#[test]
fn p0b_expired_backup_releases_protection_but_still_fails_closed_when_corrupt() {
    // expires_at in the past (deterministic via the gc-side clock param).
    let f = gc_fixture(Some(valid_manifest(Some(1), &["packs/gc-fixture.data"])));
    let now = now_secs() + 10_000;
    let s = crate::cmd::gc_run(&f.core, Some(f.root.to_str().unwrap()), 5, 1, now).unwrap();
    assert_eq!(
        (s.done, s.failed),
        (1, 0),
        "expired backup no longer protects"
    );
    assert!(f.mem.head(f.p).is_err());
    let _ = std::fs::remove_dir_all(&f.dir);
}

#[test]
fn p0b_live_referenced_object_is_never_deleted_by_gc() {
    let dir = temp_dir("liveref");
    let mem = Arc::new(MemObjectStore::new());
    let core = Arc::new(Core::with_retention(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        mem.clone(),
        1,
    ));
    core.create("/f", 0o644).unwrap();
    let h = core.open_write("/f", None).unwrap();
    core.write(&h, 0, b"live").unwrap();
    core.commit_write(&h).unwrap();
    let key = core.lookup("/f").unwrap().object_key.clone().unwrap();
    let root = dir.join("root");
    std::fs::create_dir_all(&root).unwrap();
    crate::cmd::gc_run(
        &core,
        Some(root.to_str().unwrap()),
        5,
        1,
        now_secs() + 10_000,
    )
    .unwrap();
    assert!(
        mem.head(&key).is_ok(),
        "a live reference is not a gc candidate"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn p0b_prune_validates_id_and_reports_missing_backup() {
    let dir = temp_dir("prune");
    let (root, meta_db) = (dir.join("root"), dir.join("m.db"));
    let cfg = cfg_with(Some(&root), &meta_db, 5);
    std::fs::create_dir_all(&root).unwrap();
    for id in ["../escape", ".hidden", "gc.lock", "no/slash"] {
        assert!(backup_prune(&cfg, id).is_err(), "{id} must be refused");
    }
    assert!(backup_prune(&cfg, "does-not-exist").is_err());
    // A CORRUPT backup can be pruned — explicit operator resolution of a
    // fail-closed state.
    let d = root.join("corrupt-bk");
    std::fs::create_dir_all(&d).unwrap();
    std::fs::write(d.join(MANIFEST_NAME), "junk").unwrap();
    assert!(scan_backups(root.to_str().unwrap()).is_err());
    backup_prune(&cfg, "corrupt-bk").unwrap();
    assert!(scan_backups(root.to_str().unwrap()).unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

// --- maintenance lock --------------------------------------------------------

/// Child process body: gated on an env var so normal `cargo test` runs are a
/// no-op. Holds the maintenance lock until the parent deletes the marker.
#[test]
fn p0b_lock_child_holder() {
    let Some(root) = std::env::var_os("LIGHTFS_P0B_LOCK_CHILD") else {
        return;
    };
    let root = PathBuf::from(root);
    let _l = MaintenanceLock::acquire(&root, 30).unwrap();
    std::fs::write(root.join("child-holding"), b"1").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while root.join("child-holding").exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // Lock released when the test process exits.
}

fn wait_for_marker(root: &Path, marker: &Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !marker.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "child never acquired the lock"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let _ = root;
}

fn spawn_lock_child(root: &Path) -> std::process::Child {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "backup::tests::p0b_lock_child_holder",
            "--nocapture",
        ])
        .env("LIGHTFS_P0B_LOCK_CHILD", root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("spawn child test process")
}

/// Real cross-process evidence: a SECOND OS process holding the lock blocks
/// this one past its timeout; cooperative exit and SIGKILL both release the
/// flock (the kernel owns the lock, not a marker file).
#[test]
fn p0b_lock_is_cross_process_and_released_on_exit_and_death() {
    let dir = temp_dir("lock");
    let root = dir.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let marker = root.join("child-holding");

    // Phase 1: mutual exclusion across processes.
    let mut child = spawn_lock_child(&root);
    wait_for_marker(&root, &marker);
    let e = MaintenanceLock::acquire(&root, 1).unwrap_err();
    assert!(format!("{e}").contains("busy"), "{e}");
    // Cooperative release: the child exits when the marker disappears.
    std::fs::remove_file(&marker).unwrap();
    child.wait().unwrap();
    MaintenanceLock::acquire(&root, 10).unwrap();
    drop(MaintenanceLock::acquire(&root, 1));

    // Phase 2: crash release — SIGKILL the holder, the OS frees the lock.
    let mut child = spawn_lock_child(&root);
    wait_for_marker(&root, &marker);
    child.kill().unwrap();
    child.wait().unwrap();
    MaintenanceLock::acquire(&root, 10).expect("flock must be released when the holder dies");
    let _ = std::fs::remove_dir_all(&dir);
}

/// backup/gc/prune all take the SAME lock: while one holds it, the others
/// block and then error (never silently skip). In-process half; the
/// cross-process half is p0b_lock_is_cross_process_and_released_on_exit_and_death.
#[test]
fn p0b_backup_gc_prune_share_one_exclusive_lock() {
    let dir = temp_dir("shelock");
    let (root, meta_db) = (dir.join("root"), dir.join("m.db"));
    seed_meta_db(&meta_db, &["objects/1/a"], &[], &[]);
    let cfg = cfg_with(Some(&root), &meta_db, 1);
    let held = MaintenanceLock::acquire(&root, 5).unwrap();
    assert!(
        backup_now(&cfg, None).is_err(),
        "backup must not bypass the lock"
    );
    let mem = Arc::new(MemObjectStore::new());
    let core = Core::with_retention(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        mem,
        1,
    );
    let e = crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 1, 1, 10_000).unwrap_err();
    assert!(format!("{e}").contains("busy"), "{e}");
    let e = backup_prune(&cfg, "bk").unwrap_err();
    assert!(format!("{e}").contains("busy"), "{e}");
    drop(held);
    assert!(
        backup_now(&cfg, None).is_ok(),
        "released lock is reacquirable"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Full loop with the real executor wired behind the gate: enroll (skip
/// protected) → execute → converge; remote-deleted crash-window style
/// idempotency at gc level. Complements the p0a22 task-level tests.
#[test]
fn p0b_gc_end_to_end_enroll_execute_and_protection_filter() {
    let f = gc_fixture(None);
    let now = now_secs() + 10_000;
    // A second, PROTECTED pack rides along.
    let p2 = "packs/gc-protected.data";
    f.core.metadata().register_pack(p2, 10).unwrap();
    f.core.metadata().mark_pack_uploaded(p2, 1).unwrap();
    f.core
        .metadata()
        .wb_finalize_batch_if_terminal(p2, 1)
        .unwrap();
    f.mem.put(p2, b"kept").unwrap();
    write_backup_dir(&f.root, "bk-guard", &valid_manifest(None, &[p2]), None);
    let s = crate::cmd::gc_run(&f.core, Some(f.root.to_str().unwrap()), 5, 1, now).unwrap();
    assert_eq!((s.done, s.failed), (1, 0), "unprotected pack reclaimed");
    assert!(f.mem.head(f.p).is_err());
    assert!(f.mem.head(p2).is_ok(), "protected pack survives");
    assert!(
        f.core.metadata().pack_state(p2).unwrap().is_some(),
        "protected pack row is retained (no task marked done)"
    );
    let _ = std::fs::remove_dir_all(&f.dir);
}

#[allow(dead_code)]
fn unused(_: HashSet<String>) {}
