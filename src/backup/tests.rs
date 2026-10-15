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
    // Sequence suffix: macOS clock resolution is microseconds, and parallel
    // tests starting together can mint identical "nanos" names (S4.3.4b.2).
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let d = std::env::temp_dir().join(format!(
        "lightfs-p0b-{tag}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
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
/// S4.3.4b.2: the maintenance lock keys on the ROOT's canonical identity.
/// A symlink spelling and a dot-path spelling of the same root collide with
/// the absolute real path — concurrent maintenance through different
/// spellings cannot each hold "their own" gc.lock. Symlink arm: unix only
/// (executed on macOS and Linux; gated out on windows where symlink
/// creation needs privileges — the portable dot-path test below still runs
/// there).
#[test]
#[cfg(unix)]
fn p0c3_maintenance_lock_symlink_alias_is_the_same_lock() {
    let dir = temp_dir("lockalias");
    let (root, link) = (dir.join("root"), dir.join("root-link"));
    std::fs::create_dir_all(&root).unwrap();
    std::os::unix::fs::symlink(&root, &link).unwrap();
    // Held via the symlink spelling...
    let held = MaintenanceLock::acquire(&link, 0).unwrap();
    // ...the real path and the alias itself both collide with the SAME lock.
    let e = MaintenanceLock::acquire(&root, 0).unwrap_err();
    assert!(format!("{e}").contains("busy"), "{e}");
    let e = MaintenanceLock::acquire(&link, 0).unwrap_err();
    assert!(format!("{e}").contains("busy"), "{e}");
    drop(held);
    p0c3_reacquire_after_release(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Portable alias arm (runs on every platform incl. windows): a dot-path
/// spelling of the same root resolves to the same gc.lock.
#[test]
fn p0c3_maintenance_lock_dot_path_alias_is_the_same_lock() {
    let dir = temp_dir("lockalias2");
    let root = dir.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let dot = dir.join("..").join(dir.file_name().unwrap()).join("root");
    let held = MaintenanceLock::acquire(&dot, 0).unwrap();
    let e = MaintenanceLock::acquire(&root, 0).unwrap_err();
    assert!(format!("{e}").contains("busy"), "{e}");
    drop(held);
    p0c3_reacquire_after_release(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Re-acquire after a deliberate release. A short bounded wait — not
/// `timeout 0` — because Darwin can keep a closed flock visible to the
/// next flock for a few tens of microseconds under thread contention
/// (observed as a spurious EAGAIN ~45µs after `close(2)` returned on the
/// same thread); the alias-collision property itself is already proven by
/// the busy attempts made while the lock is held, and a lock that genuinely
/// leaked (never released) still fails this after the 2s bound.
fn p0c3_reacquire_after_release(root: &Path) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match MaintenanceLock::acquire(root, 0) {
            Ok(_) => return,
            Err(e) => {
                if std::time::Instant::now() >= deadline {
                    panic!("lock not reacquired after release: {e}");
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
    }
}

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

// ---- S4.3.1: retention-off gc semantics --------------------------------
// Retention OFF (`retention_secs = 0`) must NOT disable gc: retirement under
// retention-off registers durable deletion tasks, and gc is the only thing
// that drains them. The [backup]-root / lock / manifest gates apply unchanged.

fn rtoff_core(mem: Arc<MemObjectStore>) -> Arc<Core> {
    Arc::new(Core::with_retention(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        mem,
        0,
    ))
}

/// 1) retention-off + valid root/manifest + qualified superseded candidate:
///    the task enrolls at cutoff=now and executes to completion.
#[test]
fn s431_retention_off_gc_enrolls_and_completes_qualified_task() {
    let dir = temp_dir("rtoff-run");
    let (root, mem) = (dir.join("root"), Arc::new(MemObjectStore::new()));
    let core = rtoff_core(mem.clone());
    let k = "objects/9/w-rtoff.data";
    mem.put(k, b"rtoff-bytes").unwrap();
    // Retention-on era leftover: superseded row registered in the past.
    core.metadata().record_superseded(k, 1).unwrap();
    std::fs::create_dir_all(&root).unwrap();
    let s = crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap();
    assert_eq!(
        s.enrolled.1, 1,
        "candidate qualifies at cutoff=now (retention off)"
    );
    assert_eq!(
        s.done, 1,
        "the registered task executes under retention-off"
    );
    assert!(mem.head(k).is_err(), "object actually deleted");
    assert!(
        core.metadata().all_superseded().unwrap().is_empty(),
        "superseded row finalized away"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 2) retention-off + manifest-protected object: the object survives, its
///    pre-existing deletion task is kept (not marked done).
#[test]
fn s431_retention_off_gc_keeps_protected_object_and_task() {
    let dir = temp_dir("rtoff-prot");
    let (root, mem) = (dir.join("root"), Arc::new(MemObjectStore::new()));
    let core = rtoff_core(mem.clone());
    let k = "objects/8/w-rtoff-prot.data";
    mem.put(k, b"protected-bytes").unwrap();
    core.metadata().record_superseded(k, 1).unwrap();
    std::fs::create_dir_all(&root).unwrap();
    // A backup published AFTER the task was registered still protects it.
    core.metadata()
        .gc_register_superseded_deletion(k, 2)
        .unwrap();
    write_backup_dir(&root, "bk", &valid_manifest(None, &[k]), None);
    let s = crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap();
    assert_eq!(s.enrolled.1, 0, "protected key is not re-enrolled");
    assert_eq!(s.skipped_protected, 1, "the existing task is skipped, kept");
    assert!(mem.head(k).is_ok(), "manifest-protected object survives");
    let claimed = core.metadata().gc_claim_deletions(10, 1).unwrap();
    assert_eq!(claimed.len(), 1, "task NOT marked done: still claimable");
    assert_eq!(claimed[0].key, k);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 3) retention-off + no [backup] root: zero object-delete calls.
#[test]
fn s431_retention_off_without_root_makes_zero_delete_calls() {
    let mem = Arc::new(MemObjectStore::new());
    let core = rtoff_core(mem.clone());
    let k = "objects/7/w-rtoff-noroot.data";
    mem.put(k, b"bytes").unwrap();
    core.metadata().record_superseded(k, 1).unwrap();
    let e = crate::cmd::gc_run(&core, None, 5, 0, now_secs() + 10).unwrap_err();
    assert!(
        format!("{e}").contains("root is not configured"),
        "fail-closed message: {e}"
    );
    assert_eq!(
        mem.delete_calls(),
        0,
        "no object may be touched without the backup-root gate"
    );
    assert!(mem.head(k).is_ok(), "object intact");
}

/// 4) retention-off + one unverifiable manifest: the whole pass aborts with
///    zero delete calls.
#[test]
fn s431_retention_off_corrupt_manifest_fails_closed_zero_deletes() {
    let dir = temp_dir("rtoff-corrupt");
    let (root, mem) = (dir.join("root"), Arc::new(MemObjectStore::new()));
    let core = rtoff_core(mem.clone());
    let k = "objects/6/w-rtoff-corrupt.data";
    mem.put(k, b"bytes").unwrap();
    core.metadata().record_superseded(k, 1).unwrap();
    std::fs::create_dir_all(root.join("bk-broken")).unwrap();
    std::fs::write(root.join("bk-broken").join(MANIFEST_NAME), b"{ not json").unwrap();
    let e =
        crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap_err();
    assert!(format!("{e}").contains("fail-closed"), "{e}");
    assert_eq!(mem.delete_calls(), 0, "corrupt manifest = zero deletes");
    assert!(mem.head(k).is_ok(), "object intact");
    let _ = std::fs::remove_dir_all(&dir);
}

/// S4.3.1 prune persistence order: within the SAME lock hold, the backup
/// directory is removed, THEN the canonical root is fsynced — and any
/// failure is an error, never a silent success. Observable consequences
/// asserted here: after prune the directory is gone and a full re-scan of
/// the root is clean (no trace of the pruned id); a failing prune (missing
/// id) reports an error and leaves every other backup untouched. Real
/// power-loss is NOT claimed — this verifies the code order, see report §2.
#[test]
fn s431_prune_removes_directory_then_fsyncs_root_within_one_lock() {
    let dir = temp_dir("prune-order");
    let root = dir.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let cfg = cfg_with(Some(&root), &dir.join("m.db"), 5);
    // Two real backups; prune one, verify the other survives and the scan
    // is clean — the post-remove root fsync is what makes the removal a
    // durable directory state before the lock is dropped.
    write_backup_dir(
        &root,
        "bk-gone",
        &valid_manifest(None, &["objects/x"]),
        None,
    );
    write_backup_dir(
        &root,
        "bk-keep",
        &valid_manifest(None, &["objects/y"]),
        None,
    );

    crate::backup::backup_prune(&cfg, "bk-gone").unwrap();
    assert!(
        !root.join("bk-gone").exists(),
        "directory removed while locked"
    );
    let scanned = super::scan_backups(root.to_str().unwrap()).unwrap();
    let ids: Vec<&str> = scanned.iter().map(|b| b.id.as_str()).collect();
    assert_eq!(ids, vec!["bk-keep"], "root re-scans clean after the fsync");

    // Failure is an error (not silent): pruning a missing id fails loudly
    // and removes nothing else.
    let e = crate::backup::backup_prune(&cfg, "bk-gone").unwrap_err();
    assert!(format!("{e}").contains("does not exist"), "{e}");
    assert!(
        root.join("bk-keep").exists(),
        "the surviving backup is untouched"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- S4.3.2: prune persistence commit point + failure paths ------------
// Every scenario below runs the REAL prune sequence with injectable
// root-fsync / directory-removal ops (call-order counted — no sleeps, no
// timing luck). After EVERY return path we re-acquire the maintenance lock
// to prove the shared gc.lock was released (no stranded locks).

/// Small gc-capable fixture: core + fake store with one qualified superseded
/// candidate physically present, canonical root created.
fn prune_gc_fixture(dir: &Path, key: &str) -> (Arc<Core>, Arc<MemObjectStore>, PathBuf) {
    let (root, mem) = (dir.join("root"), Arc::new(MemObjectStore::new()));
    let core = Arc::new(Core::with_retention(
        Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
        mem.clone(),
        0,
    ));
    mem.put(key, b"prune-fixture-bytes").unwrap();
    core.metadata().record_superseded(key, 1).unwrap();
    std::fs::create_dir_all(&root).unwrap();
    (core, mem, root)
}

fn gc_now(core: &Core, root: &Path) -> crate::cmd::GcSummary {
    crate::cmd::gc_run(core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap()
}

/// Asserts the lock is free by taking and dropping it.
fn assert_lock_released(root: &Path) {
    drop(MaintenanceLock::acquire(root, 1).unwrap());
}

/// 1) rename done, FIRST root fsync fails: persistence is unconfirmed.
///    The pending dir must exist and block gc (fail-closed, zero deletes);
///    a retry with the real ops converges.
#[test]
fn s432_prune_fsync1_fails_keeps_pending_blocks_gc_retry_converges() {
    let dir = temp_dir("p32-fsync1");
    let cfg = cfg_with(Some(&dir.join("root")), &dir.join("m.db"), 5);
    let (core, mem, root) = prune_gc_fixture(&dir, "objects/9/w-p32.data");
    write_backup_dir(&root, "bk", &valid_manifest(None, &["objects/z"]), None);

    let mut fsync_calls = 0u32;
    let mut no_remove =
        |_p: &Path| -> Result<()> { panic!("remove must not run before the commit point") };
    let e = super::backup_prune_impl(
        &cfg,
        "bk",
        &mut |_p| {
            fsync_calls += 1;
            if fsync_calls == 1 {
                Err(Error::Storage("injected fsync failure".into()))
            } else {
                Ok(())
            }
        },
        &mut no_remove,
    )
    .unwrap_err();
    assert!(
        format!("{e}").contains("UNCONFIRMED"),
        "honest error required: {e}"
    );
    assert!(
        !root.join("bk").exists(),
        "renamed away in the current view"
    );
    assert!(root.join(".prune-pending-bk").exists(), "pending kept");
    // The scanner fail-closes on the pending entry (never ignores it).
    let e = super::scan_backups(root.to_str().unwrap()).unwrap_err();
    assert!(format!("{e}").contains("unfinished backup prune"), "{e}");
    // gc: zero object-delete calls while the pending entry blocks.
    let e =
        crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap_err();
    assert!(format!("{e}").contains("fail-closed"), "{e}");
    assert_eq!(
        mem.delete_calls(),
        0,
        "no deletes under an unfinished prune"
    );
    assert_lock_released(&root);
    // Retry with the REAL ops finishes the cleanup; protection released
    // only now.
    super::backup_prune(&cfg, "bk").unwrap();
    assert!(!root.join(".prune-pending-bk").exists());
    let scanned = super::scan_backups(root.to_str().unwrap()).unwrap();
    assert!(scanned.is_empty(), "retired backup no longer scans");
    gc_now(&core, &root);
    assert!(
        mem.head("objects/9/w-p32.data").is_err(),
        "gc runs freely only after the prune commit point"
    );
    assert_lock_released(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 2) commit point passed, pending-dir REMOVAL fails: retirement stands,
///    pending still blocks gc; retry converges.
#[test]
fn s432_prune_removal_fails_pending_still_blocks_gc_retry_converges() {
    let dir = temp_dir("p32-rm");
    let cfg = cfg_with(Some(&dir.join("root")), &dir.join("m.db"), 5);
    let (core, mem, root) = prune_gc_fixture(&dir, "objects/8/w-p32.data");
    write_backup_dir(&root, "bk", &valid_manifest(None, &["objects/z"]), None);

    let mut remove_calls = 0u32;
    let e = super::backup_prune_impl(&cfg, "bk", &mut |_p| Ok(()), &mut |_p| {
        remove_calls += 1;
        Err(Error::Storage("injected removal failure".into()))
    })
    .unwrap_err();
    assert!(
        format!("{e}").contains("cannot remove pending prune"),
        "{e}"
    );
    assert!(root.join(".prune-pending-bk").exists(), "pending kept");
    assert!(
        super::scan_backups(root.to_str().unwrap()).is_err(),
        "fail-closed"
    );
    let e =
        crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap_err();
    assert!(format!("{e}").contains("fail-closed"), "{e}");
    assert_eq!(mem.delete_calls(), 0, "protected until cleanup converges");
    assert_lock_released(&root);
    super::backup_prune(&cfg, "bk").unwrap();
    assert!(!root.join(".prune-pending-bk").exists());
    assert!(
        super::scan_backups(root.to_str().unwrap())
            .unwrap()
            .is_empty()
    );
    assert_lock_released(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 3) pending removed, SECOND root fsync fails: the commit point already
///    passed, so the retirement stands and gc may run in the CURRENT view —
///    but the error is honest, and a resurrected pending entry (the
///    power-loss worst case this second fsync guards) fail-closes gc again.
#[test]
fn s432_prune_fsync2_fails_retired_but_error_and_resurrection_blocks() {
    let dir = temp_dir("p32-fsync2");
    let cfg = cfg_with(Some(&dir.join("root")), &dir.join("m.db"), 5);
    let (core, mem, root) = prune_gc_fixture(&dir, "objects/7/w-p32.data");
    write_backup_dir(&root, "bk", &valid_manifest(None, &["objects/z"]), None);

    let mut fsync_calls = 0u32;
    let e = super::backup_prune_impl(
        &cfg,
        "bk",
        &mut |_p| {
            fsync_calls += 1;
            if fsync_calls == 2 {
                Err(Error::Storage("injected fsync failure".into()))
            } else {
                Ok(())
            }
        },
        &mut |p: &Path| {
            // the real removal: the mock must reflect the state it claims
            std::fs::remove_dir_all(p).map_err(|e| Error::Storage(format!("remove: {e}")))
        },
    )
    .unwrap_err();
    assert!(format!("{e}").contains("commit point passed"), "{e}");
    assert!(!root.join("bk").exists());
    assert!(
        !root.join(".prune-pending-bk").exists(),
        "removed before the failing fsync"
    );
    // Current view is clean and PAST the commit point: scanning succeeds
    // without the backup, and gc is allowed — it actually deletes the
    // fixture's one qualified superseded candidate (delete_calls 0 -> 1).
    let scanned = super::scan_backups(root.to_str().unwrap()).unwrap();
    assert!(scanned.is_empty());
    gc_now(&core, &root);
    assert_eq!(
        mem.delete_calls(),
        1,
        "past the commit point gc deletes the qualified candidate exactly once"
    );
    assert_lock_released(&root);
    // Worst case the failed fsync was guarding: the pending entry
    // reappears after power loss — gc must fail closed again, never
    // silently proceed, and the retired backup is never re-validated.
    std::fs::create_dir_all(root.join(".prune-pending-bk")).unwrap();
    let e = super::scan_backups(root.to_str().unwrap()).unwrap_err();
    assert!(format!("{e}").contains("unfinished backup prune"), "{e}");
    let deletes_before = mem.delete_calls();
    let e =
        crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap_err();
    assert!(format!("{e}").contains("fail-closed"), "{e}");
    assert_eq!(
        mem.delete_calls(),
        deletes_before,
        "resurrected pending = no NEW deletes"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// 4) clean path: prune completes, nothing pending, gc runs, lock released.
#[test]
fn s432_prune_clean_path_commits_and_releases_lock() {
    let dir = temp_dir("p32-clean");
    let cfg = cfg_with(Some(&dir.join("root")), &dir.join("m.db"), 5);
    let (core, mem, root) = prune_gc_fixture(&dir, "objects/6/w-p32.data");
    write_backup_dir(&root, "bk", &valid_manifest(None, &["objects/z"]), None);

    super::backup_prune(&cfg, "bk").unwrap();
    assert!(!root.join("bk").exists() && !root.join(".prune-pending-bk").exists());
    let scanned = super::scan_backups(root.to_str().unwrap()).unwrap();
    assert!(scanned.is_empty());
    // After the commit point gc works end-to-end on real candidates.
    gc_now(&core, &root);
    assert!(mem.head("objects/6/w-p32.data").is_err());
    assert_lock_released(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Guard rail: a pending entry AND a live directory for the same id is an
/// inconsistent state — refuse loudly instead of guessing.
#[test]
fn s432_prune_refuses_pending_plus_live_dir() {
    let dir = temp_dir("p32-both");
    let cfg = cfg_with(Some(&dir.join("root")), &dir.join("m.db"), 5);
    let (_, _, root) = prune_gc_fixture(&dir, "objects/5/w-p32.data");
    write_backup_dir(&root, "bk", &valid_manifest(None, &["objects/z"]), None);
    std::fs::create_dir_all(root.join(".prune-pending-bk")).unwrap();
    let e = super::backup_prune(&cfg, "bk").unwrap_err();
    assert!(format!("{e}").contains("inconsistent state"), "{e}");
    assert_lock_released(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---- S4.3.3: the pending RETRY path must have its own commit point -----
// The retry branch previously did remove(pending) -> fsync(root): a removal
// that succeeded followed by a failed fsync would make the pending entry
// vanish from the current view — gc stops being fail-closed — while the
// original rename may never have persisted. Fixed order: fsync root FIRST
// (the retry's commit point), THEN remove, then fsync again.

fn pending_only_fixture(tag: &str) -> (Config, Arc<Core>, Arc<MemObjectStore>, PathBuf, PathBuf) {
    let dir = temp_dir(tag);
    let cfg = cfg_with(Some(&dir.join("root")), &dir.join("m.db"), 5);
    let (core, mem, root) = prune_gc_fixture(&dir, "objects/3/w-p33.data");
    // A leftover pending prune (contents intact, live dir absent).
    write_backup_dir(
        &root,
        ".prune-pending-bk",
        &valid_manifest(None, &["objects/z"]),
        None,
    );
    assert!(!root.join("bk").exists());
    (cfg, core, mem, root, dir)
}

/// A. retry's FIRST fsync fails: nothing is removed, gc stays fail-closed
///    with zero delete calls, lock released; a real retry converges.
#[test]
fn s433_retry_fsync1_fails_keeps_pending_zero_deletes() {
    let (cfg, core, mem, root, dir) = pending_only_fixture("p33-retry-fsync1");
    let mut remove_calls = 0u32;
    let e = super::backup_prune_impl(
        &cfg,
        "bk",
        &mut |_p| Err(Error::Storage("injected pre-removal fsync failure".into())),
        &mut |_p| {
            remove_calls += 1;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(
        format!("{e}").contains("pre-removal root fsync failed"),
        "honest error required: {e}"
    );
    assert_eq!(
        remove_calls, 0,
        "removal must not run before the retry commit point"
    );
    assert!(root.join(".prune-pending-bk").exists(), "pending kept");
    let e = super::scan_backups(root.to_str().unwrap()).unwrap_err();
    assert!(format!("{e}").contains("unfinished backup prune"), "{e}");
    let e =
        crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap_err();
    assert!(format!("{e}").contains("fail-closed"), "{e}");
    assert_eq!(
        mem.delete_calls(),
        0,
        "zero deletes while the retry is uncommitted"
    );
    assert_lock_released(&root);
    super::backup_prune(&cfg, "bk").unwrap();
    assert!(
        !root.join(".prune-pending-bk").exists(),
        "real retry converges"
    );
    assert!(
        super::scan_backups(root.to_str().unwrap())
            .unwrap()
            .is_empty()
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// B. retry's commit-point fsync succeeds, removal fails: pending remains
///    (the mock does not delete), everything fail-closed, zero deletes;
///    a real retry converges.
#[test]
fn s433_retry_commit_ok_removal_fails_retry_converges() {
    let (cfg, core, mem, root, dir) = pending_only_fixture("p33-retry-rm");
    let e = super::backup_prune_impl(&cfg, "bk", &mut |_p| Ok(()), &mut |_p| {
        Err(Error::Storage("injected removal failure".into()))
    })
    .unwrap_err();
    assert!(
        format!("{e}").contains("cannot remove pending prune"),
        "{e}"
    );
    assert!(root.join(".prune-pending-bk").exists(), "pending kept");
    assert!(
        super::scan_backups(root.to_str().unwrap()).is_err(),
        "fail-closed"
    );
    let e =
        crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap_err();
    assert!(format!("{e}").contains("fail-closed"), "{e}");
    assert_eq!(
        mem.delete_calls(),
        0,
        "zero deletes while the pending entry blocks"
    );
    assert_lock_released(&root);
    super::backup_prune(&cfg, "bk").unwrap();
    assert!(!root.join(".prune-pending-bk").exists());
    assert!(
        super::scan_backups(root.to_str().unwrap())
            .unwrap()
            .is_empty()
    );
    assert_lock_released(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

/// C. retry removes the pending dir but the SECOND fsync fails: the commit
///    point already passed, so the error is honest while the current view is
///    clean and usable; a resurrected pending entry (power-loss worst case)
///    fail-closes scanner and gc again with zero NEW deletes.
#[test]
fn s433_retry_fsync2_fails_commit_passed_resurrection_blocks() {
    let (cfg, core, mem, root, dir) = pending_only_fixture("p33-retry-fsync2");
    let mut fsync_calls = 0u32;
    let e = super::backup_prune_impl(
        &cfg,
        "bk",
        &mut |_p| {
            fsync_calls += 1;
            if fsync_calls == 2 {
                Err(Error::Storage("injected fsync failure".into()))
            } else {
                Ok(())
            }
        },
        &mut |p: &Path| {
            std::fs::remove_dir_all(p).map_err(|e| Error::Storage(format!("remove: {e}")))
        },
    )
    .unwrap_err();
    assert!(format!("{e}").contains("commit point passed"), "{e}");
    assert!(
        !root.join(".prune-pending-bk").exists(),
        "removed before the failing fsync"
    );
    assert!(
        super::scan_backups(root.to_str().unwrap())
            .unwrap()
            .is_empty()
    );
    assert_lock_released(&root);
    // Power-loss worst case: the pending entry comes back — scanner and gc
    // must fail closed again, with zero NEW object-delete calls.
    std::fs::create_dir_all(root.join(".prune-pending-bk")).unwrap();
    let e = super::scan_backups(root.to_str().unwrap()).unwrap_err();
    assert!(format!("{e}").contains("unfinished backup prune"), "{e}");
    let deletes_before = mem.delete_calls();
    let e =
        crate::cmd::gc_run(&core, Some(root.to_str().unwrap()), 5, 0, now_secs() + 10).unwrap_err();
    assert!(format!("{e}").contains("fail-closed"), "{e}");
    assert_eq!(
        mem.delete_calls(),
        deletes_before,
        "no NEW deletes in the resurrected state"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// D. retry with the REAL ops: converges cleanly, scanner recovers, gc runs
///    end-to-end, lock released.
#[test]
fn s433_retry_clean_path_converges() {
    let (cfg, core, mem, root, dir) = pending_only_fixture("p33-retry-clean");
    super::backup_prune(&cfg, "bk").unwrap();
    assert!(!root.join(".prune-pending-bk").exists() && !root.join("bk").exists());
    assert!(
        super::scan_backups(root.to_str().unwrap())
            .unwrap()
            .is_empty()
    );
    gc_now(&core, &root);
    assert!(
        mem.head("objects/3/w-p33.data").is_err(),
        "gc works again after the retry"
    );
    assert_lock_released(&root);
    let _ = std::fs::remove_dir_all(&dir);
}

#[allow(dead_code)]
fn unused(_: HashSet<String>) {}
