//! S4.19 slice acceptance tests (§六): history stability, branch
//! isolation, concurrent-commit conflict detection, gc reference roots,
//! and the object-store-only recovery view. The cross-process crash and
//! real-server workflows live in tests/vnext_slice.rs.

use std::sync::Arc;

use super::core::Core;
use super::gc;
use super::recover;
use super::store::{Store, MODEL_MARKER};
use crate::error::Error;
use crate::object_store::mem::MemObjectStore;
use crate::object_store::ObjectStore as _;

fn mk_core(tag: &str) -> (Core, Arc<MemObjectStore>) {
    let dir = std::env::temp_dir().join(format!(
        "lfs-vnext-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(dir.join("meta.db").to_str().unwrap()).unwrap();
    let objects = Arc::new(MemObjectStore::new());
    (Core::new(store, objects.clone()).unwrap(), objects)
}

#[test]
fn slice1_init_and_reads() {
    let (core, _objects) = mk_core("init");
    let root = core.lookup("/").unwrap();
    assert!(root.is_dir());
    assert!(core.readdir("/").unwrap().is_empty());
    core.mkdir("/d", 0o755).unwrap();
    core.create("/d/f", 0o644).unwrap();
    let f = core.lookup("/d/f").unwrap();
    assert!(f.is_file());
    assert_eq!(f.version, 0);
    let names: Vec<String> = core.readdir("/d").unwrap().into_iter().map(|n| n.name).collect();
    assert_eq!(names, vec!["f"]);
}

#[test]
fn slice2_commit_updates_and_fixed_history_reads() {
    let (core, _objects) = mk_core("hist");
    core.create("/f", 0o644).unwrap();
    // 100+ commits; record every commit id as we go.
    let mut commit_ids = Vec::new();
    for i in 0..120u64 {
        let h = core.open_write("/f", None).unwrap();
        let expected = h.expected_version;
        core.write(&h, 0, format!("content-{i}").as_bytes()).unwrap();
        let node = core.commit_write(&h).unwrap();
        assert_eq!(node.version, expected + 1);
        // The commit created by THIS write is the new head.
        commit_ids.push(core.store().head("main").unwrap().unwrap().commit_id);
    }
    assert_eq!(commit_ids.len(), 120);
    // History is immutable: each recorded commit still sees its OWN
    // content, after the mainline has moved far ahead.
    for (i, cid) in commit_ids.iter().enumerate() {
        let bytes = core.read_at(*cid, "/f", 0, 1 << 20).unwrap();
        assert_eq!(
            bytes,
            format!("content-{i}").as_bytes(),
            "commit {cid} (write #{i}) changed under mainline"
        );
        let stat = core.lookup_at(*cid, "/f").unwrap();
        assert_eq!(stat.version, (i + 1) as i64);
    }
    // Head reads always see the latest.
    let head = core.read("/f", 0, 1 << 20).unwrap();
    assert_eq!(head, b"content-119");
}

#[test]
fn slice3_branch_fork_isolation() {
    let (core, _objects) = mk_core("branch");
    core.create("/shared", 0o644).unwrap();
    let h = core.open_write("/shared", None).unwrap();
    core.write(&h, 0, b"base").unwrap();
    core.commit_write(&h).unwrap();
    let fork_commit = core.store().head("main").unwrap().unwrap().commit_id;

    // Branch = zero-copy ref creation.
    core.store().create_ref("b1", fork_commit).unwrap();

    // Mainline moves; the branch state must not move with it.
    let h = core.open_write("/shared", None).unwrap();
    core.write(&h, 0, b"mainline-new").unwrap();
    core.commit_write(&h).unwrap();
    core.create("/only-main", 0o644).unwrap();

    assert_eq!(core.read_at(fork_commit, "/shared", 0, 16).unwrap(), b"base");
    assert!(core.lookup("/only-main").is_ok());
    // The branch never sees mainline-only files or contents.
    let branch_view = core.lookup_at(fork_commit, "/only-main");
    assert!(matches!(branch_view, Err(Error::NotFound(_))));
}

#[test]
fn slice4_concurrent_commits_conflict_not_overwrite() {
    let (core, _objects) = mk_core("conflict");
    core.create("/f", 0o644).unwrap();
    let h1 = core.open_write("/f", None).unwrap();
    let h2 = core.open_write("/f", None).unwrap();
    core.write(&h1, 0, b"writer-1").unwrap();
    core.write(&h2, 0, b"writer-2").unwrap();
    core.commit_write(&h1).unwrap(); // wins: version 1
    let err = core.commit_write(&h2).unwrap_err(); // MUST conflict loudly
    match err {
        Error::Conflict { expected, actual, .. } => {
            assert_eq!((expected, actual), (0, 1));
        }
        other => panic!("expected Conflict, got {other}"),
    }
    // The winner's bytes are exactly what the path serves.
    assert_eq!(core.read("/f", 0, 64).unwrap(), b"writer-1");

    // Branch-head CAS: two racers against the same expected head —
    // exactly one wins, the other gets an explicit Conflict.
    let head = core.store().head("main").unwrap().unwrap();
    core.store().create_ref("race", head.commit_id).unwrap();
    let new_commit = {
        // any real commit id (the mainline head after one more write)
        let h = core.open_write("/f", None).unwrap();
        core.write(&h, 0, b"next").unwrap();
        core.commit_write(&h).unwrap();
        core.store().head("main").unwrap().unwrap().commit_id
    };
    core.store().advance_ref("race", head.commit_id, new_commit).unwrap();
    let err = core
        .store()
        .advance_ref("race", head.commit_id, new_commit)
        .unwrap_err();
    assert!(matches!(err, Error::Conflict { .. }), "{err}");
}

#[test]
fn slice5_crash_window_leaves_recoverable_state() {
    // SQLite transaction atomicity covers the metadata window: simulate a
    // crash by panicking INSIDE the transaction, then REOPEN THE FILE the
    // way a restarted server would (fresh connection — a real crash kills
    // the process, so the poisoned in-process mutex is a test artifact).
    let dir = std::env::temp_dir().join(format!(
        "lfs-vnext-crash-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db_path = dir.join("meta.db").to_string_lossy().to_string();
    let store = Store::open(&db_path).unwrap();
    let objects = Arc::new(MemObjectStore::new());
    let core = Core::new(store, objects.clone()).unwrap();
    core.create("/f", 0o644).unwrap();
    let before_head = core.store().head("main").unwrap().unwrap().commit_id;
    let before = core.lookup("/f").unwrap().version;

    let store_for_panic = core.store_arc();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let store = store_for_panic;
        let _ = store.with_tx(|conn| {
            use crate::vnext::store::FileVersionRow;
            crate::vnext::store::Store::insert_file_version_tx(
                conn,
                &FileVersionRow {
                    file_id: 1,
                    version: before + 1,
                    size: 1,
                    mtime: 0,
                    mode: 0o644,
                    object_key: "objects/1/w-crash.data".into(),
                    etag: "x".into(),
                },
            )?;
            panic!("simulated crash mid-commit");
            #[allow(unreachable_code)]
            Ok::<(), crate::error::Error>(())
        });
    }));
    assert!(result.is_err(), "the simulated crash should have panicked");

    // Reopen the database FILE (fresh connection = restarted server):
    // state is exactly the pre-crash state — no partial commit, no
    // ref/commit disagreement.
    let store2 = Store::open(&db_path).unwrap();
    let core2 = Core::new(store2, objects).unwrap();
    assert_eq!(core2.store().head("main").unwrap().unwrap().commit_id, before_head);
    assert_eq!(core2.lookup("/f").unwrap().version, before);
}

#[test]
fn slice6_gc_respects_reference_roots() {
    let (core, objects) = mk_core("gc");
    core.create("/f", 0o644).unwrap();
    let h = core.open_write("/f", None).unwrap();
    core.write(&h, 0, b"v1-bytes").unwrap();
    core.commit_write(&h).unwrap();
    let live_key = core.lookup("/f").unwrap().object_key.clone().unwrap();

    // Overwrite: v1's object becomes unreachable from main.
    let h = core.open_write("/f", None).unwrap();
    core.write(&h, 0, b"v2-bytes").unwrap();
    core.commit_write(&h).unwrap();

    // A branch still points at the v1 commit: v1 is LIVE for that root.
    // It is the parent of the current head.
    let head = core.store().head("main").unwrap().unwrap();
    let head_row = core.store().get_commit(head.commit_id).unwrap();
    let v1_commit = head_row.parent_id.unwrap();
    core.store().create_ref("keeper", v1_commit).unwrap();

    // A ledgered-but-unreferenced object (simulated failed-write
    // leftover): known to the ledger, reachable from nothing.
    objects
        .put("objects/999/w-orphan.data", b"garbage")
        .unwrap();
    core.store().ledger_object("objects/999/w-orphan.data").unwrap();

    let report = gc::collect(&core).unwrap();
    assert_eq!(report.deleted, 1, "only the true orphan is collected");
    assert_eq!(report.failed, 0);
    // The orphan is gone.
    assert!(objects.get("objects/999/w-orphan.data", None).is_err());
    // Live mainline data survives.
    assert_eq!(core.read("/f", 0, 64).unwrap(), b"v2-bytes");
    // Branch-referenced history survives.
    let v1 = core.read_at(v1_commit, "/f", 0, 64).unwrap();
    assert_eq!(v1, b"v1-bytes");
    assert!(
        objects.get(&live_key, None).is_ok(),
        "v1 object must survive while a ref holds it"
    );
}

#[test]
fn slice7_recovery_view_from_objects_alone() {
    let (core, objects) = mk_core("recover");
    core.mkdir("/dir", 0o755).unwrap();
    core.create("/dir/f", 0o644).unwrap();
    let h = core.open_write("/dir/f", None).unwrap();
    core.write(&h, 0, b"recoverable-bytes").unwrap();
    core.commit_write(&h).unwrap();

    // What the object store holds for meta/ (diagnostics on failure):
    let keys: Vec<String> = {
        let mut out = Vec::new();
        let mut token = None;
        loop {
            let page = objects.list("meta/", token.as_deref(), 100).unwrap();
            for i in &page.items {
                out.push(i.key.clone());
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        out
    };

    // SQLite is GONE from this point: only the object store remains.
    let view = match recover::namespace_view(objects.as_ref()) {
        Ok(v) => v,
        Err(e) => panic!("namespace_view failed (meta keys: {keys:?}): {e}"),
    };
    assert_eq!(view.refs.len(), 1);
    assert_eq!(view.refs[0].name, "main");
    // Recovery folds EVERY reachable tree, so historical versions appear
    // next to the live one; the namespace's CURRENT content is the max
    // version (order among equal paths is not stable — HashSet iteration).
    let f = view
        .files
        .iter()
        .filter(|f| f.path == "/dir/f")
        .max_by_key(|f| f.version)
        .unwrap_or_else(|| panic!("recovered namespace must contain /dir/f (meta keys: {keys:?})"));
    assert_eq!(f.size, 17);
    assert_eq!(f.version, 1);
    // The recovered object reference actually reads back the bytes.
    let obj = objects.get(&f.object_key, None).unwrap();
    let (kind, content) = crate::vnext::envelope::open(&obj.data).unwrap();
    assert_eq!(kind, crate::vnext::envelope::ObjKind::Data);
    assert_eq!(content, b"recoverable-bytes");
    // Salvage without any metadata at all still finds the content.
    let salvaged = recover::salvage_all_data(objects.as_ref()).unwrap();
    assert!(salvaged.iter().any(|(_, b)| b == b"recoverable-bytes"));
    assert!(view.unpublished_commits.is_empty());
}

#[test]
fn slice8_legacy_db_refused_never_cleared() {
    let dir = std::env::temp_dir().join(format!(
        "lfs-vnext-legacy-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("legacy.db").to_string_lossy().to_string();
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE nodes (id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT);
             INSERT INTO nodes (version) VALUES ('1');",
        )
        .unwrap();
    }
    let err = match Store::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("legacy db must be refused"),
    };
    let msg = format!("{err}");
    assert!(msg.contains("legacy v0.1.x"), "{msg}");
    // Untouched: the legacy table survives exactly as it was.
    let conn = rusqlite::Connection::open(&path).unwrap();
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM nodes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 1, "refusal must not clear or modify user data");
    // A FOREIGN database (tables, no known marker) is refused too.
    let path2 = dir.join("foreign.db").to_string_lossy().to_string();
    {
        let conn = rusqlite::Connection::open(&path2).unwrap();
        conn.execute_batch("CREATE TABLE stuff (x INTEGER); INSERT INTO stuff VALUES (7);")
            .unwrap();
    }
    assert!(Store::open(&path2).is_err());
    // Fresh file initializes with the marker + main ref.
    let path3 = dir.join("fresh.db").to_string_lossy().to_string();
    let store = Store::open(&path3).unwrap();
    let marker: String = store
        .read(|conn| {
            conn.query_row("SELECT value FROM model WHERE key='model'", [], |r| r.get(0))
                .map_err(crate::error::Error::from)
        })
        .unwrap();
    assert_eq!(marker, MODEL_MARKER);
    assert!(store.head("main").unwrap().is_some());
}

