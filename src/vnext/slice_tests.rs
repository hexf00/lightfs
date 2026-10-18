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
                    object_backend: "mem".into(),
                    object_key: "objects/crashtest.data".into(),
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
    core.store().ledger_object("mem", "objects/999/w-orphan.data").unwrap();

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
fn slice7_payload_only_writes_and_content_salvage() {
    let (core, objects) = mk_core("salvage");

    // S4.20 contract: initialization writes ONE ownership marker (its own
    // prefix), and normal commits write NOTHING but payload objects.
    let marker: Vec<String> = list_keys(objects.as_ref(), "namespace/");
    assert_eq!(marker.len(), 1, "exactly one namespace marker: {marker:?}");
    core.mkdir("/dir", 0o755).unwrap();
    core.create("/dir/f", 0o644).unwrap();
    let h = core.open_write("/dir/f", None).unwrap();
    core.write(&h, 0, b"recoverable-bytes").unwrap();
    core.commit_write(&h).unwrap();
    let live_locator = core.lookup("/dir/f").unwrap().object_key.unwrap();
    core.unlink("/dir/f").unwrap();

    let meta: Vec<String> = list_keys(objects.as_ref(), "meta/");
    assert!(meta.is_empty(), "no per-commit meta/ objects may exist: {meta:?}");
    let payload = list_keys(objects.as_ref(), "objects/");
    assert_eq!(payload.len(), 1, "exactly the v1 payload object: {payload:?}");
    // The locator is a bare Object ID — no File ID, no path, none of the
    // legacy objects/<fid>/ layout (three-identity rule).
    let oid = live_locator
        .strip_prefix("objects/")
        .and_then(|r| r.strip_suffix(".data"))
        .unwrap_or_else(|| panic!("locator shape: {live_locator}"));
    assert_eq!(oid.len(), 32);
    assert!(oid.chars().all(|c| c.is_ascii_hexdigit()), "{oid}");

    // SQLite is GONE from this point: content salvage (read-only) is the
    // whole recovery guarantee — bytes plus the diagnostic locator, no
    // namespace and no identity claims parsed out of key shapes.
    let mut collected: Vec<(String, Vec<u8>)> = Vec::new();
    let report = recover::salvage_all_data(objects.as_ref(), &mut |loc: &str, _len: u64, r: &mut dyn std::io::Read| {
        let mut buf = Vec::new();
        std::io::Read::read_to_end(r, &mut buf)?;
        collected.push((loc.to_string(), buf));
        Ok(())
    })
    .unwrap();
    assert_eq!(report.scanned, 1);
    assert!(report.rejected.is_empty(), "{:?}", report.rejected);
    let obj = &report.recovered[0];
    assert_eq!(obj.locator, live_locator, "locator echoed verbatim");
    assert_eq!(collected, vec![(live_locator.clone(), b"recoverable-bytes".to_vec())]);
}

fn list_keys(objects: &dyn crate::object_store::ObjectStore, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut token = None;
    loop {
        let page = objects.list(prefix, token.as_deref(), 100).unwrap();
        for i in &page.items {
            out.push(i.key.clone());
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    out
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


// ---- S4.21: merged atomic write path, three-identity references,
// ---- atomic namespace claim ----

/// The high-level create-or-write path is ONE upload + ONE metadata
/// transaction. The empty-file create commit is gone: the first version
/// of a new file is born inside the same BEGIN IMMEDIATE window that
/// allocates its File ID and binds the object reference.
#[test]
fn s421_merged_create_write_is_single_transaction() {
    let (core, _objects) = mk_core("s421-merged");
    core.mkdir("/d", 0o755).unwrap();
    let tx_after_dirs = core.metrics.metadata_tx.load(std::sync::atomic::Ordering::Relaxed);

    let data = b"atomic-create-bytes".to_vec();
    let mut cur = std::io::Cursor::new(data.clone());
    let node = core
        .write_stream("/d/new-file", None, true, &mut cur, data.len() as u64)
        .unwrap();
    let tx_after_write = core.metrics.metadata_tx.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(tx_after_write - tx_after_dirs, 1, "one transaction per new-file write");
    assert_eq!(node.version, 1);
    assert!(node.id > 0, "File ID allocated inside the transaction");
    assert_eq!(core.read("/d/new-file", 0, 128).unwrap(), data);

    // Overwrite through the same merged path: still one transaction, CAS
    // judged in-transaction.
    let mut cur = std::io::Cursor::new(b"v2".to_vec());
    let node2 = core
        .write_stream("/d/new-file", Some(1), true, &mut cur, 2)
        .unwrap();
    assert_eq!(node2.version, 2);
    let tx3 = core.metrics.metadata_tx.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(tx3 - tx_after_write, 1);

    // CAS violation: explicit Conflict, judged inside the transaction.
    let mut cur = std::io::Cursor::new(b"v3".to_vec());
    let err = core
        .write_stream("/d/new-file", Some(1), true, &mut cur, 2)
        .unwrap_err();
    assert!(format!("{err}").contains("conflict"), "{err}");

    // create=false on a missing path stays NotFound (independent POSIX
    // create keeps its own semantics — untouched).
    let mut cur = std::io::Cursor::new(b"x".to_vec());
    let err = core
        .write_stream("/d/absent", None, false, &mut cur, 1)
        .unwrap_err();
    assert!(format!("{err}").contains("not found"), "{err}");
}

/// A backend without the atomic-claim capability must refuse the unsafe
/// namespace initialization path — never fall back to check-then-put.
#[test]
fn s421_unsupported_claim_backend_refuses_init() {
    use crate::object_store::{ListPage, ObjectData, ObjectInfo, Range};
    struct NoClaim(Arc<MemObjectStore>);
    impl crate::object_store::ObjectStore for NoClaim {
        fn backend_id(&self) -> String {
            self.0.backend_id()
        }
        fn put(&self, k: &str, d: &[u8]) -> crate::error::Result<ObjectInfo> {
            self.0.put(k, d)
        }
        fn put_stream(
            &self,
            k: &str,
            r: &mut dyn std::io::Read,
            l: u64,
        ) -> crate::error::Result<ObjectInfo> {
            self.0.put_stream(k, r, l)
        }
        fn get(&self, k: &str, r: Option<Range>) -> crate::error::Result<ObjectData> {
            self.0.get(k, r)
        }
        fn get_stream(
            &self,
            k: &str,
            r: Option<Range>,
        ) -> crate::error::Result<crate::object_store::StreamedGet> {
            self.0.get_stream(k, r)
        }
        fn head(&self, k: &str) -> crate::error::Result<ObjectInfo> {
            self.0.head(k)
        }
        fn delete(&self, k: &str) -> crate::error::Result<()> {
            self.0.delete(k)
        }
        fn list(&self, p: &str, t: Option<&str>, m: usize) -> crate::error::Result<ListPage> {
            self.0.list(p, t, m)
        }
        fn ping(&self) -> crate::error::Result<()> {
            self.0.ping()
        }
        // put_if_absent + supports_atomic_claim: intentionally the trait
        // DEFAULTS (refusing / false).
    }

    let dir = std::env::temp_dir().join(format!(
        "lfs-vnext-s421-noclaim-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("meta.db").to_string_lossy()).unwrap();
    let objects = Arc::new(NoClaim(Arc::new(MemObjectStore::new())));
    let err = Core::new(store, objects).map(|_| ()).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("atomically claim"), "{msg}");
    // Nothing was written to the store during the refused init.
    assert!(msg.contains("refusing namespace initialization"), "{msg}");
}

/// Two fresh databases racing on ONE unclaimed in-memory namespace:
/// the claim is atomic, so exactly one Core wins and the other refuses.
#[test]
fn s421_namespace_claim_race_threads_exactly_one_winner() {
    let objects = Arc::new(MemObjectStore::new_named("mem:race"));
    let dir = std::env::temp_dir().join(format!(
        "lfs-vnext-s421-race-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let dbs: Vec<String> = (0..2)
        .map(|i| dir.join(format!("db{i}.sqlite")).to_string_lossy().to_string())
        .collect();
    let objs1 = objects.clone();
    let objs2 = objects.clone();
    let db1 = dbs[0].clone();
    let db2 = dbs[1].clone();
    let h1 = std::thread::spawn(move || {
        Core::new(Store::open(&db1).unwrap(), objs1).is_ok()
    });
    let h2 = std::thread::spawn(move || {
        Core::new(Store::open(&db2).unwrap(), objs2).is_ok()
    });
    let winners = [h1.join().unwrap(), h2.join().unwrap()];
    assert_eq!(
        winners.iter().filter(|w| **w).count(),
        1,
        "exactly one database may claim the namespace: {winners:?}"
    );
    // The marker carries the winner's id; the loser's database stays
    // unusable against this namespace (mismatch on every later open).
    let marker = objects.get("namespace/marker", None).unwrap();
    let (_, id) = crate::vnext::envelope::open(&marker.data).unwrap();
    let winner_id = String::from_utf8(id).unwrap();
    let loser_db = if winners[0] { &dbs[1] } else { &dbs[0] };
    let err = Core::new(Store::open(loser_db).unwrap(), objects.clone())
        .map(|_| ())
        .unwrap_err();
    // Either refusal is correct: the loser saw the winner's marker before
    // writing its own local id ((None, Some) arm) or after (mismatch arm).
    // What matters: it is refused, never co-rooted.
    let msg = format!("{err}");
    assert!(
        msg.contains("namespace mismatch") || msg.contains("another namespace's marker"),
        "{msg}"
    );
    // The winner reopens cleanly.
    let winner_db = if winners[0] { &dbs[0] } else { &dbs[1] };
    assert!(Core::new(Store::open(winner_db).unwrap(), objects.clone()).is_ok());
    assert_eq!(
        Store::open(winner_db).unwrap().namespace_id().unwrap().as_deref(),
        Some(winner_id.as_str())
    );
}

/// Concurrent create-or-writes of the SAME path: writers serialize in
/// BEGIN IMMEDIATE, every commit builds on the committed head — no lost
/// updates, no corruption; final version == number of winners.
#[test]
fn s421_concurrent_same_path_writes_serialize() {
    let (core, _objects) = mk_core("s421-racepath");
    core.mkdir("/c", 0o755).unwrap();
    let core = std::sync::Arc::new(core);
    let mut handles = Vec::new();
    for i in 0..8 {
        let c = core.clone();
        handles.push(std::thread::spawn(move || {
            let data = format!("writer-{i}").into_bytes();
            let mut cur = std::io::Cursor::new(data.clone());
            c.write_stream("/c/hot", None, true, &mut cur, data.len() as u64)
                .map(|_| ())
        }));
    }
    let mut oks = 0usize;
    let mut conflicts = 0usize;
    for h in handles {
        match h.join().unwrap() {
            Ok(()) => oks += 1,
            Err(e) => {
                let m = format!("{e}");
                assert!(m.contains("conflict"), "unexpected error: {m}");
                conflicts += 1;
            }
        }
    }
    assert_eq!(oks + conflicts, 8);
    assert!(oks >= 1);
    let node = core.lookup("/c/hot").unwrap();
    assert_eq!(node.version as usize, oks, "every winner committed exactly one version");
    // Read-your-bytes: the head content is one of the writers' payloads.
    let content = core.read("/c/hot", 0, 128).unwrap();
    let valid: Vec<Vec<u8>> = (0..8).map(|i| format!("writer-{i}").into_bytes()).collect();
    assert!(valid.contains(&content), "head content is some writer's bytes: {content:?}");
    assert_eq!(core.metrics.conflicts.load(std::sync::atomic::Ordering::Relaxed) as usize, conflicts);
}

/// Object identity is independent of File ID: the locator never embeds
/// it, and a reference bound to another backend is refused on read
/// (fail-closed misrouting) instead of silently mis-read.
#[test]
fn s421_reference_names_backend_and_refuses_misroute() {
    let src = Arc::new(MemObjectStore::new_named("mem:alpha"));
    let dir = std::env::temp_dir().join(format!(
        "lfs-vnext-s421-ref-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let db = dir.join("meta.db").to_string_lossy().to_string();
    {
        let store = Store::open(&db).unwrap();
        let core = Core::new(store, src.clone()).unwrap();
        let data = b"located-elsewhere".to_vec();
        let mut cur = std::io::Cursor::new(data.clone());
        core.write_stream("/f", None, true, &mut cur, data.len() as u64)
            .unwrap();
        let locator = core.lookup("/f").unwrap().object_key.unwrap();
        let oid = locator
            .strip_prefix("objects/")
            .and_then(|r| r.strip_suffix(".data"))
            .unwrap_or_else(|| panic!("locator shape: {locator}"));
        assert_eq!(oid.len(), 32);
        assert!(oid.chars().all(|c| c.is_ascii_hexdigit()), "{oid}");
    }
    // A DIFFERENT backend instance receives a byte-copy of everything
    // (marker included) — the namespace check passes, but every persisted
    // reference names mem:alpha, and reads must refuse.
    let dst = Arc::new(MemObjectStore::new_named("mem:beta"));
    let mut token = None;
    loop {
        let page = src.list("", token.as_deref(), 1000).unwrap();
        for i in &page.items {
            let obj = src.get(&i.key, None).unwrap();
            dst.put(&i.key, &obj.data).unwrap();
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    let store = Store::open(&db).unwrap();
    let core = Core::new(store, dst).unwrap();
    let err = core.read("/f", 0, 64).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("mem:alpha") && msg.contains("mem:beta"), "{msg}");
    assert!(msg.contains("refusing to read across backends"), "{msg}");
}

/// put_if_absent claim semantics on a real atomic backend.
#[test]
fn s421_put_if_absent_claim_semantics() {
    use crate::object_store::ObjectStore as _;
    let s = MemObjectStore::new_named("mem:claim");
    assert!(s.supports_atomic_claim());
    assert!(s.put_if_absent("ns/marker", b"one").unwrap());
    assert!(!s.put_if_absent("ns/marker", b"two").unwrap());
    // The loser changed nothing.
    assert_eq!(s.get("ns/marker", None).unwrap().data, b"one");
}

// ---- S4.22: transactional registration & backend-aware gc ledger ----

/// A CONFIRMED logical failure (CAS conflict) rolls the main transaction
/// back, re-registers the uploaded object in an INDEPENDENT transaction,
/// and still returns the REAL error. The orphan is then provably dead
/// for gc; the committed file is untouched.
#[test]
fn s422_conflict_reregisters_uploaded_orphan_gable() {
    let (core, objects) = mk_core("s422-conflict");
    core.create("/f", 0o644).unwrap();
    let h1 = core.open_write("/f", None).unwrap();
    core.write(&h1, 0, b"winner").unwrap();
    core.commit_write(&h1).unwrap();

    let before: std::collections::HashSet<_> = core.store().known_objects().unwrap().into_iter().collect();
    // A STALE session: snapshot taken at version 0, commit attempts v1.
    let h2 = core.open_write("/f", Some(0)).unwrap();
    core.write(&h2, 0, b"loser").unwrap();
    let err = core.commit_write(&h2).unwrap_err();
    assert!(format!("{err}").contains("conflict"), "real error returned: {err}");

    // The loser's upload became a ledgered orphan: exactly one new row.
    let known = core.store().known_objects().unwrap();
    let new_rows: Vec<_> = known
        .iter()
        .filter(|p| !before.contains(*p))
        .cloned()
        .collect();
    assert_eq!(new_rows.len(), 1, "exactly the re-registered orphan: {new_rows:?}");
    let (_, locator) = new_rows[0].clone();
    assert!(
        objects.get(&locator, None).is_ok(),
        "the uploaded loser object exists"
    );
    // gc reclaims exactly the orphan; the winner is untouched.
    let rep = crate::vnext::gc::collect(&core).unwrap();
    assert_eq!(rep.deleted, 1, "{rep:?}");
    assert!(objects.get(&locator, None).is_err(), "orphan reclaimed");
    assert_eq!(core.read("/f", 0, 64).unwrap(), b"winner");
}

/// The ledger is BACKEND-QUALIFIED: the same locator under two backends
/// is two rows, never conflated; forgetting one leaves the other.
#[test]
fn s422_ledger_is_backend_qualified() {
    let dir = std::env::temp_dir().join(format!(
        "lfs-vnext-s422-ledger-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("meta.db").to_string_lossy()).unwrap();
    store.ledger_object("mem-a", "objects/dup.data").unwrap();
    store.ledger_object("mem-b", "objects/dup.data").unwrap();
    let known = store.known_objects().unwrap();
    assert_eq!(known.len(), 2, "same locator, different backends: {known:?}");
    assert!(known.contains(&("mem-a".into(), "objects/dup.data".into())));
    assert!(known.contains(&("mem-b".into(), "objects/dup.data".into())));
    store.forget_object("mem-a", "objects/dup.data").unwrap();
    let known = store.known_objects().unwrap();
    assert_eq!(known, vec![("mem-b".into(), "objects/dup.data".into())]);
}

/// gc refuses to delete rows bound to a backend it cannot see — and
/// never reinterprets them as belonging to the configured store.
#[test]
fn s422_gc_keeps_foreign_backend_rows() {
    let (core, objects) = mk_core("s422-foreign");
    core.create("/f", 0o644).unwrap();
    let h = core.open_write("/f", None).unwrap();
    core.write(&h, 0, b"live").unwrap();
    core.commit_write(&h).unwrap();
    // Our own orphan (gc-eligible) and someone else's ledger row.
    objects.put("objects/ours-orphan.data", b"o").unwrap();
    core.store().ledger_object("mem", "objects/ours-orphan.data").unwrap();
    core.store().ledger_object("another-backend", "objects/dup.data").unwrap();

    let rep = crate::vnext::gc::collect(&core).unwrap();
    assert_eq!(rep.deleted, 1, "exactly the current-backend orphan: {rep:?}");
    assert_eq!(rep.foreign_kept, 1, "foreign row kept, never deleted: {rep:?}");
    assert!(objects.get("objects/ours-orphan.data", None).is_err());
    // The foreign row is still in the ledger (untouched authority).
    let known = core.store().known_objects().unwrap();
    assert!(known.contains(&("another-backend".into(), "objects/dup.data".into())));
}

/// Success-path registration is IN the commit transaction: a rolled
/// back step leaves NEITHER a file version NOR a ledger row (atomicity
/// observed from outside; the cross-process kill-window test covers the
/// process-death variant).
#[test]
fn s422_registration_is_inside_the_commit_transaction() {
    let (core, _objects) = mk_core("s422-atomic");
    core.create("/f", 0o644).unwrap();
    let known_before = core.store().known_objects().unwrap().len();

    // A step error AFTER the in-tx registration must roll BOTH back.
    // Drive it through commit_write with a kind conflict: turn /f into a
    // directory-like mismatch by writing through a path that now names a
    // different file id (rename swap). Simplest deterministic in-tx
    // failure after registration: CAS conflict.
    let h1 = core.open_write("/f", None).unwrap();
    core.write(&h1, 0, b"winner").unwrap();
    core.commit_write(&h1).unwrap();
    let after_winner: std::collections::HashSet<_> =
        core.store().known_objects().unwrap().into_iter().collect();
    assert_eq!(after_winner.len(), known_before + 1, "winner registered in-tx");
    let h2 = core.open_write("/f", Some(0)).unwrap();
    core.write(&h2, 0, b"loser").unwrap();
    assert!(core.commit_write(&h2).is_err());

    let known = core.store().known_objects().unwrap();
    let new_rows: Vec<_> = known
        .iter()
        .filter(|p| !after_winner.contains(*p))
        .cloned()
        .collect();
    assert_eq!(
        new_rows.len(),
        1,
        "only the re-registered orphan is new; the rolled-back in-tx registration left nothing"
    );
    // No phantom version: /f is still at version 1 with winner bytes.
    assert_eq!(core.lookup("/f").unwrap().version, 1);
    assert_eq!(core.read("/f", 0, 64).unwrap(), b"winner");
}
