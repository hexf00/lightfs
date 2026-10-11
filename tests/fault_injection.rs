//! Phase 2 fault injection at the critical commit points.
//!
//! Uses test-only wrappers around the ObjectStore / MetadataStore traits —
//! no production code is modified and no hooks enter release builds.
//!
//! Covered commit points:
//!   1. object upload fails before starting     -> nothing committed
//!   2. streaming upload fails midway           -> metadata untouched, no
//!      partial/orphan version object
//!   3. upload succeeds, metadata CAS fails     -> old content/version stand;
//!      losing writer's version key removed (no orphan)
//!   4. CAS succeeded but response lost         -> state consistent; stale
//!      retry conflicts, fresh retry succeeds
//!   5. crash between upload and CAS            -> orphan/stale version key
//!      is detected by fsck
//!
//! NOT covered here (needs a seam inside S3ObjectStore — reported as a
//! Phase-2b proposal): failing an individual multipart PART/COMPLETE against
//! the real S3 API, and abort-failure observability against a real backend.

use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use lightfs::core::Core;
use lightfs::error::{Error, Result};
use lightfs::metadata::{MetadataStore, Node, NodeKind, sqlite::SqliteMetadataStore};
use lightfs::object_store::{
    ListPage, ObjectData, ObjectInfo, ObjectStore, Range, mem::MemObjectStore,
};

// ---------------------------------------------------------------- fixtures

#[derive(Default)]
struct FaultFlags {
    fail_put: AtomicBool,
    fail_put_stream: AtomicBool,
    fail_put_stream_midway: AtomicBool,
}

/// ObjectStore wrapper that injects failures at the selected commit points.
struct InjectingStore {
    inner: Arc<dyn ObjectStore>,
    faults: Arc<FaultFlags>,
}

impl ObjectStore for InjectingStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectInfo> {
        if self.faults.fail_put.load(Ordering::SeqCst) {
            return Err(Error::Storage("injected: put failure".into()));
        }
        self.inner.put(key, data)
    }

    fn put_stream(&self, key: &str, reader: &mut dyn Read, len: u64) -> Result<ObjectInfo> {
        if self.faults.fail_put_stream.load(Ordering::SeqCst) {
            return Err(Error::Storage("injected: put_stream failure".into()));
        }
        if self.faults.fail_put_stream_midway.load(Ordering::SeqCst) {
            // consume part of the stream, then fail: the store must not
            // present the partial object as complete
            let mut buf = vec![0u8; 1024];
            let _ = reader.read(&mut buf)?;
            return Err(Error::Storage("injected: midway stream failure".into()));
        }
        self.inner.put_stream(key, reader, len)
    }

    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData> {
        self.inner.get(key, range)
    }

    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<(Box<dyn Read + Send>, u64)> {
        self.inner.get_stream(key, range)
    }

    fn head(&self, key: &str) -> Result<ObjectInfo> {
        self.inner.head(key)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }

    fn list(&self, prefix: &str, token: Option<&str>, max: usize) -> Result<ListPage> {
        self.inner.list(prefix, token, max)
    }

    fn ping(&self) -> Result<()> {
        self.inner.ping()
    }
}

/// MetadataStore wrapper: fails the content CAS while reads keep working.
struct FaultyMeta {
    inner: SqliteMetadataStore,
    fail_cas: AtomicBool,
    cas_calls: AtomicU64,
}

impl MetadataStore for FaultyMeta {
    fn create_node(
        &self,
        parent_id: i64,
        name: &str,
        kind: NodeKind,
        mode: u32,
        object_key: Option<&str>,
    ) -> Result<Node> {
        self.inner
            .create_node(parent_id, name, kind, mode, object_key)
    }
    fn get_node(&self, id: i64) -> Result<Option<Node>> {
        self.inner.get_node(id)
    }
    fn get_child(&self, parent_id: i64, name: &str) -> Result<Option<Node>> {
        self.inner.get_child(parent_id, name)
    }
    fn list_children(&self, parent_id: i64) -> Result<Vec<Node>> {
        self.inner.list_children(parent_id)
    }
    fn count_children(&self, parent_id: i64) -> Result<i64> {
        self.inner.count_children(parent_id)
    }
    fn commit_content_cas(
        &self,
        id: i64,
        expected_version: i64,
        size: i64,
        mtime: i64,
        new_object_key: &str,
        etag: &str,
    ) -> Result<()> {
        self.cas_calls.fetch_add(1, Ordering::SeqCst);
        if self.fail_cas.load(Ordering::SeqCst) {
            return Err(Error::Storage("injected: CAS failure".into()));
        }
        self.inner
            .commit_content_cas(id, expected_version, size, mtime, new_object_key, etag)
    }
    fn update_attrs(&self, id: i64, mode: u32, mtime: i64) -> Result<()> {
        self.inner.update_attrs(id, mode, mtime)
    }
    fn rename_node(&self, id: i64, new_parent_id: i64, new_name: &str) -> Result<()> {
        self.inner.rename_node(id, new_parent_id, new_name)
    }
    fn delete_node(&self, id: i64) -> Result<()> {
        self.inner.delete_node(id)
    }
    fn all_nodes(&self) -> Result<Vec<Node>> {
        self.inner.all_nodes()
    }
    fn record_superseded(&self, object_key: &str, at: i64) -> Result<()> {
        self.inner.record_superseded(object_key, at)
    }
    fn gc_candidates(&self, cutoff: i64, limit: usize) -> Result<Vec<String>> {
        self.inner.gc_candidates(cutoff, limit)
    }
    fn drop_superseded(&self, object_key: &str) -> Result<()> {
        self.inner.drop_superseded(object_key)
    }
    fn all_superseded(&self) -> Result<Vec<String>> {
        self.inner.all_superseded()
    }
}

fn setup(faults: Arc<FaultFlags>) -> (Arc<Core>, Arc<FaultyMeta>, Arc<InjectingStore>) {
    let objects: Arc<InjectingStore> = Arc::new(InjectingStore {
        inner: Arc::new(MemObjectStore::new()),
        faults,
    });
    let meta: Arc<FaultyMeta> = Arc::new(FaultyMeta {
        inner: SqliteMetadataStore::open_in_memory().unwrap(),
        fail_cas: AtomicBool::new(false),
        cas_calls: AtomicU64::new(0),
    });
    let core = Arc::new(Core::new(
        meta.clone() as Arc<dyn MetadataStore>,
        objects.clone(),
    ));
    (core, meta, objects)
}

fn version_keys(core: &Core, file_id: i64) -> Vec<String> {
    core.objects()
        .list(&format!("objects/{file_id}/"), None, 100)
        .unwrap()
        .items
        .iter()
        .map(|i| i.key.clone())
        .collect()
}

// ---------------------------------------------------------------- tests

/// 1. upload fails before starting: error surfaces, metadata and object
///    store keep the pre-write state exactly.
#[test]
fn upload_fails_before_start_nothing_committed() {
    // commit_write assembles the content and uploads via a single signed PUT
    let faults = Arc::new(FaultFlags::default());
    let (core, _meta, _objects) = setup(faults.clone());
    core.create("/f", 0o644).unwrap();
    let before = core.lookup("/f").unwrap();
    faults.fail_put.store(true, Ordering::SeqCst);

    let h = core.open_write("/f", None).unwrap();
    core.write(&h, 0, b"data").unwrap();
    let err = core.commit_write(&h).unwrap_err();
    assert!(matches!(err, Error::Storage(_)));

    let after = core.lookup("/f").unwrap();
    assert_eq!(after.version, before.version);
    assert_eq!(after.size, before.size);
    // metadata-only create commits NO object: a failed first write leaves
    // exactly zero version keys - nothing to clean up, nothing to confuse
    assert_eq!(version_keys(&core, after.id).len(), 0);
}

/// 2. streaming upload fails midway: metadata untouched, no partial/orphan
///    version object left in the store.
#[test]
fn stream_fails_midway_no_partial_object() {
    let faults = Arc::new(FaultFlags::default());
    faults.fail_put_stream_midway.store(true, Ordering::SeqCst);
    let (core, _meta, objects) = setup(faults);
    core.create("/f", 0o644).unwrap();
    let before = core.lookup("/f").unwrap();

    let err = core
        .write_stream(
            "/f",
            Some(before.version),
            false,
            &mut std::io::empty(),
            100,
        )
        .unwrap_err();
    assert!(matches!(err, Error::Storage(_)));

    let after = core.lookup("/f").unwrap();
    assert_eq!(after.version, before.version);
    assert_eq!(after.size, before.size);
    // metadata-only create commits NO object: a failed stream leaves
    // exactly zero keys (no partial object, no placeholder)
    let remaining = objects
        .list(&format!("objects/{}/", after.id), None, 100)
        .unwrap()
        .items;
    assert_eq!(remaining.len(), 0);
    assert!(after.object_key.is_none());
}

/// 3. upload succeeds, metadata CAS fails: the previously committed content
///    and version stand; the losing writer's version key is removed (no
///    orphan), leaving exactly one live key.
#[test]
fn cas_failure_keeps_old_content_and_cleans_up() {
    let (core, meta, _objects) = setup(Arc::new(FaultFlags::default()));
    core.create("/f", 0o644).unwrap();
    let h = core.open_write("/f", None).unwrap();
    core.write(&h, 0, b"original").unwrap();
    let before = core.commit_write(&h).unwrap();

    meta.fail_cas.store(true, Ordering::SeqCst);
    let h2 = core.open_write("/f", None).unwrap();
    core.write(&h2, 0, b"lost update").unwrap();
    let err = core.commit_write(&h2).unwrap_err();
    assert!(matches!(err, Error::Storage(_)), "{err}");

    meta.fail_cas.store(false, Ordering::SeqCst);
    let after = core.lookup("/f").unwrap();
    assert_eq!(after.version, before.version);
    assert_eq!(core.read("/f", 0, 100).unwrap(), b"original");
    let remaining = version_keys(&core, after.id);
    // Retention keeps superseded history; the CAS-failed loser's key must
    // be gone from BOTH the store's unrecorded set and the retained set.
    let retained: std::collections::HashSet<String> =
        meta.inner.all_superseded().unwrap().into_iter().collect();
    let unrecorded: Vec<&String> = remaining
        .iter()
        .filter(|k| !retained.contains(*k))
        .collect();
    assert_eq!(unrecorded.len(), 1, "loser version key must be removed");
    assert_eq!(unrecorded[0], &after.object_key.clone().unwrap());
}

/// 4. CAS succeeded but the response was lost (disconnect/crash after
///    commit). Contract: the visible state is a complete consistent write;
///    a retry carrying the STALE expected version conflicts instead of
///    corrupting; a retry from the current version succeeds.
#[test]
fn cas_committed_response_lost_is_consistent() {
    let (core, _meta, _objects) = setup(Arc::new(FaultFlags::default()));
    core.create("/f", 0o644).unwrap();
    let v0 = core.lookup("/f").unwrap().version;

    // commit succeeds server-side; the outcome is unknown to the client
    let h = core.open_write("/f", Some(v0)).unwrap();
    core.write(&h, 0, b"committed but unacknowledged").unwrap();
    let committed = core.commit_write(&h).unwrap();

    let node = core.lookup("/f").unwrap();
    assert_eq!(node.version, v0 + 1);
    assert_eq!(node.size, committed.size);
    assert_eq!(
        core.read("/f", 0, 100).unwrap(),
        b"committed but unacknowledged"
    );

    // stale retry conflicts — it cannot corrupt the committed state
    let h_stale = core.open_write("/f", Some(v0)).unwrap();
    core.write(&h_stale, 0, b"stale retry").unwrap();
    assert!(matches!(
        core.commit_write(&h_stale),
        Err(Error::Conflict { .. })
    ));
    assert_eq!(
        core.read("/f", 0, 100).unwrap(),
        b"committed but unacknowledged"
    );

    // retry from the current version succeeds
    let h_fresh = core.open_write("/f", None).unwrap();
    core.write(&h_fresh, 0, b"fresh retry").unwrap();
    core.commit_write(&h_fresh).unwrap();
    assert_eq!(core.read("/f", 0, 100).unwrap(), b"fresh retry");
}

/// 5. crash between upload and CAS: the version object is orphaned/stale and
///    fsck must detect it (the state remains safe; cleanup is a separate,
///    explicit operation).
#[test]
fn crash_between_upload_and_cas_detected_by_fsck() {
    let (core, _meta, objects) = setup(Arc::new(FaultFlags::default()));
    core.create("/f", 0o644).unwrap();
    // model the crash: the object lands in the store, the metadata CAS never
    // happens for it (the key is written directly under the node id)
    let node = core.lookup("/f").unwrap();
    objects
        .put(
            &format!("objects/{}/w-crashed.data", node.id),
            b"half uploaded",
        )
        .unwrap();

    let report = lightfs::fsck::fsck(&core).unwrap();
    assert_eq!(report.stale_versions.len(), 1, "{}", report.render());
    assert_eq!(report.problems(), 0, "stale versions are not problems");
}
