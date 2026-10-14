//! S3.1 prototype: immutable pack aggregation for small-file commits.
//!
//! A commit whose content is at most `large_file_bytes` is APPENDED to an
//! in-memory pack buffer instead of getting its own object. Sealing the
//! buffer (size threshold, age bound, or duplicate pending file) uploads
//! ONE immutable `packs/…` object and then runs the per-file CAS for every
//! buffered entry — each logical file keeps its own id, version and CAS
//! outcome. Ordering contract: the pack upload happens BEFORE any metadata
//! row may reference it (register_pack rows are written before the PUT),
//! and a flush returns only after ITS OWN CAS resolved — strict semantics
//! are preserved file by file; only the physical layout changes.
//!
//! CAS losers (another client committed this file first) leave their bytes
//! in the pack as a dead range: the pack is shared, so per-file cleanup is
//! impossible by construction. Dead ranges are accounted (`pack_dead_bytes`)
//! and the pack becomes reclaimable when its last live reference moves on
//! (`lightfs gc`, same retention window as superseded objects).
//!
//! Reads slice the pack: one Range GET per file with the cache disabled,
//! or one full-pack GET on first touch with the pack read cache enabled
//! (LRU, byte-budgeted) and zero GETs for sibling files until eviction.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use sha2::{Digest, Sha256};

use crate::config::PackSection;
use crate::core::Metrics;
use crate::error::{Error, Result};
use crate::metadata::{MetadataStore, Node};
use crate::object_store::ObjectStore;

/// Bucket prefix for pack objects. Also the retire/delete guard: a key
/// under this prefix is shared by construction and never deleted from a
/// per-file path.
pub const PACK_KEY_PREFIX: &str = "packs/";

pub(crate) fn content_hash(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Outcome slot for one buffered entry: its flusher parks here until the
/// sealer that took its pack resolves it. Resolution carries the new
/// version, the pack key and this entry's (offset, length) inside it.
type SlotResult = std::result::Result<(i64, String, i64, i64, String), Error>;

struct Slot {
    mx: Mutex<Option<SlotResult>>,
    cv: Condvar,
}

impl Slot {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            mx: Mutex::new(None),
            cv: Condvar::new(),
        })
    }
    fn resolve(&self, r: SlotResult) {
        let mut g = self.mx.lock().unwrap();
        if g.is_none() {
            *g = Some(r);
        }
        self.cv.notify_all();
    }
    fn take(&self) -> Option<SlotResult> {
        self.mx.lock().unwrap().clone()
    }
    fn contains(&self, buf: &PackBuf) -> bool {
        // Compare slot identity by address: `self` lives inside an Arc in
        // every buffered entry.
        buf.entries
            .iter()
            .any(|e| std::ptr::eq(Arc::as_ptr(&e.slot), self))
    }
}

struct PackEntry {
    file_id: i64,
    expected: i64,
    size: i64,
    mtime: i64,
    off: u64,
    data: Arc<Vec<u8>>,
    slot: Arc<Slot>,
}

#[derive(Default)]
struct PackBuf {
    entries: Vec<Arc<PackEntry>>,
    bytes: usize,
}

impl PackBuf {
    fn take_all(&mut self) -> Vec<Arc<PackEntry>> {
        self.bytes = 0;
        std::mem::take(&mut self.entries)
    }
}

pub struct PackEngine {
    meta: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    metrics: Arc<Metrics>,
    cfg: PackSection,
    seq: AtomicU64,
    buf: Mutex<PackBuf>,
}

impl PackEngine {
    pub(crate) fn new(
        meta: Arc<dyn MetadataStore>,
        objects: Arc<dyn ObjectStore>,
        metrics: Arc<Metrics>,
        cfg: PackSection,
    ) -> Self {
        Self {
            meta,
            objects,
            metrics,
            cfg,
            seq: AtomicU64::new(0),
            buf: Mutex::new(PackBuf::default()),
        }
    }

    pub fn config(&self) -> &PackSection {
        &self.cfg
    }

    /// Buffered bytes not yet sealed (observability).
    pub fn pending_bytes(&self) -> usize {
        self.buf.lock().unwrap().bytes
    }

    /// Route a small commit through the pack buffer. Blocks until THIS
    /// file's CAS resolved (size threshold, age bound, or our own seal).
    pub(crate) fn commit(&self, node: &Node, expected: i64, content: Vec<u8>) -> Result<Node> {
        let size = content.len() as i64;
        let slot = Slot::new();
        // The staging loop may seal pre-existing entries first (same-file
        // pending, size threshold); `content` is staged exactly once, on
        // the iteration that returns None.
        let mut content = Some(content);
        loop {
            let take = {
                let mut b = self.buf.lock().unwrap();
                // A pending entry for the SAME file serializes ahead of us:
                // its CAS decides this file's version line, and a second
                // entry at a stale expected version would conflict anyway.
                let same_file_pending = b.entries.iter().any(|e| e.file_id == node.id);
                let buffer_full = !b.entries.is_empty()
                    && b.bytes + content.as_ref().unwrap().len() >= self.cfg.max_bytes as usize;
                if same_file_pending || buffer_full {
                    Some(b.take_all())
                } else {
                    let content = content.take().unwrap();
                    let off = b.bytes as u64;
                    let len = content.len();
                    b.entries.push(Arc::new(PackEntry {
                        file_id: node.id,
                        expected,
                        size,
                        mtime: node.mtime,
                        off,
                        data: Arc::new(content),
                        slot: slot.clone(),
                    }));
                    b.bytes += len;
                    self.metrics.pack_entries.fetch_add(1, Ordering::Relaxed);
                    None
                }
            };
            match take {
                Some(entries) => self.seal(entries),
                None => break,
            }
        }

        // max_age_ms == 0: seal immediately after staging (no aggregation;
        // deterministic for tests and the worst-case latency bound).
        if self.cfg.max_age_ms == 0 {
            let take = {
                let mut b = self.buf.lock().unwrap();
                if slot.contains(&b) {
                    Some(b.take_all())
                } else {
                    None
                }
            };
            if let Some(entries) = take {
                self.seal(entries);
            }
        }

        // Wait for our own entry's CAS result. When the age bound expires
        // and our entry is still buffered, WE become the sealer (this is
        // the only latency bound a small sequential commit has); if another
        // sealer already took the buffer we keep waiting — every taken
        // entry is always resolved (success or error) by its sealer.
        let age = std::time::Duration::from_millis(self.cfg.max_age_ms.max(1));
        let mut deadline = std::time::Instant::now() + age;
        loop {
            let g = slot.mx.lock().unwrap();
            if g.is_some() {
                drop(g);
                return Self::resolved_node(node, size, slot.take().unwrap());
            }
            let now = std::time::Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            if remaining.is_zero() {
                // Age bound hit: seal whatever is buffered (our entry
                // resolves inside seal()).
                drop(g);
                let take = {
                    let mut b = self.buf.lock().unwrap();
                    if slot.contains(&b) {
                        Some(b.take_all())
                    } else {
                        None
                    }
                };
                if let Some(entries) = take {
                    self.seal(entries);
                }
                deadline = std::time::Instant::now() + age;
                continue;
            }
            let (g, _timed_out) = slot.cv.wait_timeout(g, remaining).unwrap();
            let resolved = g.is_some();
            drop(g);
            if resolved {
                return Self::resolved_node(node, size, slot.take().unwrap());
            }
            // Spurious wakeup or deadline passed: loop re-checks both.
        }
    }

    fn resolved_node(node: &Node, size: i64, res: SlotResult) -> Result<Node> {
        let (version, pack_key, off, len, hash) = res?;
        let mut out = node.clone();
        out.size = size;
        out.version = version;
        out.etag = String::new(); // a shared pack's ETag is not this file's
        out.object_key = Some(pack_key);
        out.pack_off = Some(off);
        out.pack_len = Some(len);
        out.content_hash = Some(hash);
        Ok(out)
    }

    /// Upload one immutable pack object, then CAS every buffered file.
    /// Exactly one PUT per pack; per-entry CAS failures stay local (dead
    /// bytes in a shared pack), never touching other files' outcomes.
    fn seal(&self, entries: Vec<Arc<PackEntry>>) {
        if entries.is_empty() {
            return;
        }
        let total: usize = entries.iter().map(|e| e.data.len()).sum();
        let key = format!(
            "{}p-{:x}-{:x}.pack",
            PACK_KEY_PREFIX,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            self.seq.fetch_add(1, Ordering::Relaxed)
        );
        let mut body = Vec::with_capacity(total);
        for e in &entries {
            body.extend_from_slice(&e.data);
        }
        let uploaded: Result<()> = (|| {
            // The pack row exists BEFORE the bytes: a crash between the two
            // leaves a pending row and no object — visible, and pack gc
            // ignores it (uploaded_at NULL). Never the other order.
            self.meta.register_pack(&key, body.len() as i64)?;
            self.objects.put(&key, &body)?;
            self.metrics.s3_put.fetch_add(1, Ordering::Relaxed);
            self.metrics.pack_uploads.fetch_add(1, Ordering::Relaxed);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            self.meta.mark_pack_uploaded(&key, now)?;
            Ok(())
        })();
        match uploaded {
            Ok(()) => {
                for e in entries {
                    let hash = content_hash(&e.data);
                    let r = self.meta.commit_content_cas_pack(
                        e.file_id,
                        e.expected,
                        e.size,
                        e.mtime,
                        &key,
                        e.off as i64,
                        e.size,
                        &hash,
                    );
                    match r {
                        Ok(()) => e.slot.resolve(Ok((
                            e.expected + 1,
                            key.clone(),
                            e.off as i64,
                            e.size,
                            hash,
                        ))),
                        Err(err @ Error::Conflict { .. }) => {
                            self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
                            self.metrics
                                .pack_dead_bytes
                                .fetch_add(e.size as u64, Ordering::Relaxed);
                            e.slot.resolve(Err(err));
                        }
                        Err(err) => {
                            // NotFound (unlinked before commit) or a storage
                            // error: the flush reports it, bytes stay dead.
                            e.slot.resolve(Err(err));
                        }
                    }
                }
            }
            Err(err) => {
                let _ = self.meta.drop_pack(&key);
                let err = Arc::new(err);
                for e in entries {
                    e.slot.resolve(Err(clone_error(&err)));
                }
            }
        }
    }
}

fn clone_error(e: &Error) -> Error {
    // Error is not Clone in general; the upload-failure fan-out only needs
    // a message-preserving stand-in per entry.
    match e {
        Error::ObjectMissing(k) => Error::ObjectMissing(k.clone()),
        Error::Io(msg) => Error::Io(msg.clone()),
        other => Error::Storage(format!("pack upload failed: {other}")),
    }
}

/// Byte-budgeted LRU over whole pack objects. Packs are immutable, so a
/// cached pack can never go stale — eviction is purely a memory decision.
pub struct PackReadCache {
    budget: usize,
    inner: Mutex<CacheInner>,
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub evictions: AtomicU64,
    pub loaded_bytes: AtomicU64,
    resident_bytes: AtomicU64,
}

struct CacheInner {
    map: HashMap<String, (Arc<Vec<u8>>, u64)>,
    tick: u64,
}

impl PackReadCache {
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            budget: budget_bytes as usize,
            inner: Mutex::new(CacheInner {
                map: HashMap::new(),
                tick: 0,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            loaded_bytes: AtomicU64::new(0),
            resident_bytes: AtomicU64::new(0),
        }
    }

    pub fn resident_bytes(&self) -> u64 {
        self.resident_bytes.load(Ordering::Relaxed)
    }

    /// Get the whole pack through the cache, loading with `loader` on a
    /// miss. A pack larger than the budget is served but never inserted
    /// (it would evict everything and thrash). Returns the pack and
    /// whether it was a cache hit.
    pub fn get_or_load(
        &self,
        key: &str,
        loader: impl FnOnce() -> Result<Vec<u8>>,
    ) -> Result<(Arc<Vec<u8>>, bool)> {
        {
            let mut c = self.inner.lock().unwrap();
            if let Some((bytes, _)) = c.map.get_mut(key) {
                let bytes = bytes.clone();
                c.tick += 1;
                let tick = c.tick;
                if let Some((_, t)) = c.map.get_mut(key) {
                    *t = tick;
                }
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok((bytes, true));
            }
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let data = Arc::new(loader()?);
        self.loaded_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        if data.len() <= self.budget {
            let mut c = self.inner.lock().unwrap();
            // One loader may race another for the same key: first insert
            // wins, the loser's copy is dropped.
            if !c.map.contains_key(key) {
                c.tick += 1;
                let tick = c.tick;
                c.map.insert(key.to_string(), (data.clone(), tick));
                self.resident_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                // Evict least-recently-used until under budget.
                while self.resident_bytes.load(Ordering::Relaxed) > self.budget as u64 {
                    let victim = c
                        .map
                        .iter()
                        .min_by_key(|(_, (_, t))| *t)
                        .map(|(k, _)| k.clone());
                    match victim {
                        Some(k) => {
                            if let Some((bytes, _)) = c.map.remove(&k) {
                                self.resident_bytes
                                    .fetch_sub(bytes.len() as u64, Ordering::Relaxed);
                                self.evictions.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        None => break,
                    }
                }
            }
        }
        Ok((data, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Core;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::object_store::mem::MemObjectStore;
    use std::cell::RefCell;
    use std::sync::Arc as StdArc;

    thread_local! {
        static JOINS: RefCell<Vec<std::thread::JoinHandle<()>>> = const { RefCell::new(Vec::new()) };
    }

    fn pack_core(max_bytes: u64, max_age_ms: u64, cache_bytes: u64) -> Core {
        let cfg = PackSection {
            enabled: true,
            max_bytes,
            max_age_ms,
            read_cache_bytes: cache_bytes,
            ..PackSection::default()
        };
        Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            Some(cfg),
            None,
        )
        .unwrap()
    }

    fn put(c: &Core, path: &str, data: &[u8]) -> Node {
        match c.create(path, 0o644) {
            Ok(_) | Err(Error::AlreadyExists(_)) => {}
            Err(e) => panic!("{e}"),
        }
        let v = c.lookup(path).unwrap().version;
        let h = c.open_write(path, Some(v)).unwrap();
        c.write(&h, 0, data).unwrap();
        c.commit_write(&h).unwrap()
    }

    /// Stage concurrently: strict pack aggregation only happens across
    /// commits that are IN FLIGHT together (a sequential commit blocks its
    /// caller until its pack seals — the documented latency tax).
    fn put_par(c: &StdArc<Core>, path: &'static str, data: &'static [u8]) {
        let c = c.clone();
        joins_push(std::thread::spawn(move || {
            put(&c, path, data);
        }));
    }
    fn joins_push(j: std::thread::JoinHandle<()>) {
        JOINS.with(|js| js.borrow_mut().push(j));
    }
    fn joins_wait() {
        JOINS.with(|js| {
            for j in js.borrow_mut().drain(..) {
                j.join().unwrap();
            }
        });
    }

    #[test]
    fn small_commits_share_one_pack_and_read_back() {
        let c = pack_core(64 * 1024, 0, 0); // 64 KiB packs, seal immediately per commit? no: age 0 seals per commit
        // With max_age 0 each commit seals its own pack: assert the layout
        // and read path first.
        let a = put(&c, "/a", b"aaa");
        let b = put(&c, "/b", b"bbb");
        assert!(a.is_pack_backed() && b.is_pack_backed());
        assert_eq!(c.read("/a", 0, 10).unwrap(), b"aaa");
        assert_eq!(c.read("/b", 0, 10).unwrap(), b"bbb");
        assert_eq!(c.metrics.pack_uploads.load(Ordering::Relaxed), 2);
        // empty etag on pack rows: the read cache must never see them
        assert_eq!(a.etag, "");
        assert!(a.content_hash.is_some());
    }

    #[test]
    fn concurrent_commits_aggregate_into_few_packs() {
        let c = StdArc::new(pack_core(1024 * 1024, 3_000, 0));
        let mut joins = Vec::new();
        for i in 0..64 {
            let c = c.clone();
            joins.push(std::thread::spawn(move || {
                put(&c, &format!("/f{i}"), vec![b'x'; 4096].as_slice())
            }));
        }
        for j in joins {
            j.join().unwrap();
        }
        // 64 × 4 KiB = 256 KiB across 1 MiB packs: exactly one pack.
        assert_eq!(c.metrics.pack_uploads.load(Ordering::Relaxed), 1);
        assert_eq!(c.metrics.pack_entries.load(Ordering::Relaxed), 64);
        for i in 0..64 {
            let got = c.read(&format!("/f{i}"), 0, 4096).unwrap();
            assert!(got.iter().all(|&b| b == b'x'));
        }
        let packs = c.metadata().all_packs().unwrap();
        assert_eq!(packs.len(), 1);
        assert!(packs[0].2.is_some(), "pack must be marked uploaded");
    }

    #[test]
    fn cas_loser_keeps_others_intact_and_dead_bytes_accounted() {
        let c = StdArc::new(pack_core(1024 * 1024, 3_000, 0));
        put(&c, "/f", b"v1");
        let v = c.lookup("/f").unwrap().version;
        // Both sessions stage into the buffer while neither CAS ran. The
        // duplicate-file rule seals A's entry when B stages, so outcomes
        // are deterministic: A's CAS (v1->v2) wins; B's later CAS conflicts
        // and its range stays as dead bytes inside the shared pack.
        let ha = c.open_write("/f", Some(v)).unwrap();
        c.write(&ha, 0, b"writer A full content").unwrap();
        let hb = c.open_write("/f", Some(v)).unwrap();
        c.write(&hb, 0, b"writer B full content longer").unwrap();
        let (ra, rb) = {
            let ca = c.clone();
            let cb = c.clone();
            let ta = std::thread::spawn(move || ca.commit_write(&ha));
            let tb = std::thread::spawn(move || cb.commit_write(&hb));
            (ta.join().unwrap(), tb.join().unwrap())
        };
        // exactly one winner; the other conflicts
        let (winner, loser) = match (&ra, &rb) {
            (Ok(n), Err(_)) => (n, "B"),
            (Err(_), Ok(n)) => (n, "A"),
            other => panic!("expected one winner one conflict: {other:?}"),
        };
        let want: &[u8] = if loser == "B" {
            b"writer A full content"
        } else {
            b"writer B full content longer"
        };
        assert_eq!(c.read("/f", 0, 100).unwrap(), want);
        assert_eq!(c.lookup("/f").unwrap().version, winner.version);
        // SOME writer's bytes are dead in a pack (the loser's), never lost
        // silently and never served.
        let total_dead = c.metrics.pack_dead_bytes.load(Ordering::Relaxed);
        assert!(
            total_dead == b"writer A full content".len() as u64
                || total_dead == b"writer B full content longer".len() as u64,
            "dead bytes must equal exactly the loser's range: {total_dead}"
        );
        // the winning pack is still readable
        assert!(
            c.objects()
                .head(winner.object_key.as_deref().unwrap())
                .is_ok()
        );
    }

    #[test]
    fn unlink_inside_pack_keeps_pack_and_siblings() {
        let c = StdArc::new(pack_core(1024 * 1024, 3_000, 0));
        put_par(&c, "/keep", b"keep me");
        put_par(&c, "/gone", b"delete me");
        joins_wait();
        let pack_key = c.lookup("/gone").unwrap().object_key.clone().unwrap();
        assert_eq!(
            c.metadata().all_packs().unwrap().len(),
            1,
            "both files share one pack"
        );
        c.unlink("/gone").unwrap();
        // shared pack survives (never per-file deleted)
        assert!(c.objects().head(&pack_key).is_ok());
        assert_eq!(c.read("/keep", 0, 100).unwrap(), b"keep me");
        assert!(c.lookup("/gone").is_err());
        // unreferenced-after-delete is a gc candidate (behind retention)
        assert!(
            c.metadata().gc_pack_candidates(0, 10).unwrap().is_empty(),
            "retention window still open"
        );
        // ...and once the last references are gone and the window passes:
        // keep2's v1 pack (orphaned by the overwrite) AND the shared pack
        // (orphaned by this unlink) both become candidates.
        put(&c, "/keep2", b"move on");
        let v = c.lookup("/keep2").unwrap().version;
        let h = c.open_write("/keep2", Some(v)).unwrap();
        c.write(&h, 0, b"move on v2").unwrap();
        c.commit_write(&h).unwrap();
        c.unlink("/keep").unwrap();
        let cands = c.metadata().gc_pack_candidates(i64::MAX, 10).unwrap();
        assert!(
            cands.iter().any(|(k, _)| *k == pack_key),
            "the shared pack must be a candidate once its last reference is gone: {cands:?}"
        );
    }

    #[test]
    fn large_files_stay_per_file() {
        let c = pack_core(1024 * 1024, 2000, 0);
        let big = vec![7u8; 2 * 1024 * 1024]; // > default large_file_bytes (1 MiB)
        let n = put(&c, "/big", &big);
        assert!(!n.is_pack_backed(), "large file must keep its own object");
        assert!(n.object_key.as_deref().unwrap().starts_with("objects/"));
        assert_eq!(c.read("/big", 0, 100).unwrap()[..10], [7u8; 10][..]);
        assert_eq!(c.metrics.pack_uploads.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn pack_read_cache_serves_siblings_without_gets() {
        let c = StdArc::new(pack_core(1024 * 1024, 3_000, 4 * 1024 * 1024));
        let mut joins = Vec::new();
        for i in 0..32 {
            let c = c.clone();
            joins.push(std::thread::spawn(move || {
                put(&c, &format!("/f{i}"), vec![b'y'; 4096].as_slice())
            }));
        }
        for j in joins {
            j.join().unwrap();
        }
        let before = c.metrics.s3_get.load(Ordering::Relaxed);
        for i in 0..32 {
            assert_eq!(c.read(&format!("/f{i}"), 0, 10).unwrap(), vec![b'y'; 10]);
        }
        let gets = c.metrics.s3_get.load(Ordering::Relaxed) - before;
        assert_eq!(gets, 1, "one pack load, 32 sibling reads served from RAM");
        assert_eq!(c.metrics.pack_cache_hits.load(Ordering::Relaxed), 31);
        assert_eq!(c.metrics.pack_cache_misses.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn read_cache_evicts_under_budget_pressure() {
        // 4 KiB files seal into 8 KiB packs (2 files each); the cache
        // budget holds EXACTLY one pack, so every second pack load evicts.
        let c = StdArc::new(pack_core(8 * 1024, 3_000, 8 * 1024));
        for g in 0..2i64 {
            for i in 0..4i64 {
                let data: &'static [u8] = Box::leak(vec![b'z' + g as u8; 4096].into_boxed_slice());
                let name: &'static str = Box::leak(format!("/g{g}f{i}").into_boxed_str());
                put_par(&c, name, data);
            }
            joins_wait();
        }
        // reading all files repeatedly must stay CORRECT under eviction
        for round in 0..3 {
            for g in 0..2 {
                for i in 0..4 {
                    let want = vec![b'z' + g as u8; 10];
                    assert_eq!(
                        c.read(&format!("/g{g}f{i}"), 0, 10).unwrap(),
                        want,
                        "round {round}"
                    );
                }
            }
        }
        assert!(c.metrics.pack_cache_evictions.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn gc_respects_live_pack_references() {
        let c = StdArc::new(pack_core(1024 * 1024, 3_000, 0));
        put_par(&c, "/a", b"content a");
        put_par(&c, "/b", b"content b");
        joins_wait();
        let key = c.lookup("/a").unwrap().object_key.clone().unwrap();
        assert_eq!(
            key,
            c.lookup("/b").unwrap().object_key.clone().unwrap(),
            "shared pack"
        );
        // both files share the pack; gc must not return it
        let cands = c.metadata().gc_pack_candidates(i64::MAX, 10).unwrap();
        assert!(cands.is_empty());
        c.unlink("/a").unwrap();
        c.unlink("/b").unwrap();
        let cands = c.metadata().gc_pack_candidates(i64::MAX, 10).unwrap();
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].0, key);
        // and the purge path works through the same machinery as objects
        c.objects().delete(&cands[0].0).unwrap();
        c.metadata().drop_pack(&cands[0].0).unwrap();
        assert!(c.metadata().all_packs().unwrap().is_empty());
    }

    #[test]
    fn overwrite_across_packs_is_safe() {
        let c = pack_core(1024 * 1024, 3_000, 0);
        let v1 = put(&c, "/f", b"first generation");
        let old_key = v1.object_key.clone().unwrap();
        let v2 = put(&c, "/f", b"second generation content");
        assert_ne!(
            v1.object_key, v2.object_key,
            "overwrite lands in a new pack"
        );
        assert_eq!(c.read("/f", 0, 100).unwrap(), b"second generation content");
        // old pack object still exists (retention), old key intact
        assert!(c.objects().head(&old_key).is_ok());
    }
}
