//! Phase Q: single-tier memory read cache for s3-authoritative rules.
//!
//! Scope (Q1 contract): exactly one opened combination —
//! `storage = "s3"` + `cache = "memory"` + strict commit + the default
//! capacity-LRU eviction, on at most ONE rule per mount (the policy build
//! gate refuses more). Disk caches, memory+disk, write-back and TaskScoped
//! cleanup keep their named refusals.
//!
//! What is cached: COMMITTED bytes of remote files only, read through
//! [`CacheFs::read_by_id`] (the FUSE read path). Entry identity is the
//! logical key (rule, node_id, authoritative version, etag, offset, len):
//! - the RULE is the cache instance itself (one cached rule per mount;
//!   the instance records the prefix for diagnostics),
//! - the VERSION and the ETAG are validated on EVERY hit against a live
//!   authoritative lookup — an entry that no longer matches is dropped,
//!   never served. The etag is not decoration: it is what keeps a hit
//!   honest when (node_id, version) alone cannot identify content. The
//!   motivating hole is historical now — before stable id allocation
//!   (R4.2c), SQLite reused rowids, handing a deleted file's id to the
//!   next created file, and two one-commit files both sat at version 1 —
//!   and the etag stays as defense in depth for the identity pair. The etag is a content fingerprint, but the
//!   two backends do NOT give it equal strength: the embedded store
//!   computes it locally as a truncated SHA-256 of the bytes, while on S3
//!   it is whatever the service reports (the ETag header of a plain PUT,
//!   the complete-multipart XML value) — content-derived by convention
//!   for a plain PUT, implementation-defined beyond that. The guard
//!   therefore rests on the etag being NON-EMPTY and differing when the
//!   content differs; an empty etag makes the data UNCACHEABLE (no entry
//!   can form, nothing is hit), which costs hit rate and never
//!   correctness,
//! - (node_id, offset, len) is the physical slot key, so a version bump
//!   replaces the stale entry in O(log n) instead of accumulating.
//!
//! Two structural guarantees carry the correctness story:
//! 1. Never change an outcome. The cache sits BELOW the identity protocol:
//!    a hit requires a fresh `lookup_by_id` (a live RPC on both backends —
//!    the Client MetaCache TTL is never consulted), and any uncertainty
//!    (lookup error, non-file node) falls through to the plain read, which
//!    produces exactly yesterday's behavior. A deleted identity therefore
//!    cannot be served from the cache — IDENTITY_GONE surfaces as before.
//! 2. The commit chain never reads the cache. Base reads for overlay
//!    assembly (FUSE flush, truncate) go through
//!    [`FsApi::read_by_id_authoritative`], which this layer overrides as a
//!    bypass. Dirty write-session data never enters this cache either:
//!    pending chunks live in the adapters' overlays ABOVE it, and this
//!    cache only ever stores what the authority returned for a completed
//!    read.
//!
//! Concurrency (Q3): between the version pre-lookup and the content read,
//! another client may commit; the authority then serves the NEW version's
//! bytes for a read keyed at the old version. Inserting that pairing would
//! poison the cache, so every insert re-checks the version and refuses to
//! cache anything it cannot re-confirm (the caller still gets the bytes).
//! The cost is one extra metadata query per miss; hits cost exactly one.
//! This is the honest trade of the first version — the read response does
//! not carry a version (that would be an RPC protocol change, out of
//! scope) — and it is reported, not hidden.
//!
//! Eviction: hard byte budget (the rule's `memory_budget`, already
//! validated against the mount-wide ceiling), LRU order, single mutex,
//! no RPC under the lock. A read larger than the whole budget bypasses
//! (and never fails). Readers receive cloned bytes, so an eviction can
//! never pull data out from under an in-flight read.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::api::{FsApi, WriteSession};
use crate::error::Result;
use crate::metadata::Node;
use crate::policy::{normalize_path, prefix_matches};

// ---------------------------------------------------------------------------
// metrics plumbing: the crate metrics are microsecond histograms with static
// op names; event counters record one microsecond per event (the dump's
// count column IS the counter), and the cache's byte occupancy is sampled
// under `client.cache.bytes` (avg_us column reads back as average bytes).
// ---------------------------------------------------------------------------

const M_HIT: &str = "client.cache.read.hit";
const M_MISS: &str = "client.cache.read.miss";
const M_EVICT: &str = "client.cache.read.evict";
const M_BYPASS: &str = "client.cache.read.bypass";
const M_STALE: &str = "client.cache.read.stale";
const M_UNCONFIRMED: &str = "client.cache.read.unconfirmed";
const M_BYTES: &str = "client.cache.bytes";

fn count_event(op: &'static str) {
    crate::metrics::observe(op, std::time::Duration::from_micros(1));
}

fn sample_bytes(n: u64) {
    crate::metrics::observe(M_BYTES, std::time::Duration::from_micros(n));
}

// ---------------------------------------------------------------------------
// MemoryReadCache: slots + LRU + hard budget
// ---------------------------------------------------------------------------

/// Physical slot key: one slot per (file, requested range). The rule
/// identity and the version are carried by the instance and the entry
/// respectively (see module docs).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct SlotKey {
    node_id: i64,
    offset: u64,
    len: u64,
}

struct Entry {
    version: i64,
    /// Content fingerprint of the version object. NOT the same thing on
    /// the two backends: the embedded store computes it locally as a
    /// truncated SHA-256 of the bytes (proven content-derived), while on
    /// S3 it is whatever the service reported at commit time (ETag header
    /// of a plain PUT, the complete-multipart XML value) — content-derived
    /// by convention, implementation-defined beyond that. The (node_id,
    /// version) pair alone has not always identified content: before stable
    /// id allocation (R4.2c) SQLite reused rowids, so a deleted file's id
    /// could be handed to a newly created file and two one-commit files
    /// both sat at version 1. The etag closes that hole while it is
    /// non-empty and differs when content differs — so an EMPTY etag never
    /// forms an entry (see [`MemoryReadCache::insert`]): such data is
    /// simply not cached.
    etag: Box<str>,
    bytes: Arc<Vec<u8>>,
    seq: u64,
}

#[derive(Default)]
struct State {
    slots: HashMap<SlotKey, Entry>,
    /// seq -> slot key; pop_first evicts the least recently used.
    lru: BTreeMap<u64, SlotKey>,
    occupied: u64,
    seq: u64,
}

/// Event counters (always on — cheap atomics; tests and benchmarks read
/// these directly, the metrics registry mirrors them when enabled).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub oversize_bypass: u64,
    pub stale_skip: u64,
    pub unconfirmed: u64,
    /// Authoritative lookups the cache layer initiated (pre-lookup + insert
    /// recheck). This IS the honest metadata cost: one per hit, two per
    /// cached miss, one per unconfirmed insert.
    pub authority_lookups: u64,
}

pub struct MemoryReadCache {
    max_bytes: u64,
    state: Mutex<State>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    oversize: AtomicU64,
    stale: AtomicU64,
    unconfirmed: AtomicU64,
    lookups: AtomicU64,
}

impl MemoryReadCache {
    pub fn new(max_bytes: u64) -> Self {
        Self {
            max_bytes,
            state: Mutex::new(State::default()),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            oversize: AtomicU64::new(0),
            stale: AtomicU64::new(0),
            unconfirmed: AtomicU64::new(0),
            lookups: AtomicU64::new(0),
        }
    }

    /// Count one authority lookup initiated by the cache layer.
    fn note_lookup(&self) {
        self.lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    pub fn occupied_bytes(&self) -> u64 {
        self.state.lock().unwrap().occupied
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            oversize_bypass: self.oversize.load(Ordering::Relaxed),
            stale_skip: self.stale.load(Ordering::Relaxed),
            unconfirmed: self.unconfirmed.load(Ordering::Relaxed),
            authority_lookups: self.lookups.load(Ordering::Relaxed),
        }
    }

    /// Version-AND-etag-validated lookup. A slot whose version or etag no
    /// longer matches the authority's current node is dropped on the spot
    /// (counted as `stale_skip`), never served. The etag catches the id
    /// reuse + version collision case (see [`Entry::etag`]).
    pub fn get(
        &self,
        node_id: i64,
        version: i64,
        etag: &str,
        offset: u64,
        len: u64,
    ) -> Option<Arc<Vec<u8>>> {
        // R4.3: entries with empty etags never form (insert refuses), so an
        // empty-etag lookup is structurally always a miss. Kept explicit so
        // the invariant holds even if a future entry-creating path slipped
        // one in — version-only matching is exactly the id-reuse hole.
        if etag.is_empty() {
            self.misses.fetch_add(1, Ordering::Relaxed);
            count_event(M_MISS);
            return None;
        }
        let key = SlotKey {
            node_id,
            offset,
            len,
        };
        // Probe without acting (the entry borrow must not span the
        // bookkeeping), then apply under a short second lock.
        enum Found {
            Hit(Arc<Vec<u8>>),
            Stale,
            Miss,
        }
        let found = {
            let st = self.state.lock().unwrap();
            match st.slots.get(&key) {
                Some(e) if e.version == version && e.etag.as_ref() == etag => {
                    Found::Hit(e.bytes.clone())
                }
                Some(_) => Found::Stale,
                None => Found::Miss,
            }
        };
        match found {
            Found::Hit(bytes) => {
                self.touch(key);
                self.hits.fetch_add(1, Ordering::Relaxed);
                count_event(M_HIT);
                Some(bytes)
            }
            Found::Stale => {
                // the authority moved past this entry's version: drop it
                // now so the budget is not held by bytes that can never be
                // hit again
                let mut st = self.state.lock().unwrap();
                if let Some(e) = st.slots.remove(&key) {
                    st.lru.remove(&e.seq);
                    st.occupied -= e.bytes.len() as u64;
                }
                drop(st);
                self.stale.fetch_add(1, Ordering::Relaxed);
                count_event(M_STALE);
                None
            }
            Found::Miss => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                count_event(M_MISS);
                None
            }
        }
    }

    /// Mark a slot as most-recently-used (O(log n)).
    fn touch(&self, key: SlotKey) {
        let mut st = self.state.lock().unwrap();
        let old = st.slots.get(&key).map(|e| e.seq);
        if let Some(old) = old {
            st.lru.remove(&old);
            st.seq += 1;
            let seq = st.seq;
            st.lru.insert(seq, key);
            if let Some(e) = st.slots.get_mut(&key) {
                e.seq = seq;
            }
        }
    }

    /// Insert (replacing any same-slot entry). The slot is identified by
    /// the REQUESTED range (offset, len) — the authority may have returned
    /// fewer bytes than requested (clamped at EOF), and a re-read of the
    /// same range must hit. Refuses (counting a bypass) anything whose
    /// payload is larger than the whole budget; evicts LRU-first until the
    /// entry fits. Returns false when nothing was stored.
    pub fn insert(
        &self,
        node_id: i64,
        version: i64,
        etag: &str,
        offset: u64,
        len: u64,
        bytes: Arc<Vec<u8>>,
    ) -> bool {
        // R4.3: an empty etag means no content fingerprint exists (missing
        // ETag header/XML value, or a pre-etag server) — an entry without
        // one degenerates to version-only validation, which is exactly the
        // id-reuse hole the etag exists to close. Such data is never
        // cached: correctness over hit rate, reported as a bypass.
        if etag.is_empty() {
            count_event(M_BYPASS);
            return false;
        }
        let size = bytes.len() as u64;
        if size > self.max_bytes {
            self.oversize.fetch_add(1, Ordering::Relaxed);
            count_event(M_BYPASS);
            return false;
        }
        let key = SlotKey {
            node_id,
            offset,
            len,
        };
        let mut st = self.state.lock().unwrap();
        if let Some(old) = st.slots.remove(&key) {
            st.lru.remove(&old.seq);
            st.occupied -= old.bytes.len() as u64;
        }
        while st.occupied + size > self.max_bytes {
            let Some((_, evict_key)) = st.lru.pop_first() else {
                break;
            };
            if let Some(e) = st.slots.remove(&evict_key) {
                st.occupied -= e.bytes.len() as u64;
                self.evictions.fetch_add(1, Ordering::Relaxed);
                count_event(M_EVICT);
            }
        }
        // After evicting everything the entry always fits (size <= budget),
        // so this is purely defensive.
        if st.occupied + size > self.max_bytes {
            drop(st);
            self.oversize.fetch_add(1, Ordering::Relaxed);
            count_event(M_BYPASS);
            return false;
        }
        st.seq += 1;
        let seq = st.seq;
        st.slots.insert(
            key,
            Entry {
                version,
                etag: etag.to_string().into_boxed_str(),
                bytes,
                seq,
            },
        );
        st.lru.insert(seq, key);
        st.occupied += size;
        let occupied = st.occupied;
        drop(st);
        sample_bytes(occupied);
        true
    }
}

// ---------------------------------------------------------------------------
// CacheFs: the FsApi decorator the router installs in front of the s3 backend
// ---------------------------------------------------------------------------

/// Wraps the remote backend. Every method is a passthrough except
/// `read_by_id` (version-validated read-through) and
/// `read_by_id_authoritative` (structural bypass for the commit chain).
pub struct CacheFs {
    inner: Arc<dyn FsApi>,
    /// The single cached rule's prefix. Decides ELIGIBILITY only (via the
    /// path hint, the same component-boundary match the policy uses);
    /// identity and versioning never consult the hint.
    prefix: String,
    cache: Arc<MemoryReadCache>,
}

impl CacheFs {
    pub fn new(inner: Arc<dyn FsApi>, prefix: String, budget: u64) -> Self {
        Self::with_cache(inner, prefix, Arc::new(MemoryReadCache::new(budget)))
    }

    pub fn with_cache(inner: Arc<dyn FsApi>, prefix: String, cache: Arc<MemoryReadCache>) -> Self {
        Self {
            inner,
            prefix,
            cache,
        }
    }

    pub fn cache(&self) -> &Arc<MemoryReadCache> {
        &self.cache
    }

    /// Cache-eligibility of a by-id read, decided from the caller's path
    /// hint with the policy's own component-boundary match. A malformed
    /// hint is ineligible (never guess). The hint being stale after a
    /// cross-client rename can only shift eligibility at the margin — a
    /// cached byte range is still that node_id's committed bytes at a
    /// validated version, so no wrong bytes are possible, only budget
    /// attribution that trails a rename until the entry is evicted.
    fn eligible(&self, path_hint: &str) -> bool {
        match normalize_path(path_hint) {
            Ok(p) => prefix_matches(&self.prefix, &p),
            Err(_) => false,
        }
    }

    #[allow(clippy::too_many_arguments)] // one call site; the shape IS the protocol
    fn consider_insert(
        &self,
        node_id: i64,
        version: i64,
        etag: &str,
        offset: u64,
        requested_len: u64,
        bytes: &[u8],
        hint: &str,
    ) {
        let insert_etag = etag.to_string();
        if bytes.len() as u64 > self.cache.max_bytes() {
            // larger than the whole budget: skip the recheck RPC too —
            // there is nothing to insert
            self.cache.oversize.fetch_add(1, Ordering::Relaxed);
            count_event(M_BYPASS);
            return;
        }
        // R4.3: nothing would be cached for an empty etag (insert refuses
        // it), so skip the recheck RPC entirely — same reasoning as the
        // oversize skip above.
        if insert_etag.is_empty() {
            count_event(M_BYPASS);
            return;
        }
        // Insert-time recheck (Q3): the authority resolved the read at its
        // own point in time, which may have moved past `version` while the
        // bytes were in flight. Only a re-confirmable pairing is cacheable.
        self.cache.note_lookup();
        match self.inner.lookup_by_id(node_id, hint) {
            // the etag in the recheck closes the same reuse window on the
            // insert side: a recreated identity can reproduce the version
            // but not the dead file's content fingerprint
            Ok(n) if n.version == version && n.etag == insert_etag => {
                self.cache.insert(
                    node_id,
                    version,
                    &insert_etag,
                    offset,
                    requested_len,
                    Arc::new(bytes.to_vec()),
                );
            }
            _ => {
                self.cache.unconfirmed.fetch_add(1, Ordering::Relaxed);
                count_event(M_UNCONFIRMED);
            }
        }
    }
}

impl FsApi for CacheFs {
    fn lookup(&self, path: &str) -> Result<Node> {
        self.inner.lookup(path)
    }
    fn readdir(&self, path: &str) -> Result<Vec<Node>> {
        self.inner.readdir(path)
    }
    fn mkdir(&self, path: &str, mode: u32) -> Result<Node> {
        self.inner.mkdir(path, mode)
    }
    fn create(&self, path: &str, mode: u32) -> Result<Node> {
        self.inner.create(path, mode)
    }
    fn open(&self, path: &str) -> Result<Node> {
        self.inner.open(path)
    }
    fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        self.inner.read(path, offset, size)
    }
    fn open_write(
        &self,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        self.inner.open_write(path, expected_version)
    }
    fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        self.inner.rename(old_path, new_path)
    }
    fn unlink(&self, path: &str) -> Result<()> {
        self.inner.unlink(path)
    }
    fn rmdir(&self, path: &str) -> Result<()> {
        self.inner.rmdir(path)
    }
    fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
        self.inner.setattr(path, mode, mtime)
    }
    fn symlink(&self, target: &str, path: &str) -> Result<Node> {
        self.inner.symlink(target, path)
    }
    fn readlink(&self, path: &str) -> Result<String> {
        self.inner.readlink(path)
    }
    fn lookup_by_id(&self, node_id: i64, path_hint: &str) -> Result<Node> {
        self.inner.lookup_by_id(node_id, path_hint)
    }

    fn read_by_id(&self, node_id: i64, path_hint: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        if !self.eligible(path_hint) {
            return self.inner.read_by_id(node_id, path_hint, offset, size);
        }
        // Authoritative version observation: `lookup_by_id` is a live query
        // on both implementations (the id anchor exists precisely because a
        // cached path->node snapshot may lie). If it errors, the plain read
        // reproduces the caller's today-behavior exactly — the cache never
        // turns a would-succeed read into an error, and a dead identity can
        // never be served from cached bytes.
        self.cache.note_lookup();
        let node = match self.inner.lookup_by_id(node_id, path_hint) {
            Ok(n) => n,
            Err(_) => return self.inner.read_by_id(node_id, path_hint, offset, size),
        };
        if !node.is_file() {
            return self.inner.read_by_id(node_id, path_hint, offset, size);
        }
        if let Some(bytes) = self
            .cache
            .get(node_id, node.version, &node.etag, offset, size)
        {
            return Ok((*bytes).clone());
        }
        let bytes = self.inner.read_by_id(node_id, path_hint, offset, size)?;
        self.consider_insert(
            node_id,
            node.version,
            &node.etag,
            offset,
            size,
            &bytes,
            path_hint,
        );
        Ok(bytes)
    }

    /// The commit chain's base read: structural bypass (module docs,
    /// guarantee 2). Dirty data and cache contents can never meet here.
    fn read_by_id_authoritative(
        &self,
        node_id: i64,
        path_hint: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        self.inner.read_by_id(node_id, path_hint, offset, size)
    }

    fn open_write_by_id(
        &self,
        node_id: i64,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        self.inner.open_write_by_id(node_id, path, expected_version)
    }
}

// ---------------------------------------------------------------------------
// Tests: unit behavior of the store + the FsApi contract over a real
// embedded Core (the same engine the server runs, on the in-memory store).
// The network-mode two-client regression (default metadata TTL) lives in
// tests/client_integration.rs; the FUSE-level checks run against real
// kernel mounts.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, MetadataSection};
    use crate::vnext::Core;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicBool;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "lightfs-q-{}-{}-{}",
            tag,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A fresh embedded Core: SQLite metadata (own file) + in-memory
    /// objects — the real commit/CAS/identity chain, no network.
    pub(super) fn core_at(tag: &str) -> Arc<Core> {
        let cfg = Config {
            metadata: MetadataSection {
                path: tmpdir(tag).join("meta.db").to_string_lossy().into_owned(),
            },
            ..Default::default()
        };
        crate::engine::open_engine_mem(&cfg).unwrap().core
    }

    fn cached(core: Arc<Core>, prefix: &str, budget: u64) -> CacheFs {
        CacheFs::new(core, prefix.to_string(), budget)
    }

    pub(super) fn write_via(fs: &dyn FsApi, path: &str, data: &[u8]) -> i64 {
        if let Some((dir, _)) = path.rsplit_once('/') {
            if !dir.is_empty() {
                let _ = fs.mkdir(dir, 0o755); // idempotent for tests
            }
        }
        let node = fs.create(path, 0o644).unwrap();
        let mut s = fs.open_write(path, None).unwrap();
        s.write(0, data).unwrap();
        s.commit().unwrap();
        node.id
    }

    // -- store-level -------------------------------------------------------

    #[test]
    fn slot_is_version_validated_and_stale_entries_are_dropped() {
        let c = MemoryReadCache::new(1 << 20);
        let b: Arc<Vec<u8>> = Arc::new(b"one".to_vec());
        assert!(c.insert(7, 1, "eA", 0, 3, b));
        assert_eq!(
            c.get(7, 1, "eA", 0, 3).unwrap().as_slice(),
            b"one",
            "same version + same etag hits"
        );
        // version moved on: the slot must never serve, and it is dropped
        assert!(c.get(7, 2, "eA", 0, 3).is_none());
        let s = c.stats();
        assert_eq!(s.stale_skip, 1);
        assert_eq!(c.occupied_bytes(), 0, "stale slot released");
        assert!(c.get(7, 1, "eA", 0, 3).is_none(), "dropped means dropped");
        // an etag mismatch on the same slot is equally stale (R0.1)
        assert!(c.insert(8, 1, "eX", 0, 3, Arc::new(b"two".to_vec())));
        assert!(
            c.get(8, 1, "eY", 0, 3).is_none(),
            "etag mismatch never serves"
        );
    }

    #[test]
    fn lru_eviction_respects_touch_order_and_budget() {
        let c = MemoryReadCache::new(10);
        assert!(c.insert(1, 1, "e", 0, 4, Arc::new(vec![0u8; 4]))); // A
        assert!(c.insert(2, 1, "e", 0, 4, Arc::new(vec![0u8; 4]))); // B
        assert!(c.get(1, 1, "e", 0, 4).is_some(), "touch A");
        assert!(c.insert(3, 1, "e", 0, 4, Arc::new(vec![0u8; 4]))); // needs 4: B evicted
        let s = c.stats();
        assert_eq!(s.evictions, 1, "exactly the LRU (untouched B) evicted");
        assert!(c.get(1, 1, "e", 0, 4).is_some(), "touched A survives");
        assert!(c.get(2, 1, "e", 0, 4).is_none(), "B was the LRU victim");
        assert!(c.occupied_bytes() <= 10);
    }

    #[test]
    fn oversize_read_is_refused_by_the_store_and_counted() {
        let c = MemoryReadCache::new(8);
        assert!(!c.insert(1, 1, "e", 0, 9, Arc::new(vec![0u8; 9])));
        assert_eq!(c.stats().oversize_bypass, 1);
        assert_eq!(c.occupied_bytes(), 0);
    }

    #[test]
    fn same_slot_new_version_replaces_without_leaking_budget() {
        let c = MemoryReadCache::new(16);
        assert!(c.insert(5, 1, "e1", 0, 8, Arc::new(vec![b'a'; 8])));
        assert!(c.insert(5, 2, "e2", 0, 8, Arc::new(vec![b'b'; 8])));
        assert_eq!(c.occupied_bytes(), 8, "replacement, not accumulation");
        assert_eq!(c.get(5, 2, "e2", 0, 8).unwrap().as_slice(), &[b'b'; 8]);
        assert_eq!(c.stats().evictions, 0);
    }

    #[test]
    fn eviction_never_pulls_data_from_an_in_flight_reader() {
        // the reader holds an Arc clone; dropping the entry only drops the
        // cache's reference (Q4: eviction vs in-flight reads)
        let c = MemoryReadCache::new(4);
        assert!(c.insert(1, 1, "e", 0, 4, Arc::new(vec![7u8; 4])));
        let held = c.get(1, 1, "e", 0, 4).unwrap();
        assert!(c.insert(2, 1, "e", 0, 4, Arc::new(vec![9u8; 4]))); // evicts entry 1
        assert_eq!(c.stats().evictions, 1);
        assert_eq!(held.as_slice(), &[7u8; 4], "reader's bytes are its own");
        assert_eq!(c.occupied_bytes(), 4);
    }

    // -- FsApi contract over a real embedded Core ---------------------------

    #[test]
    fn read_through_hit_serves_identical_bytes_and_counts() {
        let core = core_at("hit");
        let f = cached(core, "/hot", 1 << 20);
        let id = write_via(&f, "/hot/f", b"hello cached world");

        let a = f.read_by_id(id, "/hot/f", 0, 100).unwrap();
        let s1 = f.cache().stats();
        assert_eq!(s1.misses, 1);
        assert_eq!(s1.hits, 0);
        let b = f.read_by_id(id, "/hot/f", 0, 100).unwrap();
        let s2 = f.cache().stats();
        assert_eq!(s2.hits, 1, "second read is a hit");
        assert_eq!(s2.misses, 1, "no extra authority read");
        assert_eq!(a, b);
        assert_eq!(b, b"hello cached world");

        // ranged reads are their own slots (exact-range entries, Q2)
        let r = f.read_by_id(id, "/hot/f", 6, 6).unwrap();
        assert_eq!(r, b"cached");
        let s3 = f.cache().stats();
        assert_eq!(s3.misses, 2, "a different range is a different slot");
        assert_eq!(f.read_by_id(id, "/hot/f", 6, 6).unwrap(), b"cached");
        assert_eq!(f.cache().stats().hits, 2);
    }

    #[test]
    fn ineligible_prefixes_never_touch_the_cache() {
        let core = core_at("elig");
        let f = cached(core, "/hot", 1 << 20);
        let hot = write_via(&f, "/hot/f", b"hot");
        let cold = write_via(&f, "/cold/g", b"cold");

        for _ in 0..2 {
            assert_eq!(f.read_by_id(cold, "/cold/g", 0, 10).unwrap(), b"cold");
        }
        let s = f.cache().stats();
        assert_eq!((s.hits, s.misses), (0, 0), "outside the rule: no cache");

        assert_eq!(f.read_by_id(hot, "/hot/f", 0, 10).unwrap(), b"hot");
        assert_eq!(f.cache().stats().misses, 1, "inside the rule: cached");

        // component boundary: /hot-old is NOT the rule
        let other = write_via(&f, "/hot-old/x", b"other");
        assert_eq!(f.read_by_id(other, "/hot-old/x", 0, 10).unwrap(), b"other");
        assert_eq!(f.cache().stats().misses, 1);
    }

    /// THE Q3 regression: after another writer advances the version, a
    /// cached read must not serve the stale bytes. Runs against the real
    /// commit chain (the writer bypasses the cache exactly like a second
    /// client's commit bypasses this mount).
    #[test]
    fn stale_version_is_never_served_after_a_second_writer() {
        let core = core_at("stale");
        let f = cached(core.clone(), "/hot", 1 << 20);
        let id = write_via(&f, "/hot/f", b"version one");

        assert_eq!(f.read_by_id(id, "/hot/f", 0, 100).unwrap(), b"version one");
        assert_eq!(f.cache().stats().hits, 0);

        // "client B" commits through the shared authority
        let mut ws = FsApi::open_write(core.as_ref(), "/hot/f", None).unwrap();
        ws.write(0, b"version two from B").unwrap();
        ws.commit().unwrap();

        let now = f.read_by_id(id, "/hot/f", 0, 100).unwrap();
        assert_eq!(now, b"version two from B", "the stale entry must lose");
        let s = f.cache().stats();
        assert_eq!(s.stale_skip, 1, "the version check caught the old entry");
        assert_eq!(s.hits, 0, "nothing stale was served");
        // and the fresh entry now hits
        assert_eq!(f.read_by_id(id, "/hot/f", 0, 100).unwrap(), now);
        assert_eq!(f.cache().stats().hits, 1);
    }

    /// Q3 race: a commit lands BETWEEN the version pre-lookup and the
    /// content read, so the bytes that come back belong to the NEW version.
    /// The insert-time recheck must refuse to cache them. Deterministic:
    /// the racing commit happens inside the inner read, exactly once.
    struct RaceFs {
        inner: Arc<Core>,
        path: String,
        armed: AtomicBool,
    }
    impl FsApi for RaceFs {
        fn lookup(&self, p: &str) -> Result<Node> {
            self.inner.lookup(p)
        }
        fn readdir(&self, p: &str) -> Result<Vec<Node>> {
            self.inner.readdir(p)
        }
        fn mkdir(&self, p: &str, m: u32) -> Result<Node> {
            self.inner.mkdir(p, m)
        }
        fn create(&self, p: &str, m: u32) -> Result<Node> {
            self.inner.create(p, m)
        }
        fn open(&self, p: &str) -> Result<Node> {
            self.inner.open(p)
        }
        fn read(&self, p: &str, o: u64, s: u64) -> Result<Vec<u8>> {
            self.inner.read(p, o, s)
        }
        fn open_write(&self, p: &str, e: Option<i64>) -> Result<Box<dyn WriteSession + '_>> {
            // trait form: Core's inherent open_write returns a bare handle
            FsApi::open_write(self.inner.as_ref(), p, e)
        }
        fn rename(&self, o: &str, n: &str) -> Result<()> {
            self.inner.rename(o, n)
        }
        fn unlink(&self, p: &str) -> Result<()> {
            self.inner.unlink(p)
        }
        fn rmdir(&self, p: &str) -> Result<()> {
            self.inner.rmdir(p)
        }
        fn setattr(&self, p: &str, m: Option<u32>, t: Option<i64>) -> Result<Node> {
            self.inner.setattr(p, m, t)
        }
        fn symlink(&self, t: &str, p: &str) -> Result<Node> {
            self.inner.symlink(t, p)
        }
        fn readlink(&self, p: &str) -> Result<String> {
            self.inner.readlink(p)
        }
        fn lookup_by_id(&self, id: i64, h: &str) -> Result<Node> {
            FsApi::lookup_by_id(self.inner.as_ref(), id, h)
        }
        fn read_by_id(&self, id: i64, _hint: &str, o: u64, s: u64) -> Result<Vec<u8>> {
            // the race, once: another client's commit completes while "the
            // read is on the wire", so the authority serves the NEW version
            if self.armed.swap(false, Ordering::SeqCst) {
                let mut ws = FsApi::open_write(self.inner.as_ref(), &self.path, None).unwrap();
                ws.write(0, b"raced past the pre-lookup").unwrap();
                ws.commit().unwrap();
            }
            FsApi::read_by_id(self.inner.as_ref(), id, "", o, s)
        }
    }

    #[test]
    fn read_raced_by_a_commit_is_returned_but_not_cached() {
        let core = core_at("race");
        let id = write_via(core.as_ref(), "/hot/f", b"version one");
        let inner: Arc<dyn FsApi> = Arc::new(RaceFs {
            inner: core.clone(),
            path: "/hot/f".to_string(),
            armed: AtomicBool::new(true),
        });
        let f = CacheFs::with_cache(
            inner,
            "/hot".to_string(),
            Arc::new(MemoryReadCache::new(1 << 20)),
        );

        let got = f.read_by_id(id, "/hot/f", 0, 100).unwrap();
        assert_eq!(got, b"raced past the pre-lookup", "caller keeps the bytes");
        let s = f.cache().stats();
        assert_eq!(s.unconfirmed, 1, "version moved under the read: no insert");
        assert_eq!(f.cache().occupied_bytes(), 0, "poison never stored");

        // a follow-up read (race disarmed) observes the new version, caches
        // cleanly, and the read after THAT hits
        assert_eq!(f.read_by_id(id, "/hot/f", 0, 100).unwrap(), got);
        assert_eq!(f.read_by_id(id, "/hot/f", 0, 100).unwrap(), got);
        assert_eq!(f.cache().stats().hits, 1);
    }

    /// Q4: a poisoned entry (wrong bytes under the CURRENT version) cannot
    /// reach the commit chain — the authoritative read bypasses structurally.
    #[test]
    fn poisoned_cache_never_reaches_the_authoritative_read_or_the_commit() {
        let core = core_at("poison");
        let f = cached(core.clone(), "/hot", 1 << 20);
        let id = write_via(&f, "/hot/f", b"clean authoritative bytes");
        let now = core.lookup("/hot/f").unwrap();
        let (v, etag) = (now.version, now.etag.clone());

        // adversarial injection: wrong bytes under the CURRENT version+etag
        assert!(
            f.cache()
                .insert(id, v, &etag, 0, 6, Arc::new(b"POISON".to_vec()))
        );
        // probe proves the poison would hit a plain read (this probe is
        // itself the only hit the cache ever records in this test)
        assert_eq!(
            f.cache().get(id, v, &etag, 0, 6).unwrap().as_slice(),
            b"POISON"
        );
        let after_setup = f.cache().stats();

        // the commit chain's base read ignores it
        let base = f.read_by_id_authoritative(id, "/hot/f", 0, 100).unwrap();
        assert_eq!(base, b"clean authoritative bytes");
        assert_eq!(f.cache().stats(), after_setup, "no cache traffic");

        // and the whole flush-shaped commit (overlay assembly + CAS) lands
        // on authority bytes, exactly as it would without any cache
        let mut ws = f.open_write_by_id(id, "/hot/f", Some(v)).unwrap();
        ws.write(0, b"clean authoritative bytes+overlay").unwrap();
        let committed = ws.commit().unwrap();
        assert!(committed.version > v);
        assert_eq!(
            core.read("/hot/f", 0, 100).unwrap(),
            b"clean authoritative bytes+overlay"
        );
        let s = f.cache().stats();
        assert_eq!(
            (s.hits, s.misses),
            (after_setup.hits, after_setup.misses),
            "the commit chain is not a cache client"
        );
    }

    /// Q4: a deleted identity must fail through the cache exactly as it
    /// would without one — cached bytes must not resurrect IDENTITY_GONE.
    #[test]
    fn deleted_identity_is_never_served_from_the_cache() {
        let core = core_at("gone");
        let f = cached(core.clone(), "/hot", 1 << 20);
        let id = write_via(&f, "/hot/f", b"soon gone");
        assert_eq!(f.read_by_id(id, "/hot/f", 0, 100).unwrap(), b"soon gone");
        assert_eq!(f.cache().stats().hits, 0); // first read: miss

        core.unlink("/hot/f").unwrap();

        let err = f.read_by_id(id, "/hot/f", 0, 100).unwrap_err();
        assert!(
            matches!(err, crate::error::Error::IdentityGone(_)),
            "the identity error must surface, got {err:?}"
        );
        assert_eq!(f.cache().stats().hits, 0, "no cached bytes were served");
    }

    /// Q2: a read larger than the whole budget bypasses without failing.
    #[test]
    fn oversize_read_bypasses_and_still_succeeds() {
        let core = core_at("big");
        let f = cached(core, "/hot", 16);
        let big = vec![b'x'; 1024];
        let id = write_via(&f, "/hot/f", &big);

        let got = f.read_by_id(id, "/hot/f", 0, 1024).unwrap();
        assert_eq!(got, big);
        let s = f.cache().stats();
        assert_eq!(s.oversize_bypass, 1);
        assert_eq!(f.cache().occupied_bytes(), 0);
        // and again: still correct, still bypassing
        assert_eq!(f.read_by_id(id, "/hot/f", 0, 1024).unwrap(), big);
        assert_eq!(f.cache().stats().oversize_bypass, 2);
    }

    /// Q5: the authoritative bypass is not a cache client — no hit/miss
    /// accounting, so the numbers stay attributable.
    #[test]
    fn authoritative_read_leaves_no_cache_traces() {
        let core = core_at("auth");
        let f = cached(core, "/hot", 1 << 20);
        let id = write_via(&f, "/hot/f", b"data");
        let before = f.cache().stats();
        assert_eq!(
            f.read_by_id_authoritative(id, "/hot/f", 0, 100).unwrap(),
            b"data"
        );
        assert_eq!(f.cache().stats(), before);
    }
}

#[cfg(test)]
mod perf {
    //! Q5 evidence: cold vs hot ranged reads on the embedded engine, with
    //! REAL S3-GET and metadata-query counters (Core::metrics). Run with
    //! `cargo test --lib readcache::perf -- --nocapture` to see the table;
    //! the asserts pin the structural facts (hot pass does zero object
    //! reads, the hit rate is exact, occupancy stays under budget).

    use super::tests::core_at;
    use super::*;
    use crate::api::FsApi;

    const CHUNK: u64 = 128 * 1024;
    const FILE: u64 = 4 * 1024 * 1024;

    #[test]
    fn cold_vs_hot_ranged_reads() {
        let core = core_at("perf");
        let f = CacheFs::new(core.clone(), "/hot".to_string(), 8 << 20);
        f.mkdir("/hot", 0o755).unwrap();
        f.create("/hot/f", 0o644).unwrap();
        let mut ws = f.open_write("/hot/f", None).unwrap();
        ws.write(0, &vec![0xA5u8; FILE as usize]).unwrap();
        ws.commit().unwrap();
        let id = f.lookup("/hot/f").unwrap().id;

        let ranges: Vec<(u64, u64)> = (0..(FILE / CHUNK)).map(|i| (i * CHUNK, CHUNK)).collect();

        let gets0 = core.metrics.s3_get.load(Ordering::Relaxed);
        let _meta0 = core.metrics.metadata_tx.load(Ordering::Relaxed);
        let lookups0 = f.cache().stats().authority_lookups;
        let t0 = std::time::Instant::now();
        for &(off, len) in &ranges {
            let got = f.read_by_id(id, "/hot/f", off, len).unwrap();
            assert_eq!(got.len() as u64, len);
        }
        let cold = t0.elapsed();
        let gets1 = core.metrics.s3_get.load(Ordering::Relaxed);
        let _meta1 = core.metrics.metadata_tx.load(Ordering::Relaxed);
        let lookups1 = f.cache().stats().authority_lookups;

        let t1 = std::time::Instant::now();
        for &(off, len) in &ranges {
            let got = f.read_by_id(id, "/hot/f", off, len).unwrap();
            assert_eq!(got.len() as u64, len);
        }
        let hot = t1.elapsed();
        let gets2 = core.metrics.s3_get.load(Ordering::Relaxed);
        let _meta2 = core.metrics.metadata_tx.load(Ordering::Relaxed);

        let s = f.cache().stats();
        println!(
            "\nQ5 cold-vs-hot (embedded engine, {} x {} KiB ranges over {} MiB file):\n  cold: {:>10.1?}  (s3_get +{}, authority lookups +{})\n  hot:  {:>10.1?}  (s3_get +{}, authority lookups +{})\n  speedup: {:.1}x  hits={} misses={} stale={} unconfirmed={} occupied={}/{}",
            ranges.len(),
            CHUNK / 1024,
            FILE >> 20,
            cold,
            gets1 - gets0,
            lookups1 - lookups0,
            hot,
            gets2 - gets1,
            s.authority_lookups - lookups1,
            cold.as_nanos() as f64 / hot.as_nanos() as f64,
            s.hits,
            s.misses,
            s.stale_skip,
            s.unconfirmed,
            f.cache().occupied_bytes() >> 10,
            f.cache().max_bytes() >> 10,
        );

        // structural facts
        assert_eq!(gets2 - gets1, 0, "hot pass must not touch the object store");
        assert_eq!(s.hits as usize, ranges.len(), "every hot read hits");
        assert_eq!(s.misses as usize, ranges.len(), "every cold read misses");
        // the honest cost model: 2 authority lookups per cached miss
        // (pre-lookup + insert recheck), 1 per hit
        assert_eq!(s.authority_lookups - lookups1, 32, "one lookup per hit");
        assert_eq!(
            (lookups1 - lookups0) as usize,
            2 * ranges.len(),
            "two lookups per cached miss"
        );
        assert!(
            f.cache().occupied_bytes() <= f.cache().max_bytes(),
            "hard budget holds"
        );
    }
}

#[cfg(test)]
mod r1 {
    //! Phase R: boundary tests for the audit findings. Deterministic only:
    //! barriers and controlled doubles, no random stress.

    use super::tests::core_at;
    use super::*;
    use crate::api::FsApi;
    use crate::vnext::Core;

    fn cached(core: Arc<Core>, prefix: &str, budget: u64) -> CacheFs {
        CacheFs::new(core, prefix.to_string(), budget)
    }

    fn commit_bytes(fs: &CacheFs, path: &str, data: &[u8]) -> Node {
        let mut ws = FsApi::open_write(fs, path, None).unwrap();
        ws.write(0, data).unwrap();
        ws.commit().unwrap()
    }

    /// R4.2c update: id reuse is structurally dead (AUTOINCREMENT), so the
    /// cache can never alias a recreated file onto a dead file's entries:
    /// the new file gets a fresh id (fresh slots), the dead id resolves to
    /// nothing, and every identity reads its own bytes. The etag guard
    /// (version+fingerprint on every hit) remains as defense in depth.
    #[test]
    fn recreated_identity_never_aliases_the_dead_files_cache_entries() {
        let core = core_at("reuse");
        let f = cached(core.clone(), "/hot", 1 << 20);
        f.mkdir("/hot", 0o755).unwrap();

        // file A: one commit -> version 1, read into the cache
        f.create("/hot/f", 0o644).unwrap();
        commit_bytes(&f, "/hot/f", b"the DEAD file's bytes");
        let dead_id = f.lookup("/hot/f").unwrap().id;
        assert_eq!(
            f.read_by_id(dead_id, "/hot/f", 0, 100).unwrap(),
            b"the DEAD file's bytes"
        );

        // unlink + recreate: the NEW node gets a fresh id
        f.unlink("/hot/f").unwrap();
        f.create("/hot/g", 0o644).unwrap();
        commit_bytes(&f, "/hot/g", b"the LIVE file's bytes");
        let live = f.lookup("/hot/g").unwrap();
        assert_ne!(
            live.id, dead_id,
            "stable allocation: the recreated file never inherits the id"
        );

        // the dead identity is gone; the live identity reads its OWN bytes
        assert!(
            f.read_by_id(dead_id, "/hot/g", 0, 100).is_err(),
            "the dead id must not resolve to the new file"
        );
        let got = f.read_by_id(live.id, "/hot/g", 0, 100).unwrap();
        assert_eq!(got, b"the LIVE file's bytes");
    }

    /// R4.3 unit: an entry can never FORM for an empty etag — no insert,
    /// no occupancy, no hit. This is the whole fix: empty etag (missing
    /// ETag on the write response, or a pre-etag server) = not cached,
    /// never version-only.
    #[test]
    fn empty_etag_entries_are_never_formed() {
        let cache = MemoryReadCache::new(1 << 20);
        assert!(
            !cache.insert(7, 1, "", 0, 8, Arc::new(vec![0u8; 8])),
            "empty-etag insert must be refused"
        );
        assert_eq!(cache.occupied_bytes(), 0, "nothing was stored");
        assert!(
            cache.get(7, 1, "", 0, 8).is_none(),
            "an empty-etag lookup can never hit"
        );
        assert_eq!(cache.stats().misses, 1);
    }

    /// R4.3 degenerate scenario, end to end: a backend that provides NO
    /// etags at all. Rowid reuse + version collision then cannot be told
    /// apart by ANY metadata — so nothing may be cached for such data,
    /// and the recreated file must read its own bytes through the cold
    /// path. The double blanks the etag on both lookup forms, matching a
    /// deployment where the etag channel is absent (missing header, old
    /// server): the cache must degrade to NO caching, never to
    /// version-only.
    struct BlankEtagFs {
        inner: Arc<Core>,
    }
    impl FsApi for BlankEtagFs {
        fn lookup(&self, p: &str) -> Result<Node> {
            let mut n = self.inner.lookup(p)?;
            n.etag = String::new();
            Ok(n)
        }
        fn readdir(&self, p: &str) -> Result<Vec<Node>> {
            self.inner.readdir(p)
        }
        fn mkdir(&self, p: &str, m: u32) -> Result<Node> {
            self.inner.mkdir(p, m)
        }
        fn create(&self, p: &str, m: u32) -> Result<Node> {
            self.inner.create(p, m)
        }
        fn open(&self, p: &str) -> Result<Node> {
            self.inner.open(p)
        }
        fn read(&self, p: &str, o: u64, s: u64) -> Result<Vec<u8>> {
            self.inner.read(p, o, s)
        }
        fn open_write(&self, p: &str, e: Option<i64>) -> Result<Box<dyn WriteSession + '_>> {
            FsApi::open_write(self.inner.as_ref(), p, e)
        }
        fn rename(&self, o: &str, n: &str) -> Result<()> {
            self.inner.rename(o, n)
        }
        fn unlink(&self, p: &str) -> Result<()> {
            self.inner.unlink(p)
        }
        fn rmdir(&self, p: &str) -> Result<()> {
            self.inner.rmdir(p)
        }
        fn setattr(&self, p: &str, m: Option<u32>, t: Option<i64>) -> Result<Node> {
            self.inner.setattr(p, m, t)
        }
        fn symlink(&self, t: &str, p: &str) -> Result<Node> {
            self.inner.symlink(t, p)
        }
        fn readlink(&self, p: &str) -> Result<String> {
            self.inner.readlink(p)
        }
        fn lookup_by_id(&self, id: i64, h: &str) -> Result<Node> {
            let mut n = FsApi::lookup_by_id(self.inner.as_ref(), id, h)?;
            n.etag = String::new();
            Ok(n)
        }
        fn read_by_id(&self, id: i64, _h: &str, o: u64, s: u64) -> Result<Vec<u8>> {
            FsApi::read_by_id(self.inner.as_ref(), id, "", o, s)
        }
    }

    #[test]
    fn etagless_backend_degenerates_to_no_caching_never_to_stale_bytes() {
        let core = core_at("etagless");
        let inner: Arc<dyn FsApi> = Arc::new(BlankEtagFs {
            inner: core.clone(),
        });
        let f = CacheFs::with_cache(
            inner,
            "/hot".to_string(),
            Arc::new(MemoryReadCache::new(1 << 20)),
        );
        f.mkdir("/hot", 0o755).unwrap();

        // DEAD: create + commit + read. The read must NOT be cached.
        f.create("/hot/f", 0o644).unwrap();
        commit_bytes(&f, "/hot/f", b"the DEAD file's bytes");
        let dead_id = f.lookup("/hot/f").unwrap().id;
        assert_eq!(
            f.read_by_id(dead_id, "/hot/f", 0, 100).unwrap(),
            b"the DEAD file's bytes"
        );
        assert_eq!(
            f.cache().occupied_bytes(),
            0,
            "etagless data is never cached"
        );

        // LIVE: unlink + recreate. The new file gets a fresh id (stable
        // allocation), and this backend STILL provides no etags for it.
        f.unlink("/hot/f").unwrap();
        f.create("/hot/g", 0o644).unwrap();
        commit_bytes(&f, "/hot/g", b"the LIVE file's bytes");
        let live = f.lookup("/hot/g").unwrap();
        assert_ne!(
            live.id, dead_id,
            "stable allocation: the recreated file never inherits the id"
        );
        assert_eq!(
            live.etag, "",
            "precondition: this backend provides no etags"
        );

        // THE assertion: etagless data stays uncached, and the recreated
        // file reads its OWN bytes through the cold path.
        let got = f.read_by_id(live.id, "/hot/g", 0, 100).unwrap();
        assert_eq!(
            got, b"the LIVE file's bytes",
            "an etagless backend must never serve another file's bytes"
        );
        assert_eq!(f.cache().stats().hits, 0, "nothing was ever cacheable");
        assert_eq!(f.cache().occupied_bytes(), 0);
    }
}

#[cfg(test)]
mod r1b {
    //! R1 continued: concurrency, capacity and boundary edges. All
    //! deterministic (barriers, exact counters); no random stress.

    use super::tests::core_at;
    use super::*;
    use crate::api::FsApi;
    use crate::vnext::Core;
    use std::sync::Barrier;
    use std::sync::atomic::AtomicBool;
    use std::thread;

    fn cached(core: Arc<Core>, budget: u64) -> CacheFs {
        CacheFs::new(core, "/hot".to_string(), budget)
    }

    fn setup_file(f: &CacheFs, size: usize) -> i64 {
        f.mkdir("/hot", 0o755).unwrap();
        f.create("/hot/f", 0o644).unwrap();
        let mut ws = FsApi::open_write(f, "/hot/f", None).unwrap();
        ws.write(0, &vec![0x77u8; size]).unwrap();
        ws.commit().unwrap();
        f.lookup("/hot/f").unwrap().id
    }

    /// R0.2: concurrent cold reads of the SAME range all read the object
    /// (no single-flight in v1 - quantified here) and all serve identical
    /// bytes; the store stays consistent under the race.
    #[test]
    fn concurrent_cold_reads_of_one_range() {
        const N: usize = 8;
        let core = core_at("ccold");
        let gets0 = core.metrics.s3_get.load(Ordering::Relaxed);
        let f = cached(core.clone(), 1 << 20);
        let id = setup_file(&f, 256 * 1024);

        let barrier = Arc::new(Barrier::new(N));
        let f = Arc::new(f);
        let mut joins = Vec::new();
        for _ in 0..N {
            let b = barrier.clone();
            let f = f.clone();
            joins.push(thread::spawn(move || {
                b.wait();
                f.read_by_id(id, "/hot/f", 0, 128 * 1024).unwrap()
            }));
        }
        let mut results = Vec::new();
        for j in joins {
            results.push(j.join().unwrap());
        }
        let gets = core.metrics.s3_get.load(Ordering::Relaxed) - gets0;
        for r in &results {
            assert_eq!(r, &results[0], "every racing reader sees identical bytes");
            assert_eq!(r.len(), 128 * 1024);
        }
        let s = f.cache().stats();
        // the barrier starts the readers together but thread SCHEDULING
        // decides how many actually race the miss: later readers may hit
        // the entry an earlier reader inserted. Data-wise everything is
        // exact (asserted above); cost-wise the repeat-read bound is:
        assert!(
            s.misses >= 1 && s.misses as usize <= N,
            "misses must be in [1, {N}]: {}",
            s.misses
        );
        assert_eq!(
            gets, s.misses,
            "exactly one object GET per miss (embedded: no other GET source)"
        );
        println!(
            "R1 concurrent cold reads: {N} readers -> {} misses/GETs, {} hits (repeat-read cost bounded by N, 1x-({N}x))",
            s.misses, s.hits
        );
        assert_eq!(
            f.cache().occupied_bytes(),
            128 * 1024,
            "one slot, last writer wins with identical bytes"
        );
    }

    /// R0.3: concurrent inserts of DIFFERENT slots never exceed the budget
    /// (the accounting is entirely inside the critical section) and the
    /// LRU counters stay consistent (occupied == sum of live entries).
    #[test]
    fn concurrent_inserts_respect_budget_and_lru() {
        const N: usize = 8;
        const ENTRY: u64 = 100;
        let c = Arc::new(MemoryReadCache::new(ENTRY * 4)); // fits 4 of 8
        let barrier = Arc::new(Barrier::new(N));
        let mut joins = Vec::new();
        for i in 0..N {
            let b = barrier.clone();
            let c = c.clone();
            joins.push(thread::spawn(move || {
                b.wait();
                c.insert(
                    1000 + i as i64,
                    1,
                    "e",
                    0,
                    ENTRY,
                    Arc::new(vec![0u8; ENTRY as usize]),
                );
            }));
        }
        for j in joins {
            j.join().unwrap();
        }
        assert!(
            c.occupied_bytes() <= ENTRY * 4,
            "occupied {} exceeds budget {}",
            c.occupied_bytes(),
            ENTRY * 4
        );
        // capacity consistency: the residue is exact (no partial accounting)
        let s = c.stats();
        assert_eq!(c.occupied_bytes() % ENTRY, 0, "no partial accounting");
        assert!(
            s.evictions >= 4,
            "at least the overflow entries were evicted: {}",
            s.evictions
        );
        assert_eq!(s.oversize_bypass, 0);
    }

    /// R0.1: version advance racing a hit loop. Every returned byte vector
    /// must be EXACTLY one of the committed versions - never a mix - and
    /// after the writer settles, reads converge to the new version.
    #[test]
    fn version_advance_racing_hit_loop_never_mixes_bytes() {
        let core = core_at("vrace");
        let f = Arc::new(cached(core.clone(), 1 << 20));
        let id = super::tests::write_via(f.as_ref(), "/hot/f", &vec![b'A'; 4096]);
        assert_eq!(
            f.read_by_id(id, "/hot/f", 0, 4096).unwrap(),
            vec![b'A'; 4096]
        );

        let stop = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(2));
        let reader_stop = stop.clone();
        let reader_barrier = barrier.clone();
        let reader_f = f.clone();
        let reader = thread::spawn(move || {
            let mut seen_a = false;
            let mut seen_b = false;
            reader_barrier.wait();
            while !reader_stop.load(Ordering::Relaxed) {
                let got = reader_f.read_by_id(id, "/hot/f", 0, 4096).unwrap();
                if got == vec![b'A'; 4096] {
                    seen_a = true;
                } else if got == vec![b'B'; 4096] {
                    seen_b = true;
                } else {
                    panic!("read returned bytes that match NEITHER committed version");
                }
            }
            (seen_a, seen_b)
        });
        barrier.wait();
        let mut ws = FsApi::open_write(core.as_ref(), "/hot/f", None).unwrap();
        ws.write(0, &vec![b'B'; 4096]).unwrap();
        ws.commit().unwrap();
        stop.store(true, Ordering::Relaxed);
        let (seen_a, seen_b) = reader.join().unwrap();
        assert!(seen_a, "some reads hit the pre-commit version");
        assert_eq!(
            f.read_by_id(id, "/hot/f", 0, 4096).unwrap(),
            vec![b'B'; 4096],
            "converges to the new version"
        );
        // seen_b is possible but timing-dependent; never assert it
        let _ = seen_b;
    }

    /// R0.3: an oversize read bypasses WITHOUT poisoning later small reads
    /// (the budget stays usable afterwards).
    #[test]
    fn oversize_read_leaves_the_budget_usable() {
        let core = core_at("big2");
        let f = cached(core, 64);
        let big_id = {
            f.mkdir("/hot", 0o755).unwrap();
            f.create("/hot/big", 0o644).unwrap();
            let mut ws = FsApi::open_write(&f, "/hot/big", None).unwrap();
            ws.write(0, &vec![b'X'; 1024]).unwrap();
            ws.commit().unwrap();
            f.lookup("/hot/big").unwrap().id
        };
        assert_eq!(
            f.read_by_id(big_id, "/hot/big", 0, 1024).unwrap().len(),
            1024
        );
        assert_eq!(f.cache().stats().oversize_bypass, 1);
        assert_eq!(f.cache().occupied_bytes(), 0);

        // a small read still caches and hits normally afterwards
        let small_id = super::tests::write_via(&f, "/hot/s", b"small");
        assert_eq!(f.read_by_id(small_id, "/hot/s", 0, 100).unwrap(), b"small");
        assert_eq!(f.cache().stats().misses, 2);
        assert_eq!(f.read_by_id(small_id, "/hot/s", 0, 100).unwrap(), b"small");
        assert_eq!(f.cache().stats().hits, 1);
        assert_eq!(f.cache().occupied_bytes(), 5);
    }

    /// R0.3: zero-length reads, huge offsets and huge lengths behave
    /// EXACTLY like the plain read (no arithmetic in the cache to overflow;
    /// the range is only a key), and the cached state stays explainable.
    #[test]
    fn zero_len_and_extreme_ranges_match_plain_reads() {
        let core = core_at("edges");
        let f = cached(core.clone(), 1 << 20);
        let id = setup_file(&f, 64);

        // zero-length read: cached as an empty slot, consistent on re-read
        let plain = FsApi::read_by_id(core.as_ref(), id, "", 64, 0).unwrap();
        let got = f.read_by_id(id, "/hot/f", 64, 0).unwrap();
        assert_eq!(got, plain);
        assert_eq!(f.read_by_id(id, "/hot/f", 64, 0).unwrap(), got);

        // offset far beyond EOF: empty result, identical to the plain read
        let plain = FsApi::read_by_id(core.as_ref(), id, "", u64::MAX / 2, 64).unwrap();
        let got = f.read_by_id(id, "/hot/f", u64::MAX / 2, 64).unwrap();
        assert_eq!(got, plain, "huge offset matches plain read");

        // enormous requested length: the authority clamps; the slot keys on
        // the REQUESTED length, and a re-read hits the same clamped bytes
        let plain = FsApi::read_by_id(core.as_ref(), id, "", 0, u64::MAX).unwrap();
        let got = f.read_by_id(id, "/hot/f", 0, u64::MAX).unwrap();
        assert_eq!(got, plain);
        assert_eq!(got.len(), 64, "authority clamps at EOF");
        assert_eq!(f.read_by_id(id, "/hot/f", 0, u64::MAX).unwrap(), got);
        assert!(f.cache().stats().hits >= 1, "extreme-range re-read hits");
    }
}

#[cfg(test)]
mod r2 {
    //! R2: performance attribution on the embedded engine. Repeats each
    //! scenario 5 times and reports the MEDIAN; the divides are measured
    //! directly (authority lookup, object read, cache lookup, byte copy)
    //! so the hot-read cost structure is explicit. Run with --nocapture.

    use super::tests::core_at;
    use super::*;
    use crate::api::FsApi;
    use crate::vnext::Core;
    use std::time::Instant;

    const CHUNK: u64 = 128 * 1024;
    const FILE: u64 = 4 * 1024 * 1024;

    fn setup(_core: &Arc<Core>, f: &CacheFs) -> i64 {
        f.mkdir("/hot", 0o755).unwrap();
        f.create("/hot/f", 0o644).unwrap();
        let mut ws = FsApi::open_write(f, "/hot/f", None).unwrap();
        ws.write(0, &vec![0xA5u8; FILE as usize]).unwrap();
        ws.commit().unwrap();
        f.lookup("/hot/f").unwrap().id
    }

    fn median(v: &mut [u128]) -> f64 {
        v.sort();
        v[v.len() / 2] as f64
    }

    #[test]
    fn attribution_fits_budget_and_sequential_small_budget() {
        // ---- scenario A: budget holds the whole file (8MiB > 4MiB) ----
        let core = core_at("r2a");
        let f = CacheFs::new(core.clone(), "/hot".to_string(), 8 << 20);
        let id = setup(&core, &f);
        let ranges: Vec<(u64, u64)> = (0..(FILE / CHUNK)).map(|i| (i * CHUNK, CHUNK)).collect();

        // one throwaway cold pass (warm the metadata paths too), then 5
        // measured cold+hot pairs; report medians
        for &(off, len) in &ranges {
            f.read_by_id(id, "/hot/f", off, len).unwrap();
        }
        let mut cold = Vec::new();
        let mut hot = Vec::new();
        let mut lookup_only = Vec::new();
        let mut clone_only = Vec::new();
        for _ in 0..5 {
            // cold pass: bust the cache by bumping the version (one commit),
            // which is exactly what makes every slot stale
            let mut ws = FsApi::open_write(core.as_ref(), "/hot/f", None).unwrap();
            ws.write(0, &vec![0xA5u8; FILE as usize]).unwrap();
            ws.commit().unwrap();
            let t = Instant::now();
            for &(off, len) in &ranges {
                f.read_by_id(id, "/hot/f", off, len).unwrap();
            }
            cold.push(t.elapsed().as_micros());

            let t = Instant::now();
            for &(off, len) in &ranges {
                f.read_by_id(id, "/hot/f", off, len).unwrap();
            }
            hot.push(t.elapsed().as_micros());
        }
        // hot-read divides, measured separately (medians of 5):
        for _ in 0..5 {
            let t = Instant::now();
            for _ in 0..32 {
                core.lookup_by_id(id).unwrap();
            }
            lookup_only.push(t.elapsed().as_micros());

            let bytes = f.cache().get(id, i64::MAX, "", 0, CHUNK); // miss path not needed; measure the copy
            let _ = bytes;
            let data = Arc::new(vec![0u8; CHUNK as usize]);
            let t = Instant::now();
            for _ in 0..32 {
                let _copy = (*data).clone();
            }
            clone_only.push(t.elapsed().as_micros());
        }
        let s = f.cache().stats();
        println!(
            "\nR2-A embedded, 32x128KiB over 4MiB, budget 8MiB (medians of 5):\n  cold read pass:  {:>8.1} ms  (miss: lookup + GET + recheck + insert)\n  hot read pass:   {:>8.1} ms  (hit: 1 authority lookup + cache find + copy)\n  hot divides:     32 authority lookups = {:>6.2} ms | 32x128KiB copies = {:>5.2} ms\n  hot total explains as: lookups {:.0}% + copies {:.0}% + cache find (remainder)",
            median(&mut cold) / 1000.0,
            median(&mut hot) / 1000.0,
            median(&mut lookup_only) / 1000.0,
            median(&mut clone_only) / 1000.0,
            100.0 * median(&mut lookup_only) / median(&mut hot),
            100.0 * median(&mut clone_only) / median(&mut hot),
        );
        println!(
            "  stats: hits={} misses={} evictions={} occupied={}KiB/budget={}KiB",
            s.hits,
            s.misses,
            s.evictions,
            f.cache().occupied_bytes() >> 10,
            f.cache().max_bytes() >> 10,
        );
        assert_eq!(s.evictions, 0, "budget holds the whole file: no evictions");

        // ---- scenario B: sequential re-scan with a HALF-size budget ----
        // LRU under sequential scans is the WORST case: by the time chunk i
        // is re-read, it was evicted by the later chunks of the previous
        // pass. The honest expectation is ~0% hits on pass 2 - the cache
        // buys nothing for pure sequential re-scans smaller than the file,
        // and the report must say so.
        let core = core_at("r2b");
        let f = CacheFs::new(core.clone(), "/hot".to_string(), 2 << 20);
        let id = setup(&core, &f);
        for &(off, len) in &ranges {
            f.read_by_id(id, "/hot/f", off, len).unwrap();
        }
        let misses_after_pass1 = f.cache().stats().misses;
        for &(off, len) in &ranges {
            f.read_by_id(id, "/hot/f", off, len).unwrap();
        }
        let s = f.cache().stats();
        let pass2_misses = s.misses - misses_after_pass1;
        println!(
            "\nR2-B sequential re-scan, budget 2MiB < file 4MiB (LRU worst case):\n  pass2 misses: {}/{} (hit rate {:.0}%) evictions={} - sequential re-scans below file size get NOTHING from the cache",
            pass2_misses,
            ranges.len(),
            100.0 * (1.0 - pass2_misses as f64 / ranges.len() as f64),
            s.evictions,
        );
        assert!(
            pass2_misses as f64 > 0.9 * ranges.len() as f64,
            "sequential re-scan below budget must be near-0% hits (documented worst case)"
        );
    }
}
