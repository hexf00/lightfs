//! Shared ObjectStore contract suite (OS-1, S4.6). One function drives the
//! WHOLE behavioral contract of [`crate::object_store::ObjectStore`] against
//! any backend, so a new backend proves itself by running the same suite the
//! in-memory and S3 backends run — not by hand-copying a test checklist.
//!
//! Rules the suite itself obeys:
//! - trait surface only: no backend fields, no error-text matching (the one
//!   error identity the contract names is `Error::ObjectMissing`, a variant),
//!   no request-count assertions;
//! - every case keys under the caller-supplied namespace `ns`, so parallel
//!   suites (unit + integration, or two backends) never collide;
//! - the suite cleans up its own namespace even when an assertion panics
//!   (catch/resume around the body) and never touches keys outside `ns`;
//! - environment failures (MinIO down, credentials) are the CALLER's
//!   concern: run `ping()` before the suite — inside it, any error is
//!   reported as a contract failure with the backend error inline.
//!
//! What the suite deliberately does NOT assert (documented in the trait):
//! - `start >= object_size` ranges and `len == 0` ranges (backends differ;
//!   core clamps both before the store sees them);
//! - keys that are another key's directory prefix (`b` beside `b/y`) —
//!   MinIO drops the subtree from listings, S3 returns both; LightFS
//!   never writes such keys, so the fixture avoids the shape entirely;
//! - remote state after a failed `put_stream`;
//! - `version_id` on any call;
//! - presign success (optional capability); and it makes no claim about
//!   bounded-memory or real-network behavior — those need the integration
//!   harness and fault injection, not a functional suite.


use crate::error::Error;
use crate::object_store::{ObjectStore, Range};

/// Run the full contract suite against `store`, keying everything under
/// `ns` (the caller picks a unique prefix, e.g. `os1-mem/` or
/// `os1-s3-<ts>/`). Panics with an `OS-1 contract:` message on the first
/// violated clause; the namespace is cleaned up on the way out either way.
pub fn run_contract_suite(store: &dyn ObjectStore, ns: &str) {
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        basic_objects(store, ns);
        streaming(store, ns);
        ranges(store, ns);
        delete_and_listing(store, ns);
        object_info_and_presign(store, ns);
    }));
    cleanup_namespace(store, ns);
    if let Err(err) = r {
        std::panic::resume_unwind(err);
    }
}

/// Best-effort removal of every object the suite created. Counted failures
/// are reported but never mask the original assertion panic.
fn cleanup_namespace(store: &dyn ObjectStore, ns: &str) {
    let mut token: Option<String> = None;
    let mut failed = 0usize;
    loop {
        let page = match store.list(ns, token.as_deref(), 1000) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("OS-1 contract cleanup: list {ns} failed: {e}");
                return;
            }
        };
        let next = page.next_token.clone();
        for it in &page.items {
            if let Err(e) = store.delete(&it.key) {
                failed += 1;
                eprintln!("OS-1 contract cleanup: delete {} failed: {e}", it.key);
            }
        }
        token = next;
        if token.is_none() {
            break;
        }
    }
    if failed > 0 {
        eprintln!("OS-1 contract cleanup: {failed} object(s) under {ns} left behind");
    }
}

fn pattern(len: u64) -> Vec<u8> {
    (0..len as usize)
        .map(|i| (i.wrapping_mul(31) % 251) as u8)
        .collect()
}

fn must<T>(what: &str, r: Result<T, Error>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("OS-1 contract: {what} failed: {e}"),
    }
}

/// A: put → get → head consistency, overwrite, empty object, missing.
fn basic_objects(store: &dyn ObjectStore, ns: &str) {
    let k = format!("{ns}basic/plain");
    let info = must("put", store.put(&k, b"hello world"));
    assert_eq!(info.key, k, "OS-1 contract: put must echo the key");
    assert_eq!(
        info.size, 11,
        "OS-1 contract: put must report the exact size"
    );
    assert!(
        !info.etag.is_empty(),
        "OS-1 contract: put must return a non-empty etag"
    );

    let got = must("get", store.get(&k, None));
    assert_eq!(
        got.data, b"hello world",
        "OS-1 contract: get must return the stored bytes"
    );
    assert_eq!(
        got.offset, 0,
        "OS-1 contract: full get must report offset 0"
    );
    assert_eq!(got.info.key, k, "OS-1 contract: get must echo the key");

    let head = must("head", store.head(&k));
    assert_eq!(head.key, k);
    assert_eq!(
        head.size, 11,
        "OS-1 contract: head must agree with the stored size"
    );
    assert_eq!(
        head.etag, info.etag,
        "OS-1 contract: head etag must equal the put etag"
    );

    // Overwrite with a DIFFERENT length: after Ok, reads see the new object in full.
    let i2 = must("put(overwrite)", store.put(&k, b"xy"));
    assert_eq!(i2.size, 2);
    let got2 = must("get(after overwrite)", store.get(&k, None));
    assert_eq!(
        got2.data, b"xy",
        "OS-1 contract: overwrite must be complete on return"
    );
    assert_eq!(got2.info.size, 2);

    // Empty object is a real object.
    let ke = format!("{ns}basic/empty");
    let ie = must("put(empty)", store.put(&ke, b""));
    assert_eq!(ie.size, 0);
    assert!(
        !ie.etag.is_empty(),
        "OS-1 contract: empty object still carries a non-empty etag"
    );
    let ge = must("get(empty)", store.get(&ke, None));
    assert!(ge.data.is_empty());
    let he = must("head(empty)", store.head(&ke));
    assert_eq!(he.size, 0);

    // Missing key: the one error identity the contract names.
    let km = format!("{ns}basic/never-created");
    assert!(
        matches!(store.get(&km, None), Err(Error::ObjectMissing(_))),
        "OS-1 contract: get of a missing key must be Error::ObjectMissing"
    );
    assert!(
        matches!(store.head(&km), Err(Error::ObjectMissing(_))),
        "OS-1 contract: head of a missing key must be Error::ObjectMissing"
    );
}

/// B: put_stream exactness (0 / 1 / regular / multi-buffer lengths), short
/// input fails, get_stream yields exactly the promised bytes.
fn streaming(store: &dyn ObjectStore, ns: &str) {
    let cases: [(&str, u64); 4] = [
        ("empty", 0),
        ("one", 1),
        ("regular", 1024),
        ("across-buffers", 1024 * 1024 + 137),
    ];
    for (name, len) in cases {
        let k = format!("{ns}stream/{name}");
        let data = pattern(len);
        let info = must(
            "put_stream",
            store.put_stream(&k, &mut std::io::Cursor::new(&data), len),
        );
        assert_eq!(
            info.size, len,
            "OS-1 contract: put_stream must report the exact size"
        );
        let got = must("get(after put_stream)", store.get(&k, None));
        assert_eq!(
            got.data, data,
            "OS-1 contract: put_stream must store EXACTLY the len bytes read"
        );
    }

    // Reader ends before `len`: the call must fail. (Whether the remote
    // object exists afterwards is unspecified — not asserted.)
    let ks = format!("{ns}stream/short-input");
    let short = b"only-ten!".to_vec();
    let r = store.put_stream(&ks, &mut std::io::Cursor::new(short), 100);
    assert!(
        r.is_err(),
        "OS-1 contract: put_stream with a short reader must fail"
    );

    // get_stream: the promised length is exact and the bytes complete.
    let kg = format!("{ns}stream/across-buffers");
    let mut sg = must("get_stream", store.get_stream(&kg, None));
    let (rd, n) = (sg.reader.as_mut(), sg.len);
    let mut buf = Vec::new();
    rd.read_to_end(&mut buf)
        .unwrap_or_else(|e| panic!("OS-1 contract: get_stream reader failed: {e}"));
    assert_eq!(
        n as usize,
        buf.len(),
        "OS-1 contract: get_stream length must equal the bytes the reader yields"
    );
    assert_eq!(buf, pattern(1024 * 1024 + 137));
}

/// C: head / middle / tail ranges, EOF clamping — every range starts below
/// the object size and has len > 0 (the caller-clamped subset; see trait).
fn ranges(store: &dyn ObjectStore, ns: &str) {
    let k = format!("{ns}range/obj");
    must("put(range fixture)", store.put(&k, b"0123456789"));

    let d = must(
        "get(head range)",
        store.get(&k, Some(Range { start: 0, len: 3 })),
    );
    assert_eq!(d.data, b"012");
    assert_eq!(
        d.offset, 0,
        "OS-1 contract: ranged get offset must equal the requested start"
    );

    let d = must(
        "get(middle range)",
        store.get(&k, Some(Range { start: 4, len: 3 })),
    );
    assert_eq!(d.data, b"456");
    assert_eq!(d.offset, 4);

    let d = must(
        "get(tail exact)",
        store.get(&k, Some(Range { start: 5, len: 5 })),
    );
    assert_eq!(d.data, b"56789");

    // start+len beyond EOF clamps to the object end.
    let d = must(
        "get(tail clamp)",
        store.get(&k, Some(Range { start: 9, len: 5 })),
    );
    assert_eq!(d.data, b"9");
    assert_eq!(d.offset, 9);

    // A range on a MISSING key is still ObjectMissing, never a range error.
    assert!(
        matches!(
            store.get(&format!("{ns}range/none"), Some(Range { start: 0, len: 1 })),
            Err(Error::ObjectMissing(_))
        ),
        "OS-1 contract: ranged get of a missing key must be Error::ObjectMissing"
    );

    // get_stream honors the same range semantics.
    let mut sgr = must(
        "get_stream(range)",
        store.get_stream(&k, Some(Range { start: 4, len: 3 })),
    );
    let (rd, n) = (sgr.reader.as_mut(), sgr.len);
    let mut buf = Vec::new();
    rd.read_to_end(&mut buf)
        .unwrap_or_else(|e| panic!("OS-1 contract: ranged get_stream reader failed: {e}"));
    assert_eq!(
        n, 3,
        "OS-1 contract: ranged get_stream length must be the clamped range"
    );
    assert_eq!(buf, b"456");
}

/// D: delete idempotency, listing order/prefix/pagination.
fn delete_and_listing(store: &dyn ObjectStore, ns: &str) {
    // Delete semantics.
    let k = format!("{ns}del/one");
    must("put(delete fixture)", store.put(&k, b"x"));
    must("delete(existing)", store.delete(&k));
    assert!(
        matches!(store.get(&k, None), Err(Error::ObjectMissing(_))),
        "OS-1 contract: a deleted key must read as ObjectMissing"
    );
    must("delete(already deleted)", store.delete(&k));
    must(
        "delete(never existed)",
        store.delete(&format!("{ns}del/never")),
    );

    // Listing fixture: similar names where byte order matters ('-' = 0x2D
    // sorts before '/' = 0x2F), plus a sibling prefix that must not leak in.
    // Deliberately NO bare `b` beside `b/y`: a key that is another key's
    // directory prefix is outside the contract (see trait `list`) — MinIO
    // drops the whole `b/` subtree from listings when the object `b`
    // exists, while S3 returns both. LightFS never writes such keys.
    let base = format!("{ns}list");
    let keys: Vec<String> = ["a", "b-x", "b/y", "c"]
        .iter()
        .map(|s| format!("{base}/{s}"))
        .collect();
    for (i, key) in keys.iter().enumerate() {
        must(
            "put(list fixture)",
            store.put(key, format!("v{i}").as_bytes()),
        );
    }
    let other = format!("{ns}other/z");
    must("put(other prefix)", store.put(&other, b"v"));

    // Full single-page walk: exact key set, ascending.
    let page = must("list(full)", store.list(&base, None, 1000));
    assert_eq!(
        page.items.iter().map(|i| i.key.clone()).collect::<Vec<_>>(),
        keys,
        "OS-1 contract: list must return exactly the prefix's keys in ascending order"
    );
    assert!(
        page.next_token.is_none(),
        "OS-1 contract: an untruncated list has no next token"
    );
    // listed sizes agree with the stored content.
    for it in &page.items {
        assert_eq!(
            it.size, 2,
            "OS-1 contract: list size must match the stored object"
        );
    }

    // Paged walk with a tiny max: every page ≤ max, tokens advance, the
    // union is exactly the same key set with no duplicates — the token is
    // treated as opaque throughout.
    let mut walked: Vec<String> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = must("list(paged)", store.list(&base, token.as_deref(), 2));
        assert!(
            page.items.len() <= 2,
            "OS-1 contract: a list page must never exceed max"
        );
        for it in &page.items {
            assert!(
                !walked.contains(&it.key),
                "OS-1 contract: paged list must not repeat keys"
            );
            walked.push(it.key.clone());
        }
        match page.next_token {
            Some(t) => {
                assert!(
                    !t.is_empty(),
                    "OS-1 contract: a truncated page carries a non-empty token"
                );
                token = Some(t);
            }
            None => break,
        }
    }
    assert_eq!(
        walked, keys,
        "OS-1 contract: paged walk must cover every key exactly once, in order"
    );
}

/// E: ObjectInfo guarantees (key/size/etag; version_id deliberately
/// unasserted) and presign as an optional capability.
fn object_info_and_presign(store: &dyn ObjectStore, ns: &str) {
    let k = format!("{ns}info/one");
    let p = must("put(info fixture)", store.put(&k, b"contract"));
    assert_eq!(p.key, k);
    assert_eq!(p.size, 8);
    assert!(!p.etag.is_empty());

    let g = must("get(info)", store.get(&k, None));
    assert_eq!(g.info.key, k);
    assert_eq!(g.info.size, 8);
    assert_eq!(
        g.info.etag, p.etag,
        "OS-1 contract: get etag must equal the put etag"
    );

    let h = must("head(info)", store.head(&k));
    assert_eq!(h.size, 8);
    assert_eq!(h.etag, p.etag);

    // version_id: NO guarantee (in-memory fabricates one, S3 returns it
    // only from versioned buckets and not on GET at all) — unasserted.

    // presign_get is optional: None is always acceptable; Some must at
    // least be a usable-looking URL string.
    if let Some(url) = store.presign_get(&k, 60) {
        assert!(
            !url.is_empty(),
            "OS-1 contract: a presigned URL must be non-empty"
        );
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn mem_object_store_passes_the_contract_suite() {
        let s = crate::object_store::mem::MemObjectStore::new();
        super::run_contract_suite(&s, "os1-mem/");
    }
}
