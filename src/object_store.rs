//! ObjectStore abstraction: the data plane. Backends: S3-compatible
//! (MinIO/OSS/AWS) and an in-memory store used by network-free tests.

pub mod contract;
pub mod mem;
pub mod s3;

use std::io::Read;

use crate::error::{Error, Result};

/// Byte range for partial reads: [start, start + len).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub start: u64,
    pub len: u64,
}

/// One streamed GET response (S4.24): body reader + the response's OWN
/// object metadata + the exact length the reader yields. The metadata is
/// never fetched separately.
pub struct StreamedGet {
    pub reader: Box<dyn Read + Send>,
    /// Backend validator for THIS response (S3 ETag); empty when the
    /// backend supplies none. NOT a content hash — validator semantics only.
    pub etag: String,
    /// Exact physical byte count the reader will yield (Content-Length /
    /// Content-Range derived).
    pub len: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Content fingerprint. CONTRACT: non-empty for every successfully
    /// stored object — metadata persists it as the node fingerprint and the
    /// read cache treats an empty etag as uncacheable, so a backend that
    /// cannot produce one is not a valid ObjectStore.
    pub etag: String,
    /// No contract: backends without bucket versioning return `None`, and a
    /// value from one call need not round-trip through another (the S3 GET
    /// path does not parse it). Never load-bearing.
    pub version_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ObjectData {
    pub info: ObjectInfo,
    /// The requested byte slice; `offset` is the absolute start within the object.
    pub offset: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct ListPage {
    pub items: Vec<ObjectInfo>,
    pub next_token: Option<String>,
}

pub trait ObjectStore: Send + Sync {
    /// Stable identity of THIS configured backend instance (e.g. derived
    /// from endpoint + bucket). Persisted object references name the
    /// backend they were written through, so a read against a different
    /// backend can be refused fail-closed instead of silently mis-read.
    /// The value is opaque to the core; it must be stable across process
    /// restarts for the same configuration.
    fn backend_id(&self) -> String;
    /// Store full content for `key`. Overwrites atomically: once `put`
    /// returns Ok, every later `get`/`head` observes the new content in
    /// full — never a mix of generations. The returned info carries the
    /// stored key, the exact size and a NON-EMPTY etag (see ObjectInfo).
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectInfo>;
    /// Store `data` at `key` ONLY IF `key` does not exist (an atomic
    /// create-if-absent claim). `Ok(true)` = this call created the object;
    /// `Ok(false)` = the key already existed and this call changed
    /// nothing; `Err` = the outcome is UNKNOWN (transport/remote error).
    /// The atomicity contract is absolute: two concurrent claims on the
    /// same key yield exactly one `Ok(true)`.
    ///
    /// CAPABILITY: only meaningful when [`ObjectStore::supports_atomic_claim`]
    /// is true. The default implementation refuses — core must NEVER
    /// degrade to a check-then-put fallback, which has a claim race.
    fn put_if_absent(&self, _key: &str, _data: &[u8]) -> Result<bool> {
        Err(Error::Storage(
            "backend does not implement atomic put-if-absent".into(),
        ))
    }
    /// True when [`ObjectStore::put_if_absent`] is a REAL atomic claim on
    /// this backend. Capability declaration: core refuses unsafe
    /// initialization paths on backends where this is false, instead of
    /// silently falling back to check-then-put.
    fn supports_atomic_claim(&self) -> bool {
        false
    }
    /// Store content for `key` streaming exactly `len` bytes from `reader`.
    /// Bounded memory: the backend must not buffer the whole object (a
    /// plain-PUT path for small `len` is fine — the contract bounds
    /// AGGREGATE memory, not every implementation branch). Reads EXACTLY
    /// `len` bytes: a reader that ends early fails the call. The remote
    /// state after a failed `put_stream` is UNSPECIFIED — the caller owns
    /// the key namespace and must not assume the object is absent.
    fn put_stream(&self, key: &str, reader: &mut dyn Read, len: u64) -> Result<ObjectInfo>;
    /// Read `key`, optionally a byte range. A missing key is
    /// `Error::ObjectMissing`. With a range, the returned bytes are
    /// `[start, min(start+len, object_size))` and `ObjectData.offset`
    /// equals the requested `start`. CALLER RESPONSIBILITY (core clamps
    /// before the store sees a range): `start < object_size` and
    /// `len > 0`. `start >= size` or `len == 0` is deliberately NOT part
    /// of this contract — backends legitimately differ there (in-memory
    /// returns empty data, S3 answers 416) and callers must never rely on
    /// either.
    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData>;
    /// Stream `key` (optionally a range): bounded memory on the backend
    /// side. Range semantics as [`ObjectStore::get`].
    ///
    /// Contract (S4.24): the reader, the object's validator (`etag`) and
    /// the exact physical byte length the reader will yield ALL come from
    /// the SAME response — no second request, and no HEAD-then-GET
    /// two-version assumption. A backend that cannot supply an etag for
    /// this response reports the empty string; callers compare recorded
    /// validators only against non-empty values.
    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<StreamedGet>;
    /// Metadata for `key`; a missing key is `Error::ObjectMissing`.
    fn head(&self, key: &str) -> Result<ObjectInfo>;
    /// Presigned GET URL where supported (S3); None otherwise. OPTIONAL
    /// capability: `None` is always an acceptable answer.
    fn presign_get(&self, _key: &str, _expires_secs: u64) -> Option<String> {
        None
    }
    /// Delete `key`; deleting a missing key is not an error (idempotent).
    fn delete(&self, key: &str) -> Result<()>;
    /// List keys under `prefix`, one page at a time: keys ascending,
    /// at most `max` items per page, no keys outside the prefix, and a
    /// consecutive walk (feeding each page's `next_token` back) yields
    /// every matching key exactly once. Tokens are opaque: a backend may
    /// use any string (the in-memory backend uses the last key, S3 uses
    /// continuation tokens — callers must not parse either).
    ///
    /// CALLER RESPONSIBILITY (not part of this contract): keys where one
    /// key is another key's directory prefix (`b` together with `b/y`).
    /// Real backends genuinely disagree there — S3's flat namespace
    /// returns both, while MinIO (RELEASE.2025-04-22, verified on
    /// darwin/arm64 and linux/arm64) silently drops the whole subtree
    /// under `b` from every listing. LightFS never creates such keys
    /// (content, version and staging keys are all single-leaf paths under
    /// a directory-shaped prefix), and callers must keep it that way.
    fn list(&self, prefix: &str, token: Option<&str>, max: usize) -> Result<ListPage>;
    /// True when the backend can reach its remote endpoint.
    fn ping(&self) -> Result<()>;
}

pub fn range_to_header(range: Range) -> String {
    format!(
        "bytes={}-{}",
        range.start,
        range.start + range.len.saturating_sub(1)
    )
}

pub fn parse_http_range(header: &str, object_size: u64) -> Result<Option<Range>> {
    let spec = match header.strip_prefix("bytes=") {
        Some(s) => s.trim(),
        None => return Err(Error::InvalidArgument("range must be bytes=..".into())),
    };
    let Some((start_s, end_s)) = spec.split_once('-') else {
        return Err(Error::InvalidArgument("range needs start-end".into()));
    };
    let start: u64 = start_s
        .parse()
        .map_err(|_| Error::InvalidArgument("range start".into()))?;
    let range = if end_s.is_empty() {
        Range {
            start,
            len: object_size.saturating_sub(start),
        }
    } else {
        let end: u64 = end_s
            .parse()
            .map_err(|_| Error::InvalidArgument("range end".into()))?;
        Range {
            start,
            len: end.saturating_sub(start).saturating_add(1),
        }
    };
    if start >= object_size {
        return Ok(None); // empty read past EOF
    }
    Ok(Some(Range {
        start,
        len: range.len.min(object_size - start),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_parse() {
        assert_eq!(
            parse_http_range("bytes=0-4", 100).unwrap(),
            Some(Range { start: 0, len: 5 })
        );
        assert_eq!(
            parse_http_range("bytes=10-", 100).unwrap(),
            Some(Range { start: 10, len: 90 })
        );
        assert_eq!(
            parse_http_range("bytes=95-1000", 100).unwrap(),
            Some(Range { start: 95, len: 5 })
        );
        assert_eq!(parse_http_range("bytes=100-", 100).unwrap(), None);
        assert!(parse_http_range("junk=1-2", 10).is_err());
    }

    #[test]
    fn range_header() {
        assert_eq!(range_to_header(Range { start: 2, len: 3 }), "bytes=2-4");
    }
}
