//! ObjectStore abstraction: the data plane. Backends: S3-compatible
//! (MinIO/OSS/AWS) and an in-memory store used by network-free tests.

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    pub etag: String,
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
    /// Store full content for `key`. Overwrites atomically.
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectInfo>;
    /// Store content for `key` streaming exactly `len` bytes from `reader`.
    /// Bounded memory: the backend must not buffer the whole object.
    fn put_stream(&self, key: &str, reader: &mut dyn Read, len: u64) -> Result<ObjectInfo>;
    /// Read `key`, optionally a byte range.
    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData>;
    /// Stream `key` (optionally a range): returns a reader and the exact
    /// number of bytes it will yield. Bounded memory on the backend side.
    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<(Box<dyn Read + Send>, u64)>;
    fn head(&self, key: &str) -> Result<ObjectInfo>;
    /// Presigned GET URL where supported (S3); None otherwise.
    fn presign_get(&self, _key: &str, _expires_secs: u64) -> Option<String> {
        None
    }
    /// Delete `key`; deleting a missing key is not an error (idempotent).
    fn delete(&self, key: &str) -> Result<()>;
    /// List keys under `prefix`, one page at a time (keys ascending).
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
