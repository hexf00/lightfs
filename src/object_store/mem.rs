//! In-memory ObjectStore: deterministic, no network. Used by core unit tests
//! and anywhere correctness must not depend on MinIO.

use std::collections::BTreeMap;
use std::io::Read;
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::object_store::{ListPage, ObjectData, ObjectInfo, ObjectStore, Range};

#[derive(Default)]
struct Entry {
    data: Vec<u8>,
    etag: String,
    version: u64,
}

pub struct MemObjectStore {
    objects: Mutex<BTreeMap<String, Entry>>,
    counter: std::sync::atomic::AtomicU64,
    /// Test-only failure injection: deletes of a key present here error
    /// once (gc retry-path coverage without a network).
    delete_failures: Mutex<std::collections::HashSet<String>>,
    /// Test-only failure injection: heads of a key present here error once
    /// (recover() fail-closed coverage without a network).
    head_failures: Mutex<std::collections::HashSet<String>>,
}

impl Default for MemObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemObjectStore {
    pub fn new() -> Self {
        Self {
            objects: Mutex::new(BTreeMap::new()),
            counter: std::sync::atomic::AtomicU64::new(1),
            delete_failures: Mutex::new(std::collections::HashSet::new()),
            head_failures: Mutex::new(std::collections::HashSet::new()),
        }
    }

    /// Test-only: make the next `delete(key)` return a storage error so gc
    /// deletion-task retry paths can be driven deterministically.
    pub fn inject_delete_failure(&self, key: &str) {
        self.delete_failures.lock().unwrap().insert(key.to_string());
    }

    /// Test-only: make the next `head(key)` return a storage error (not
    /// ObjectMissing) so recover()'s fail-closed branch can be driven
    /// deterministically.
    pub fn inject_head_failure(&self, key: &str) {
        self.head_failures.lock().unwrap().insert(key.to_string());
    }

    fn info(&self, key: &str, e: &Entry) -> ObjectInfo {
        ObjectInfo {
            key: key.to_string(),
            size: e.data.len() as u64,
            etag: e.etag.clone(),
            version_id: Some(e.version.to_string()),
        }
    }
}

impl ObjectStore for MemObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectInfo> {
        let version = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let etag = crate::sigv4::sha256_hex(data)[..32].to_string();
        let mut objs = self.objects.lock().unwrap();
        let e = Entry {
            data: data.to_vec(),
            etag: etag.clone(),
            version,
        };
        let info = self.info(key, &e);
        objs.insert(key.to_string(), e);
        Ok(info)
    }

    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData> {
        let objs = self.objects.lock().unwrap();
        let e = objs
            .get(key)
            .ok_or_else(|| Error::ObjectMissing(key.to_string()))?;
        let info = self.info(key, e);
        let (offset, data) = match range {
            None => (0u64, e.data.clone()),
            Some(r) => {
                if r.start >= e.data.len() as u64 {
                    (r.start, Vec::new())
                } else {
                    let s = r.start as usize;
                    let end = (r.start + r.len).min(e.data.len() as u64) as usize;
                    (r.start, e.data[s..end].to_vec())
                }
            }
        };
        Ok(ObjectData { info, offset, data })
    }

    fn put_stream(&self, key: &str, reader: &mut dyn Read, len: u64) -> Result<ObjectInfo> {
        // In-memory store: buffering is inherent, but keep it exact.
        let mut data = vec![0u8; len as usize];
        reader.read_exact(&mut data)?;
        self.put(key, &data)
    }

    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<(Box<dyn Read + Send>, u64)> {
        let objs = self.objects.lock().unwrap();
        let e = objs
            .get(key)
            .ok_or_else(|| Error::ObjectMissing(key.to_string()))?;
        let (slice, offset) = match range {
            None => (&e.data[..], 0u64),
            Some(r) => {
                if r.start >= e.data.len() as u64 {
                    (&e.data[e.data.len()..], r.start)
                } else {
                    let s_ = r.start as usize;
                    let end = (r.start + r.len).min(e.data.len() as u64) as usize;
                    (&e.data[s_..end], r.start)
                }
            }
        };
        let len = slice.len() as u64;
        let _ = offset;
        Ok((Box::new(std::io::Cursor::new(slice.to_vec())), len))
    }

    fn head(&self, key: &str) -> Result<ObjectInfo> {
        if self.head_failures.lock().unwrap().remove(key) {
            return Err(Error::Storage(format!(
                "injected head failure for testing: {key}"
            )));
        }
        let objs = self.objects.lock().unwrap();
        let e = objs
            .get(key)
            .ok_or_else(|| Error::ObjectMissing(key.to_string()))?;
        Ok(self.info(key, e))
    }

    fn delete(&self, key: &str) -> Result<()> {
        if self.delete_failures.lock().unwrap().remove(key) {
            return Err(Error::Storage(format!(
                "injected delete failure for testing: {key}"
            )));
        }
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }

    fn list(&self, prefix: &str, token: Option<&str>, max: usize) -> Result<ListPage> {
        use std::ops::Bound::{Excluded, Included, Unbounded};
        let objs = self.objects.lock().unwrap();
        let mut items: Vec<ObjectInfo> = Vec::new();
        let mut next_token = None;
        // A continuation token is the last key already returned: resume strictly after it.
        let start = match token {
            Some(t) => Excluded(t.to_string()),
            None => Included(prefix.to_string()),
        };
        for (k, e) in objs.range::<String, _>((start, Unbounded)) {
            if !k.starts_with(prefix) {
                break;
            }
            if items.len() >= max {
                next_token = Some(items.last().unwrap().key.clone());
                break;
            }
            items.push(self.info(k, e));
        }
        Ok(ListPage { items, next_token })
    }

    fn ping(&self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> MemObjectStore {
        MemObjectStore::new()
    }

    #[test]
    fn put_get_head_delete() {
        let s = store();
        let info = s.put("objects/1/data", b"hello").unwrap();
        assert_eq!(info.size, 5);
        assert_eq!(s.get("objects/1/data", None).unwrap().data, b"hello");
        let head = s.head("objects/1/data").unwrap();
        assert_eq!(head.etag, info.etag);
        s.delete("objects/1/data").unwrap();
        assert!(matches!(
            s.head("objects/1/data"),
            Err(Error::ObjectMissing(_))
        ));
        s.delete("objects/1/data").unwrap(); // idempotent
    }

    #[test]
    fn range_get() {
        let s = store();
        s.put("k", b"0123456789").unwrap();
        let d = s.get("k", Some(Range { start: 2, len: 3 })).unwrap();
        assert_eq!(d.data, b"234");
        assert_eq!(d.offset, 2);
        let d = s.get("k", Some(Range { start: 9, len: 100 })).unwrap();
        assert_eq!(d.data, b"9");
        let d = s.get("k", Some(Range { start: 10, len: 5 })).unwrap();
        assert!(d.data.is_empty());
    }

    #[test]
    fn overwrite_changes_etag() {
        let s = store();
        let a = s.put("k", b"aaa").unwrap();
        let b = s.put("k", b"bbbb").unwrap();
        assert_ne!(a.etag, b.etag);
        assert_eq!(b.size, 4);
    }

    #[test]
    fn list_prefix_and_pagination() {
        let s = store();
        for i in 0..7 {
            s.put(&format!("objects/{i}/data"), b"x").unwrap();
        }
        s.put("other/junk", b"x").unwrap();
        let page1 = s.list("objects/", None, 3).unwrap();
        assert_eq!(page1.items.len(), 3);
        let token = page1.next_token.clone().unwrap();
        let page2 = s.list("objects/", Some(&token), 3).unwrap();
        assert_eq!(page2.items.len(), 3);
        let page3 = s.list("objects/", page2.next_token.as_deref(), 10).unwrap();
        assert_eq!(page3.items.len(), 1);
        assert!(page3.next_token.is_none());
        // full walk equals 7 keys
        let all = s.list("objects/", None, 100).unwrap();
        assert_eq!(all.items.len(), 7);
    }
}
