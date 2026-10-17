//! vnext disaster recovery, S4.20 contract: when SQLite is gone, the
//! guarantee is CONTENT SALVAGE — never namespace reconstruction.
//!
//! Backend-neutrality note (long-term constraint): this entry point
//! DEPENDS on the backend's enumeration capability (`ObjectStore::list`
//! over a prefix). That is a declared capability of the SALVAGE path,
//! not a requirement imposed on every storage backend — a backend
//! without enumeration simply cannot serve this entry point, and the
//! failure is explicit (the list error propagates), never silent.
//!
//! The process is strictly READ-ONLY: no object is written, deleted or
//! modified. It runs entirely outside the commit path; salvage and GC
//! are separate, unrelated operations.
//!
//! Every object under the data prefix is verified (envelope magic,
//! format version, kind, encoder/cipher ids, logical length, sha256
//! digest). Valid payloads are exported verbatim with their original
//! key, the stable file identity parsed from the key layout, and their
//! verification status. Anything invalid, unknown-format or unreadable
//! is REPORTED per object — a bad object never aborts the pass and
//! never reaches the caller as content. No distinction is drawn between
//! "committed" and "orphan" objects: without the ledger there is no
//! authority to make that call, so every recovered object is a
//! restoration CANDIDATE. Paths, directory structure, deletion state
//! and latest-version selection are NOT recoverable here.

use crate::error::{Error, Result};
use crate::object_store::ObjectStore;
use crate::vnext::envelope::{self, ObjKind};

/// The object-store prefix holding immutable data payloads. Both the
/// commit path (writes) and this salvage path (reads) share the
/// constant so a layout change cannot silently orphan salvage.
pub const DATA_PREFIX: &str = "objects/";

/// One verifiably intact data payload.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SalvagedObject {
    /// Original object-store key (the physical locator as written).
    pub key: String,
    /// Stable logical file identity parsed from the key layout
    /// (`objects/{fid}/w-{uniq}.data`); `None` when a key does not
    /// follow the layout (still salvageable as content).
    pub file_id: Option<i64>,
    pub logical_len: u64,
    /// Payload bytes, byte-exact as committed (digest-verified).
    pub content: Vec<u8>,
}

/// One object that could NOT be restored, with the reason. Never an
/// error-level failure of the whole pass.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RejectedObject {
    pub key: String,
    pub reason: String,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct SalvageReport {
    /// Verified restoration candidates (committed or not — unknown).
    pub recovered: Vec<SalvagedObject>,
    /// Explicitly reported failures: corrupt envelopes, unknown format
    /// versions, unknown object types, unreadable objects.
    pub rejected: Vec<RejectedObject>,
    /// Everything the enumeration returned.
    pub scanned: usize,
}

impl SalvageReport {
    /// Export every recovered payload; the caller owns the bytes.
    pub fn contents(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.recovered
            .iter()
            .map(|o| (o.key.as_str(), o.content.as_slice()))
    }
}

/// Enumerate the data prefix and salvage every verifiable object.
///
/// CAPABILITY: relies on `ObjectStore::list` (see module docs). Errors
/// from the backend (including permission failures) propagate — an
/// inaccessible store is an explicit error, not an empty report.
pub fn salvage_all_data(objects: &dyn ObjectStore) -> Result<SalvageReport> {
    let mut report = SalvageReport::default();
    let mut token: Option<String> = None;
    loop {
        let page = objects.list(DATA_PREFIX, token.as_deref(), 100)?;
        for info in &page.items {
            report.scanned += 1;
            let key = info.key.clone();
            match salvage_one(objects, &key) {
                Ok(obj) => report.recovered.push(obj),
                Err(reason) => report.rejected.push(RejectedObject { key, reason }),
            }
        }
        token = page.next_token;
        if token.is_none() {
            break;
        }
    }
    Ok(report)
}

/// Read and verify one object. `Err` carries the PER-OBJECT reason.
fn salvage_one(objects: &dyn ObjectStore, key: &str) -> std::result::Result<SalvagedObject, String> {
    let file_id = key
        .strip_prefix(DATA_PREFIX)
        .and_then(|rest| rest.split('/').next())
        .and_then(|fid| fid.parse::<i64>().ok());
    let obj = objects.get(key, None).map_err(|e| match e {
        Error::ObjectMissing(_) => "object listed but missing on read".to_string(),
        Error::Storage(m) => format!("unreadable: {m}"),
        other => format!("unreadable: {other}"),
    })?;
    let (kind, content) = envelope::open(&obj.data).map_err(|e| e.to_string())?;
    if kind != ObjKind::Data {
        // Legacy metadata copies (S4.19 Tree/Commit/Ref objects) or
        // anything unexpected: reported, never returned as file content.
        return Err(format!("not a data object (kind {})", kind as u8));
    }
    Ok(SalvagedObject {
        key: key.to_string(),
        file_id,
        logical_len: content.len() as u64,
        content,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::mem::MemObjectStore;
    use crate::vnext::envelope::ObjKind;

    #[test]
    fn salvage_reports_per_object_and_is_read_only() {
        let m = MemObjectStore::new();
        m.put("objects/1/w-a.data", &envelope::seal(ObjKind::Data, b"good-one"))
            .unwrap();
        // 60 junk bytes: parses far enough to fail on the magic
        m.put("objects/2/w-b.data", &vec![0x00u8; 60]).unwrap();
        // unknown format version
        let mut unknown = envelope::seal(ObjKind::Data, b"x");
        unknown[4] = 99;
        m.put("objects/3/w-c.data", &unknown).unwrap();
        // a legacy metadata copy where a payload would sit
        m.put("objects/9/legacy.data", &envelope::seal(ObjKind::Tree, b"[]"))
            .unwrap();

        let before = m.list(DATA_PREFIX, None, 1000).unwrap().items.len();
        let report = salvage_all_data(&m).unwrap();
        let after = m.list(DATA_PREFIX, None, 1000).unwrap().items.len();
        assert_eq!(before, after, "salvage must not mutate the store");
        assert_eq!(report.scanned, 4);
        assert_eq!(report.recovered.len(), 1);
        assert_eq!(report.recovered[0].file_id, Some(1));
        assert_eq!(report.recovered[0].content, b"good-one");
        assert_eq!(report.rejected.len(), 3, "{:?}", report.rejected);
        assert!(report.rejected.iter().any(|r| r.reason.contains("bad magic")));
        assert!(report
            .rejected
            .iter()
            .any(|r| r.reason.contains("unknown format version")));
        assert!(report
            .rejected
            .iter()
            .any(|r| r.reason.contains("not a data object")));
        let exported: Vec<_> = report.contents().collect();
        assert_eq!(exported, vec![("objects/1/w-a.data", b"good-one".as_slice())]);
    }
}
