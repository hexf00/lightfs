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
//! physical LOCATOR (diagnostic information only) and their verification
//! status. Anything invalid, unknown-format or unreadable is REPORTED per
//! object — a bad object never aborts the pass and never reaches the
//! caller as content. No distinction is drawn between "committed" and
//! "orphan" objects: without the ledger there is no authority to make
//! that call, so every recovered object is a restoration CANDIDATE.
//! Paths, directory structure, deletion state and latest-version
//! selection are NOT recoverable here.
//!
//! Identity rule (S4.21): the locator is NOT parsed for logical identity.
//! The core's own reference model never derives a File ID from a storage
//! key, and salvage holds to the same rule — a key that happens to embed
//! a file id confers no authority. Recovered content is matched to files
//! by integrity (etag/digest) through whatever adoption tooling consumes
//! this report, never by key shape.

use crate::error::{Error, Result};
use crate::object_store::ObjectStore;
use crate::vnext::envelope::{self, ObjKind};
use crate::vnext::core::TempFileGuard;

/// The object-store prefix holding immutable data payloads. Both the
/// commit path (writes) and this salvage path (reads) share the
/// constant so a layout change cannot silently orphan salvage.
pub const DATA_PREFIX: &str = "objects/";

/// One verifiably intact data payload's METADATA (S4.24: the payload
/// bytes themselves are never accumulated in this report — they stream
/// once through [`SalvageSink::verified`] and are gone).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct SalvagedItem {
    /// Original physical locator, verbatim as the backend reported it.
    /// DIAGNOSTIC ONLY — carries no logical identity and must never be
    /// parsed to guess one.
    pub locator: String,
    pub payload_len: u64,
}

/// Per-object consumer for verified salvage output. `verified` runs ONLY
/// after the object's footer and header fully validated; `reader` yields
/// the payload exactly `payload_len` bytes from a private staging file
/// that is DELETED when the call returns — consume (copy/hash/forward)
/// inside the call. A consumer IO error is recorded per-object as a
/// rejection; the staging file is never delivered as a recovered file on
/// any failure path.
pub trait SalvageSink {
    fn verified(
        &mut self,
        locator: &str,
        payload_len: u64,
        reader: &mut dyn std::io::Read,
    ) -> std::io::Result<()>;
}

impl<F> SalvageSink for F
where
    F: FnMut(&str, u64, &mut dyn std::io::Read) -> std::io::Result<()>,
{
    fn verified(
        &mut self,
        locator: &str,
        payload_len: u64,
        reader: &mut dyn std::io::Read,
    ) -> std::io::Result<()> {
        self(locator, payload_len, reader)
    }
}

/// One object that could NOT be restored, with the reason. Never an
/// error-level failure of the whole pass.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RejectedObject {
    /// Physical locator, verbatim (diagnostic only, see SalvagedObject).
    pub locator: String,
    pub reason: String,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct SalvageReport {
    /// Verified restoration candidates (committed or not — unknown);
    /// metadata only, see [`SalvagedItem`].
    pub recovered: Vec<SalvagedItem>,
    /// Explicitly reported failures: corrupt envelopes, unknown format
    /// versions, unknown object types, unreadable objects, consumer
    /// failures.
    pub rejected: Vec<RejectedObject>,
    /// Everything the enumeration returned.
    pub scanned: usize,
}

/// Enumerate the data prefix and salvage every verifiable object.
///
/// CAPABILITY: relies on `ObjectStore::list` (see module docs). Errors
/// from the backend (including permission failures) propagate — an
/// inaccessible store is an explicit error, not an empty report.
pub fn salvage_all_data(
    objects: &dyn ObjectStore,
    sink: &mut dyn SalvageSink,
) -> Result<SalvageReport> {
    let mut report = SalvageReport::default();
    let mut token: Option<String> = None;
    loop {
        let page = objects.list(DATA_PREFIX, token.as_deref(), 100)?;
        for info in &page.items {
            report.scanned += 1;
            let key = info.key.clone();
            match salvage_one(objects, &key, sink) {
                Ok(item) => report.recovered.push(item),
                Err(reason) => report.rejected.push(RejectedObject { locator: key, reason }),
            }
        }
        token = page.next_token;
        if token.is_none() {
            break;
        }
    }
    Ok(report)
}

/// Stream-decode and verify ONE object into a private staging file, then
/// hand it to the consumer. `Err` carries the PER-OBJECT reason. Bounded
/// memory: one 64 KiB decode buffer plus the staging file — no payload
/// bytes ever accumulate here. Nothing is written back to the store.
fn salvage_one(
    objects: &dyn ObjectStore,
    key: &str,
    sink: &mut dyn SalvageSink,
) -> std::result::Result<SalvagedItem, String> {
    let mut sg = objects.get_stream(key, None).map_err(|e| match e {
        Error::ObjectMissing(_) => "object listed but missing on read".to_string(),
        Error::Storage(m) => format!("unreadable: {m}"),
        other => format!("unreadable: {other}"),
    })?;
    let mut staged = TempFileGuard::create().map_err(|e| format!("staging file: {e}"))?;
    let item = {
        let mut file = staged.file.take().expect("fresh guard holds a file");
        use std::io::Write as _;
        let d = envelope::decode_stream(&mut sg.reader, &mut file, None);
        let flushed = match &d {
            Ok(_) => file.flush().err(),
            Err(_) => None,
        };
        staged.file = Some(file);
        match (d, flushed) {
            (Ok(d), None) => Ok(d),
            (Err(e), _) => Err(e.to_string()),
            (Ok(_), Some(io)) => Err(format!("staging write failed: {io}")),
        }
    }?;
    if item.kind != ObjKind::Data {
        // Legacy metadata copies (S4.19 Tree/Commit/Ref objects) or
        // anything unexpected: reported, never returned as file content.
        return Err(format!("not a data object (kind {})", item.kind as u8));
    }
    let mut file = staged.file.take().expect("guard still holds a file");
    use std::io::Seek as _;
    file.rewind().map_err(|e| format!("staging rewind: {e}"))?;
    if let Err(e) = sink.verified(key, item.payload_len, &mut file) {
        return Err(format!("consumer failed: {e}"));
    }
    Ok(SalvagedItem {
        locator: key.to_string(),
        payload_len: item.payload_len,
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
        m.put("objects/2/w-b.data", &[0x00u8; 60]).unwrap();
        // unknown format version
        let mut unknown = envelope::seal(ObjKind::Data, b"x");
        unknown[4] = 99;
        m.put("objects/3/w-c.data", &unknown).unwrap();
        // a legacy metadata copy where a payload would sit
        m.put("objects/9/legacy.data", &envelope::seal(ObjKind::Tree, b"[]"))
            .unwrap();

        // Test sink: captures (locator, bytes) per verified object. The
        // production shape streams instead of collecting.
        let mut collected: Vec<(String, Vec<u8>)> = Vec::new();
        let mut sink = |locator: &str, _len: u64, r: &mut dyn std::io::Read| {
            let mut buf = Vec::new();
            r.read_to_end(&mut buf)?;
            collected.push((locator.to_string(), buf));
            Ok(())
        };
        let before = m.list(DATA_PREFIX, None, 1000).unwrap().items.len();
        let report = salvage_all_data(&m, &mut sink).unwrap();
        let after = m.list(DATA_PREFIX, None, 1000).unwrap().items.len();
        assert_eq!(before, after, "salvage must not mutate the store");
        assert_eq!(report.scanned, 4);
        assert_eq!(report.recovered.len(), 1);
        // The locator is echoed verbatim (diagnostic) — never parsed into
        // a logical identity.
        assert_eq!(report.recovered[0].locator, "objects/1/w-a.data");
        assert_eq!(report.recovered[0].payload_len, 8);
        assert_eq!(collected, vec![("objects/1/w-a.data".into(), b"good-one".to_vec())]);
        assert_eq!(report.rejected.len(), 3, "{:?}", report.rejected);
        assert!(report.rejected.iter().any(|r| r.reason.contains("bad magic")));
        assert!(report
            .rejected
            .iter()
            .any(|r| r.reason.contains("unsupported format version")));
        assert!(report
            .rejected
            .iter()
            .any(|r| r.reason.contains("not a data object")));
    }
}
