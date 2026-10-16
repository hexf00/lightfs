//! fsck / reconciliation (Phase 12): detect divergence between the metadata
//! authority and the data plane. S3 has no distributed transaction with the
//! DB, so failures surface as four classes:
//!
//! - missing object:   node row exists, object gone (failed/incomplete delete
//!                     or external deletion)
//! - size mismatch:    object size differs from metadata (external overwrite)
//! - etag mismatch:    object fingerprint differs from metadata (stale version)
//! - orphan object:    object with no metadata node (crash between S3 PUT and
//!                     metadata commit, or incomplete delete of a replaced object)
//!
//! `--fix` adopts object state into metadata (size/etag) so clients converge;
//! it never deletes anything.

use crate::core::{Core, object_key_for};
use crate::error::Result;
use crate::metadata::Node;

#[derive(Debug, Default, Clone)]
pub struct FsckReport {
    pub checked_nodes: usize,
    pub missing_objects: Vec<String>,
    pub size_mismatches: Vec<String>,
    pub etag_mismatches: Vec<String>,
    pub orphan_objects: Vec<String>,
    /// Superseded content keys of live nodes (per-write version objects that
    /// lost a race, or leftovers from a crash between upload and cleanup).
    /// Never a correctness problem; deletable garbage.
    pub stale_versions: Vec<String>,
    /// Objects recorded in superseded_objects (retention): intentional
    /// history kept so a metadata backup can restore point-in-time
    /// content until `lightfs gc` purges them past the window.
    pub retained_objects: Vec<String>,
    /// S3.1: pack objects uploaded but referenced by NO live node anymore
    /// (gc-managed; never a correctness problem).
    pub unreferenced_packs: Vec<String>,
    /// S3.1: distinct pack objects still referenced by live nodes.
    pub live_packs: usize,
    /// S3.1: registered packs never uploaded (pending rows = dead-server
    /// mid-upload leftovers; visible, gc ignores them). S4.10: rows are
    /// enumerated directly, so a row WITHOUT its object is reported too —
    /// a crash between `register_pack` and the PUT, or a stale row whose
    /// object the sweep already reclaimed. Startup recovery and the gc
    /// stale-pack sweep resolve these; fsck only reports.
    pub pending_packs: Vec<String>,
}

impl FsckReport {
    pub fn problems(&self) -> usize {
        self.missing_objects.len()
            + self.size_mismatches.len()
            + self.etag_mismatches.len()
            + self.orphan_objects.len()
    }

    pub fn render(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("checked nodes: {}\n", self.checked_nodes));
        for k in &self.missing_objects {
            out.push_str(&format!("MISSING OBJECT: {k}\n"));
        }
        for k in &self.size_mismatches {
            out.push_str(&format!("SIZE MISMATCH: {k}\n"));
        }
        for k in &self.etag_mismatches {
            out.push_str(&format!("ETAG MISMATCH: {k}\n"));
        }
        for k in &self.orphan_objects {
            out.push_str(&format!("ORPHAN OBJECT: {k}\n"));
        }
        for k in &self.stale_versions {
            out.push_str(&format!("STALE VERSION: {k}\n"));
        }
        for k in &self.retained_objects {
            out.push_str(&format!("RETAINED (restorable): {k}\n"));
        }
        for k in &self.pending_packs {
            out.push_str(&format!("PENDING PACK: {k}\n"));
        }
        out.push_str(&format!(
            "packs: {live} live, {unref} unreferenced (gc), {pend} pending\n",
            live = self.live_packs,
            unref = self.unreferenced_packs.len(),
            pend = self.pending_packs.len()
        ));
        if self.problems() == 0 {
            out.push_str(&format!(
                "clean: metadata and objects agree ({} stale, {} retained)\n",
                self.stale_versions.len(),
                self.retained_objects.len()
            ));
        } else {
            out.push_str(&format!("{} problem(s) found\n", self.problems()));
        }
        out
    }
}

/// Scan everything. Network-bound: one HEAD per live file, one LIST page walk.
pub fn fsck(core: &Core) -> Result<FsckReport> {
    let mut report = FsckReport::default();
    let mut live_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    // Pack-backed nodes share objects: HEAD each DISTINCT pack once and
    // check the file's window is inside it (a pack's ETag is never a
    // file's, so the per-file etag comparison does not apply).
    let mut pack_windows: std::collections::HashMap<String, (i64, i64, i64)> =
        std::collections::HashMap::new(); // key -> (min_off, max_end, node_count)

    for node in core.metadata().all_nodes()? {
        report.checked_nodes += 1;
        if !node.is_file() {
            continue;
        }
        if node.size == 0 {
            // Contract: an empty file has no content object (create is
            // metadata-only; the first write commit uploads content).
            continue;
        }
        let key = node
            .object_key
            .clone()
            .unwrap_or_else(|| object_key_for(node.id));
        live_keys.insert(key.clone());
        if node.is_pack_backed() {
            let off = node.pack_off.unwrap_or(0);
            let end = off + node.pack_len.unwrap_or(node.size);
            let e = pack_windows.entry(key).or_insert((off, end, 0));
            e.0 = e.0.min(off);
            e.1 = e.1.max(end);
            e.2 += 1;
            continue;
        }
        match core.objects().head(&key) {
            Ok(info) => {
                if info.size != node.size as u64 {
                    report.size_mismatches.push(format!(
                        "{key} metadata_size={} object_size={}",
                        node.size, info.size
                    ));
                }
                if !info.etag.is_empty() && info.etag != node.etag {
                    report.etag_mismatches.push(format!(
                        "{key} metadata_version={} metadata_etag={} object_etag={}",
                        node.version, node.etag, info.etag
                    ));
                }
            }
            Err(crate::error::Error::ObjectMissing(_)) => {
                report
                    .missing_objects
                    .push(format!("{key} (node id {})", node.id));
            }
            Err(e) => return Err(e),
        }
    }

    // One HEAD per distinct live pack; window bounds must fit.
    for (key, (min_off, max_end, count)) in &pack_windows {
        report.live_packs += 1;
        match core.objects().head(key) {
            Ok(info) => {
                if *max_end > info.size as i64 || *min_off < 0 {
                    report.size_mismatches.push(format!(
                        "{key} pack_size={} window=[{min_off},{max_end}) nodes={count}",
                        info.size
                    ));
                }
            }
            Err(crate::error::Error::ObjectMissing(_)) => {
                report
                    .missing_objects
                    .push(format!("{key} (shared pack, {count} nodes)"));
            }
            Err(e) => return Err(e),
        }
    }

    // Classify every stored key:
    //   live node's current key  -> checked above
    //   recorded superseded key  -> retained history (restorable, gc-managed)
    //   key under a LIVE node id -> stale version (superseded content)
    //   key under a dead node id -> orphan object
    let superseded: std::collections::HashSet<String> =
        core.metadata().all_superseded()?.into_iter().collect();
    // Pack classification: referenced -> live (counted above); unreferenced
    // uploaded -> gc-managed; pending rows -> visible leftovers; unknown
    // bucket packs (no row) -> orphan (rows are written before uploads, so
    // a bucket pack without a row is external tampering or a deleted row).
    let pack_rows: std::collections::HashMap<String, Option<i64>> = core
        .metadata()
        .all_packs()?
        .into_iter()
        .map(|(k, _, up)| (k, up))
        .collect();
    let mut token = None;
    let mut bucket_packs: std::collections::HashSet<String> = std::collections::HashSet::new();
    loop {
        let page = core.objects().list("packs/", token.as_deref(), 1000)?;
        for item in &page.items {
            bucket_packs.insert(item.key.clone());
            if live_keys.contains(&item.key) {
                continue;
            }
            match pack_rows.get(&item.key) {
                Some(Some(_)) => report
                    .unreferenced_packs
                    .push(format!("{} ({}B)", item.key, item.size)),
                Some(None) => report.pending_packs.push(format!(
                    "{} ({}B, pending row, object present — sweep adjudicates)",
                    item.key, item.size
                )),
                None => report
                    .orphan_objects
                    .push(format!("{} ({}B, pack without row)", item.key, item.size)),
            }
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    // S4.10: rows the bucket listing cannot show — a row without its object
    // (crash between register_pack and the PUT; a stale row the sweep has
    // not dropped yet). The old bucket-driven walk was blind to these.
    for (key, _bytes, uploaded_at) in core.metadata().all_packs()? {
        if bucket_packs.contains(&key) || live_keys.contains(&key) {
            continue;
        }
        match uploaded_at {
            None => report
                .pending_packs
                .push(format!("{key} (registered, object absent)")),
            Some(_) => report
                .pending_packs
                .push(format!("{key} (row only, object absent)")),
        }
    }
    let mut token = None;
    loop {
        let page = core.objects().list("objects/", token.as_deref(), 1000)?;
        for item in &page.items {
            if live_keys.contains(&item.key) {
                continue;
            }
            if superseded.contains(&item.key) {
                report
                    .retained_objects
                    .push(format!("{} ({}B)", item.key, item.size));
                continue;
            }
            match file_id_from_key(&item.key) {
                Some(id) => {
                    if core.metadata().get_node(id)?.is_some() {
                        report
                            .stale_versions
                            .push(format!("{} ({}B)", item.key, item.size));
                    } else {
                        report
                            .orphan_objects
                            .push(format!("{} ({}B)", item.key, item.size));
                    }
                }
                None => {
                    report
                        .orphan_objects
                        .push(format!("{} (non-conforming key)", item.key));
                }
            }
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    Ok(report)
}

/// Adopt object state into metadata (--fix): size/etag converge to what the
/// data plane actually holds; the CAS bump means a concurrent writer that
/// already committed wins and the adoption is skipped (next scan re-checks).
pub fn fix(core: &Core, report: &FsckReport) -> Result<usize> {
    let mut adopted = 0;
    // A node can appear in both size and etag classes; dedupe by object key.
    let mut keys: Vec<String> = report
        .size_mismatches
        .iter()
        .chain(report.etag_mismatches.iter())
        .map(|entry| entry.split(' ').next().unwrap_or_default().to_string())
        .collect();
    keys.sort();
    keys.dedup();
    for key in &keys {
        let key = key.as_str();
        let Some(id) = file_id_from_key(key) else {
            continue;
        };
        let Some(node) = core.metadata().get_node(id)? else {
            continue;
        };
        // Only adopt what is actually the node's current object.
        if node.object_key.as_deref() != Some(key) {
            continue;
        }
        if let Ok(info) = core.objects().head(key) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            core.metadata().commit_content_cas(
                node.id,
                node.version,
                info.size as i64,
                now,
                key,
                &info.etag,
            )?;
            adopted += 1;
        }
    }
    Ok(adopted)
}

fn file_id_from_key(key: &str) -> Option<i64> {
    let rest = key.strip_prefix("objects/")?;
    let id_str = rest.split('/').next()?;
    id_str.parse().ok()
}

/// Nodes carry object keys assigned at create; keep the helper local for fsck.
#[allow(dead_code)]
fn _object_key(n: &Node) -> String {
    object_key_for(n.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::object_store::mem::MemObjectStore;
    use std::sync::Arc;

    fn core() -> Core {
        Core::new(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(MemObjectStore::new()),
        )
    }

    fn write_file(c: &Core, path: &str, content: &[u8]) {
        c.create(path, 0o644).unwrap();
        let h = c.open_write(path, None).unwrap();
        c.write(&h, 0, content).unwrap();
        c.commit_write(&h).unwrap();
    }

    #[test]
    fn clean_filesystem_reports_clean() {
        let c = core();
        write_file(&c, "/f", b"normal");
        let r = fsck(&c).unwrap();
        assert_eq!(r.problems(), 0, "{}", r.render());
        assert_eq!(r.checked_nodes, 2); // root + file
    }

    #[test]
    fn detects_external_overwrite_and_missing_and_orphan() {
        let c = core();
        write_file(&c, "/kept", b"original content");
        write_file(&c, "/gone", b"will be externally deleted");
        write_file(&c, "/changed", b"will be externally replaced");

        // external overwrite (bypasses Core entirely)
        let changed_node = c.lookup("/changed").unwrap();
        let changed_key = changed_node.object_key.clone().unwrap();
        c.objects()
            .put(&changed_key, b"entirely different and longer content")
            .unwrap();
        // external deletion
        let gone_key = c.lookup("/gone").unwrap().object_key.clone().unwrap();
        c.objects().delete(&gone_key).unwrap();
        // orphan: object with no metadata
        c.objects().put("objects/424242/data", b"who am i").unwrap();

        // a leftover version object of a LIVE node (crash between upload and
        // cleanup) is a stale version, not an orphan and not a problem
        let kept_id = c.lookup("/kept").unwrap().id;
        c.objects()
            .put(
                &format!("objects/{kept_id}/w-crashed.data"),
                b"half written",
            )
            .unwrap();

        let r = fsck(&c).unwrap();
        assert_eq!(r.missing_objects.len(), 1, "{}", r.render());
        assert_eq!(r.size_mismatches.len(), 1, "{}", r.render());
        assert_eq!(r.etag_mismatches.len(), 1, "{}", r.render());
        assert_eq!(r.orphan_objects.len(), 1, "{}", r.render());
        assert_eq!(r.stale_versions.len(), 1, "{}", r.render());
        assert_eq!(
            r.problems(),
            4,
            "stale versions are not problems: {}",
            r.render()
        );

        // --fix adopts the external modification
        let adopted = fix(&c, &r).unwrap();
        assert_eq!(adopted, 1);
        let node = c.lookup("/changed").unwrap();
        assert_eq!(
            node.size,
            b"entirely different and longer content".len() as i64
        );
        assert_eq!(node.version, changed_node.version + 1);
        // still fine after re-scan except the classes fix cannot solve
        let r2 = fsck(&c).unwrap();
        assert_eq!(r2.size_mismatches.len(), 0);
        assert_eq!(r2.etag_mismatches.len(), 0);
        assert_eq!(r2.missing_objects.len(), 1);
        assert_eq!(r2.orphan_objects.len(), 1);
        assert_eq!(r2.stale_versions.len(), 1);
    }

    /// S4.10: fsck enumerates pack ROWS, not just bucket objects — a row
    /// without its object (crash between register_pack and the PUT) was
    /// invisible before. The pending labels distinguish what the sweep
    /// will find: object absent (row drop) vs object present (HEAD + intent).
    #[test]
    fn s410_fsck_reports_pack_rows_and_pending_labels() {
        let c = core();
        c.metadata().register_pack("packs/absent.data", 5).unwrap();
        c.metadata().register_pack("packs/pending.data", 5).unwrap();
        c.objects().put("packs/pending.data", b"xyz").unwrap();
        // rowless bucket pack stays the fsck-visible orphan it always was
        c.objects().put("packs/rowless.data", b"zzz").unwrap();

        let r = fsck(&c).unwrap();
        assert!(
            r.pending_packs
                .iter()
                .any(|s| s.starts_with("packs/absent.data") && s.contains("object absent")),
            "{:?}",
            r.pending_packs
        );
        assert!(
            r.pending_packs
                .iter()
                .any(|s| s.starts_with("packs/pending.data") && s.contains("object present")),
            "{:?}",
            r.pending_packs
        );
        assert!(
            r.orphan_objects
                .iter()
                .any(|s| s.starts_with("packs/rowless.data")),
            "{:?}",
            r.orphan_objects
        );
    }
}
