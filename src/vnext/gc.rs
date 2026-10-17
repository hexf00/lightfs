//! vnext reachability gc. Deletion domain = objects ledgered under our
//! authority MINUS objects reachable from EVERY valid reference root
//! (all refs → their full commit chains → COW trees → file entries).
//! The ledger and the reachability walk are BACKEND-QUALIFIED
//! end-to-end (S4.22): a candidate is a (backend, locator) pair, and a
//! row whose backend is NOT the engine's bound store is never deleted
//! and never assumed to belong to the configured backend — this engine
//! has no handle to it, so deletion would be a guess. Fail-closed:
//! without a readable model marker and at least one ref, reachability
//! cannot be proven and NOTHING is deleted. Recovery objects
//! (`meta/…`, content-addressed) are never in the deletion domain —
//! they are the disaster-recovery assets, not garbage.

use std::collections::HashSet;

use crate::error::Result;
use crate::vnext::core::Core;

#[derive(Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub live_objects: usize,
    pub candidates: usize,
    pub deleted: u64,
    pub failed: u64,
    /// Ledger rows bound to a backend this engine cannot see: kept
    /// unconditionally (never deleted, never reinterpreted as belonging
    /// to the configured store).
    pub foreign_kept: u64,
    pub skipped_unreachable_proof: bool,
}

pub fn collect(core: &Core) -> Result<GcReport> {
    // Fail-closed gate first: no marker / no refs ⇒ no deletions, ever.
    if let Err(_e) = core.store().gc_gate() {
        return Ok(GcReport {
            skipped_unreachable_proof: true,
            ..GcReport::default()
        });
    }

    // Live set: walk EVERY commit reachable from EVERY ref. Under the
    // single-writer discipline every commit descends from a ref, so
    // walking all commits is equivalent — but derive it from the refs
    // (not from the table) so a future branch-delete can shrink history
    // without this walk silently over-retaining or over-deleting.
    let refs = core.store().all_refs()?;
    let commits = core.store().all_commits()?;
    let by_id: std::collections::HashMap<i64, _> =
        commits.iter().map(|c| (c.id, c)).collect();
    let mut live_commit_ids: HashSet<i64> = HashSet::new();
    for (_, head) in &refs {
        let mut cur = Some(head.commit_id);
        while let Some(id) = cur {
            if !live_commit_ids.insert(id) {
                break;
            }
            cur = by_id.get(&id).and_then(|c| c.parent_id);
        }
    }

    let mut live_objects: HashSet<(String, String)> = HashSet::new();
    let mut tree_stack: Vec<i64> = Vec::new();
    for id in &live_commit_ids {
        if let Some(c) = by_id.get(id) {
            tree_stack.push(c.root_tree_id);
        }
    }
    let mut seen_trees: HashSet<i64> = HashSet::new();
    while let Some(tid) = tree_stack.pop() {
        if !seen_trees.insert(tid) {
            continue;
        }
        for e in core.store().get_tree(tid)? {
            match (e.k.as_str(), e.tid) {
                ("dir", Some(child)) => tree_stack.push(child),
                _ => {
                    // Reachability is judged on the FULL storage
                    // reference: the same locator text on a different
                    // backend is a different physical object and must
                    // never stand in for it.
                    if let Some(r) = &e.o {
                        live_objects.insert((r.backend.clone(), r.locator.clone()));
                    }
                }
            }
        }
    }

    let bound_backend = core.objects().backend_id();
    let known = core.store().known_objects()?;
    let candidates: Vec<&(String, String)> = known
        .iter()
        .filter(|pair| !live_objects.contains(*pair))
        .collect();
    let report = GcReport {
        live_objects: live_objects.len(),
        candidates: candidates.len(),
        ..GcReport::default()
    };
    let mut report = report;
    for (backend, locator) in candidates {
        if *backend != bound_backend {
            // No deletion domain authority over another backend: kept and
            // reported, never reinterpreted as "probably ours".
            report.foreign_kept += 1;
            continue;
        }
        // Idempotent delete (missing key is fine); only forget the ledger
        // row AFTER the delete reported success.
        match core.objects().delete(locator) {
            Ok(()) => {
                core.store().forget_object(backend, locator)?;
                report.deleted += 1;
            }
            Err(_) => {
                report.failed += 1;
            }
        }
    }
    Ok(report)
}
