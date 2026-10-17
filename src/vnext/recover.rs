//! vnext disaster recovery (first slice): rebuild a namespace VIEW from
//! the object store alone — no SQLite involved.
//!
//! Recovery layers (design contract A):
//! 1. data-block salvage: every `objects/<fid>/…` object is
//!    envelope-sealed (type, logical length, sha256) — content is
//!    verifiable and readable without any metadata.
//! 2. file reassembly: a data object IS a whole file (v1 has no chunking;
//!    a future chunked encoding would live inside this layer).
//! 3. namespace recovery: `meta/refs/<name>` projections point at
//!    `meta/commits/<sha>`; each commit carries its parent hash and root
//!    `meta/trees/<sha>`; tree objects carry self-contained file entries
//!    (identity, version, object key). Folding yields paths+sizes.
//! 4. latest trusted commit: per ref, the projection with the HIGHEST
//!    seq whose whole chain verifies. A commit object that exists but is
//!    not reachable from any projection is an UNPUBLISHED transaction —
//!    reported, never adopted as trusted state.
//!
//! This module READS only. Rebuilding a live SQLite store from the view
//! is a future slice (the view is designed to carry everything needed).

use std::collections::{HashMap, HashSet};

use crate::error::{Error, Result};
use crate::object_store::ObjectStore;
use crate::vnext::core::{commit_key, tree_key};
use crate::vnext::envelope::{self, ObjKind};
use crate::vnext::store::Entry;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RefView {
    pub name: String,
    pub commit_sha: String,
    pub seq: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FileView {
    pub path: String,
    pub size: i64,
    pub object_key: String,
    pub etag: String,
    pub file_id: i64,
    pub version: i64,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct NamespaceView {
    pub refs: Vec<RefView>,
    pub files: Vec<FileView>,
    /// Objects of type commit/tree present but unreachable from any ref
    /// projection: unpublished transactions or torn writes. Reported,
    /// never adopted.
    pub unpublished_commits: Vec<String>,
}

fn read_object(objects: &dyn ObjectStore, key: &str, want: ObjKind) -> Result<Vec<u8>> {
    let obj = objects.get(key, None).map_err(|e| match e {
        Error::ObjectMissing(_) => e,
        other => other,
    })?;
    let (kind, content) = envelope::open(&obj.data)?;
    if kind != want {
        return Err(Error::ObjectCorrupt(format!(
            "{key}: expected object type {}, found {}",
            want as u8, kind as u8
        )));
    }
    Ok(content)
}

/// List every ref projection we can find and verify.
pub fn ref_views(objects: &dyn ObjectStore) -> Result<Vec<RefView>> {
    // Ref projections are a small, fixed-shaped prefix; list it.
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = objects.list("meta/refs/", token.as_deref(), 100)?;
        for info in &page.items {
            let name = info
                .key
                .strip_prefix("meta/refs/")
                .unwrap_or(&info.key)
                .to_string();
            let bytes = read_object(objects, &info.key, ObjKind::Ref)?;
            let v: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| Error::ObjectCorrupt(format!("{}: {e}", info.key)))?;
            out.push(RefView {
                name,
                commit_sha: v
                    .get("commit")
                    .and_then(|x| x.as_str())
                    .unwrap_or_default()
                    .to_string(),
                seq: v.get("seq").and_then(|x| x.as_i64()).unwrap_or(-1),
            });
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    Ok(out)
}

fn load_commit(
    objects: &dyn ObjectStore,
    sha: &str,
    cache: &mut HashMap<String, (Option<String>, String)>,
) -> Result<(Option<String>, String)> {
    if let Some(hit) = cache.get(sha) {
        return Ok(hit.clone());
    }
    let bytes = read_object(objects, &commit_key(sha), ObjKind::Commit)?;
    let v: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|e| Error::ObjectCorrupt(format!("commit {sha}: {e}")))?;
    let parent = v
        .get("parent")
        .and_then(|x| x.as_str())
        .map(String::from);
    let root_tree = v
        .get("root_tree")
        .and_then(|x| x.as_str())
        .unwrap_or_default()
        .to_string();
    cache.insert(sha.to_string(), (parent.clone(), root_tree.clone()));
    Ok((parent, root_tree))
}

fn load_tree(objects: &dyn ObjectStore, sha: &str) -> Result<Vec<Entry>> {
    let bytes = read_object(objects, &tree_key(sha), ObjKind::Tree)?;
    serde_json::from_slice(&bytes).map_err(|e| Error::ObjectCorrupt(format!("tree {sha}: {e}")))
}

fn fold_tree(
    objects: &dyn ObjectStore,
    tree_sha: &str,
    prefix: &str,
    files: &mut Vec<FileView>,
) -> Result<()> {
    for e in load_tree(objects, tree_sha)? {
        let path = format!("{prefix}/{}", e.n);
        match e.k.as_str() {
            "dir" => {
                // Child subtrees are referenced by CONTENT hash (the `w`
                // field), so recovery never needs SQLite row ids.
                if let Some(child_sha) = &e.w {
                    fold_tree(objects, child_sha, &path, files)?;
                }
            }
            "file" => {
                if let (Some(fid), Some(fv)) = (e.fid, e.fv) {
                    files.push(FileView {
                        path,
                        size: e.s,
                        object_key: e.o.clone().unwrap_or_default(),
                        etag: e.e.clone().unwrap_or_default(),
                        file_id: fid,
                        version: fv,
                    });
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Build the full namespace view from the object store alone. Also
/// reports commit objects that no ref projection reaches (unpublished).
pub fn namespace_view(objects: &dyn ObjectStore) -> Result<NamespaceView> {
    let refs = ref_views(objects)?;
    let mut view = NamespaceView::default();
    let mut commit_cache = HashMap::new();
    let mut reachable_commits: HashSet<String> = HashSet::new();
    let mut reachable_trees: HashSet<String> = HashSet::new();

    for r in &refs {
        view.refs.push(r.clone());
        // Walk the parent chain, collecting every tree on the way.
        let mut cur = Some(r.commit_sha.clone());
        while let Some(sha) = cur {
            if !reachable_commits.insert(sha.clone()) {
                break;
            }
            let (parent, root_tree) = load_commit(objects, &sha, &mut commit_cache)?;
            reachable_trees.insert(root_tree.clone());
            cur = parent;
        }
    }

    let mut files = Vec::new();
    for tsha in &reachable_trees {
        fold_tree(objects, tsha, "", &mut files)?;
    }
    files.sort_by(|a, b| a.path.cmp(&b.path));
    view.files = files;

    // Unpublished commits: present in the store, unreachable from any
    // ref projection. Enumerate meta/commits/ listing.
    let mut token: Option<String> = None;
    loop {
        let page = objects.list("meta/commits/", token.as_deref(), 100)?;
        for info in &page.items {
            if let Some(sha) = info.key.strip_prefix("meta/commits/") {
                if !reachable_commits.contains(sha) {
                    view.unpublished_commits.push(sha.to_string());
                }
            }
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    Ok(view)
}

/// Content salvage WITHOUT any metadata: read every data object under
/// objects/, verify its envelope, return (key, logical bytes).
pub fn salvage_all_data(objects: &dyn ObjectStore) -> Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let page = objects.list("objects/", token.as_deref(), 100)?;
        for info in &page.items {
            let obj = objects.get(&info.key, None)?;
            let (kind, content) = envelope::open(&obj.data)?;
            if kind == ObjKind::Data {
                out.push((info.key.clone(), content));
            }
        }
        match page.next_token {
            Some(t) => token = Some(t),
            None => break,
        }
    }
    Ok(out)
}
