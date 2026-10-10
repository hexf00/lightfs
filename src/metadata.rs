//! MetadataStore abstraction (control plane) and the SQLite backend.
//! One process owns one metadata store; all namespace mutations happen here.

pub mod sqlite;

use crate::error::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Dir,
    File,
}

impl NodeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeKind::Dir => "dir",
            NodeKind::File => "file",
        }
    }
    /// Parse the stored string form ("dir"/"file"). Named to avoid confusion
    /// with std::str::FromStr.
    pub fn parse_kind(s: &str) -> Option<Self> {
        match s {
            "dir" => Some(NodeKind::Dir),
            "file" => Some(NodeKind::File),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub name: String,
    pub kind: NodeKind,
    pub size: i64,
    /// Unix seconds.
    pub mtime: i64,
    /// Permission bits (u32 as stored in sqlite INTEGER).
    pub mode: u32,
    /// Data plane key (`objects/<id>/data` for files, None for dirs).
    pub object_key: Option<String>,
    /// Content version, increments on every committed write (stored as TEXT).
    pub version: i64,
    /// ETag of the last committed object.
    pub etag: String,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        self.kind == NodeKind::Dir
    }
    pub fn is_file(&self) -> bool {
        self.kind == NodeKind::File
    }
}

pub trait MetadataStore: Send + Sync {
    /// Create a node; fails with AlreadyExists on duplicate (parent_id, name).
    fn create_node(
        &self,
        parent_id: i64,
        name: &str,
        kind: NodeKind,
        mode: u32,
        object_key: Option<&str>,
    ) -> Result<Node>;
    fn get_node(&self, id: i64) -> Result<Option<Node>>;
    fn get_child(&self, parent_id: i64, name: &str) -> Result<Option<Node>>;
    fn list_children(&self, parent_id: i64) -> Result<Vec<Node>>;
    fn count_children(&self, parent_id: i64) -> Result<i64>;
    /// Commit new content state for a file with optimistic concurrency:
    /// succeeds only if the node's current version is still `expected_version`
    /// (compare-and-swap inside one statement). On mismatch returns
    /// Error::Conflict and changes nothing — a losing writer can never
    /// overwrite the winner's metadata or bytes.
    fn commit_content_cas(
        &self,
        id: i64,
        expected_version: i64,
        size: i64,
        mtime: i64,
        new_object_key: &str,
        etag: &str,
    ) -> Result<()>;
    /// Update permissions/mtime only.
    fn update_attrs(&self, id: i64, mode: u32, mtime: i64) -> Result<()>;
    /// Move a node within the namespace. Transactionally re-checks that the
    /// destination slot is free.
    fn rename_node(&self, id: i64, new_parent_id: i64, new_name: &str) -> Result<()>;
    /// Remove a node. Caller guarantees no children (rmdir check lives in Core).
    fn delete_node(&self, id: i64) -> Result<()>;
    /// Full table scan for fsck/reconciliation.
    fn all_nodes(&self) -> Result<Vec<Node>>;

    /// Record a COMMITTED object as superseded: it is no longer the live
    /// pointer, but its bytes must stay in the object store until retention
    /// expires, so a metadata snapshot (lightfs backup) can restore
    /// point-in-time content. Idempotent.
    fn record_superseded(&self, object_key: &str, at: i64) -> Result<()>;
    /// Superseded rows recorded at or before `cutoff` whose object_key is
    /// NOT referenced by any live node (crash windows can leave a row while
    /// the node still points at the key — those must never be purged).
    fn gc_candidates(&self, cutoff: i64, limit: usize) -> Result<Vec<String>>;
    /// Remove one superseded row (after its object was purged).
    fn drop_superseded(&self, object_key: &str) -> Result<()>;
    /// All superseded keys (fsck classification).
    fn all_superseded(&self) -> Result<Vec<String>>;
}
