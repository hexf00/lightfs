//! The Node value type shared by the FsApi surface. The vnext authority
//! lives in `crate::vnext` (COW trees + commits + refs); this module only
//! carries the adapter-facing data shape.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Dir,
    File,
    Symlink,
}

impl NodeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            NodeKind::Dir => "dir",
            NodeKind::File => "file",
            NodeKind::Symlink => "symlink",
        }
    }
    /// Parse the stored string form ("dir"/"file"/"symlink"). Named to avoid
    /// confusion with std::str::FromStr.
    pub fn parse_kind(s: &str) -> Option<Self> {
        match s {
            "dir" => Some(NodeKind::Dir),
            "file" => Some(NodeKind::File),
            "symlink" => Some(NodeKind::Symlink),
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
    /// Symlink target, stored VERBATIM (relative or absolute); None for
    /// every other kind. Resolution semantics live in Core, not here.
    pub target: Option<String>,
    /// S3.1 pack prototype: when `pack_off` is set, the file's bytes live
    /// INSIDE the immutable pack object `object_key` at [pack_off,
    /// pack_off+pack_len). The pack's ETag is NOT this file's etag (which
    /// is empty for pack-backed rows — the per-file content digest rides
    /// in `content_hash` instead).
    pub pack_off: Option<i64>,
    pub pack_len: Option<i64>,
    pub content_hash: Option<String>,
}

impl Node {
    pub fn is_dir(&self) -> bool {
        self.kind == NodeKind::Dir
    }
    pub fn is_file(&self) -> bool {
        self.kind == NodeKind::File
    }
    pub fn is_symlink(&self) -> bool {
        self.kind == NodeKind::Symlink
    }
    /// True when this node's content lives inside a shared immutable pack
    /// object (S3.1 prototype). Such rows must never be retired/deleted as
    /// if `object_key` were their own — the pack is shared.
    pub fn is_pack_backed(&self) -> bool {
        self.pack_off.is_some()
    }
}
