//! Unified error type for LightFS. All layers map into this.

use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Path or node does not exist.
    NotFound(String),
    /// Parent does not exist or is not a directory.
    ParentNotFound(String),
    /// A node with the same name already exists at the destination.
    AlreadyExists(String),
    /// Optimistic concurrency conflict: content version changed underneath us.
    Conflict {
        path: String,
        expected: i64,
        actual: i64,
    },
    /// Directory operation on a file, or file operation on a directory.
    KindMismatch {
        path: String,
        expected: &'static str,
        actual: String,
    },
    /// rmdir on a non-empty directory.
    DirectoryNotEmpty(String),
    /// rename/move would create a cycle (dir into its own descendant).
    Cycle(String),
    /// Malformed path (empty component, trailing junk, not absolute).
    InvalidPath(String),
    /// Invalid argument (bad mode, negative offset, ...).
    InvalidArgument(String),
    /// Object storage backend failure.
    Storage(String),
    /// Object missing in the object store (fsck/detach between layers).
    ObjectMissing(String),
    /// A stable node identity (node id) an open handle was bound to no
    /// longer exists: the file was deleted by another client while the
    /// handle stayed open. Distinct from NotFound (path-based) so a caller
    /// can tell "the path is gone" from "THIS identity is gone" - the
    /// identity-aware commit must never mask this behind a generic error.
    IdentityGone(String),
    /// Metadata store failure.
    Db(String),
    /// I/O failure.
    Io(String),
    /// rename/move across two different policy backends (e.g. an s3 rule
    /// and a client-local rule). Refused outright (Phase P): no implicit
    /// data migration between authorities.
    Exdev(String),
    /// Configuration problem.
    Config(String),
    /// Write-back staging capacity exhausted. Deliberately its own variant:
    /// callers (and operators) must be able to tell "no staging budget
    /// left" apart from every other failure — capacity exhaustion is NEVER
    /// silently degraded, retried, or worked around by evicting staged
    /// data (S4 P0-c invariant 7).
    Capacity(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::NotFound(p) => write!(f, "not found: {p}"),
            Error::ParentNotFound(p) => write!(f, "parent not found: {p}"),
            Error::AlreadyExists(p) => write!(f, "already exists: {p}"),
            Error::Conflict {
                path,
                expected,
                actual,
            } => {
                write!(
                    f,
                    "conflict on {path}: expected version {expected}, actual {actual}"
                )
            }
            Error::KindMismatch {
                path,
                expected,
                actual,
            } => {
                write!(f, "{path} is a {actual}, expected {expected}")
            }
            Error::DirectoryNotEmpty(p) => write!(f, "directory not empty: {p}"),
            Error::Cycle(p) => write!(f, "rename would create a cycle: {p}"),
            Error::InvalidPath(p) => write!(f, "invalid path: {p}"),
            Error::InvalidArgument(m) => write!(f, "invalid argument: {m}"),
            Error::Storage(m) => write!(f, "object storage: {m}"),
            Error::ObjectMissing(k) => write!(f, "object missing: {k}"),
            Error::IdentityGone(k) => write!(f, "identity gone: node {k} no longer exists"),
            Error::Db(m) => write!(f, "metadata: {m}"),
            Error::Io(m) => write!(f, "io: {m}"),
            Error::Exdev(m) => write!(f, "cross-device link: {m}"),
            Error::Config(m) => write!(f, "config: {m}"),
            Error::Capacity(m) => write!(f, "capacity-exhausted: {m}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        match &e {
            rusqlite::Error::QueryReturnedNoRows => Error::NotFound("row".into()),
            _ => Error::Db(e.to_string()),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

impl Error {
    /// Stable machine-readable code used by the RPC protocol.
    pub fn code(&self) -> &'static str {
        match self {
            Error::NotFound(_) | Error::ParentNotFound(_) | Error::ObjectMissing(_) => "NOT_FOUND",
            Error::IdentityGone(_) => "IDENTITY_GONE",
            Error::AlreadyExists(_) => "ALREADY_EXISTS",
            Error::Conflict { .. } => "CONFLICT",
            Error::KindMismatch { .. } => "KIND_MISMATCH",
            Error::DirectoryNotEmpty(_) => "DIRECTORY_NOT_EMPTY",
            Error::Cycle(_) => "CYCLE",
            Error::InvalidPath(_) | Error::InvalidArgument(_) => "INVALID",
            Error::Storage(_) => "STORAGE",
            Error::Db(_) => "DB",
            Error::Io(_) => "IO",
            Error::Exdev(_) => "EXDEV",
            Error::Config(_) => "CONFIG",
            Error::Capacity(_) => "CAPACITY",
        }
    }
}
