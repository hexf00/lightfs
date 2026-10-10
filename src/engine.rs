//! Engine composition: Filesystem Core + MetadataStore + ObjectStore.
//! Embedded mode and server mode both start from here (one
//! server process owns one SQLite metadata store; PostgreSQL postponed).

use std::sync::Arc;

use crate::config::Config;
use crate::core::Core;
use crate::error::Result;
use crate::metadata::MetadataStore;
use crate::metadata::sqlite::SqliteMetadataStore;
use crate::object_store::ObjectStore;
use crate::object_store::s3::S3ObjectStore;

pub struct Engine {
    pub core: Arc<Core>,
}

/// Ensure the metadata directory exists before opening SQLite.
fn ensure_parent_dir(path: &str) {
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
}

/// Embedded mode: Core + SQLite + S3 in one process. No daemon required.
pub fn open_embedded(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(cfg.s3_config())?);
    objects.ping()?;
    Ok(Engine {
        core: Arc::new(Core::with_retention(
            meta,
            objects,
            cfg.retention.superseded_secs,
        )),
    })
}

/// Embedded engine on the in-memory object store: network-free loop for
/// tests and throwaway exploration. The SQLite file still persists.
pub fn open_embedded_mem(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(crate::object_store::mem::MemObjectStore::new());
    Ok(Engine {
        core: Arc::new(Core::with_retention(
            meta,
            objects,
            cfg.retention.superseded_secs,
        )),
    })
}
