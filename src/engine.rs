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

/// Build the Core from config, wiring the S3.1 prototype engines (pack /
/// write-back) when enabled. Shared by embedded and server startup.
fn build_core(
    cfg: &Config,
    meta: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
) -> Result<Core> {
    let pack = if cfg.pack.enabled {
        Some(cfg.pack.clone())
    } else {
        None
    };
    let wb = if cfg.commit.mode == "writeback" {
        Some(crate::wb::WbOptions {
            staging_dir: cfg.writeback.staging_dir.clone(),
            drain_workers: cfg.writeback.drain_workers,
            batch_pack_bytes: cfg.writeback.batch_pack_bytes,
            batch_max_wait_ms: cfg.writeback.batch_max_wait_ms,
            queue_max_tasks: cfg.writeback.queue_max_tasks,
        })
    } else {
        None
    };
    let core = Core::with_options(meta, objects, cfg.retention.superseded_secs, pack, wb)?;
    if core.wb().is_some() {
        core.wb().unwrap().recover()?;
        core.start_wb_workers();
    }
    Ok(core)
}

/// Embedded mode: Core + SQLite + S3 in one process. No daemon required.
pub fn open_embedded(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(cfg.s3_config())?);
    objects.ping()?;
    Ok(Engine {
        core: Arc::new(build_core(cfg, meta, objects)?),
    })
}

/// Embedded engine on the in-memory object store: network-free loop for
/// tests and throwaway exploration. The SQLite file still persists.
pub fn open_embedded_mem(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(crate::object_store::mem::MemObjectStore::new());
    Ok(Engine {
        core: Arc::new(build_core(cfg, meta, objects)?),
    })
}
