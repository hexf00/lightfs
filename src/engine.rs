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

/// Resolve the effective write-back drain worker count. The config value
/// is the base; `LIGHTFS_WB_WORKERS` is an explicit override. This is the
/// SINGLE wiring path (S3.5.1): the old code read the env inside
/// `Core::start_wb_workers` and ignored the config field entirely, so a
/// config could claim 8 while the process silently ran 1.
///
/// Multi-worker draining is DISABLED for production: the safety contract
/// covered only same-expected CAS races, not per-node FIFO ordering across
/// separate queued tasks (a later version could commit before an earlier
/// one). Any effective count >1 - from config OR env - is a startup error,
/// never a clamp; 0 is refused too (nothing would ever drain); a
/// non-numeric env value is refused instead of silently meaning 1.
fn resolve_wb_workers(configured: usize, env: Option<&str>) -> Result<usize> {
    let workers = match env {
        Some(raw) => raw.trim().parse::<usize>().map_err(|_| {
            crate::error::Error::InvalidArgument(format!(
                "LIGHTFS_WB_WORKERS={raw:?} is not a valid drain worker count"
            ))
        })?,
        None => configured,
    };
    if workers == 0 {
        return Err(crate::error::Error::InvalidArgument(
            "write-back drain workers = 0 refused: nothing would ever drain the queue".into(),
        ));
    }
    if workers > 1 {
        return Err(crate::error::Error::InvalidArgument(format!(
            "write-back drain_workers = {workers} refused: multi-worker draining is disabled \
             (no per-node FIFO ordering across queued tasks); configure 1"
        )));
    }
    Ok(workers)
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
            staging_budget_bytes: cfg.writeback.staging_budget_bytes,
        })
    } else {
        None
    };
    let core = Core::with_options(meta, objects, cfg.retention.superseded_secs, pack, wb)?;
    if core.wb().is_some() {
        core.wb().unwrap().recover()?;
        let workers = resolve_wb_workers(
            cfg.writeback.drain_workers,
            std::env::var("LIGHTFS_WB_WORKERS").ok().as_deref(),
        )?;
        core.start_wb_workers(workers);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_resolution_accepts_single_worker() {
        assert_eq!(resolve_wb_workers(1, None).unwrap(), 1);
        assert_eq!(resolve_wb_workers(1, Some("1")).unwrap(), 1);
    }

    #[test]
    fn worker_resolution_refuses_multiple_from_config_or_env() {
        let err = resolve_wb_workers(2, None).unwrap_err();
        assert!(format!("{err}").contains("disabled"), "got: {err}");
        let err = resolve_wb_workers(1, Some("4")).unwrap_err();
        assert!(format!("{err}").contains("disabled"), "got: {err}");
    }

    #[test]
    fn worker_resolution_refuses_zero_and_garbage_env() {
        assert!(resolve_wb_workers(0, None).is_err());
        assert!(resolve_wb_workers(1, Some("0")).is_err());
        let err = resolve_wb_workers(1, Some("abc")).unwrap_err();
        assert!(
            format!("{err}").contains("LIGHTFS_WB_WORKERS"),
            "got: {err}"
        );
    }

    #[test]
    fn engine_startup_refuses_multi_worker_config() {
        let mut cfg = Config::default();
        cfg.commit.mode = "writeback".into();
        cfg.writeback.drain_workers = 2;
        cfg.metadata.path = std::env::temp_dir()
            .join(format!(
                "lightfs-eng-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .to_string();
        let msg = match open_embedded_mem(&cfg) {
            Err(e) => format!("{e}"),
            Ok(_) => panic!("startup must refuse drain_workers=2"),
        };
        assert!(
            msg.contains("disabled") && msg.contains("2"),
            "startup must name the refusal explicitly, got: {msg}"
        );
    }
}
