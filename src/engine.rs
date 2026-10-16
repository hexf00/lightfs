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

/// The single-writer engine lock path for a config — the same derivation
/// the write-back engine uses (`<canonical db path>.engine-lock`), shared so
/// strict-mode engines (S4.10) and write-back engines contend on ONE lock.
pub(crate) fn engine_lock_path(cfg: &Config) -> Result<std::path::PathBuf> {
    let db_identity = crate::fslock::canonical_identity(
        std::path::Path::new(&cfg.metadata.path),
        "metadata database",
    )?;
    Ok(std::path::PathBuf::from(format!(
        "{}.engine-lock",
        db_identity.display()
    )))
}

/// Build the Core from config, wiring the S3.1 prototype engines (pack /
/// write-back) when enabled. Shared by embedded and server startup.
/// `enable_wb = false` (maintenance commands) never constructs the
/// write-back engine: `gc`/`fsck` stage and drain nothing, and they must
/// stay usable on a write-back deployment while the live server owns the
/// staging directory and the capacity ledger.
fn build_core(
    cfg: &Config,
    meta: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    enable_wb: bool,
) -> Result<Core> {
    let pack = if cfg.pack.enabled {
        Some(cfg.pack.clone())
    } else {
        None
    };
    let wb = if enable_wb && cfg.commit.mode == "writeback" {
        // The ledger lock keys on the DATABASE FILE's canonical identity
        // (S4.3.4b.2): the store was opened above, so the file exists and
        // its symlink-free absolute path is resolvable. Two configs sharing
        // one DB — through any spelling — derive the same lock.
        let db_identity = crate::fslock::canonical_identity(
            std::path::Path::new(&cfg.metadata.path),
            "metadata database",
        )?;
        Some(crate::wb::WbOptions {
            staging_dir: cfg.writeback.staging_dir.clone(),
            drain_workers: cfg.writeback.drain_workers,
            batch_pack_bytes: cfg.writeback.batch_pack_bytes,
            batch_max_wait_ms: cfg.writeback.batch_max_wait_ms,
            queue_max_tasks: cfg.writeback.queue_max_tasks,
            staging_budget_bytes: cfg.writeback.staging_budget_bytes,
            ledger_lock_path: Some(format!("{}.engine-lock", db_identity.display())),
            #[cfg(test)]
            probe_fault: false,
        })
    } else {
        None
    };
    let mut core = Core::with_options(meta, objects, cfg.retention.superseded_secs, pack, wb)?;
    if core.wb().is_some() {
        core.wb().unwrap().recover()?;
        let workers = resolve_wb_workers(
            cfg.writeback.drain_workers,
            std::env::var("LIGHTFS_WB_WORKERS").ok().as_deref(),
        )?;
        core.start_wb_workers(workers);
    } else if enable_wb {
        // S4.10: strict-mode engines take the single-writer engine lock too
        // (write-back engines acquire it inside their constructor; maintenance
        // commands never construct an engine and stay lock-free). Two engines
        // on one database now fail loudly at startup instead of silently
        // corrupting each other's packs. Holding the lock is also what makes
        // the startup stale-pack sweep safe: a building/committing row cannot
        // belong to an in-flight seal of a dead writer once we own the lock.
        let lock = crate::fslock::FileLock::try_acquire(&engine_lock_path(cfg)?)?;
        let sweep = core.recover_stale_packs()?;
        if sweep.finalized_committing > 0
            || sweep.dropped_building > 0
            || sweep.enrolled_stale > 0
            || sweep.head_unavailable > 0
        {
            eprintln!(
                "lightfs: startup stale-pack sweep: {} committing finalized, {} building rows dropped, {} stale uploads enrolled, {} head unavailable (kept)",
                sweep.finalized_committing,
                sweep.dropped_building,
                sweep.enrolled_stale,
                sweep.head_unavailable
            );
        }
        core.hold_engine_lock(lock);
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
        core: Arc::new(build_core(cfg, meta, objects, true)?),
    })
}

/// Embedded engine on the in-memory object store: network-free loop for
/// tests and throwaway exploration. The SQLite file still persists.
pub fn open_embedded_mem(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(crate::object_store::mem::MemObjectStore::new());
    Ok(Engine {
        core: Arc::new(build_core(cfg, meta, objects, true)?),
    })
}

/// Maintenance entry (`lightfs gc`, `lightfs fsck`): metadata + object
/// stores WITHOUT the write-back engine. These commands stage and drain
/// nothing; skipping the WB engine keeps them lock-free so they run against
/// a write-back deployment while the live server owns the staging directory
/// and the capacity ledger.
pub fn open_embedded_maintenance(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(cfg.s3_config())?);
    objects.ping()?;
    Ok(Engine {
        core: Arc::new(build_core(cfg, meta, objects, false)?),
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

    /// S4.10: a strict-mode engine holds the single-writer engine lock for
    /// its whole lifetime — a second engine on the same database is refused
    /// loudly (previously two strict engines silently shared the store).
    /// After the holder goes away, the restart runs the stale-pack sweep:
    /// a `building` row whose object never materialized is dropped without
    /// any manual step.
    #[test]
    fn s410_strict_engine_lock_and_startup_sweep() {
        let mut cfg = Config::default();
        cfg.metadata.path = std::env::temp_dir()
            .join(format!(
                "lightfs-s410-eng-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .to_string();
        let eng = open_embedded_mem(&cfg).unwrap();
        // Simulate a crash leftover: a registered pack whose upload never
        // happened (this engine's in-memory object store holds nothing).
        eng.core
            .metadata()
            .register_pack("packs/s410-lost.pack", 5)
            .unwrap();

        let msg = match open_embedded_mem(&cfg) {
            Err(e) => format!("{e}"),
            Ok(_) => panic!("second strict engine on one db must be refused"),
        };
        assert!(msg.contains("locked"), "got: {msg}");

        drop(eng);
        let eng2 = open_embedded_mem(&cfg).unwrap();
        assert!(
            eng2.core
                .metadata()
                .pack_state("packs/s410-lost.pack")
                .unwrap()
                .is_none(),
            "startup sweep drops the building row whose object is confirmed absent"
        );
    }

    /// S4.10: the gc stale-pack phase is gated on single-writer ownership.
    /// With the lock free it runs; while any engine holds the lock it is
    /// skipped (`None`) — never guessed around.
    #[test]
    fn s410_gc_stale_phase_respects_engine_lock() {
        let mut cfg = Config::default();
        cfg.metadata.path = std::env::temp_dir()
            .join(format!(
                "lightfs-s410-gc-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .to_string();
        let core = crate::core::Core::new(
            std::sync::Arc::new(
                crate::metadata::sqlite::SqliteMetadataStore::open_in_memory().unwrap(),
            ),
            std::sync::Arc::new(crate::object_store::mem::MemObjectStore::new()),
        );
        // canonical_identity refuses a nonexistent database path, so the
        // lock derivation target must exist — as it always does for a real
        // engine (the store is opened before any lock is taken).
        std::fs::write(&cfg.metadata.path, b"").unwrap();
        assert!(
            super::super::cmd::gc_stale_pack_phase(&core, &cfg)
                .unwrap()
                .is_some()
        );
        let _live = crate::fslock::FileLock::try_acquire(&engine_lock_path(&cfg).unwrap()).unwrap();
        assert!(
            super::super::cmd::gc_stale_pack_phase(&core, &cfg)
                .unwrap()
                .is_none()
        );
    }

    /// S4.13 (model gap A): a live engine lock skips ONLY the stale-pack
    /// sweep. The deletion-intent drain still runs with its real semantics,
    /// the engine's in-flight stale building row is NOT touched while the
    /// lock is held, and a sweep after the lock is released converges it.
    /// Every assertion reads SQLite row state, object-store state or the
    /// durable intent queue — never a log line.
    #[test]
    fn s413_gc_lock_skip_keeps_stale_row_and_still_drains_intents() {
        let mut cfg = Config::default();
        cfg.metadata.path = std::env::temp_dir()
            .join(format!(
                "lightfs-s413-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .to_string();
        std::fs::write(&cfg.metadata.path, b"").unwrap();
        let mem = std::sync::Arc::new(crate::object_store::mem::MemObjectStore::new());
        // retention off: candidates qualify as soon as they exist, so the
        // pre-existing intent below drains deterministically in one run.
        let core = std::sync::Arc::new(crate::core::Core::with_retention(
            std::sync::Arc::new(
                crate::metadata::sqlite::SqliteMetadataStore::open_in_memory().unwrap(),
            ),
            mem.clone(),
            0,
        ));
        let now = 10_000i64;

        // A stale building pack left by the "live engine" (row + object up):
        // exactly what the sweep must NOT adjudicate behind the owner's back.
        let stale = "packs/s413-stale.pack";
        core.metadata().register_pack(stale, 5).unwrap();
        mem.put(stale, b"stale").unwrap();

        // A reclaimable pack that is already unreferenced: its durable
        // deletion intent is exactly the work the drain must keep doing
        // even while the sweep is skipped.
        let reclaimable = "packs/s413-reclaim.pack";
        core.metadata().register_pack(reclaimable, 7).unwrap();
        core.metadata().mark_pack_uploaded(reclaimable, 1).unwrap();
        core.metadata()
            .wb_finalize_batch_if_terminal(reclaimable, 1)
            .unwrap();
        mem.put(reclaimable, b"dead").unwrap();

        // Backup root exists but holds no manifest: the protection set is
        // empty and gc_run may run (fail-closed gate satisfied).
        let backup_root =
            std::env::temp_dir().join(format!("lightfs-s413-root-{}", std::process::id()));
        std::fs::create_dir_all(&backup_root).unwrap();

        // A live engine holds the lock for the whole first phase.
        let live = crate::fslock::FileLock::try_acquire(&engine_lock_path(&cfg).unwrap()).unwrap();

        // 1) sweep skipped under the live lock ...
        let sweep = super::super::cmd::gc_stale_pack_phase(&core, &cfg).unwrap();
        assert!(sweep.is_none(), "gc must skip the stale-pack sweep");
        assert_eq!(
            core.metadata().pack_state(stale).unwrap().map(|(s, _)| s),
            Some("building".into()),
            "the stale building row survives the skipped sweep untouched"
        );
        assert!(mem.head(stale).is_ok(), "its object is not touched either");

        // 2) ... but the deletion-intent drain still runs to real semantics.
        let summary =
            crate::cmd::gc_run(&core, Some(backup_root.to_str().unwrap()), 5, 0, now).unwrap();
        assert_eq!(
            (summary.enrolled.0, summary.done),
            (1, 1),
            "the reclaimable pack is enrolled and executed while the sweep is skipped"
        );
        assert!(
            mem.head(reclaimable).is_err(),
            "the drain really deleted the reclaimable object"
        );
        assert!(
            core.metadata().pack_state(reclaimable).unwrap().is_none(),
            "its row went with the completed intent"
        );
        assert!(
            core.metadata()
                .gc_claim_deletions(10, now)
                .unwrap()
                .is_empty(),
            "no durable intent is left queued after the drain"
        );
        assert_eq!(
            core.metadata().pack_state(stale).unwrap().map(|(s, _)| s),
            Some("building".into()),
            "the skipped sweep left the stale row exactly as it was"
        );

        // 3) the engine dies, the lock is released: the NEXT sweep pass
        //    adjudicates the stale row (HEAD 200 -> deleting + intent).
        drop(live);
        let sweep = super::super::cmd::gc_stale_pack_phase(&core, &cfg)
            .unwrap()
            .expect("the sweep runs once the lock is free");
        assert_eq!(sweep.enrolled_stale, 1, "the stale upload is enrolled");
        assert_eq!(
            core.metadata().pack_state(stale).unwrap().map(|(s, _)| s),
            Some("deleting".into()),
            "building -> deleting under a durable pending intent"
        );

        // 4) and the next gc run converges it all the way.
        let summary =
            crate::cmd::gc_run(&core, Some(backup_root.to_str().unwrap()), 5, 0, now).unwrap();
        assert_eq!(summary.done, 1, "the enrolled stale pack executes");
        assert!(mem.head(stale).is_err(), "stale object actually deleted");
        assert!(
            core.metadata().pack_state(stale).unwrap().is_none(),
            "stale row dropped with its completed intent"
        );
        assert!(
            core.metadata()
                .gc_claim_deletions(10, now)
                .unwrap()
                .is_empty(),
            "the queue is fully drained"
        );
        let _ = std::fs::remove_file(&cfg.metadata.path);
        let _ = std::fs::remove_dir_all(&backup_root);
    }
}
