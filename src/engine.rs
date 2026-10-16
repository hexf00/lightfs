//! Engine composition: Filesystem Core + MetadataStore + ObjectStore.
//! The server process (and integration tests) start from here. One engine
//! process owns one SQLite metadata store, enforced by the engine lock.

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

/// Create the zero-byte database placeholder RACE-SAFELY (S4.14.1): the
/// check and the creation are one atomic `create_new` syscall, so a
/// concurrent first-open can never truncate a database another process has
/// already initialized — `create_new` fails with AlreadyExists if the path
/// exists (including as a dangling symlink, which is reported, not
/// followed). Callers then resolve the canonical identity of a file that
/// is guaranteed to exist.
fn ensure_db_file_exists(path: &str) -> Result<()> {
    match std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
    {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.into()),
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
/// write-back) when enabled. Shared by the server and maintenance startup.
/// OWNERSHIP IS NOT TAKEN HERE (S4.14.1): the `open_*` entry acquires the
/// engine lock BEFORE SQLite is opened and hands it to the Core afterwards,
/// so every branch below runs under an already-established single-writer
/// title and there is exactly ONE lock-acquisition point per engine.
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
        // The DATABASE engine lock is NOT taken here (S4.14.1): the open_*
        // entry already holds it before SQLite was ever opened, and holds
        // it for the engine's lifetime. The write-back engine keeps only
        // its own staging-directory lock.
        Some(crate::wb::WbOptions {
            staging_dir: cfg.writeback.staging_dir.clone(),
            drain_workers: cfg.writeback.drain_workers,
            batch_pack_bytes: cfg.writeback.batch_pack_bytes,
            batch_max_wait_ms: cfg.writeback.batch_max_wait_ms,
            queue_max_tasks: cfg.writeback.queue_max_tasks,
            staging_budget_bytes: cfg.writeback.staging_budget_bytes,
            #[cfg(test)]
            probe_fault: false,
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
    } else if enable_wb {
        // S4.10/S4.14.1: the single-writer engine lock is already HELD —
        // the open_* entry took it before SQLite was opened and hands it
        // to the Core below. Two engines on one database fail loudly at
        // that entry instead of silently corrupting each other's packs.
        // Holding the lock is also what makes the startup stale-pack sweep
        // safe: a building/committing row cannot belong to an in-flight
        // seal of a dead writer once we own the lock.
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
    }
    Ok(core)
}

/// The engine entry point: Core + SQLite + S3 in one process. `lightfs
/// server` runs this; integration tests use it to exercise the exact
/// production wiring.
///
/// Ownership order (S4.14.1) — config/parent-dir prep, then the engine
/// lock taken from the database's canonical identity WITHOUT opening or
/// initializing SQLite, then everything else (SQLite, ObjectStore, ping,
/// Core, WB ledger recovery, workers, startup recovery) under that lock.
/// A held lock is a loud refusal BEFORE any data store is touched, and the
/// lock is handed to the Core and kept for the engine's lifetime.
pub fn open_engine(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    ensure_db_file_exists(&cfg.metadata.path)?;
    let lock = crate::fslock::FileLock::try_acquire(&engine_lock_path(cfg)?)?;
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(cfg.s3_config())?);
    objects.ping()?;
    let mut core = build_core(cfg, meta, objects, true)?;
    core.hold_engine_lock(lock);
    Ok(Engine {
        core: Arc::new(core),
    })
}

/// Engine over the in-memory object store: network-free loop for tests and
/// throwaway exploration. The SQLite file still persists. This is a test
/// facility, NOT a deployment mode — production code must go through
/// [`open_engine`] or [`open_maintenance`]. Ownership order is identical
/// to [`open_engine`].
pub fn open_engine_mem(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    ensure_db_file_exists(&cfg.metadata.path)?;
    let lock = crate::fslock::FileLock::try_acquire(&engine_lock_path(cfg)?)?;
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(crate::object_store::mem::MemObjectStore::new());
    let mut core = build_core(cfg, meta, objects, true)?;
    core.hold_engine_lock(lock);
    Ok(Engine {
        core: Arc::new(core),
    })
}

/// Maintenance entry (`lightfs gc`, `lightfs fsck`): an exclusive, offline
/// engine. Ownership contract (S4.14): the engine lock is acquired BEFORE
/// SQLite is opened or initialized, and the lock is held until the process
/// exits — so a maintenance command against a running server fails loudly
/// at the lock and never even opens the database, and a server cannot start
/// while a maintenance pass runs. No write-back engine is constructed:
/// maintenance stages and drains nothing; the live server owns the staging
/// directory and the capacity ledger.
pub fn open_maintenance(cfg: &Config) -> Result<Engine> {
    ensure_parent_dir(&cfg.metadata.path);
    // The lock derivation (canonical_identity) needs the target file to
    // exist. The placeholder is created race-safely (S4.14.1): create_new
    // never truncates an existing database, and an empty placeholder is
    // not an initialized SQLite file — a real open writes the header page
    // — so lock-before-SQLite holds.
    ensure_db_file_exists(&cfg.metadata.path)?;
    let lock = crate::fslock::FileLock::try_acquire(&engine_lock_path(cfg)?)?;
    let meta: Arc<dyn MetadataStore> = Arc::new(SqliteMetadataStore::open(&cfg.metadata.path)?);
    let objects: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(cfg.s3_config())?);
    objects.ping()?;
    let mut core = build_core(cfg, meta, objects, false)?;
    core.hold_engine_lock(lock);
    Ok(Engine {
        core: Arc::new(core),
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
        let msg = match open_engine_mem(&cfg) {
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
        let eng = open_engine_mem(&cfg).unwrap();
        // Simulate a crash leftover: a registered pack whose upload never
        // happened (this engine's in-memory object store holds nothing).
        eng.core
            .metadata()
            .register_pack("packs/s410-lost.pack", 5)
            .unwrap();

        let msg = match open_engine_mem(&cfg) {
            Err(e) => format!("{e}"),
            Ok(_) => panic!("second strict engine on one db must be refused"),
        };
        assert!(msg.contains("locked"), "got: {msg}");

        drop(eng);
        let eng2 = open_engine_mem(&cfg).unwrap();
        assert!(
            eng2.core
                .metadata()
                .pack_state("packs/s410-lost.pack")
                .unwrap()
                .is_none(),
            "startup sweep drops the building row whose object is confirmed absent"
        );
    }

    /// S4.13.1 / S4.14 (model gap A, re-staged): the GC STATE MACHINE under
    /// LEGITIMATE ownership — the maintenance entry already holds the engine
    /// lock (see the s414 ownership tests for the refusal side), so this is
    /// exactly the sequence `lightfs gc` runs: stale-pack sweep FIRST, then
    /// the enroll/execute pass. Coverage: an intent durably registered
    /// BEFORE the run, a candidate the run enrolls itself, and a stale
    /// building row the sweep adjudicates — objects really deleted, pack
    /// rows really gone, the durable queue fully drained. Every assertion
    /// reads SQLite row state, object-store state or the intent queue.
    #[test]
    fn s413_gc_state_machine_drains_both_intent_sources_and_converges_stale() {
        let mem = std::sync::Arc::new(crate::object_store::mem::MemObjectStore::new());
        // retention off: candidates qualify as soon as they exist, so every
        // intent below drains deterministically in one run.
        let core = std::sync::Arc::new(crate::core::Core::with_retention(
            std::sync::Arc::new(
                crate::metadata::sqlite::SqliteMetadataStore::open_in_memory().unwrap(),
            ),
            mem.clone(),
            0,
        ));
        let now = 10_000i64;

        // A stale building pack (row + object): the sweep's job.
        let stale = "packs/s413-stale.pack";
        core.metadata().register_pack(stale, 5).unwrap();
        mem.put(stale, b"stale").unwrap();

        // (a) A PRE-EXISTING durable deletion intent, fully registered
        // BEFORE gc_run is ever invoked, so the drain below proves it
        // executes work it did not create in this run. Registration and
        // the pending intent row land in ONE transaction, so pack state
        // `deleting` is the durable evidence the intent exists.
        let preexisting = "packs/s413-preexisting.pack";
        core.metadata().register_pack(preexisting, 7).unwrap();
        core.metadata().mark_pack_uploaded(preexisting, 1).unwrap();
        core.metadata()
            .wb_finalize_batch_if_terminal(preexisting, 1)
            .unwrap();
        mem.put(preexisting, b"dead-before-gc").unwrap();
        assert!(
            core.metadata()
                .gc_register_pack_deletion(preexisting, 1)
                .unwrap(),
            "the unreferenced pack registers its durable intent"
        );
        assert_eq!(
            core.metadata()
                .pack_state(preexisting)
                .unwrap()
                .map(|(s, _)| s),
            Some("deleting".into()),
            "registered = flipping unreferenced -> deleting with a pending intent"
        );

        // (b) A reclaimable pack that is merely unreferenced: NO intent
        // yet — gc_run must enroll it itself in this same run.
        let reclaimable = "packs/s413-reclaim.pack";
        core.metadata().register_pack(reclaimable, 9).unwrap();
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

        // Phase 1 (the sweep, run under the lock before anything else):
        // the stale building row is adjudicated — building -> deleting
        // with a durable pending intent. The two deletion candidates are
        // NOT the sweep's business.
        let sweep = core.recover_stale_packs().unwrap();
        assert_eq!(sweep.enrolled_stale, 1, "the stale upload is enrolled");
        assert_eq!(
            sweep.dropped_building, 0,
            "its object is up, nothing to drop"
        );
        assert_eq!(
            core.metadata().pack_state(stale).unwrap().map(|(s, _)| s),
            Some("deleting".into()),
            "building -> deleting under a durable pending intent"
        );

        // Phase 2 (the enroll/execute pass): ONE candidate enrolled fresh
        // this run (the reclaimable pack), THREE intents completed (the
        // pre-existing one, the freshly enrolled one, and the sweep's).
        let summary =
            crate::cmd::gc_run(&core, Some(backup_root.to_str().unwrap()), 5, 0, now).unwrap();
        assert_eq!(
            summary.enrolled.0, 1,
            "only the not-yet-registered pack is enrolled by this run"
        );
        assert_eq!(
            summary.done, 3,
            "pre-existing + freshly enrolled + sweep-enrolled intents all execute"
        );
        assert!(
            mem.head(preexisting).is_err(),
            "the pre-existing intent really deleted its object"
        );
        assert!(
            core.metadata().pack_state(preexisting).unwrap().is_none(),
            "the pre-existing pack row went with its completed intent"
        );
        assert!(
            mem.head(reclaimable).is_err(),
            "the drain really deleted the freshly enrolled object"
        );
        assert!(
            core.metadata().pack_state(reclaimable).unwrap().is_none(),
            "its row went with the completed intent"
        );
        assert!(
            mem.head(stale).is_err(),
            "the sweep's intent really deleted the stale object"
        );
        assert!(
            core.metadata().pack_state(stale).unwrap().is_none(),
            "the stale row dropped with its completed intent"
        );
        assert!(
            core.metadata()
                .gc_claim_deletions(10, now)
                .unwrap()
                .is_empty(),
            "the durable queue is fully drained — converged in one pass"
        );
        let _ = std::fs::remove_dir_all(&backup_root);
    }

    /// S4.14 ownership contract, in-process layer: `open_maintenance` takes
    /// the engine lock BEFORE SQLite is opened, and a held lock is a loud
    /// refusal — never "skip the sweep and continue". The durable evidence
    /// that SQLite was never opened: the database file is still ZERO bytes
    /// (a real SQLite open writes the header page). With the lock free the
    /// same entry succeeds and the file becomes a real database.
    #[test]
    fn s414_maintenance_lock_precedes_sqlite_open() {
        let mut cfg = Config::default();
        cfg.metadata.path = std::env::temp_dir()
            .join(format!(
                "lightfs-s414-mnt-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ))
            .to_string_lossy()
            .to_string();
        // The lock derivation needs the target to exist; an empty file is
        // the point — creating it does not initialize SQLite.
        std::fs::write(&cfg.metadata.path, b"").unwrap();
        // A live engine (exactly what `lightfs server` holds) owns the lock.
        let live = crate::fslock::FileLock::try_acquire(&engine_lock_path(&cfg).unwrap()).unwrap();
        let err = match open_maintenance(&cfg) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("maintenance must be refused while the lock is held"),
        };
        assert!(
            err.contains("is locked by"),
            "maintenance must be refused at the lock, got: {err}"
        );
        assert_eq!(
            std::fs::metadata(&cfg.metadata.path).unwrap().len(),
            0,
            "refusal happened BEFORE SQLite was opened — the header was never written"
        );
        drop(live);
        // The lock dies with its holder: a maintenance run is grantable
        // again. (The granted branch itself — full gc/fsck to completion —
        // is exercised end-to-end with a real object store by
        // scripts/s431-gc-e2e.sh; this layer stays network-free.)
        // Bounded retry: Darwin shows a close->flock visibility window of
        // tens of microseconds under parallel load (S4.3.4b.2), so the
        // re-acquire is retried, not assumed instantaneous.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match crate::fslock::FileLock::try_acquire(&engine_lock_path(&cfg).unwrap()) {
                Ok(_) => break,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(20))
                }
                Err(e) => {
                    panic!("the engine lock must be re-acquirable once the holder is gone: {e}")
                }
            }
        }
        let _ = std::fs::remove_file(&cfg.metadata.path);
    }

    /// S4.14 child (holder): builds an engine through the exact production
    /// wiring — `lightfs server` uses open_engine, whose strict branch takes
    /// the engine lock and holds it for the process lifetime; the _mem
    /// variant differs only in the object store, the lock path is identical.
    /// Signals readiness and parks until SIGKILLed. No-op without the env.
    #[test]
    fn s414_gc_hold_child_helper() {
        let Some(db) = std::env::var_os("LIGHTFS_S414_HOLD_DB") else {
            return;
        };
        let db = std::path::PathBuf::from(db);
        let mut cfg = Config::default();
        cfg.metadata.path = db.to_string_lossy().to_string();
        let _eng = open_engine_mem(&cfg).expect("child engine");
        let ready = db.parent().unwrap().join("child.ready");
        std::fs::write(ready, b"held").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// S4.14 child (gc): a full `lightfs gc` invocation (cmd::gc =
    /// open_maintenance -> sweep -> gc_run) against the held lock. Exit 3
    /// with the lock error on stdout's stderr twin; exit 0 would mean the
    /// ownership contract is broken. No-op without the env.
    #[test]
    fn s414_gc_refused_child_helper() {
        let Some(db) = std::env::var_os("LIGHTFS_S414_GC_DB") else {
            return;
        };
        let mut cfg = Config::default();
        cfg.metadata.path = db.to_string_lossy().to_string();
        if let Some(ep) = std::env::var_os("LIGHTFS_S414_GC_ENDPOINT") {
            cfg.s3.endpoint = ep.to_string_lossy().to_string();
            cfg.s3.path_style = true;
        }
        match crate::cmd::gc(&cfg) {
            Ok(()) => {
                eprintln!("S414-GC-UNEXPECTED-OK");
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("S414-GC-ERR:{e}");
                std::process::exit(3);
            }
        }
    }

    /// S4.14 ownership contract, cross-process layer (the one the model
    /// talks about): a REAL server-path process holds the engine lock, and
    /// a REAL `lightfs gc` process must fail BEFORE it opens SQLite, let
    /// alone deletes objects — the lock refusal is the whole pass, not a
    /// skipped phase. Bounded readiness wait over a ready file + exit-code
    /// IPC; no timing assumptions.
    #[test]
    fn s414_gc_refused_while_server_holds_engine_lock() {
        let dir = std::env::temp_dir().join(format!("lightfs-s414-own-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("meta.db");
        let ready = dir.join("child.ready");
        let exe = std::env::current_exe().unwrap();
        let holder = std::process::Command::new(&exe)
            .args(["s414_gc_hold_child_helper", "--nocapture"])
            .env("LIGHTFS_S414_HOLD_DB", &db)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        struct Guard(std::process::Child);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let guard = Guard(holder);
        let mut waited = 0u32;
        while !ready.exists() && waited < 10_000 {
            std::thread::sleep(std::time::Duration::from_millis(20));
            waited += 20;
        }
        assert!(ready.exists(), "holder child never took the engine lock");
        let _ = &guard;

        let out = std::process::Command::new(&exe)
            .args(["s414_gc_refused_child_helper", "--nocapture"])
            .env("LIGHTFS_S414_GC_DB", &db)
            // S4.14.1 direction A hardening: the endpoint is UNREACHABLE,
            // so if a regression ever moved the lock behind the object
            // store ping, the child would die with a transport error and
            // the lock-refusal assertions below would fail.
            .env("LIGHTFS_S414_GC_ENDPOINT", "http://127.0.0.1:1")
            .output()
            .unwrap();
        let stderr_txt = String::from_utf8_lossy(&out.stderr);
        assert_eq!(
            out.status.code(),
            Some(3),
            "gc must be REFUSED by the held lock; stderr: {stderr_txt}"
        );
        assert!(
            stderr_txt.contains("is locked by"),
            "the refusal must be the engine lock itself, got: {stderr_txt}"
        );
        assert!(
            !stderr_txt.contains("backup"),
            "the lock gate sits BEFORE every other gc gate, got: {stderr_txt}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- S4.14.1: race-safe initialization + direction B ------------------

    /// The placeholder helper must never touch an existing file: the whole
    /// point of `create_new` is that a database another process already
    /// initialized is byte-untouchable.
    #[test]
    fn s4141_placeholder_never_truncates_existing_file() {
        let dir = std::env::temp_dir().join(format!("lightfs-s4141-trunc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("m.db");
        let body = b"PRE-EXISTING DATABASE BYTES - not a placeholder, must survive";
        std::fs::write(&path, body).unwrap();
        ensure_db_file_exists(path.to_str().unwrap()).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            body,
            "an existing database must survive the initialization helper byte-exact"
        );
        // End to end: with the lock free, the engine entry refuses a
        // garbage non-empty database at SQLite open — and the bytes are
        // STILL intact (nothing truncated or rewritten it first).
        let mut cfg = Config::default();
        cfg.metadata.path = path.to_string_lossy().to_string();
        assert!(
            open_engine_mem(&cfg).is_err(),
            "garbage db must fail to open"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            body,
            "the engine startup path must not truncate or rewrite an existing database"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// N processes race the FIRST initialization of one database through
    /// the PRODUCTION entry `open_engine_mem` — the placeholder/lock
    /// sequence is not re-implemented here — released by a start file so
    /// they overlap for real. Exactly ONE child may succeed, and it keeps
    /// the returned Engine (and with it the engine lock) alive until the
    /// parent has collected every outcome. Every loser must report the
    /// engine-lock conflict; a SQLite, schema, or any other error is a
    /// failed race, not a legitimate loser.
    #[test]
    fn s4141_first_open_race_child_helper() {
        let Some(dir) = std::env::var_os("LIGHTFS_S4141_RACE_DIR") else {
            return;
        };
        let Some(id) = std::env::var_os("LIGHTFS_S4141_RACE_ID") else {
            return;
        };
        let dir = std::path::PathBuf::from(dir);
        let start = dir.join("start");
        let mut waited = 0u32;
        while !start.exists() && waited < 10_000 {
            std::thread::sleep(std::time::Duration::from_millis(20));
            waited += 20;
        }
        if !start.exists() {
            eprintln!("S4141-RACE: start file never appeared");
            std::process::exit(2);
        }
        let mut cfg = Config::default();
        cfg.metadata.path = dir.join("m.db").to_string_lossy().to_string();
        let result_file = dir.join(format!(
            "result-{}-{}",
            id.to_string_lossy(),
            std::process::id()
        ));
        match open_engine_mem(&cfg) {
            Ok(engine) => {
                std::fs::write(result_file, b"locked").unwrap();
                // Hold the ENGINE — the lock lives as long as this value —
                // until the parent has collected every outcome: a loser
                // must observe the conflict WHILE the winner holds it, not
                // after the winner exits.
                let release = dir.join("release");
                let mut held = 0u32;
                while !release.exists() && held < 15_000 {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                    held += 20;
                }
                drop(engine);
            }
            Err(e) => {
                let msg = e.to_string();
                let text = if msg.contains("is locked by") {
                    "refused".to_string()
                } else {
                    format!("UNEXPECTED:{msg}")
                };
                std::fs::write(result_file, text.as_bytes()).unwrap();
            }
        }
    }

    #[test]
    fn s4141_concurrent_first_open_yields_exactly_one_lock_holder() {
        let dir = std::env::temp_dir().join(format!("lightfs-s4141-race-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let exe = std::env::current_exe().unwrap();
        const N: u32 = 4;
        let mut children = Vec::new();
        for i in 0..N {
            children.push(
                std::process::Command::new(&exe)
                    .args(["s4141_first_open_race_child_helper", "--nocapture"])
                    .env("LIGHTFS_S4141_RACE_DIR", &dir)
                    .env("LIGHTFS_S4141_RACE_ID", i.to_string())
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .spawn()
                    .unwrap(),
            );
        }
        struct Guard(Vec<std::process::Child>);
        impl Drop for Guard {
            fn drop(&mut self) {
                for c in &mut self.0 {
                    let _ = c.kill();
                    let _ = c.wait();
                }
            }
        }
        let guard = Guard(children);
        // All children are waiting on the start file; release them at once.
        std::fs::write(dir.join("start"), b"go").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let mut outcomes: Vec<String> = Vec::new();
        while outcomes.len() < N as usize && std::time::Instant::now() < deadline {
            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.starts_with("result-") {
                    let text = std::fs::read_to_string(entry.path()).unwrap();
                    std::fs::remove_file(entry.path()).unwrap();
                    assert!(
                        text == "locked" || text == "refused",
                        "unexpected race outcome: {text}"
                    );
                    outcomes.push(text);
                }
            }
            if outcomes.len() < N as usize {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
        }
        assert_eq!(
            outcomes.len(),
            N as usize,
            "all racers must report; got {outcomes:?}"
        );
        let _ = &guard;
        // Exactly one winner; every other racer saw the SAME lock conflict.
        assert_eq!(
            outcomes.iter().filter(|o| o.as_str() == "locked").count(),
            1,
            "exactly one first-opener may hold the lock; outcomes: {outcomes:?}"
        );
        assert_eq!(
            outcomes.iter().filter(|o| o.as_str() == "refused").count(),
            N as usize - 1,
            "every loser must see the lock conflict"
        );
        // All outcomes observed while the winner still holds the lock.
        std::fs::write(dir.join("release"), b"done").unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Direction B (S4.14.1): the maintenance side holds the engine lock
    /// over a NOT-YET-INITIALIZED database (zero-byte placeholder, SQLite
    /// never opened), and a REAL server start — the production
    /// `open_engine` entry, spawned as its own process — must be refused
    /// at that lock BEFORE SQLite is opened and BEFORE the object store is
    /// touched. Evidence: exit code + lock-conflict text (not a transport
    /// error, despite an unreachable endpoint) + the database file is
    /// still zero bytes.
    #[test]
    fn s414_server_hold_child_helper() {
        let Some(db) = std::env::var_os("LIGHTFS_S414_MNT_DB") else {
            return;
        };
        let db = std::path::PathBuf::from(db);
        // The maintenance owner's exact contract state: placeholder exists,
        // lock held, SQLite not opened. (`open_maintenance` itself would
        // initialize the database, which would erase the zero-byte
        // evidence the server child is assertable against — the lock is
        // the same kernel lock either way.)
        ensure_db_file_exists(db.to_str().unwrap()).unwrap();
        let mut cfg = Config::default();
        cfg.metadata.path = db.to_string_lossy().to_string();
        let _held = crate::fslock::FileLock::try_acquire(&engine_lock_path(&cfg).unwrap())
            .expect("maintenance child takes the engine lock");
        let ready = db.parent().unwrap().join("mnt.ready");
        std::fs::write(ready, b"held").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    #[test]
    fn s414_server_refused_child_helper() {
        let Some(db) = std::env::var_os("LIGHTFS_S414_SRV_DB") else {
            return;
        };
        let mut cfg = Config::default();
        cfg.metadata.path = db.to_string_lossy().to_string();
        // Unreachable endpoint: if a regression ever pings the object store
        // before taking the lock, this child fails with a transport error
        // instead of the lock conflict — and the assertions below fail.
        cfg.s3.endpoint = "http://127.0.0.1:1".into();
        cfg.s3.path_style = true;
        match open_engine(&cfg) {
            Ok(_) => {
                eprintln!("S414-SRV-UNEXPECTED-OK");
                std::process::exit(0);
            }
            Err(e) => {
                eprintln!("S414-SRV-ERR:{e}");
                std::process::exit(3);
            }
        }
    }

    #[test]
    fn s414_server_refused_while_maintenance_holds_engine_lock() {
        let dir = std::env::temp_dir().join(format!("lightfs-s414-dirb-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("meta.db");
        let exe = std::env::current_exe().unwrap();
        let holder = std::process::Command::new(&exe)
            .args(["s414_server_hold_child_helper", "--nocapture"])
            .env("LIGHTFS_S414_MNT_DB", &db)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        struct Guard(std::process::Child);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let guard = Guard(holder);
        let ready = dir.join("mnt.ready");
        let mut waited = 0u32;
        while !ready.exists() && waited < 10_000 {
            std::thread::sleep(std::time::Duration::from_millis(20));
            waited += 20;
        }
        assert!(
            ready.exists(),
            "maintenance child never took the engine lock"
        );

        let out = std::process::Command::new(&exe)
            .args(["s414_server_refused_child_helper", "--nocapture"])
            .env("LIGHTFS_S414_SRV_DB", &db)
            .output()
            .unwrap();
        let stderr_txt = String::from_utf8_lossy(&out.stderr);
        assert_eq!(
            out.status.code(),
            Some(3),
            "the server start must be REFUSED by the held lock; stderr: {stderr_txt}"
        );
        assert!(
            stderr_txt.contains("is locked by"),
            "the refusal must be the engine lock itself, got: {stderr_txt}"
        );
        assert!(
            !stderr_txt.contains("transport") && !stderr_txt.contains("Connection refused"),
            "the lock gate must sit BEFORE the object store ping (unreachable endpoint must never be hit), got: {stderr_txt}"
        );
        assert_eq!(
            std::fs::metadata(&db).unwrap().len(),
            0,
            "the refused server start must not have opened or initialized SQLite"
        );
        let _ = &guard;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Alias regression (S4.14.1 §2): two spellings of one database derive
    /// the SAME engine lock, so whichever entry takes it first wins and the
    /// other is refused. Unix exercises the symlink alias; every platform
    /// exercises the dot-path alias.
    #[test]
    fn s4141_alias_leads_to_one_engine_lock() {
        let dir = std::env::temp_dir().join(format!("lightfs-s4141-alias-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("m.db");
        ensure_db_file_exists(db.to_str().unwrap()).unwrap();
        #[cfg(unix)]
        let alias = {
            let l = dir.join("alias.db");
            std::os::unix::fs::symlink(&db, &l).unwrap();
            l
        };
        #[cfg(not(unix))]
        let alias = dir.join(".").join("m.db");
        let cfg_for = |p: &std::path::Path| {
            let mut c = Config::default();
            c.metadata.path = p.to_string_lossy().to_string();
            c
        };
        assert_eq!(
            engine_lock_path(&cfg_for(&db)).unwrap(),
            engine_lock_path(&cfg_for(&alias)).unwrap(),
            "both spellings must resolve to one lock file"
        );
        let _eng = open_engine_mem(&cfg_for(&db)).unwrap();
        let err = match open_maintenance(&cfg_for(&alias)) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("the alias must reach the SAME engine lock and be refused"),
        };
        assert!(err.contains("is locked by"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
