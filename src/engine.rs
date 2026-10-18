//! Engine composition (vnext): the vnext commit/ref Core + ObjectStore.
//! The server process (and integration tests) start from here. One engine
//! process owns one metadata store, enforced by the engine lock.

use std::sync::Arc;

use crate::config::Config;
use crate::error::{Error, Result};
use crate::object_store::ObjectStore;
use crate::object_store::s3::S3ObjectStore;
use crate::vnext::Core;

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

/// Create the zero-byte database placeholder RACE-SAFELY: the check and
/// the creation are one atomic `create_new` syscall, so a concurrent
/// first-open can never truncate a database another process has already
/// initialized. (Carried over from v0.1 — the lock derivation needs the
/// file to exist, and initialization must never truncate.)
fn ensure_db_file_exists(path: &str) -> Result<()> {
    // Dangling symlink: refuse EXPLICITLY, before any filesystem mutation.
    if let Ok(md) = std::fs::symlink_metadata(path) {
        if md.file_type().is_symlink() && std::fs::metadata(path).is_err() {
            return Err(Error::InvalidArgument(format!(
                "metadata path {path:?} is a dangling symlink; refusing to initialize a database through it"
            )));
        }
    }
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

/// The single-writer engine lock path for a config
/// (`<canonical db path>.engine-lock`). One lock per store, held for the
/// engine's lifetime; the second engine is a loud refusal.
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

fn reject_non_strict(cfg: &Config) -> Result<()> {
    match cfg.commit.mode.as_str() {
        "" | "strict" => Ok(()),
        "writeback" => Err(Error::Config(
            "commit.mode = \"writeback\" is not implemented in vnext: the \
             long-term core is strict-commit only in this slice"
                .into(),
        )),
        other => Err(Error::Config(format!("config: unknown commit mode {other:?}"))),
    }
}

/// The engine entry point: vnext Core + SQLite + S3 in one process.
/// `lightfs server` runs this; integration tests use it to exercise the
/// exact production wiring. Ownership order (S4.14.1, carried over):
/// config/parent-dir prep, then the engine lock taken from the
/// database's canonical identity WITHOUT opening or initializing
/// SQLite, then everything else under that lock.
/// The `[staging]` section as core limits (S4.25).
fn staging_limits(cfg: &Config) -> crate::staging::StagingLimits {
    crate::staging::StagingLimits {
        budget_bytes: cfg.staging.budget_bytes,
        max_object_bytes: cfg.staging.max_object_bytes,
    }
}

/// Open the engine's staging budget under the configured `[staging].dir`
/// root (empty = the system temp directory, swept at startup like every
/// managed budget). A directory that cannot be created or locked degrades
/// to a headless budget with a loud warning — engine open never fails on
/// staging governance.
fn open_staging_budget(cfg: &Config) -> std::sync::Arc<crate::staging::StagingBudget> {
    let limits = staging_limits(cfg);
    let opened = if cfg.staging.dir.is_empty() {
        crate::staging::StagingBudget::open(limits)
    } else {
        crate::staging::StagingBudget::open_at(limits, std::path::PathBuf::from(&cfg.staging.dir))
    };
    match opened {
        Ok(b) => b,
        Err(e) => {
            eprintln!(
                "lightfs: WARNING managed staging directory unavailable ({e}); \
                 admission stays arithmetic-only (budget still enforced, files \
                 land in the raw temp dir)"
            );
            crate::staging::StagingBudget::headless(limits)
        }
    }
}

pub fn open_engine(cfg: &Config) -> Result<Engine> {
    reject_non_strict(cfg)?;
    ensure_parent_dir(&cfg.metadata.path);
    ensure_db_file_exists(&cfg.metadata.path)?;
    let lock = crate::fslock::FileLock::try_acquire(&engine_lock_path(cfg)?)?;
    let store = crate::vnext::Store::open(&cfg.metadata.path)?;
    let objects: Arc<dyn ObjectStore> = Arc::new(S3ObjectStore::new(cfg.s3_config())?);
    let staging = open_staging_budget(cfg);
    let mut core =
        Core::new_shared_with_staging_budget(Arc::new(store), objects, staging)?;
    core.hold_engine_lock(lock);
    Ok(Engine {
        core: Arc::new(core),
    })
}

/// Engine over the in-memory object store: network-free loop for tests.
/// The SQLite file still persists. This is a test facility, NOT a
/// deployment mode. Ownership order is identical to [`open_engine`].
pub fn open_engine_mem(cfg: &Config) -> Result<Engine> {
    reject_non_strict(cfg)?;
    ensure_parent_dir(&cfg.metadata.path);
    ensure_db_file_exists(&cfg.metadata.path)?;
    let lock = crate::fslock::FileLock::try_acquire(&engine_lock_path(cfg)?)?;
    let store = crate::vnext::Store::open(&cfg.metadata.path)?;
    let objects: Arc<dyn ObjectStore> = Arc::new(crate::object_store::mem::MemObjectStore::new());
    let staging = open_staging_budget(cfg);
    let mut core =
        Core::new_shared_with_staging_budget(Arc::new(store), objects, staging)?;
    core.hold_engine_lock(lock);
    Ok(Engine {
        core: Arc::new(core),
    })
}
