//! `lightfs` subcommand implementations live here so the library (and tests)
//! can drive the same code paths the binary uses.

use crate::config::Config;
use crate::engine;
use crate::error::{Error, Result};

/// `lightfs server`: own the metadata authority and serve the client
/// protocol. Clients never touch the metadata DB directly.
pub fn server(cfg: &Config) -> Result<()> {
    let eng = engine::open_engine(cfg)?;
    crate::server::serve(eng, &cfg.server.bind)
}

/// `lightfs backup [--expires-in SECS]` (P0-b): atomically publish one
/// backup directory under the canonical `[backup] root` — a `VACUUM INTO`
/// metadata snapshot, a versioned object manifest exported FROM THE
/// SNAPSHOT ITSELF, and `backup.json` pinning the manifest's sha256. Helds
/// the maintenance lock from before the snapshot until after the rename.
/// See `crate::backup` for the full contract.
pub fn backup(cfg: &Config, expires_in: Option<u64>) -> Result<()> {
    crate::backup::backup_now(cfg, expires_in)?;
    Ok(())
}

/// `lightfs backup prune --id ID` (P0-b): remove one backup under the
/// exclusive maintenance lock — the ONLY sanctioned way to resolve a
/// fail-closed corrupt backup.
pub fn backup_prune(cfg: &Config, id: &str) -> Result<()> {
    crate::backup::backup_prune(cfg, id)
}

/// One gated gc pass over `core`: verify EVERY published manifest (fail
/// closed on any problem), then enroll reclaimable candidates and execute
/// their durable deletion tasks — never touching manifest-protected keys.
/// Shared by the CLI (`lightfs gc`) and the acceptance tests (fake store).
///
/// `retention_secs = 0` (retention off) does NOT disable gc: retirement
/// under retention-off registers durable deletion tasks instead of
/// superseded rows, so gc is the ONLY path that drains them. The cutoff
/// becomes `now`, i.e. candidates qualify from the moment they are
/// registered — which is exactly what "no retention window" means.
/// The [backup]-root / lock / manifest-verification gates apply unchanged.
#[derive(Debug)]
pub struct GcSummary {
    pub backups: usize,
    pub protected_keys: usize,
    pub enrolled: (usize, usize),
    pub done: u64,
    pub failed: u64,
    pub skipped_protected: u64,
}

pub fn gc_run(
    core: &crate::core::Core,
    backup_root: Option<&str>,
    lock_timeout_secs: u64,
    retention_secs: u64,
    now: i64,
) -> Result<GcSummary> {
    let Some(root) = backup_root else {
        return Err(Error::Config(
            "gc: [backup] root is not configured — there is no way to know which objects \
             historical backups still reference; refusing to delete anything"
                .into(),
        ));
    };
    let root_path = std::path::PathBuf::from(root);
    // The lock spans manifest verification through deletion finalization:
    // a backup can never publish (or a prune remove) while its protection
    // set is being read and acted on.
    let _lock = crate::backup::MaintenanceLock::acquire(&root_path, lock_timeout_secs)?;
    let verified = crate::backup::scan_backups(root)?;
    let mut protected = std::collections::HashSet::new();
    for b in &verified {
        if b.expires_at.is_none_or(|t| t > now) {
            protected.extend(b.keys.iter().cloned());
        }
    }
    let enrolled = core.gc_enroll_candidates(now - retention_secs as i64, &protected)?;
    let (done, failed, skipped_protected) = core.gc_execute_deletions(1000, &protected)?;
    Ok(GcSummary {
        backups: verified.len(),
        protected_keys: protected.len(),
        enrolled,
        done,
        failed,
        skipped_protected,
    })
}

/// `lightfs gc` (P0-b): the ONLY production path to real object deletion.
/// Gate order is load-bearing: [backup] root configured → engine lock (via
/// the offline maintenance entry, BEFORE SQLite is opened) → maintenance
/// lock → every manifest verified fail-closed → protected keys filtered →
/// durable deletion tasks. The lock covers the whole window, so a backup
/// publishing concurrently is structurally excluded.
///
/// Retention OFF (`superseded_secs = 0`) does not gate gc: retirement then
/// registers durable deletion tasks (no superseded window), and gc is the
/// only thing that drains them. `retention_secs` only shapes the candidate
/// cutoff (0 → candidates qualify as soon as they are registered).
pub fn gc(cfg: &Config) -> Result<()> {
    // Offline maintenance (S4.14): the engine lock is acquired BEFORE the
    // metadata store is opened, and held for the whole process. Against a
    // running server this fails at the lock and never touches the database.
    let eng = engine::open_maintenance(cfg)?;
    // We own the lock, so the stale-pack sweep runs unconditionally — its
    // intents are drained by THIS gc run's enroll/execute pass below.
    let sweep = eng.core.recover_stale_packs()?;
    println!(
        "gc: stale packs: {} committing finalized, {} building row(s) dropped, \
         {} stale upload(s) enrolled, {} head unavailable (kept)",
        sweep.finalized_committing,
        sweep.dropped_building,
        sweep.enrolled_stale,
        sweep.head_unavailable,
    );
    let summary = gc_run(
        &eng.core,
        cfg.backup.root.as_deref(),
        cfg.backup.lock_timeout_secs,
        cfg.retention.superseded_secs,
        now_secs(),
    )?;
    println!(
        "gc: {} backup(s) verified, {} protected key(s), enrolled {} pack(s) + {} object(s), \
         deleted {}, failed {} (kept for retry), skipped-protected {}",
        summary.backups,
        summary.protected_keys,
        summary.enrolled.0,
        summary.enrolled.1,
        summary.done,
        summary.failed,
        summary.skipped_protected,
    );
    Ok(())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// `lightfs create-bucket`: one-time bootstrap for a fresh bucket. Real
/// production buckets are provisioned by ops; this exists for local setups.
/// `--insecure-public-rw` installs an anonymous read+WRITE policy
/// (Principal `*`, GetObject+PutObject) — ONLY for throwaway benchmark
/// buckets (the comparison harness needs unauthenticated uploads). Never
/// on a bucket that holds, or will hold, real data.
pub fn create_bucket(cfg: &Config, public: bool) -> Result<()> {
    let store = crate::object_store::s3::S3ObjectStore::new(cfg.s3_config())?;
    store.ensure_bucket()?;
    if public {
        eprintln!(
            "WARNING: --insecure-public-rw grants ANONYMOUS read+write (Principal *) on bucket '{}'. \
             Never use this on a bucket holding real data.",
            cfg.s3.bucket
        );
        store.ensure_public_read()?;
        println!(
            "bucket ready (INSECURE anonymous read+write): {}",
            cfg.s3.bucket
        );
    } else {
        println!("bucket ready: {}", cfg.s3.bucket);
    }
    Ok(())
}

/// `lightfs fsck`: report metadata/object divergence; `--fix` adopts the
/// data plane's state into metadata (never deletes anything).
pub fn fsck(cfg: &Config, fix_problems: bool) -> Result<()> {
    // Offline maintenance (S4.14): engine lock before SQLite, same contract
    // as `gc` — a running server makes this fail at the lock.
    let eng = engine::open_maintenance(cfg)?;
    let report = crate::fsck::fsck(&eng.core)?;
    print!("{}", report.render());
    if fix_problems {
        let adopted = crate::fsck::fix(&eng.core, &report)?;
        println!("fixed: adopted {adopted} externally modified object(s) into metadata");
    }
    if report.problems() > 0 && !fix_problems {
        std::process::exit(2);
    }
    Ok(())
}

/// `lightfs mount`: FUSE adapter (Linux only). Mounting always goes through
/// the network client — the metadata authority is the `lightfs server`
/// process, never this one (S4.14: no second production execution path).
#[cfg(all(feature = "fuse", target_os = "linux"))]
pub fn mount(cfg: &Config, mountpoint: &str, allow_other: bool) -> Result<()> {
    let fs: std::sync::Arc<dyn crate::api::FsApi> =
        std::sync::Arc::new(crate::client::Client::new(crate::client::ClientConfig {
            url: format!("http://{}", cfg.server.bind),
            metadata_ttl: cache_ttl(cfg),
        }));
    // Path policy (Phase P): route by rule, or pass the backend through
    // untouched when no client-local rule is configured.
    let fs: std::sync::Arc<dyn crate::api::FsApi> = match crate::routing::RoutingFs::build(
        fs.clone(),
        cfg.policy.as_ref(),
        std::path::Path::new(mountpoint),
    )? {
        Some(routed) => std::sync::Arc::new(routed),
        None => fs,
    };
    crate::fuse::mount(fs, mountpoint, allow_other)
}

/// `lightfs mount` on Windows: WinFsp adapter. The backend selection is the
/// same as Linux; `allow_other` has no Windows equivalent (access follows
/// the mounting user via -o uid/gid=-1).
#[cfg(all(feature = "fuse", target_os = "windows"))]
pub fn mount(cfg: &Config, mountpoint: &str, _allow_other: bool) -> Result<()> {
    let fs: std::sync::Arc<dyn crate::api::FsApi> =
        std::sync::Arc::new(crate::client::Client::new(crate::client::ClientConfig {
            url: format!("http://{}", cfg.server.bind),
            metadata_ttl: cache_ttl(cfg),
        }));
    let fs: std::sync::Arc<dyn crate::api::FsApi> = match crate::routing::RoutingFs::build(
        fs.clone(),
        cfg.policy.as_ref(),
        std::path::Path::new(mountpoint),
    )? {
        Some(routed) => std::sync::Arc::new(routed),
        None => fs,
    };
    crate::winfsp::mount(fs, mountpoint)
}

/// `lightfs mount` on platforms without a kernel FUSE integration here.
#[cfg(not(any(
    all(feature = "fuse", target_os = "linux"),
    all(feature = "fuse", target_os = "windows")
)))]
pub fn mount(_cfg: &Config, _mountpoint: &str, _allow_other: bool) -> Result<()> {
    Err(Error::Config(
        "FUSE mounting requires a Linux host with /dev/fuse (run \
         scripts/test-fuse.sh there) or Windows with WinFsp installed"
            .to_string(),
    ))
}

#[allow(dead_code)]
fn cache_ttl(cfg: &Config) -> Option<std::time::Duration> {
    if cfg.cache.metadata.enabled {
        Some(std::time::Duration::from_secs(
            cfg.cache.metadata.ttl_seconds,
        ))
    } else {
        None
    }
}
