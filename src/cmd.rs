//! `lightfs` subcommand implementations live here so the library (and tests)
//! can drive the same code paths the binary uses.

use crate::config::Config;
use crate::engine;
use crate::error::{Error, Result};

/// `lightfs server`: own the metadata authority and serve the client
/// protocol. Clients never touch the metadata DB directly.
pub fn server(cfg: &Config) -> Result<()> {
    let eng = engine::open_embedded(cfg)?;
    crate::server::serve(eng, &cfg.server.bind)
}

/// `lightfs backup`: consistent point-in-time snapshot of the SQLite
/// metadata (VACUUM INTO works on a live WAL database and yields a compact
/// standalone file). Object bytes are NOT in the backup — they live in the
/// bucket. With retention enabled ([retention] superseded_secs > 0),
/// replaced/deleted objects stay in the bucket for the retention window, so
/// restoring this snapshot recovers point-in-time CONTENT. Retention
/// disabled = metadata-only restore (overwritten/deleted content is gone).
pub fn backup(cfg: &Config, output: &str) -> Result<()> {
    use std::path::Path;
    if Path::new(output).exists() {
        return Err(Error::Config(format!(
            "backup target {output} already exists (refusing to overwrite)"
        )));
    }
    ensure_parent_dir_pub(output);
    let conn = rusqlite::Connection::open(&cfg.metadata.path)
        .map_err(|e| Error::Config(format!("cannot open metadata {}: {e}", cfg.metadata.path)))?;
    conn.execute_batch(&format!("VACUUM INTO {}", sqlite_quote(output)))
        .map_err(|e| Error::Config(format!("backup failed: {e}")))?;

    // Report the recovery guarantee of THIS snapshot from its own tables.
    let nodes: i64 = conn.query_row("SELECT COUNT(*) FROM nodes", [], |r| r.get(0))?;
    let (superseded, oldest): (i64, Option<i64>) = conn
        .query_row(
            "SELECT COUNT(*), MIN(superseded_at) FROM superseded_objects",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap_or((0, None));
    println!("metadata backup written: {output}");
    println!("  nodes: {nodes}; retained superseded objects: {superseded}");
    if cfg.retention.superseded_secs == 0 {
        eprintln!(
            "  WARNING: retention disabled (retention.superseded_secs = 0). This backup \
             restores NAMES ONLY — content overwritten or deleted after it is unrecoverable."
        );
    } else {
        let age = oldest
            .map(|t| now_secs() - t)
            .unwrap_or(0)
            .clamp(0, cfg.retention.superseded_secs as i64);
        println!(
            "  content restorable while objects are retained (superseded_secs = {}); \
             oldest retained object is {age}s old",
            cfg.retention.superseded_secs
        );
    }
    Ok(())
}

/// `lightfs gc`: purge retained (superseded) objects whose retention window
/// has passed and that no live node references anymore. The live-reference
/// guard makes this safe against crash windows between the supersede record
/// and the metadata update.
pub fn gc(cfg: &Config) -> Result<()> {
    if cfg.retention.superseded_secs == 0 {
        return Err(Error::Config(
            "retention.superseded_secs = 0: retention is disabled, there is nothing to gc".into(),
        ));
    }
    let eng = engine::open_embedded(cfg)?;
    let cutoff = now_secs() - cfg.retention.superseded_secs as i64;
    let mut purged = 0u64;
    let mut failed = 0u64;
    loop {
        let keys = eng.core.metadata().gc_candidates(cutoff, 100)?;
        if keys.is_empty() {
            break;
        }
        for key in keys {
            match eng.core.objects().delete(&key) {
                Ok(()) => purged += 1,
                Err(_) => {
                    failed += 1;
                    continue; // keep the row; next gc retries the object
                }
            }
            eng.core.metadata().drop_superseded(&key)?;
        }
    }
    let retained = eng.core.metadata().all_superseded()?.len();
    println!(
        "gc: purged {purged} object(s), {failed} failed (kept for retry), {retained} still retained (superseded_secs = {})",
        cfg.retention.superseded_secs
    );
    Ok(())
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Single-quoted SQL string literal.
fn sqlite_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Public alias so cmd.rs does not reach into engine internals.
fn ensure_parent_dir_pub(path: &str) {
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            let _ = std::fs::create_dir_all(parent);
        }
    }
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
    let eng = engine::open_embedded(cfg)?;
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

/// `lightfs mount`: FUSE adapter (Linux only). The backend follows the
/// config mode: embedded opens Core+SQLite+S3 in this process; server mode
/// mounts through the network client.
#[cfg(all(feature = "fuse", target_os = "linux"))]
pub fn mount(cfg: &Config, mountpoint: &str, allow_other: bool) -> Result<()> {
    let fs: std::sync::Arc<dyn crate::api::FsApi> = if cfg.mode == "server" {
        std::sync::Arc::new(crate::client::Client::new(crate::client::ClientConfig {
            url: format!("http://{}", cfg.server.bind),
            metadata_ttl: cache_ttl(cfg),
        }))
    } else {
        engine::open_embedded(cfg)?.core
    };
    crate::fuse::mount(fs, mountpoint, allow_other)
}

/// `lightfs mount` on Windows: WinFsp adapter. The backend selection is the
/// same as Linux; `allow_other` has no Windows equivalent (access follows
/// the mounting user via -o uid/gid=-1).
#[cfg(all(feature = "fuse", target_os = "windows"))]
pub fn mount(cfg: &Config, mountpoint: &str, _allow_other: bool) -> Result<()> {
    let fs: std::sync::Arc<dyn crate::api::FsApi> = if cfg.mode == "server" {
        std::sync::Arc::new(crate::client::Client::new(crate::client::ClientConfig {
            url: format!("http://{}", cfg.server.bind),
            metadata_ttl: cache_ttl(cfg),
        }))
    } else {
        engine::open_embedded(cfg)?.core
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

/// `lightfs embedded`: open the embedded engine, verify the object store is
/// reachable, and stay alive until interrupted. The primary Agent/CI loop
/// uses the in-process engine through lightfs-shell; this daemon form exists
/// for interactive inspection.
pub fn embedded(cfg: &Config) -> Result<()> {
    let _eng = engine::open_embedded(cfg)?;
    println!(
        "lightfs embedded ready: mode=embedded db={} s3={}/{}",
        cfg.metadata.path, cfg.s3.endpoint, cfg.s3.bucket
    );
    // Block until SIGINT/SIGTERM. std has no signal API; parking forever is
    // enough for v1 — the OS tears the process down.
    loop {
        std::thread::park();
    }
}
