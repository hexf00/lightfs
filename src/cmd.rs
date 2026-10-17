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
