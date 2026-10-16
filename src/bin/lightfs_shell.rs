//! `lightfs-shell` — the shell adapter binary.
//!
//! lightfs-shell [--config PATH] [--embedded] [--mem] script tests/basic.lfs
//! lightfs-shell [--embedded|--mem] ls /docs
//! lightfs-shell [--server URL] ...   (Phase 7: network client backend)

use std::sync::Arc;

use lightfs::api::FsApi;
use lightfs::config::Config;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(args) {
        eprintln!("lightfs-shell: {e}");
        std::process::exit(1);
    }
}

fn run(args: Vec<String>) -> Result<(), lightfs::error::Error> {
    let mut config_path: Option<String> = None;
    let mut embedded = false;
    let mut mem = false;
    let mut server: Option<String> = None;
    let mut rest = Vec::new();
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" | "-c" => {
                config_path = Some(it.next().unwrap_or_else(|| die_usage()));
            }
            "--embedded" => embedded = true,
            "--mem" => mem = true,
            "--server" => {
                server = Some(it.next().unwrap_or_else(|| die_usage()));
            }
            _ => rest.push(a),
        }
    }

    if rest.is_empty() {
        die_usage();
    }

    let cfg_for_warn = Config::load_or_defaults(config_path.as_deref()).ok();
    if let Some(c) = &cfg_for_warn {
        lightfs::client::Client::warn_if_data_cache_enabled(c);
    }

    let fs: Arc<dyn FsApi> = if let Some(url) = server {
        let cfg = Config::load_or_defaults(config_path.as_deref())?;
        let ttl = if cfg.cache.metadata.enabled {
            Some(std::time::Duration::from_secs(
                cfg.cache.metadata.ttl_seconds,
            ))
        } else {
            None
        };
        Arc::new(lightfs::client::Client::new(
            lightfs::client::ClientConfig {
                url,
                metadata_ttl: ttl,
            },
        ))
    } else {
        // Test-driver default: a local engine in THIS process (`--mem` for
        // the network-free object store). lightfs-shell is not a production
        // entry point — the metadata authority is always `lightfs server`.
        let cfg = Config::load_or_defaults(config_path.as_deref())?;
        let engine = if mem {
            lightfs::engine::open_engine_mem(&cfg)?
        } else {
            lightfs::engine::open_engine(&cfg)?
        };
        engine.core as Arc<dyn FsApi>
    };

    let _ = embedded;
    let shell = lightfs::shell::Shell::new(fs);

    if rest[0] == "script" {
        let script = rest.get(1).ok_or_else(|| {
            lightfs::error::Error::InvalidArgument("script needs a .lfs file".into())
        })?;
        let n = shell.run_script(script)?;
        println!("[script] {n} commands executed: {script}");
        return Ok(());
    }

    let out = shell.exec(&rest.join(" "))?;
    for line in out {
        println!("{line}");
    }
    Ok(())
}

fn die_usage() -> ! {
    eprintln!(
        "usage: lightfs-shell [--config PATH] [--embedded] [--mem] <ls|stat|mkdir|touch|cat|write|mv|rm|tree> ARGS...\n\
         usage: lightfs-shell [--config PATH] script tests/basic.lfs"
    );
    std::process::exit(2);
}
