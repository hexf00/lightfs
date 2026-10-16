//! `lightfs-shell` — the shell adapter over the NETWORK CLIENT.
//!
//! lightfs-shell [--config PATH] --server URL script tests/basic.lfs
//! lightfs-shell [--config PATH] --server URL ls /docs
//!
//! There is no local-engine mode (S4.14.1): the metadata authority is
//! always a `lightfs server` process, and this binary is only a client.

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
    let mut server: Option<String> = None;
    let mut rest = Vec::new();
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" | "-c" => {
                config_path = Some(it.next().unwrap_or_else(|| die_usage()));
            }
            "--server" => {
                server = Some(it.next().unwrap_or_else(|| die_usage()));
            }
            _ => rest.push(a),
        }
    }

    if rest.is_empty() || server.is_none() {
        die_usage();
    }
    let url = server.unwrap();

    let cfg = Config::load_or_defaults(config_path.as_deref())?;
    lightfs::client::Client::warn_if_data_cache_enabled(&cfg);
    let ttl = if cfg.cache.metadata.enabled {
        Some(std::time::Duration::from_secs(
            cfg.cache.metadata.ttl_seconds,
        ))
    } else {
        None
    };
    let fs: Arc<dyn FsApi> = Arc::new(lightfs::client::Client::new(
        lightfs::client::ClientConfig {
            url,
            metadata_ttl: ttl,
        },
    ));

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
        "usage: lightfs-shell [--config PATH] --server URL <ls|stat|mkdir|touch|cat|write|mv|rm|tree> ARGS...\n\
         usage: lightfs-shell [--config PATH] --server URL script tests/basic.lfs"
    );
    std::process::exit(2);
}
