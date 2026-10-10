//! `lightfs` — single binary entry point.
//!
//! lightfs embedded [--config PATH]   run embedded engine (Core+SQLite+S3)
//! lightfs server   [--config PATH]   serve the client protocol (Phase 6)
//! lightfs fsck     [--config PATH]   report metadata/object divergence
//! lightfs mount    [--config PATH] MOUNTPOINT   FUSE (Linux only, Phase 8)
//! lightfs version

use lightfs::config::Config;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let Err(e) = run(args) {
        eprintln!("lightfs: {e}");
        std::process::exit(1);
    }
}

fn print_usage() {
    eprintln!(
        "usage: lightfs [--config PATH] <embedded|server|fsck|mount|backup OUTPUT|gc|create-bucket [--insecure-public-rw]|version>\n\
         see README"
    );
}

fn run(args: Vec<String>) -> Result<(), lightfs::error::Error> {
    let mut config_path: Option<String> = None;
    let mut rest = Vec::new();
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--config" | "-c" => {
                config_path = Some(it.next().unwrap_or_else(|| {
                    print_usage();
                    std::process::exit(2);
                }));
            }
            _ => rest.push(a),
        }
    }

    match rest.first().map(String::as_str) {
        Some("version") => {
            println!("lightfs {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("backup") => {
            let cfg = Config::load_or_defaults(config_path.as_deref())?;
            let output = rest[1..].iter().find(|a| !a.starts_with("--"));
            let Some(output) = output else {
                return Err(lightfs::error::Error::Config(
                    "backup needs an OUTPUT path argument".into(),
                ));
            };
            lightfs::cmd::backup(&cfg, output)
        }
        Some("create-bucket") => {
            let cfg = Config::load_or_defaults(config_path.as_deref())?;
            let public = rest.iter().any(|a| a == "--insecure-public-rw");
            if rest.iter().any(|a| a == "--public") {
                return Err(lightfs::error::Error::Config(
                    "--public was removed: it granted ANONYMOUS read+write. Use \
                     --insecure-public-rw only for throwaway benchmark buckets."
                        .into(),
                ));
            }
            lightfs::cmd::create_bucket(&cfg, public)
        }
        Some("gc") => {
            let cfg = Config::load_or_defaults(config_path.as_deref())?;
            lightfs::cmd::gc(&cfg)
        }
        Some("fsck") => {
            let cfg = Config::load_or_defaults(config_path.as_deref())?;
            let fix = rest.iter().any(|a| a == "--fix");
            lightfs::cmd::fsck(&cfg, fix)
        }
        Some("server") => {
            let cfg = Config::load_or_defaults(config_path.as_deref())?;
            lightfs::cmd::server(&cfg)
        }
        Some("mount") => {
            let cfg = Config::load_or_defaults(config_path.as_deref())?;
            let allow_other = rest.iter().any(|a| a == "--allow-other");
            let mountpoint = rest[1..].iter().find(|a| !a.starts_with("--"));
            let Some(mountpoint) = mountpoint else {
                return Err(lightfs::error::Error::Config(
                    "mount needs a MOUNTPOINT argument".into(),
                ));
            };
            lightfs::cmd::mount(&cfg, mountpoint, allow_other)
        }
        Some("embedded") => {
            let cfg = Config::load_or_defaults(config_path.as_deref())?;
            lightfs::cmd::embedded(&cfg)
        }
        None => {
            print_usage();
            Ok(())
        }
        Some(other) => {
            print_usage();
            Err(lightfs::error::Error::Config(format!(
                "unknown command: {other}"
            )))
        }
    }
}
