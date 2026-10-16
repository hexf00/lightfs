//! `lightfs` — single binary entry point.
//!
//! lightfs server   [--config PATH]   own the metadata + serve the protocol
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
        "usage: lightfs [--config PATH] <server|fsck|mount|backup [--expires-in SECS]|backup prune --id ID|gc|create-bucket [--insecure-public-rw]|version>\n\
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
            // `lightfs backup prune --id ID` — the only sanctioned way to
            // remove a backup (incl. resolving a fail-closed corrupt one).
            if rest.get(1).map(String::as_str) == Some("prune") {
                let mut id: Option<&str> = None;
                let mut it = rest[2..].iter();
                while let Some(a) = it.next() {
                    match a.as_str() {
                        "--id" => {
                            id = Some(it.next().ok_or_else(|| {
                                lightfs::error::Error::Config(
                                    "backup prune: --id needs a value".into(),
                                )
                            })?);
                        }
                        other => {
                            return Err(lightfs::error::Error::Config(format!(
                                "backup prune: unknown argument '{other}' (usage: backup prune --id ID)"
                            )));
                        }
                    }
                }
                let Some(id) = id else {
                    return Err(lightfs::error::Error::Config(
                        "backup prune needs --id ID".into(),
                    ));
                };
                return lightfs::cmd::backup_prune(&cfg, id);
            }
            // Legacy single-file form (`lightfs backup OUTPUT`) is gone:
            // a bare file can't carry a manifest, so gc could never prove
            // what it references. The canonical root is the only backup.
            let mut expires_in: Option<u64> = None;
            let mut it = rest[1..].iter();
            while let Some(a) = it.next() {
                match a.as_str() {
                    "--expires-in" => {
                        let v = it.next().ok_or_else(|| {
                            lightfs::error::Error::Config("--expires-in needs a value".into())
                        })?;
                        expires_in = Some(v.parse().map_err(|_| {
                            lightfs::error::Error::Config(format!(
                                "--expires-in: expected a non-negative integer, got '{v}'"
                            ))
                        })?);
                    }
                    other => {
                        return Err(lightfs::error::Error::Config(format!(
                            "backup: unknown argument '{other}': backups publish as directories \
                             under the canonical [backup] root; a positional OUTPUT path (legacy \
                             single-file backup) is not supported and not auto-protected"
                        )));
                    }
                }
            }
            lightfs::cmd::backup(&cfg, expires_in)
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
