//! HTTP transport for the RPC protocol (tiny_http, blocking workers).
//! The server owns the Filesystem Core; multiple clients share it. Workers
//! are a fixed pool — concurrency is bounded by design (TECH_STACK.md).

use std::io::Read;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;

use crate::core::Core;
use crate::engine::Engine;
use crate::error::{Error, Result};

const MAX_BODY: usize = 256 * 1024 * 1024; // 256 MiB writes in one RPC
// Worker pool size; LIGHTFS_SERVER_WORKERS overrides (concurrency review:
// saturation point is a measured quantity, not a constant).
fn worker_count() -> usize {
    std::env::var("LIGHTFS_SERVER_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| (1..=512).contains(&n))
        .unwrap_or(8)
}

/// Remove staging files orphaned by a previous SIGKILLed server process.
/// RAII cleanup covers every graceful path; this startup sweep covers the
/// one it cannot (files older than 1h can no longer be an in-flight upload:
/// the client HTTP timeout is 300s). Removal failures are logged.
fn sweep_stale_staging() {
    let cutoff = std::time::SystemTime::now() - std::time::Duration::from_secs(3600);
    let mut removed = 0usize;
    if let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !name.starts_with("lightfs-up-") {
                continue;
            }
            let stale = entry
                .metadata()
                .and_then(|m| m.modified())
                .map(|t| t < cutoff)
                .unwrap_or(false);
            if !stale {
                continue;
            }
            match std::fs::remove_file(entry.path()) {
                Ok(()) => removed += 1,
                Err(e) => eprintln!(
                    "lightfs: WARNING failed to remove stale staging file {}: {e}",
                    entry.path().display()
                ),
            }
        }
    }
    if removed > 0 {
        eprintln!("lightfs: removed {removed} stale staging file(s) from previous runs");
    }
}

pub fn serve(engine: Engine, bind: &str) -> Result<()> {
    sweep_stale_staging();
    let server = tiny_http::Server::http(bind)
        .map_err(|e| Error::Config(format!("cannot bind {bind}: {e}")))?;
    eprintln!("lightfs server listening on {bind}");
    serve_on(server, engine)
}

/// Run the accept loop on an existing server (tests can bind port 0 first).
pub fn serve_on(server: tiny_http::Server, engine: Engine) -> Result<()> {
    crate::metrics::init_from_env();
    let server = Arc::new(server);
    let core = engine.core.clone();
    let workers = worker_count();
    eprintln!("lightfs server workers: {workers}");
    let mut joins = Vec::new();
    for _ in 0..workers {
        let server = server.clone();
        let core = core.clone();
        joins.push(std::thread::spawn(move || {
            // server closed (recv error) ends the loop
            while let Ok(req) = server.recv() {
                handle_request(req, &core);
            }
        }));
    }
    for j in joins {
        let _ = j.join();
    }
    Ok(())
}

fn handle_request(mut req: tiny_http::Request, core: &Core) {
    let url = req.url().split('?').next().unwrap_or("/").to_string();
    match req.method() {
        tiny_http::Method::Get if url == "/health" => {
            let resp = tiny_http::Response::from_string("ok\n");
            let _ = req.respond(resp);
        }
        tiny_http::Method::Get if url == "/metrics" => {
            let resp = tiny_http::Response::from_string(crate::metrics::dump());
            let _ = req.respond(resp);
        }
        tiny_http::Method::Post if url == "/rpc" => {
            let _g = crate::metrics::scope("srv.rpc.total");
            let mut body = Vec::new();
            let too_large = req
                .as_reader()
                .take((MAX_BODY + 1) as u64)
                .read_to_end(&mut body)
                .map(|n| n > MAX_BODY)
                .unwrap_or(true);
            let (_id, json) = if too_large {
                let resp =
                    crate::rpc::RpcResponse::Err(Error::InvalidArgument("body too large".into()));
                (
                    serde_json::Value::Null,
                    resp.to_json(&serde_json::Value::Null),
                )
            } else {
                crate::rpc::handle_body(core, &body)
            };
            let header =
                tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..])
                    .unwrap();
            let resp = tiny_http::Response::from_string(json).with_header(header);
            let _ = req.respond(resp);
        }
        // Binary data endpoints: file bytes travel raw (no base64, no JSON
        // envelope around the payload). Metadata stays on /rpc.
        tiny_http::Method::Post if url == "/data/read" => {
            let _g = crate::metrics::scope("srv.data.read");
            let mut body = Vec::new();
            let too_large = req
                .as_reader()
                .take((64 * 1024 + 1) as u64)
                .read_to_end(&mut body)
                .map(|n| n > 64 * 1024)
                .unwrap_or(true);
            let params: serde_json::Value = if too_large {
                serde_json::Value::Null
            } else {
                serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null)
            };
            let parsed = (|| -> Result<(String, u64, u64)> {
                let path = params["path"]
                    .as_str()
                    .ok_or_else(|| Error::InvalidArgument("path".into()))?;
                let offset = params["offset"]
                    .as_u64()
                    .ok_or_else(|| Error::InvalidArgument("offset".into()))?;
                let size = params["size"]
                    .as_u64()
                    .ok_or_else(|| Error::InvalidArgument("size".into()))?;
                Ok((path.to_string(), offset, size))
            })();
            match parsed.and_then(|(path, offset, size)| core.open_read(&path, offset, size)) {
                Ok((reader, len)) => {
                    let header = tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"application/octet-stream"[..],
                    )
                    .unwrap();
                    // Large responses go chunked (tiny_http threshold), so
                    // the exact length rides in an explicit header.
                    let len_header = tiny_http::Header::from_bytes(
                        &b"X-LightFS-Length"[..],
                        len.to_string().as_bytes(),
                    )
                    .unwrap();
                    let resp = tiny_http::Response::new(
                        tiny_http::StatusCode(200),
                        vec![header, len_header],
                        reader,
                        Some(len as usize),
                        None,
                    );
                    let _ = req.respond(resp);
                }
                Err(e) => respond_error(req, &e),
            }
        }
        tiny_http::Method::Post if url == "/data/write" => {
            let _g = crate::metrics::scope("srv.data.write");

            let path = req
                .headers()
                .iter()
                .find(|h| h.field.equiv("X-LightFS-Path"))
                .and_then(|h| percent_decode(h.value.as_str()));
            let expected_version = req
                .headers()
                .iter()
                .find(|h| h.field.equiv("X-LightFS-Expected-Version"))
                .and_then(|h| h.value.as_str().parse::<i64>().ok());
            let create = req
                .headers()
                .iter()
                .find(|h| h.field.equiv("X-LightFS-Create"))
                .map(|h| h.value.as_str() != "0")
                .unwrap_or(true);
            let Some(path) = path else {
                return respond_error(req, &Error::InvalidArgument("X-LightFS-Path header".into()));
            };
            // The declared length is mandatory: without it we could not tell
            // a truncated body from a complete one before committing.
            let Some(len) = req
                .headers()
                .iter()
                .find(|h| h.field.equiv("X-LightFS-Length"))
                .and_then(|h| h.value.as_str().parse::<u64>().ok())
            else {
                return respond_error(
                    req,
                    &Error::InvalidArgument("X-LightFS-Length header required".into()),
                );
            };
            // Reject an over-limit declaration BEFORE touching the disk: a
            // client must not be able to spend our disk space on a body we
            // already know we will refuse. (An undeclared oversized body is
            // still bounded: the copy below reads at most MAX_BODY + 1 bytes
            // and then fails.)
            if len > MAX_BODY as u64 {
                return respond_error(
                    req,
                    &Error::InvalidArgument(format!(
                        "declared length {len} exceeds limit {MAX_BODY}"
                    )),
                );
            }
            // Bounded-memory proxy: stage the request body to a temp file
            // (disk, not RAM), then PUT with a real Content-Length — S3
            // rejects chunked unsigned PUTs (411). True reader-to-reader
            // streaming onto S3 needs multipart upload (documented
            // follow-up).
            let outcome = (|| -> Result<crate::metadata::Node> {
                // Exclusive creation (atomic, no symlink/target collision),
                // owner-only permissions: this file holds user data.
                let mut stage = None;
                for _ in 0..8 {
                    let p = std::env::temp_dir().join(format!(
                        "lightfs-up-{}-{}",
                        std::process::id(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos()
                    ));
                    let mut f = std::fs::OpenOptions::new();
                    f.read(true).write(true).create_new(true);
                    // Owner-only staging file; the mode(2) call is unix-only.
                    #[cfg(unix)]
                    f.mode(0o600);
                    let f = f.open(&p);
                    match f {
                        Ok(file) => {
                            stage = Some((p, file));
                            break;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                        Err(e) => return Err(e.into()),
                    }
                }
                let Some((stage_path, staged)) = stage else {
                    return Err(Error::Io("could not create staging file".into()));
                };
                // RAII cleanup: the staging file is removed on every exit
                // path, success, validation failure, or error.
                struct StageGuard(std::path::PathBuf);
                impl Drop for StageGuard {
                    fn drop(&mut self) {
                        if let Err(e) = std::fs::remove_file(&self.0) {
                            // The staging file contains user data; a failed
                            // cleanup must be visible, not silent.
                            eprintln!(
                                "lightfs: WARNING failed to remove staging file {}: {e}",
                                self.0.display()
                            );
                        }
                    }
                }
                let _guard = StageGuard(stage_path);

                // Receive and VALIDATE the body fully BEFORE anything is
                // committed: a length mismatch must never leave a committed
                // write behind (review P1).
                // Large buffered reads: tiny_http hands out small chunks;
                // a big BufReader keeps io::copy off the 8KiB default path.
                let body = std::io::BufReader::with_capacity(
                    512 * 1024,
                    req.as_reader().take(MAX_BODY as u64 + 1),
                );
                let mut body_reader = body;
                let mut staged_writer = std::io::BufWriter::with_capacity(512 * 1024, &staged);
                let written = std::io::copy(&mut body_reader, &mut staged_writer)?;
                use std::io::Write as _;
                staged_writer.flush()?;
                drop(staged_writer);
                drop(staged);
                if written > MAX_BODY as u64 {
                    return Err(Error::InvalidArgument("body too large".into()));
                }
                if written != len {
                    return Err(Error::InvalidArgument(format!(
                        "declared length {len} != body length {written}"
                    )));
                }
                // Validation passed — only now touch the object store and
                // the metadata CAS.
                let mut f = std::fs::OpenOptions::new().read(true).open(&_guard.0)?;
                core.write_stream(&path, expected_version, create, &mut f, written)
            })();
            match outcome {
                Ok(node) => {
                    let json = crate::rpc::node_to_json(&node).to_string();
                    let header = tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"application/json"[..],
                    )
                    .unwrap();
                    let _ = req.respond(tiny_http::Response::from_string(json).with_header(header));
                }
                Err(e) => respond_error(req, &e),
            }
        }
        _ => {
            let resp = tiny_http::Response::from_string("not found\n").with_status_code(404);
            let _ = req.respond(resp);
        }
    }
}

fn respond_error(req: tiny_http::Request, e: &Error) {
    let body = serde_json::json!({
        "error": { "code": e.code(), "message": e.to_string() }
    })
    .to_string();
    let status = match e.code() {
        "NOT_FOUND" => 404,
        "ALREADY_EXISTS" => 409,
        "CONFLICT" => 409,
        "INVALID" => 400,
        _ => 500,
    };
    let header =
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"application/json"[..]).unwrap();
    let resp = tiny_http::Response::from_string(body)
        .with_status_code(status)
        .with_header(header);
    let _ = req.respond(resp);
}

fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok()?;
            let byte = u8::from_str_radix(hex, 16).ok()?;
            out.push(byte);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}
