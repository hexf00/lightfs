//! S3 latency probe (Phase D1 diagnostics). Measures single-request latency
//! of the EXACT runtime S3 client path and controlled variants, with
//! nanosecond TSV output for offline percentile analysis. The 16-bucket
//! runtime histograms cannot answer "why is one PUT 20 ms", so this tool
//! exists; it is NOT part of the product and never runs in the server.
//!
//! Arms:
//!   put-store   - S3ObjectStore::put()               (runtime path, shared pool)
//!   put-stream  - S3ObjectStore::put_stream()+Cursor (runtime commit path incl. permit)
//!   put-shared  - probe-drawn request + ONE shared Agent (same config as runtime)
//!   put-fresh   - probe-drawn request + a NEW Agent per request (new TCP each)
//!   get-store / get-shared / get-fresh - same split on GET (body read to vec)
//!   connect     - raw TCP connect/close to endpoint host (no HTTP)
//!   dns         - hostname resolution only
//!   sign        - SigV4 signing only, no network
//!
//! put-store vs put-shared cross-validates the probe-drawn request bytes
//! against the real client; put-shared vs put-fresh isolates connection
//! setup. Request construction (headers, addressing, agent config) mirrors
//! object_store/s3.rs exactly; a divergence would show up as a store/shared
//! gap, which the report would then have to explain.

use std::io::{Cursor, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use lightfs::config::Config;
use lightfs::error::Result;
use lightfs::object_store::ObjectStore;
use lightfs::object_store::s3::{S3Config, S3ObjectStore};
use lightfs::sigv4::{SigV4, uri_encode};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let get = |name: &str| -> String {
        let i = args.iter().position(|a| a == name).unwrap_or_else(|| {
            eprintln!("missing {name}");
            std::process::exit(2);
        });
        args.get(i + 1).cloned().unwrap_or_else(|| {
            eprintln!("missing value for {name}");
            std::process::exit(2);
        })
    };
    let cfg_path = get("--config");
    let arm = get("--arm");
    let count: usize = get("--count").parse().expect("count");
    let size: usize = get("--size").parse().unwrap_or(0);
    let prefix = get("--prefix");
    let key = args
        .iter()
        .position(|a| a == "--key")
        .map(|i| args[i + 1].clone());

    let cfg = Config::load(&cfg_path).expect("config load");
    let s3cfg: S3Config = cfg.s3_config();

    // Deterministic pseudo-random payload: never all-zeros, so no transport
    // layer can compress it; content does not otherwise matter (SigV4 hashes
    // it either way).
    let mut data = vec![0u8; size];
    let mut x: u64 = 0x9E3779B97F4A7C15;
    for b in data.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mk_key = |tag: &str, seq: usize| {
        key.clone()
            .unwrap_or_else(|| format!("{prefix}/{ts}/{tag}/{size}/{seq:05}"))
    };

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    writeln!(
        out,
        "# arm={arm} size={size} count={count} scheme={} path_style={} host={}",
        scheme_of(&s3cfg.endpoint),
        s3cfg.path_style,
        host_of(&s3cfg.endpoint)
    )
    .unwrap();

    match arm.as_str() {
        "put-store" => {
            let store = S3ObjectStore::new(s3cfg).expect("store");
            for seq in 0..count {
                let k = mk_key("store", seq);
                let t = Instant::now();
                let r = store.put(&k, &data);
                let ns = t.elapsed().as_nanos() as u64;
                ok_or_die(&r.map(|_| ()), "put-store", seq);
                writeln!(out, "put-store\t{size}\t{seq}\t{ns}\t200").unwrap();
            }
        }
        "put-stream" => {
            let store = S3ObjectStore::new(s3cfg).expect("store");
            for seq in 0..count {
                let k = mk_key("stream", seq);
                let mut reader = Cursor::new(data.clone());
                let t = Instant::now();
                let r = store.put_stream(&k, &mut reader, size as u64);
                let ns = t.elapsed().as_nanos() as u64;
                ok_or_die(&r.map(|_| ()), "put-stream", seq);
                writeln!(out, "put-stream\t{size}\t{seq}\t{ns}\t200").unwrap();
            }
        }
        "put-shared" | "put-fresh" => {
            let shared = arm == "put-shared";
            let signer = signer_of(&s3cfg);
            let host = vhost_of(&s3cfg);
            let base = base_path_of(&s3cfg);
            let url_base = url_base_of(&s3cfg);
            // shared: ONE agent (and thus one pooled connection) for the
            // whole arm, mirroring the runtime's process-wide agent;
            // fresh: a new agent per request = a new TCP connection each.
            let pooled = probe_agent();
            for seq in 0..count {
                let k = mk_key("pf", seq);
                let path = format!("{}/{}", base, uri_encode(&k, true));
                let url = format!("{}{}", url_base, path);
                let fresh = probe_agent();
                let a: &ureq::Agent = if shared { &pooled } else { &fresh };
                let t = Instant::now();
                let r = signed_put(a, &signer, &host, &path, &url, &data);
                let ns = t.elapsed().as_nanos() as u64;
                let status = r.unwrap_or_else(|e| die(&format!("put arm {arm} seq {seq}: {e}")));
                drop(fresh);
                let tag = if shared { "put-shared" } else { "put-fresh" };
                writeln!(out, "{tag}\t{size}\t{seq}\t{ns}\t{status}").unwrap();
            }
        }
        "get-store" => {
            let store = S3ObjectStore::new(s3cfg).expect("store");
            let k = key.expect("get-store needs --key");
            for seq in 0..count {
                let t = Instant::now();
                let r = store.get(&k, None);
                let ns = t.elapsed().as_nanos() as u64;
                ok_or_die(&r.map(|_| ()), "get-store", seq);
                writeln!(out, "get-store\t{size}\t{seq}\t{ns}\t200").unwrap();
            }
        }
        "get-shared" | "get-fresh" => {
            let signer = signer_of(&s3cfg);
            let host = vhost_of(&s3cfg);
            let base = base_path_of(&s3cfg);
            let url_base = url_base_of(&s3cfg);
            let k = key.expect("get arm needs --key");
            let path = format!("{}/{}", base, uri_encode(&k, true));
            let url = format!("{}{}", url_base, path);
            let pooled = probe_agent();
            for seq in 0..count {
                let fresh = probe_agent();
                let a: &ureq::Agent = if arm == "get-shared" { &pooled } else { &fresh };
                let t = Instant::now();
                let r = signed_get(a, &signer, &host, &path, &url);
                let ns = t.elapsed().as_nanos() as u64;
                let status = r.unwrap_or_else(|e| die(&format!("get arm seq {seq}: {e}")));
                drop(fresh);
                let tag = if arm == "get-shared" {
                    "get-shared"
                } else {
                    "get-fresh"
                };
                writeln!(out, "{tag}\t{size}\t{seq}\t{ns}\t{status}").unwrap();
            }
        }
        "connect" => {
            let host = host_of(&s3cfg.endpoint);
            let port = port_of(&s3cfg.endpoint);
            for seq in 0..count {
                let t = Instant::now();
                let r = TcpStream::connect((host.as_str(), port));
                let ns = t.elapsed().as_nanos() as u64;
                drop(r.unwrap_or_else(|e| die(&format!("connect seq {seq}: {e}"))));
                writeln!(out, "connect\t0\t{seq}\t{ns}\t0").unwrap();
            }
        }
        "dns" => {
            let host = host_of(&s3cfg.endpoint);
            let port = port_of(&s3cfg.endpoint);
            for seq in 0..count {
                let t = Instant::now();
                let r = (host.as_str(), port).to_socket_addrs();
                let ns = t.elapsed().as_nanos() as u64;
                r.unwrap_or_else(|e| die(&format!("dns seq {seq}: {e}")));
                writeln!(out, "dns\t0\t{seq}\t{ns}\t0").unwrap();
            }
        }
        "sign" => {
            let signer = signer_of(&s3cfg);
            let host = vhost_of(&s3cfg);
            let path = format!("{}/{}", base_path_of(&s3cfg), "sign-probe");
            for seq in 0..count {
                let t = Instant::now();
                let s = signer.sign("PUT", &host, &path, "", &data, ts);
                let ns = t.elapsed().as_nanos() as u64;
                std::hint::black_box(&s);
                writeln!(out, "sign\t{size}\t{seq}\t{ns}\t0").unwrap();
            }
        }
        "conc-store" | "conc-raw" => {
            // Phase E2: N workers issuing independent 2 KiB PUTs until `count`
            // requests are done. conc-store = the runtime S3ObjectStore shared
            // by all workers (its pool, its signing, its with_retry);
            // conc-raw = probe-drawn signed PUT over ONE shared Agent, NO
            // retry - every 503/SlowDown surfaces as a status code. The two
            // variants are separate arms and are never mixed in analysis.
            // Failures are recorded as rows with the error text; none are
            // dropped. Store-variant retries (503 retried inside with_retry to
            // success) are NOT individually observable - a retry shows up as a
            // latency tail (>=100ms backoff), which the percentiles expose.
            let n: usize = get("--concurrency").parse().expect("concurrency");
            let s3cfg2 = s3cfg.clone();
            let signer = Arc::new(signer_of(&s3cfg2));
            let host = Arc::new(vhost_of(&s3cfg2));
            let base = Arc::new(base_path_of(&s3cfg2));
            let url_base = Arc::new(url_base_of(&s3cfg2));
            let data = Arc::new(data);
            let next = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(Barrier::new(n));
            let store: Option<Arc<S3ObjectStore>> = if arm == "conc-store" {
                Some(Arc::new(S3ObjectStore::new(s3cfg2).expect("store")))
            } else {
                None
            };
            let agent: Option<Arc<ureq::Agent>> = if arm == "conc-raw" {
                Some(Arc::new(probe_agent()))
            } else {
                None
            };
            let cpu0 = proc_cpu_ticks();
            let wall0 = Instant::now();
            let mut handles = Vec::new();
            for _ in 0..n {
                let (next, data, barrier) = (next.clone(), data.clone(), barrier.clone());
                let (store, agent, signer) = (store.clone(), agent.clone(), signer.clone());
                let (host, base, url_base) = (host.clone(), base.clone(), url_base.clone());
                let prefix = prefix.clone();
                handles.push(std::thread::spawn(move || {
                    barrier.wait();
                    let mut rows: Vec<(usize, u64, u16, String)> = Vec::new();
                    loop {
                        let seq = next.fetch_add(1, Ordering::SeqCst);
                        if seq >= count {
                            break;
                        }
                        let payload = payload_for(seq, data.len());
                        let k = format!("{prefix}/{seq:06}");
                        let t = Instant::now();
                        if let Some(s) = &store {
                            match s.put(&k, &payload) {
                                Ok(_) => rows.push((
                                    seq,
                                    t.elapsed().as_nanos() as u64,
                                    200,
                                    String::new(),
                                )),
                                Err(e) => rows.push((
                                    seq,
                                    t.elapsed().as_nanos() as u64,
                                    0,
                                    truncate_err(&e.to_string()),
                                )),
                            }
                        } else if let Some(a) = &agent {
                            let path = format!("{}/{}", base, uri_encode(&k, true));
                            let url = format!("{}{}", url_base, path);
                            match signed_put(a, &signer, &host, &path, &url, &payload) {
                                Ok(st) => rows.push((
                                    seq,
                                    t.elapsed().as_nanos() as u64,
                                    st,
                                    String::new(),
                                )),
                                Err(e) => rows.push((
                                    seq,
                                    t.elapsed().as_nanos() as u64,
                                    0,
                                    truncate_err(&e),
                                )),
                            }
                        }
                    }
                    rows
                }));
            }
            let mut all: Vec<(usize, u64, u16, String)> = handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect();
            all.sort_by_key(|r| r.0);
            let wall_ms = wall0.elapsed().as_millis() as u64;
            let (du, ds) = proc_cpu_ticks();
            let ok = all.iter().filter(|r| (200..300).contains(&r.2)).count();
            let failed = all.len() - ok;
            for (seq, ns, st, err) in &all {
                writeln!(out, "{arm}\t{size}\t{seq}\t{ns}\t{st}\t{err}").unwrap();
            }
            // Content verification on a sample (every 13th ok request): GET
            // back and compare bytes against the deterministic payload.
            let mut verified = 0usize;
            let mut mismatches = 0usize;
            let va = probe_agent();
            for (seq, _, st, _) in all.iter().filter(|r| (200..300).contains(&r.2)) {
                if seq % 13 != 0 || verified + mismatches >= 60 {
                    continue;
                }
                let k = format!("{prefix}/{seq:06}");
                let path = format!("{}/{}", base, uri_encode(&k, true));
                let url = format!("{}{}", url_base, path);
                match signed_get_body(&va, &signer, &host, &path, &url) {
                    Ok(body) => {
                        if body == *payload_for(*seq, data.len()) {
                            verified += 1;
                        } else {
                            mismatches += 1;
                            eprintln!("s3probe: VERIFY MISMATCH seq {seq} len {}", body.len());
                        }
                    }
                    Err(e) => {
                        mismatches += 1;
                        eprintln!("s3probe: VERIFY FAILED seq {seq}: {e}");
                    }
                }
                let _ = st;
            }
            writeln!(
                out,
                "# conc-summary arm={arm} concurrency={n} requested={count} done={} failed={failed} wall_ms={wall_ms} rps={:.1} cpu_ticks(u+s)={} verified={verified} verify_mismatch={mismatches}",
                all.len(),
                all.len() as f64 / (wall_ms as f64 / 1000.0).max(0.001),
                (du + ds).saturating_sub(cpu0.0 + cpu0.1)
            )
            .unwrap();
        }
        "cleanup" => {
            // List + delete every object under --prefix, then re-list to
            // verify the prefix is empty. Used to keep throwaway buckets from
            // accumulating residue between rounds.
            let store = S3ObjectStore::new(s3cfg).expect("store");
            let mut deleted = 0usize;
            let mut token = None;
            loop {
                let page = store.list(&prefix, token.as_deref(), 1000).expect("list");
                let n = page.items.len();
                for it in &page.items {
                    store
                        .delete(&it.key)
                        .unwrap_or_else(|e| die(&format!("delete {}: {e}", it.key)));
                }
                deleted += n;
                match page.next_token {
                    Some(t) => token = Some(t),
                    None => break,
                }
            }
            let page = store.list(&prefix, None, 10).expect("relist");
            writeln!(
                out,
                "# cleanup deleted={deleted} remaining={}",
                page.items.len()
            )
            .unwrap();
        }
        other => die(&format!("unknown arm {other}")),
    }
    out.flush().unwrap();
}

// ---- probe-drawn request code: mirrors object_store/s3.rs byte for byte ----

fn probe_agent() -> ureq::Agent {
    // Same config as s3.rs agent(): global timeout, statuses delivered as
    // responses instead of Err. No Expect header anywhere: ureq only honors
    // one if the caller sets it (ureq-proto prepare.rs), and we never do.
    ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(300)))
        .http_status_as_error(false)
        .build()
        .new_agent()
}

fn signed_put(
    agent: &ureq::Agent,
    signer: &SigV4,
    host: &str,
    canonical_path: &str,
    url: &str,
    data: &[u8],
) -> std::result::Result<u16, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let s = signer.sign("PUT", host, canonical_path, "", data, now);
    let resp = agent
        .put(url)
        .header("x-amz-date", &s.amz_date)
        .header("x-amz-content-sha256", &s.payload_hash)
        .header("Authorization", &s.authorization)
        .header("Content-Type", "application/octet-stream")
        .send(data)
        .map_err(|e| format!("transport: {e}"))?;
    Ok(resp.status().as_u16())
}

fn signed_get(
    agent: &ureq::Agent,
    signer: &SigV4,
    host: &str,
    canonical_path: &str,
    url: &str,
) -> std::result::Result<u16, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let s = signer.sign("GET", host, canonical_path, "", b"", now);
    let mut resp = agent
        .get(url)
        .header("x-amz-date", &s.amz_date)
        .header("x-amz-content-sha256", &s.payload_hash)
        .header("Authorization", &s.authorization)
        .call()
        .map_err(|e| format!("transport: {e}"))?;
    let status = resp.status().as_u16();
    // Drain like store.get() does so the pooled connection is reusable.
    let n = resp
        .body_mut()
        .with_config()
        .limit(1024 * 1024 * 1024)
        .read_to_vec()
        .map_err(|e| format!("body: {e}"))?
        .len();
    std::hint::black_box(n);
    Ok(status)
}

/// GET returning the raw body bytes (verification path).
fn signed_get_body(
    agent: &ureq::Agent,
    signer: &SigV4,
    host: &str,
    canonical_path: &str,
    url: &str,
) -> std::result::Result<Vec<u8>, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let s = signer.sign("GET", host, canonical_path, "", b"", now);
    let mut resp = agent
        .get(url)
        .header("x-amz-date", &s.amz_date)
        .header("x-amz-content-sha256", &s.payload_hash)
        .header("Authorization", &s.authorization)
        .call()
        .map_err(|e| format!("transport: {e}"))?;
    let status = resp.status().as_u16();
    if !(200..300).contains(&status) {
        return Err(format!("status {status}"));
    }
    resp.body_mut()
        .with_config()
        .limit(1024 * 1024 * 1024)
        .read_to_vec()
        .map_err(|e| format!("body: {e}"))
}

// ---- addressing helpers: replicate S3ObjectStore::new's public behavior ----

fn scheme_of(endpoint: &str) -> String {
    endpoint
        .split_once("://")
        .map(|(s, _)| s.to_string())
        .unwrap_or_else(|| "https".into())
}

fn host_of(endpoint: &str) -> String {
    endpoint
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(endpoint)
        .split('/')
        .next()
        .unwrap_or_default()
        .to_string()
}

fn port_of(endpoint: &str) -> u16 {
    let host = host_of(endpoint);
    match host.rsplit_once(':') {
        Some((_, p)) if p.chars().all(|c| c.is_ascii_digit()) && !p.is_empty() => {
            p.parse().unwrap()
        }
        _ => {
            if scheme_of(endpoint) == "https" {
                443
            } else {
                80
            }
        }
    }
}

/// SigV4 Host: virtual-host style moves the bucket into the host.
fn vhost_of(cfg: &S3Config) -> String {
    let bare = host_of(&cfg.endpoint);
    if cfg.path_style {
        bare
    } else {
        format!("{}.{}", cfg.bucket, bare)
    }
}

fn base_path_of(cfg: &S3Config) -> String {
    if cfg.path_style {
        format!("/{}", cfg.bucket)
    } else {
        String::new()
    }
}

fn url_base_of(cfg: &S3Config) -> String {
    if cfg.path_style {
        cfg.endpoint.trim_end_matches('/').to_string()
    } else {
        format!("{}://{}", scheme_of(&cfg.endpoint), vhost_of(cfg))
    }
}

fn signer_of(cfg: &S3Config) -> SigV4 {
    SigV4 {
        access_key: cfg.access_key.clone(),
        secret_key: cfg.secret_key.clone(),
        region: cfg.region.clone(),
        service: if cfg.service.is_empty() {
            "s3".into()
        } else {
            cfg.service.clone()
        },
    }
}

fn ok_or_die(r: &Result<()>, what: &str, seq: usize) {
    if let Err(e) = r {
        die(&format!("{what} seq {seq}: {e}"));
    }
}

/// Deterministic per-sequence payload for the concurrency arms: xorshift
/// seeded by seq so the verifier can regenerate the expected bytes without
/// storing them. Never all-zeros.
fn payload_for(seq: usize, size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size];
    let mut x: u64 = 0x9E3779B97F4A7C15 ^ ((seq as u64).wrapping_mul(0xA0761D6478BD642F));
    for b in buf.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = x as u8;
    }
    buf
}

/// utime+stime clock ticks for this process (Linux /proc/self/stat fields
/// 14 and 15). Returns (0,0) off-Linux - CPU accounting is then absent from
/// the report rather than wrong.
fn proc_cpu_ticks() -> (u64, u64) {
    if let Ok(s) = std::fs::read_to_string("/proc/self/stat") {
        if let Some(i) = s.rfind(')') {
            let f: Vec<&str> = s[i + 2..].split_whitespace().collect();
            // after ") " field 3 (state) is index 0; utime is field 14 -> 11,
            // stime field 15 -> 12.
            let ut = f.get(11).and_then(|v| v.parse().ok()).unwrap_or(0);
            let st = f.get(12).and_then(|v| v.parse().ok()).unwrap_or(0);
            return (ut, st);
        }
    }
    (0, 0)
}

fn truncate_err(s: &str) -> String {
    let one_line = s.replace(['\n', '\t', '\r'], " ");
    match one_line.char_indices().nth(90) {
        Some((i, _)) => one_line[..i].to_string(),
        None => one_line,
    }
}

fn die(msg: &str) -> ! {
    eprintln!("s3probe: {msg}");
    std::process::exit(1);
}
