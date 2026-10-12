//! RPC write-path probe (Phase F1 diagnostics). Drives the REAL production
//! write chain - Client::write_from_reader -> POST /data/write -> server
//! worker -> Core::write_stream -> S3 put_stream (small-object single PUT,
//! part-budget permit) -> CAS commit - at controlled concurrency. This is
//! NOT an S3ObjectStore bypass: every request is a full RPC write that the
//! mount path itself would issue. Diagnostics only; never runs in the
//! server; runtime behavior untouched.

use std::io::{Cursor, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use lightfs::client::{Client, ClientConfig};

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
    let url = get("--url");
    let n: usize = get("--concurrency").parse().expect("concurrency");
    let count: usize = get("--count").parse().expect("count");
    let size: usize = get("--size").parse().unwrap_or(2048);
    let prefix = get("--prefix");

    // metadata_ttl None: the probe measures the write path, not the cache.
    let client = Arc::new(Client::new(ClientConfig {
        url,
        metadata_ttl: None,
    }));

    // Health gate: refuse to measure against a dead server.
    // (write_full on a probe-owned path doubles as the connectivity check.)
    let warm = format!("/{prefix}-warm");
    client
        .write_full(&warm, b"probe-warm", true)
        .expect("server write probe failed (is the server up?)");

    let next = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(n));
    let cpu0 = proc_cpu_ticks();
    let wall0 = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..n {
        let (client, next, barrier) = (client.clone(), next.clone(), barrier.clone());
        let prefix = prefix.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            let mut rows: Vec<(usize, u64, u16, String)> = Vec::new();
            loop {
                let seq = next.fetch_add(1, Ordering::SeqCst);
                if seq >= count {
                    break;
                }
                let payload = payload_for(seq, size);
                let path = format!("/{prefix}/{seq:06}");
                let t = Instant::now();
                match client.write_from_reader(
                    &path,
                    &mut Cursor::new(payload),
                    size as u64,
                    None,
                    true,
                ) {
                    Ok(_) => rows.push((seq, t.elapsed().as_nanos() as u64, 200, String::new())),
                    Err(e) => rows.push((
                        seq,
                        t.elapsed().as_nanos() as u64,
                        0,
                        truncate_err(&e.to_string()),
                    )),
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
    let ok = all.iter().filter(|r| r.2 == 200).count();
    let failed = all.len() - ok;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    for (seq, ns, st, err) in &all {
        writeln!(out, "rpc-write\t{size}\t{seq}\t{ns}\t{st}\t{err}").unwrap();
    }
    writeln!(
        out,
        "# rpc-summary concurrency={n} requested={count} done={} failed={failed} wall_ms={wall_ms} rps={:.1} cpu_ticks(u+s)={}",
        all.len(),
        all.len() as f64 / (wall_ms as f64 / 1000.0).max(0.001),
        (du + ds).saturating_sub(cpu0.0 + cpu0.1)
    )
    .unwrap();
    out.flush().unwrap();
}

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

fn proc_cpu_ticks() -> (u64, u64) {
    if let Ok(s) = std::fs::read_to_string("/proc/self/stat") {
        if let Some(i) = s.rfind(')') {
            let f: Vec<&str> = s[i + 2..].split_whitespace().collect();
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
