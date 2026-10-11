//! Lightweight operation metrics: per-op call counts and latency
//! histograms for performance attribution (which layer eats the time in
//! metadata-heavy workloads: FUSE callback, client RPC, server handler,
//! SQLite, S3).
//!
//! Opt-in via `LIGHTFS_METRICS=1` (checked once, first use); when
//! disabled the cost is one atomic load per scope. Dumps:
//! - server: GET /metrics (text table)
//! - mount:  LIGHTFS_METRICS_FILE=<path>, written when the FUSE session
//!   is destroyed (unmount)
//!
//! NOT a production telemetry facility: no export, no sampling, no
//! aggregation across processes - a benchmarking instrument.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

static ENABLED: AtomicBool = AtomicBool::new(false);

// Histogram bucket upper edges (inclusive), milliseconds. Log-ish scale
// wide enough to separate "SQLite hit" from "loopback S3" from "cloud
// network RTT" - the attribution question this module exists for.
const BUCKET_EDGES_MS: [u64; 12] = [
    1,
    3,
    10,
    30,
    100,
    300,
    1_000,
    3_000,
    10_000,
    30_000,
    100_000,
    u64::MAX,
];

#[derive(Default)]
struct Metric {
    count: u64,
    total_ms: u64,
    buckets: [u64; BUCKET_EDGES_MS.len()],
}

static REGISTRY: LazyLock<Mutex<BTreeMap<String, Metric>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub fn init_from_env() {
    if std::env::var("LIGHTFS_METRICS").as_deref() == Ok("1") {
        ENABLED.store(true, Ordering::Relaxed);
    }
}

pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn record(op: &str, elapsed: Duration) {
    if !enabled() {
        return;
    }
    let ms = elapsed.as_millis() as u64;
    let mut reg = REGISTRY.lock().unwrap();
    let m = reg.entry(op.to_string()).or_default();
    m.count += 1;
    m.total_ms += ms;
    for (i, edge) in BUCKET_EDGES_MS.iter().enumerate() {
        if ms <= *edge {
            m.buckets[i] += 1;
            break;
        }
    }
}

/// RAII timer: `let _g = metrics::scope("fuse.getattr");` - binds the
/// guard to the enclosing scope (do NOT write `let _ =`, which drops
/// immediately and records nothing).
pub fn scope(op: &'static str) -> Option<Scope> {
    if !enabled() {
        return None;
    }
    Some(Scope {
        op,
        start: std::time::Instant::now(),
    })
}

pub struct Scope {
    op: &'static str,
    start: std::time::Instant,
}

impl Drop for Scope {
    fn drop(&mut self) {
        record(self.op, self.start.elapsed());
    }
}

/// Timer for dynamic op names (e.g. "rpc.<method>"): measures and
/// records on `finish`. Cheap no-op when disabled.
pub fn time<F, T>(op: &str, f: F) -> T
where
    F: FnOnce() -> T,
{
    if !enabled() {
        return f();
    }
    let start = std::time::Instant::now();
    let out = f();
    record(op, start.elapsed());
    out
}

/// Human-readable table sorted by total time (the attribution view).
pub fn dump() -> String {
    let reg = REGISTRY.lock().unwrap();
    if reg.is_empty() {
        return String::from("(metrics empty or disabled)\n");
    }
    let mut rows: Vec<(&String, &Metric)> = reg.iter().collect();
    rows.sort_by_key(|(_, m)| std::cmp::Reverse(m.total_ms));
    let mut out = String::from(
        "op                              count      total_ms    avg_ms   | <=1ms <=10ms <=100ms <=1s <=10s >10s\n",
    );
    for (name, m) in rows {
        let avg = if m.count > 0 { m.total_ms / m.count } else { 0 };
        let b = &m.buckets;
        out.push_str(&format!(
            "{:<30} {:>7} {:>13} {:>9}   | {:>5} {:>6} {:>7} {:>4} {:>5} {:>4}\n",
            name,
            m.count,
            m.total_ms,
            avg,
            b[0] + b[1],
            b[2] + b[3],
            b[4] + b[5] + b[6],
            b[6] + b[7],
            b[8] + b[9] + b[10],
            b[11],
        ));
    }
    out
}

/// Mount-process dump: write the table to LIGHTFS_METRICS_FILE (called
/// from the FUSE destroy callback; the file is the only channel out of
/// the mount process).
pub fn dump_to_file_if_configured() {
    if !enabled() {
        return;
    }
    if let Ok(path) = std::env::var("LIGHTFS_METRICS_FILE") {
        if let Err(e) = std::fs::write(&path, dump()) {
            eprintln!("lightfs-metrics: cannot write {path}: {e}");
        }
    }
}
