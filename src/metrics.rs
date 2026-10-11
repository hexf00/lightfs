//! Lightweight operation metrics: per-op call counts and latency
//! histograms for performance attribution (which layer eats the time in
//! metadata-heavy workloads: FUSE callback, client RPC, server handler,
//! SQLite, S3).
//!
//! Durations are recorded in MICROSECONDS: sub-millisecond ops (most of
//! the metadata path) must not floor to zero - at 1 ms granularity an
//! op costing 800 us would contribute 0 to its total, and a hot op run
//! 10k times would report ~0 for ~8 s of real time.
//!
//! Opt-in via `LIGHTFS_METRICS=1` (checked once, first use); when
//! disabled the cost is one atomic load per scope. Dumps:
//! - server: GET /metrics (text table)
//! - mount:  LIGHTFS_METRICS_FILE=<path>, written when the FUSE session
//!   is destroyed (unmount)
//!
//! NOT a production telemetry facility: no export, no sampling, no
//! aggregation across processes - a benchmarking instrument. Timed
//! benchmark runs should keep it OFF (the on-cost is a mutex'd BTreeMap
//! update per recorded op); use a separate metrics-on run for
//! attribution, which is what scripts/git-workload-perf.sh does.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

static ENABLED: AtomicBool = AtomicBool::new(false);

// Histogram bucket upper edges (inclusive), MICROSECONDS. Log-ish scale
// with sub-millisecond resolution up front: the metadata path lives at
// 10 us (SQLite hit) - 1 ms (loopback RPC), and the attribution question
// is exactly which of those shelves an op lands on. Above 1 ms the
// buckets widen toward loopback-S3 and cloud-network RTT territory.
const BUCKET_EDGES_US: [u64; 16] = [
    50,
    100,
    250,
    500,
    1_000,
    2_500,
    5_000,
    10_000,
    30_000,
    100_000,
    300_000,
    1_000_000,
    3_000_000,
    10_000_000,
    100_000_000,
    u64::MAX,
];

#[derive(Default, Clone)]
struct Metric {
    count: u64,
    total_us: u64,
    buckets: [u64; BUCKET_EDGES_US.len()],
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
    let us = elapsed.as_micros() as u64;
    let mut reg = REGISTRY.lock().unwrap();
    let m = reg.entry(op.to_string()).or_default();
    m.count += 1;
    m.total_us += us;
    for (i, edge) in BUCKET_EDGES_US.iter().enumerate() {
        if us <= *edge {
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
/// Totals/averages in microseconds; histogram columns are cumulative
/// counts, so every op appears in exactly one column per row.
pub fn dump() -> String {
    let reg = REGISTRY.lock().unwrap();
    if reg.is_empty() {
        return String::from("(metrics empty or disabled)\n");
    }
    let mut rows: Vec<(&String, &Metric)> = reg.iter().collect();
    rows.sort_by_key(|(_, m)| std::cmp::Reverse(m.total_us));
    let mut out = String::from(
        "op                              count       total_us      avg_us   | <=50us <=100us <=250us <=500us <=1ms <=10ms <=100ms <=1s >1s\n",
    );
    for (name, m) in rows {
        let avg = if m.count > 0 { m.total_us / m.count } else { 0 };
        let b = &m.buckets;
        let le = |from: usize, to: usize| -> u64 { b[from..=to].iter().sum() };
        out.push_str(&format!(
            "{:<30} {:>7} {:>13} {:>10}   | {:>6} {:>7} {:>7} {:>7} {:>5} {:>6} {:>7} {:>4} {:>3}\n",
            name,
            m.count,
            m.total_us,
            avg,
            b[0],
            b[1],
            b[2],
            b[3],
            le(4, 4),
            le(5, 7),
            le(8, 10),
            b[11],
            le(12, 15),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn recorded(op: &str) -> Metric {
        REGISTRY
            .lock()
            .unwrap()
            .get(op)
            .cloned()
            .expect("op must be recorded")
    }

    // The truncation bug this module once had: as_millis floors, so any
    // sub-ms op contributed 0 to its total. These pin microsecond
    // fidelity at the exact durations from the review.
    #[test]
    fn sub_ms_durations_are_not_truncated() {
        ENABLED.store(true, Ordering::Relaxed);
        record("t.800us", Duration::from_nanos(800_000));
        record("t.100us", Duration::from_nanos(100_000));
        assert_eq!(recorded("t.800us").total_us, 800);
        assert_eq!(recorded("t.100us").total_us, 100);
    }

    // The review's motivating case: 10_000 ops x 800 us must read as
    // ~8 s of total time, not 0.
    #[test]
    fn ten_k_sub_ms_ops_accumulate_to_seconds() {
        ENABLED.store(true, Ordering::Relaxed);
        for _ in 0..10_000 {
            record("t.hot", Duration::from_nanos(800_000));
        }
        let m = recorded("t.hot");
        assert_eq!(m.count, 10_000);
        assert_eq!(m.total_us, 8_000_000); // 8 s, previously reported as 0
        assert_eq!(m.total_us / m.count, 800);
    }

    #[test]
    fn bucket_edges_are_inclusive_micros() {
        ENABLED.store(true, Ordering::Relaxed);
        record("t.b50", Duration::from_micros(50));
        record("t.b51", Duration::from_micros(51));
        record("t.b1k", Duration::from_micros(1_000));
        record("t.b1001", Duration::from_micros(1_001));
        assert_eq!(recorded("t.b50").buckets[0], 1); // <=50us
        assert_eq!(recorded("t.b51").buckets[1], 1); // <=100us
        assert_eq!(recorded("t.b1k").buckets[4], 1); // <=1ms (inclusive)
        assert_eq!(recorded("t.b1001").buckets[5], 1); // <=2.5ms
    }

    #[test]
    fn dump_reports_micros_for_sub_ms_ops() {
        ENABLED.store(true, Ordering::Relaxed);
        record("t.dump", Duration::from_nanos(800_000));
        let table = dump();
        assert!(table.contains("total_us"), "header must name the unit");
        let row = table
            .lines()
            .find(|l| l.starts_with("t.dump"))
            .expect("dumped row");
        assert!(row.contains(" 800 "), "total must read 800us: {row}");
    }
}
