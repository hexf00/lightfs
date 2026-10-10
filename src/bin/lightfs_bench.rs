//! `lightfs-bench` — layered I/O benchmark (performance review harness).
//!
//! Measures the same dataset through two paths so the bottleneck can be
//! attributed:
//!   raw S3  : S3ObjectStore in this process (MinIO round trip only)
//!   lightfs : network Client -> LightFS server -> S3 (full product path)
//!
//! Scenarios: single large object (full + 8 MiB-chunked), N small objects
//! serial, and N small objects with bounded in-flight concurrency
//! (4/8/16/32). Per-request latencies are collected and reported as
//! avg/P50/P95/P99. No Tokio: bounded worker threads over an atomic queue.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use lightfs::client::{Client, ClientConfig};
use lightfs::error::Result;
use lightfs::object_store::s3::{S3Config, S3ObjectStore};
use lightfs::object_store::{ObjectStore, Range};

#[derive(Clone)]
struct Args {
    endpoint: String,
    bucket: String,
    server: Option<String>,
    objects: u64,
    obj_size: u64,
    large_mb: u64,
    lfs_large_mb: u64,
    scenario: String,
}

struct Metrics {
    name: String,
    requests: u64,
    files: u64,
    bytes: u64,
    wall: Duration,
    latencies_us: Mutex<Vec<u64>>,
}

impl Metrics {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            requests: 0,
            files: 0,
            bytes: 0,
            wall: Duration::ZERO,
            latencies_us: Mutex::new(Vec::new()),
        }
    }
    fn report(&self) {
        let mut l = self.latencies_us.lock().unwrap().clone();
        l.sort_unstable();
        let (avg, p50, p95, p99) = if l.is_empty() {
            (0, 0, 0, 0)
        } else {
            let sum: u64 = l.iter().sum();
            (
                sum / l.len() as u64,
                l[l.len() / 2],
                l[l.len() * 95 / 100],
                l[l.len() * 99 / 100],
            )
        };
        let mbps = self.bytes as f64 / 1_048_576.0 / self.wall.as_secs_f64();
        let fps = self.files as f64 / self.wall.as_secs_f64();
        println!(
            "{:<28} {:>8} reqs {:>6} files {:>9} bytes {:>8.2}s {:>7.2} MB/s {:>7.1} files/s | lat ms avg {:>6.2} p50 {:>6.2} p95 {:>6.2} p99 {:>6.2}",
            self.name,
            self.requests,
            self.files,
            self.bytes,
            self.wall.as_secs_f64(),
            mbps,
            fps,
            avg as f64 / 1000.0,
            p50 as f64 / 1000.0,
            p95 as f64 / 1000.0,
            p99 as f64 / 1000.0,
        );
    }
}

fn key_for(i: u64) -> String {
    format!("bench2/obj-{i:06}")
}

fn parse_args() -> Args {
    let mut a = Args {
        endpoint: "http://127.0.0.1:9000".into(),
        bucket: "lightfs-bench".into(),
        server: None,
        objects: 6000,
        obj_size: 100_000,
        large_mb: 256,
        lfs_large_mb: 64,
        scenario: "all".into(),
    };
    let it = std::env::args().skip(1);
    let mut vals = it.peekable();
    while let Some(arg) = vals.next() {
        let mut next = || {
            vals.next().unwrap_or_else(|| {
                eprintln!("missing value for {arg}");
                std::process::exit(2);
            })
        };
        match arg.as_str() {
            "--s3-endpoint" => a.endpoint = next(),
            "--bucket" => a.bucket = next(),
            "--server" => a.server = Some(next()),
            "--objects" => a.objects = next().parse().unwrap(),
            "--obj-size" => a.obj_size = next().parse().unwrap(),
            "--large-mb" => a.large_mb = next().parse().unwrap(),
            "--lfs-large-mb" => a.lfs_large_mb = next().parse().unwrap(),
            "--scenario" => a.scenario = next(),
            other => {
                eprintln!("unknown arg {other}");
                std::process::exit(2);
            }
        }
    }
    a
}

fn main() {
    let args = parse_args();
    if let Err(e) = run(args) {
        eprintln!("lightfs-bench: {e}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> Result<()> {
    // The direct-S3 store is only needed by raw-* comparison scenarios.
    // Server-mode (lfs-*) benchmarks must not require a reachable MinIO:
    // they exercise the SERVER's S3 path, wherever that points. Constructed
    // lazily below; found when benchmarking against real cloud endpoints.
    let need_s3 = matches!(
        args.scenario.as_str(),
        "all" | "prepare"
    ) || args.scenario.starts_with("raw-");
    let s3: Option<Arc<S3ObjectStore>> = if need_s3 {
        let s3 = Arc::new(S3ObjectStore::new(S3Config {
            endpoint: args.endpoint.clone(),
            region: "us-east-1".into(),
            bucket: args.bucket.clone(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            path_style: true,
            service: String::new(),
        })?);
        s3.ping()?;
        Some(s3)
    } else {
        None
    };

    let need_lfs = matches!(
        args.scenario.as_str(),
        "all"
            | "lfs-large"
            | "lfs-serial"
            | "lfs-par"
            | "lfs-chunked"
            | "lfs-stream-large"
            | "lfs-stream-write"
            | "lfs-presign-par"
    ) || args.scenario.starts_with("lfs-");
    if need_lfs && args.server.is_none() {
        return Err(lightfs::error::Error::Config(
            "lfs scenarios need --server URL".into(),
        ));
    }

    if args.scenario == "prepare" || args.scenario == "all" {
        prepare(&args, s3.as_ref().expect("raw scenario without s3 store"))?;
    }

    let scenarios: Vec<&str> = if args.scenario == "all" {
        vec![
            "raw-large",
            "raw-chunked",
            "raw-serial",
            "raw-par4",
            "raw-par8",
            "raw-par16",
            "raw-par32",
            "lfs-large",
            "lfs-chunked",
            "lfs-serial",
            "lfs-par4",
            "lfs-par8",
            "lfs-par16",
            "lfs-par32",
        ]
    } else {
        args.scenario.split(',').collect()
    };

    for s in scenarios {
        match s {
            "prepare" => {}
            "raw-large" => raw_large(&args, s3.as_ref().expect("raw scenario without s3 store"), false)?,
            "raw-chunked" => raw_large(&args, s3.as_ref().expect("raw scenario without s3 store"), true)?,
            "raw-serial" => raw_many(&args, s3.as_ref().expect("raw scenario without s3 store"), 1)?,
            s if s.starts_with("raw-par") => {
                let c: usize = s.trim_start_matches("raw-par").parse().unwrap();
                raw_many(&args, s3.as_ref().expect("raw scenario without s3 store"), c)?;
            }
            "lfs-large" => lfs_large(&args, false)?,
            "lfs-stream-write" => lfs_stream_write(&args)?,
            "lfs-stream-large" => lfs_stream_large(&args)?,
            "lfs-chunked" => lfs_large(&args, true)?,
            "lfs-serial" => lfs_many(&args, 1, false)?,
            s if s.starts_with("lfs-par") => {
                let c: usize = s.trim_start_matches("lfs-par").parse().unwrap();
                lfs_many(&args, c, false)?;
            }
            "lfs-4k-rand" => lfs_4k_rand(&args, 1)?,
            s if s.starts_with("lfs-4k-rand") => {
                let c: usize = s.trim_start_matches("lfs-4k-rand").parse().unwrap();
                lfs_4k_rand(&args, c)?;
            }
            "lfs-meta" => lfs_meta(&args, 1)?,
            s if s.starts_with("lfs-meta-par") => {
                let c: usize = s.trim_start_matches("lfs-meta-par").parse().unwrap();
                lfs_meta(&args, c)?;
            }
            "raw-4k-rand" => raw_4k_rand(&args, s3.as_ref().expect("raw scenario without s3 store"), 1)?,
            s if s.starts_with("raw-4k-rand") => {
                let c: usize = s.trim_start_matches("raw-4k-rand").parse().unwrap();
                raw_4k_rand(&args, s3.as_ref().expect("raw scenario without s3 store"), c)?;
            }
            "raw-proxy-par" => raw_proxy_par(&args, 1)?,
            s if s.starts_with("raw-proxy-par") => {
                let c: usize = s.trim_start_matches("raw-proxy-par").parse().unwrap();
                raw_proxy_par(&args, c)?;
            }
            s if s.starts_with("lfs-presign-par") => {
                let c: usize = s.trim_start_matches("lfs-presign-par").parse().unwrap();
                lfs_presign_many(&args, c)?;
            }
            "lfs-serial-noagent-reuse" => lfs_many(&args, 1, true)?,
            other => {
                return Err(lightfs::error::Error::Config(format!(
                    "unknown scenario {other}"
                )));
            }
        }
    }
    Ok(())
}

/// Idempotent dataset creation: 6000 exact-size objects, one 256 MiB object.
fn prepare(args: &Args, s3: &Arc<S3ObjectStore>) -> Result<()> {
    let t0 = Instant::now();
    let large_key = "bench2/large.bin";
    let large_size = args.large_mb * 1_048_576;
    let ok = matches!(&s3.head(large_key), Ok(i) if i.size == large_size);
    if !ok {
        let content = vec![0xA5u8; large_size as usize];
        s3.put(large_key, &content)?;
    }

    let first = s3.head(&key_for(0));
    let ok = matches!(&first, Ok(i) if i.size == args.obj_size);
    if !ok {
        let content: Vec<u8> = (0..args.obj_size).map(|b| (b % 251) as u8).collect();
        for i in 0..args.objects {
            s3.put(&key_for(i), &content)?;
        }
    }
    println!(
        "# dataset ready: {} x {}B small objects + {}MiB large object (prepare {:.1}s)",
        args.objects,
        args.obj_size,
        args.large_mb,
        t0.elapsed().as_secs_f64()
    );
    Ok(())
}

/// Worker-pool runner: `workers` threads pull a shared atomic index; each
/// job runs `f(i)` and records its latency. Bounded in-flight = workers.
fn pool(
    workers: usize,
    total: u64,
    f: impl Fn(u64) -> Result<usize> + Sync,
) -> (u64, Vec<u64>, Duration) {
    let next = AtomicU64::new(0);
    let lat: Mutex<Vec<u64>> = Mutex::new(Vec::new());
    let bytes = AtomicU64::new(0);
    let t0 = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..workers.max(1) {
            s.spawn(|| {
                let mut local_lat = Vec::new();
                let mut local_bytes = 0u64;
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= total {
                        break;
                    }
                    let t = Instant::now();
                    let n = f(i).expect("job failed");
                    local_lat.push(t.elapsed().as_micros() as u64);
                    local_bytes += n as u64;
                }
                lat.lock().unwrap().extend(local_lat);
                bytes.fetch_add(local_bytes, Ordering::Relaxed);
            });
        }
    });
    (bytes.into_inner(), lat.into_inner().unwrap(), t0.elapsed())
}

fn raw_large(args: &Args, s3: &Arc<S3ObjectStore>, chunked: bool) -> Result<()> {
    let key = "bench2/large.bin";
    let size = args.large_mb * 1_048_576;
    let mut m = Metrics::new(if chunked {
        "raw-s3 large 8MiB-chunked"
    } else {
        "raw-s3 large full-GET"
    });
    let t0 = Instant::now();
    if chunked {
        let chunk = 8 * 1_048_576u64;
        let mut off = 0u64;
        while off < size {
            let len = chunk.min(size - off);
            let t = Instant::now();
            let d = s3.get(key, Some(Range { start: off, len }))?;
            m.latencies_us
                .lock()
                .unwrap()
                .push(t.elapsed().as_micros() as u64);
            std::hint::black_box(d.data);
            m.requests += 1;
            m.bytes += len;
            off += len;
        }
    } else {
        let t = Instant::now();
        let d = s3.get(key, None)?;
        m.latencies_us
            .lock()
            .unwrap()
            .push(t.elapsed().as_micros() as u64);
        std::hint::black_box(d.data);
        m.requests = 1;
        m.bytes = size;
    }
    m.files = if chunked { m.requests } else { 1 };
    m.wall = t0.elapsed();
    m.report();
    Ok(())
}

fn raw_many(args: &Args, s3: &Arc<S3ObjectStore>, workers: usize) -> Result<()> {
    let name = if workers == 1 {
        "raw-s3 serial".to_string()
    } else {
        format!("raw-s3 par-{workers}")
    };
    let mut m = Metrics::new(&name);
    let s3 = s3.clone();
    let (bytes, lat, wall) = pool(workers, args.objects, move |i| {
        let d = s3.get(&key_for(i), None)?;
        Ok(d.data.len())
    });
    m.requests = args.objects;
    m.files = args.objects;
    m.bytes = bytes;
    m.latencies_us = Mutex::new(lat);
    m.wall = wall;
    m.report();
    Ok(())
}

fn lfs_client(args: &Args) -> Arc<Client> {
    Arc::new(Client::new(ClientConfig {
        url: args.server.clone().unwrap(),
        metadata_ttl: None, // benchmark measures the data path, not caching
    }))
}

/// Import the dataset into the LightFS namespace once (through the product
/// write path), then read it back in the lfs scenarios.
fn ensure_lfs_import(args: &Args, client: &Arc<Client>) -> Result<u64> {
    use lightfs::api::FsApi;
    let root = "/bench-lfs";
    match client.lookup(root) {
        Ok(_) => {}
        Err(_) => {
            client.mkdir(root, 0o755)?;
        }
    }
    // marker object records how many files were imported
    let marker = format!("{root}/.imported-{}", args.objects);
    match client.lookup(&marker) {
        Ok(_) => Ok(args.objects),
        Err(_) => {
            let t0 = Instant::now();
            let content: Vec<u8> = (0..args.obj_size).map(|b| (b % 251) as u8).collect();
            for i in 0..args.objects {
                let path = format!("{root}/obj-{i:06}");
                match client.lookup(&path) {
                    Ok(n) if n.size == content.len() as i64 => {}
                    _ => {
                        client.write_full(&path, &content, true)?;
                    }
                }
            }
            client.write_full(&marker, b"ok", true)?;
            eprintln!(
                "# lfs import: {} files through product write path in {:.1}s",
                args.objects,
                t0.elapsed().as_secs_f64()
            );
            Ok(args.objects)
        }
    }
}

fn lfs_large(args: &Args, chunked: bool) -> Result<()> {
    use lightfs::api::FsApi;
    let client = lfs_client(args);
    let path = "/bench-lfs-large.bin";
    let size = args.lfs_large_mb * 1_048_576;
    if !matches!(&client.lookup(path), Ok(n) if n.size == size as i64) {
        let content = vec![0x5Au8; size as usize];
        client.write_full(path, &content, true)?;
    }
    let mut m = Metrics::new(if chunked {
        "lfs large 8MiB-chunked"
    } else {
        "lfs large full-read"
    });
    let t0 = Instant::now();
    if chunked {
        let chunk = 8 * 1_048_576u64;
        let mut off = 0u64;
        while off < size {
            let len = chunk.min(size - off);
            let t = Instant::now();
            let d = client.read(path, off, len)?;
            m.latencies_us
                .lock()
                .unwrap()
                .push(t.elapsed().as_micros() as u64);
            std::hint::black_box(&d);
            m.requests += 1;
            m.bytes += d.len() as u64;
            off += len;
        }
    } else {
        let t = Instant::now();
        let d = client.read(path, 0, size)?;
        m.latencies_us
            .lock()
            .unwrap()
            .push(t.elapsed().as_micros() as u64);
        std::hint::black_box(&d);
        m.requests = 1;
        m.bytes = d.len() as u64;
        m.files = 1;
    }
    m.files = if chunked { m.requests } else { 1 };
    m.wall = t0.elapsed();
    m.report();
    Ok(())
}

/// A reader that produces deterministic bytes without materializing them.
struct GenReader {
    remaining: u64,
    state: u64,
}
impl GenReader {
    fn new(len: u64) -> Self {
        Self {
            remaining: len,
            state: 0x1234_5678_9abc_def0,
        }
    }
}
impl std::io::Read for GenReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = buf.len().min(self.remaining as usize);
        for b in buf[..n].iter_mut() {
            self.state = self
                .state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            *b = (self.state >> 33) as u8;
        }
        self.remaining -= n as u64;
        Ok(n)
    }
}

/// 4 KiB random reads inside the 100KB objects: the request-rate-bound
/// regime (IOPS and latency, not sequential throughput).
fn lfs_4k_rand(args: &Args, workers: usize) -> Result<()> {
    use lightfs::api::FsApi;
    let client = lfs_client(args);
    ensure_lfs_import(args, &client)?;
    let ops = args.objects; // 6000 random 4KiB reads
    let obj_size = args.obj_size;
    let client2 = client.clone();
    let (bytes, lat, wall) = pool(workers, ops, move |i| {
        // deterministic pseudo-random offset, 4KiB slice
        let off = i.wrapping_mul(2654435761) % (obj_size - 4096);
        let d = client2.read(&path_for(i), off, 4096)?;
        Ok(d.len())
    });
    let mut m = Metrics::new(&format!("lfs 4KiB-rand par-{workers}"));
    m.requests = ops;
    m.files = ops;
    m.bytes = bytes;
    m.latencies_us = Mutex::new(lat);
    m.wall = wall;
    m.report();
    Ok(())
}

/// Metadata operations: lookup/stat of existing paths (the per-request fixed
/// cost that bounds small-file workloads).
fn lfs_meta(args: &Args, workers: usize) -> Result<()> {
    use lightfs::api::FsApi;
    let client = lfs_client(args);
    ensure_lfs_import(args, &client)?;
    let ops = args.objects * 2; // 12000 lookups
    let client2 = client.clone();
    let (bytes, lat, wall) = pool(workers, ops, move |i| {
        let path = if i % 2 == 0 {
            path_for(i % args.objects)
        } else {
            "/bench-lfs".to_string()
        };
        let n = client2.lookup(&path)?;
        Ok(n.name.len())
    });
    let mut m = Metrics::new(&format!("lfs meta(lookup) par-{workers}"));
    m.requests = ops;
    m.files = ops;
    m.bytes = bytes;
    m.latencies_us = Mutex::new(lat);
    m.wall = wall;
    m.report();
    Ok(())
}

/// Raw S3 4 KiB random range reads (baseline for lfs-4k-rand).
fn raw_4k_rand(args: &Args, s3: &Arc<S3ObjectStore>, workers: usize) -> Result<()> {
    let ops = args.objects;
    let obj_size = args.obj_size;
    let s3 = s3.clone();
    let (bytes, lat, wall) = pool(workers, ops, move |i| {
        let off = i.wrapping_mul(2654435761) % (obj_size - 4096);
        let d = s3.get(
            &key_for(i),
            Some(Range {
                start: off,
                len: 4096,
            }),
        )?;
        Ok(d.data.len())
    });
    let mut m = Metrics::new(&format!("raw-s3 4KiB-rand par-{workers}"));
    m.requests = ops;
    m.files = ops;
    m.bytes = bytes;
    m.latencies_us = Mutex::new(lat);
    m.wall = wall;
    m.report();
    Ok(())
}

/// Plain HTTP proxy reference (Experiment C): forwards the same objects
/// from a minimal Node.js process so per-request CPU cost can be compared
/// against the LightFS server on identical work.
fn raw_proxy_par(args: &Args, workers: usize) -> Result<()> {
    let proxy =
        std::env::var("LIGHTFS_PROXY_URL").unwrap_or_else(|_| "http://127.0.0.1:18086".into());
    let ops = args.objects;
    let obj_size = args.obj_size;
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(120)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let (bytes, lat, wall) = pool(workers, ops, move |i| {
        let mut resp = agent
            .get(format!("{proxy}/{}", key_for(i)))
            .call()
            .map_err(|e| lightfs::error::Error::Io(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(lightfs::error::Error::Storage(format!(
                "proxy {}",
                resp.status()
            )));
        }
        let data = resp
            .body_mut()
            .with_config()
            .limit(1 << 30)
            .read_to_vec()
            .map_err(|e| lightfs::error::Error::Io(e.to_string()))?;
        Ok(data.len())
    });
    let _ = obj_size;
    let mut m = Metrics::new(&format!("node-proxy par-{workers}"));
    m.requests = ops;
    m.files = ops;
    m.bytes = bytes;
    m.latencies_us = Mutex::new(lat);
    m.wall = wall;
    m.report();
    Ok(())
}

/// Streaming WRITE acceptance: bytes flow from a generator reader through
/// the server into a version object; neither side materializes the file.
fn lfs_stream_write(args: &Args) -> Result<()> {
    use lightfs::api::FsApi;
    let client = lfs_client(args);
    // Distinct path per process (set by harnesses running parallel writers)
    // so concurrent-writer experiments exercise CAS on DIFFERENT files.
    let path = std::env::var("LIGHTFS_BENCH_PATH")
        .unwrap_or_else(|_| "/bench-lfs-large-stream.bin".to_string());
    let size = args.lfs_large_mb * 1_048_576;
    let _ = client.unlink(&path);
    let mut reader = GenReader::new(size);
    let t0 = Instant::now();
    let node = client.write_from_reader(&path, &mut reader, size, None, true)?;
    let mut m = Metrics::new("lfs large STREAMED-WRITE");
    m.requests = 1;
    m.files = 1;
    m.bytes = node.size as u64;
    m.wall = t0.elapsed();
    m.report();
    Ok(())
}

/// Streaming read of the large file: bounded buffers end-to-end (server
/// proxies from S3 with a reader; client consumes a reader). This is the
/// memory-efficiency acceptance scenario.
fn lfs_stream_large(args: &Args) -> Result<()> {
    use lightfs::api::FsApi;
    use std::io::Read;
    let client = lfs_client(args);
    let path = "/bench-lfs-large-stream.bin";
    let size = args.lfs_large_mb * 1_048_576;
    if !matches!(&client.lookup(path), Ok(n) if n.size == size as i64) {
        // one-time import through the streaming write path
        let content = vec![0x77u8; size as usize];
        client.write_from_reader(path, &mut std::io::Cursor::new(content), size, None, true)?;
    }
    let mut m = Metrics::new("lfs large STREAMED");
    let t0 = Instant::now();
    let (mut reader, len) = client.read_stream(path, 0, size)?;
    let mut buf = vec![0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let t = Instant::now();
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        m.latencies_us
            .lock()
            .unwrap()
            .push(t.elapsed().as_micros() as u64);
        total += n as u64;
        m.requests += 1;
    }
    assert_eq!(total, len, "streamed length must match");
    m.bytes = total;
    m.files = 1;
    m.wall = t0.elapsed();
    m.report();
    Ok(())
}

/// Direct-S3 read PoC: metadata via the server (presign RPC), bytes straight
/// from S3 with a plain unauthenticated GET. Same dataset/benchmark as the
/// proxied lfs-par scenarios.
fn lfs_presign_many(args: &Args, workers: usize) -> Result<()> {
    let client = lfs_client(args);
    ensure_lfs_import(args, &client)?;
    let mut m = Metrics::new(&format!("lfs PRESIGN-par-{workers}"));
    let objects = args.objects;
    let obj_size = args.obj_size;
    let c = client.clone();
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(120)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let (bytes, lat, wall) = pool(workers, objects, move |i| {
        let path = path_for(i);
        let url = c.presign(&path, 60)?;
        let mut resp = agent
            .get(&url)
            .call()
            .map_err(|e| lightfs::error::Error::Io(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(lightfs::error::Error::Storage(format!(
                "presigned GET {}",
                resp.status()
            )));
        }
        let data = resp
            .body_mut()
            .with_config()
            .limit(1 << 30)
            .read_to_vec()
            .map_err(|e| lightfs::error::Error::Io(e.to_string()))?;
        Ok(data.len())
    });
    let _ = obj_size;
    m.requests = objects;
    m.files = objects;
    m.bytes = bytes;
    m.latencies_us = Mutex::new(lat);
    m.wall = wall;
    m.report();
    Ok(())
}

fn lfs_many(args: &Args, workers: usize, no_agent_reuse: bool) -> Result<()> {
    use lightfs::api::FsApi;
    let client = lfs_client(args);
    ensure_lfs_import(args, &client)?;
    let name = if no_agent_reuse {
        "lfs serial NO-CONN-REUSE".to_string()
    } else if workers == 1 {
        "lfs serial".to_string()
    } else {
        format!("lfs par-{workers}")
    };
    let mut m = Metrics::new(&name);
    let objects = args.objects;
    let obj_size = args.obj_size;
    // Shared client = one pooled Agent = connection reuse. The no-reuse
    // variant builds a fresh Agent (fresh TCP connection) per request.
    let job: Arc<dyn Fn(u64) -> Result<usize> + Sync + Send> = if no_agent_reuse {
        let url = args.server.clone().unwrap();
        Arc::new(move |i| {
            let c = Client::new(ClientConfig {
                url: url.clone(),
                metadata_ttl: None,
            });
            Ok(c.read(&path_for(i), 0, obj_size)?.len())
        })
    } else {
        let c = client.clone();
        Arc::new(move |i| Ok(c.read(&path_for(i), 0, obj_size)?.len()))
    };
    let (bytes, lat, wall) = pool(workers, objects, move |i| job(i));
    m.requests = objects;
    m.files = objects;
    m.bytes = bytes;
    m.latencies_us = Mutex::new(lat);
    m.wall = wall;
    m.report();
    Ok(())
}

fn path_for(i: u64) -> String {
    format!("/bench-lfs/obj-{i:06}")
}
