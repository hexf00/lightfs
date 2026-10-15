//! S4.8 phase-2 performance baseline harness (STANDALONE, no production
//! logic changed): drives the two pack-seal paths against a real S3/MinIO
//! endpoint through the same engine entry points the tests use.
//!
//!   lightfs_packbase sync   <members> <file_bytes> <threads>   # strict
//!       PackEngine aggregation (write_stream -> commit -> slot answer)
//!   lightfs_packbase stream <members> <file_bytes> <threads>   # WB
//!       batch-pack drain (write_stream staged answer -> seal_batch ->
//!       PackReader streaming upload -> per-member CAS)
//!
//! Environment: LIGHTFS_S3_ENDPOINT (default http://127.0.0.1:9000),
//! LIGHTFS_S3_BUCKET_BASE (default lightfs-pkbase). A per-run bucket is
//! created and emptied (bucket removal is not in the store API — the run
//! leaves an empty bucket behind). Peak RSS is measured by wrapping the
//! process in `/usr/bin/time -l` (ru_maxrss), NOT guessed from code shape.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use lightfs::config::Config;
use lightfs::config::PackSection;
use lightfs::core::{Core, DEFAULT_SUPERSEDED_SECS};
use lightfs::error::Result;
use lightfs::object_store::{ListPage, ObjectData, ObjectInfo, ObjectStore, Range};
use lightfs::wb::WbOptions;

#[derive(Default)]
struct Counts {
    put: AtomicU64,
    put_stream: AtomicU64,
    put_bytes: AtomicU64,
}

/// Counting wrapper: delegates everything, counts uploads.
struct CountingStore {
    inner: lightfs::object_store::s3::S3ObjectStore,
    c: Counts,
}

impl ObjectStore for CountingStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectInfo> {
        self.c.put.fetch_add(1, Ordering::Relaxed);
        self.c
            .put_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        self.inner.put(key, data)
    }
    fn put_stream(
        &self,
        key: &str,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<ObjectInfo> {
        self.c.put_stream.fetch_add(1, Ordering::Relaxed);
        self.c.put_bytes.fetch_add(len, Ordering::Relaxed);
        self.inner.put_stream(key, reader, len)
    }
    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData> {
        self.inner.get(key, range)
    }
    fn get_stream(
        &self,
        key: &str,
        range: Option<Range>,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        self.inner.get_stream(key, range)
    }
    fn head(&self, key: &str) -> Result<ObjectInfo> {
        self.inner.head(key)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
    fn list(&self, prefix: &str, token: Option<&str>, max: usize) -> Result<ListPage> {
        self.inner.list(prefix, token, max)
    }
    fn ping(&self) -> Result<()> {
        self.inner.ping()
    }
}

fn make_store(bucket: &str) -> CountingStore {
    let endpoint = std::env::var("LIGHTFS_S3_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let mut cfg = Config::default();
    cfg.s3.endpoint = endpoint;
    cfg.s3.bucket = bucket.to_string();
    cfg.s3.region = "us-east-1".to_string();
    cfg.s3.path_style = true; // Default-derive leaves this false (vhost would DNS the bucket)
    cfg.s3.access_key = std::env::var("LIGHTFS_S3_AK").unwrap_or_else(|_| "minioadmin".into());
    cfg.s3.secret_key = std::env::var("LIGHTFS_S3_SK").unwrap_or_else(|_| "minioadmin".into());
    let inner = lightfs::object_store::s3::S3ObjectStore::new(cfg.s3_config()).expect("s3 store");
    inner.ensure_bucket().expect("ensure bucket");
    CountingStore {
        inner,
        c: Counts::default(),
    }
}

fn empty_bucket(store: &CountingStore) {
    let mut token: Option<String> = None;
    while let Ok(page) = store.inner.list("", token.as_deref(), 1000) {
        let next = page.next_token.clone();
        for it in &page.items {
            let _ = store.inner.delete(&it.key);
        }
        token = next;
        if token.is_none() {
            break;
        }
    }
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (((sorted.len() as f64) - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 5 || (args[1] != "sync" && args[1] != "stream") {
        eprintln!("usage: lightfs_packbase <sync|stream> <members> <file_bytes> <threads>");
        std::process::exit(2);
    }
    let mode = args[1].clone();
    let members: usize = args[2].parse().unwrap();
    let file_bytes: usize = args[3].parse().unwrap();
    let threads: usize = args[4].parse().unwrap();

    let base = std::env::var("LIGHTFS_S3_BUCKET_BASE").unwrap_or_else(|_| "lightfs-pkbase".into());
    let bucket = format!(
        "{base}-{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = Arc::new(make_store(&bucket));
    store.inner.ping().expect("MinIO unreachable (environment)");

    let staging = std::env::temp_dir().join(format!(
        "pkbase-{}-{:x}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&staging).unwrap();

    let t0 = Instant::now();
    let write_lat: Arc<std::sync::Mutex<Vec<u128>>> = Arc::default();

    let meta: Arc<dyn lightfs::metadata::MetadataStore> =
        Arc::new(lightfs::metadata::sqlite::SqliteMetadataStore::open_in_memory().unwrap());

    match mode.as_str() {
        "sync" => {
            let pack = PackSection {
                enabled: true,
                max_bytes: 8 * 1024 * 1024,
                max_age_ms: 500, // production-scale age bound; keeps the age-seal inside the measured span
                large_file_bytes: 16 * 1024 * 1024, // everything goes to the pack
                read_cache_bytes: 0,
            };
            let core = Arc::new(
                Core::with_options(
                    meta,
                    store.clone(),
                    DEFAULT_SUPERSEDED_SECS,
                    Some(pack),
                    None,
                )
                .unwrap(),
            );
            let mut handles = Vec::new();
            for t in 0..threads {
                let core = core.clone();
                let lat = write_lat.clone();
                handles.push(std::thread::spawn(move || {
                    let per = members / threads;
                    let mut local = Vec::with_capacity(per);
                    let content = vec![b'a' + (t as u8 % 26); file_bytes];
                    core.mkdir(&format!("/t{t}"), 0o755).unwrap();
                    for i in 0..per {
                        let path = format!("/t{t}/f{i}");
                        core.create(&path, 0o644).unwrap();
                        let t0 = Instant::now();
                        // write_stream routes small files into PackEngine::commit;
                        // the answer resolves THIS file's CAS (synchronous slot)
                        core.write_stream(
                            &path,
                            Some(0),
                            false,
                            &mut std::io::Cursor::new(content.clone()),
                            file_bytes as u64,
                        )
                        .unwrap();
                        local.push(t0.elapsed().as_micros());
                    }
                    lat.lock().unwrap().extend(local);
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        }
        "stream" => {
            let wb = WbOptions {
                staging_dir: staging.to_string_lossy().to_string(),
                drain_workers: 0, // manual drain: the seal is the measured span
                batch_pack_bytes: 8 * 1024 * 1024,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 0,
                ledger_lock_path: None,
            };
            let core = Arc::new(
                Core::with_options(meta, store.clone(), DEFAULT_SUPERSEDED_SECS, None, Some(wb))
                    .unwrap(),
            );
            let mut handles = Vec::new();
            for t in 0..threads {
                let core = core.clone();
                let lat = write_lat.clone();
                handles.push(std::thread::spawn(move || {
                    let per = members / threads;
                    let mut local = Vec::with_capacity(per);
                    let content = vec![b'a' + (t as u8 % 26); file_bytes];
                    core.mkdir(&format!("/t{t}"), 0o755).unwrap();
                    for i in 0..per {
                        let path = format!("/t{t}/f{i}");
                        core.create(&path, 0o644).unwrap();
                        let t0 = Instant::now();
                        // staged answer: the front stage never waits for a seal
                        core.write_stream(
                            &path,
                            Some(0),
                            false,
                            &mut std::io::Cursor::new(content.clone()),
                            file_bytes as u64,
                        )
                        .unwrap();
                        local.push(t0.elapsed().as_micros());
                    }
                    lat.lock().unwrap().extend(local);
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
            // THE measured span: seal_batch + per-member CAS to terminal.
            let d0 = Instant::now();
            loop {
                let (q, d, dead) = core.wb().unwrap().queue_counts();
                if q == 0 && d == 0 {
                    break;
                }
                core.wb().unwrap().drain_once().unwrap();
                if dead > 0 {
                    panic!("unexpected dead tasks");
                }
            }
            let drain_us = d0.elapsed().as_micros();
            let mut all = write_lat.lock().unwrap().clone();
            all.sort();
            let total_us = t0.elapsed().as_micros();
            let c = &store.c;
            println!(
                r#"{{"mode":"stream","members":{members},"file_bytes":{file_bytes},"threads":{threads},"packs_sealed":{},"put_calls":{},"put_stream_calls":{},"uploaded_bytes":{},"wall_ms":{},"drain_ms":{},"write_p50_us":{},"write_p99_us":{},"write_max_us":{}}}"#,
                c.put.load(Ordering::Relaxed) + c.put_stream.load(Ordering::Relaxed),
                c.put.load(Ordering::Relaxed),
                c.put_stream.load(Ordering::Relaxed),
                c.put_bytes.load(Ordering::Relaxed),
                total_us / 1000,
                drain_us / 1000,
                percentile(&all, 0.5),
                percentile(&all, 0.99),
                percentile(&all, 1.0),
            );
            empty_bucket(&store);
            let _ = std::fs::remove_dir_all(&staging);
            return;
        }
        _ => unreachable!(),
    }

    // sync-mode report
    let mut all = write_lat.lock().unwrap().clone();
    all.sort();
    let total_us = t0.elapsed().as_micros();
    let c = &store.c;
    println!(
        r#"{{"mode":"sync","members":{members},"file_bytes":{file_bytes},"threads":{threads},"packs_sealed":{},"put_calls":{},"put_stream_calls":{},"uploaded_bytes":{},"wall_ms":{},"drain_ms":0,"write_p50_us":{},"write_p99_us":{},"write_max_us":{}}}"#,
        c.put.load(Ordering::Relaxed) + c.put_stream.load(Ordering::Relaxed),
        c.put.load(Ordering::Relaxed),
        c.put_stream.load(Ordering::Relaxed),
        c.put_bytes.load(Ordering::Relaxed),
        total_us / 1000,
        percentile(&all, 0.5),
        percentile(&all, 0.99),
        percentile(&all, 1.0),
    );
    empty_bucket(&store);
    let _ = std::fs::remove_dir_all(&staging);
}
