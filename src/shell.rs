//! Shell adapter: CLI + scripted mode over any FsApi backend.
//! Deliberately business-logic-free (adapters only).
//!
//! Commands: ls, stat, mkdir, touch, cat, write, mv, rm, tree,
//! plus `assert-error <cmd...>` inside scripts.

use std::sync::{Arc, Mutex};

use crate::api::FsApi;
use crate::error::{Error, Result};

pub struct Shell {
    fs: Arc<dyn FsApi>,
}

fn fmt_size(n: i64) -> String {
    if n >= 1 << 20 {
        format!("{:.1}M", n as f64 / (1 << 20) as f64)
    } else if n >= 1 << 10 {
        format!("{:.1}K", n as f64 / (1 << 10) as f64)
    } else {
        format!("{n}B")
    }
}

fn fmt_time(unix: i64) -> String {
    // Compact fixed-format UTC stamp (no chrono dependency).
    let days = unix.div_euclid(86400);
    let secs = unix.rem_euclid(86400);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}:{:02}",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn join_dir(dir: &str, name: &str) -> String {
    let dir = dir.trim_end_matches('/');
    if dir.is_empty() {
        format!("/{name}")
    } else {
        format!("{dir}/{name}")
    }
}

/// Plain substring search (no regex): good enough for benchmark scans.
fn memchr_needle(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() || haystack.len() < needle.len() {
        return false;
    }
    let first = needle[0];
    let mut from = 0;
    while let Some(rel) = haystack[from..].iter().position(|&b| b == first) {
        let start = from + rel;
        if haystack[start..].starts_with(needle) {
            return true;
        }
        from = start + 1;
    }
    false
}

impl Shell {
    pub fn new(fs: Arc<dyn FsApi>) -> Self {
        Self { fs }
    }

    /// Execute one command line. Returns output lines to print.
    pub fn exec(&self, line: &str) -> Result<Vec<String>> {
        let argv: Vec<String> = line.split_whitespace().map(String::from).collect();
        if argv.is_empty() {
            return Ok(Vec::new());
        }
        self.dispatch(&argv)
    }

    fn dispatch(&self, argv: &[String]) -> Result<Vec<String>> {
        let cmd = argv[0].as_str();
        let args = &argv[1..];
        match cmd {
            "ls" => self.cmd_ls(args),
            "stat" => self.cmd_stat(args),
            "mkdir" => self.cmd_mkdir(args),
            "touch" => self.cmd_touch(args),
            "cat" => self.cmd_cat(args),
            "write" => self.cmd_write(args),
            "cas-write" => self.cmd_cas_write(args),
            "mv" => self.cmd_mv(args),
            "rm" => self.cmd_rm(args),
            "tree" => self.cmd_tree(args),
            "bench-generate" => self.cmd_bench_generate(args),
            "bench-read" => self.cmd_bench_read(args),
            "bench-grep" => self.cmd_bench_grep(args),
            _ => Err(Error::InvalidArgument(format!("unknown command: {cmd}"))),
        }
    }

    fn one_path<'a>(&self, args: &'a [String], cmd: &str) -> Result<&'a str> {
        args.first()
            .map(String::as_str)
            .ok_or_else(|| Error::InvalidArgument(format!("{cmd} needs a path")))
    }

    fn cmd_ls(&self, args: &[String]) -> Result<Vec<String>> {
        let path = self.one_path(args, "ls")?;
        let node = self.fs.lookup(path)?;
        let mut out = Vec::new();
        if node.is_file() {
            out.push(self.fmt_node_line(path, &node));
            return Ok(out);
        }
        for child in self.fs.readdir(path)? {
            let child_path = join_dir(path, &child.name);
            out.push(self.fmt_node_line(&child_path, &child));
        }
        Ok(out)
    }

    fn fmt_node_line(&self, path: &str, n: &crate::metadata::Node) -> String {
        let kind = if n.is_dir() { 'd' } else { '-' };
        format!(
            "{}{:04o} {:>10} {} {}",
            kind,
            n.mode & 0o7777,
            fmt_size(n.size),
            fmt_time(n.mtime),
            path
        )
    }

    fn cmd_stat(&self, args: &[String]) -> Result<Vec<String>> {
        let path = self.one_path(args, "stat")?;
        let n = self.fs.lookup(path)?;
        Ok(vec![
            format!("path: {path}"),
            format!("kind: {}", if n.is_dir() { "dir" } else { "file" }),
            format!("size: {}", n.size),
            format!("mtime: {}", fmt_time(n.mtime)),
            format!("mode: {:04o}", n.mode),
            format!("id: {}", n.id),
            format!("version: {}", n.version),
            format!("object_key: {}", n.object_key.as_deref().unwrap_or("-")),
            format!(
                "etag: {}",
                if n.etag.is_empty() {
                    "-".into()
                } else {
                    n.etag.clone()
                }
            ),
        ])
    }

    fn cmd_mkdir(&self, args: &[String]) -> Result<Vec<String>> {
        let path = self.one_path(args, "mkdir")?;
        self.fs.mkdir(path, 0o755)?;
        Ok(vec![format!("created {path}")])
    }

    fn cmd_touch(&self, args: &[String]) -> Result<Vec<String>> {
        let path = self.one_path(args, "touch")?;
        match self.fs.create(path, 0o644) {
            Ok(_) => Ok(vec![format!("created {path}")]),
            Err(Error::AlreadyExists(_)) => Ok(vec![format!("exists {path}")]),
            Err(e) => Err(e),
        }
    }

    fn cmd_cat(&self, args: &[String]) -> Result<Vec<String>> {
        let path = self.one_path(args, "cat")?;
        let node = self.fs.lookup(path)?;
        // Stream to stdout with a bounded buffer (large files never
        // materialize in memory).
        {
            use std::io::Write;
            let (mut reader, _len) = self.fs.read_stream(path, 0, node.size as u64)?;
            let stdout = std::io::stdout();
            let mut lock = stdout.lock();
            std::io::copy(&mut reader, &mut lock)?;
            lock.flush()?;
        }
        // NO trailing newline: cat streams raw bytes and must stay
        // byte-exact for binaries. The caller (the shell binary) prints a
        // line per returned String - returning an empty one used to append
        // a \n to every cat output (found by a real byte-exact comparison
        // on the ECS/VPC run; cat-vs-cat comparisons had cancelled it out).
        Ok(vec![])
    }

    /// write PATH --string TEXT | write PATH --stdin
    fn cmd_write(&self, args: &[String]) -> Result<Vec<String>> {
        let path = args
            .first()
            .ok_or_else(|| Error::InvalidArgument("write needs a path".into()))?;
        let data: Vec<u8> = match args.get(1).map(String::as_str) {
            Some("--string") => args
                .get(2)
                .map(|s| s.as_bytes().to_vec())
                .ok_or_else(|| Error::InvalidArgument("--string needs text".into()))?,
            Some("--stdin") => {
                use std::io::Read;
                let mut buf = Vec::new();
                std::io::stdin().lock().read_to_end(&mut buf)?;
                buf
            }
            _ => {
                return Err(Error::InvalidArgument(
                    "write needs --string or --stdin".into(),
                ));
            }
        };
        // write through regardless of prior existence
        match self.fs.create(path, 0o644) {
            Ok(_) => {}
            Err(Error::AlreadyExists(_)) => {}
            Err(e) => return Err(e),
        }
        let mut session = self.fs.open_write(path, None)?;
        session.write(0, &data)?;
        let node = session.commit()?;
        Ok(vec![format!(
            "wrote {} {}B v{}",
            path, node.size, node.version
        )])
    }

    /// cas-write PATH EXPECTED_VERSION --string TEXT
    /// Optimistic concurrency write for tests/scripts: fails with CONFLICT
    /// when the node's version moved past EXPECTED_VERSION.
    fn cmd_cas_write(&self, args: &[String]) -> Result<Vec<String>> {
        let path = args
            .first()
            .ok_or_else(|| Error::InvalidArgument("cas-write needs a path".into()))?;
        let expected: i64 = args
            .get(1)
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| Error::InvalidArgument("cas-write needs EXPECTED_VERSION".into()))?;
        let text = args.get(3).ok_or_else(|| {
            Error::InvalidArgument("cas-write PATH EXPECTED_VERSION --string TEXT".into())
        })?;
        let mut session = self.fs.open_write(path, Some(expected))?;
        session.write(0, text.as_bytes())?;
        let node = session.commit()?;
        Ok(vec![format!(
            "cas-wrote {} {}B v{}",
            path, node.size, node.version
        )])
    }

    fn cmd_mv(&self, args: &[String]) -> Result<Vec<String>> {
        if args.len() < 2 {
            return Err(Error::InvalidArgument("mv OLD NEW".into()));
        }
        self.fs.rename(&args[0], &args[1])?;
        Ok(vec![format!("moved {} -> {}", args[0], args[1])])
    }

    fn cmd_rm(&self, args: &[String]) -> Result<Vec<String>> {
        let path = self.one_path(args, "rm")?;
        self.fs.unlink(path)?;
        Ok(vec![format!("removed {path}")])
    }

    fn cmd_tree(&self, args: &[String]) -> Result<Vec<String>> {
        let root = args.first().map(String::as_str).unwrap_or("/");
        let mut out = Vec::new();
        out.push(root.to_string());
        self.tree_walk(root, 1, &mut out)?;
        Ok(out)
    }

    fn tree_walk(&self, dir: &str, depth: usize, out: &mut Vec<String>) -> Result<()> {
        if depth > 16 {
            return Ok(()); // safety bound
        }
        for child in self.fs.readdir(dir)? {
            let path = join_dir(dir, &child.name);
            out.push(format!("{}{}", "  ".repeat(depth), child.name));
            if child.is_dir() {
                self.tree_walk(&path, depth + 1, out)?;
            }
        }
        Ok(())
    }

    /// bench-generate DIR COUNT AVG_SIZE: deterministic dataset through the
    /// backend (files sharded 60/dirs; sizes vary ±10% like the target
    /// workload).
    fn cmd_bench_generate(&self, args: &[String]) -> Result<Vec<String>> {
        let dir = self.one_path(args, "bench-generate")?;
        let count: u64 = args.get(1).and_then(|v| v.parse().ok()).unwrap_or(6000);
        let avg: u64 = args.get(2).and_then(|v| v.parse().ok()).unwrap_or(102_400);
        let t0 = std::time::Instant::now();
        let shards = 60u64;
        // base dir and shard dirs; tolerant of reruns
        let _ = self.fs.mkdir(dir, 0o755);
        for s in 0..shards {
            let _ = self.fs.mkdir(&format!("{dir}/shard-{s:02}"), 0o755);
        }
        let mut total: u64 = 0;
        let mut seed: u64 = 0x9E3779B97F4A7C15;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for i in 0..count {
            let shard = i % shards;
            let dir_path = format!("{dir}/shard-{shard:02}");
            let path = format!("{dir_path}/f-{i:06}.dat");
            let size = avg.saturating_sub(avg / 10) + next() % (avg / 5 + 1);
            // ASCII text content so grep-style scans are text-realistic;
            // every file embeds one needle line ("needle-{i}-marker") at a
            // quarter offset for bench-grep to find.
            let mut content: Vec<u8> = (0u64..size)
                .map(|b| (32 + ((b.wrapping_mul(31).wrapping_add(i)) % 95)) as u8)
                .collect();
            let marker = format!("\nneedle-{i}-marker\n");
            let at = (size / 4) as usize;
            if at + marker.len() < content.len() {
                content[at..at + marker.len()].copy_from_slice(marker.as_bytes());
            }
            total += size;
            self.fs.create(&path, 0o644)?;
            let mut ws = self.fs.open_write(&path, None)?;
            ws.write(0, &content)?;
            ws.commit()?;
        }
        let elapsed = t0.elapsed();
        Ok(vec![format!(
            "bench-generate: {count} files, {total} bytes in {:.2}s ({:.1} files/s)",
            elapsed.as_secs_f64(),
            count as f64 / elapsed.as_secs_f64()
        )])
    }

    /// bench-read DIR [CONCURRENCY]: read every file under DIR; reports
    /// totals and time. CONCURRENCY (default 1) bounds in-flight reads;
    /// memory stays bounded by CONCURRENCY x largest file.
    fn cmd_bench_read(&self, args: &[String]) -> Result<Vec<String>> {
        let dir = self.one_path(args, "bench-read")?;
        let concurrency: usize = args
            .get(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
            .clamp(1, 64);
        let t0 = std::time::Instant::now();
        let mut list = Vec::new();
        self.collect_files(dir, &mut list)?;
        let (files, bytes) = self.read_all_bounded(&list, concurrency, &|_| {})?;
        let elapsed = t0.elapsed();
        Ok(vec![format!(
            "bench-read: {files} files, {bytes} bytes in {:.2}s ({:.1} MB/s, {concurrency} in-flight)",
            elapsed.as_secs_f64(),
            bytes as f64 / 1_048_576.0 / elapsed.as_secs_f64()
        )])
    }

    /// bench-grep NEEDLE DIR [CONCURRENCY]: read every file and count
    /// matches — the query-time scan path `rg`/`grep -R` would take (minus
    /// their regex engines). Content is ASCII so the scan is text-realistic.
    fn cmd_bench_grep(&self, args: &[String]) -> Result<Vec<String>> {
        let needle = args
            .first()
            .ok_or_else(|| Error::InvalidArgument("bench-grep NEEDLE DIR".into()))?
            .clone();
        let dir = self.one_path(&args[1..], "bench-grep")?;
        let concurrency: usize = args
            .get(2)
            .and_then(|v| v.parse().ok())
            .unwrap_or(1)
            .clamp(1, 64);
        let t0 = std::time::Instant::now();
        let mut list = Vec::new();
        self.collect_files(dir, &mut list)?;
        let needle_bytes = needle.as_bytes().to_vec();
        let matches = std::sync::atomic::AtomicU64::new(0);
        let (files, bytes) = self.read_all_bounded(&list, concurrency, &|data| {
            if memchr_needle(data, &needle_bytes) {
                matches.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        })?;
        let matches = matches.load(std::sync::atomic::Ordering::Relaxed);
        let elapsed = t0.elapsed();
        Ok(vec![format!(
            "bench-grep: {matches} matching of {files} files, {bytes} bytes scanned in {:.2}s ({:.1} MB/s, {concurrency} in-flight)",
            elapsed.as_secs_f64(),
            bytes as f64 / 1_048_576.0 / elapsed.as_secs_f64()
        )])
    }

    /// Flatten a directory tree into (path, size) pairs.
    fn collect_files(&self, dir: &str, out: &mut Vec<(String, u64)>) -> Result<()> {
        for node in self.fs.readdir(dir)? {
            let path = join_dir(dir, &node.name);
            if node.is_dir() {
                self.collect_files(&path, out)?;
            } else {
                out.push((path, node.size as u64));
            }
        }
        Ok(())
    }

    /// Bounded-concurrency bulk read over a file list: a fixed pool of
    /// scoped threads pulls indices from an atomic queue (no unbounded
    /// buffers; memory stays within CONCURRENCY x largest file). `per_file`
    /// inspects each file's bytes (used by bench-grep).
    fn read_all_bounded(
        &self,
        list: &[(String, u64)],
        concurrency: usize,
        per_file: &(dyn Fn(&[u8]) + Sync),
    ) -> Result<(u64, u64)> {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        let next = AtomicUsize::new(0);
        let total_bytes = AtomicU64::new(0);
        let first_error: Mutex<Option<Error>> = Mutex::new(None);
        std::thread::scope(|s| {
            for _ in 0..concurrency.min(list.len().max(1)) {
                s.spawn(|| {
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= list.len() {
                            break;
                        }
                        let (path, size) = &list[i];
                        match self.fs.read(path, 0, *size) {
                            Ok(data) => {
                                total_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
                                per_file(&data);
                            }
                            Err(e) => {
                                let mut guard = first_error.lock().unwrap();
                                if guard.is_none() {
                                    *guard = Some(e);
                                }
                            }
                        }
                    }
                });
            }
        });
        if let Some(e) = first_error.into_inner().unwrap() {
            return Err(e);
        }
        let files = list.len() as u64;
        let bytes = total_bytes.load(Ordering::Relaxed);
        Ok((files, bytes))
    }

    /// Scripted mode: run a .lfs file. `assert-error CMD...` expects failure.
    /// Any unexpected error aborts with a nonzero status.
    pub fn run_script(&self, script: &str) -> Result<usize> {
        let text = std::fs::read_to_string(script)
            .map_err(|e| Error::Config(format!("cannot read script {script}: {e}")))?;
        let mut executed = 0;
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (expect_err, cmd) = match line.strip_prefix("assert-error") {
                Some(rest) => (true, rest.trim()),
                None => (false, line),
            };
            let result = self.exec(cmd);
            match result {
                Ok(out) => {
                    if expect_err {
                        return Err(Error::InvalidArgument(format!(
                            "{script}:{}: expected error but succeeded: {cmd}",
                            lineno + 1
                        )));
                    }
                    for l in out {
                        println!("{l}");
                    }
                }
                Err(e) => {
                    if expect_err {
                        println!("ok (expected error): {e}");
                    } else {
                        return Err(Error::InvalidArgument(format!(
                            "{script}:{}: {cmd} -> {e}",
                            lineno + 1
                        )));
                    }
                }
            }
            executed += 1;
        }
        Ok(executed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object_store::mem::MemObjectStore;
    use crate::vnext::{Core, Store};
    use std::io::Write;

    fn shell() -> Shell {
        let core =
            Core::new(Store::open_in_memory().unwrap(), Arc::new(MemObjectStore::new())).unwrap();
        Shell::new(Arc::new(core))
    }

    fn temp_script(name: &str, content: &str) -> String {
        let p =
            std::env::temp_dir().join(format!("lightfs-shell-{name}-{}.lfs", std::process::id()));
        let mut f = std::fs::File::create(&p).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        p.to_string_lossy().to_string()
    }

    #[test]
    fn shell_commands() {
        let sh = shell();
        sh.exec("mkdir /docs").unwrap();
        sh.exec("write /docs/a.txt --string hello").unwrap();
        let out = sh.exec("ls /docs").unwrap();
        assert_eq!(out.len(), 1);
        assert!(out[0].ends_with("/docs/a.txt"));
        // cat streams straight to stdout with a bounded buffer; the command
        // itself returns no captured lines.
        assert!(sh.exec("cat /docs/a.txt").is_ok());
        sh.exec("mv /docs/a.txt /docs/b.txt").unwrap();
        assert!(sh.exec("cat /docs/a.txt").is_err());
        sh.exec("rm /docs/b.txt").unwrap();
        assert!(matches!(sh.exec("rm /docs/b.txt"), Err(Error::NotFound(_))));
        let tree = sh.exec("tree /").unwrap();
        assert!(tree.iter().any(|l| l.contains("docs")));
        let stat = sh.exec("stat /docs").unwrap();
        assert!(stat.iter().any(|l| l.starts_with("kind: dir")));
    }

    #[test]
    fn shell_script_mode() {
        let sh = shell();
        let script = temp_script(
            "basic",
            "# comment\nmkdir /a\nwrite /a/x --string data\nassert-error cat /missing\nmv /a/x /a/y\nrm /a/y\nrmdir-x /a\n",
        );
        // rmdir-x is unknown -> script aborts (even without assert-error)
        let err = sh.run_script(&script).unwrap_err();
        assert!(err.to_string().contains("unknown command"));
        let _ = std::fs::remove_file(&script);
    }

    #[test]
    fn shell_script_ok() {
        let sh = shell();
        let script = temp_script(
            "ok",
            "mkdir /proj\nwrite /proj/README --string v1\nassert-error mkdir /proj\ntree /\n",
        );
        let n = sh.run_script(&script).unwrap();
        assert_eq!(n, 4);
        let _ = std::fs::remove_file(&script);
    }
}
