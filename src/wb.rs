//! S3.1 prototype: server-side deferred commit ("write-back" policy).
//!
//! In this mode a flush succeeds once the file's bytes are DURABLY STAGED
//! on the server's local disk AND the task row is committed in the same
//! SQLite database as the metadata — not once the object store has the
//! bytes. Background drain workers then replay staged tasks through the
//! exact same PUT + CAS chain as strict commits, so the correctness model
//! (immutable version keys, single-statement CAS, loser never touches the
//! winner) is inherited unchanged; only its timing moves.
//!
//! Contract boundaries (all deliberately explicit, this is a prototype):
//! - Durability: staging disk survives process death (staging file fsync +
//!   task row in WAL SQLite). Disk loss before drain loses that window's
//!   writes — the write-back boundary, documented not hidden.
//! - Visibility: a second client sees the old content until the drain
//!   commits; the writing client reads its own staged bytes (read-your-
//!   writes via the pending overlay in the read paths).
//! - Same file, same client: a newer queued flush COALESCES into the
//!   queued task (one PUT for N flushes — the W0 load-2 win). A task
//!   already being drained is left alone; the newer flush queues behind
//!   it. Coalescing requires the new expected version to be strictly
//!   newer, so two different clients never silently overwrite each other:
//!   the second one drains into a CAS conflict and dead-letters.
//! - CAS conflict at drain time: the task dead-letters (state='dead',
//!   staged bytes retained for inspection). The winner's commit is never
//!   touched (S1 red line).
//! - unlink while queued: the task is CANCELLED — zero remote requests
//!   (the create-then-delete-before-commit load never reaches the store).
//!   A draining task rides to its CAS: if the node is gone it reports
//!   NotFound and is discarded.
//!
//! Crash recovery: at engine start, draining tasks are requeued (their
//! staging file was already atomically renamed), queued tasks whose
//! staging file is missing go to state='dead' (lost-staging), and staging
//! files no task references are swept (crash between fsync and task
//! insert).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::core::Metrics;
use crate::error::{Error, Result};
use crate::metadata::{MetadataStore, Node};
use crate::object_store::ObjectStore;

#[derive(Clone, Debug)]
pub struct WbOptions {
    pub staging_dir: String,
    /// Drain concurrency. >1 is SUPPORTED and safe, with an explicit
    /// contract (S3.5): every task PUTs its own immutable version key and
    /// lands through a single-statement per-node CAS, so two workers on the
    /// same node yield exactly one winner + one visible conflict regardless
    /// of network completion order; same-client flushes never race because
    /// they COALESCE into one queued task while it is still queued. What
    /// >1 does NOT mean: per-node FIFO ordering across separate tasks
    /// (cross-client conflicts decide, the winner is never touched).
    /// Verified by two_workers_reversed_completion_order_single_winner.
    pub drain_workers: usize,
    /// S3.2 batch-pack drain: > 0 merges queued tasks into immutable pack
    /// objects in the background (one PUT per pack, per-file CAS after).
    /// 0 = per-file drain.
    pub batch_pack_bytes: u64,
    /// Background-only seal wait for a partially-filled batch.
    pub batch_max_wait_ms: u64,
    /// Queued-task bound; 0 = unbounded. At the limit the flushing write
    /// degrades to a synchronous per-file commit instead of erroring.
    pub queue_max_tasks: usize,
}

pub struct WriteBackEngine {
    meta: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    metrics: Arc<Metrics>,
    staging_dir: PathBuf,
    retention_supersede: bool,
    batch_pack_bytes: u64,
    batch_max_wait_ms: u64,
    queue_max_tasks: usize,
    /// Signalled whenever a task is enqueued so idle workers wake.
    wake: Condvar,
    idle: Mutex<usize>,
    stop: AtomicBool,
    /// Test-only determinism seam: fired between the staging-path QUERY and
    /// the staging-file OPEN in read_pending, so a test can run a full
    /// drain inside that window and pin the race deterministically. Never
    /// set outside #[cfg(test)] builds.
    #[cfg(test)]
    pub(crate) test_hook_before_open: Mutex<Option<Box<dyn Fn() + Send>>>,
}

type TaskRow = crate::metadata::WbTask;

impl WriteBackEngine {
    pub(crate) fn new(
        meta: Arc<dyn MetadataStore>,
        objects: Arc<dyn ObjectStore>,
        metrics: Arc<Metrics>,
        opts: &WbOptions,
        retention_supersede: bool,
    ) -> Result<Self> {
        std::fs::create_dir_all(&opts.staging_dir)?;
        Ok(Self {
            meta,
            objects,
            metrics,
            staging_dir: PathBuf::from(&opts.staging_dir),
            retention_supersede,
            batch_pack_bytes: opts.batch_pack_bytes,
            batch_max_wait_ms: opts.batch_max_wait_ms,
            queue_max_tasks: opts.queue_max_tasks,
            wake: Condvar::new(),
            idle: Mutex::new(0),
            stop: AtomicBool::new(false),
            #[cfg(test)]
            test_hook_before_open: Mutex::new(None),
        })
    }

    pub fn staging_dir(&self) -> &Path {
        &self.staging_dir
    }

    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    /// Startup recovery: requeue draining tasks, dead-letter tasks whose
    /// staging vanished, sweep orphaned staging files. Idempotent.
    pub(crate) fn recover(&self) -> Result<()> {
        // Requeue draining tasks: their staging file was already renamed
        // under them, so the recorded path still points at the bytes.
        self.meta.wb_requeue_draining()?;
        // Tasks whose staging file vanished: the durability boundary was
        // hit. Visible dead state, never silent.
        for t in self.active_tasks()? {
            if !Path::new(&t.staging).exists() {
                self.set_state(t.id, "dead")?;
                eprintln!(
                    "lightfs wb: task {id} for node {node} dead: staging file missing (lost staging?)",
                    id = t.id,
                    node = t.node_id
                );
            }
        }
        // Orphan staging files no task references: a crash between staging
        // fsync and task insert. The data has no task; sweep.
        let referenced: std::collections::HashSet<String> =
            self.all_task_stagings()?.into_iter().collect();
        let mut swept = 0usize;
        if let Ok(entries) = std::fs::read_dir(&self.staging_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if !(name.starts_with("wb-") || name.starts_with("drain-")) {
                    continue;
                }
                let p = entry.path();
                let path_str = p.to_string_lossy().to_string();
                if !referenced.contains(&path_str) && std::fs::remove_file(&p).is_ok() {
                    swept += 1;
                }
            }
        }
        if swept > 0 {
            eprintln!("lightfs wb: swept {swept} orphaned staging file(s)");
        }
        Ok(())
    }

    fn active_tasks(&self) -> Result<Vec<TaskRow>> {
        self.meta.wb_active_tasks()
    }

    fn all_task_stagings(&self) -> Result<Vec<String>> {
        self.meta.wb_all_stagings()
    }

    fn set_state(&self, id: i64, state: &str) -> Result<()> {
        self.meta.wb_set_state(id, state, self.now())
    }

    /// Deferred write entry (replaces write_stream's PUT+CAS in
    /// write-back mode). Stage → record → ack. Returns the PROJECTED node:
    /// committed version plus one per still-pending task for the node, so
    /// the client's next flush carries an expected version that will match
    /// reality once the drains land.
    pub(crate) fn write_deferred(
        &self,
        node: &Node,
        expected: i64,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<Node> {
        // Queue bound: when the queue is at the limit the flush WAITS for
        // the drain workers to free a slot (backpressure). Erroring the
        // flush would break the durability promise; committing inline would
        // need a CAS anchor version that does not exist yet (the projected
        // version includes pending tasks). Waiting preserves flush order
        // and the coalescing anchor exactly; the cost shows up as front
        // stage latency ONLY when the queue is actually full, which the
        // wb_overflow counter makes visible.
        if self.queue_max_tasks > 0 {
            let (queued, _, _) = self.meta.wb_counts()?;
            if queued as usize >= self.queue_max_tasks {
                self.metrics.wb_overflow.fetch_add(1, Ordering::Relaxed);
                loop {
                    let (queued, _, _) = self.meta.wb_counts()?;
                    if (queued as usize) < self.queue_max_tasks {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
        // Stage first (durable), record second; a crash in between leaves
        // an orphan staging file that recover() sweeps at next start.
        let staging = self.staging_dir.join(format!(
            "wb-{}-{:x}.data",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        {
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&staging)?;
            {
                let mut w = std::io::BufWriter::with_capacity(512 * 1024, &mut f);
                std::io::copy(reader, &mut w)?;
                w.flush()?;
            }
            f.sync_all()?; // the durability contract: staged bytes survive process death
        }
        let (projected_version, replaced_staging) = self.meta.wb_enqueue(
            node.id,
            staging.to_string_lossy().as_ref(),
            len as i64,
            expected,
            self.now(),
        )?;
        if let Some(old) = replaced_staging {
            self.metrics.wb_coalesced.fetch_add(1, Ordering::Relaxed);
            let _ = std::fs::remove_file(old); // coalesced away; row no longer references it
        }
        self.metrics.wb_enqueued.fetch_add(1, Ordering::Relaxed);
        self.wake.notify_all();
        let mut out = node.clone();
        out.size = len as i64;
        out.version = projected_version;
        // object_key still points at the previous committed object until
        // the drain flips it — stale by the write-back contract.
        Ok(out)
    }

    /// Content of a node's pending staged write, if any (read-your-writes
    /// overlay; also backs projected stat).
    ///
    /// S3.5 lifecycle contract: a task dies through commit CAS → wb_finish
    /// (row delete) → staging unlink, in that order. The only windows where
    /// a concurrent read can be hurt are BETWEEN the staging-path QUERY and
    /// the staging-file OPEN. Both are handled here without locks:
    /// - open hits ENOENT: the drain finished in that window - the task is
    ///   committed now, so report "no pending" and let the caller serve the
    ///   committed object;
    /// - open succeeded: the fd PINS the bytes (POSIX unlink-open
    ///   semantics), so the later wb_finish+unlink cannot invalidate the
    ///   read. A post-open re-validation distinguishes "task still on this
    ///   staging file" (serve the fd) from "a newer flush coalesced over
    ///   this task" (re-resolve, latest wins) from "task finished right
    ///   after the open" (the fd holds exactly the committed bytes - serve
    ///   them).
    /// Returns the slice [offset, offset+len) clamped to the TASK's staged
    /// size (the projected EOF) - callers pass their raw window.
    pub fn read_pending(&self, node_id: i64, offset: u64, len: u64) -> Result<Option<Vec<u8>>> {
        for _ in 0..3 {
            let Some((staging, size)) = self.meta.wb_latest_staging(node_id)? else {
                return Ok(None);
            };
            if offset >= size as u64 || len == 0 {
                return Ok(Some(Vec::new()));
            }
            let end = (offset + len).min(size as u64);
            #[cfg(test)]
            if let Some(hook) = self.test_hook_before_open.lock().unwrap().as_ref() {
                hook(); // test seam: a full drain may land right here
            }
            let mut f = match std::fs::File::open(&staging) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e.into()),
            };
            match self.meta.wb_latest_staging(node_id)? {
                Some((p, _)) if p == staging => {}
                Some(_) => continue, // coalesced onto a newer staging file
                None => {}           // finished right after the open: fd == committed
            }
            use std::io::{Read, Seek, SeekFrom};
            f.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; (end - offset) as usize];
            f.read_exact(&mut buf)?;
            return Ok(Some(buf));
        }
        // Persistently raced (three coalesces in a row): report no pending;
        // the caller re-resolves and serves the committed object, which by
        // then can only be this task's own successor content.
        Ok(None)
    }

    /// Projected stat for a node with pending tasks: (latest staged size,
    /// committed version + pending task count). None = no pending tasks.
    /// Errors propagate: a metadata failure must never silently degrade a
    /// lookup to the un-projected (stale-size) node.
    pub fn projected(&self, node: &Node) -> Result<Option<(i64, i64)>> {
        Ok(self
            .meta
            .wb_projection(node.id)?
            .map(|(size, pending)| (size, node.version + pending)))
    }

    /// Cancel queued tasks for a node (unlink): queued staged bytes never
    /// reach the store. Draining tasks are untouched — their CAS or the
    /// missing node decides the outcome.
    pub fn cancel(&self, node_id: i64) {
        match self.meta.wb_cancel_queued(node_id) {
            Ok(paths) => {
                if !paths.is_empty() {
                    // S3.5: cancellations are a TERMINAL outcome - count
                    // them so enqueued == drained+conflicts+discarded
                    // +cancelled+dead always reconciles.
                    self.metrics
                        .wb_cancelled
                        .fetch_add(paths.len() as u64, Ordering::Relaxed);
                }
                for p in paths {
                    let _ = std::fs::remove_file(p);
                }
            }
            Err(e) => eprintln!("lightfs wb: cancel for node {node_id} failed: {e}"),
        }
    }

    /// Drain worker body, per-file mode. Claims one queued task at a time,
    /// replays it through PUT + CAS.
    pub fn drain_once(&self) -> Result<bool> {
        if self.batch_pack_bytes > 0 {
            return self.drain_batch_once();
        }
        let Some(task) = self.meta.wb_claim(self.now())? else {
            return Ok(false);
        };
        self.drain_task(task)?;
        Ok(true)
    }

    /// Body of draining one claimed task through the exact same chain as
    /// strict write_stream: immutable version key → PUT → CAS → retire.
    fn drain_task(&self, task: TaskRow) -> Result<()> {
        let content = match std::fs::read(&task.staging) {
            Ok(c) => c,
            Err(e) => {
                self.set_state(task.id, "dead")?;
                eprintln!(
                    "lightfs wb: task {id} dead: staging unreadable: {e}",
                    id = task.id
                );
                return Ok(());
            }
        };
        if content.len() as i64 != task.size {
            self.set_state(task.id, "dead")?;
            eprintln!(
                "lightfs wb: task {id} dead: staged {got} bytes != declared {want}",
                id = task.id,
                got = content.len(),
                want = task.size
            );
            return Ok(());
        }
        // Same chain as strict write_stream_node: immutable version key →
        // CAS → retire. A losing writer cleans its own key up.
        let node = match self.meta.get_node(task.node_id)? {
            Some(n) => n,
            None => {
                // unlinked while queued/draining: discard silently (this is
                // the delete-before-commit cancel outcome)
                self.meta.wb_finish(task.id)?;
                let _ = std::fs::remove_file(&task.staging);
                return Ok(());
            }
        };
        let version_key = crate::core::new_version_key_for(task.node_id);
        let put = self.objects.put(&version_key, &content);
        self.metrics.s3_put.fetch_add(1, Ordering::Relaxed);
        let outcome = match put {
            Ok(info) => self.meta.commit_content_cas(
                task.node_id,
                task.expected,
                task.size,
                self.now(),
                &version_key,
                &info.etag,
            ),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(()) => {
                self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
                self.metrics.wb_cas_ok.fetch_add(1, Ordering::Relaxed);
                if let Some(old_key) = &node.object_key {
                    self.retire(old_key);
                }
                self.meta.wb_finish(task.id)?;
                let _ = std::fs::remove_file(&task.staging);
                self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
            }
            Err(Error::Conflict { .. }) => {
                // Crash between CAS and wb_finish: the commit LANDED - the
                // recovered task must finish as a success, never as a
                // conflict (S3.5 recovery semantics).
                if self.task_already_committed(&task, &content)? {
                    self.meta.wb_finish(task.id)?;
                    let _ = std::fs::remove_file(&task.staging);
                    self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                // Never corrupt the winner: dead-letter with bytes retained.
                self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
                self.metrics.wb_conflicts.fetch_add(1, Ordering::Relaxed);
                let _ = self.objects.delete(&version_key);
                self.set_state(task.id, "dead")?;
                eprintln!(
                    "lightfs wb: task {id} for node {node} dead: CAS conflict (winner kept)",
                    id = task.id,
                    node = task.node_id
                );
            }
            Err(Error::NotFound(_)) => {
                // raced with unlink after the get_node above: TERMINAL
                // discard, counted (S3.5 - never a silent disappearance)
                self.metrics.wb_discarded.fetch_add(1, Ordering::Relaxed);
                let _ = self.objects.delete(&version_key);
                self.meta.wb_finish(task.id)?;
                let _ = std::fs::remove_file(&task.staging);
            }
            Err(e) => {
                let _ = self.objects.delete(&version_key);
                // transient: back to the queue with a bounded retry budget
                if !self.meta.wb_retry_or_dead(task.id, 3, self.now())? {
                    eprintln!(
                        "lightfs wb: task {id} dead after retries: {e}",
                        id = task.id
                    );
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
        Ok(())
    }

    /// S3.2 batch-pack drain: merge queued tasks' staged bytes into ONE
    /// immutable pack object (one PUT), then CAS each logical file
    /// individually against its enqueued expected version. All waiting
    /// happens here on the worker — the front stage never waits for a
    /// batch, a seal, or an upload.
    pub fn drain_batch_once(&self) -> Result<bool> {
        let max_bytes = self.batch_pack_bytes;
        let mut entries: Vec<(TaskRow, usize, usize)> = Vec::new();
        let mut buf: Vec<u8> = Vec::new();
        let mut first_at: Option<std::time::Instant> = None;
        loop {
            if self.stop.load(Ordering::Relaxed) && !entries.is_empty() {
                break;
            }
            let fit = (max_bytes - buf.len() as u64).min(i64::MAX as u64) as i64;
            match self.meta.wb_claim_fit(fit, self.now())? {
                None => {
                    if entries.is_empty() {
                        // nothing fits: either the queue is dry, or only
                        // oversized tasks remain — take one unconditionally
                        // and drain it per-file so it can never starve.
                        return match self.meta.wb_claim(self.now())? {
                            None => Ok(false),
                            Some(big) => {
                                self.drain_task(big)?;
                                Ok(true)
                            }
                        };
                    }
                    let waited_long_enough = match first_at {
                        Some(t0) => t0.elapsed() >= Duration::from_millis(self.batch_max_wait_ms),
                        None => true,
                    };
                    if !waited_long_enough {
                        std::thread::sleep(Duration::from_millis(2));
                        continue; // give coalescing stragglers a chance
                    }
                    break; // seal what we have
                }
                Some(task) => {
                    if first_at.is_none() {
                        first_at = Some(std::time::Instant::now());
                    }
                    let content = match std::fs::read(&task.staging) {
                        Ok(c) if c.len() as i64 == task.size => c,
                        Ok(c) => {
                            self.set_state(task.id, "dead")?;
                            eprintln!(
                                "lightfs wb: task {id} dead: staged {got} != declared {want}",
                                id = task.id,
                                got = c.len(),
                                want = task.size
                            );
                            continue;
                        }
                        Err(e) => {
                            self.set_state(task.id, "dead")?;
                            eprintln!(
                                "lightfs wb: task {id} dead: staging unreadable: {e}",
                                id = task.id
                            );
                            continue;
                        }
                    };
                    entries.push((task, buf.len(), content.len()));
                    buf.extend_from_slice(&content);
                    if buf.len() as u64 >= max_bytes {
                        break; // pack budget reached
                    }
                }
            }
        }
        self.seal_batch(entries, buf)?;
        Ok(true)
    }

    /// Upload one assembled pack and resolve every entry's CAS. The PUT
    /// runs while holding NO shared lock — only this worker's entries are
    /// in flight, and sibling tasks stay claimable throughout.
    fn seal_batch(&self, entries: Vec<(TaskRow, usize, usize)>, buf: Vec<u8>) -> Result<()> {
        let pack_key = format!(
            "{}wb-{:x}-{:x}.data",
            crate::pack::PACK_KEY_PREFIX,
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        // Register the pack row (uploaded_at NULL) before the upload — the
        // same "intent before object" invariant the strict pack engine uses.
        self.meta.register_pack(&pack_key, buf.len() as i64)?;
        let put = self.objects.put(&pack_key, &buf);
        self.metrics.s3_put.fetch_add(1, Ordering::Relaxed);
        if put.is_ok() {
            self.metrics.wb_packs.fetch_add(1, Ordering::Relaxed);
        }
        let put = match put {
            Ok(info) => info,
            Err(e) => {
                // upload failed: nothing references the pack yet — undo row
                // + object, requeue the batch with the retry budget.
                let _ = self.objects.delete(&pack_key);
                let _ = self.meta.drop_pack(&pack_key);
                for (task, _, _) in &entries {
                    if !self.meta.wb_retry_or_dead(task.id, 3, self.now())? {
                        eprintln!(
                            "lightfs wb: task {id} dead after retries: {e}",
                            id = task.id
                        );
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
                return Ok(());
            }
        };
        let _ = put;
        self.meta.mark_pack_uploaded(&pack_key, self.now())?;
        for (task, off, len) in entries {
            let hash = crate::pack::content_hash(&buf[off..off + len]);
            // Errors propagate: a get_node failure must not silently skip
            // the retire of the previous generation (S3.5 swallow fix).
            let old_key = match self.meta.get_node(task.node_id)? {
                Some(n) => n.object_key,
                None => None,
            };
            let outcome = self.meta.commit_content_cas_pack(
                task.node_id,
                task.expected,
                task.size,
                self.now(),
                &pack_key,
                off as i64,
                len as i64,
                &hash,
            );
            match outcome {
                Ok(()) => {
                    self.metrics.metadata_tx.fetch_add(1, Ordering::Relaxed);
                    self.metrics.wb_cas_ok.fetch_add(1, Ordering::Relaxed);
                    if let Some(k) = old_key {
                        self.retire(&k);
                    }
                    self.meta.wb_finish(task.id)?;
                    let _ = std::fs::remove_file(&task.staging);
                    self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
                }
                Err(Error::Conflict { .. }) => {
                    // Crash between the per-file CAS and wb_finish: the
                    // commit landed - finish as a success (S3.5 recovery).
                    let staged = &buf[off..off + len];
                    if self.task_already_committed(&task, staged)? {
                        self.meta.wb_finish(task.id)?;
                        let _ = std::fs::remove_file(&task.staging);
                        self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    // The winner stands; this entry's bytes stay in the pack
                    // as unreferenced residue (pack gc reclaims them).
                    self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
                    self.metrics.wb_conflicts.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .pack_dead_bytes
                        .fetch_add(len as u64, Ordering::Relaxed);
                    self.set_state(task.id, "dead")?;
                    eprintln!(
                        "lightfs wb: task {id} for node {node} dead: CAS conflict (pack sibling bytes kept for gc)",
                        id = task.id,
                        node = task.node_id
                    );
                }
                Err(Error::NotFound(_)) => {
                    // unlinked while the batch was in flight: TERMINAL
                    // discard, counted (S3.5)
                    self.metrics.wb_discarded.fetch_add(1, Ordering::Relaxed);
                    self.meta.wb_finish(task.id)?;
                    let _ = std::fs::remove_file(&task.staging);
                }
                Err(e) => {
                    // transient CAS failure: retry via the queue. The entry
                    // will be re-staged as its own task in a later pack.
                    if !self.meta.wb_retry_or_dead(task.id, 3, self.now())? {
                        eprintln!(
                            "lightfs wb: task {id} dead after retries: {e}",
                            id = task.id
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// True when `task`'s bytes are ALREADY the node's committed state -
    /// the signature of a crash between the content CAS and wb_finish. The
    /// recovered task must then complete as a success, never dead-letter as
    /// a conflict. Verified by CONTENT (bytes for per-file objects, the
    /// committed pack range for pack-backed nodes), not just version arith.
    fn task_already_committed(&self, task: &TaskRow, staged: &[u8]) -> Result<bool> {
        let Some(n) = self.meta.get_node(task.node_id)? else {
            return Ok(false);
        };
        if n.version != task.expected + 1 || n.size != task.size {
            return Ok(false);
        }
        let Some(key) = &n.object_key else {
            return Ok(false);
        };
        // Object unreadable = cannot verify = NOT committed (conservative:
        // the normal conflict path then decides, the winner stays intact).
        let read = if let (Some(off), Some(plen)) = (n.pack_off, n.pack_len) {
            self.objects.get(key, Some(crate::object_store::Range {
                start: off as u64,
                len: plen as u64,
            }))
        } else {
            self.objects.get(key, None)
        };
        match read {
            Ok(d) => Ok(d.data == staged),
            Err(_) => Ok(false),
        }
    }

    fn retire(&self, key: &str) {
        if key.starts_with(crate::pack::PACK_KEY_PREFIX) {
            return; // shared pack: lifecycle owned by pack gc
        }
        if !(self.retention_supersede && self.meta.record_superseded(key, self.now()).is_ok())
            && self.objects.delete(key).is_ok()
        {
            self.metrics.s3_delete.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Spawn `workers` drain threads. Prototype lifetime: detached threads
    /// that end when the process ends or `stop()` is called.
    pub(crate) fn spawn_workers(self: &Arc<Self>, workers: usize) {
        for _ in 0..workers.max(1) {
            let eng = self.clone();
            std::thread::spawn(move || {
                loop {
                    if eng.stop.load(Ordering::Relaxed) {
                        return;
                    }
                    match eng.drain_once() {
                        Ok(true) => {}
                        Ok(false) => {
                            // queue empty: park until signalled (bounded, so a
                            // missed wakeup costs 200ms, not a hang)
                            let mut idle = eng.idle.lock().unwrap();
                            *idle += 1;
                            let (mut idle, timeout) = eng
                                .wake
                                .wait_timeout(idle, Duration::from_millis(200))
                                .unwrap();
                            *idle -= 1;
                            drop((idle, timeout));
                        }
                        Err(e) => {
                            eprintln!("lightfs wb: drain error: {e}");
                            std::thread::sleep(Duration::from_millis(200));
                        }
                    }
                }
            });
        }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
        self.wake.notify_all();
    }

    /// Queue snapshot for observability: (queued, draining, dead).
    pub fn queue_counts(&self) -> (u64, u64, u64) {
        self.meta.wb_counts().unwrap_or((0, 0, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Core;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::object_store::mem::MemObjectStore;
    use std::sync::Arc as StdArc;

    fn wb_core(tag: &str) -> Core {
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wb-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0, // manual drain in these tests
                batch_pack_bytes: 0,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
            }),
        )
        .unwrap()
    }

    fn write_via_rpc(c: &Core, path: &str, content: &[u8], expected: Option<i64>) -> Node {
        // The FUSE client path: full-content write through write_stream.
        c.write_stream(
            path,
            expected,
            false,
            &mut std::io::Cursor::new(content.to_vec()),
            content.len() as u64,
        )
        .unwrap()
    }

    #[test]
    fn deferred_write_is_invisible_then_drains() {
        let c = wb_core("vis");
        c.create("/f", 0o644).unwrap();
        let n = write_via_rpc(&c, "/f", b"staged bytes", Some(0));
        assert_eq!(n.version, 1, "projected version");
        // not yet on the store; the overlay serves the staged bytes
        assert_eq!(c.read("/f", 0, 100).unwrap(), b"staged bytes");
        let vkey = c.lookup("/f").unwrap().object_key;
        assert!(vkey.is_none(), "no object committed yet");
        // drain replays through the strict chain
        assert!(c.wb().unwrap().drain_once().unwrap());
        let after = c.lookup("/f").unwrap();
        assert_eq!(after.version, 1);
        assert!(after.object_key.is_some());
        let key = after.object_key.clone().unwrap();
        assert_eq!(c.objects().get(&key, None).unwrap().data, b"staged bytes");
        // queue is empty; overlay no longer needed
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 0));
        assert_eq!(c.read("/f", 0, 100).unwrap(), b"staged bytes");
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn coalesced_rewrites_cost_one_put() {
        let c = wb_core("coalesce");
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", b"gen one", Some(0));
        // second flush with the projected version coalesces into the queued task
        let _ = write_via_rpc(&c, "/f", b"generation two wins", Some(1));
        assert_eq!(c.wb().unwrap().queue_counts().0, 1, "one queued task");
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert!(
            !c.wb().unwrap().drain_once().unwrap(),
            "queue empty after one drain"
        );
        let after = c.lookup("/f").unwrap();
        assert_eq!(after.version, 1, "two flushes, one drain, one version bump");
        assert_eq!(
            c.objects()
                .get(after.object_key.as_deref().unwrap(), None)
                .unwrap()
                .data,
            b"generation two wins"
        );
        assert_eq!(c.metrics.s3_put.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cross_client_conflict_dead_letters_and_keeps_winner() {
        let c = wb_core("conflict");
        c.create("/f", 0o644).unwrap();
        // two clients both observed version 0
        let _ = write_via_rpc(&c, "/f", b"client A staged", Some(0));
        // B's equal expected version cannot coalesce onto A's task:
        // write B through the engine directly at the same expected version
        {
            let eng = c.wb().unwrap();
            let node = c.lookup("/f").unwrap();
            let out = eng
                .write_deferred(
                    &node,
                    0,
                    &mut std::io::Cursor::new(b"client B staged".to_vec()),
                    15,
                )
                .unwrap();
            assert_eq!(out.version, 2, "projection counts both pending tasks");
        }
        // drain A's task: CAS wins
        assert!(c.wb().unwrap().drain_once().unwrap());
        // drain B's task: CAS conflict → dead-letter, A's bytes stand
        assert!(c.wb().unwrap().drain_once().unwrap());
        let after = c.lookup("/f").unwrap();
        assert_eq!(
            c.objects()
                .get(after.object_key.as_deref().unwrap(), None)
                .unwrap()
                .data,
            b"client A staged"
        );
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 1));
        assert_eq!(c.metrics.wb_conflicts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn unlink_before_commit_cancels_with_zero_puts() {
        let c = wb_core("cancel");
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", b"never committed", Some(0));
        c.unlink("/f").unwrap();
        assert_eq!(
            c.metrics.s3_put.load(Ordering::Relaxed),
            0,
            "cancel = zero remote writes"
        );
        assert!(!c.wb().unwrap().drain_once().unwrap(), "queue empty");
        assert!(c.lookup("/f").is_err());
    }

    #[test]
    fn read_your_writes_serves_slices_of_staged_bytes() {
        let c = wb_core("overlay");
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", b"0123456789", Some(0));
        assert_eq!(c.read("/f", 2, 4).unwrap(), b"2345");
        assert_eq!(c.read("/f", 8, 100).unwrap(), b"89");
        assert_eq!(c.read("/f", 100, 10).unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn projected_stat_reflects_staged_size_for_base_reads() {
        let c = wb_core("proj");
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", vec![b'a'; 100].as_slice(), Some(0));
        // the client's commit chain reads lookup_by_id().size to assemble
        // the base: it must see the staged size, not the stale 0
        let node = c.lookup_by_id(c.lookup("/f").unwrap().id).unwrap();
        assert_eq!(node.size, 100);
        assert_eq!(node.version, 1);
    }

    fn batch_core(tag: &str, pack_bytes: u64, queue_max: usize) -> Core {
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wbb-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: pack_bytes,
                batch_max_wait_ms: 0, // no straggler wait in tests
                queue_max_tasks: queue_max,
            }),
        )
        .unwrap()
    }

    #[test]
    fn batch_drain_merges_files_into_one_pack() {
        let c = batch_core("merge", 1 << 20, 0);
        let mut nodes = Vec::new();
        for (i, body) in [
            b"alpha-content".as_slice(),
            b"beta-content",
            b"gamma-content",
        ]
        .into_iter()
        .enumerate()
        {
            let path = format!("/f{i}");
            c.create(&path, 0o644).unwrap();
            nodes.push(write_via_rpc(&c, &path, body, Some(0)));
        }
        assert!(c.wb().unwrap().drain_once().unwrap(), "one batch drain");
        assert!(
            !c.wb().unwrap().drain_once().unwrap(),
            "queue empty after one seal"
        );
        assert_eq!(c.metrics.s3_put.load(Ordering::Relaxed), 1, "one pack PUT");
        assert_eq!(c.metrics.wb_packs.load(Ordering::Relaxed), 1);
        // every file: pack-backed, byte-correct, independently versioned
        let mut pack_keys = std::collections::HashSet::new();
        for (i, _) in [0usize; 3].into_iter().enumerate() {
            let n = c.lookup(&format!("/f{i}")).unwrap();
            let key = n.object_key.as_deref().unwrap();
            assert!(key.starts_with(crate::pack::PACK_KEY_PREFIX), "{key}");
            assert!(n.pack_len.is_some());
            pack_keys.insert(key.to_string());
            let got = c.read(&format!("/f{i}"), 0, 1000).unwrap();
            let want = [
                b"alpha-content".as_slice(),
                b"beta-content",
                b"gamma-content",
            ][i];
            assert_eq!(got, want);
        }
        assert_eq!(pack_keys.len(), 1, "all three share one pack");
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 3);
        assert_eq!(c.metrics.pack_dead_bytes.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn batch_respects_pack_budget_and_drains_oversized_per_file() {
        let c = batch_core("budget", 64, 0);
        // three 40-byte tasks: two fit a 64-byte pack, one goes per-file
        for i in 0..3 {
            let path = format!("/g{i}");
            c.create(&path, 0o644).unwrap();
            let _ = write_via_rpc(&c, &path, &vec![b'x' + i as u8; 40][..], Some(0));
        }
        // 40-byte tasks against a 64-byte budget: packs must split
        for _ in 0..10 {
            if !c.wb().unwrap().drain_once().unwrap() {
                break;
            }
        }
        let packs = c.metrics.wb_packs.load(Ordering::Relaxed);
        let per_file = c.metrics.s3_put.load(Ordering::Relaxed) - packs;
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 3);
        assert!(
            packs >= 2,
            "budget 64 with 40-byte files must split: packs={packs} per_file={per_file}"
        );
        for i in 0..3 {
            let n = c.lookup(&format!("/g{i}")).unwrap();
            let got = c.read(&format!("/g{i}"), 0, 100).unwrap();
            assert_eq!(got, vec![b'x' + i as u8; 40]);
            let _ = n;
        }
    }

    #[test]
    fn batch_conflict_dead_letters_without_breaking_pack_siblings() {
        let c = batch_core("conflict", 1 << 20, 0);
        c.create("/f", 0o644).unwrap();
        let node = c.lookup("/f").unwrap();
        // two clients staged against the SAME committed version: both
        // tasks queue separately and land in the SAME pack
        let _ = write_via_rpc(&c, "/f", b"client A wins this", Some(0));
        {
            let eng = c.wb().unwrap();
            eng.write_deferred(
                &node,
                0,
                &mut std::io::Cursor::new(b"client B loses".to_vec()),
                14,
            )
            .unwrap();
        }
        // one batch drain seals both entries into one pack, then CASes
        // each: A wins v1, B dead-letters with its bytes left in the pack
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert!(!c.wb().unwrap().drain_once().unwrap(), "queue empty");
        let after = c.lookup("/f").unwrap();
        assert_eq!(after.version, 1, "one commit, one winner");
        assert_eq!(c.read("/f", 0, 100).unwrap(), b"client A wins this");
        assert_eq!(c.metrics.wb_conflicts.load(Ordering::Relaxed), 1);
        assert_eq!(
            c.metrics.pack_dead_bytes.load(Ordering::Relaxed),
            14,
            "loser's bytes remain in the shared pack as residue"
        );
        // the winner's pack-backed object exists and serves the bytes
        let key = after.object_key.unwrap();
        assert!(key.starts_with(crate::pack::PACK_KEY_PREFIX));
        assert_eq!(c.objects().get(&key, None).unwrap().data.len(), 32);
    }

    #[test]
    fn batch_upload_failure_requeues_and_recovers() {
        struct FlakyThenOk {
            inner: MemObjectStore,
            fail_first: std::sync::atomic::AtomicBool,
        }
        impl crate::object_store::ObjectStore for FlakyThenOk {
            fn put(
                &self,
                key: &str,
                data: &[u8],
            ) -> crate::error::Result<crate::object_store::ObjectInfo> {
                if key.starts_with(crate::pack::PACK_KEY_PREFIX)
                    && self.fail_first.swap(false, Ordering::Relaxed)
                {
                    return Err(crate::error::Error::Storage(
                        "simulated upload outage".into(),
                    ));
                }
                self.inner.put(key, data)
            }
            fn put_stream(
                &self,
                key: &str,
                reader: &mut dyn std::io::Read,
                len: u64,
            ) -> crate::error::Result<crate::object_store::ObjectInfo> {
                self.inner.put_stream(key, reader, len)
            }
            fn get(
                &self,
                key: &str,
                range: Option<crate::object_store::Range>,
            ) -> crate::error::Result<crate::object_store::ObjectData> {
                self.inner.get(key, range)
            }
            fn get_stream(
                &self,
                key: &str,
                range: Option<crate::object_store::Range>,
            ) -> crate::error::Result<(Box<dyn std::io::Read + Send>, u64)> {
                self.inner.get_stream(key, range)
            }
            fn head(&self, key: &str) -> crate::error::Result<crate::object_store::ObjectInfo> {
                self.inner.head(key)
            }
            fn delete(&self, key: &str) -> crate::error::Result<()> {
                self.inner.delete(key)
            }
            fn list(
                &self,
                prefix: &str,
                token: Option<&str>,
                max: usize,
            ) -> crate::error::Result<crate::object_store::ListPage> {
                self.inner.list(prefix, token, max)
            }
            fn ping(&self) -> crate::error::Result<()> {
                self.inner.ping()
            }
        }
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wbf-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = StdArc::new(FlakyThenOk {
            inner: MemObjectStore::new(),
            fail_first: std::sync::atomic::AtomicBool::new(true),
        });
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            store,
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 1 << 20,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
            }),
        )
        .unwrap();
        c.create("/resilient", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/resilient", b"survives an outage", Some(0));
        // first attempt: upload fails, task requeued
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 0);
        assert_eq!(c.wb().unwrap().queue_counts().0, 1, "back in the queue");
        // second attempt: succeeds
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 1);
        assert_eq!(c.read("/resilient", 0, 100).unwrap(), b"survives an outage");
        // the failed pack left no row and no object behind
        assert_eq!(c.metadata().all_packs().unwrap().len(), 1);
    }

    #[test]
    fn queue_full_applies_backpressure_and_then_drains() {
        let c = batch_core("backpressure", 1 << 20, 1); // queue cap 1
        c.create("/hot", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/hot", b"occupies the queue", Some(0));
        // background drainer: keeps freeing slots while the front stage
        // writes under backpressure. The handle is JOINED before the
        // assertions below - the only race-free way to know every claim
        // has settled (wb_drained increments after the row delete).
        let eng = std::sync::Arc::clone(c.wb().unwrap());
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let drainer = {
            let stop = std::sync::Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if !eng.drain_once().unwrap() {
                        std::thread::sleep(Duration::from_millis(2));
                    }
                }
            })
        };
        // 20 flushes against a cap of 1: some MUST observe the full queue
        for i in 0..20 {
            let _ = write_via_rpc(&c, "/hot", format!("generation {i}").as_bytes(), None);
        }
        // wait for the front stage to finish, stop the drainer, JOIN it
        // (every claim settled), then synchronously finish the tail
        for _ in 0..5000 {
            if c.wb().unwrap().queue_counts().0 == 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        stop.store(true, Ordering::Relaxed);
        drainer.join().unwrap();
        while c.wb().unwrap().drain_once().unwrap() {}
        assert_eq!(c.wb().unwrap().queue_counts().0, 0);
        assert!(
            c.metrics.wb_overflow.load(Ordering::Relaxed) > 0,
            "backpressure must have been hit with cap=1 and 20 flushes"
        );
        assert_eq!(
            c.metrics.wb_drained.load(Ordering::Relaxed),
            21,
            "initial + 20 flushes"
        );
        let fin = c.lookup("/hot").unwrap();
        assert_eq!(fin.version, 21, "initial commit + 20 flushes");
        assert_eq!(
            c.read("/hot", 0, 100).unwrap(),
            b"generation 19",
            "last flush must be the surviving content"
        );
    }

    #[test]
    fn crash_recovery_requeues_and_detects_lost_staging() {
        // covered at the metadata layer in sqlite tests (wb_requeue_draining,
        // dead on missing staging); the engine-level recover() runs in
        // with_options startup for file-backed stores.
    }

    // ------------------------------------------------------------------
    // S3.5 red tests: deterministic interleavings that the read snapshot
    // rework must survive. Each test pins one production interleaving via
    // the cfg(test) hook or a pre-drain node snapshot — no sleeps.
    // ------------------------------------------------------------------

    /// Red: the drain lands between the staging-path QUERY and the staging
    /// file OPEN. The task row is gone and the staging file is removed; the
    /// read must still serve the correct complete bytes (they are, at that
    /// instant, the committed content), never ENOENT.
    #[test]
    fn red_staging_vanishes_between_query_and_open() {
        let c = wb_core("red-open");
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", b"staged bytes", Some(0));
        {
            let eng = std::sync::Arc::clone(c.wb().unwrap());
            let eng_in_hook = std::sync::Arc::clone(&eng);
            *eng.test_hook_before_open.lock().unwrap() = Some(Box::new(move || {
                eng_in_hook.drain_once().unwrap(); // commit + wb_finish + remove staging
            }));
        }
        let got = c.read("/f", 0, 100);
        assert_eq!(
            got.unwrap(),
            b"staged bytes",
            "drain between query and open must not break the read"
        );
    }
    /// Evidence-chain test (S3.5 phase 3 shape): several independent files
    /// staged, rename and unlink interleaved, one batch drain. Every
    /// survivor must read back byte-exact; every task must reach a terminal
    /// state; the committed metadata refs must match what actually serves
    /// the bytes. Old implementation included: this documents the invariants
    /// the read/commit lifecycle must keep under rename/unlink pressure.
    #[test]
    fn mixed_batch_with_rename_and_unlink_accounting() {
        let c = batch_core("mixed", 1 << 20, 0);
        let bodies: [&[u8]; 6] = [
            b"file-zero", b"file-one", b"file-two", b"file-three", b"file-four", b"file-five",
        ];
        let mut expect: Vec<(String, &[u8])> = Vec::new();
        for (i, body) in bodies.iter().enumerate() {
            let path = format!("/m{i}");
            c.create(&path, 0o644).unwrap();
            let _ = write_via_rpc(&c, &path, body, Some(0));
            expect.push((path, body));
        }
        // interleave metadata churn over staged-but-undrained files
        c.rename("/m1", "/m1-renamed").unwrap();
        c.unlink("/m3").unwrap(); // queued → cancelled
        c.rename("/m4", "/m4-renamed").unwrap();
        c.unlink("/m5").unwrap(); // queued → cancelled
        // drain everything
        for _ in 0..10 {
            if !c.wb().unwrap().drain_once().unwrap() {
                break;
            }
        }
        // survivors: byte-exact under their CURRENT path (two were renamed
        // while their bytes were still staged)
        let survivor = |p: &str| -> String {
            if p == "/m1" {
                "/m1-renamed".to_string()
            } else if p == "/m4" {
                "/m4-renamed".to_string()
            } else {
                p.to_string()
            }
        };
        for (path, body) in &expect {
            if *path == "/m3" || *path == "/m5" {
                continue;
            }
            let cur = survivor(path);
            assert_eq!(
                c.read(&cur, 0, 1000).unwrap(),
                *body,
                "{} content after mixed batch", cur
            );
        }
        // the unlinked pair: identity gone, no residue rows
        assert!(matches!(
            c.lookup("/m3"),
            Err(Error::NotFound(_)) | Err(crate::error::Error::IdentityGone(_))
        ));
        assert_eq!(c.wb().unwrap().queue_counts().0, 0, "nothing queued");
        assert_eq!(c.wb().unwrap().queue_counts().1, 0, "nothing draining");
        assert_eq!(
            c.metrics.wb_drained.load(Ordering::Relaxed),
            4,
            "4 committed (2 cancelled before drain never reach CAS)"
        );
        // every live ref resolves to real pack content with the right bytes
        for (path, body) in &expect {
            if *path == "/m3" || *path == "/m5" {
                continue;
            }
            let cur = survivor(path);
            let n = c.lookup(&cur).unwrap();
            let key = n.object_key.clone().unwrap();
            assert!(key.starts_with(crate::pack::PACK_KEY_PREFIX));
            let got = c.read(&cur, 0, 1000).unwrap();
            assert_eq!(got, *body);
        }
    }

    /// S3.5 recovery semantics: a task whose CAS committed but which died
    /// before wb_finish must recover as a SUCCESS, never as a conflict
    /// dead-letter. Simulated with a file-backed store + engine restart
    /// (recover()), exactly the server startup path.
    #[test]
    fn cas_committed_task_recovers_as_success_not_conflict() {
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wbrec-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db = dir.join("meta.db");
        let staging = dir.join("staging");
        let opts = |staging: &String| WbOptions {
            staging_dir: staging.clone(),
            drain_workers: 0,
            batch_pack_bytes: 0,
            batch_max_wait_ms: 0,
            queue_max_tasks: 0,
        };
        let body = b"survives a crash";
        let store: StdArc<dyn crate::object_store::ObjectStore> = StdArc::new(MemObjectStore::new());
        let node_id = {
            let c = Core::with_options(
                StdArc::new(SqliteMetadataStore::open(db.to_string_lossy().as_ref()).unwrap()),
                store.clone(),
                crate::core::DEFAULT_SUPERSEDED_SECS,
                None,
                Some(opts(&staging.to_string_lossy().to_string())),
            )
            .unwrap();
            c.create("/f", 0o644).unwrap();
            let n = write_via_rpc(&c, "/f", body, Some(0));
            // crash window: replay the CAS exactly as drain_task would,
            // then "die" before wb_finish
            let task = c.metadata().wb_active_tasks().unwrap().pop().unwrap();
            let vkey = crate::core::new_version_key_for(task.node_id);
            c.objects()
                .put(&vkey, body)
                .unwrap();
            c.metadata()
                .commit_content_cas(
                    task.node_id,
                    task.expected,
                    task.size,
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                    &vkey,
                    "etag-crash",
                )
                .unwrap();
            n.id
            // Core dropped here = process death with the task still 'draining'
        };
        // restart: recovery requeues the draining task, then the drain must
        // recognize its own commit and FINISH it - not dead-letter it
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open(db.to_string_lossy().as_ref()).unwrap()),
            store,
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(opts(&staging.to_string_lossy().to_string())),
        )
        .unwrap();
        c.wb().unwrap().recover().unwrap();
        assert_eq!(c.wb().unwrap().queue_counts(), (1, 0, 0), "requeued");
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(
            c.wb().unwrap().queue_counts(),
            (0, 0, 0),
            "finished as success - zero dead rows"
        );
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 1);
        assert_eq!(c.metrics.wb_conflicts.load(Ordering::Relaxed), 0);
        let after = c.lookup_by_id(node_id).unwrap();
        assert_eq!(c.read("/f", 0, 100).unwrap(), body);
        assert_eq!(after.size, body.len() as i64);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// S3.5 red test 4: two drain workers, two tasks on ONE node (cross-
    /// client same-expected), network completion order REVERSED via a
    /// delay-injecting store. CAS anchoring must yield exactly one winner,
    /// one visible conflict, zero silent losses - multi-worker drain is
    /// safe BECAUSE every task PUTs its own immutable version key.
    #[test]
    fn two_workers_reversed_completion_order_single_winner() {
        struct SlowFirstPut {
            inner: MemObjectStore,
            gate: std::sync::atomic::AtomicU64,
        }
        impl crate::object_store::ObjectStore for SlowFirstPut {
            fn put(
                &self,
                key: &str,
                data: &[u8],
            ) -> crate::error::Result<crate::object_store::ObjectInfo> {
                // delay the FIRST per-file put to land: worker A's upload
                // finishes AFTER worker B's, reversing network order
                if !key.starts_with(crate::pack::PACK_KEY_PREFIX)
                    && self.gate.fetch_add(1, Ordering::SeqCst) == 0
                {
                    std::thread::sleep(Duration::from_millis(120));
                }
                self.inner.put(key, data)
            }
            fn put_stream(
                &self,
                key: &str,
                reader: &mut dyn std::io::Read,
                len: u64,
            ) -> crate::error::Result<crate::object_store::ObjectInfo> {
                self.inner.put_stream(key, reader, len)
            }
            fn get(
                &self,
                key: &str,
                range: Option<crate::object_store::Range>,
            ) -> crate::error::Result<crate::object_store::ObjectData> {
                self.inner.get(key, range)
            }
            fn get_stream(
                &self,
                key: &str,
                range: Option<crate::object_store::Range>,
            ) -> crate::error::Result<(Box<dyn std::io::Read + Send>, u64)> {
                self.inner.get_stream(key, range)
            }
            fn head(&self, key: &str) -> crate::error::Result<crate::object_store::ObjectInfo> {
                self.inner.head(key)
            }
            fn delete(&self, key: &str) -> crate::error::Result<()> {
                self.inner.delete(key)
            }
            fn list(
                &self,
                prefix: &str,
                token: Option<&str>,
                max: usize,
            ) -> crate::error::Result<crate::object_store::ListPage> {
                self.inner.list(prefix, token, max)
            }
            fn ping(&self) -> crate::error::Result<()> {
                self.inner.ping()
            }
        }
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wborder-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let store = StdArc::new(SlowFirstPut {
            inner: MemObjectStore::new(),
            gate: std::sync::atomic::AtomicU64::new(0),
        });
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            store,
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 0,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
            }),
        )
        .unwrap();
        c.create("/f", 0o644).unwrap();
        let node = c.lookup("/f").unwrap();
        let _ = write_via_rpc(&c, "/f", b"client A content", Some(0));
        {
            let eng = c.wb().unwrap();
            eng.write_deferred(
                &node,
                0,
                &mut std::io::Cursor::new(b"client B content".to_vec()),
                16,
            )
            .unwrap();
        }
        // two workers claim the two tasks CONCURRENTLY; A's put is delayed
        // so B completes first (reversed order)
        let eng = std::sync::Arc::clone(c.wb().unwrap());
        let w1 = std::thread::spawn(move || eng.drain_once().unwrap());
        std::thread::sleep(Duration::from_millis(10));
        let w2 = {
            let eng = std::sync::Arc::clone(c.wb().unwrap());
            std::thread::spawn(move || eng.drain_once().unwrap())
        };
        assert!(w1.join().unwrap());
        assert!(w2.join().unwrap());
        // exactly one winner; the other is a VISIBLE conflict
        assert_eq!(
            c.metrics.wb_cas_ok.load(Ordering::Relaxed),
            1,
            "single CAS winner"
        );
        assert_eq!(c.metrics.wb_conflicts.load(Ordering::Relaxed), 1);
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 1));
        // the winner's bytes are what the node serves; nothing vanished
        let total_terminal = c.metrics.wb_drained.load(Ordering::Relaxed)
            + c.metrics.wb_conflicts.load(Ordering::Relaxed)
            + c.metrics.wb_discarded.load(Ordering::Relaxed)
            + c.metrics.wb_cancelled.load(Ordering::Relaxed);
        assert_eq!(total_terminal, 2, "every task reached a terminal state");
        let served = c.read("/f", 0, 100).unwrap();
        assert!(
            served == b"client A content" || served == b"client B content",
            "served content must be one of the two clients' staged bytes"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// S3.5 evidence chain: enqueued == new tasks + coalesced, and every
    /// task lands in exactly one terminal bucket.
    #[test]
    fn evidence_chain_reconciles() {
        let c = batch_core("evidence", 1 << 20, 0);
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", b"gen one", Some(0));
        let _ = write_via_rpc(&c, "/f", b"gen two (coalesces)", Some(1));
        assert_eq!(c.metrics.wb_enqueued.load(Ordering::Relaxed), 2);
        assert_eq!(c.metrics.wb_coalesced.load(Ordering::Relaxed), 1);
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert!(!c.wb().unwrap().drain_once().unwrap());
        assert_eq!(c.metrics.wb_cas_ok.load(Ordering::Relaxed), 1);
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 1);
        // 2 flushes = 1 new task + 1 coalesced = 1 drained, zero residue
        assert_eq!(
            c.metrics.wb_enqueued.load(Ordering::Relaxed),
            c.metrics.wb_drained.load(Ordering::Relaxed)
                + c.metrics.wb_conflicts.load(Ordering::Relaxed)
                + c.metrics.wb_discarded.load(Ordering::Relaxed)
                + c.metrics.wb_cancelled.load(Ordering::Relaxed)
                + c.metrics.wb_coalesced.load(Ordering::Relaxed),
        );
        // unlink accounting path
        c.create("/g", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/g", b"never drains", Some(0));
        c.unlink("/g").unwrap();
        assert_eq!(c.metrics.wb_cancelled.load(Ordering::Relaxed), 1);
    }
}
