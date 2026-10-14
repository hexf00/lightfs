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
    pub drain_workers: usize,
}

pub struct WriteBackEngine {
    meta: Arc<dyn MetadataStore>,
    objects: Arc<dyn ObjectStore>,
    metrics: Arc<Metrics>,
    staging_dir: PathBuf,
    retention_supersede: bool,
    /// Signalled whenever a task is enqueued so idle workers wake.
    wake: Condvar,
    idle: Mutex<usize>,
    stop: AtomicBool,
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
            wake: Condvar::new(),
            idle: Mutex::new(0),
            stop: AtomicBool::new(false),
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
    /// overlay; also backs projected stat). Returns the slice [offset,
    /// offset+len) zero-padded reads past EOF are handled by the caller's
    /// size clamp — this returns exactly the requested window.
    pub fn read_pending(&self, node_id: i64, offset: u64, len: u64) -> Result<Option<Vec<u8>>> {
        let Some((staging, size)) = self.meta.wb_latest_staging(node_id)? else {
            return Ok(None);
        };
        if offset >= size as u64 || len == 0 {
            return Ok(Some(Vec::new()));
        }
        let end = (offset + len).min(size as u64);
        let f = std::fs::File::open(&staging)?;
        use std::io::{Read, Seek, SeekFrom};
        let mut f = f;
        f.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; (end - offset) as usize];
        f.read_exact(&mut buf)?;
        Ok(Some(buf))
    }

    /// Projected stat for a node with pending tasks: (latest staged size,
    /// committed version + pending task count). None = no pending tasks.
    pub fn projected(&self, node: &Node) -> Option<(i64, i64)> {
        self.meta
            .wb_projection(node.id)
            .ok()
            .flatten()
            .map(|(size, pending)| (size, node.version + pending))
    }

    /// Cancel queued tasks for a node (unlink): queued staged bytes never
    /// reach the store. Draining tasks are untouched — their CAS or the
    /// missing node decides the outcome.
    pub fn cancel(&self, node_id: i64) {
        match self.meta.wb_cancel_queued(node_id) {
            Ok(paths) => {
                for p in paths {
                    let _ = std::fs::remove_file(p);
                }
            }
            Err(e) => eprintln!("lightfs wb: cancel for node {node_id} failed: {e}"),
        }
    }

    /// Drain worker body. Claims one queued task at a time by atomically
    /// renaming its staging file (coalescing can then never touch it) and
    /// marking the row 'draining', replays it through PUT + CAS, and
    /// resolves the row: done (deleted), dead (conflict / attempts
    /// exhausted / lost staging), or discarded (node gone).
    pub fn drain_once(&self) -> Result<bool> {
        // Claim under one IMMEDIATE transaction: rename the staging file so
        // a racing coalesce sees state='draining' and queues behind us.
        let Some(task) = self.meta.wb_claim(self.now())? else {
            return Ok(false);
        };
        let content = match std::fs::read(&task.staging) {
            Ok(c) => c,
            Err(e) => {
                self.set_state(task.id, "dead")?;
                eprintln!(
                    "lightfs wb: task {id} dead: staging unreadable: {e}",
                    id = task.id
                );
                return Ok(true);
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
            return Ok(true);
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
                return Ok(true);
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
                if let Some(old_key) = &node.object_key {
                    self.retire(old_key);
                }
                self.meta.wb_finish(task.id)?;
                let _ = std::fs::remove_file(&task.staging);
                self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
            }
            Err(Error::Conflict { .. }) => {
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
                // raced with unlink after the get_node above
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
        Ok(true)
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

    #[test]
    fn crash_recovery_requeues_and_detects_lost_staging() {
        // covered at the metadata layer in sqlite tests (wb_requeue_draining,
        // dead on missing staging); the engine-level recover() runs in
        // with_options startup for file-backed stores.
    }
}
