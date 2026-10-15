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

use sha2::Digest;

use crate::core::Metrics;
use crate::error::{Error, Result};
use crate::metadata::{MetadataStore, Node};
use crate::object_store::ObjectStore;

#[derive(Clone, Debug)]
pub struct WbOptions {
    pub staging_dir: String,
    /// Drain concurrency. **1 is the only supported value in production**
    /// (S3.5.1): values >1 are REJECTED at engine startup
    /// (`resolve_wb_workers`), never silently clamped. Multi-worker
    /// draining was demoted because its safety argument covered only
    /// same-expected CAS races — there is NO per-node FIFO ordering
    /// across SEPARATE queued tasks, so a later version could commit
    /// before an earlier one lands. Re-enabling requires the per-node
    /// ordering conditions listed in docs/S35-wb-consistency-report.md
    /// (§B). The only multi-worker-shaped test that remains
    /// (`cas_conflict_reversed_completion_order_single_winner`) drives
    /// two manual drain threads and proves CONFLICT PROTECTION only.
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
    /// P0-c staging capacity budget: ceiling for staged_bytes +
    /// reserved_bytes. 0 = unbounded (historical behavior, the default).
    /// Exhaustion fails the write with `Error::Capacity` — never a spin
    /// (dead bytes never drain, waiting cannot help) and never an eviction.
    pub staging_budget_bytes: u64,
    /// P0-c acceptance supplement: path of the ledger lock, `<db>.engine-lock`
    /// next to the SQLite file, taken by the engine alongside the staging-dir
    /// lock so a capacity ledger admits exactly one live writer even across
    /// configs that share the DB but not the staging dir. `None` for
    /// in-memory test ledgers (nothing on disk to exclude).
    pub ledger_lock_path: Option<String>,
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
    staging_budget_bytes: u64,
    /// P0-c acceptance supplement: single-writer locks, held for the whole
    /// engine lifetime. `staging` guards the staging DIRECTORY (two engines
    /// sharing it would sweep each other's staging files as orphans);
    /// `ledger` guards the capacity-ledger DB (reconcile would void the
    /// other engine's reservations). Kernel-released on process death.
    /// Held, never read — keeping the fields alive keeps the OS locks.
    _staging_lock: crate::fslock::FileLock,
    _ledger_lock: Option<crate::fslock::FileLock>,
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
    /// S4.4 P0-d (R15) test-only seam: records the order of the three
    /// durability steps of a deferred write (file sync → staging-dir sync →
    /// enqueue) and injects a failure at a chosen step. Never set outside
    /// #[cfg(test)] builds; production always performs the real operations.
    #[cfg(test)]
    pub(crate) sync_probe: Mutex<Option<SyncProbe>>,
}

/// S4.4 P0-d (R15) injection seam, see `WriteBackEngine::sync_probe`.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct SyncProbe {
    /// Completed durability steps, in call order.
    pub log: Mutex<Vec<&'static str>>,
    /// Steps that fail (the step is still recorded, the real operation is
    /// skipped and the error propagates to the flush caller).
    pub fail_on: Vec<&'static str>,
}

type TaskRow = crate::metadata::WbTask;

/// Make a directory entry durable (S4.4 P0-d): after a file's CONTENT is
/// synced, the directory holding its name must also be synced before the
/// write may be acknowledged — otherwise a power loss can lose the name
/// (and with it the bytes) while the content blocks were already on disk.
/// No silent platform no-ops: both supported platforms really flush.
/// Windows opens the directory handle with FILE_FLAG_BACKUP_SEMANTICS (the
/// documented way to hold a directory handle) and FlushFileBuffers it;
/// compile-checked on the windows-gnu target, not exercised on real
/// windows hardware — reported as such, not claimed.
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let dir = std::fs::File::open(path)
            .map_err(|e| Error::Io(format!("open directory {} for sync: {e}", path.display())))?;
        dir.sync_all()
            .map_err(|e| Error::Io(format!("fsync directory {}: {e}", path.display())))?;
        Ok(())
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
        let dir = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
            .open(path)
            .map_err(|e| Error::Io(format!("open directory {} for sync: {e}", path.display())))?;
        dir.sync_all().map_err(|e| {
            Error::Io(format!(
                "FlushFileBuffers directory {}: {e}",
                path.display()
            ))
        })?;
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(Error::Io(
            "directory sync unsupported on this platform — refusing to pretend it succeeded".into(),
        ))
    }
}

/// S4.4 P0-d dead-letter error classes, persisted on `wb_tasks.error_class`.
/// The retry-admission contract lives in [`retry_admissible`]; the drain
/// workers only AUTO-requeue what classifies as transient.
pub const CLASS_CONFLICT: &str = "conflict";
pub const CLASS_UPLOAD_TRANSIENT: &str = "upload-transient";
pub const CLASS_UPLOAD_PERMANENT: &str = "upload-permanent";
pub const CLASS_STAGING_LOST: &str = "staging-lost";
/// The honest label for an error that cannot be judged: never treated as
/// retryable (neither auto nor by a future operator command).
pub const CLASS_UNKNOWN: &str = "unknown";

/// Retry admission for a DEAD task's class (P2's future `wb dead retry`
/// decides on this; it is deliberately independent of any CLI):
/// conflicts can never be retried (the winner stands), lost staging has
/// nothing to upload, unjudged errors must not masquerade as retryable.
/// Test-enforced contract today; P2's operator command builds on it.
#[cfg(test)]
pub(crate) fn retry_admissible(class: &str) -> bool {
    matches!(class, CLASS_UPLOAD_TRANSIENT | CLASS_UPLOAD_PERMANENT)
}

/// Classify an object-upload failure by its ERROR ORIGIN, not by position
/// in the drain code. The S3 layer has three shapes: `s3 transport: …`
/// (network), `s3 {status} on {key}: …` (HTTP response), and anything else
/// (protocol/parse — an upload outcome we cannot vouch for).
pub(crate) fn classify_object_upload_failure(e: &Error) -> &'static str {
    match e {
        Error::Storage(m) => {
            if m.starts_with("s3 transport:") {
                return CLASS_UPLOAD_TRANSIENT;
            }
            if let Some(status) = m
                .strip_prefix("s3 ")
                .and_then(|rest| rest.split(' ').next())
                .and_then(|code| code.parse::<u16>().ok())
            {
                // transient: server-side overload/failure or explicit
                // retryable client conditions (rate limit, request timeout)
                if status >= 500 || status == 429 || status == 408 {
                    return CLASS_UPLOAD_TRANSIENT;
                }
                // any other remote verdict (403 permissions, 400 shape,
                // bucket policy...) — retrying cannot fix the cause
                return CLASS_UPLOAD_PERMANENT;
            }
            CLASS_UNKNOWN
        }
        _ => CLASS_UNKNOWN,
    }
}

/// Result of probing a node's pending staged content (S3.5.1 three-state
/// contract - the old `Option<Vec<u8>>` could not distinguish "no pending"
/// from "the probe kept losing the race", and callers read exhaustion as
/// absence, serving a previous generation or a spurious EOF).
pub(crate) enum PendingRead {
    /// Served from a pinned staging snapshot (or its EOF, when the caller's
    /// window starts at/after the staged size - an answer about a CURRENTLY
    /// valid staged view, never about committed). S3.5 lifecycle: the
    /// staging query→open window is closed open-before-validate - open
    /// ENOENT means the drain committed right there (no pending), an open
    /// fd PINS the bytes (POSIX unlink-open), and the post-open
    /// re-validation distinguishes serve-fd / coalesced / committed.
    Bytes(Vec<u8>),
    /// Authoritative: no pending task existed at query time. The caller may
    /// serve the committed object.
    None,
    /// The probe kept losing the race (a newer flush coalesced over the
    /// task on every attempt; retry loop exhausted). NOT "no pending": the
    /// caller must re-resolve the WHOLE read snapshot - falling through to
    /// the committed branch here would serve a previous generation (or a
    /// spurious EOF for a file with no committed object at all).
    Retry,
}

impl WriteBackEngine {
    pub(crate) fn new(
        meta: Arc<dyn MetadataStore>,
        objects: Arc<dyn ObjectStore>,
        metrics: Arc<Metrics>,
        opts: &WbOptions,
        retention_supersede: bool,
    ) -> Result<Self> {
        std::fs::create_dir_all(&opts.staging_dir)?;
        // P0-c acceptance supplement: single-writer enforcement. Until now
        // "one engine per staging dir / capacity ledger" was a comment, not
        // a mechanism — a second engine could open the same staging dir and
        // its recover() would sweep the first engine's staging files as
        // orphans and void its reservations. Both locks are kernel-owned
        // (die with the process) and held for THIS struct's lifetime, so a
        // second engine fails loudly while the first is alive and can start
        // cleanly once it is gone. Acquired BEFORE any recover/reconcile.
        let staging_identity = crate::fslock::canonical_identity(
            Path::new(&opts.staging_dir),
            "write-back staging directory",
        )?;
        let staging_lock =
            crate::fslock::FileLock::try_acquire(&staging_identity.join("engine.lock")).map_err(
                |e| {
                    Error::Config(format!(
                        "write-back staging directory {} is already owned by a live engine \
                         (only one engine may own a staging directory and its capacity ledger): {e}",
                        opts.staging_dir
                    ))
                },
            )?;
        let ledger_lock = match &opts.ledger_lock_path {
            Some(p) => Some(
                crate::fslock::FileLock::try_acquire(Path::new(p)).map_err(|e| {
                    Error::Config(format!(
                        "capacity ledger {p} is already owned by a live engine \
                     (only one engine may write one ledger): {e}"
                    ))
                })?,
            ),
            None => None,
        };
        Ok(Self {
            meta,
            objects,
            metrics,
            staging_dir: PathBuf::from(&opts.staging_dir),
            _staging_lock: staging_lock,
            _ledger_lock: ledger_lock,
            retention_supersede,
            batch_pack_bytes: opts.batch_pack_bytes,
            batch_max_wait_ms: opts.batch_max_wait_ms,
            queue_max_tasks: opts.queue_max_tasks,
            staging_budget_bytes: opts.staging_budget_bytes,
            wake: Condvar::new(),
            idle: Mutex::new(0),
            stop: AtomicBool::new(false),
            #[cfg(test)]
            test_hook_before_open: Mutex::new(None),
            #[cfg(test)]
            sync_probe: Mutex::new(None),
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
                self.meta
                    .wb_mark_dead(t.id, CLASS_STAGING_LOST, self.now())?;
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
        let on_disk = self.reconcile_capacity()?;
        // Sweep orphans; each release is bound to the confirmed physical
        // delete (invariant 4) — a failed delete keeps its bytes accounted.
        let mut swept = 0usize;
        for (path_str, size) in &on_disk {
            if !referenced.contains(path_str) && std::fs::remove_file(path_str).is_ok() {
                let _ = self.meta.wb_release_staged(*size);
                swept += 1;
            }
        }
        if swept > 0 {
            eprintln!(
                "lightfs wb: swept {swept} orphaned staging file(s) and released their bytes"
            );
        }
        // P0-a recovery sweep 1: batches a crash left in `committing`
        // finalize here when no non-terminal task remains for them —
        // the same verify-references-then-decide transaction the drain
        // uses, never a state guessed from stale rows.
        for pk in self.meta.committing_packs()? {
            self.meta.wb_finalize_batch_if_terminal(&pk, self.now())?;
        }
        // P0-a recovery sweep 2: dead tasks that had already uploaded bytes
        // leave orphan candidates. Enrollment re-checks "no live reference"
        // in the registration transaction and confirms the object still
        // exists before recording the durable deletion intent. HEAD is a
        // three-state signal (S4.2.1): exists → enroll the intent (never a
        // synchronous delete); confirmed absent → nothing to delete, no
        // intent either; any OTHER error → fail closed, keep the record
        // for the next recover() — "cannot tell" is never read as "absent".
        for key in self.meta.dead_task_orphans()? {
            match self.objects.head(&key) {
                Ok(_) => {
                    if self.meta.gc_enqueue_orphan_object(&key, self.now())? {
                        eprintln!("lightfs wb: orphan object {key} enrolled for deletion");
                    }
                }
                Err(Error::ObjectMissing(_)) => {
                    eprintln!("lightfs wb: orphan object {key} already absent; no deletion intent");
                }
                Err(e) => {
                    eprintln!(
                        "lightfs wb: orphan object {key}: head unavailable ({e}); record kept for next recover"
                    );
                }
            }
        }
        Ok(())
    }

    /// P0-c (invariant 8): rebuild the capacity ledger from the REAL
    /// staging files — queued/draining/dead/referenced AND orphans are all
    /// physically on disk and all count — voiding every leftover
    /// reservation from the previous process. Runs at engine construction,
    /// before any worker or front write can reserve (single-authority: two
    /// engines never share one db). Unstatable files fail the rebuild:
    /// partial information is never guessed into the ledger.
    pub(crate) fn reconcile_capacity(&self) -> Result<Vec<(String, i64)>> {
        let mut on_disk: Vec<(String, i64)> = Vec::new();
        let entries = std::fs::read_dir(&self.staging_dir)
            .map_err(|e| Error::Storage(format!("read staging dir: {e}")))?;
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            if !(name.starts_with("wb-") || name.starts_with("drain-")) {
                continue;
            }
            let p = entry.path();
            match entry.metadata() {
                Ok(m) => on_disk.push((p.to_string_lossy().to_string(), m.len() as i64)),
                Err(e) => {
                    return Err(Error::Storage(format!(
                        "cannot stat staging file {}: {e} — refusing to rebuild the capacity ledger on partial information",
                        p.display()
                    )));
                }
            }
        }
        let (staged_total, _) = self.meta.wb_reconcile_capacity(&on_disk)?;
        eprintln!(
            "lightfs wb: capacity ledger reconciled: {staged_total}B staged across {} file(s); reservations voided",
            on_disk.len()
        );
        Ok(on_disk)
    }

    /// Test/ops convenience: run the reconciliation and report the ledger.
    #[cfg(test)]
    pub(crate) fn reconcile_capacity_and_report(&self) -> Result<(i64, i64)> {
        self.reconcile_capacity()?;
        self.meta.wb_capacity_snapshot()
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

    /// R15 seam: record a durability step; when the probe names this step
    /// as a failure point, skip the real operation and fail the flush.
    #[cfg(test)]
    fn sync_step(&self, step: &'static str) -> Result<()> {
        let mut guard = self.sync_probe.lock().unwrap();
        if let Some(p) = guard.as_mut() {
            p.log.lock().unwrap().push(step);
            if p.fail_on.contains(&step) {
                return Err(Error::Io(format!("injected {step} failure")));
            }
        }
        Ok(())
    }

    #[cfg(not(test))]
    fn sync_step(&self, _step: &'static str) -> Result<()> {
        Ok(())
    }

    /// An unenqueued staging file (file-sync or dir-sync failed, or the
    /// enqueue transaction never committed): the write was NOT acked, so
    /// the bytes have no owner. Delete them and free the reservation; a
    /// failed delete keeps the bytes accounted (invariant 4) for recover()
    /// to sweep — never released while they may still occupy the disk.
    fn discard_unenqueued(&self, staging: &Path, len: i64) {
        match std::fs::remove_file(staging) {
            Ok(()) => {
                let _ = self.meta.wb_release_reserved(len);
            }
            Err(rm) => eprintln!(
                "lightfs wb: unenqueued staging {} kept ({rm}); its reservation stays until reconcile",
                staging.display()
            ),
        }
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
        // P0-c: reserve the staging capacity BEFORE the file exists
        // (invariant 1: 预留先行 — a write must never be able to exceed
        // the budget, even briefly). Exhaustion is an explicit
        // `capacity-exhausted` error: no queue-style spin (dead bytes
        // never drain, waiting cannot help), no eviction, no silent
        // degrade (invariant 7).
        if self.staging_budget_bytes > 0 && len as i64 > self.staging_budget_bytes as i64 {
            return Err(Error::Capacity(format!(
                "write of {len}B alone exceeds the whole staging budget of {}B",
                self.staging_budget_bytes
            )));
        }
        self.meta
            .wb_capacity_reserve(len as i64, self.staging_budget_bytes as i64)?;
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
        let staged = (|| -> Result<()> {
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
            // the durability contract: staged bytes survive process death
            self.sync_step("file-sync")?;
            f.sync_all()?;
            Ok(())
        })();
        if let Err(e) = staged {
            // The half-written file is physically gone; only then does the
            // reservation come back (invariant 4 inverted: never release
            // capacity for bytes that might still occupy the disk).
            self.discard_unenqueued(&staging, len as i64);
            return Err(e);
        }
        // S4.4 P0-d: the file's NAME must be durable before the task that
        // promises those bytes is committed — a crash after the enqueue but
        // before the name lands would leave a task pointing at bytes whose
        // directory entry was lost. Ordered: file sync → dir sync →
        // enqueue → ack. A failure here is an explicit flush error: the
        // write was NOT acknowledged, so dropping the unenqueued staging
        // file (and its reservation) is the correct recovery.
        if let Err(e) = (|| {
            self.sync_step("dir-sync")?;
            sync_directory(&self.staging_dir)
        })() {
            self.discard_unenqueued(&staging, len as i64);
            return Err(Error::Io(format!(
                "staging namespace not durable; write not acknowledged: {e}"
            )));
        }
        // Converts the reservation to staged accounting in the SAME
        // transaction that records the task (invariant 3).
        let (projected_version, replaced_staging) = match (|| {
            self.sync_step("enqueue")?;
            self.meta.wb_enqueue(
                node.id,
                staging.to_string_lossy().as_ref(),
                len as i64,
                expected,
                self.now(),
            )
        })() {
            Ok(ok) => ok,
            Err(e) => {
                // The task row never committed: the bytes have no owner and
                // the ack never went out. Same recovery as a crash here —
                // drop the file (freeing its reservation); if the delete
                // fails the bytes stay accounted for recover() to sweep.
                self.discard_unenqueued(&staging, len as i64);
                return Err(e);
            }
        };
        if let Some((old, old_size)) = replaced_staging {
            self.metrics.wb_coalesced.fetch_add(1, Ordering::Relaxed);
            // Coalesced away; the row no longer references it. Its bytes
            // leave the ledger ONLY when the physical delete confirms
            // (invariant 4); a failed delete leaves accounted orphan bytes
            // for recover() to sweep.
            match std::fs::remove_file(&old) {
                Ok(()) => {
                    let _ = self.meta.wb_release_staged(old_size);
                }
                Err(e) => eprintln!(
                    "lightfs wb: coalesced staging {old} not removed ({e}); its {old_size}B stay accounted"
                ),
            }
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

    /// Probe the node's pending staged content (read-your-writes overlay;
    /// also backs projected stat). See [`PendingRead`] for the three-state
    /// contract; the S3.5 lifecycle notes live on the enum variants.
    pub(crate) fn read_pending(&self, node_id: i64, offset: u64, len: u64) -> Result<PendingRead> {
        for _ in 0..3 {
            let Some((staging, size)) = self.meta.wb_latest_staging(node_id)? else {
                return Ok(PendingRead::None);
            };
            // EOF fast path against the CURRENT staged view: a pending task
            // exists, so this is a statement about the staged size, never a
            // license for the caller to skip the overlay.
            if offset >= size as u64 || len == 0 {
                return Ok(PendingRead::Bytes(Vec::new()));
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
            use std::io::{Read, Seek, SeekFrom};
            match self.meta.wb_latest_staging(node_id)? {
                Some((p, _)) if p == staging => {}
                Some(_) => continue, // coalesced onto a newer staging file
                None => {}           // finished right after the open: fd == committed
            }
            f.seek(SeekFrom::Start(offset))?;
            let mut buf = vec![0u8; (end - offset) as usize];
            f.read_exact(&mut buf)?;
            return Ok(PendingRead::Bytes(buf));
        }
        // S3.5.1: three coalesces in a row is a RACE, not an absence. The
        // old behavior (Ok(None)) made Core fall through to the committed
        // branch - serving EOF for a never-committed file or the previous
        // generation for an overwritten one. Report Retry explicitly; the
        // caller re-resolves the entire snapshot.
        Ok(PendingRead::Retry)
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
                for (p, size) in paths {
                    match std::fs::remove_file(&p) {
                        Ok(()) => {
                            let _ = self.meta.wb_release_staged(size);
                        }
                        Err(e) => eprintln!(
                            "lightfs wb: cancelled staging {p} not removed ({e}); its {size}B stay accounted"
                        ),
                    }
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
        // P0-c streaming: the staging file goes to put_stream via an open
        // File — never fs::read into memory (a 1 GiB write used to peak at
        // ~2x its size here). Length is verified against the file METADATA,
        // before a single byte is uploaded.
        let mut f = match std::fs::File::open(&task.staging) {
            Ok(f) => f,
            Err(e) => {
                self.meta
                    .wb_mark_dead(task.id, CLASS_STAGING_LOST, self.now())?;
                eprintln!(
                    "lightfs wb: task {id} dead: staging unreadable: {e}",
                    id = task.id
                );
                return Ok(());
            }
        };
        let actual_len = match f.metadata() {
            Ok(m) => m.len() as i64,
            Err(e) => {
                self.meta
                    .wb_mark_dead(task.id, CLASS_STAGING_LOST, self.now())?;
                eprintln!(
                    "lightfs wb: task {id} dead: staging stat failed: {e}",
                    id = task.id
                );
                return Ok(());
            }
        };
        if actual_len != task.size {
            self.meta
                .wb_mark_dead(task.id, CLASS_STAGING_LOST, self.now())?;
            eprintln!(
                "lightfs wb: task {id} dead: staged {got} bytes != declared {want}",
                id = task.id,
                got = actual_len,
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
                self.remove_staged(&task);
                return Ok(());
            }
        };
        let version_key = crate::core::new_version_key_for(task.node_id);
        let put = self
            .objects
            .put_stream(&version_key, &mut f, task.size as u64);
        self.metrics.s3_put.fetch_add(1, Ordering::Relaxed);
        let outcome = match put {
            Ok(info) => {
                // Bytes are on the remote: from this point a terminal
                // failure (dead-letter) leaves `version_key` as an orphan
                // candidate — record it on the task so recover() can
                // enroll it for the durable deletion task.
                self.meta
                    .wb_set_uploaded_key(task.id, &version_key, self.now())?;
                self.meta.commit_content_cas(
                    task.node_id,
                    task.expected,
                    task.size,
                    self.now(),
                    &version_key,
                    &info.etag,
                )
            }
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
                self.remove_staged(&task);
                self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
            }
            Err(Error::Conflict { .. }) => {
                // Crash between CAS and wb_finish: the commit LANDED - the
                // recovered task must finish as a success, never as a
                // conflict (S3.5 recovery semantics).
                if self.task_already_committed(&task)? {
                    self.meta.wb_finish(task.id)?;
                    self.remove_staged(&task);
                    self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                // Never corrupt the winner: dead-letter with bytes retained.
                self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
                self.metrics.wb_conflicts.fetch_add(1, Ordering::Relaxed);
                let _ = self.objects.delete(&version_key);
                self.meta
                    .wb_mark_dead(task.id, CLASS_CONFLICT, self.now())?;
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
                self.remove_staged(&task);
            }
            Err(e) => {
                let _ = self.objects.delete(&version_key);
                // S4.4: the class comes from the error's ORIGIN — transient
                // upload failures requeue with a bounded budget; permanent
                // remote verdicts and unjudgeable errors dead-letter now
                // (retrying cannot fix a permission or a bucket policy).
                let class = classify_object_upload_failure(&e);
                if class == CLASS_UPLOAD_TRANSIENT
                    && self.meta.wb_retry_or_dead(task.id, 3, self.now(), class)?
                {
                    std::thread::sleep(Duration::from_millis(50));
                    return Ok(());
                }
                if class == CLASS_UPLOAD_TRANSIENT {
                    eprintln!(
                        "lightfs wb: task {id} dead after retries ({class}): {e}",
                        id = task.id
                    );
                } else if class == CLASS_UPLOAD_PERMANENT {
                    eprintln!(
                        "lightfs wb: task {id} dead ({class}): {e} — fix the cause before retrying",
                        id = task.id
                    );
                } else {
                    eprintln!("lightfs wb: task {id} dead ({class}): {e}", id = task.id);
                }
                self.meta.wb_mark_dead(task.id, class, self.now())?;
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
        // P0-c streaming: entries are (task, pack_offset, len) — the bytes
        // are NEVER read into memory here. Content-length validation uses
        // the file METADATA; the PackReader streams the files straight
        // into put_stream inside seal_batch.
        let mut entries: Vec<(TaskRow, u64, u64)> = Vec::new();
        let mut total: u64 = 0;
        let mut first_at: Option<std::time::Instant> = None;
        loop {
            if self.stop.load(Ordering::Relaxed) && !entries.is_empty() {
                break;
            }
            let fit = (max_bytes - total).min(i64::MAX as u64) as i64;
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
                    // Validate WITHOUT loading content: open + metadata.
                    let declared = match std::fs::File::open(&task.staging) {
                        Ok(f) => match f.metadata() {
                            Ok(m) if m.len() as i64 == task.size => Some(task.size),
                            Ok(m) => {
                                self.set_state(task.id, "dead")?;
                                eprintln!(
                                    "lightfs wb: task {id} dead: staged {got} != declared {want}",
                                    id = task.id,
                                    got = m.len(),
                                    want = task.size
                                );
                                None
                            }
                            Err(e) => {
                                self.set_state(task.id, "dead")?;
                                eprintln!(
                                    "lightfs wb: task {id} dead: staging stat failed: {e}",
                                    id = task.id
                                );
                                None
                            }
                        },
                        Err(e) => {
                            self.set_state(task.id, "dead")?;
                            eprintln!(
                                "lightfs wb: task {id} dead: staging unreadable: {e}",
                                id = task.id
                            );
                            None
                        }
                    };
                    let Some(len) = declared else { continue };
                    entries.push((task, total, len as u64));
                    total += len as u64;
                    if total >= max_bytes {
                        break; // pack budget reached
                    }
                }
            }
        }
        self.seal_batch(entries)?;
        Ok(true)
    }

    /// Remove a drained task's staging file and release its staged bytes
    /// ONLY on the confirmed physical delete (P0-c invariants 4/5). A
    /// failed delete keeps the bytes accounted: the file becomes an orphan
    /// that the next recover() sweep deletes and releases.
    fn remove_staged(&self, task: &TaskRow) {
        match std::fs::remove_file(&task.staging) {
            Ok(()) => {
                let _ = self.meta.wb_release_staged(task.size);
            }
            Err(e) => eprintln!(
                "lightfs wb: staging {} not removed ({e}); its {}B stay accounted",
                task.staging, task.size
            ),
        }
    }

    /// Upload one assembled pack and resolve every entry's CAS. The PUT
    /// runs while holding NO shared lock — only this worker's entries are
    /// in flight, and sibling tasks stay claimable throughout.
    fn seal_batch(&self, entries: Vec<(TaskRow, u64, u64)>) -> Result<()> {
        let total: u64 = entries.last().map(|(_, off, len)| off + len).unwrap_or(0);
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
        self.meta.register_pack(&pack_key, total as i64)?;
        // P0-c streaming: the pack is the sequential concatenation of its
        // staging files, streamed straight into put_stream (bounded 8 MiB
        // part windows on the S3 side; ONE staging fd at a time). Segment
        // hashes come out of the same pass — no in-memory pack buffer.
        let mut reader = PackReader::new(
            entries
                .iter()
                .map(|(t, _, len)| (std::path::PathBuf::from(&t.staging), *len))
                .collect(),
        );
        let put = self.objects.put_stream(&pack_key, &mut reader, total);
        self.metrics.s3_put.fetch_add(1, Ordering::Relaxed);
        if put.is_ok() {
            self.metrics.wb_packs.fetch_add(1, Ordering::Relaxed);
        }
        if let Err(e) = put {
            // upload failed: nothing references the pack yet — undo row
            // + object, then S4.4 classification: transient failures
            // requeue with the retry budget; permanent remote verdicts and
            // unjudgeable outcomes dead-letter their members immediately.
            let _ = self.objects.delete(&pack_key);
            let _ = self.meta.drop_pack(&pack_key);
            let class = classify_object_upload_failure(&e);
            for (task, _, _) in &entries {
                if class == CLASS_UPLOAD_TRANSIENT
                    && self.meta.wb_retry_or_dead(task.id, 3, self.now(), class)?
                {
                    continue;
                }
                if class == CLASS_UPLOAD_PERMANENT {
                    eprintln!(
                        "lightfs wb: task {id} dead ({class}): {e} — fix the cause before retrying",
                        id = task.id
                    );
                } else if class == CLASS_UPLOAD_TRANSIENT {
                    eprintln!(
                        "lightfs wb: task {id} dead after retries ({class}): {e}",
                        id = task.id
                    );
                } else {
                    eprintln!("lightfs wb: task {id} dead ({class}): {e}", id = task.id);
                }
                self.meta.wb_mark_dead(task.id, class, self.now())?;
            }
            if class == CLASS_UPLOAD_TRANSIENT {
                std::thread::sleep(Duration::from_millis(50));
            }
            return Ok(());
        }
        self.meta.mark_pack_uploaded(&pack_key, self.now())?;
        // P0-a: bind the claimed tasks to THIS batch before any member CAS —
        // the batch is now `committing`, its members are tracked, and the
        // termination decision keys off this binding.
        let member_ids: Vec<i64> = entries.iter().map(|(t, _, _)| t.id).collect();
        self.meta
            .wb_bind_pack_keys(&member_ids, Some(&pack_key), self.now())?;
        let mut batch_finalized = false;
        let hashes = reader.hashes;
        assert_eq!(
            hashes.len(),
            entries.len(),
            "PackReader must produce one hash per entry after a full upload"
        );
        for (idx, (task, off, len)) in entries.into_iter().enumerate() {
            let hash = hashes[idx].clone();
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
                    self.remove_staged(&task);
                    self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
                }
                Err(Error::Conflict { .. }) => {
                    // Crash between the per-file CAS and wb_finish: the
                    // commit landed - finish as a success (S3.5 recovery).
                    if self.task_already_committed(&task)? {
                        self.meta.wb_finish(task.id)?;
                        self.remove_staged(&task);
                        self.metrics.wb_drained.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                    // The winner stands; this entry's bytes stay in the pack
                    // as unreferenced residue (pack gc reclaims them).
                    self.metrics.conflicts.fetch_add(1, Ordering::Relaxed);
                    self.metrics.wb_conflicts.fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .pack_dead_bytes
                        .fetch_add(len, Ordering::Relaxed);
                    self.meta
                        .wb_mark_dead(task.id, CLASS_CONFLICT, self.now())?;
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
                    self.remove_staged(&task);
                }
                Err(e) => {
                    // Metadata-layer CAS failure (not a conflict): the
                    // requeue behavior is unchanged; exhaustion dead-letters
                    // as `unknown` — an unjudgeable error is never recorded
                    // as a retryable upload failure.
                    if !self
                        .meta
                        .wb_retry_or_dead(task.id, 3, self.now(), CLASS_UNKNOWN)?
                    {
                        eprintln!(
                            "lightfs wb: task {id} dead after retries (unknown): {e}",
                            id = task.id
                        );
                    } else {
                        // requeued: stays non-terminal, the batch cannot
                        // finalize until this task reaches a terminal state.
                        continue;
                    }
                }
            }
            // P0-a: every member terminal transition re-runs the (idempotent)
            // batch termination decision in its own transaction — the pack
            // becomes active/unreferenced exactly when the last member lands.
            if !batch_finalized {
                self.meta
                    .wb_finalize_batch_if_terminal(&pack_key, self.now())?;
                if matches!(
                    self.meta.pack_state(&pack_key),
                    Ok(Some((s, _))) if s != "committing"
                ) {
                    batch_finalized = true;
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
    fn task_already_committed(&self, task: &TaskRow) -> Result<bool> {
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
        // P0-c: byte comparison in bounded 8 MiB windows against the
        // staging FILE — the old slice-vs-Vec compare required the staged
        // bytes resident, which is exactly the memory spike this stage
        // removes (a 1 GiB conflict would have re-read 1 GiB).
        let range = match (n.pack_off, n.pack_len) {
            (Some(off), Some(plen)) => Some(crate::object_store::Range {
                start: off as u64,
                len: plen as u64,
            }),
            _ => None,
        };
        let (mut remote, remote_len) = match self.objects.get_stream(key, range) {
            Ok(r) => r,
            Err(_) => return Ok(false),
        };
        if remote_len != task.size as u64 {
            return Ok(false);
        }
        let mut local = match std::fs::File::open(&task.staging) {
            Ok(f) => f,
            Err(_) => return Ok(false),
        };
        // Heap buffers: two 8 MiB stack arrays would overflow a default
        // 2 MiB worker/test thread stack.
        let mut a = vec![0u8; 8 * 1024 * 1024];
        let mut b = vec![0u8; 8 * 1024 * 1024];
        loop {
            let na = read_full(&mut local, &mut a)?;
            let nb = read_full(&mut remote, &mut b)?;
            if na != nb || a[..na] != b[..nb] {
                return Ok(false);
            }
            if na == 0 {
                return Ok(true);
            }
        }
    }

    fn retire(&self, key: &str) {
        if key.starts_with(crate::pack::PACK_KEY_PREFIX) {
            return; // shared pack: lifecycle owned by pack gc
        }
        // Retention: record for point-in-time restore — reclamation happens
        // only through the P0-b-gated gc pipeline. Retention disabled (or
        // the record failed): park a durable deletion intent instead of
        // deleting inline; a committed object may sit in historical
        // metadata backups, so its bytes never leave synchronously here.
        if self.retention_supersede && self.meta.record_superseded(key, self.now()).is_ok() {
            return;
        }
        match self.meta.gc_enqueue_orphan_object(key, self.now()) {
            Ok(true) => {}
            Ok(false) => {
                eprintln!("lightfs wb: retire of {key} skipped: a live reference appeared")
            }
            Err(e) => eprintln!(
                "lightfs wb: retire of {key} could not record a deletion intent: {e} (left for fsck)"
            ),
        }
    }

    /// Spawn `workers` drain threads. Prototype lifetime: detached threads
    /// that end when the process ends or `stop()` is called. The caller
    /// (engine startup via `resolve_wb_workers`) owns the validation —
    /// this takes the count verbatim, no clamping.
    pub(crate) fn spawn_workers(self: &Arc<Self>, workers: usize) {
        for _ in 0..workers {
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

/// Reads until `buf` is full or EOF; returns bytes read (0 = EOF at the
/// boundary). A short read from the middle would silently corrupt a pack,
/// so callers that need exactness loop here.
fn read_full(r: &mut dyn std::io::Read, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0usize;
    while filled < buf.len() {
        let got = r.read(&mut buf[filled..])?;
        if got == 0 {
            break;
        }
        filled += got;
    }
    Ok(filled)
}

/// Streams a pack as the sequential concatenation of its staging files
/// (pack layout: no header; per-entry (offset, len) lives in SQLite).
/// ONE staging file is open at any time (fd peak = 1) and no pack-sized
/// buffer ever exists — the caller's read window (S3 put_stream: 8 MiB
/// parts) is the memory bound (S4 §3.D frozen streaming design).
/// Each entry's SHA-256 is computed on the SAME pass (tee-style), so the
/// per-file content hashes are ready when the upload finishes, without a
/// second read. A staging file that deviates from its declared length
/// fails the upload (io error) — data integrity is never weakened by the
/// streaming rewrite.
struct PackReader {
    segs: std::collections::VecDeque<(std::path::PathBuf, u64)>,
    cur: Option<(std::io::BufReader<std::fs::File>, u64)>,
    seg_hasher: Option<sha2::Sha256>,
    seg_remaining: u64,
    hashes: Vec<String>,
}

impl PackReader {
    fn new(segs: Vec<(std::path::PathBuf, u64)>) -> Self {
        Self {
            segs: segs.into(),
            cur: None,
            seg_hasher: None,
            seg_remaining: 0,
            hashes: Vec::new(),
        }
    }
}

impl std::io::Read for PackReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if out.is_empty() {
                return Ok(0);
            }
            if self.cur.is_none() {
                let Some((path, len)) = self.segs.pop_front() else {
                    return Ok(0); // pack exhausted
                };
                let f = std::fs::File::open(&path).map_err(|e| {
                    std::io::Error::other(format!("staging {}: {e}", path.display()))
                })?;
                self.cur = Some((std::io::BufReader::with_capacity(512 * 1024, f), len));
                self.seg_hasher = Some(sha2::Sha256::new());
                self.seg_remaining = len;
            }
            let (reader, remaining) = self.cur.as_mut().unwrap();
            let want = (out.len() as u64).min(*remaining) as usize;
            let got = reader.read(&mut out[..want])?;
            if got == 0 {
                return Err(std::io::Error::other(format!(
                    "staging file ended {} bytes short of its declared length",
                    self.seg_remaining
                )));
            }
            self.seg_hasher
                .as_mut()
                .expect("hasher exists while a segment is open")
                .update(&out[..got]);
            self.seg_remaining -= got as u64;
            *remaining -= got as u64;
            if self.seg_remaining == 0 {
                self.hashes.push(hex::encode(
                    self.seg_hasher.take().expect("hasher").finalize(),
                ));
                // The segment declared length is consumed; the file must be
                // exactly at EOF, or the next entry's bytes would shift.
                let mut probe = [0u8; 1];
                if reader.read(&mut probe)? != 0 {
                    return Err(std::io::Error::other(
                        "staging file longer than its declared length".to_string(),
                    ));
                }
                self.cur = None;
            }
            if got > 0 {
                return Ok(got);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::Core;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::object_store::mem::MemObjectStore;
    use std::collections::HashSet;
    use std::sync::Arc as StdArc;
    use std::sync::atomic::AtomicU64;

    /// macOS SystemTime has microsecond real resolution (`as_nanos()` is
    /// microseconds x 1000), so parallel tests starting together can build
    /// identical "nanos" names and collide on the same temp dir — observed
    /// as flaky lock-busy errors on gc.lock/engine.lock. A per-process
    /// sequence makes every temp dir unique regardless of clock ticks.
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

    fn tmp_unique() -> u128 {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        (nanos << 8) | TMP_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128
    }

    fn wb_core(tag: &str) -> Core {
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wb-{tag}-{}-{}",
            std::process::id(),
            tmp_unique()
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
                staging_budget_bytes: 0,

                ledger_lock_path: None,
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
        // S4.4: the dead letter carries the `conflict` class, never retryable
        {
            let rows = c.metadata().wb_task_rows().unwrap();
            assert!(
                rows.iter().any(|(_, state, _, class)| state == "dead"
                    && class.as_deref() == Some(CLASS_CONFLICT))
            );
        }
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
            tmp_unique()
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
                staging_budget_bytes: 0,

                ledger_lock_path: None,
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
                        // transport-shaped on purpose: an outage IS a
                        // transient failure, and S4.4 classification keys
                        // off the error's origin shape
                        "s3 transport: simulated upload outage".into(),
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
                // P0-c: batch seals stream via put_stream — the outage
                // injection must live on the path production actually uses.
                if key.starts_with(crate::pack::PACK_KEY_PREFIX)
                    && self.fail_first.swap(false, Ordering::Relaxed)
                {
                    return Err(crate::error::Error::Storage(
                        // transport-shaped on purpose: an outage IS a
                        // transient failure, and S4.4 classification keys
                        // off the error's origin shape
                        "s3 transport: simulated upload outage".into(),
                    ));
                }
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
            tmp_unique()
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
                staging_budget_bytes: 0,

                ledger_lock_path: None,
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
            b"file-zero",
            b"file-one",
            b"file-two",
            b"file-three",
            b"file-four",
            b"file-five",
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
                "{} content after mixed batch",
                cur
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
            tmp_unique()
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
            staging_budget_bytes: 0,

            ledger_lock_path: None,
        };
        let body = b"survives a crash";
        let store: StdArc<dyn crate::object_store::ObjectStore> =
            StdArc::new(MemObjectStore::new());
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
            c.objects().put(&vkey, body).unwrap();
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

    /// S3.5 red test 4 (renamed S3.5.1): two MANUAL drain threads, two
    /// tasks on ONE node (cross-client same-expected), network completion
    /// order REVERSED via a delay-injecting store. CAS anchoring yields
    /// exactly one winner, one visible conflict, zero silent losses.
    /// **Scope statement (S3.5.1): this proves CONFLICT PROTECTION of the
    /// single-statement CAS under reversed completion — it is NOT evidence
    /// that multiple drain workers preserve per-node FIFO order across
    /// separate queued tasks. Multi-worker draining is disabled for
    /// production (startup refuses >1); the two threads here simulate
    /// concurrent completion of two ALREADY-CLAIMED tasks, which real
    /// workers would only reach if the ordering gap were closed.**
    #[test]
    fn cas_conflict_reversed_completion_order_single_winner() {
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
            tmp_unique()
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
                staging_budget_bytes: 0,

                ledger_lock_path: None,
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

    /// S3.5.1 B.3: the ONE supported worker configuration. Same file,
    /// consecutive flushes from the same client land in the queue as
    /// separate enqueue attempts; the queued-period coalesce folds them
    /// into ONE task (later expected replaces the staging), and the single
    /// drain commits the final content at the first version. This is the
    /// ordering guarantee production actually relies on — the cross-client
    /// same-expected race above only proves conflict protection.
    #[test]
    fn single_worker_same_file_consecutive_flushes_coalesce_and_land() {
        use crate::core::DEFAULT_SUPERSEDED_SECS;
        use crate::metadata::sqlite::SqliteMetadataStore;
        use crate::object_store::mem::MemObjectStore;
        type StdArc<T> = std::sync::Arc<T>;
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wbsame-{}-{}",
            std::process::id(),
            tmp_unique()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0, // manual drain, one claim at a time
                batch_pack_bytes: 0,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 0,

                ledger_lock_path: None,
            }),
        )
        .unwrap();
        c.create("/f", 0o644).unwrap();
        let node = c.lookup("/f").unwrap();

        // flush #1 (expected v0) then flush #2 (expected v1 = projected
        // after #1) with NOTHING drained in between: the queued-period
        // coalesce must fold #2 onto #1's task (staging replaced).
        let r1 = c
            .wb()
            .unwrap()
            .write_deferred(&node, 0, &mut std::io::Cursor::new(b"first".to_vec()), 5)
            .unwrap();
        let r2 = c
            .wb()
            .unwrap()
            .write_deferred(
                &r1,
                r1.version,
                &mut std::io::Cursor::new(b"second-generation".to_vec()),
                17,
            )
            .unwrap();
        assert_eq!(r1.version, 1, "flush #1 projects committed v0 + 1 pending");
        assert_eq!(
            r2.version, 1,
            "coalesce must NOT bump the projected version"
        );

        // one drain, one task, one CAS: the FINAL staging wins wholesale
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(
            c.metrics.wb_coalesced.load(Ordering::Relaxed),
            1,
            "flush #2 must be accounted as a coalesce"
        );
        assert_eq!(c.metrics.wb_cas_ok.load(Ordering::Relaxed), 1);
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 0));
        let served = c.read("/f", 0, 100).unwrap();
        assert_eq!(served, b"second-generation", "the later flush must win");
        let final_node = c.lookup("/f").unwrap();
        assert_eq!(
            final_node.version, 1,
            "one CAS: v0 -> v1, no intermediate commits"
        );
        assert_eq!(final_node.size, 17);
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

    /// P0-a: recover() resolves batches a crash left in `committing` and
    /// enrolls dead-task orphan objects as durable deletion intents; the
    /// enrolled object is then reclaimed by the gc executor (Core side).
    #[test]
    fn recover_finalizes_stale_batches_and_enrolls_dead_task_orphans() {
        let meta = StdArc::new(SqliteMetadataStore::open_in_memory().unwrap());
        let objects = StdArc::new(MemObjectStore::new());
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wb-recover-{}-{}",
            std::process::id(),
            tmp_unique()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let opts = WbOptions {
            staging_dir: dir.join("staging").to_string_lossy().to_string(),
            drain_workers: 0,
            batch_pack_bytes: 0,
            batch_max_wait_ms: 0,
            queue_max_tasks: 0,
            staging_budget_bytes: 0,

            ledger_lock_path: None,
        };
        let eng = WriteBackEngine::new(
            meta.clone(),
            objects.clone(),
            StdArc::new(Metrics::default()),
            &opts,
            false,
        )
        .unwrap();

        // A crash left this batch `committing` with no tasks left behind.
        meta.register_pack("packs/stale.data", 10).unwrap();
        meta.mark_pack_uploaded("packs/stale.data", 1).unwrap();
        // A dead task whose bytes had already been uploaded: orphan candidate.
        let n = meta
            .create_node(1, "f", crate::metadata::NodeKind::File, 0o644, None)
            .unwrap();
        let staged = dir.join("wb-dead.data").to_string_lossy().to_string();
        std::fs::write(&staged, b"x").unwrap();
        let (task_id, _) = meta.wb_enqueue(n.id, &staged, 1, 0, 1).unwrap();
        let orphan = "objects/1/w-orphan.data";
        meta.wb_set_uploaded_key(task_id, orphan, 1).unwrap();
        meta.wb_set_state(task_id, "dead", 1).unwrap();
        objects.put(orphan, b"z").unwrap();

        eng.recover().unwrap();

        // Batch sweep: no non-terminal tasks -> terminated, retention starts.
        let (st, unref) = meta.pack_state("packs/stale.data").unwrap().unwrap();
        assert_eq!(st, "unreferenced");
        assert!(unref.is_some());
        // Orphan enrollment: the intent is durable; nothing is deleted here.
        assert!(objects.head(orphan).is_ok());
        // The gc executor (Core side) reclaims the enrolled object.
        let c = Core::new(meta.clone(), objects.clone());
        let (done, failed, _) = c.gc_execute_deletions(10, &HashSet::new()).unwrap();
        assert_eq!((done, failed), (1, 0));
        assert!(objects.head(orphan).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- S4.2.1: recover()/HEAD semantics ----------------------------------

    fn recover_engine() -> (
        StdArc<SqliteMetadataStore>,
        StdArc<MemObjectStore>,
        WriteBackEngine,
        std::path::PathBuf,
    ) {
        let meta = StdArc::new(SqliteMetadataStore::open_in_memory().unwrap());
        let objects = StdArc::new(MemObjectStore::new());
        let dir = std::env::temp_dir().join(format!(
            "lightfs-wb-p0a1-{}-{}",
            std::process::id(),
            tmp_unique()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let opts = WbOptions {
            staging_dir: dir.join("staging").to_string_lossy().to_string(),
            drain_workers: 0,
            batch_pack_bytes: 0,
            batch_max_wait_ms: 0,
            queue_max_tasks: 0,
            staging_budget_bytes: 0,

            ledger_lock_path: None,
        };
        let eng = WriteBackEngine::new(
            meta.clone(),
            objects.clone(),
            StdArc::new(Metrics::default()),
            &opts,
            false,
        )
        .unwrap();
        (meta, objects, eng, dir)
    }

    fn seed_dead_uploaded_task(meta: &SqliteMetadataStore, staging: &str, orphan: &str) {
        let n = meta
            .create_node(1, "f", crate::metadata::NodeKind::File, 0o644, None)
            .unwrap();
        let (task_id, _) = meta.wb_enqueue(n.id, staging, 1, 0, 1).unwrap();
        meta.wb_set_uploaded_key(task_id, orphan, 1).unwrap();
        meta.wb_set_state(task_id, "dead", 1).unwrap();
    }

    /// HEAD confirms the object is absent: no deletion intent may be
    /// enrolled for an object that no longer exists (baseline semantics —
    /// the pre-S4.2.1 code also skipped this case, silently).
    #[test]
    fn p0a1_recover_head_absent_enrolls_nothing() {
        let (meta, _objects, eng, dir) = recover_engine();
        let staged = dir.join("s1.data").to_string_lossy().to_string();
        std::fs::write(&staged, b"x").unwrap();
        seed_dead_uploaded_task(&meta, &staged, "objects/1/absent.data");
        eng.recover().unwrap();
        let claimed = meta.gc_claim_deletions(10, 1).unwrap();
        assert!(
            claimed.is_empty(),
            "no executable intent for an absent object: {claimed:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// HEAD unavailable (network/permission/timeout): fail closed — no
    /// intent, record kept, and the next determinate recover() converges.
    #[test]
    fn p0a1_recover_head_error_fails_closed_keeps_record() {
        let (meta, objects, eng, dir) = recover_engine();
        let orphan = "objects/1/unreachable.data";
        let staged = dir.join("s2.data").to_string_lossy().to_string();
        std::fs::write(&staged, b"x").unwrap();
        seed_dead_uploaded_task(&meta, &staged, orphan);
        objects.put(orphan, b"z").unwrap();
        objects.inject_head_failure(orphan);

        eng.recover().unwrap();
        assert!(
            meta.gc_claim_deletions(10, 1).unwrap().is_empty(),
            "cannot-tell must not become a deletion intent"
        );
        assert!(
            meta.dead_task_orphans()
                .unwrap()
                .contains(&orphan.to_string()),
            "the record is kept for the next recover, not silently dropped"
        );

        // Once HEAD is determinate again, the deferred record converges —
        // into an INTENT; nothing is deleted here.
        eng.recover().unwrap();
        let claimed = meta.gc_claim_deletions(10, 1).unwrap();
        assert_eq!(claimed.len(), 1, "deferred, not dropped");
        assert!(objects.head(orphan).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Repeated recover() runs: lifecycle state is never reset and a
    /// completed intent is not resurrected (the object is confirmed gone).
    #[test]
    fn p0a1_recover_repeated_runs_are_idempotent() {
        let (meta, objects, eng, dir) = recover_engine();
        meta.register_pack("packs/stale2.data", 10).unwrap();
        meta.mark_pack_uploaded("packs/stale2.data", 1).unwrap();
        let orphan = "objects/1/twice.data";
        let staged = dir.join("s3.data").to_string_lossy().to_string();
        std::fs::write(&staged, b"x").unwrap();
        seed_dead_uploaded_task(&meta, &staged, orphan);
        objects.put(orphan, b"z").unwrap();

        eng.recover().unwrap();
        let (st1, unref1) = meta.pack_state("packs/stale2.data").unwrap().unwrap();
        assert_eq!(st1, "unreferenced");
        assert!(unref1.is_some());
        let claimed = meta.gc_claim_deletions(10, 1).unwrap();
        assert_eq!(claimed.len(), 1);
        // Executor order: remote delete first, then the durable finalize.
        objects.delete(orphan).unwrap();
        meta.gc_complete_deletion(claimed[0].id, 1).unwrap();

        eng.recover().unwrap();
        let (st2, unref2) = meta.pack_state("packs/stale2.data").unwrap().unwrap();
        assert_eq!(
            (st2.as_str(), unref2),
            ("unreferenced", unref1),
            "no lifecycle reset on re-recover"
        );
        let claimed2 = meta.gc_claim_deletions(10, 1).unwrap();
        assert!(claimed2.is_empty(), "a completed intent is not resurrected");
        let _ = std::fs::remove_dir_all(&dir);
    }
    /// Drive write_deferred with an explicit len (tests stage from memory).
    fn write_deferred_for_test(c: &Core, n: &Node, body: &[u8], len: usize) -> Result<Node> {
        c.wb()
            .unwrap()
            .write_deferred(n, 0, &mut std::io::Cursor::new(body.to_vec()), len as u64)
    }

    // ---- P0-c: staging capacity accounting + bounded-memory streaming ----

    fn cap_core(tag: &str, budget: u64, batch_bytes: u64) -> Core {
        let dir = std::env::temp_dir().join(format!("lightfs-p0c-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: batch_bytes,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: budget,
                ledger_lock_path: None,
            }),
        )
        .unwrap();
        c
    }

    /// Boundary: a write that exactly fills the budget lands; one more
    /// byte gets the explicit capacity-exhausted error.
    #[test]
    fn p0c_capacity_boundary_exact_and_overflow() {
        let c = cap_core("boundary", 100, 0);
        c.create("/f", 0o644).unwrap();
        let n = c.lookup("/f").unwrap();
        let _ = write_deferred_for_test(&c, &n, &[b'a'; 100], 100).unwrap();
        let (staged, reserved) = c.metadata().wb_capacity_snapshot().unwrap();
        assert_eq!(
            (staged, reserved),
            (100, 0),
            "sync_all passed: staged, not reserved"
        );
        let e = write_deferred_for_test(&c, &n, b"x", 1).unwrap_err();
        assert!(
            format!("{e}").starts_with("capacity-exhausted"),
            "identifiable exhaustion: {e}"
        );
        // the failed write reserved nothing and staged nothing
        assert_eq!(c.metadata().wb_capacity_snapshot().unwrap(), (100, 0));
    }

    /// Concurrent reservations serialize in SQLite: exactly floor(budget/N)
    /// of N equal reservations can win, never one more.
    #[test]
    fn p0c_concurrent_reserves_never_overbook() {
        let meta: StdArc<SqliteMetadataStore> =
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap());
        let budget = 1000i64;
        let ok = StdArc::new(std::sync::atomic::AtomicUsize::new(0));
        let exhausted = StdArc::new(std::sync::atomic::AtomicUsize::new(0));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let meta = meta.clone();
                let ok = ok.clone();
                let exhausted = exhausted.clone();
                scope.spawn(move || {
                    for _ in 0..50 {
                        match meta.wb_capacity_reserve(200, budget) {
                            Ok(()) => {
                                ok.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(Error::Capacity(_)) => {
                                exhausted.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => panic!("unexpected: {e}"),
                        }
                    }
                });
            }
        });
        assert_eq!(
            ok.load(Ordering::Relaxed),
            5,
            "budget 1000 / 200 = exactly 5"
        );
        assert_eq!(exhausted.load(Ordering::Relaxed), 8 * 50 - 5);
        assert_eq!(meta.wb_capacity_snapshot().unwrap(), (0, 1000));
    }

    /// A staging write whose half-written file CANNOT be deleted keeps its
    /// reservation (invariant 4: bytes may still occupy the disk). A later
    /// reconcile() — which reflects reality — clears it.
    #[test]
    fn p0c_half_written_file_keeps_reservation_until_reconciled() {
        let c = cap_core("halfwrite", 0, 0);
        c.create("/f", 0o644).unwrap();
        let n = c.lookup("/f").unwrap();
        use std::os::unix::fs::PermissionsExt;
        let staging_dir = c.wb().unwrap().staging_dir().to_path_buf();
        std::fs::set_permissions(&staging_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        let e = write_deferred_for_test(&c, &n, b"doomed", 6).unwrap_err();
        assert!(!format!("{e}").is_empty());
        std::fs::set_permissions(&staging_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        // the half-written file could not be deleted -> reservation kept
        let (_, reserved) = c.metadata().wb_capacity_snapshot().unwrap();
        assert_eq!(reserved, 6, "no release while the file may still exist");
        // reconcile reflects reality: no staging file survived
        let (staged, reserved) = c.wb().unwrap().reconcile_capacity_and_report().unwrap();
        assert_eq!(
            (staged, reserved),
            (0, 0),
            "reconcile sees no file, voids the reservation"
        );
    }

    /// Dead tasks keep occupying capacity; exhaustion says so explicitly.
    #[test]
    fn p0c_dead_and_orphans_occupy_and_reconcile() {
        let dir = std::env::temp_dir().join(format!("lightfs-p0c-dead-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("staging")).unwrap();
        let meta_path = dir.join("m.db");
        let staging_dir = dir.join("staging");
        // Build a db + staging set in a first engine: one dead task, one orphan file.
        {
            let c = Core::with_options(
                StdArc::new(SqliteMetadataStore::open(&meta_path.to_string_lossy()).unwrap()),
                StdArc::new(MemObjectStore::new()),
                crate::core::DEFAULT_SUPERSEDED_SECS,
                None,
                Some(WbOptions {
                    staging_dir: staging_dir.to_string_lossy().to_string(),
                    drain_workers: 0,
                    batch_pack_bytes: 0,
                    batch_max_wait_ms: 0,
                    queue_max_tasks: 0,
                    staging_budget_bytes: 0,

                    ledger_lock_path: None,
                }),
            )
            .unwrap();
            c.create("/f", 0o644).unwrap();
            let n = c.lookup("/f").unwrap();
            // dead task with staging retained
            let dead_path = staging_dir.join("wb-dead-1.data");
            std::fs::write(&dead_path, vec![b'd'; 30]).unwrap();
            let meta = SqliteMetadataStore::open(&meta_path.to_string_lossy()).unwrap();
            meta.wb_enqueue(n.id, &dead_path.to_string_lossy(), 30, 0, 1)
                .unwrap();
            meta.wb_set_state(1, "dead", 2).unwrap();
            // orphan file with no task
            std::fs::write(staging_dir.join("wb-orphan-1.data"), vec![b'o'; 50]).unwrap();
        }
        // Reopen: reconcile counts BOTH files (dead stays, orphan counted
        // then swept+released), reservations void.
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open(&meta_path.to_string_lossy()).unwrap()),
            StdArc::new(MemObjectStore::new()),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: staging_dir.to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 0,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 0,

                ledger_lock_path: None,
            }),
        )
        .unwrap();
        c.wb().unwrap().recover().unwrap();
        let (staged, reserved) = c.metadata().wb_capacity_snapshot().unwrap();
        assert_eq!(reserved, 0, "reboot voids every reservation");
        assert_eq!(
            staged, 30,
            "orphan 50B swept+released; dead 30B still accounted"
        );
        // dead bytes keep occupying: an explicit budget that only fits the
        // dead file refuses further writes with the explicit error.
        // (P0-c supplement: the staging dir admits one live engine, so the
        // previous engine exits before the budgeted reopen.)
        drop(c);
        let c2 = Core::with_options(
            StdArc::new(SqliteMetadataStore::open(&meta_path.to_string_lossy()).unwrap()),
            StdArc::new(MemObjectStore::new()),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: staging_dir.to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 0,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 100,

                ledger_lock_path: None,
            }),
        )
        .unwrap();
        c2.wb().unwrap().recover().unwrap();
        let e =
            write_deferred_for_test(&c2, &c2.lookup("/f").unwrap(), &[b'x'; 71], 71).unwrap_err();
        assert!(format!("{e}").starts_with("capacity-exhausted"), "{e}");
        // 30 (dead) + 60 fits exactly
        write_deferred_for_test(&c2, &c2.lookup("/f").unwrap(), &[b'y'; 60], 60).unwrap();
        assert_eq!(c2.metadata().wb_capacity_snapshot().unwrap(), (90, 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// drain success with a staging file that cannot be deleted: bytes stay
    /// accounted (invariant 5), the task is still terminal, and a later
    /// recover() sweep releases them once the delete actually succeeds.
    #[test]
    fn p0c_drained_but_undeletable_keeps_bytes_until_sweep() {
        let dir = std::env::temp_dir().join(format!("lightfs-p0c-undel-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("staging")).unwrap();
        let meta_path = dir.join("m.db");
        let staging_dir = dir.join("staging");
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open(&meta_path.to_string_lossy()).unwrap()),
            StdArc::new(MemObjectStore::new()),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: staging_dir.to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 0,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 0,

                ledger_lock_path: None,
            }),
        )
        .unwrap();
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", b"payload-30-bytes----padding!", Some(0));
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staging_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        assert!(
            c.wb().unwrap().drain_once().unwrap(),
            "drain succeeds (upload+CAS+finish)"
        );
        std::fs::set_permissions(&staging_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(
            c.read("/f", 0, 100).unwrap(),
            b"payload-30-bytes----padding!"
        );
        let (staged, _) = c.metadata().wb_capacity_snapshot().unwrap();
        assert!(staged > 0, "undeletable staging keeps its bytes accounted");
        // the file physically gone-able now: recover sweeps and releases
        c.wb().unwrap().recover().unwrap();
        assert_eq!(c.metadata().wb_capacity_snapshot().unwrap(), (0, 0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Tracking wrapper: forwards to a MemObjectStore but mirrors the
    /// production S3 put_stream read pattern (bounded 8 MiB windows) and
    /// records the LARGEST single read request ever made — the observable
    /// proof that no caller reads a whole pack/file in one go.
    struct TrackingStore {
        inner: MemObjectStore,
        max_window: StdArc<std::sync::atomic::AtomicUsize>,
    }
    impl TrackingStore {
        fn new() -> Self {
            Self {
                inner: MemObjectStore::new(),
                max_window: StdArc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }
        fn max_window(&self) -> usize {
            self.max_window.load(Ordering::Relaxed)
        }
    }
    struct WindowTrackingReader {
        inner: Box<dyn std::io::Read + Send>,
        max: StdArc<std::sync::atomic::AtomicUsize>,
    }
    impl std::io::Read for WindowTrackingReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.max.fetch_max(buf.len(), Ordering::Relaxed);
            self.inner.read(buf)
        }
    }
    impl crate::object_store::ObjectStore for TrackingStore {
        fn put(
            &self,
            key: &str,
            data: &[u8],
        ) -> crate::error::Result<crate::object_store::ObjectInfo> {
            self.inner.put(key, data)
        }
        fn put_stream(
            &self,
            key: &str,
            reader: &mut dyn std::io::Read,
            len: u64,
        ) -> crate::error::Result<crate::object_store::ObjectInfo> {
            // Mirrors s3.rs put_stream: >=8 MiB goes out in bounded 8 MiB
            // part windows; smaller objects in exactly one len-sized read.
            const PART: usize = 8 * 1024 * 1024;
            let mut collected = Vec::with_capacity(len as usize);
            loop {
                let mut buf = vec![0u8; PART.min(len as usize).max(1)];
                let filled = read_full(reader, &mut buf)?;
                if filled == 0 {
                    break;
                }
                buf.truncate(filled);
                collected.extend_from_slice(&buf);
                if filled < buf.len() {
                    break;
                }
            }
            self.max_window
                .fetch_max(collected.len().clamp(1, PART), Ordering::Relaxed);
            self.inner.put(key, &collected)
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
            let (r, len) = self.inner.get_stream(key, range)?;
            Ok((
                Box::new(WindowTrackingReader {
                    inner: r,
                    max: self.max_window.clone(),
                }),
                len,
            ))
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

    /// Streaming batch: byte layout identical to the old in-memory concat,
    /// and the largest read window is bounded (<= 8 MiB, the S3 part size),
    /// NOT the pack size.
    #[test]
    fn p0c_batch_streams_bounded_and_layout_is_identical() {
        let dir = std::env::temp_dir().join(format!("lightfs-p0c-bstream-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("staging")).unwrap();
        let store = StdArc::new(TrackingStore::new());
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            store.clone(),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 1 << 20,
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 0,

                ledger_lock_path: None,
            }),
        )
        .unwrap();
        let bodies = [vec![b'a'; 1000], vec![b'b'; 500], vec![b'c'; 250]];
        for (i, body) in bodies.iter().enumerate() {
            let p = format!("/s{i}");
            c.create(&p, 0o644).unwrap();
            let _ = write_via_rpc(&c, &p, body, Some(0));
        }
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert!(!c.wb().unwrap().drain_once().unwrap(), "queue drained");
        // byte layout: the pack object IS the concatenation
        let n = c.lookup("/s0").unwrap();
        let key = n.object_key.as_deref().unwrap().to_string();
        let mut want = Vec::new();
        want.extend_from_slice(&bodies[0]);
        want.extend_from_slice(&bodies[1]);
        want.extend_from_slice(&bodies[2]);
        let got = store.get(&key, None).unwrap();
        assert_eq!(
            got.data, want,
            "pack = exact concatenation of staging files"
        );
        for (i, body) in bodies.iter().enumerate() {
            assert_eq!(c.read(&format!("/s{i}"), 0, 10000).unwrap(), *body);
        }
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 3);
        // bounded window: 1750-byte pack read in windows <= 8 MiB — and,
        // decisively, no single read as large as reading beyond one part
        assert!(store.max_window() <= 8 * 1024 * 1024, "window bound");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A single file LARGER than the 8 MiB part size drains per-file via
    /// put_stream with the same bounded window; content verifies bytewise.
    #[test]
    fn p0c_oversized_single_file_streams_bounded() {
        let dir = std::env::temp_dir().join(format!("lightfs-p0c-big-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("staging")).unwrap();
        let store = StdArc::new(TrackingStore::new());
        let c = Core::with_options(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            store.clone(),
            crate::core::DEFAULT_SUPERSEDED_SECS,
            None,
            Some(WbOptions {
                staging_dir: dir.join("staging").to_string_lossy().to_string(),
                drain_workers: 0,
                batch_pack_bytes: 0, // per-file drain
                batch_max_wait_ms: 0,
                queue_max_tasks: 0,
                staging_budget_bytes: 0,

                ledger_lock_path: None,
            }),
        )
        .unwrap();
        let big: Vec<u8> = (0..17 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
        c.create("/big", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/big", &big, Some(0));
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(c.read("/big", 0, big.len() as u64).unwrap(), big);
        assert_eq!(c.metrics.wb_drained.load(Ordering::Relaxed), 1);
        assert!(
            store.max_window() <= 8 * 1024 * 1024,
            "largest read window {} must be the 8 MiB part bound, not the {}-byte file",
            store.max_window(),
            big.len()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- P0-c acceptance supplement: single-writer enforcement ------------

    fn p0c2_opts(staging: &Path, ledger: Option<String>) -> WbOptions {
        WbOptions {
            staging_dir: staging.to_string_lossy().to_string(),
            drain_workers: 0,
            batch_pack_bytes: 0,
            batch_max_wait_ms: 0,
            queue_max_tasks: 0,
            staging_budget_bytes: 1000,
            ledger_lock_path: ledger,
        }
    }

    /// `.unwrap_err()` needs the Ok type to be Debug (the engine is not);
    /// match the refusal out instead.
    fn p0c2_expect_refused<T>(r: Result<T>) -> Error {
        match r {
            Ok(_) => panic!("second engine must be refused while the first is live"),
            Err(e) => e,
        }
    }

    fn p0c2_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "lightfs-p0c2-{tag}-{}-{}",
            std::process::id(),
            tmp_unique()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("staging")).unwrap();
        dir
    }

    /// A (dual-engine conflict): while the first engine is live — with
    /// staged bytes AND an in-flight reservation — a second engine is
    /// refused on every sharing axis (same staging dir; same ledger with a
    /// different staging dir; same staging dir with a different ledger).
    /// After the first engine exits, the second starts, reconciles safely
    /// (in-flight reservation of the dead engine voided, live staged bytes
    /// kept) and owns both locks.
    #[test]
    fn p0c2_second_engine_refused_while_first_is_active() {
        let dir = p0c2_dir("dual");
        let db = dir.join("m.db");
        let ledger = format!("{}.engine-lock", db.display());
        let staging = dir.join("staging");
        let meta = StdArc::new(SqliteMetadataStore::open(&db.to_string_lossy()).unwrap());
        let objects = StdArc::new(MemObjectStore::new());
        let eng = WriteBackEngine::new(
            meta.clone(),
            objects.clone(),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&staging, Some(ledger.clone())),
            false,
        )
        .unwrap();
        // Live state the second engine's reconcile would corrupt: one
        // staged file plus one reservation still in flight.
        let n = meta
            .create_node(1, "f", crate::metadata::NodeKind::File, 0o644, None)
            .unwrap();
        eng.write_deferred(&n, 0, &mut std::io::Cursor::new(vec![b'a'; 100]), 100)
            .unwrap();
        meta.wb_capacity_reserve(50, 1000).unwrap();
        assert_eq!(
            meta.wb_capacity_snapshot().unwrap(),
            (100, 50),
            "first engine holds staged bytes and an in-flight reservation"
        );

        // Same staging dir (fresh ledger even): refused — this is the
        // orphan-sweep hazard (the second engine's recover would delete the
        // first engine's staging files).
        let r = WriteBackEngine::new(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            objects.clone(),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&staging, None),
            false,
        );
        let e = p0c2_expect_refused(r);
        assert!(format!("{e}").contains("staging directory"), "{e}");

        // Different staging dir, same ledger: refused — reconcile there
        // would void this engine's reservations.
        let staging2 = dir.join("staging2");
        std::fs::create_dir_all(&staging2).unwrap();
        let r = WriteBackEngine::new(
            meta.clone(),
            objects.clone(),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&staging2, Some(ledger.clone())),
            false,
        );
        let e = p0c2_expect_refused(r);
        assert!(format!("{e}").contains("capacity ledger"), "{e}");

        // First engine exits: the second starts cleanly, reconciles, and
        // takes over both locks.
        drop(eng);
        let eng2 = WriteBackEngine::new(
            meta.clone(),
            objects.clone(),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&staging, Some(ledger)),
            false,
        )
        .unwrap();
        eng2.recover().unwrap();
        assert_eq!(
            eng2.reconcile_capacity_and_report().unwrap(),
            (100, 0),
            "live staged bytes kept; the dead engine's in-flight reservation voided"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// B (deleted file, ledger not yet released): the drain confirmed the
    /// physical delete and the process died before `staged_bytes` caught
    /// up. Restart + reconcile must drop the phantom bytes, keep the
    /// occupancy of files that still exist (dead letter), and apply the
    /// orphan contract (counted on disk, released on confirmed sweep).
    #[test]
    fn p0c2_crash_between_delete_and_release_reconciles() {
        let core = cap_core("p0c2b", 10_000, 0);
        core.create("/f", 0o644).unwrap();
        let n = core.lookup("/f").unwrap();
        write_deferred_for_test(&core, &n, &[b'a'; 100], 100).unwrap();
        write_deferred_for_test(&core, &n, &[b'b'; 200], 200).unwrap();
        let meta = core.metadata().clone();
        let objects = core.objects().clone();
        let staging_dir = core.wb().unwrap().staging_dir().to_path_buf();
        let mut tasks = meta.wb_active_tasks().unwrap();
        tasks.sort_by_key(|t| t.size);
        assert_eq!(tasks.len(), 2);
        let (t_small, t_big) = (tasks[0].clone(), tasks[1].clone());
        assert_eq!((t_small.size, t_big.size), (100, 200));
        assert_eq!(
            meta.wb_capacity_snapshot().unwrap(),
            (300, 0),
            "the ledger is correct up to the simulated crash"
        );
        // The crash window: physical delete confirmed, ledger update never
        // happened. 200 phantom bytes.
        std::fs::remove_file(&t_big.staging).unwrap();
        // A dead letter that still occupies its real file, and an orphan
        // nobody has a task row for.
        meta.wb_set_state(t_small.id, "dead", 1).unwrap();
        let orphan = staging_dir.join("wb-orphan-p0c2b.data");
        std::fs::write(&orphan, vec![b'o'; 50]).unwrap();

        // Restart: the ledger is rebuilt from REAL files only.
        drop(core);
        let eng = WriteBackEngine::new(
            meta.clone(),
            objects,
            StdArc::new(Metrics::default()),
            &p0c2_opts(&staging_dir, None),
            false,
        )
        .unwrap();
        eng.recover().unwrap();
        assert_eq!(
            eng.reconcile_capacity_and_report().unwrap(),
            (100, 0),
            "phantom 200B dropped; dead letter's 100B kept; orphan 50B swept+released"
        );
        assert!(
            !meta
                .wb_active_tasks()
                .unwrap()
                .iter()
                .any(|t| t.id == t_big.id || t.id == t_small.id),
            "the deleted-file task is dead-lettered (staging vanished), not resurrected"
        );
        assert!(
            Path::new(&t_small.staging).exists(),
            "dead letter file kept"
        );
        assert!(!orphan.exists(), "orphan swept");
        let _ = std::fs::remove_dir_all(staging_dir.parent().unwrap());
    }

    /// C (concurrent recovery vs writes cannot arise): a SECOND PROCESS
    /// holding the staging lock is refused entry while alive, and the lock
    /// is released by the kernel the moment that process is SIGKILLed —
    /// no stale lock, no retry protocol. The child is this same test
    /// binary running `p0c2_lock_child_helper`.
    #[test]
    fn p0c2_cross_process_exclusion_and_release_on_death() {
        let dir = p0c2_dir("xproc");
        let staging = dir.join("staging");
        let ready = dir.join("child.ready");
        let exe = std::env::current_exe().unwrap();
        let child = std::process::Command::new(&exe)
            .args(["p0c2_lock_child_helper", "--nocapture"])
            .env("LIGHTFS_P0C2_CHILD_STAGING", &staging)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        // Kill the child on every path: a failing parent must not leak a
        // lock-holding process.
        struct Guard(std::process::Child);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut guard = Guard(child);
        let mut waited = 0u32;
        while !ready.exists() && waited < 10_000 {
            std::thread::sleep(Duration::from_millis(20));
            waited += 20;
        }
        assert!(ready.exists(), "child never acquired the lock");
        let r = WriteBackEngine::new(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&staging, None),
            false,
        );
        let e = p0c2_expect_refused(r);
        assert!(
            format!("{e}").contains("already owned by a live engine"),
            "{e}"
        );
        // SIGKILL the holder: the kernel drops the lock immediately.
        let _ = guard.0.kill();
        let _ = guard.0.wait();
        // Bounded retry (2s): kernel lock release can stay invisible for a
        // few tens of microseconds; a lock that never released still fails.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let eng = loop {
            match WriteBackEngine::new(
                StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
                StdArc::new(MemObjectStore::new()),
                StdArc::new(Metrics::default()),
                &p0c2_opts(&staging, None),
                false,
            ) {
                Ok(eng) => break eng,
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        panic!("staging lock not reacquired after holder death: {e}");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        };
        assert_eq!(
            eng.reconcile_capacity_and_report().unwrap(),
            (0, 0),
            "clean start after the previous holder died"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Helper for `p0c2_cross_process_exclusion_and_release_on_death`:
    /// takes the staging lock in THIS process, signals readiness, and holds
    /// until SIGKILLed. A no-op when the env var is absent (plain suite
    /// run), so the normal test sweep passes through instantly.
    #[test]
    fn p0c2_lock_child_helper() {
        let Some(staging) = std::env::var_os("LIGHTFS_P0C2_CHILD_STAGING") else {
            return;
        };
        let staging = PathBuf::from(staging);
        let _engine = WriteBackEngine::new(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&staging, None),
            false,
        )
        .expect("child takes the fresh staging lock");
        let ready = staging.parent().unwrap().join("child.ready");
        std::fs::write(ready, b"lock held").unwrap();
        loop {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    // ---- S4.3.4b.2: lock identity canonicalization (path aliases) ---------

    /// A: the staging lock keys on the directory's CANONICAL identity.
    /// Engine 1 holds it via the real absolute path; engine 2 through a
    /// symlink to the same directory is refused; after engine 1 exits the
    /// symlink spelling starts cleanly. Symlinks: unix (executed on macOS
    /// and Linux; gated out on windows where creation needs privileges).
    #[test]
    #[cfg(unix)]
    fn p0c3_staging_symlink_alias_refused() {
        let dir = p0c2_dir("alias");
        let real = dir.join("staging-real");
        std::fs::create_dir_all(&real).unwrap();
        let link = dir.join("staging-link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let eng = WriteBackEngine::new(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&real, None),
            false,
        )
        .unwrap();
        let e = p0c2_expect_refused(WriteBackEngine::new(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&link, None),
            false,
        ));
        assert!(format!("{e}").contains("staging directory"), "{e}");
        drop(eng);
        // Bounded retry (2s) — see p0c2: kernel lock release after a close
        // can stay invisible for a few tens of microseconds.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let eng2 = loop {
            match WriteBackEngine::new(
                StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
                StdArc::new(MemObjectStore::new()),
                StdArc::new(Metrics::default()),
                &p0c2_opts(&link, None),
                false,
            ) {
                Ok(eng) => break eng,
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        panic!("staging lock not reacquired after drop: {e}");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        };
        assert_eq!(
            eng2.reconcile_capacity_and_report().unwrap(),
            (0, 0),
            "the alias spelling owns the directory once the holder exits"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A, relative-vs-absolute: a CHILD PROCESS takes the lock via the
    /// RELATIVE spelling (it chdir's first); the parent is refused through
    /// both the absolute real path and the symlink — one canonical lock.
    /// After the child is SIGKILLed, the parent starts via the symlink.
    #[test]
    #[cfg(unix)]
    fn p0c3_relative_spelling_holds_the_same_lock_cross_process() {
        let dir = p0c2_dir("relalias");
        let real = dir.join("staging-real");
        std::fs::create_dir_all(&real).unwrap();
        let link = dir.join("staging-link");
        std::os::unix::fs::symlink(&real, &link).unwrap();
        let ready = dir.join("child.ready");
        let exe = std::env::current_exe().unwrap();
        let child = std::process::Command::new(&exe)
            .args(["p0c3_lock_child_helper_relative", "--nocapture"])
            .env("LIGHTFS_P0C3_CHILD_DIR", &dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        struct Guard(std::process::Child);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut guard = Guard(child);
        let mut waited = 0u32;
        while !ready.exists() && waited < 10_000 {
            std::thread::sleep(Duration::from_millis(20));
            waited += 20;
        }
        assert!(
            ready.exists(),
            "child never acquired the lock (relative spelling)"
        );
        let e = p0c2_expect_refused(WriteBackEngine::new(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            StdArc::new(Metrics::default()),
            &p0c2_opts(&link, None),
            false,
        ));
        assert!(format!("{e}").contains("staging directory"), "{e}");
        let _ = guard.0.kill();
        let _ = guard.0.wait();
        // Bounded retry (2s) — see p0c2: kernel lock release after the
        // holder's death can stay invisible for a few tens of microseconds.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        let eng = loop {
            match WriteBackEngine::new(
                StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
                StdArc::new(MemObjectStore::new()),
                StdArc::new(Metrics::default()),
                &p0c2_opts(&link, None),
                false,
            ) {
                Ok(eng) => break eng,
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        panic!("staging lock not reacquired after holder death: {e}");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        };
        assert_eq!(
            eng.reconcile_capacity_and_report().unwrap(),
            (0, 0),
            "clean start after the relative-spelling holder died"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Helper for `p0c3_relative_spelling_holds_the_same_lock_cross_process`:
    /// chdir's into the given dir and locks the staging dir by its RELATIVE
    /// name. No-op without the env var.
    #[test]
    fn p0c3_lock_child_helper_relative() {
        let Some(dir) = std::env::var_os("LIGHTFS_P0C3_CHILD_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        std::env::set_current_dir(&dir).expect("child chdir");
        let _engine = WriteBackEngine::new(
            StdArc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            StdArc::new(MemObjectStore::new()),
            StdArc::new(Metrics::default()),
            &p0c2_opts(Path::new("staging-real"), None),
            false,
        )
        .expect("child takes the lock via the relative spelling");
        std::fs::write(dir.join("child.ready"), b"lock held").unwrap();
        loop {
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// B: the ledger lock AND the SQLite connection key on the DATABASE
    /// FILE's canonical identity. A dot-path alias and (unix) a symlink
    /// both resolve to the real file: `db_path()` reports the canonical
    /// path, the -wal sidecar lands beside the real file and never beside
    /// the alias, and a second engine through the alias with a different
    /// staging dir is refused by the LEDGER lock. Alias spellings start
    /// cleanly once the real-path engine exits.
    #[test]
    fn p0c3_db_alias_one_identity_one_ledger() {
        let dir = p0c2_dir("dbalias");
        let db = dir.join("m.db");
        let dot = dir.join("..").join(dir.file_name().unwrap()).join("m.db");
        // 1) the connection itself opens the canonical identity
        let meta = SqliteMetadataStore::open(&dot.to_string_lossy()).unwrap();
        let canonical = meta.db_path().unwrap().to_path_buf();
        assert_eq!(
            canonical,
            std::fs::canonicalize(&db).unwrap(),
            "a dot-path spelling resolves to the real file identity"
        );
        meta.create_node(1, "f", crate::metadata::NodeKind::File, 0o644, None)
            .unwrap();
        assert!(
            canonical.with_file_name("m.db-wal").exists(),
            "the -wal sidecar is anchored at the canonical name"
        );
        drop(meta);
        // 2) unix: a symlink entry resolves to the same identity and NEVER
        //    produces its own sidecar stream beside the same file.
        #[cfg(unix)]
        {
            let link = dir.join("m-link.db");
            std::os::unix::fs::symlink(&db, &link).unwrap();
            let meta2 = SqliteMetadataStore::open(&link.to_string_lossy()).unwrap();
            assert_eq!(
                meta2.db_path().unwrap(),
                std::fs::canonicalize(&db).unwrap()
            );
            meta2
                .create_node(1, "g", crate::metadata::NodeKind::File, 0o644, None)
                .unwrap();
            assert!(
                canonical.with_file_name("m.db-wal").exists(),
                "writes through the alias land in the canonical WAL stream"
            );
            assert!(
                !dir.join("m-link.db-wal").exists(),
                "no second WAL stream may appear under the alias name"
            );
            drop(meta2);
        }
        // 3) the engine's ledger lock derives from the same identity:
        //    engine 1 via the real path, second engine via the alias with a
        //    DIFFERENT staging dir — refused by the ledger axis.
        let cfg_for = |path: &Path, staging: &Path| {
            let mut cfg = crate::config::Config::load_or_defaults(None).unwrap();
            cfg.metadata.path = path.to_string_lossy().to_string();
            cfg.commit.mode = "writeback".into();
            cfg.writeback.staging_dir = staging.to_string_lossy().to_string();
            cfg
        };
        let eng = crate::engine::open_embedded_mem(&cfg_for(&db, &dir.join("s1"))).unwrap();
        let alias = {
            #[cfg(unix)]
            {
                dir.join("m-link.db")
            }
            #[cfg(not(unix))]
            {
                dot
            }
        };
        let e = p0c2_expect_refused(crate::engine::open_embedded_mem(&cfg_for(
            &alias,
            &dir.join("s2"),
        )));
        assert!(format!("{e}").contains("capacity ledger"), "{e}");
        // Graceful engine exit: stop the drain worker (it holds an Arc of
        // the engine, so a bare drop would keep the locks in-process for up
        // to its 200ms park), then drop and wait for the kernel locks to
        // actually re-acquire.
        eng.core.wb().unwrap().stop();
        drop(eng);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        let _eng2 = loop {
            match crate::engine::open_embedded_mem(&cfg_for(&alias, &dir.join("s2"))) {
                Ok(e) => break e,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                Err(e) => panic!("alias engine never started after the first exited: {e}"),
            }
        };
        let _ = std::fs::remove_dir_all(dir.parent().unwrap().join(dir.file_name().unwrap()));
    }

    // ------------------------------------------------------------------
    // S4.4 P0-d: R15 durability order (staging namespace), R17 error
    // classes + retry admission, R16 process-crash recovery (G1).
    // ------------------------------------------------------------------

    /// Read the engine's recorded durability steps.
    #[cfg(test)]
    fn p0d_log(wb: &WriteBackEngine) -> Vec<&'static str> {
        wb.sync_probe
            .lock()
            .unwrap()
            .as_ref()
            .map(|p| p.log.lock().unwrap().clone())
            .unwrap_or_default()
    }

    /// R15: the success path performs file sync, THEN staging-dir sync,
    /// THEN the enqueue commit — in that order, before any ack.
    #[test]
    fn p0d_r15_durability_order_is_file_sync_dir_sync_enqueue() {
        let c = wb_core("r15order");
        c.create("/f", 0o644).unwrap();
        let node = c.lookup("/f").unwrap();
        let wb = c.wb().unwrap();
        *wb.sync_probe.lock().unwrap() = Some(SyncProbe::default());
        let out = write_deferred_for_test(&c, &node, b"r15", 3).unwrap();
        assert_eq!(out.version, 1);
        assert_eq!(
            p0d_log(wb),
            vec!["file-sync", "dir-sync", "enqueue"],
            "durability steps must run in this order before the ack"
        );
        assert_eq!(wb.queue_counts(), (1, 0, 0));
    }

    /// R15: a failed FILE sync must not run any later step and must not
    /// acknowledge — the unenqueued staging file is dropped and its
    /// reservation released.
    #[test]
    fn p0d_r15_file_sync_failure_refuses_the_ack() {
        let c = wb_core("r15filesync");
        c.create("/f", 0o644).unwrap();
        let node = c.lookup("/f").unwrap();
        let wb = c.wb().unwrap();
        *wb.sync_probe.lock().unwrap() = Some(SyncProbe {
            fail_on: vec!["file-sync"],
            ..SyncProbe::default()
        });
        let e = write_deferred_for_test(&c, &node, b"r15", 3).unwrap_err();
        assert!(format!("{e}").contains("file-sync"), "{e}");
        assert_eq!(
            p0d_log(wb),
            vec!["file-sync"],
            "nothing after the failed step"
        );
        assert_eq!(wb.queue_counts(), (0, 0, 0), "no task may exist");
        assert_eq!(
            wb.reconcile_capacity_and_report().unwrap(),
            (0, 0),
            "bytes dropped with the ack"
        );
    }

    /// R15: a failed staging-DIR sync must not enqueue and must not
    /// acknowledge — the file name was not proven durable, so the task
    /// that promises those bytes must never commit.
    #[test]
    fn p0d_r15_dir_sync_failure_refuses_the_ack() {
        let c = wb_core("r15dirsync");
        c.create("/f", 0o644).unwrap();
        let node = c.lookup("/f").unwrap();
        let wb = c.wb().unwrap();
        *wb.sync_probe.lock().unwrap() = Some(SyncProbe {
            fail_on: vec!["dir-sync"],
            ..SyncProbe::default()
        });
        let e = write_deferred_for_test(&c, &node, b"r15", 3).unwrap_err();
        assert!(format!("{e}").contains("dir-sync"), "{e}");
        assert_eq!(p0d_log(wb), vec!["file-sync", "dir-sync"]);
        assert_eq!(wb.queue_counts(), (0, 0, 0), "enqueue must not have run");
        assert_eq!(wb.reconcile_capacity_and_report().unwrap(), (0, 0));
    }

    /// R15 + task §2: a failed enqueue (SQLite commit never happened) must
    /// not acknowledge, and the now-ownerless staging bytes are dropped
    /// with their reservation — the same recovery the crash path gets.
    #[test]
    fn p0d_r15_enqueue_failure_drops_unenqueued_staging() {
        let c = wb_core("r15enqueue");
        c.create("/f", 0o644).unwrap();
        let node = c.lookup("/f").unwrap();
        let wb = c.wb().unwrap();
        *wb.sync_probe.lock().unwrap() = Some(SyncProbe {
            fail_on: vec!["enqueue"],
            ..SyncProbe::default()
        });
        let e = write_deferred_for_test(&c, &node, b"r15", 3).unwrap_err();
        assert!(format!("{e}").contains("enqueue"), "{e}");
        assert_eq!(p0d_log(wb), vec!["file-sync", "dir-sync", "enqueue"]);
        assert_eq!(wb.queue_counts(), (0, 0, 0));
        assert_eq!(wb.reconcile_capacity_and_report().unwrap(), (0, 0));
    }

    /// R17: classification keys off the error ORIGIN (transport shape /
    /// HTTP status / unrecognizable), and the admission table is exactly
    /// the contract — conflicts and lost staging never retry, permanent
    /// verdicts are admitted only for an operator who fixes the cause,
    /// unjudgeable errors are never retryable.
    #[test]
    fn p0d_r17_error_class_origin_and_admission_table() {
        use crate::error::Error;
        assert_eq!(
            classify_object_upload_failure(&Error::Storage(
                "s3 transport: connection reset by peer".into()
            )),
            CLASS_UPLOAD_TRANSIENT
        );
        assert_eq!(
            classify_object_upload_failure(&Error::Storage(
                "s3 503 on objects/x/k: SlowDown".into()
            )),
            CLASS_UPLOAD_TRANSIENT
        );
        assert_eq!(
            classify_object_upload_failure(&Error::Storage(
                "s3 429 on objects/x/k: TooManyRequests".into()
            )),
            CLASS_UPLOAD_TRANSIENT
        );
        assert_eq!(
            classify_object_upload_failure(&Error::Storage(
                "s3 408 on objects/x/k: RequestTimeout".into()
            )),
            CLASS_UPLOAD_TRANSIENT
        );
        assert_eq!(
            classify_object_upload_failure(&Error::Storage(
                "s3 403 on objects/x/k: AccessDenied".into()
            )),
            CLASS_UPLOAD_PERMANENT
        );
        assert_eq!(
            classify_object_upload_failure(&Error::Storage(
                "s3 400 on objects/x/k: MalformedXML".into()
            )),
            CLASS_UPLOAD_PERMANENT
        );
        assert_eq!(
            classify_object_upload_failure(&Error::Storage("part missing ETag".into())),
            CLASS_UNKNOWN,
            "an unrecognizable upload outcome is never guessed into a retryable class"
        );
        assert!(!retry_admissible(CLASS_CONFLICT));
        assert!(!retry_admissible(CLASS_STAGING_LOST));
        assert!(!retry_admissible(CLASS_UNKNOWN));
        assert!(retry_admissible(CLASS_UPLOAD_TRANSIENT));
        assert!(retry_admissible(CLASS_UPLOAD_PERMANENT));
    }

    /// Object store failing put_stream ONCE with a caller-chosen error
    /// shape (per-file drain path, the one production drain_task uses).
    struct P0dFailOnce {
        inner: MemObjectStore,
        armed: std::sync::atomic::AtomicBool,
        msg: &'static str,
    }
    impl P0dFailOnce {
        fn new(msg: &'static str) -> Self {
            Self {
                inner: MemObjectStore::new(),
                armed: std::sync::atomic::AtomicBool::new(true),
                msg,
            }
        }
    }
    impl crate::object_store::ObjectStore for P0dFailOnce {
        fn put(
            &self,
            key: &str,
            data: &[u8],
        ) -> crate::error::Result<crate::object_store::ObjectInfo> {
            self.inner.put(key, data)
        }
        fn put_stream(
            &self,
            key: &str,
            reader: &mut dyn std::io::Read,
            len: u64,
        ) -> crate::error::Result<crate::object_store::ObjectInfo> {
            if self.armed.swap(false, std::sync::atomic::Ordering::Relaxed) {
                return Err(crate::error::Error::Storage(self.msg.into()));
            }
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

    /// Core over a custom object store, manual per-file drain.
    fn p0d_core_with_store(tag: &str, store: StdArc<dyn crate::object_store::ObjectStore>) -> Core {
        let dir = std::env::temp_dir().join(format!(
            "lightfs-p0d-{tag}-{}-{}",
            std::process::id(),
            tmp_unique()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Core::with_options(
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
                staging_budget_bytes: 0,

                ledger_lock_path: None,
            }),
        )
        .unwrap()
    }

    /// R17: a 503-shaped upload failure is transient — the task requeues
    /// with `upload-transient` persisted, and the next drain succeeds.
    #[test]
    fn p0d_r17_upload_transient_requeues_with_class_then_recovers() {
        let c = p0d_core_with_store(
            "r17t",
            StdArc::new(P0dFailOnce::new("s3 503 on objects/x/k: SlowDown")),
        );
        c.create("/f", 0o644).unwrap();
        write_via_rpc(&c, "/f", b"transient body", Some(0));
        assert!(
            c.wb().unwrap().drain_once().unwrap(),
            "first drain requeues"
        );
        assert_eq!(
            c.wb().unwrap().queue_counts(),
            (1, 0, 0),
            "back in the queue"
        );
        let rows = c.metadata().wb_task_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "queued");
        assert_eq!(
            rows[0].3.as_deref(),
            Some(CLASS_UPLOAD_TRANSIENT),
            "the class is persisted while the task waits"
        );
        assert!(
            c.wb().unwrap().drain_once().unwrap(),
            "second drain succeeds"
        );
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 0));
        let after = c.lookup("/f").unwrap();
        assert_eq!(
            c.objects()
                .get(after.object_key.as_deref().unwrap(), None)
                .unwrap()
                .data,
            b"transient body"
        );
    }

    /// R17: a 403-shaped upload failure is a permanent remote verdict —
    /// dead-lettered IMMEDIATELY (no auto requeue) with the class
    /// persisted and an explicit fix-the-cause reminder; the staged bytes
    /// stay accounted per P0-c dead-byte rules, and the class IS
    /// operator-retryable (admission true) for the future P2 command.
    #[test]
    fn p0d_r17_upload_permanent_dead_letters_immediately_with_class() {
        let c = p0d_core_with_store(
            "r17p",
            StdArc::new(P0dFailOnce::new("s3 403 on objects/x/k: AccessDenied")),
        );
        c.create("/f", 0o644).unwrap();
        write_via_rpc(&c, "/f", b"permanent body", Some(0));
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(
            c.wb().unwrap().queue_counts(),
            (0, 0, 1),
            "one drain is enough: dead, never requeued"
        );
        let rows = c.metadata().wb_task_rows().unwrap();
        assert_eq!(rows[0].1, "dead");
        assert_eq!(rows[0].2, 0, "no retry attempt was spent");
        assert_eq!(rows[0].3.as_deref(), Some(CLASS_UPLOAD_PERMANENT));
        assert!(retry_admissible(CLASS_UPLOAD_PERMANENT));
        assert_eq!(
            c.wb().unwrap().reconcile_capacity_and_report().unwrap(),
            (14, 0),
            "dead bytes stay staged/accounted until physically dropped"
        );
    }

    /// R17: an unrecognizable upload outcome is `unknown` — dead-lettered
    /// immediately and never admissible, so it can never masquerade as a
    /// retryable failure.
    #[test]
    fn p0d_r17_unknown_error_never_requeues() {
        let c = p0d_core_with_store("r17u", StdArc::new(P0dFailOnce::new("part missing ETag")));
        c.create("/f", 0o644).unwrap();
        write_via_rpc(&c, "/f", b"unknown body", Some(0));
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 1));
        let rows = c.metadata().wb_task_rows().unwrap();
        assert_eq!(rows[0].1, "dead");
        assert_eq!(rows[0].3.as_deref(), Some(CLASS_UNKNOWN));
        assert!(!retry_admissible(CLASS_UNKNOWN));
    }

    /// R17: staging content missing at drain time is `staging-lost` —
    /// dead without requeue (there is nothing to upload), class persisted.
    #[test]
    fn p0d_r17_staging_lost_dead_letters_without_requeue() {
        let c = wb_core("r17lost");
        c.create("/f", 0o644).unwrap();
        write_via_rpc(&c, "/f", b"lost body", Some(0));
        // the staging file vanishes before the drain (data-loss scenario)
        let (staging, _) = c
            .metadata()
            .wb_latest_staging(c.lookup("/f").unwrap().id)
            .unwrap()
            .unwrap();
        std::fs::remove_file(&staging).unwrap();
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 1));
        let rows = c.metadata().wb_task_rows().unwrap();
        assert_eq!(rows[0].1, "dead");
        assert_eq!(rows[0].3.as_deref(), Some(CLASS_STAGING_LOST));
        assert!(!retry_admissible(CLASS_STAGING_LOST));
    }

    /// R17: a CAS conflict dead-letters with the `conflict` class (the
    /// winner always stands); the already-committed guard is untouched and
    /// still rescues a crash between CAS and finish.
    #[test]
    fn p0d_r17_conflict_dead_letters_with_class() {
        let c = wb_core("r17conf");
        c.create("/f", 0o644).unwrap();
        let _ = write_via_rpc(&c, "/f", b"client A staged", Some(0));
        {
            let eng = c.wb().unwrap();
            let node = c.lookup("/f").unwrap();
            eng.write_deferred(
                &node,
                0,
                &mut std::io::Cursor::new(b"client B staged".to_vec()),
                15,
            )
            .unwrap();
        }
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert!(c.wb().unwrap().drain_once().unwrap());
        assert_eq!(c.wb().unwrap().queue_counts(), (0, 0, 1));
        let rows = c.metadata().wb_task_rows().unwrap();
        assert_eq!(rows[0].1, "dead");
        assert_eq!(rows[0].3.as_deref(), Some(CLASS_CONFLICT));
        assert!(!retry_admissible(CLASS_CONFLICT));
        let after = c.lookup("/f").unwrap();
        assert_eq!(
            c.objects()
                .get(after.object_key.as_deref().unwrap(), None)
                .unwrap()
                .data,
            b"client A staged",
            "the winner is never corrupted"
        );
    }

    /// R17 (recover arm): a task whose staging file vanished by restart
    /// time dead-letters with `staging-lost` — visible, never silent, and
    /// never re-enqueued.
    #[test]
    fn p0d_r17_recover_marks_lost_staging_with_class() {
        let c = wb_core("r17rec");
        c.create("/f", 0o644).unwrap();
        write_via_rpc(&c, "/f", b"vanishing body", Some(0));
        let (staging, _) = c
            .metadata()
            .wb_latest_staging(c.lookup("/f").unwrap().id)
            .unwrap()
            .unwrap();
        std::fs::remove_file(&staging).unwrap();
        c.wb().unwrap().recover().unwrap();
        let rows = c.metadata().wb_task_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, "dead");
        assert_eq!(rows[0].3.as_deref(), Some(CLASS_STAGING_LOST));
    }

    /// R16 child: opens the PERSISTENT engine (file-backed SQLite +
    /// staging dir), writes one deferred task to a successful enqueue,
    /// signals readiness, and parks holding the engine until SIGKILLed.
    /// No-op without the env var (plain suite sweep).
    #[test]
    fn p0d_r16_child_helper() {
        let Some(dir) = std::env::var_os("LIGHTFS_P0D_CHILD_DIR") else {
            return;
        };
        let dir = PathBuf::from(dir);
        // The long batch seal window pins the task in non-terminal state
        // (claimed, waiting for the seal) so the parent has a deterministic
        // crash window instead of racing a millisecond per-file drain.
        let mut cfg = p0d_cfg(&dir.join("meta.db"), &dir.join("staging"));
        cfg.writeback.batch_pack_bytes = 8388608;
        cfg.writeback.batch_max_wait_ms = 600_000;
        let eng = crate::engine::open_embedded_mem(&cfg).expect("child engine");
        let c = &eng.core;
        c.create("/f", 0o644).unwrap();
        write_via_rpc(c, "/f", P0D_R16_BODY, Some(0));
        assert_eq!(c.wb().unwrap().queue_counts(), (1, 0, 0));
        std::fs::write(dir.join("child.ready"), b"enqueued").unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// R16: a REAL process crash after a successful enqueue. The child
    /// commits the task and is SIGKILLed; the parent reopens the same
    /// database + staging dir and must find the staged bytes byte-exact,
    /// the capacity ledger equal to the real files, and the task draining
    /// to a full commit. This proves G1 (process crash recovery). It does
    /// NOT prove G2 (host power loss): process death preserves the OS page
    /// cache, so dir-fsync/synchronous=FULL margins are not exercised here
    /// — no power-loss claim is made.
    #[test]
    fn p0d_r16_process_crash_recovery_preserves_task_and_bytes() {
        let dir = p0c2_dir("r16crash");
        let ready = dir.join("child.ready");
        let exe = std::env::current_exe().unwrap();
        let child = std::process::Command::new(&exe)
            .args(["p0d_r16_child_helper", "--nocapture"])
            .env("LIGHTFS_P0D_CHILD_DIR", &dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        struct Guard(std::process::Child);
        impl Drop for Guard {
            fn drop(&mut self) {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
        let mut guard = Guard(child);
        let mut waited = 0u32;
        while !ready.exists() && waited < 10_000 {
            std::thread::sleep(Duration::from_millis(20));
            waited += 20;
        }
        assert!(ready.exists(), "child never reached the enqueue");
        // Deterministic crash point: poll the database READ-ONLY (WAL
        // allows concurrent readers; the child keeps running) until the
        // task is durably enqueued, then SIGKILL. The child's long batch
        // seal window pins the task in non-terminal state so the crash
        // cannot race a millisecond drain.
        let db = rusqlite::Connection::open_with_flags(
            dir.join("meta.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let mut tasks = 0i64;
        let poll_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while tasks == 0 {
            assert!(
                std::time::Instant::now() < poll_deadline,
                "the enqueue never became visible"
            );
            tasks = db
                .query_row(
                    "SELECT count(*) FROM wb_tasks WHERE state IN ('queued','draining')",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
        // SIGKILL: the engine dies holding its locks; the kernel releases.
        let _ = guard.0.kill();
        let _ = guard.0.wait();
        drop(db);
        // 1) BEFORE any engine lives in this process again: the staged
        // bytes are byte-exact on disk — no worker can have touched them,
        // the child is dead, so this is the crash instant's truth.
        let staging_dir = dir.join("staging");
        let mut staged = Vec::new();
        for e in std::fs::read_dir(&staging_dir).unwrap().flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            if name.starts_with("wb-") && name.ends_with(".data") {
                staged.push(e.path());
            }
        }
        assert_eq!(staged.len(), 1, "exactly one staged file after the crash");
        assert_eq!(
            std::fs::read(&staged[0]).unwrap(),
            P0D_R16_BODY,
            "staged bytes must be byte-exact after SIGKILL"
        );
        // 2) reopen the engine (bounded retry: the killed holder's lock
        // release can stay invisible for a few tens of microseconds — see
        // S4.3.4b.2): recover() enrolls the task; the single drain worker
        // settles it to a full commit. Poll the SETTLED state.
        let cfg = p0d_cfg(&dir.join("meta.db"), &staging_dir);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let eng = loop {
            match crate::engine::open_embedded_mem(&cfg) {
                Ok(eng) => break eng,
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        panic!("engine not reacquired after holder death: {e}");
                    }
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        };
        let c = &eng.core;
        let settle = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if c.wb().unwrap().queue_counts() == (0, 0, 0) {
                break;
            }
            assert!(
                std::time::Instant::now() < settle,
                "the recovered task must drain on its own: {:?}",
                c.wb().unwrap().queue_counts()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        // 3) the acked write became a full commit with the exact bytes
        let after = c.lookup("/f").unwrap();
        assert_eq!(after.version, 1);
        assert_eq!(
            c.objects()
                .get(after.object_key.as_deref().unwrap(), None)
                .unwrap()
                .data,
            P0D_R16_BODY,
            "the bytes that survived the crash are the bytes that commit"
        );
        // 4) the ledger converged with reality
        assert_eq!(
            c.wb().unwrap().reconcile_capacity_and_report().unwrap(),
            (0, 0),
            "staged bytes fully drained; no phantom reservation"
        );
        // stop the drain worker so the engine's locks release before the
        // directory removal (workers keep the engine Arc alive otherwise)
        eng.core.wb().unwrap().stop();
        drop(eng);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R16 payload (byte-exact assertion target).
    const P0D_R16_BODY: &[u8] = b"process-crash body 0123456789";

    /// R16 config: file-backed SQLite + its own staging dir, manual drain.
    fn p0d_cfg(db: &Path, staging: &Path) -> crate::config::Config {
        let mut cfg = crate::config::Config::load_or_defaults(None).unwrap();
        cfg.metadata.path = db.to_string_lossy().to_string();
        cfg.commit.mode = "writeback".into();
        cfg.writeback.staging_dir = staging.to_string_lossy().to_string();
        // 1 worker: open_embedded refuses 0 (an embedded engine must
        // drain). The parent polls the SETTLED state instead of racing
        // intermediate queue shapes.
        cfg.writeback.drain_workers = 1;
        cfg.writeback.batch_pack_bytes = 0;
        cfg.writeback.staging_budget_bytes = 0;
        cfg
    }
}
