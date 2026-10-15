//! P0-b backup protection: canonical root, atomic publication, versioned
//! manifests, and the backup/gc/prune maintenance lock.
//!
//! Contract (design §3.E.2, frozen in S4.1.1):
//!
//! - `[backup] root` is the ONLY supported backup location. `lightfs backup`
//!   without it errors; `gc` without it refuses to delete objects. No
//!   legacy single-file fallback, no scanning arbitrary directories.
//! - Every backup is a subdirectory `<root>/<id>/` containing `meta.db`
//!   (VACUUM INTO of the live database), `meta.db.manifest` (the object-key
//!   set, exported FROM THE SNAPSHOT FILE ITSELF so manifest and snapshot
//!   are the same logical instant), and `backup.json` (operator metadata +
//!   the manifest's sha256 as an integrity anchor).
//! - Publication is atomic: everything is generated in `<root>/.tmp-<id>/`,
//!   fsynced, then renamed. A `.tmp-` leftover never counts as a backup (it
//!   is cleaned by the next backup run under the lock) and never protects.
//! - `gc.lock` inside the root is an OS-maintained exclusive file lock
//!   (flock): backup holds it from before the snapshot until after the
//!   rename; gc holds it from manifest verification until deletion
//!   finalization; prune holds it while removing a directory. The kernel
//!   releases it when a process dies — a crashed holder cannot wedge the
//!   system. Waiters block up to `[backup] lock_timeout_secs`, then error.
//! - fail-closed: ANY published backup directory with a missing, corrupt or
//!   unknown-version manifest stops ALL object deletion, naming the backup
//!   and the problem. The only resolution is an explicit operator action
//!   (`backup prune --id X`, which itself takes the lock).
//!
//! Manifest object-key scope (restore semantics, audited in S4.3): the
//! objects a restored snapshot needs to serve reads and finish recovery —
//! `nodes.object_key` (live content, incl. pack-backed keys) ∪ `packs.key`
//! (conservative: unreferenced packs are tiny over-protection) ∪
//! `superseded_objects.object_key` (the retention capability the restored
//! system itself promises). Deliberately NOT protected: `wb_tasks.
//! uploaded_key` (a restored task's staging file is gone; recovery marks it
//! dead and the uploaded bytes are orphan candidates — the restored system
//! never reads them) and `gc_deletions.key` (deletion intents: after a
//! restore the executor converges them idempotently — a missing object
//! completes the task; protecting them would stall gc progress while any
//! backup exists).

use std::collections::BTreeSet;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::Config;
use crate::error::{Error, Result};

pub const LOCK_NAME: &str = "gc.lock";
pub const MANIFEST_NAME: &str = "meta.db.manifest";
pub const META_NAME: &str = "backup.json";
pub const TMP_PREFIX: &str = ".tmp-";
/// The only manifest version this build reads. Anything else in a published
/// backup directory is fail-closed corruption for gc.
pub const MANIFEST_VERSION: u64 = 1;

// ---------------------------------------------------------------------------
// Maintenance lock
// ---------------------------------------------------------------------------

/// Exclusive inter-process maintenance lock over the canonical backup root.
/// Held by `backup` (snapshot → publish), `gc` (manifest verify → deletion
/// finalize) and `backup prune` (directory removal). The lock is an flock on
/// `<root>/gc.lock`: the kernel drops it when the holder dies, and waiters
/// block up to a deadline and then ERROR (never silently skip).
#[derive(Debug)]
pub struct MaintenanceLock {
    _file: std::fs::File,
}

impl MaintenanceLock {
    /// Block until the lock is acquired or `timeout` elapses. The lock file
    /// itself is a stable path inside the root — locking is provided by the
    /// OS, not by create-new marker semantics.
    pub fn acquire(root: &Path, timeout_secs: u64) -> Result<Self> {
        std::fs::create_dir_all(root).map_err(|e| {
            Error::Config(format!("cannot create backup root {}: {e}", root.display()))
        })?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .read(true)
            .open(root.join(LOCK_NAME))
            .map_err(|e| Error::Config(format!("cannot open maintenance lock: {e}")))?;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
        loop {
            if crate::fslock::try_lock_raw(&file) {
                return Ok(Self { _file: file });
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::Config(format!(
                    "maintenance lock {} busy: another backup/gc/prune holds it \
                     (waited {timeout_secs}s); refusing to proceed",
                    root.join(LOCK_NAME).display()
                )));
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
}

// ---------------------------------------------------------------------------
// Manifest model
// ---------------------------------------------------------------------------

/// Versioned protection manifest. `objects` is the restore-required key set
/// (see module docs); `objects_sha256` protects the list against silent
/// truncation/editing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    pub created_at: i64,
    /// None = protected until explicitly pruned. NEVER derived from object
    /// retention — backup lifetime is an independent, explicit decision.
    #[serde(default)]
    pub expires_at: Option<i64>,
    pub objects: Vec<String>,
    pub objects_sha256: String,
}

/// Operator-facing metadata. `manifest_sha256` pins the manifest bytes: gc
/// verifies the anchor and treats any mismatch as corruption.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupMeta {
    pub backup_id: String,
    pub created_at: i64,
    pub expires_at: Option<i64>,
    pub manifest_sha256: String,
}

/// A verified published backup. Expired backups are still verified (fail
/// closed regardless) but contribute no protection after their expiry.
#[derive(Debug, Clone)]
pub struct VerifiedBackup {
    pub id: String,
    pub dir: PathBuf,
    pub expires_at: Option<i64>,
    pub keys: std::collections::HashSet<String>,
}

pub(crate) fn objects_sha256(objects: &[String]) -> String {
    let mut h = Sha256::new();
    for k in objects {
        h.update(k.as_bytes());
        h.update(b"\n");
    }
    hex(&h.finalize())
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn fsync_dir(p: &Path) -> Result<()> {
    let f =
        std::fs::File::open(p).map_err(|e| Error::Storage(format!("open {}: {e}", p.display())))?;
    f.sync_all()
        .map_err(|e| Error::Storage(format!("fsync {}: {e}", p.display())))
}

pub(crate) fn sqlite_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

// ---------------------------------------------------------------------------
// backup
// ---------------------------------------------------------------------------

/// `lightfs backup`: atomically publish one backup directory under the
/// canonical root. Returns the published directory. The maintenance lock is
/// held from before the snapshot until after the rename.
pub fn backup_now(cfg: &Config, expires_in: Option<u64>) -> Result<PathBuf> {
    let Some(root) = cfg.backup.root.as_deref() else {
        return Err(Error::Config(
            "[backup] root is not configured: there is no supported backup \
             location. Configure [backup] root in the config file; the legacy \
             single-file backup is not supported anymore."
                .into(),
        ));
    };
    let root_path = PathBuf::from(root);
    let _lock = MaintenanceLock::acquire(&root_path, cfg.backup.lock_timeout_secs)?;
    let now = now_secs();

    // Leftovers from a crash before publication are not backups and never
    // protected; under the lock it is safe to clean them.
    for entry in std::fs::read_dir(&root_path)
        .map_err(|e| Error::Config(format!("cannot read backup root: {e}")))?
    {
        let entry = entry.map_err(|e| Error::Config(format!("cannot read backup root: {e}")))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with(TMP_PREFIX) {
            std::fs::remove_dir_all(entry.path())
                .map_err(|e| Error::Config(format!("cannot clean stale {}: {e}", name)))?;
        }
    }

    let id = format!(
        "{:x}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        std::process::id()
    );
    let tmp = root_path.join(format!("{TMP_PREFIX}{id}"));
    std::fs::create_dir_all(&tmp)
        .map_err(|e| Error::Config(format!("cannot create {}: {e}", tmp.display())))?;

    // 1. Snapshot the metadata database.
    let live = rusqlite::Connection::open(&cfg.metadata.path)
        .map_err(|e| Error::Config(format!("cannot open metadata {}: {e}", cfg.metadata.path)))?;
    live.busy_timeout(std::time::Duration::from_secs(30)).ok();
    let snap_path = tmp.join("meta.db");
    live.execute_batch(&format!(
        "VACUUM INTO {}",
        sqlite_quote(&snap_path.to_string_lossy())
    ))
    .map_err(|e| Error::Config(format!("VACUUM INTO failed: {e}")))?;
    drop(live);

    // 2. Export the object-key set FROM THE SNAPSHOT ITSELF — one logical
    //    instant, no live-database race.
    let snap = rusqlite::Connection::open_with_flags(
        &snap_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .map_err(|e| Error::Config(format!("cannot open snapshot: {e}")))?;
    let mut objects: BTreeSet<String> = BTreeSet::new();
    let mut load = |sql: &str| -> Result<()> {
        let mut stmt = snap
            .prepare(sql)
            .map_err(|e| Error::Config(format!("manifest query failed: {e}")))?;
        let rows = stmt
            .query_map([], |r| r.get::<_, Option<String>>(0))
            .map_err(|e| Error::Config(format!("manifest query failed: {e}")))?;
        for row in rows {
            // Fail-closed: a row we cannot read must abort the backup,
            // never silently shrink the protected set.
            let k = row.map_err(|e| Error::Config(format!("manifest query failed: {e}")))?;
            if let Some(k) = k {
                objects.insert(k);
            }
        }
        Ok(())
    };
    load("SELECT object_key FROM nodes WHERE object_key IS NOT NULL")?;
    load("SELECT key FROM packs")?;
    load("SELECT object_key FROM superseded_objects")?;
    drop(snap);

    let objects: Vec<String> = objects.into_iter().collect();
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        created_at: now,
        expires_at: expires_in.map(|s| now + s as i64),
        objects_sha256: objects_sha256(&objects),
        objects,
    };
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)
        .map_err(|e| Error::Config(format!("manifest serialize: {e}")))?;
    let meta = BackupMeta {
        backup_id: id.clone(),
        created_at: now,
        expires_at: manifest.expires_at,
        manifest_sha256: hex(&Sha256::digest(&manifest_bytes)),
    };
    let meta_bytes = serde_json::to_vec_pretty(&meta)
        .map_err(|e| Error::Config(format!("backup meta serialize: {e}")))?;

    // 3. Self-verify BEFORE publishing: a backup that would fail gc's
    //    verification must never be published.
    verify_manifest_bytes(&manifest_bytes, &meta_bytes)?;

    // 4. Write, fsync files, fsync the tmp dir.
    for (name, bytes) in [(MANIFEST_NAME, &manifest_bytes), (META_NAME, &meta_bytes)] {
        let mut f = std::fs::File::create(tmp.join(name))
            .map_err(|e| Error::Config(format!("write {name}: {e}")))?;
        f.write_all(bytes)
            .map_err(|e| Error::Config(format!("write {name}: {e}")))?;
        f.sync_all()
            .map_err(|e| Error::Config(format!("fsync {name}: {e}")))?;
    }
    fsync_dir(&tmp)?;

    // 5. Atomic same-filesystem publish, then fsync the root entry.
    let published = root_path.join(&id);
    std::fs::rename(&tmp, &published)
        .map_err(|e| Error::Config(format!("publish {}: {e}", published.display())))?;
    fsync_dir(&root_path)?;

    println!(
        "backup published: {} ({} object keys, expires_at = {})",
        published.display(),
        manifest.objects.len(),
        manifest
            .expires_at
            .map(|t| t.to_string())
            .unwrap_or_else(|| "never (protected until pruned)".into())
    );
    Ok(published)
}

/// Full gc-side verification of one published backup's files. Any problem
/// is a hard error naming the backup — callers must fail closed.
fn verify_manifest_bytes(manifest_bytes: &[u8], meta_bytes: &[u8]) -> Result<Manifest> {
    let m: Manifest = serde_json::from_slice(manifest_bytes)
        .map_err(|e| Error::Config(format!("corrupt manifest: {e}")))?;
    if m.version != MANIFEST_VERSION {
        return Err(Error::Config(format!(
            "unknown manifest version {} (this build reads v{MANIFEST_VERSION}); \
             refusing to guess the protection set",
            m.version
        )));
    }
    if m.objects_sha256 != objects_sha256(&m.objects) {
        return Err(Error::Config(
            "manifest integrity check failed: objects_sha256 does not match the object list".into(),
        ));
    }
    let meta: BackupMeta = serde_json::from_slice(meta_bytes)
        .map_err(|e| Error::Config(format!("corrupt backup.json: {e}")))?;
    if meta.manifest_sha256 != hex(&Sha256::digest(manifest_bytes)) {
        return Err(Error::Config(
            "backup.json manifest_sha256 does not match meta.db.manifest: the backup is \
             incomplete or was tampered with"
                .into(),
        ));
    }
    Ok(m)
}

/// Verify EVERY published backup under the root. `.tmp-*` entries are not
/// backups (unpublished, unprotected) and `gc.lock` is the lock file; both
/// are skipped. Anything else in the root that is not a fully verifiable
/// backup directory is a hard error — gc must not guess.
pub fn scan_backups(root: &str) -> Result<Vec<VerifiedBackup>> {
    let root_path = PathBuf::from(root);
    let mut out = Vec::new();
    let entries = std::fs::read_dir(&root_path)
        .map_err(|e| Error::Config(format!("cannot read backup root {root}: {e}")))?;
    for entry in entries {
        let entry = entry.map_err(|e| Error::Config(format!("cannot read backup root: {e}")))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == LOCK_NAME || name.starts_with(TMP_PREFIX) {
            continue;
        }
        if name.starts_with(PRUNE_PENDING_PREFIX) {
            // An unfinished prune: the backup inside is either not yet
            // durably renamed or explicitly retired with an unconfirmed
            // cleanup. NEVER a valid backup, NEVER ignorable — gc must
            // stop until the prune is finished or retried.
            let orig = name.strip_prefix(PRUNE_PENDING_PREFIX).unwrap_or(&name);
            return Err(Error::Config(format!(
                "{name}: unfinished backup prune — gc fail-closed; finish it with \
                 `lightfs backup prune --id {orig}` (retry converges; the pending \
                 directory is never re-validated as a backup)"
            )));
        }
        let bad = |why: String| {
            Error::Config(format!(
                "backup {name}: {why} — gc fail-closed; \
            resolve explicitly with `lightfs backup prune --id {name}` if it is really dispensable"
            ))
        };
        if !entry
            .file_type()
            .map_err(|e| bad(format!("stat: {e}")))?
            .is_dir()
        {
            return Err(bad(
                "unexpected non-directory entry in the canonical root (legacy single-file \
                 backups are NOT supported and are not auto-protected)"
                    .into(),
            ));
        }
        let dir = entry.path();
        let manifest_bytes = std::fs::read(dir.join(MANIFEST_NAME))
            .map_err(|e| bad(format!("unreadable {MANIFEST_NAME} ({e})")))?;
        let meta_bytes = std::fs::read(dir.join(META_NAME))
            .map_err(|e| bad(format!("unreadable {META_NAME} ({e})")))?;
        let m = verify_manifest_bytes(&manifest_bytes, &meta_bytes)
            .map_err(|e| Error::Config(format!("backup {name}: {} — gc fail-closed", e)))?;
        out.push(VerifiedBackup {
            id: name,
            dir,
            expires_at: m.expires_at,
            keys: m.objects.into_iter().collect(),
        });
    }
    Ok(out)
}

/// The union of object keys protected by all VERIFIED, UNEXPIRED backups.
/// Expired backups keep being verified (their directories stay diagnosable)
/// but no longer protect objects.
pub fn protected_keys(root: &str, now: i64) -> Result<std::collections::HashSet<String>> {
    let mut keys = std::collections::HashSet::new();
    for b in scan_backups(root)? {
        if b.expires_at.is_none_or(|t| t > now) {
            keys.extend(b.keys);
        }
    }
    Ok(keys)
}

/// Directory name prefix of a prune that has not finished its cleanup. A
/// pending entry is a FAIL-CLOSED condition for `scan_backups` (unlike
/// `.tmp-*`): the backup it belongs to is either still fully present inside
/// it (rename not yet confirmed durable) or has been explicitly retired
/// with only the directory-entry removal unconfirmed — in both cases gc
/// must not run until a human finishes or retries the prune.
pub const PRUNE_PENDING_PREFIX: &str = ".prune-pending-";

/// `lightfs backup prune --id ID`: retire one backup under the exclusive
/// maintenance lock, with an explicit PERSISTENCE COMMIT POINT. Order:
///
///   1. rename the backup dir to `.prune-pending-<id>` (atomic; takes the
///      backup out of the supported namespace in the current view);
///   2. fsync the canonical root — CONFIRM step 1 persisted. This is the
///      commit point: only after it may the prune be considered done;
///   3. remove the pending dir, then fsync the root again (failure here is
///      reported honestly but the retirement stands — a resurrected pending
///      entry after power loss keeps gc fail-closed, never re-validating
///      the backup).
///
/// Any failure before the commit point leaves the pending dir (or the
/// original dir) in place and returns an error; gc stays fail-closed while
/// a pending entry exists. Retrying `prune --id <id>` finishes the cleanup
/// and converges.
pub fn backup_prune(cfg: &Config, id: &str) -> Result<()> {
    backup_prune_impl(cfg, id, &mut |p| fsync_dir(p), &mut |p| {
        std::fs::remove_dir_all(p)
            .map_err(|e| Error::Storage(format!("remove {}: {e}", p.display())))
    })
}

/// Test seam: identical to [`backup_prune`] with injectable root-fsync and
/// directory-removal operations (call-order counted in the tests).
pub(crate) fn backup_prune_impl(
    cfg: &Config,
    id: &str,
    fsync_root: &mut dyn FnMut(&Path) -> Result<()>,
    remove_dir: &mut dyn FnMut(&Path) -> Result<()>,
) -> Result<()> {
    let Some(root) = cfg.backup.root.as_deref() else {
        return Err(Error::Config(
            "[backup] root is not configured: there is no supported backup location".into(),
        ));
    };
    if id.starts_with('.') || id == LOCK_NAME || id.contains('/') || id.contains('\\') {
        return Err(Error::Config(format!("invalid backup id {id:?}")));
    }
    let root_path = PathBuf::from(root);
    let _lock = MaintenanceLock::acquire(&root_path, cfg.backup.lock_timeout_secs)?;
    let dir = root_path.join(id);
    let pending = root_path.join(format!("{PRUNE_PENDING_PREFIX}{id}"));

    if pending.exists() {
        // A previous prune of this id did not finish. Whether it reached
        // the commit point is unknowable here — treat the pending dir as
        // retired-but-uncleaned: NEVER re-validate it as a backup, just
        // finish the removal. Its contents stop protecting objects the
        // moment it left the supported namespace (gc was fail-closed the
        // whole time).
        if dir.exists() {
            return Err(Error::Config(format!(
                "backup {id}: both the live directory and a pending prune exist — \
                 inconsistent state, refusing to proceed"
            )));
        }
        // The retry builds its OWN persistence commit point BEFORE any
        // removal: fsync the root so the current state (pending present,
        // live dir absent) is durable first. Removing first would repeat
        // the exact hole S4.3.2 closed on the main path — removal succeeds,
        // the fsync fails, the pending entry vanishes from the current
        // view, gc stops being fail-closed, and the original rename may
        // never have persisted at all.
        fsync_root(&root_path).map_err(|e| {
            Error::Config(format!(
                "pending prune of {id} kept: the pre-removal root fsync failed ({e}); \
                 the pending entry keeps gc fail-closed; retry `backup prune --id {id}`"
            ))
        })?;
        remove_dir(&pending)
            .map_err(|e| Error::Config(format!("cannot remove pending prune of {id}: {e}")))?;
        fsync_root(&root_path).map_err(|e| {
            Error::Config(format!(
                "pending prune of {id} removed and the retirement commit point passed, \
                 but syncing the removal failed ({e}); if a pending entry reappears, \
                 gc stays fail-closed"
            ))
        })?;
        println!("backup pruned (pending cleanup finished): {id}");
        return Ok(());
    }

    if !dir.is_dir() {
        return Err(Error::Config(format!(
            "backup {id} does not exist under {root}"
        )));
    }

    // Commit-point sequence. A failure in either of the first two steps
    // leaves the pending dir behind: gc stays fail-closed until a retry
    // converges — the backup can never silently leave protection.
    std::fs::rename(&dir, &pending)
        .map_err(|e| Error::Config(format!("cannot retire backup {id} (rename): {e}")))?;
    fsync_root(&root_path).map_err(|e| {
        Error::Config(format!(
            "backup {id} renamed to {} but the root fsync failed ({e}); the rename's \
             persistence is UNCONFIRMED — a pending prune blocks gc until this \
             completes; retry `backup prune --id {id}`",
            pending.display()
        ))
    })?;
    // Committed: the backup has left the supported namespace durably.
    remove_dir(&pending)
        .map_err(|e| Error::Config(format!("cannot remove pending prune of {id}: {e}")))?;
    fsync_root(&root_path).map_err(|e| {
        Error::Config(format!(
            "backup {id} is retired (commit point passed), but syncing the pending-dir \
             removal failed ({e}); if a pending entry reappears, gc stays fail-closed"
        ))
    })?;
    println!("backup pruned: {id}");
    Ok(())
}

#[cfg(test)]
mod tests;
