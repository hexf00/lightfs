//! SQLite MetadataStore backend.
//!
//! Root node is
//! fixed at id=1 (parent NULL). All operations take BEGIN IMMEDIATE so a
//! process sharing this store (the single server) sees serialized writes.
//!
//! Stable identity (R4.2c): `nodes.id` is AUTOINCREMENT, so ids a database
//! has already allocated are never re-issued by that same database —
//! (id, version) identifies content for the database's lifetime. The
//! property is created WITH the schema: a database written before this
//! schema cannot satisfy it and is rejected at open (never auto-migrated);
//! re-initialize the store instead.

use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, params};

use crate::error::{Error, Result};
use crate::metadata::{GcDeletion, MetadataStore, Node, NodeKind};

pub struct SqliteMetadataStore {
    conn: Mutex<Connection>,
}

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS nodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    parent_id INTEGER,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    mtime INTEGER NOT NULL,
    mode INTEGER,
    object_key TEXT,
    version TEXT,
    etag TEXT,
    target TEXT,
    pack_key TEXT,
    pack_off INTEGER,
    pack_len INTEGER,
    content_hash TEXT,
    UNIQUE(parent_id, name)
);
CREATE TABLE IF NOT EXISTS superseded_objects (
    object_key TEXT PRIMARY KEY,
    superseded_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS packs (
    key TEXT PRIMARY KEY,
    bytes INTEGER NOT NULL,
    uploaded_at INTEGER,
    state TEXT NOT NULL DEFAULT 'building',
    unref_since INTEGER
);
CREATE TABLE IF NOT EXISTS wb_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id INTEGER NOT NULL,
    staging TEXT NOT NULL,
    size INTEGER NOT NULL,
    expected INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'queued',
    attempts INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    pack_key TEXT,
    uploaded_key TEXT
);
CREATE INDEX IF NOT EXISTS idx_wb_tasks_node ON wb_tasks(node_id, state);
CREATE INDEX IF NOT EXISTS idx_wb_tasks_pack ON wb_tasks(pack_key, state);
CREATE TABLE IF NOT EXISTS gc_deletions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL UNIQUE,
    kind TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at INTEGER NOT NULL
);
"#;

/// S3.1 branch: add the pack columns to a nodes table created by an older
/// build of THIS branch lineage. Pure additive ALTERs (idempotent); the
/// AUTOINCREMENT identity contract check above still runs first and still
/// rejects genuinely legacy databases.
fn add_missing_pack_columns(conn: &Connection) -> rusqlite::Result<()> {
    let mut present: Vec<String> = Vec::new();
    let mut stmt = conn.prepare("SELECT name FROM pragma_table_info('nodes')")?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    for name in rows {
        present.push(name?);
    }
    drop(stmt);
    for (col, ddl) in [
        ("pack_key", "ALTER TABLE nodes ADD COLUMN pack_key TEXT"),
        ("pack_off", "ALTER TABLE nodes ADD COLUMN pack_off INTEGER"),
        ("pack_len", "ALTER TABLE nodes ADD COLUMN pack_len INTEGER"),
        (
            "content_hash",
            "ALTER TABLE nodes ADD COLUMN content_hash TEXT",
        ),
    ] {
        if !present.iter().any(|c| c == col) {
            conn.execute_batch(ddl)?;
        }
    }
    Ok(())
}

/// S4.2 P0-a: add the pack-lifecycle columns to databases created by an
/// older build of THIS branch lineage. Pure additive ALTERs (idempotent).
/// Backfill: an already-uploaded pack without lifecycle state becomes
/// `active` (conservative — the gc re-query converges it); a never-uploaded
/// one stays `building`. Existing task rows carry no pack binding.
fn add_pack_lifecycle_columns(conn: &Connection) -> rusqlite::Result<()> {
    let pack_cols: Vec<String> = {
        let mut stmt = conn.prepare("SELECT name FROM pragma_table_info('packs')")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    let added_state = !pack_cols.iter().any(|c| c == "state");
    if added_state {
        conn.execute_batch("ALTER TABLE packs ADD COLUMN state TEXT NOT NULL DEFAULT 'building'")?;
    }
    if !pack_cols.iter().any(|c| c == "unref_since") {
        conn.execute_batch("ALTER TABLE packs ADD COLUMN unref_since INTEGER")?;
    }
    if added_state {
        // Every pre-lifecycle row predates the new column: classify by what
        // the old schema knew — uploaded packs are conservatively `active`
        // (the gc re-query converges them), never-uploaded stay `building`.
        conn.execute_batch(
            "UPDATE packs SET state = CASE WHEN uploaded_at IS NULL THEN 'building' ELSE 'active' END",
        )?;
    }
    let task_cols: Vec<String> = {
        let mut stmt = conn.prepare("SELECT name FROM pragma_table_info('wb_tasks')")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<_>>()?
    };
    if !task_cols.iter().any(|c| c == "pack_key") {
        conn.execute_batch("ALTER TABLE wb_tasks ADD COLUMN pack_key TEXT")?;
    }
    if !task_cols.iter().any(|c| c == "uploaded_key") {
        conn.execute_batch("ALTER TABLE wb_tasks ADD COLUMN uploaded_key TEXT")?;
    }
    Ok(())
}

/// S4.2 P0-a — the pack reference lifecycle, applied INSIDE the caller's
/// SQLite transaction. Two distinct events, never collapsed:
///
/// - `ref_gone`: a node stopped referencing `old_key`. For a pack key the
///   remaining live reference count is recounted in the same transaction;
///   an `active` pack whose count hit zero transitions to `unreferenced`
///   with `unref_since = now` (retention start). A `committing` pack is
///   NEVER transitioned here even at zero references — the batch finalize
///   decides (invariant: a submitting batch cannot become gc-eligible).
/// - `ref_new`: a node started referencing `new_key`. Legal while the pack
///   is `committing` (the submitting batch) or `active` (batch-terminated;
///   the strict pack engine aggregates further files into the same
///   immutable object). `deleting` — and every other state — is refused,
///   so a pack with a recorded deletion intent can never regain references.
fn pack_ref_gone_tx(tx: &Connection, old_key: Option<&str>, now: i64) -> Result<()> {
    let Some(k) = old_key else {
        return Ok(());
    };
    if !k.starts_with(crate::pack::PACK_KEY_PREFIX) {
        return Ok(());
    }
    let refs: i64 = tx.query_row(
        "SELECT COUNT(*) FROM nodes WHERE object_key = ?1",
        params![k],
        |r| r.get(0),
    )?;
    let state: Option<String> = tx
        .query_row("SELECT state FROM packs WHERE key = ?1", params![k], |r| {
            r.get(0)
        })
        .optional()?;
    match state.as_deref() {
        Some("active") => {
            if refs == 0 {
                tx.execute(
                    "UPDATE packs SET state = 'unreferenced', unref_since = ?2 WHERE key = ?1 AND state = 'active'",
                    params![k, now],
                )?;
            }
        }
        // A reference event on a submitting batch re-runs the (idempotent)
        // termination decision: with non-terminal members it stays
        // `committing`; otherwise it resolves to active/unreferenced here —
        // regardless of the current reference count.
        Some("committing") => finalize_batch_tx(tx, k, now)?,
        _ => {}
    }
    Ok(())
}

fn pack_ref_new_tx(tx: &Connection, new_key: Option<&str>, now: i64) -> Result<()> {
    let Some(k) = new_key else {
        return Ok(());
    };
    if !k.starts_with(crate::pack::PACK_KEY_PREFIX) {
        return Ok(());
    }
    let state: Option<String> = tx
        .query_row("SELECT state FROM packs WHERE key = ?1", params![k], |r| {
            r.get(0)
        })
        .optional()?;
    match state.as_deref() {
        // `committing`: the submitting batch this reference belongs to.
        // `active`: batch-terminated and valid — the strict pack engine
        // legitimately CASes additional files into the same immutable pack
        // inside its aggregation window (schema-exposed correction to the
        // S4.1.1 sketch, which allowed only `committing`).
        Some("committing") => finalize_batch_tx(tx, k, now),
        Some("active") => Ok(()),
        Some(other) => Err(Error::Storage(format!(
            "refusing to reference pack {k} in state '{other}': new references are only legal while the pack is committing or active"
        ))),
        None => Err(Error::Storage(format!(
            "refusing to reference pack {k}: no packs row (registration is the intent-before-object invariant)"
        ))),
    }
}

/// Batch termination decision, executed entirely inside the caller's
/// transaction: only when NO non-terminal wb task still carries this
/// pack_key does the pack's final reference count get verified — refs>0
/// becomes `active`, refs==0 becomes `unreferenced` with `unref_since`.
/// Idempotent: anything not in `committing` is left untouched.
fn finalize_batch_tx(tx: &Connection, pack_key: &str, now: i64) -> Result<()> {
    let pending: i64 = tx.query_row(
        "SELECT COUNT(*) FROM wb_tasks WHERE pack_key = ?1 AND state IN ('queued','draining')",
        params![pack_key],
        |r| r.get(0),
    )?;
    if pending > 0 {
        return Ok(());
    }
    let state: Option<String> = tx
        .query_row(
            "SELECT state FROM packs WHERE key = ?1",
            params![pack_key],
            |r| r.get(0),
        )
        .optional()?;
    if state.as_deref() != Some("committing") {
        return Ok(());
    }
    let refs: i64 = tx.query_row(
        "SELECT COUNT(*) FROM nodes WHERE object_key = ?1",
        params![pack_key],
        |r| r.get(0),
    )?;
    if refs > 0 {
        tx.execute(
            "UPDATE packs SET state = 'active' WHERE key = ?1 AND state = 'committing'",
            params![pack_key],
        )?;
    } else {
        tx.execute(
            "UPDATE packs SET state = 'unreferenced', unref_since = ?2 WHERE key = ?1 AND state = 'committing'",
            params![pack_key, now],
        )?;
    }
    Ok(())
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Map UNIQUE(parent_id, name) violations to AlreadyExists; pass other errors through.
fn map_constraint(e: rusqlite::Error, or: impl FnOnce() -> Error) -> Error {
    match &e {
        rusqlite::Error::SqliteFailure(err, msg)
            if err.code == rusqlite::ErrorCode::ConstraintViolation
                && msg.as_deref().is_some_and(|m| m.contains("UNIQUE")) =>
        {
            or()
        }
        _ => e.into(),
    }
}

fn row_to_node(row: &rusqlite::Row<'_>) -> rusqlite::Result<Node> {
    let kind_s: String = row.get("kind")?;
    Ok(Node {
        id: row.get("id")?,
        parent_id: row.get("parent_id")?,
        name: row.get("name")?,
        kind: NodeKind::parse_kind(&kind_s).unwrap_or(NodeKind::File),
        size: row.get("size")?,
        mtime: row.get("mtime")?,
        mode: row.get::<_, Option<i64>>("mode")?.unwrap_or(0o644) as u32,
        object_key: row.get("object_key")?,
        version: row
            .get::<_, Option<String>>("version")?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0),
        etag: row.get::<_, Option<String>>("etag")?.unwrap_or_default(),
        target: row.get("target")?,
        pack_off: row.get("pack_off")?,
        pack_len: row.get("pack_len")?,
        content_hash: row.get("content_hash")?,
    })
}

const NODE_COLS: &str = "id, parent_id, name, kind, size, mtime, mode, object_key, version, \
                         etag, target, pack_off, pack_len, content_hash";

impl SqliteMetadataStore {
    /// Open (creating if needed) the store and ensure schema + root.
    ///
    /// A database whose `nodes` table predates AUTOINCREMENT cannot satisfy
    /// the stable-identity guarantee and is rejected BEFORE anything is
    /// written (even the journal-mode pragma) — the legacy database is
    /// left untouched: no schema change, no rows, no pragma side effects.
    /// There is deliberately no migration: the store must be re-initialized
    /// (stop the service, unmount all clients, discard the metadata
    /// database and the object namespace). The check keys on the id
    /// column's actual definition (see
    /// [`Self::nodes_table_matches_contract`]), not on the stored SQL
    /// text.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        let nodes_exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'nodes'",
            [],
            |r| r.get(0),
        )?;
        if nodes_exists > 0 && !Self::nodes_table_matches_contract(&conn)? {
            return Err(Error::Storage(
                "nodes table predates AUTOINCREMENT id allocation: stable identity \
                 is not satisfiable on this database. Re-initialize the store \
                 (stop the service, unmount all clients, discard the metadata \
                 database and the test object namespace, then start fresh); \
                 automatic migration is intentionally not supported"
                    .into(),
            ));
        }
        conn.busy_timeout(std::time::Duration::from_secs(10))?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.execute_batch(SCHEMA)?;
        add_missing_pack_columns(&conn)?;
        add_pack_lifecycle_columns(&conn)?;
        // Root: id 1, no parent. Ignore conflict on reopen.
        conn.execute(
            "INSERT OR IGNORE INTO nodes (id, parent_id, name, kind, size, mtime, mode) VALUES (1, NULL, '', 'dir', 0, ?1, ?2)",
            params![now(), 0o755],
        )?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// Definition-level contract check for an existing `nodes` table — it
    /// reads what the ENGINE recorded, never the CREATE statement's text:
    ///
    /// - every required column is present (`pragma_table_info`);
    /// - `sqlite_sequence` holds a row for this table, the engine's own
    ///   bookkeeping for AUTOINCREMENT tables (the row appears on first
    ///   insert; `open` always seeds root id 1, so every database this code
    ///   initialized has one). A legacy table that merely mentions the word
    ///   AUTOINCREMENT anywhere in its SQL text has no such row and must
    ///   not pass. The marker is engine-managed: a hand-forged
    ///   sqlite_sequence row is out-of-model tampering, the same class as
    ///   external tools writing the table directly.
    fn nodes_table_matches_contract(conn: &Connection) -> Result<bool> {
        const REQUIRED: [&str; 11] = [
            "id",
            "parent_id",
            "name",
            "kind",
            "size",
            "mtime",
            "mode",
            "object_key",
            "version",
            "etag",
            "target",
        ];
        let mut present: Vec<String> = Vec::new();
        let mut stmt = conn.prepare("SELECT name FROM pragma_table_info('nodes')")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        for name in rows {
            present.push(name?);
        }
        if REQUIRED.iter().any(|col| !present.iter().any(|c| c == col)) {
            return Ok(false);
        }
        // The sqlite_sequence table itself exists only once some
        // AUTOINCREMENT table exists; a legacy-only database has neither
        // (the failed lookup counts as "no marker").
        let marker: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_sequence WHERE name = 'nodes'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        Ok(marker > 0)
    }

    /// In-memory store for tests.
    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    fn query_one(&self, sql: &str, p: impl rusqlite::Params) -> Result<Option<Node>> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.query_row(sql, p, row_to_node).optional()?)
    }
}

impl MetadataStore for SqliteMetadataStore {
    fn create_node(
        &self,
        parent_id: i64,
        name: &str,
        kind: NodeKind,
        mode: u32,
        object_key: Option<&str>,
    ) -> Result<Node> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let parent_kind: Option<String> = tx
            .query_row(
                "SELECT kind FROM nodes WHERE id = ?1",
                params![parent_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(pk) = parent_kind else {
            return Err(Error::ParentNotFound(format!("id {parent_id}")));
        };
        if pk != "dir" {
            return Err(Error::KindMismatch {
                path: format!("id {parent_id}"),
                expected: "dir",
                actual: pk.clone(),
            });
        }
        tx.execute(
            "INSERT INTO nodes (parent_id, name, kind, size, mtime, mode, object_key, version, etag)
             VALUES (?1, ?2, ?3, 0, ?4, ?5, ?6, '0', '')",
            params![parent_id, name, kind.as_str(), now(), mode as i64, object_key],
        )
        .map_err(|e| map_constraint(e, || Error::AlreadyExists(name.to_string())))?;
        let id = tx.last_insert_rowid();
        // Files are created with object_key = NULL (metadata-only create):
        // the first write commit installs the version key via CAS, and no
        // superseded record or delete ever references a key that never
        // existed. Rename/move never touches bytes either way (AD-010).
        let node = tx.query_row(
            &format!("SELECT {NODE_COLS} FROM nodes WHERE id = ?1"),
            params![id],
            row_to_node,
        )?;
        tx.commit()?;
        Ok(node)
    }

    fn create_symlink(&self, parent_id: i64, name: &str, target: &str, mode: u32) -> Result<Node> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let parent_kind: Option<String> = tx
            .query_row(
                "SELECT kind FROM nodes WHERE id = ?1",
                params![parent_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(pk) = parent_kind else {
            return Err(Error::ParentNotFound(format!("id {parent_id}")));
        };
        if pk != "dir" {
            return Err(Error::KindMismatch {
                path: format!("id {parent_id}"),
                expected: "dir",
                actual: pk.clone(),
            });
        }
        tx.execute(
            "INSERT INTO nodes (parent_id, name, kind, size, mtime, mode, object_key, version, etag, target)
             VALUES (?1, ?2, 'symlink', ?3, ?4, ?5, NULL, '0', '', ?6)",
            params![parent_id, name, target.len() as i64, now(), mode as i64, target],
        )
        .map_err(|e| map_constraint(e, || Error::AlreadyExists(name.to_string())))?;
        let id = tx.last_insert_rowid();
        let node = tx.query_row(
            &format!("SELECT {NODE_COLS} FROM nodes WHERE id = ?1"),
            params![id],
            row_to_node,
        )?;
        tx.commit()?;
        Ok(node)
    }

    fn get_node(&self, id: i64) -> Result<Option<Node>> {
        self.query_one(
            &format!("SELECT {NODE_COLS} FROM nodes WHERE id = ?1"),
            params![id],
        )
    }

    fn get_child(&self, parent_id: i64, name: &str) -> Result<Option<Node>> {
        self.query_one(
            &format!("SELECT {NODE_COLS} FROM nodes WHERE parent_id = ?1 AND name = ?2"),
            params![parent_id, name],
        )
    }

    fn list_children(&self, parent_id: i64) -> Result<Vec<Node>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!(
            "SELECT {NODE_COLS} FROM nodes WHERE parent_id = ?1 ORDER BY name"
        ))?;
        let rows = stmt.query_map(params![parent_id], row_to_node)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn count_children(&self, parent_id: i64) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        Ok(conn.query_row(
            "SELECT COUNT(*) FROM nodes WHERE parent_id = ?1",
            params![parent_id],
            |r| r.get(0),
        )?)
    }

    fn commit_content_cas(
        &self,
        id: i64,
        expected_version: i64,
        size: i64,
        mtime: i64,
        new_object_key: &str,
        etag: &str,
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        // P0-a path A: reference retirement is part of the SAME transaction
        // as the CAS. Single-statement compare-and-swap: the WHERE clause
        // re-checks the version at commit time, so a writer that read the
        // node before another writer committed can never clobber it.
        // rows==0 means the version moved (or the node is gone);
        // distinguish via a lookup. The pack lifecycle update (old key's
        // last reference vanishing) commits atomically with the CAS.
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let old_key: Option<String> = tx
            .query_row(
                "SELECT object_key FROM nodes WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let n = tx.execute(
            "UPDATE nodes
             SET size = ?2, mtime = ?3, version = version + 1, object_key = ?4, etag = ?5
             WHERE id = ?1 AND version = ?6",
            params![
                id,
                size,
                mtime,
                new_object_key,
                etag,
                expected_version.to_string()
            ],
        )?;
        if n == 0 {
            let exists: Option<String> = tx
                .query_row(
                    "SELECT version FROM nodes WHERE id = ?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()?;
            return match exists {
                Some(actual) => {
                    let actual: i64 = actual.parse().unwrap_or(0);
                    Err(Error::Conflict {
                        path: format!("id {id}"),
                        expected: expected_version,
                        actual,
                    })
                }
                None => Err(Error::NotFound(format!("id {id}"))),
            };
        }
        pack_ref_new_tx(&tx, Some(new_object_key), mtime)?;
        pack_ref_gone_tx(&tx, old_key.as_deref(), mtime)?;
        tx.commit()?;
        Ok(())
    }

    fn update_attrs(&self, id: i64, mode: u32, mtime: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE nodes SET mode = ?2, mtime = ?3 WHERE id = ?1",
            params![id, mode as i64, mtime],
        )?;
        if n == 0 {
            return Err(Error::NotFound(format!("id {id}")));
        }
        Ok(())
    }

    fn rename_node(&self, id: i64, new_parent_id: i64, new_name: &str) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let exists: Option<i64> = tx
            .query_row("SELECT id FROM nodes WHERE id = ?1", params![id], |r| {
                r.get(0)
            })
            .optional()?;
        if exists.is_none() {
            return Err(Error::NotFound(format!("id {id}")));
        }
        let dest_parent: Option<String> = tx
            .query_row(
                "SELECT kind FROM nodes WHERE id = ?1",
                params![new_parent_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(pk) = dest_parent else {
            return Err(Error::ParentNotFound(format!("id {new_parent_id}")));
        };
        if pk != "dir" {
            return Err(Error::KindMismatch {
                path: format!("id {new_parent_id}"),
                expected: "dir",
                actual: pk.clone(),
            });
        }
        let dup: Option<i64> = tx
            .query_row(
                "SELECT id FROM nodes WHERE parent_id = ?1 AND name = ?2",
                params![new_parent_id, new_name],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(d) = dup {
            if d != id {
                return Err(Error::AlreadyExists(format!("id {d}")));
            }
        }
        tx.execute(
            "UPDATE nodes SET parent_id = ?2, name = ?3, mtime = ?4 WHERE id = ?1",
            params![id, new_parent_id, new_name, now()],
        )
        .map_err(|e| map_constraint(e, || Error::AlreadyExists(new_name.to_string())))?;
        tx.commit()?;
        Ok(())
    }

    fn rename_node_overwrite(
        &self,
        source_id: i64,
        target_id: i64,
        new_parent_id: i64,
        new_name: &str,
    ) -> Result<Node> {
        let mut conn = self.conn.lock().unwrap();
        if source_id == target_id {
            return Err(Error::InvalidArgument(
                "cannot overwrite a node with itself".into(),
            ));
        }
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        // The target must still live exactly where the caller resolved it;
        // anything else means the namespace drifted, and we must not touch
        // anything.
        let target: Option<Node> = tx
            .query_row(
                &format!(
                    "SELECT {NODE_COLS} FROM nodes WHERE id = ?1 AND parent_id = ?2 AND name = ?3"
                ),
                params![target_id, new_parent_id, new_name],
                row_to_node,
            )
            .optional()?;
        let Some(target) = target else {
            return Err(Error::NotFound(format!("id {target_id}")));
        };
        // The destination parent must still exist and be a directory.
        let dest_parent: Option<String> = tx
            .query_row(
                "SELECT kind FROM nodes WHERE id = ?1",
                params![new_parent_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(pk) = dest_parent else {
            return Err(Error::ParentNotFound(format!("id {new_parent_id}")));
        };
        if pk != "dir" {
            return Err(Error::KindMismatch {
                path: format!("id {new_parent_id}"),
                expected: "dir",
                actual: pk,
            });
        }
        // Free the slot, then move: both statements sit in this one
        // transaction, so the UNIQUE(parent_id, name) window never exists
        // outside it and any failure rolls BOTH halves back.
        // P0-a path C: only the OVERWRITTEN target's reference disappears;
        // the source's object_key moves verbatim and is deliberately NOT
        // re-validated or re-counted as a new reference.
        tx.execute("DELETE FROM nodes WHERE id = ?1", params![target_id])?;
        pack_ref_gone_tx(&tx, target.object_key.as_deref(), now())?;
        let moved = tx.execute(
            "UPDATE nodes SET parent_id = ?2, name = ?3, mtime = ?4 WHERE id = ?1",
            params![source_id, new_parent_id, new_name, now()],
        )?;
        if moved == 0 {
            // The source vanished under us; the transaction rolls back and
            // the target stands.
            return Err(Error::NotFound(format!("id {source_id}")));
        }
        tx.commit()?;
        Ok(target)
    }

    fn delete_node(&self, id: i64) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        // P0-a path D: the reference retirement for the deleted node's
        // object key (pack state + unref_since) commits in the SAME
        // transaction as the row deletion — never via the out-of-band
        // best-effort retire path.
        if id == 1 {
            return Err(Error::InvalidArgument("cannot delete root".into()));
        }
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let old_key: Option<String> = tx
            .query_row(
                "SELECT object_key FROM nodes WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let n = tx.execute("DELETE FROM nodes WHERE id = ?1", params![id])?;
        if n == 0 {
            return Err(Error::NotFound(format!("id {id}")));
        }
        pack_ref_gone_tx(&tx, old_key.as_deref(), now())?;
        tx.commit()?;
        Ok(())
    }

    fn all_nodes(&self) -> Result<Vec<Node>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(&format!("SELECT {NODE_COLS} FROM nodes ORDER BY id"))?;
        let rows = stmt.query_map([], row_to_node)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn record_superseded(&self, object_key: &str, at: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO superseded_objects (object_key, superseded_at) VALUES (?1, ?2)",
            params![object_key, at],
        )?;
        Ok(())
    }

    fn gc_candidates(&self, cutoff: i64, limit: usize) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT s.object_key FROM superseded_objects s \
             WHERE s.superseded_at <= ?1 \
               AND NOT EXISTS (SELECT 1 FROM nodes n WHERE n.object_key = s.object_key) \
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![cutoff, limit as i64], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn drop_superseded(&self, object_key: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM superseded_objects WHERE object_key = ?1",
            params![object_key],
        )?;
        Ok(())
    }

    fn all_superseded(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT object_key FROM superseded_objects ORDER BY object_key")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn commit_content_cas_pack(
        &self,
        id: i64,
        expected_version: i64,
        size: i64,
        mtime: i64,
        pack_key: &str,
        pack_off: i64,
        pack_len: i64,
        content_hash: &str,
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        // Same single-statement CAS shape as commit_content_cas: the WHERE
        // re-check is the only success path. etag is CLEARED (empty) — a
        // shared pack's ETag is not this file's; empty-etag rows are
        // excluded from the read cache by the R4.3 guards.
        // P0-a path B: the new reference on Q (must be `committing`) and
        // the old key's reference retirement commit in the SAME transaction
        // as the CAS; a failure rolls all three back.
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let old_key: Option<String> = tx
            .query_row(
                "SELECT object_key FROM nodes WHERE id = ?1",
                params![id],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let n = tx.execute(
            "UPDATE nodes
             SET size = ?2, mtime = ?3, version = version + 1, object_key = ?4, etag = '',
                 pack_off = ?5, pack_len = ?6, content_hash = ?7
             WHERE id = ?1 AND version = ?8",
            params![
                id,
                size,
                mtime,
                pack_key,
                pack_off,
                pack_len,
                content_hash,
                expected_version.to_string()
            ],
        )?;
        if n == 0 {
            let exists: Option<String> = tx
                .query_row(
                    "SELECT version FROM nodes WHERE id = ?1",
                    params![id],
                    |r| r.get(0),
                )
                .optional()?;
            return match exists {
                Some(actual) => {
                    let actual: i64 = actual.parse().unwrap_or(0);
                    Err(Error::Conflict {
                        path: format!("id {id}"),
                        expected: expected_version,
                        actual,
                    })
                }
                None => Err(Error::NotFound(format!("id {id}"))),
            };
        }
        pack_ref_new_tx(&tx, Some(pack_key), mtime)?;
        pack_ref_gone_tx(&tx, old_key.as_deref(), mtime)?;
        tx.commit()?;
        Ok(())
    }

    fn register_pack(&self, key: &str, bytes: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // INSERT OR IGNORE: a re-registration after a crash keeps the
        // original row (and its uploaded_at state) untouched.
        conn.execute(
            "INSERT OR IGNORE INTO packs (key, bytes, uploaded_at) VALUES (?1, ?2, NULL)",
            params![key, bytes],
        )?;
        Ok(())
    }

    fn mark_pack_uploaded(&self, key: &str, at: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE packs SET uploaded_at = ?2, state = 'committing' WHERE key = ?1 AND state = 'building'",
            params![key, at],
        )?;
        Ok(())
    }

    fn gc_pack_candidates(&self, cutoff: i64, limit: usize) -> Result<Vec<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        // P0-a: retention runs from the moment the LAST live reference
        // disappeared (`unref_since`), and only batch-terminated packs that
        // are not already being deleted qualify. The NOT EXISTS guard is
        // re-checked at deletion-registration time inside a transaction.
        let mut stmt = conn.prepare(
            "SELECT p.key, p.bytes FROM packs p \
             WHERE p.state = 'unreferenced' AND p.unref_since IS NOT NULL AND p.unref_since <= ?1 \
               AND NOT EXISTS (SELECT 1 FROM nodes n WHERE n.object_key = p.key) \
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![cutoff, limit as i64], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn drop_pack(&self, key: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM packs WHERE key = ?1", params![key])?;
        Ok(())
    }

    fn all_packs(&self) -> Result<Vec<(String, i64, Option<i64>)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT key, bytes, uploaded_at FROM packs ORDER BY key")?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Option<i64>>(2)?,
            ))
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ---- S4.2 P0-a: pack lifecycle, batch binding/termination, durable
    // deletion tasks. Everything here is either its own transaction or a
    // helper invoked inside the caller's transaction — never a mix.

    fn pack_state(&self, key: &str) -> Result<Option<(String, Option<i64>)>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT state, unref_since FROM packs WHERE key = ?1",
                params![key],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<i64>>(1)?)),
            )
            .optional()?;
        Ok(row)
    }

    fn committing_packs(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt =
            conn.prepare("SELECT key FROM packs WHERE state = 'committing' ORDER BY key")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    fn wb_bind_pack_keys(&self, ids: &[i64], pack_key: Option<&str>, now: i64) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let placeholders = ids.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let params: Vec<&dyn rusqlite::ToSql> =
            ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        // Distinct pack bindings these tasks are leaving behind.
        let sql_old = format!(
            "SELECT DISTINCT pack_key FROM wb_tasks WHERE id IN ({placeholders}) AND pack_key IS NOT NULL"
        );
        let mut stmt = tx.prepare(&sql_old)?;
        let old_packs: Vec<String> = stmt
            .query_map(params.as_slice(), |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        drop(stmt);
        let sql = format!(
            "UPDATE wb_tasks SET pack_key = ?, updated_at = ?2 WHERE id IN ({placeholders})"
        );
        let mut bind: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(ids.len() + 2);
        bind.push(&pack_key);
        bind.push(&now);
        bind.extend(ids.iter().map(|id| id as &dyn rusqlite::ToSql));
        tx.execute(&sql, bind.as_slice())?;
        // A batch whose members all moved away terminates in this same
        // transaction; the newly claimed batch (if any) is not finalized.
        for old in &old_packs {
            if Some(old.as_str()) != pack_key {
                finalize_batch_tx(&tx, old, now)?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn wb_finalize_batch_if_terminal(&self, pack_key: &str, now: i64) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        finalize_batch_tx(&tx, pack_key, now)?;
        tx.commit()?;
        Ok(())
    }

    fn wb_set_uploaded_key(&self, id: i64, key: &str, now: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE wb_tasks SET uploaded_key = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, key, now],
        )?;
        Ok(())
    }

    fn dead_task_orphans(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        // Dead tasks that had already uploaded their bytes: the object is an
        // orphan candidate ONLY if no live node references that key (the
        // single-winner always holds the reference it created).
        let mut stmt = conn.prepare(
            "SELECT DISTINCT t.uploaded_key FROM wb_tasks t \
             WHERE t.state = 'dead' AND t.uploaded_key IS NOT NULL \
               AND NOT EXISTS (SELECT 1 FROM nodes n WHERE n.object_key = t.uploaded_key)",
        )?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<_>>().map_err(Into::into)
    }

    fn gc_register_pack_deletion(&self, key: &str, now: i64) -> Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        // Registration re-checks the full predicate inside the transaction
        // and flips the pack to `deleting` — from which ref_new refuses.
        let ok: bool = tx
            .query_row(
                "SELECT NOT EXISTS (SELECT 1 FROM nodes WHERE object_key = ?1)",
                params![key],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(false);
        if !ok {
            return Ok(false);
        }
        let n = tx.execute(
            "UPDATE packs SET state = 'deleting' WHERE key = ?1 AND state = 'unreferenced'",
            params![key],
        )?;
        if n == 0 {
            return Ok(false);
        }
        tx.execute(
            "INSERT OR IGNORE INTO gc_deletions (key, kind, state, updated_at) \
             VALUES (?1, 'pack', 'pending', ?2)",
            params![key, now],
        )?;
        tx.commit()?;
        Ok(true)
    }

    fn gc_register_superseded_deletion(&self, key: &str, now: i64) -> Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ok: bool = tx
            .query_row(
                "SELECT EXISTS (SELECT 1 FROM superseded_objects WHERE object_key = ?1) \
                 AND NOT EXISTS (SELECT 1 FROM nodes WHERE object_key = ?1)",
                params![key],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(false);
        if !ok {
            return Ok(false);
        }
        tx.execute(
            "INSERT OR IGNORE INTO gc_deletions (key, kind, state, updated_at) \
             VALUES (?1, 'object', 'pending', ?2)",
            params![key, now],
        )?;
        tx.commit()?;
        Ok(true)
    }

    fn gc_enqueue_orphan_object(&self, key: &str, now: i64) -> Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let ok: bool = tx
            .query_row(
                "SELECT NOT EXISTS (SELECT 1 FROM nodes WHERE object_key = ?1)",
                params![key],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(false);
        if !ok {
            return Ok(false);
        }
        tx.execute(
            "INSERT OR IGNORE INTO gc_deletions (key, kind, state, updated_at) \
             VALUES (?1, 'object', 'pending', ?2)",
            params![key, now],
        )?;
        tx.commit()?;
        Ok(true)
    }

    fn gc_claim_deletions(&self, limit: usize, now: i64) -> Result<Vec<GcDeletion>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        // A deletion left `deleting` by a crash between the remote delete
        // and the local finalize re-enters the queue here; the remote
        // delete itself is idempotent, so re-execution converges.
        tx.execute(
            "UPDATE gc_deletions SET state = 'pending', updated_at = ?1 WHERE state = 'deleting'",
            params![now],
        )?;
        let mut stmt = tx.prepare(
            "SELECT id, key, kind FROM gc_deletions \
             WHERE state IN ('pending','failed') ORDER BY id LIMIT ?1",
        )?;
        let rows: Vec<GcDeletion> = stmt
            .query_map(params![limit as i64], |r| {
                Ok(GcDeletion {
                    id: r.get(0)?,
                    key: r.get(1)?,
                    kind: r.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
        drop(stmt);
        for d in &rows {
            tx.execute(
                "UPDATE gc_deletions SET state = 'deleting', updated_at = ?2 WHERE id = ?1",
                params![d.id, now],
            )?;
        }
        tx.commit()?;
        Ok(rows)
    }

    fn gc_complete_deletion(&self, id: i64, now: i64) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (key, kind): (String, String) = tx.query_row(
            "SELECT key, kind FROM gc_deletions WHERE id = ?1 AND state = 'deleting'",
            params![id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        match kind.as_str() {
            "pack" => tx.execute("DELETE FROM packs WHERE key = ?1", params![key])?,
            "object" => tx.execute(
                "DELETE FROM superseded_objects WHERE object_key = ?1",
                params![key],
            )?,
            _ => 0,
        };
        tx.execute(
            "UPDATE gc_deletions SET state = 'done', updated_at = ?2 WHERE id = ?1",
            params![id, now],
        )?;
        tx.commit()?;
        Ok(())
    }

    fn gc_fail_deletion(&self, id: i64, err: &str, now: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE gc_deletions SET state = 'failed', attempts = attempts + 1, \
             last_error = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, err, now],
        )?;
        Ok(())
    }

    fn wb_enqueue(
        &self,
        node_id: i64,
        staging: &str,
        size: i64,
        expected: i64,
        now: i64,
    ) -> Result<(i64, Option<String>)> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let version: Option<String> = tx
            .query_row(
                "SELECT version FROM nodes WHERE id = ?1",
                params![node_id],
                |r| r.get(0),
            )
            .optional()?;
        let Some(v) = version else {
            return Err(Error::NotFound(format!("id {node_id}")));
        };
        let committed: i64 = v.parse().unwrap_or(0);
        // Coalesce only a strictly newer expected version (same writer
        // lineage). An equal expected means another writer aiming at the
        // same base: it queues separately and the CAS at drain decides.
        let existing: Option<(i64, i64, String)> = tx
            .query_row(
                "SELECT id, expected, staging FROM wb_tasks \
                 WHERE node_id = ?1 AND state = 'queued' ORDER BY id LIMIT 1",
                params![node_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let mut replaced = None;
        if let Some((id, task_expected, old_staging)) = existing {
            if expected > task_expected {
                // Coalesce CONTENT only. The task's `expected` stays at the
                // committed anchor the first flush observed: the drain's
                // CAS gates against the COMMITTED version (which has not
                // moved — the task is still queued), while the client's
                // newer `expected` is a PROJECTED version. Rewriting it
                // would make the drain conflict with reality.
                tx.execute(
                    "UPDATE wb_tasks SET staging = ?2, size = ?3, attempts = 0, \
                     updated_at = ?4 WHERE id = ?1",
                    params![id, staging, size, now],
                )?;
                replaced = Some(old_staging);
            }
        }
        if replaced.is_none() {
            tx.execute(
                "INSERT INTO wb_tasks (node_id, staging, size, expected, state, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, 'queued', ?5)",
                params![node_id, staging, size, expected, now],
            )
            .map_err(|e| map_constraint(e, || Error::Storage("wb insert".into())))?;
        }
        let pending: i64 = tx.query_row(
            "SELECT COUNT(*) FROM wb_tasks WHERE node_id = ?1 AND state IN ('queued','draining')",
            params![node_id],
            |r| r.get(0),
        )?;
        tx.commit()?;
        Ok((committed + pending, replaced))
    }

    fn wb_claim(&self, now: i64) -> Result<Option<crate::metadata::WbTask>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row = tx
            .query_row(
                "SELECT id, node_id, staging, size, expected FROM wb_tasks \
                 WHERE state = 'queued' ORDER BY id LIMIT 1",
                [],
                |r| {
                    Ok(crate::metadata::WbTask {
                        id: r.get(0)?,
                        node_id: r.get(1)?,
                        staging: r.get(2)?,
                        size: r.get(3)?,
                        expected: r.get(4)?,
                    })
                },
            )
            .optional()?;
        if let Some(t) = &row {
            // P0-a: a per-file claim moves the task out of any batch it was
            // bound to; the abandoned batch finalizes in this same
            // transaction when no non-terminal task remains for it.
            let old_pack: Option<String> = tx
                .query_row(
                    "SELECT pack_key FROM wb_tasks WHERE id = ?1",
                    params![t.id],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            let n = tx.execute(
                "UPDATE wb_tasks SET state = 'draining', updated_at = ?2, pack_key = NULL \
                 WHERE id = ?1 AND state = 'queued'",
                params![t.id, now],
            )?;
            if n == 0 {
                tx.commit()?;
                return Ok(None); // raced: another drainer claimed it
            }
            if let Some(pk) = old_pack {
                finalize_batch_tx(&tx, &pk, now)?;
            }
        }
        tx.commit()?;
        Ok(row)
    }

    fn wb_claim_fit(&self, max_bytes: i64, now: i64) -> Result<Option<crate::metadata::WbTask>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let row = tx
            .query_row(
                "SELECT id, node_id, staging, size, expected FROM wb_tasks \
                 WHERE state = 'queued' AND size <= ?1 ORDER BY id LIMIT 1",
                params![max_bytes],
                |r| {
                    Ok(crate::metadata::WbTask {
                        id: r.get(0)?,
                        node_id: r.get(1)?,
                        staging: r.get(2)?,
                        size: r.get(3)?,
                        expected: r.get(4)?,
                    })
                },
            )
            .optional()?;
        if let Some(t) = &row {
            let n = tx.execute(
                "UPDATE wb_tasks SET state = 'draining', updated_at = ?2 \
                 WHERE id = ?1 AND state = 'queued'",
                params![t.id, now],
            )?;
            if n == 0 {
                tx.commit()?;
                return Ok(None);
            }
        }
        tx.commit()?;
        Ok(row)
    }

    fn wb_finish(&self, id: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM wb_tasks WHERE id = ?1", params![id])?;
        Ok(())
    }

    fn wb_retry_or_dead(&self, id: i64, max_attempts: u32, now: i64) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE wb_tasks SET attempts = attempts + 1, updated_at = ?2 WHERE id = ?1",
            params![id, now],
        )?;
        let attempts: i64 = conn.query_row(
            "SELECT attempts FROM wb_tasks WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )?;
        if attempts >= max_attempts as i64 {
            conn.execute(
                "UPDATE wb_tasks SET state = 'dead', updated_at = ?2 WHERE id = ?1",
                params![id, now],
            )?;
            return Ok(false);
        }
        conn.execute(
            "UPDATE wb_tasks SET state = 'queued', updated_at = ?2 WHERE id = ?1",
            params![id, now],
        )?;
        Ok(true)
    }

    fn wb_set_state(&self, id: i64, state: &str, now: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE wb_tasks SET state = ?2, updated_at = ?3 WHERE id = ?1",
            params![id, state, now],
        )?;
        Ok(())
    }

    fn wb_requeue_draining(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE wb_tasks SET state = 'queued', updated_at = ?1 WHERE state = 'draining'",
            params![now()],
        )?;
        Ok(())
    }

    fn wb_active_tasks(&self) -> Result<Vec<crate::metadata::WbTask>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, node_id, staging, size, expected FROM wb_tasks \
             WHERE state IN ('queued','draining') ORDER BY id",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(crate::metadata::WbTask {
                id: r.get(0)?,
                node_id: r.get(1)?,
                staging: r.get(2)?,
                size: r.get(3)?,
                expected: r.get(4)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn wb_all_stagings(&self) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT staging FROM wb_tasks")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    fn wb_latest_staging(&self, node_id: i64) -> Result<Option<(String, i64)>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT staging, size FROM wb_tasks \
                 WHERE node_id = ?1 AND state IN ('queued','draining') ORDER BY id DESC LIMIT 1",
                params![node_id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?;
        Ok(row)
    }

    fn wb_projection(&self, node_id: i64) -> Result<Option<(i64, i64)>> {
        let conn = self.conn.lock().unwrap();
        let row: Option<(i64, i64)> = conn
            .query_row(
                "SELECT (SELECT size FROM wb_tasks WHERE node_id = ?1 \
                         AND state IN ('queued','draining') ORDER BY id DESC LIMIT 1), \
                        COUNT(*) FROM wb_tasks WHERE node_id = ?1 \
                        AND state IN ('queued','draining')",
                params![node_id],
                |r| {
                    Ok((
                        r.get::<_, Option<i64>>(0)?.unwrap_or(0),
                        r.get::<_, i64>(1)?,
                    ))
                },
            )
            .optional()?;
        match row {
            Some((size, count)) if count > 0 => Ok(Some((size, count))),
            _ => Ok(None),
        }
    }

    fn wb_cancel_queued(&self, node_id: i64) -> Result<Vec<String>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut stmt =
            tx.prepare("SELECT staging FROM wb_tasks WHERE node_id = ?1 AND state = 'queued'")?;
        let paths: Vec<String> = stmt
            .query_map(params![node_id], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<_>>()?;
        drop(stmt);
        tx.execute(
            "DELETE FROM wb_tasks WHERE node_id = ?1 AND state = 'queued'",
            params![node_id],
        )?;
        tx.commit()?;
        Ok(paths)
    }

    fn wb_counts(&self) -> Result<(u64, u64, u64)> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT state, COUNT(*) FROM wb_tasks GROUP BY state")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        let mut q = 0u64;
        let mut d = 0u64;
        let mut dead = 0u64;
        for r in rows {
            let (state, n) = r?;
            match state.as_str() {
                "queued" => q = n as u64,
                "draining" => d = n as u64,
                "dead" => dead = n as u64,
                _ => {}
            }
        }
        Ok((q, d, dead))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> SqliteMetadataStore {
        SqliteMetadataStore::open_in_memory().unwrap()
    }

    fn temp_path(tag: &str) -> String {
        let p = std::env::temp_dir().join(format!(
            "lightfs-meta-{tag}-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        p.to_string_lossy().to_string()
    }

    #[test]
    fn root_exists() {
        let s = store();
        let root = s.get_node(1).unwrap().unwrap();
        assert!(root.is_dir());
        assert_eq!(root.parent_id, None);
    }

    #[test]
    fn create_lookup_list() {
        let s = store();
        let dir = s
            .create_node(1, "docs", NodeKind::Dir, 0o755, None)
            .unwrap();
        assert!(dir.is_dir());
        let file = s
            .create_node(
                dir.id,
                "a.txt",
                NodeKind::File,
                0o644,
                Some("objects/3/data"),
            )
            .unwrap();
        assert_eq!(file.object_key.as_deref(), Some("objects/3/data"));
        assert_eq!(file.version, 0);

        assert!(s.get_child(1, "docs").unwrap().is_some());
        assert!(s.get_child(1, "nope").unwrap().is_none());
        let kids = s.list_children(dir.id).unwrap();
        assert_eq!(kids.len(), 1);
        assert_eq!(kids[0].name, "a.txt");
        assert_eq!(s.count_children(dir.id).unwrap(), 1);
    }

    #[test]
    fn duplicate_names_rejected() {
        let s = store();
        s.create_node(1, "x", NodeKind::Dir, 0o755, None).unwrap();
        let err = s
            .create_node(1, "x", NodeKind::Dir, 0o755, None)
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
        // same name allowed under different parents
        let d2 = s.create_node(1, "y", NodeKind::Dir, 0o755, None).unwrap();
        s.create_node(d2.id, "x", NodeKind::Dir, 0o755, None)
            .unwrap();
    }

    #[test]
    fn parent_must_exist_and_be_dir() {
        let s = store();
        assert!(matches!(
            s.create_node(999, "x", NodeKind::Dir, 0o755, None),
            Err(Error::ParentNotFound(_))
        ));
        let f = s
            .create_node(1, "f", NodeKind::File, 0o644, Some("objects/9/data"))
            .unwrap();
        assert!(matches!(
            s.create_node(f.id, "child", NodeKind::Dir, 0o755, None),
            Err(Error::KindMismatch { .. })
        ));
    }

    #[test]
    fn content_update_and_rename() {
        let s = store();
        let f = s
            .create_node(1, "f", NodeKind::File, 0o644, Some("objects/5/w-a.data"))
            .unwrap();
        s.commit_content_cas(f.id, 0, 42, 1000, "objects/5/w-b.data", "etag-1")
            .unwrap();
        let f2 = s.get_node(f.id).unwrap().unwrap();
        assert_eq!(f2.size, 42);
        assert_eq!(f2.version, 1);
        assert_eq!(f2.etag, "etag-1");
        assert_eq!(f2.object_key.as_deref(), Some("objects/5/w-b.data"));

        let dir = s.create_node(1, "d", NodeKind::Dir, 0o755, None).unwrap();
        s.rename_node(f.id, dir.id, "g").unwrap();
        assert!(s.get_child(dir.id, "g").unwrap().is_some());
        assert!(s.get_child(1, "f").unwrap().is_none());

        // rename onto occupied slot fails
        let f3 = s
            .create_node(1, "f", NodeKind::File, 0o644, Some("objects/6/w-a.data"))
            .unwrap();
        assert!(matches!(
            s.rename_node(f3.id, dir.id, "g"),
            Err(Error::AlreadyExists(_))
        ));
        // rename onto itself is fine
        s.rename_node(f.id, dir.id, "g").unwrap();
    }

    /// The concurrency contract at the metadata layer: exactly one CAS with a
    /// given expected_version can succeed; the loser gets Conflict and the
    /// stored state stays the winner's. This is deterministic.
    #[test]
    fn commit_content_cas_is_single_winner() {
        let s = store();
        let f = s
            .create_node(1, "f", NodeKind::File, 0o644, Some("objects/9/w-init.data"))
            .unwrap();
        s.commit_content_cas(f.id, 0, 1, 0, "objects/9/w-v1.data", "etag-v1")
            .unwrap();

        // Two writers both observed version 1.
        s.commit_content_cas(f.id, 1, 100, 0, "objects/9/w-winner.data", "etag-winner")
            .unwrap();
        let err = s
            .commit_content_cas(f.id, 1, 200, 0, "objects/9/w-loser.data", "etag-loser")
            .unwrap_err();
        assert!(matches!(
            err,
            Error::Conflict {
                expected: 1,
                actual: 2,
                ..
            }
        ));

        let after = s.get_node(f.id).unwrap().unwrap();
        assert_eq!(after.version, 2);
        assert_eq!(after.size, 100);
        assert_eq!(after.etag, "etag-winner");
        assert_eq!(after.object_key.as_deref(), Some("objects/9/w-winner.data"));

        // CAS against a vanished node is NotFound, not Conflict.
        assert!(matches!(
            s.commit_content_cas(424242, 1, 1, 0, "objects/x/w.data", "e"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn delete_and_persistence() {
        let path = temp_path("persist");
        {
            let s = SqliteMetadataStore::open(&path).unwrap();
            let d = s
                .create_node(1, "docs", NodeKind::Dir, 0o755, None)
                .unwrap();
            s.create_node(
                d.id,
                "report",
                NodeKind::File,
                0o644,
                Some("objects/7/data"),
            )
            .unwrap();
        }
        {
            // "restart": reopen and verify namespace persisted
            let s = SqliteMetadataStore::open(&path).unwrap();
            let d = s.get_child(1, "docs").unwrap().unwrap();
            let f = s.get_child(d.id, "report").unwrap().unwrap();
            // an explicit key persists verbatim (no id-rewrite anymore)
            assert_eq!(f.object_key.as_deref(), Some("objects/7/data"));
            assert!(matches!(s.delete_node(f.id), Ok(())));
            assert_eq!(s.count_children(d.id).unwrap(), 0);
            s.delete_node(d.id).unwrap();
            assert!(s.get_node(d.id).unwrap().is_none());
            assert!(matches!(s.delete_node(1), Err(Error::InvalidArgument(_))));
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{path}-wal"));
        let _ = std::fs::remove_file(format!("{path}-shm"));
    }

    // --- R4.2c: stable identity starts at CREATE time -----------------------

    #[test]
    fn fresh_databases_get_autoincrement_identity() {
        let dir = std::env::temp_dir().join(format!("lightfs-ra-schema-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fresh.db");
        SqliteMetadataStore::open(path.to_str().unwrap()).unwrap();
        let conn = rusqlite::Connection::open(&path).unwrap();
        let sql: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'nodes'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            sql.contains("AUTOINCREMENT"),
            "nodes must allocate ids without reuse from CREATE time: {sql}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A database written before AUTOINCREMENT cannot satisfy the identity
    /// guarantee: open refuses LOUDLY and leaves the legacy file untouched —
    /// no migration, no journal-mode switch, no rows added. The test
    /// environment is reinitialized by hand, never automatically.
    #[test]
    fn legacy_databases_are_rejected_loudly_and_left_unmodified() {
        let dir = std::env::temp_dir().join(format!("lightfs-ra-legacy-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE nodes (
                    id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT NOT NULL,
                    kind TEXT NOT NULL, size INTEGER NOT NULL DEFAULT 0,
                    mtime INTEGER NOT NULL, mode INTEGER, object_key TEXT,
                    version TEXT, etag TEXT, target TEXT, UNIQUE(parent_id, name)
                );
                INSERT INTO nodes (id, parent_id, name, kind, size, mtime, mode)
                VALUES (1, NULL, '', 'dir', 0, 0, 493);",
            )
            .unwrap();
        }
        let snapshot = |p: &std::path::Path| {
            let conn = rusqlite::Connection::open(p).unwrap();
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'nodes'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let rows: i64 = conn
                .query_row("SELECT COUNT(*) FROM nodes", [], |r| r.get(0))
                .unwrap();
            (sql, rows)
        };
        let before = snapshot(&path);

        let err = match SqliteMetadataStore::open(path.to_str().unwrap()) {
            Ok(_) => panic!("a legacy database must be rejected at open"),
            Err(e) => e,
        };
        let msg = format!("{err}");
        assert!(
            msg.contains("AUTOINCREMENT") && msg.contains("Re-initialize"),
            "the rejection must say what is wrong and what to do: {msg}"
        );

        let after = snapshot(&path);
        assert_eq!(
            before, after,
            "a rejected open must not touch the legacy file"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// R4.2c review fix: the rejection must key on the id column's ACTUAL
    /// definition, not on the word AUTOINCREMENT appearing in the stored
    /// CREATE text. This legacy table is plain INTEGER PRIMARY KEY, but its
    /// SQL text contains the word (in a column DEFAULT) — the previous
    /// text-substring check admitted it; the definition-level check must
    /// refuse it.
    #[test]
    fn legacy_table_mentioning_autoincrement_in_text_is_still_rejected() {
        let dir = std::env::temp_dir().join(format!("lightfs-ra-word-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("legacy-word.db");
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute_batch(
                "CREATE TABLE nodes (
                    id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT NOT NULL,
                    kind TEXT NOT NULL, size INTEGER NOT NULL DEFAULT 0,
                    mtime INTEGER NOT NULL, mode INTEGER, object_key TEXT,
                    version TEXT, etag TEXT, target TEXT,
                    remark TEXT DEFAULT 'AUTOINCREMENT',
                    UNIQUE(parent_id, name)
                );
                INSERT INTO nodes (id, parent_id, name, kind, size, mtime, mode)
                VALUES (1, NULL, '', 'dir', 0, 0, 493);",
            )
            .unwrap();
        }
        let snapshot = |p: &std::path::Path| {
            let conn = rusqlite::Connection::open(p).unwrap();
            let sql: String = conn
                .query_row(
                    "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'nodes'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            let rows: i64 = conn
                .query_row("SELECT COUNT(*) FROM nodes", [], |r| r.get(0))
                .unwrap();
            (sql, rows)
        };
        assert!(
            snapshot(&path).0.contains("AUTOINCREMENT"),
            "counterexample premise: the word IS in the stored SQL text"
        );
        let before = snapshot(&path);

        match SqliteMetadataStore::open(path.to_str().unwrap()) {
            Ok(_) => panic!(
                "a table whose id is not AUTOINCREMENT must be rejected even when its SQL text mentions the word"
            ),
            Err(e) => {
                let msg = format!("{e}");
                assert!(msg.contains("AUTOINCREMENT"), "actionable message: {msg}");
            }
        }

        assert_eq!(before, snapshot(&path), "rejected open must not write");
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- S4.2 P0-a: pack lifecycle + reference atomicity + durable
    // deletion tasks. Each test names the object refs, pack states and task
    // states it asserts; all run on the in-memory store, no sleeps.

    fn pk(tag: &str) -> String {
        format!("{}{tag}.data", crate::pack::PACK_KEY_PREFIX)
    }

    /// register + mark uploaded: the pack sits in `committing` exactly as a
    /// batch does between its PUT and its last member CAS.
    fn seed_committing(s: &SqliteMetadataStore, key: &str) {
        s.register_pack(key, 100).unwrap();
        s.mark_pack_uploaded(key, 5).unwrap();
        assert_eq!(
            s.pack_state(key).unwrap().map(|(st, _)| st),
            Some("committing".into())
        );
    }

    fn file_node(s: &SqliteMetadataStore, name: &str, key: Option<&str>) -> Node {
        s.create_node(1, name, NodeKind::File, 0o644, key).unwrap()
    }

    fn state_of(s: &SqliteMetadataStore, key: &str) -> (String, Option<i64>) {
        s.pack_state(key)
            .unwrap()
            .unwrap_or_else(|| panic!("pack {key} has no row"))
    }

    /// Path A: strict CAS off a pack onto a per-file version key. The pack
    /// reference decrement is part of the CAS transaction; with no other
    /// references and no batch tasks, the pack terminates to `unreferenced`
    /// with `unref_since` set from the same commit.
    #[test]
    fn p0a_path_a_strict_cas_last_pack_ref_goes_unreferenced_atomically() {
        let s = store();
        let p = pk("a1");
        seed_committing(&s, &p);
        let n = file_node(&s, "f", Some(&p));
        let vk = "objects/x/v1.data";
        s.commit_content_cas(n.id, 0, 10, 42, vk, "e1").unwrap();
        assert_eq!(s.get_node(n.id).unwrap().unwrap().object_key.unwrap(), vk);
        let (st, unref) = state_of(&s, &p);
        assert_eq!(st, "unreferenced", "last reference gone in the CAS tx");
        assert_eq!(unref, Some(42), "retention starts at the CAS commit");
    }

    /// Path A variant: another node still references the pack — it must
    /// stay valid (batch-terminated `active`), never unreferenced.
    #[test]
    fn p0a_path_a_shared_pack_stays_active_after_one_strict_cas() {
        let s = store();
        let p = pk("a2");
        seed_committing(&s, &p);
        let n1 = file_node(&s, "f1", Some(&p));
        let n2 = file_node(&s, "f2", Some(&p));
        s.commit_content_cas(n1.id, 0, 10, 42, "objects/x/v1.data", "e")
            .unwrap();
        let (st, unref) = state_of(&s, &p);
        assert_eq!(st, "active", "one reference remains");
        assert_eq!(unref, None);
        assert_eq!(
            s.get_node(n2.id).unwrap().unwrap().object_key.unwrap(),
            p,
            "sibling reference untouched"
        );
    }

    /// Path B: pack-to-pack CAS requires the target in `committing`, moves
    /// the reference atomically, and a failed CAS changes nothing.
    #[test]
    fn p0a_path_b_pack_cas_requires_committing_target_and_moves_ref() {
        let s = store();
        let old = pk("b-old");
        let new = pk("b-new");
        seed_committing(&s, &old);
        seed_committing(&s, &new);
        let n = file_node(&s, "f", Some(&old));
        s.commit_content_cas_pack(n.id, 0, 10, 42, &new, 0, 100, "h")
            .unwrap();
        assert_eq!(
            s.get_node(n.id).unwrap().unwrap().object_key.unwrap(),
            new,
            "node now references the new pack"
        );
        assert_eq!(state_of(&s, &new), ("active".into(), None), "ref held");
        assert_eq!(
            state_of(&s, &old),
            ("unreferenced".into(), Some(42)),
            "old pack's last reference vanished in the same tx"
        );

        // A losing CAS must not create the new reference: old stays
        // `unreferenced`, new stays `active`, node still points at new.
        let err = s
            .commit_content_cas_pack(n.id, 0, 10, 43, &new, 0, 100, "h")
            .unwrap_err();
        assert!(matches!(err, Error::Conflict { .. }), "{err:?}");
        assert_eq!(state_of(&s, &new), ("active".into(), None));
    }

    /// Path B guard: `deleting` (and any non-committing state) refuses new
    /// references; the CAS is rolled back entirely.
    #[test]
    fn p0a_path_b_deleting_pack_refuses_new_reference() {
        let s = store();
        let p = pk("b-del");
        seed_committing(&s, &p);
        // No references + no tasks: finalize to `unreferenced`, then register
        // the deletion intent (flips to `deleting`).
        s.wb_finalize_batch_if_terminal(&p, 5).unwrap();
        assert_eq!(state_of(&s, &p).0, "unreferenced");
        assert!(s.gc_register_pack_deletion(&p, 6).unwrap());
        assert_eq!(state_of(&s, &p).0, "deleting");

        let n = file_node(&s, "f", None);
        let err = s
            .commit_content_cas_pack(n.id, 0, 10, 42, &p, 0, 100, "h")
            .unwrap_err();
        assert!(
            matches!(&err, Error::Storage(m) if m.contains("deleting")),
            "must refuse a deleting pack, got {err:?}"
        );
        assert_eq!(
            s.get_node(n.id).unwrap().unwrap().object_key,
            None,
            "CAS must be rolled back with the refused reference"
        );
        // `building` (row registered, upload unfinished) is equally refused.
        let b = pk("b-bld");
        s.register_pack(&b, 10).unwrap();
        let n2 = file_node(&s, "g", None);
        assert!(
            s.commit_content_cas_pack(n2.id, 0, 10, 42, &b, 0, 10, "h")
                .is_err()
        );
    }

    /// Path C: rename-over decrements ONLY the overwritten target's
    /// reference; the source's key moves verbatim without re-validation.
    #[test]
    fn p0a_path_c_rename_overwrite_decrements_target_ref_only() {
        let s = store();
        let pt = pk("c-t");
        let ps = pk("c-s");
        seed_committing(&s, &pt);
        seed_committing(&s, &ps);
        let target = file_node(&s, "t", Some(&pt));
        let source = file_node(&s, "s", Some(&ps));
        // (The return value mirrors the replaced target snapshot — an
        // existing API shape; the durable truth is what the row now holds.)
        let _moved = s
            .rename_node_overwrite(source.id, target.id, 1, "t")
            .unwrap();
        let moved_row = s.get_node(source.id).unwrap().unwrap();
        assert_eq!(moved_row.id, source.id, "source node identity survives");
        assert_eq!(
            moved_row.object_key.as_deref(),
            Some(ps.as_str()),
            "source key moves verbatim"
        );
        let (tst, tunref) = state_of(&s, &pt);
        assert_eq!(tst, "unreferenced", "target was the pack's last reference");
        assert!(tunref.is_some(), "retention start recorded");
        // The source's pack saw NO reference event (its key moved verbatim),
        // so it stays `committing` — promotion is its own batch's business:
        assert_eq!(state_of(&s, &ps), ("committing".into(), None));
        s.wb_finalize_batch_if_terminal(&ps, 9).unwrap();
        assert_eq!(
            state_of(&s, &ps),
            ("active".into(), None),
            "and the surviving reference keeps it valid at termination"
        );
    }

    /// Path C variant: source and target share one pack — exactly one
    /// reference (the target's) is removed, the source keeps the pack alive.
    #[test]
    fn p0a_path_c_rename_overwrite_same_pack_keeps_one_ref() {
        let s = store();
        let p = pk("c-same");
        seed_committing(&s, &p);
        let target = file_node(&s, "t", Some(&p));
        let source = file_node(&s, "s", Some(&p));
        s.rename_node_overwrite(source.id, target.id, 1, "t")
            .unwrap();
        let refs_left = s.get_node(source.id).unwrap().unwrap();
        assert_eq!(refs_left.object_key.as_deref(), Some(p.as_str()));
        assert_eq!(
            state_of(&s, &p),
            ("active".into(), None),
            "source still references the pack"
        );
    }

    /// Path D: unlinking the last reference commits the node deletion and
    /// the lifecycle update (`unreferenced` + `unref_since`) atomically;
    /// shared packs stay alive; failed deletions change nothing.
    #[test]
    fn p0a_path_d_unlink_last_ref_atomic_and_shared_pack_stays() {
        let s = store();
        let p = pk("d");
        seed_committing(&s, &p);
        let n1 = file_node(&s, "f1", Some(&p));
        let n2 = file_node(&s, "f2", Some(&p));
        // Failure first: a NotFound delete must not touch the pack.
        assert!(matches!(
            s.delete_node(9999).unwrap_err(),
            Error::NotFound(_)
        ));
        assert_eq!(state_of(&s, &p).0, "committing");
        // One of two references goes away: pack remains valid.
        s.delete_node(n1.id).unwrap();
        assert_eq!(
            state_of(&s, &p),
            ("active".into(), None),
            "sibling reference keeps the pack"
        );
        // Last reference: same-transaction lifecycle update.
        s.delete_node(n2.id).unwrap();
        let (st, unref) = state_of(&s, &p);
        assert_eq!(st, "unreferenced");
        assert!(unref.is_some(), "retention start recorded atomically");
    }

    /// Path E: reference-free creation paths (file with NULL key, symlink)
    /// must not perturb pack lifecycle state.
    #[test]
    fn p0a_path_e_null_key_creations_are_lifecycle_noops() {
        let s = store();
        let p = pk("e");
        seed_committing(&s, &p);
        file_node(&s, "plain", None);
        s.create_symlink(1, "link", "/f", 0o777).unwrap();
        assert_eq!(state_of(&s, &p), ("committing".into(), None));
    }

    /// Invariant 3: a `committing` pack never becomes `unreferenced` while
    /// a batch member is still non-terminal — even at zero references.
    /// (Red-test 5: member A committed then unlinked, member B still queued.)
    #[test]
    fn p0a_committing_zero_refs_survives_until_batch_members_terminal() {
        let s = store();
        let p = pk("inv3");
        seed_committing(&s, &p);
        let n = file_node(&s, "f", Some(&p));
        // Two batch members bound to the pack (B still queued = non-terminal).
        let staged = temp_path("inv3-stage");
        let (projected, _) = s.wb_enqueue(n.id, &staged, 4, 1, 5).unwrap();
        let _ = projected;
        {
            let conn = s.conn.lock().unwrap();
            conn.execute("UPDATE wb_tasks SET pack_key = ?1", params![p])
                .unwrap();
        }
        // The batch member commits (task still queued = crashed before
        // finish): references hit zero, but the pack MUST stay committing.
        s.commit_content_cas_pack(n.id, 0, 4, 42, &p, 0, 4, "h")
            .unwrap();
        assert_eq!(
            state_of(&s, &p).0,
            "committing",
            "zero refs must not finalize a submitting batch"
        );
        // Unlink the file while B is still queued: still committing.
        s.delete_node(n.id).unwrap();
        assert_eq!(state_of(&s, &p).0, "committing");
        // All members terminal (row removed): now the batch terminates to
        // `unreferenced` and retention starts.
        {
            let conn = s.conn.lock().unwrap();
            conn.execute("DELETE FROM wb_tasks", []).unwrap();
        }
        s.wb_finalize_batch_if_terminal(&p, 50).unwrap();
        assert_eq!(state_of(&s, &p), ("unreferenced".into(), Some(50)));
    }

    /// Red-test 6/12: a batch whose members ALL fail still terminates —
    /// `unreferenced` + `unref_since`, never permanently stuck.
    #[test]
    fn p0a_all_failed_batch_terminates_to_unreferenced() {
        let s = store();
        let p = pk("allfail");
        seed_committing(&s, &p);
        let staged = temp_path("allfail-stage");
        let n = file_node(&s, "f", None);
        for _ in 0..2 {
            s.wb_enqueue(n.id, &staged, 4, 1, 5).unwrap();
        }
        {
            let conn = s.conn.lock().unwrap();
            conn.execute("UPDATE wb_tasks SET pack_key = ?1", params![p])
                .unwrap();
            conn.execute("UPDATE wb_tasks SET state = 'dead'", [])
                .unwrap();
        }
        s.wb_finalize_batch_if_terminal(&p, 77).unwrap();
        assert_eq!(state_of(&s, &p), ("unreferenced".into(), Some(77)));
        // gc sees it as a candidate once the retention window passes.
        let cands = s.gc_pack_candidates(i64::MAX, 10).unwrap();
        assert_eq!(cands, vec![(p.clone(), 100)]);
    }

    /// Red-test 7: partial success — the winner keeps the pack `active`,
    /// the loser is a terminal `dead` row; no drift between refs and state.
    #[test]
    fn p0a_partial_success_batch_ends_active_with_winner_ref() {
        let s = store();
        let p = pk("partial");
        seed_committing(&s, &p);
        let n1 = file_node(&s, "f1", None);
        let n2 = file_node(&s, "f2", None);
        let staged = temp_path("partial-stage");
        s.wb_enqueue(n1.id, &staged, 4, 0, 5).unwrap();
        s.wb_enqueue(n2.id, &staged, 4, 0, 5).unwrap();
        {
            let conn = s.conn.lock().unwrap();
            conn.execute("UPDATE wb_tasks SET pack_key = ?1", params![p])
                .unwrap();
        }
        // Member 1 commits; member 2 dead-letters (its CAS failed for good).
        // Task rows are addressed by node (wb_enqueue returns the projected
        // version, not the task id).
        s.commit_content_cas_pack(n1.id, 0, 4, 42, &p, 0, 4, "h")
            .unwrap();
        {
            let conn = s.conn.lock().unwrap();
            conn.execute(
                "UPDATE wb_tasks SET state = 'dead' WHERE node_id = ?1",
                params![n2.id],
            )
            .unwrap();
            conn.execute("DELETE FROM wb_tasks WHERE node_id = ?1", params![n1.id])
                .unwrap();
        }
        s.wb_finalize_batch_if_terminal(&p, 44).unwrap();
        assert_eq!(
            state_of(&s, &p),
            ("active".into(), None),
            "winner's reference keeps the pack"
        );
        assert_eq!(s.get_node(n1.id).unwrap().unwrap().object_key.unwrap(), p);
    }

    /// Red-test 9 (deterministic race expression): two serialized paths
    /// taking the last reference converge to the same state regardless of
    /// order, and re-running the finalize is idempotent.
    #[test]
    fn p0a_last_ref_race_orders_converge_and_finalize_is_idempotent() {
        for cas_first in [true, false] {
            let s = store();
            let p = pk("race");
            seed_committing(&s, &p);
            let n = file_node(&s, "f", Some(&p));
            if cas_first {
                s.commit_content_cas(n.id, 0, 1, 42, "objects/x/v.data", "e")
                    .unwrap();
                s.delete_node(n.id).unwrap();
                assert_eq!(state_of(&s, &p), ("unreferenced".into(), Some(42)));
            } else {
                // Same two events, opposite serialization point: the delete
                // wins the race, the CAS then fails (node gone) and must not
                // change the pack.
                s.delete_node(n.id).unwrap();
                assert!(
                    s.commit_content_cas(n.id, 0, 1, 42, "objects/x/v.data", "e")
                        .is_err()
                );
                let (st, unref) = state_of(&s, &p);
                assert_eq!(st, "unreferenced");
                assert!(unref.is_some());
            }
            s.wb_finalize_batch_if_terminal(&p, 99).unwrap();
            let (_, unref_after) = state_of(&s, &p);
            assert!(
                unref_after.is_some() && unref_after != Some(99),
                "finalize must be idempotent (unref_since never reset)"
            );
        }
    }

    /// Retention boundary: candidates appear only after `unref_since` +
    /// window, and registration re-checks guards before flipping to
    /// `deleting` (red-tests 3-window + 8-registration-guard).
    #[test]
    fn p0a_retention_window_and_registration_guards() {
        let s = store();
        let p = pk("ret");
        seed_committing(&s, &p);
        let n = file_node(&s, "f", Some(&p));
        s.delete_node(n.id).unwrap(); // unref_since = wall-clock now
        assert!(
            s.gc_pack_candidates(0, 10).unwrap().is_empty(),
            "retention window not elapsed (unref_since is wall-clock, cutoff 0)"
        );
        let cands = s.gc_pack_candidates(i64::MAX, 10).unwrap();
        assert_eq!(cands.len(), 1, "candidate after the window");
        // Live reference reappears? Impossible through the API (a deleting
        // pack refuses refs), but a STILL-REFERENCED pack must never be a
        // candidate even past the window:
        let p2 = pk("ret-live");
        seed_committing(&s, &p2);
        file_node(&s, "live", Some(&p2));
        s.wb_finalize_batch_if_terminal(&p2, 0).unwrap();
        assert!(
            s.gc_pack_candidates(i64::MAX, 10)
                .unwrap()
                .iter()
                .all(|(k, _)| *k != p2)
        );
        // Registration flips to deleting; a second registration refuses.
        assert!(s.gc_register_pack_deletion(&p, 7).unwrap());
        assert_eq!(state_of(&s, &p).0, "deleting");
        assert!(!s.gc_register_pack_deletion(&p, 8).unwrap());
    }

    /// Durable deletion task for an orphan object uploaded by a dead task:
    /// live-referenced keys are refused, unreferenced keys enroll once.
    #[test]
    fn p0a_orphan_enrollment_guards() {
        let s = store();
        let staged = temp_path("orphan-stage");
        let n = file_node(&s, "f", None);
        s.wb_enqueue(n.id, &staged, 4, 1, 5).unwrap();
        {
            let conn = s.conn.lock().unwrap();
            conn.execute("UPDATE wb_tasks SET state = 'dead'", [])
                .unwrap();
            conn.execute(
                "UPDATE wb_tasks SET uploaded_key = ?1",
                params!["objects/x/w-dead.data"],
            )
            .unwrap();
        }
        // Guard: the key IS live-referenced now → not an orphan candidate.
        let live = file_node(&s, "g", Some("objects/x/w-dead.data"));
        assert!(s.dead_task_orphans().unwrap().is_empty());
        assert!(
            !s.gc_enqueue_orphan_object("objects/x/w-dead.data", 6)
                .unwrap()
        );
        s.delete_node(live.id).unwrap();
        assert_eq!(
            s.dead_task_orphans().unwrap(),
            vec!["objects/x/w-dead.data".to_string()]
        );
        assert!(
            s.gc_enqueue_orphan_object("objects/x/w-dead.data", 6)
                .unwrap()
        );
    }
}
