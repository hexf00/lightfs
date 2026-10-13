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
use crate::metadata::{MetadataStore, Node, NodeKind};

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
    UNIQUE(parent_id, name)
);
CREATE TABLE IF NOT EXISTS superseded_objects (
    object_key TEXT PRIMARY KEY,
    superseded_at INTEGER NOT NULL
);
"#;

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
    })
}

const NODE_COLS: &str =
    "id, parent_id, name, kind, size, mtime, mode, object_key, version, etag, target";

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
        let conn = self.conn.lock().unwrap();
        // Single-statement compare-and-swap: the WHERE clause re-checks the
        // version at commit time, so a writer that read the node before
        // another writer committed can never clobber it. rows==0 means the
        // version moved (or the node is gone); distinguish via a lookup.
        let n = conn.execute(
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
            let exists: Option<String> = conn
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
        tx.execute("DELETE FROM nodes WHERE id = ?1", params![target_id])?;
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
        let conn = self.conn.lock().unwrap();
        if id == 1 {
            return Err(Error::InvalidArgument("cannot delete root".into()));
        }
        let n = conn.execute("DELETE FROM nodes WHERE id = ?1", params![id])?;
        if n == 0 {
            return Err(Error::NotFound(format!("id {id}")));
        }
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
}
