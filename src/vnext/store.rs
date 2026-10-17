//! vnext metadata store: the SQLite execution layer of the commit/ref
//! model. Immutable rows (trees, file_versions, commits) are INSERT-only;
//! the ONLY mutable state is `refs` (and the `objects` ledger's delete
//! column handled by gc). Opening a database that is not a vnext store —
//! in particular a legacy v0.1 `nodes`-table database — is a LOUD
//! refusal before any write: vnext performs no migration and never
//! clears user data.

use std::collections::HashSet;
use std::sync::Mutex;

use rusqlite::Connection;

use crate::error::{Error, Result};

pub const MODEL_MARKER: &str = "vnext-1";

/// One serialized directory entry inside an immutable tree row.
/// JSON shape (serde): name/kind always present; the rest optional and
/// omitted when absent. A FILE entry is self-contained for disaster
/// recovery: it carries its stable identity (`fid`), content version
/// (`fv`) and the immutable object reference (`o` + `e`), so a tree
/// object copied out of band can locate file bytes without SQLite.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    pub n: String, // name (single component)
    pub k: String, // "dir" | "file" | "symlink"
    #[serde(default)]
    pub m: u32, // mode
    #[serde(default)]
    pub s: i64, // size
    #[serde(default)]
    pub mt: i64, // mtime (unix secs)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tid: Option<i64>, // dir: child tree row id (execution layer only)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub w: Option<String>, // dir: child tree content hash (recovery anchor) (execution-layer id)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fid: Option<i64>, // file: stable file identity
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fv: Option<i64>, // file: content version
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub o: Option<String>, // file: immutable object key
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub e: Option<String>, // file: object etag
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tgt: Option<String>, // symlink target (verbatim)
}

impl Entry {
    pub fn kind(&self) -> &str {
        &self.k
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeadRow {
    pub commit_id: i64,
    pub seq: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitRow {
    pub id: i64,
    pub parent_id: Option<i64>,
    pub root_tree_id: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileVersionRow {
    pub file_id: i64,
    pub version: i64,
    pub size: i64,
    pub mtime: i64,
    pub mode: u32,
    pub object_key: String,
    pub etag: String,
}

pub struct Store {
    conn: Mutex<Connection>,
}

fn db_err(e: rusqlite::Error) -> Error {
    Error::Db(format!("vnext store: {e}"))
}

const LEGACY_TABLES: &[&str] = &["nodes", "wb_tasks", "wb_capacity", "packs", "superseded_objects"];
const OWN_TABLES: &[&str] = &[
    "model", "trees", "files", "file_versions", "commits", "refs", "objects",
];

impl Store {
    /// Open (or initialize) a vnext store. Non-vnext databases — legacy
    /// v0.1 stores, foreign files, garbage — are refused WITHOUT any
    /// write, per the freeze contract: no migration, no clearing.
    pub fn open(path: &str) -> Result<Store> {
        let conn = Connection::open(path).map_err(db_err)?;
        Self::init_conn(conn)
    }

    /// Network-free store for tests. The file-based open path shares all
    /// of the contract logic through `init_conn`.
    pub fn open_in_memory() -> Result<Store> {
        let conn = Connection::open_in_memory().map_err(db_err)?;
        Self::init_conn(conn)
    }

    fn init_conn(conn: Connection) -> Result<Store> {
        conn.pragma_update(None, "journal_mode", "WAL").map_err(db_err)?;
        conn.pragma_update(None, "synchronous", "FULL").map_err(db_err)?;
        // Classify BEFORE any DDL: creating our tables first would make
        // every foreign database look like "tables without a marker" and
        // every fresh file look non-empty.
        let state = Self::classify(&conn)?;
        match state {
            Classify::Empty | Classify::HalfInitialized => {
                Self::create_schema(&conn)?;
                Self::initialize(conn)
            }
            Classify::Vnext => Ok(Store { conn: Mutex::new(conn) }),
            Classify::Foreign(desc) => Err(Error::Db(format!(
                "refusing to open {desc}: this is not a vnext metadata store \
                 (vnext performs no migration and never clears or overwrites \
                 existing data; initialize a fresh store instead)"
            ))),
        }
    }

    fn create_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS model (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS trees (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                entries_json TEXT NOT NULL,
                content_sha  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT
            );
            CREATE TABLE IF NOT EXISTS file_versions (
                file_id    INTEGER NOT NULL,
                version    INTEGER NOT NULL,
                size       INTEGER NOT NULL,
                mtime      INTEGER NOT NULL,
                mode       INTEGER NOT NULL,
                object_key TEXT NOT NULL,
                etag       TEXT NOT NULL,
                PRIMARY KEY (file_id, version)
            );
            CREATE TABLE IF NOT EXISTS commits (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                parent_id    INTEGER,
                root_tree_id INTEGER NOT NULL,
                created_at   INTEGER NOT NULL,
                content_sha  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS refs (
                name      TEXT PRIMARY KEY,
                commit_id INTEGER NOT NULL,
                seq       INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS objects (
                key           TEXT PRIMARY KEY,
                registered_at INTEGER NOT NULL
            );
            "#,
        )
        .map_err(db_err)?;
        Ok(())
    }

    fn classify(conn: &Connection) -> Result<Classify> {
        let tables: Vec<String> = {
            let mut stmt = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                .map_err(db_err)?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0)).map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db_err)?
        };
        let user_tables: Vec<&str> = tables.iter().map(|s| s.as_str()).collect();
        if user_tables.is_empty() {
            return Ok(Classify::Empty);
        }
        let marker: Option<String> = conn
            .query_row(
                "SELECT value FROM model WHERE key='model'",
                [],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match &e {
                // No marker ROW and no marker TABLE both mean "not
                // initialized as vnext" — the difference between foreign
                // and empty/half-initialized is drawn from the table set.
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                rusqlite::Error::SqliteFailure(err, _)
                    if err.code == rusqlite::ErrorCode::NotADatabase
                        || format!("{e}").contains("no such table") =>
                {
                    Ok(None)
                }
                _ => Err(db_err(e)),
            })?;
        match marker.as_deref() {
            Some(MODEL_MARKER) => Ok(Classify::Vnext),
            Some(other) => Ok(Classify::Foreign(format!(
                "a vnext store with unknown model marker {other:?}"
            ))),
            None => {
                if user_tables.iter().any(|t| LEGACY_TABLES.contains(t)) {
                    return Ok(Classify::Foreign(
                        "a legacy v0.1.x database (nodes/write-back/pack schema)".into(),
                    ));
                }
                // A torn first-open may have our CREATE batch applied but
                // no marker row. Initializing is safe ONLY when the set
                // of tables is exactly ours AND all data tables are
                // empty — anything else is foreign and refused.
                let ours: HashSet<&str> = OWN_TABLES.iter().copied().collect();
                let present: HashSet<&str> = user_tables.into_iter().collect();
                if present != ours {
                    return Ok(Classify::Foreign(
                        "a foreign or malformed database (tables but no model marker)".into(),
                    ));
                }
                let empty = conn
                    .query_row(
                        "SELECT (SELECT COUNT(*) FROM trees) + (SELECT COUNT(*) FROM commits)
                              + (SELECT COUNT(*) FROM refs) + (SELECT COUNT(*) FROM files)
                              + (SELECT COUNT(*) FROM file_versions) + (SELECT COUNT(*) FROM objects)",
                        [],
                        |r| r.get::<_, i64>(0),
                    )
                    .map_err(db_err)?;
                if empty == 0 {
                    Ok(Classify::HalfInitialized)
                } else {
                    Ok(Classify::Foreign(
                        "a database with vnext-shaped tables, a non-empty data set and no model marker"
                            .into(),
                    ))
                }
            }
        }
    }

    fn initialize(conn: Connection) -> Result<Store> {
        conn.execute(
            "INSERT INTO model(key, value) VALUES ('model', ?1)",
            [MODEL_MARKER],
        )
        .map_err(db_err)?;
        let store = Store { conn: Mutex::new(conn) };
        let (root_tree, root_sha) = store.insert_tree(&[])?;
        store.with_tx(|conn| {
            let record = serde_json::json!({
                "ref": "main",
                "parent": serde_json::Value::Null,
                "root_tree": root_sha,
                "seq": 0,
            });
            let record_json = record.to_string().into_bytes();
            let sha = content_sha(&record_json);
            conn.execute(
                "INSERT INTO commits(parent_id, root_tree_id, created_at, content_sha) VALUES (NULL, ?1, ?2, ?3)",
                rusqlite::params![root_tree, now_secs(), sha],
            )
            .map_err(db_err)?;
            let commit_id = conn.last_insert_rowid();
            conn.execute(
                "INSERT INTO refs(name, commit_id, seq) VALUES ('main', ?1, 0)",
                [commit_id],
            )
            .map_err(db_err)?;
            Ok(())
        })?;
        Ok(store)
    }

    // ---- primitive row operations (callers own transaction boundaries
    // through `with_tx`; read paths take the connection lock directly) ----

    pub(crate) fn with_tx<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").map_err(db_err)?;
        match f(&conn) {
            Ok(v) => {
                conn.execute_batch("COMMIT").map_err(db_err)?;
                Ok(v)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    pub(crate) fn read<T>(
        &self,
        f: impl FnOnce(&Connection) -> Result<T>,
    ) -> Result<T> {
        let conn = self.conn.lock().unwrap();
        f(&conn)
    }

    pub fn insert_tree(&self, entries: &[Entry]) -> Result<(i64, String)> {
        let json = serde_json::to_string(entries)
            .map_err(|e| Error::Db(format!("serialize tree: {e}")))?;
        let sha = content_sha(json.as_bytes());
        self.read(|conn| {
            conn.execute(
                "INSERT INTO trees(entries_json, content_sha) VALUES (?1, ?2)",
                rusqlite::params![json, sha],
            )
            .map_err(db_err)?;
            Ok((conn.last_insert_rowid(), sha))
        })
    }

    /// Insert a tree INSIDE an open transaction (COW path).
    pub fn insert_tree_tx(conn: &Connection, entries: &[Entry]) -> Result<(i64, String)> {
        let json = serde_json::to_string(entries)
            .map_err(|e| Error::Db(format!("serialize tree: {e}")))?;
        let sha = content_sha(json.as_bytes());
        conn.execute(
            "INSERT INTO trees(entries_json, content_sha) VALUES (?1, ?2)",
            rusqlite::params![json, sha],
        )
        .map_err(db_err)?;
        Ok((conn.last_insert_rowid(), sha))
    }

    /// Insert a tree INSIDE an open transaction from pre-serialized JSON
    /// (the COW path serializes each level exactly once and needs the
    /// content hash for the recovery copy).
    pub fn insert_tree_json_tx(conn: &Connection, json: &str) -> Result<(i64, String)> {
        let sha = content_sha(json.as_bytes());
        conn.execute(
            "INSERT INTO trees(entries_json, content_sha) VALUES (?1, ?2)",
            rusqlite::params![json, sha],
        )
        .map_err(db_err)?;
        Ok((conn.last_insert_rowid(), sha))
    }

    pub fn get_tree(&self, id: i64) -> Result<Vec<Entry>> {
        self.read(|conn| Self::get_tree_tx(conn, id))
    }

    pub fn get_tree_tx(conn: &Connection, id: i64) -> Result<Vec<Entry>> {
        let json: String = conn
            .query_row("SELECT entries_json FROM trees WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    Error::Db(format!("vnext store: tree {id} missing (corrupt commit chain)"))
                }
                other => db_err(other),
            })?;
        serde_json::from_str(&json).map_err(|e| Error::Db(format!("deserialize tree {id}: {e}")))
    }

    pub fn get_commit(&self, id: i64) -> Result<CommitRow> {
        self.read(|conn| {
            conn.query_row(
                "SELECT id, parent_id, root_tree_id, created_at FROM commits WHERE id=?1",
                [id],
                |r| {
                    Ok(CommitRow {
                        id: r.get(0)?,
                        parent_id: r.get(1)?,
                        root_tree_id: r.get(2)?,
                        created_at: r.get(3)?,
                    })
                },
            )
            .map_err(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => {
                    Error::Db(format!("vnext store: commit {id} missing (dangling ref?)"))
                }
                other => db_err(other),
            })
        })
    }

    /// Same read on a CALLER-HELD connection (inside with_tx).
    pub fn get_commit_tx(conn: &Connection, id: i64) -> Result<CommitRow> {
        conn.query_row(
            "SELECT id, parent_id, root_tree_id, created_at FROM commits WHERE id=?1",
            [id],
            |r| {
                Ok(CommitRow {
                    id: r.get(0)?,
                    parent_id: r.get(1)?,
                    root_tree_id: r.get(2)?,
                    created_at: r.get(3)?,
                })
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                Error::Db(format!("vnext store: commit {id} missing (dangling ref?)"))
            }
            other => db_err(other),
        })
    }

    pub fn head(&self, name: &str) -> Result<Option<HeadRow>> {
        self.read(|conn| {
            conn.query_row(
                "SELECT commit_id, seq FROM refs WHERE name=?1",
                [name],
                |r| {
                    Ok(HeadRow {
                        commit_id: r.get(0)?,
                        seq: r.get(1)?,
                    })
                },
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(db_err(other)),
            })
        })
    }

    pub fn head_tx(conn: &Connection, name: &str) -> Result<Option<HeadRow>> {
        conn.query_row(
            "SELECT commit_id, seq FROM refs WHERE name=?1",
            [name],
            |r| {
                Ok(HeadRow {
                    commit_id: r.get(0)?,
                    seq: r.get(1)?,
                })
            },
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(db_err(other)),
        })
    }

    /// Append a commit and advance the ref, inside ONE transaction: the
    /// parent is read from the ref at execution time (single-writer +
    /// BEGIN IMMEDIATE => serialized, no lost commits), so concurrent
    /// commits to DISJOINT files chain cleanly. File-level concurrency is
    /// detected by the (file_id, version) primary key, not the ref.
    /// Returns the row id, new seq, and the CANONICAL RECOVERY RECORD
    /// (json bytes + its sha) — the exact object to copy to
    /// `meta/commits/<sha>` after COMMIT.
    #[allow(clippy::type_complexity)]
    pub fn commit_mainline_tx(
        conn: &Connection,
        ref_name: &str,
        root_tree_id: i64,
        root_tree_sha: &str,
    ) -> Result<(i64, i64, Vec<u8>, String)> {
        let head = Self::head_tx(conn, ref_name)?
            .ok_or_else(|| Error::Db(format!("vnext store: ref {ref_name:?} vanished")))?;
        let parent_sha: Option<String> = conn
            .query_row(
                "SELECT content_sha FROM commits WHERE id=?1",
                [head.commit_id],
                |r| r.get(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(db_err(other)),
            })?;
        let record = serde_json::json!({
            "ref": ref_name,
            "parent": parent_sha,
            "root_tree": root_tree_sha,
            "seq": head.seq + 1,
        });
        let record_json = record.to_string().into_bytes();
        let sha = content_sha(&record_json);
        conn.execute(
            "INSERT INTO commits(parent_id, root_tree_id, created_at, content_sha) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![head.commit_id, root_tree_id, now_secs(), sha],
        )
        .map_err(db_err)?;
        let commit_id = conn.last_insert_rowid();
        conn.execute(
            "UPDATE refs SET commit_id=?1, seq=seq+1 WHERE name=?2 AND commit_id=?3",
            rusqlite::params![commit_id, ref_name, head.commit_id],
        )
        .map_err(db_err)?;
        if conn.changes() != 1 {
            return Err(Error::Db(format!(
                "vnext store: ref {ref_name:?} changed inside its own commit transaction"
            )));
        }
        Ok((commit_id, head.seq + 1, record_json, sha))
    }

    /// Explicit branch-head compare-and-swap. Returns the new seq;
    /// `expected_commit` mismatch (someone else advanced the head) is an
    /// explicit Conflict — never a silent overwrite.
    pub fn advance_ref(
        &self,
        name: &str,
        expected_commit: i64,
        new_commit: i64,
    ) -> Result<i64> {
        self.with_tx(|conn| {
            conn.execute(
                "UPDATE refs SET commit_id=?1, seq=seq+1 WHERE name=?2 AND commit_id=?3",
                rusqlite::params![new_commit, name, expected_commit],
            )
            .map_err(db_err)?;
            if conn.changes() == 0 {
                let actual = Self::head_tx(conn, name)?
                    .map(|h| h.commit_id)
                    .unwrap_or(-1);
                return Err(Error::Conflict {
                    path: format!("ref:{name}"),
                    expected: expected_commit,
                    actual,
                });
            }
            Ok(Self::head_tx(conn, name)?.map(|h| h.seq).unwrap_or(0))
        })
    }

    /// Create a new ref pointing at an existing commit (branch creation:
    /// zero data copied). A duplicate name is AlreadyExists.
    pub fn create_ref(&self, name: &str, commit_id: i64) -> Result<()> {
        // The commit must exist: a ref to a nonexistent commit is a
        // corrupt ref by construction, refuse it at creation.
        self.get_commit(commit_id)?;
        self.with_tx(|conn| {
            conn.execute(
                "INSERT INTO refs(name, commit_id, seq) VALUES (?1, ?2, 0)",
                rusqlite::params![name, commit_id],
            )
            .map_err(|e| match &e {
                rusqlite::Error::SqliteFailure(err, _)
                    if err.code == rusqlite::ErrorCode::ConstraintViolation =>
                {
                    Error::AlreadyExists(format!("ref {name:?} already exists"))
                }
                _ => db_err(e),
            })?;
            Ok(())
        })
    }

    /// Allocate a stable file identity (AUTOINCREMENT; never reused).
    pub fn alloc_file_id_tx(conn: &Connection) -> Result<i64> {
        conn.execute("INSERT INTO files DEFAULT VALUES", [])
            .map_err(db_err)?;
        Ok(conn.last_insert_rowid())
    }

    /// Insert a file version INSIDE the commit transaction. A duplicate
    /// (file_id, version) means a concurrent writer committed first —
    /// surfaced as an explicit Conflict with the actual version.
    pub fn insert_file_version_tx(conn: &Connection, v: &FileVersionRow) -> Result<()> {
        let inserted = conn
            .execute(
                "INSERT INTO file_versions(file_id, version, size, mtime, mode, object_key, etag)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![v.file_id, v.version, v.size, v.mtime, v.mode, v.object_key, v.etag],
            )
            .map_err(|e| match &e {
                rusqlite::Error::SqliteFailure(err, _)
                    if err.code == rusqlite::ErrorCode::ConstraintViolation => {
                    // Someone committed a newer version of this file
                    // first; read the actual head version for the error.
                    Error::Conflict {
                        path: format!("file:{}", v.file_id),
                        expected: v.version - 1,
                        actual: conn
                            .query_row(
                                "SELECT COALESCE(MAX(version),0) FROM file_versions WHERE file_id=?1",
                                [v.file_id],
                                |r| r.get(0),
                            )
                            .unwrap_or(0),
                    }
                }
                _ => db_err(e),
            })?;
        debug_assert_eq!(inserted, 1);
        Ok(())
    }

    pub fn get_file_version(&self, file_id: i64, version: i64) -> Result<FileVersionRow> {
        self.read(|conn| Self::get_file_version_tx(conn, file_id, version))
    }

    pub fn get_file_version_tx(
        conn: &Connection,
        file_id: i64,
        version: i64,
    ) -> Result<FileVersionRow> {
        conn.query_row(
            "SELECT file_id, version, size, mtime, mode, object_key, etag
             FROM file_versions WHERE file_id=?1 AND version=?2",
            rusqlite::params![file_id, version],
            |r| {
                Ok(FileVersionRow {
                    file_id: r.get(0)?,
                    version: r.get(1)?,
                    size: r.get(2)?,
                    mtime: r.get(3)?,
                    mode: r.get(4)?,
                    object_key: r.get(5)?,
                    etag: r.get(6)?,
                })
            },
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => {
                Error::ObjectMissing(format!("file version ({file_id},{version})"))
            }
            other => db_err(other),
        })
    }

    /// Ledger an object BEFORE/AFTER its PUT: a row here means "this key
    /// was written under our authority and must be considered for gc".
    /// Crash windows can leave ledger rows for objects no commit
    /// references — exactly what reachability gc collects.
    pub fn ledger_object(&self, key: &str) -> Result<()> {
        self.read(|conn| {
            conn.execute(
                "INSERT OR IGNORE INTO objects(key, registered_at) VALUES (?1, ?2)",
                rusqlite::params![key, now_secs()],
            )
            .map_err(db_err)?;
            Ok(())
        })
    }

    pub fn ledger_object_tx(conn: &Connection, key: &str) -> Result<()> {
        conn.execute(
            "INSERT OR IGNORE INTO objects(key, registered_at) VALUES (?1, ?2)",
            rusqlite::params![key, now_secs()],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn forget_object(&self, key: &str) -> Result<()> {
        self.read(|conn| {
            conn.execute("DELETE FROM objects WHERE key=?1", [key])
                .map_err(db_err)?;
            Ok(())
        })
    }

    pub fn known_objects(&self) -> Result<Vec<String>> {
        self.read(|conn| {
            let mut stmt = conn
                .prepare("SELECT key FROM objects ORDER BY key")
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |r| r.get::<_, String>(0))
                .map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db_err)
        })
    }

    /// All commits reachable from ANY ref (walks parent links). These are
    /// the gc roots' full history set: every commit a ref can ever fold.
    pub fn reachable_commits(&self) -> Result<Vec<i64>> {
        self.read(|conn| {
            let mut stmt = conn
                .prepare(
                    "WITH RECURSIVE live(id) AS (
                        SELECT commit_id FROM refs
                        UNION
                        SELECT c.parent_id FROM commits c JOIN live ON c.id = live.id
                        WHERE c.parent_id IS NOT NULL
                     )
                     SELECT DISTINCT id FROM live",
                )
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |r| r.get::<_, i64>(0))
                .map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db_err)
        })
    }

    /// Every ref with its head (gc roots + recovery listing).
    pub fn all_refs(&self) -> Result<Vec<(String, HeadRow)>> {
        self.read(|conn| {
            let mut stmt = conn
                .prepare("SELECT name, commit_id, seq FROM refs ORDER BY name")
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        HeadRow { commit_id: r.get(1)?, seq: r.get(2)? },
                    ))
                })
                .map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db_err)
        })
    }

    /// Every commit row (for gc's full reachability walk: under the
    /// single-writer discipline every commit descends from some ref).
    pub fn all_commits(&self) -> Result<Vec<CommitRow>> {
        self.read(|conn| {
            let mut stmt = conn
                .prepare("SELECT id, parent_id, root_tree_id, created_at FROM commits ORDER BY id")
                .map_err(db_err)?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(CommitRow {
                        id: r.get(0)?,
                        parent_id: r.get(1)?,
                        root_tree_id: r.get(2)?,
                        created_at: r.get(3)?,
                    })
                })
                .map_err(db_err)?;
            rows.collect::<std::result::Result<Vec<_>, _>>().map_err(db_err)
        })
    }

    /// Fail-closed gate for destructive maintenance (gc): the store must
    /// be a vnext model with at least one readable ref. Running gc with
    /// a lost or unreadable ref set would treat reachable objects as
    /// garbage — refuse instead.
    /// The namespace id this database owns (None until initialized).
    /// Lives in the existing `model` table: no schema change, and the
    /// ownership marker on the object store must carry the SAME id.
    pub fn namespace_id(&self) -> Result<Option<String>> {
        self.read(|conn| {
            conn.query_row(
                "SELECT value FROM model WHERE key='namespace'",
                [],
                |r| r.get::<_, String>(0),
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(db_err(other)),
            })
        })
    }

    pub fn set_namespace_id_tx(conn: &Connection, id: &str) -> Result<()> {
        conn.execute(
            "INSERT INTO model(key, value) VALUES ('namespace', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [id],
        )
        .map_err(db_err)?;
        Ok(())
    }

    pub fn gc_gate(&self) -> Result<()> {
        let marker_ok = self.read(|conn| {
            conn.query_row::<String, _, _>(
                "SELECT value FROM model WHERE key='model'",
                [],
                |r| r.get(0),
            )
            .map(|v| v == MODEL_MARKER)
            .map_err(db_err)
        })?;
        if !marker_ok {
            return Err(Error::Db("gc refused: model marker missing/mismatched".into()));
        }
        let refs = self.all_refs()?;
        if refs.is_empty() {
            return Err(Error::Db(
                "gc refused: no refs are readable — reachability cannot be proven, \
                 deleting anything would be guesswork"
                    .into(),
            ));
        }
        Ok(())
    }
}

enum Classify {
    Empty,
    /// Our CREATE batch applied but the marker row never landed (torn
    /// first open). Safe to initialize ONLY because every data table is
    /// empty — no user data can be lost.
    HalfInitialized,
    Vnext,
    Foreign(String),
}

/// Content hash (hex sha256) — the identity of tree/commit recovery
/// objects under `meta/…` in the object store.
pub(crate) fn content_sha(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(data))
}

fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}
