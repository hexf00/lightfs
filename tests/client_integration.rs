//! Phase 7 integration: network client semantics — shared namespace through
//! the server, client-side buffered writes, TTL cache + invalidation.

use std::sync::Arc;
use std::time::Duration;

use lightfs::api::FsApi;
use lightfs::client::{Client, ClientConfig};
use lightfs::config::Config;
use lightfs::engine;
use lightfs::error::{Error, Result};

struct TestServer {
    url: String,
    db: String,
}

impl TestServer {
    fn start(db: &str) -> Result<Self> {
        let mut cfg = Config::load_or_defaults(None)?;
        cfg.metadata.path = std::env::temp_dir()
            .join(format!("lightfs-cli-{db}-{}.db", std::process::id()))
            .to_string_lossy()
            .to_string();
        let _ = std::fs::remove_file(&cfg.metadata.path);
        let eng = engine::open_engine_mem(&cfg)?;
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        std::thread::spawn(move || {
            let _ = lightfs::server::serve_on(server, eng);
        });
        Ok(TestServer {
            url: format!("http://127.0.0.1:{port}"),
            db: cfg.metadata.path,
        })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.db);
        let _ = std::fs::remove_file(format!("{}-wal", self.db));
        let _ = std::fs::remove_file(format!("{}-shm", self.db));
    }
}

fn client(url: &str, ttl: Option<Duration>) -> Arc<Client> {
    Arc::new(Client::new(ClientConfig {
        url: url.to_string(),
        metadata_ttl: ttl,
    }))
}

/// TEST_PLAN §4: A mkdir, B ls, A create, B reads, A renames, B sees rename.
#[test]
fn shared_namespace_two_clients() {
    let srv = TestServer::start("shared").unwrap();
    let a = client(&srv.url, None);
    let b = client(&srv.url, None);

    a.mkdir("/proj", 0o755).unwrap();
    let listing = b.readdir("/").unwrap();
    assert!(listing.iter().any(|n| n.name == "proj"));

    a.write_full("/proj/report", b"multi client data", true)
        .unwrap();
    let bytes = b.read("/proj/report", 0, 100).unwrap();
    assert_eq!(bytes, b"multi client data");

    a.rename("/proj/report", "/proj/report-v2").unwrap();
    let node = b.lookup("/proj/report-v2").unwrap();
    assert_eq!(node.size, 17);
    assert!(b.lookup("/proj/report").is_err());

    // B writes, A reads
    let mut ws = b.open_write("/proj/report-v2", None).unwrap();
    ws.write(0, b"B was here").unwrap();
    ws.commit().unwrap();
    assert_eq!(a.read("/proj/report-v2", 0, 100).unwrap(), b"B was here");

    a.unlink("/proj/report-v2").unwrap();
    a.rmdir("/proj").unwrap();
    assert!(b.lookup("/proj").is_err());
}

/// TEST_PLAN §5: concurrent write conflict is detected, not overwritten.
#[test]
fn optimistic_conflict_across_clients() {
    let srv = TestServer::start("conflict").unwrap();
    let a = client(&srv.url, None);
    let b = client(&srv.url, None);

    a.write_full("/f", b"version one data", true).unwrap();

    // Both observe version 2.
    let va = a.lookup("/f").unwrap().version;
    let vb = b.lookup("/f").unwrap().version;
    assert_eq!(va, vb);

    let mut wb = b.open_write("/f", Some(va)).unwrap();
    wb.write(0, b"writer B").unwrap();
    wb.commit().unwrap();

    let mut wa = a.open_write("/f", Some(va)).unwrap();
    wa.write(0, b"writer A stale").unwrap();
    let err = wa.commit().unwrap_err();
    assert!(matches!(err, lightfs::error::Error::Conflict { .. }));

    // B's write stands; A's did not silently overwrite it.
    assert_eq!(a.read("/f", 0, 100).unwrap(), b"writer B");
}

/// Metadata TTL cache: own mutations invalidate immediately, remote mutations
/// appear after the TTL, and disabling the cache always sees fresh state.
#[test]
fn metadata_ttl_cache_and_invalidation() {
    let srv = TestServer::start("cache").unwrap();
    let a = client(&srv.url, None);
    let ttl = Duration::from_millis(300);
    let b = client(&srv.url, Some(ttl));

    a.mkdir("/c", 0o755).unwrap();
    a.write_full("/c/x", b"data", true).unwrap();

    // B caches /c/x (size 4) and the /c listing (1 entry).
    assert_eq!(b.lookup("/c/x").unwrap().size, 4);
    assert_eq!(b.readdir("/c").unwrap().len(), 1);

    // Remote mutation on a path B has cached: within the TTL B may serve the
    // stale value (allowed), after the TTL it must see the new state.
    a.write_full("/c/x", b"longer data!!", true).unwrap();
    let within_ttl = b.lookup("/c/x").unwrap().size;
    assert!(
        within_ttl == 4 || within_ttl == 13,
        "either freshness is correct"
    );

    std::thread::sleep(ttl + Duration::from_millis(200));
    assert_eq!(
        b.lookup("/c/x").unwrap().size,
        13,
        "after TTL remote state must appear"
    );
    assert_eq!(b.readdir("/c").unwrap().len(), 1);

    // Own mutations are visible immediately even with cache on.
    b.mkdir("/c/z", 0o755).unwrap();
    let immediate = b.readdir("/c").unwrap();
    assert!(
        immediate.iter().any(|n| n.name == "z"),
        "own mutation bypasses TTL"
    );
}

/// The client implements FsApi, so the shell runs unmodified on top of it.
#[test]
fn shell_runs_over_client_backend() {
    let srv = TestServer::start("shell").unwrap();
    let fs = client(&srv.url, None);
    let shell = lightfs::shell::Shell::new(fs.clone());
    shell.exec("mkdir /shelled").unwrap();
    shell
        .exec("write /shelled/f --string over-the-wire")
        .unwrap();
    // cat streams to stdout directly; verify content through the API.
    assert_eq!(fs.read("/shelled/f", 0, 100).unwrap(), b"over-the-wire");
    let out = shell.exec("tree /").unwrap();
    assert!(out.iter().any(|l| l.contains("shelled")));
}

// ---------------------------------------------------------------------------
// Phase Q: the memory read cache in NETWORK mode. The embedded tests in
// src/readcache.rs prove the mechanics; this one proves the version check
// under the DEFAULT metadata TTL on the real client (Q3: "测试应覆盖默认
// 属性缓存设置,不能只靠将 TTL 设为 0"), plus identity-gone surfacing.
// ---------------------------------------------------------------------------

use lightfs::readcache::CacheFs;

/// The mount's default attribute-cache setting: the config default
/// (`cache.metadata.ttl_seconds = 5`), NOT zero — the version validation on
/// cache hits must not depend on disabling the TTL.
fn default_ttl() -> Option<Duration> {
    Some(Duration::from_secs(
        Config::load_or_defaults(None)
            .unwrap()
            .cache
            .metadata
            .ttl_seconds,
    ))
}

#[test]
fn read_cache_stays_authoritative_under_default_metadata_ttl() {
    let srv = TestServer::start("q3-cache").unwrap();
    let ttl = default_ttl();
    let a = CacheFs::new(client(&srv.url, ttl), "/hot".to_string(), 1 << 20);
    let b = client(&srv.url, ttl);

    use lightfs::api::FsApi;
    a.mkdir("/hot", 0o755).unwrap();
    a.create("/hot/f", 0o644).unwrap();
    let mut ws = a.open_write("/hot/f", None).unwrap();
    ws.write(0, b"version one").unwrap();
    ws.commit().unwrap();
    let id = a.lookup("/hot/f").unwrap().id;

    // A populates its read cache.
    assert_eq!(a.read_by_id(id, "/hot/f", 0, 100).unwrap(), b"version one");
    assert_eq!(a.cache().stats().misses, 1);

    // B (second client, default TTL) commits a new version.
    b.write_full("/hot/f", b"version two from B", false)
        .unwrap();

    // A's very next read must observe it: the hit validation is a LIVE
    // lookup (lookup_by_id never consults the MetaCache), the stale entry
    // is dropped, and B's bytes come from the authority.
    assert_eq!(
        a.read_by_id(id, "/hot/f", 0, 100).unwrap(),
        b"version two from B"
    );
    let s = a.cache().stats();
    assert_eq!(s.stale_skip, 1, "the version check caught the stale entry");
    assert_eq!(s.hits, 0, "nothing stale was served");

    // the fresh entry now hits, with B's bytes
    assert_eq!(
        a.read_by_id(id, "/hot/f", 0, 100).unwrap(),
        b"version two from B"
    );
    assert_eq!(a.cache().stats().hits, 1);

    // deleted identity: the error surfaces; cached bytes never resurrect it
    b.unlink("/hot/f").unwrap();
    assert!(
        a.read_by_id(id, "/hot/f", 0, 100).is_err(),
        "IDENTITY_GONE must not be served from the cache"
    );
    assert_eq!(a.cache().stats().hits, 1, "no hit happened for the dead id");
}

/// R4.2c (RPC): stable id allocation end to end — a deleted file's id is
/// never re-issued, so by-id reads through the old identity fail, a stale
/// session opened before the delete cannot commit, and the recreated file
/// is untouched (bytes, version).
#[test]
fn recycled_identity_rpc_old_sessions_cannot_touch_recreated_file() {
    let srv = TestServer::start("r4stable").unwrap();
    let a = client(&srv.url, None);

    // A at version 1; open a second session on it (expected version 1).
    a.write_full("/f", b"first bytes", true).unwrap();
    let node = a.lookup("/f").unwrap();
    let mut stale = a.open_write("/f", Some(node.version)).unwrap();
    stale.write(0, b"POISON").unwrap();

    // delete + recreate + one commit: the new file gets a FRESH id.
    a.unlink("/f").unwrap();
    a.write_full("/f", b"reborn bytes", true).unwrap();
    let reborn = a.lookup("/f").unwrap();
    assert_ne!(
        reborn.id, node.id,
        "AUTOINCREMENT never re-issues the deleted id"
    );

    // by-id read via the old identity is precisely IdentityGone over the
    // wire, never the new bytes
    assert!(
        matches!(
            a.read_by_id(node.id, "/f", 0, 64),
            Err(Error::IdentityGone(_))
        ),
        "the old identity must surface as IdentityGone on the client"
    );

    // the stale session's commit is rejected; the new file is untouched:
    // bytes, version, object key
    assert!(stale.commit().is_err(), "stale session must not commit");
    assert_eq!(a.read("/f", 0, 64).unwrap(), b"reborn bytes");
    let after = a.lookup("/f").unwrap();
    assert_eq!(after.version, 1, "recreated file untouched");
    assert_eq!(after.id, reborn.id);
    assert_eq!(after.object_key, reborn.object_key, "object key unchanged");
}
