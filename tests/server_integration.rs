//! Phase 6 integration: server transport. Binds a real tiny_http server on an
//! ephemeral port and drives the JSON protocol the way remote clients do.

use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

/// Serialize the /data/write staging-hygiene tests: their temp-dir scan is
/// global, so concurrent in-flight staging files from sibling tests would
/// trip the assertion (tests in one binary run in parallel by default).
static DATA_WRITE_SERIAL: Mutex<()> = Mutex::new(());

use lightfs::config::Config;
use lightfs::engine;
use lightfs::error::Result;

fn mem_config(db: &str) -> Result<Config> {
    let mut cfg = Config::load_or_defaults(None)?;
    cfg.metadata.path = std::env::temp_dir()
        .join(format!("lightfs-srv-{db}-{}.db", std::process::id()))
        .to_string_lossy()
        .to_string();
    Ok(cfg)
}

struct TestServer {
    url: String,
}

impl TestServer {
    fn start(db: &str) -> Result<Self> {
        let cfg = mem_config(db)?;
        let _ = std::fs::remove_file(&cfg.metadata.path);
        let eng = engine::open_embedded_mem(&cfg)?;
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let port = server.server_addr().to_ip().unwrap().port();
        std::thread::spawn(move || {
            let _ = lightfs::server::serve_on(server, eng);
        });
        Ok(TestServer {
            url: format!("http://127.0.0.1:{port}"),
        })
    }

    fn rpc(&self, method: &str, params: Value) -> (bool, Value) {
        self.rpc_raw(json!({"id": 1, "method": method, "params": params}))
    }

    fn rpc_raw(&self, body: Value) -> (bool, Value) {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(std::time::Duration::from_secs(30)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        let mut resp = agent
            .post(format!("{}/rpc", self.url))
            .header("Content-Type", "application/json")
            .send(body.to_string().as_bytes())
            .expect("rpc transport");
        let text = resp.body_mut().read_to_string().expect("rpc body");
        let v: Value = serde_json::from_str(&text).expect("rpc json");
        let ok = v["ok"].as_bool().unwrap();
        (
            ok,
            if ok {
                v["result"].clone()
            } else {
                v["error"].clone()
            },
        )
    }

    fn health(&self) -> u16 {
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .new_agent();
        agent
            .get(format!("{}/health", self.url))
            .call()
            .unwrap()
            .status()
            .as_u16()
    }
}

fn b64(d: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(d)
}

fn unb64(s: &str) -> Vec<u8> {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

#[test]
fn server_health_and_protocol() {
    let srv = TestServer::start("proto").unwrap();
    assert_eq!(srv.health(), 200);

    let (ok, _) = srv.rpc("ping", json!({}));
    assert!(ok);
    let (ok, r) = srv.rpc("mkdir", json!({"path": "/shared"}));
    assert!(ok, "{r}");
    let (ok, r) = srv.rpc(
        "write",
        json!({"path": "/shared/data", "data": b64(b"server bytes")}),
    );
    assert!(ok, "{r}");
    assert_eq!(r["size"], 12);
    let (ok, r) = srv.rpc(
        "read",
        json!({"path": "/shared/data", "offset": 7, "size": 5}),
    );
    assert!(ok);
    assert_eq!(unb64(r["data"].as_str().unwrap()), b"bytes");
    // getattr alias
    let (ok, r) = srv.rpc("getattr", json!({"path": "/shared/data"}));
    assert!(ok);
    // create_if_missing is metadata-only (v0, no object commit - B-1),
    // then this write is the first content commit (v1)
    assert_eq!(r["version"], 1);
    // structured error
    let (ok, _) = srv.rpc("unlink", json!({"path": "/shared/data"}));
    assert!(ok);
    let (ok, e) = srv.rpc(
        "read",
        json!({"path": "/shared/data", "offset": 0, "size": 1}),
    );
    assert!(!ok);
    assert_eq!(e["code"], "NOT_FOUND");
}

#[test]
fn server_handles_concurrent_clients() {
    let srv = Arc::new(TestServer::start("conc").unwrap());
    srv.rpc("mkdir", json!({"path": "/c"}));
    let mut joins = Vec::new();
    for i in 0..6 {
        let srv = srv.clone();
        joins.push(std::thread::spawn(move || {
            let path = format!("/c/f{i}");
            let (ok, _) = srv.rpc(
                "write",
                json!({"path": path, "data": b64(format!("payload-{i}").as_bytes())}),
            );
            assert!(ok);
            let (ok, r) = srv.rpc("read", json!({"path": path, "offset": 0, "size": 100}));
            assert!(ok);
            assert_eq!(
                unb64(r["data"].as_str().unwrap()),
                format!("payload-{i}").as_bytes()
            );
        }));
    }
    for j in joins {
        j.join().unwrap();
    }
    let (ok, r) = srv.rpc("readdir", json!({"path": "/c"}));
    assert!(ok);
    assert_eq!(r["nodes"].as_array().unwrap().len(), 6);
    let (ok, m) = srv.rpc("metrics", json!({}));
    assert!(ok);
    assert!(m["s3_put"].as_u64().unwrap() >= 6);
}

/// Binary data endpoints: exact raw bytes (no base64 inflation), correct
/// content type, and structured error mapping on the same path.
#[test]
fn binary_data_endpoints() {
    let _serial = DATA_WRITE_SERIAL.lock().unwrap();
    let srv = TestServer::start("bindata").unwrap();
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(60)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let url = srv.url.clone();

    // write 3 MiB of patterned bytes via /data/write (raw body)
    assert!(srv.rpc("mkdir", json!({"path": "/big"})).0);
    let payload: Vec<u8> = (0..3 * 1024 * 1024u64).map(|b| (b % 251) as u8).collect();
    let mut resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/big/blob.bin")
        .header("X-LightFS-Create", "1")
        .header("X-LightFS-Length", payload.len().to_string())
        .send(payload.as_slice())
        .unwrap();
    assert_eq!(
        resp.status().as_u16(),
        200,
        "write should succeed: {:?}",
        resp.body_mut().read_to_string().unwrap_or_default()
    );
    let node: Value = serde_json::from_str(&resp.body_mut().read_to_string().unwrap()).unwrap();
    assert_eq!(node["size"], payload.len() as i64);

    // read a middle slice via /data/read: exact bytes, octet-stream
    let mut resp = agent
        .post(format!("{url}/data/read"))
        .header("Content-Type", "application/json")
        .send(r#"{"path":"/big/blob.bin","offset":1048576,"size":1024}"#.as_bytes())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/octet-stream")
    );
    let bytes = resp.body_mut().read_to_vec().unwrap();
    assert_eq!(bytes.len(), 1024);
    assert_eq!(&bytes[..], &payload[1048576..1048576 + 1024]);

    // full read returns everything
    let mut resp = agent
        .post(format!("{url}/data/read"))
        .header("Content-Type", "application/json")
        .send(r#"{"path":"/big/blob.bin","offset":0,"size":99999999}"#.as_bytes())
        .unwrap();
    let bytes = resp.body_mut().read_to_vec().unwrap();
    assert_eq!(bytes.len(), payload.len());
    assert_eq!(bytes, payload);

    // structured error on missing file: 404 + code NOT_FOUND
    let mut resp = agent
        .post(format!("{url}/data/read"))
        .header("Content-Type", "application/json")
        .send(r#"{"path":"/big/missing","offset":0,"size":10}"#.as_bytes())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 404);
    let v: Value = serde_json::from_str(&resp.body_mut().read_to_string().unwrap()).unwrap();
    assert_eq!(v["error"]["code"], "NOT_FOUND");
}

/// Streaming acceptance: a 16 MiB write_from_reader + read_stream roundtrip
/// must preserve exact bytes; a range stream must slice exactly; CAS
/// conflict on a streaming write must still be detected.
#[test]
fn streaming_roundtrip_and_conflict() {
    let _serial = DATA_WRITE_SERIAL.lock().unwrap();
    let srv = TestServer::start("stream").unwrap();
    let cfg = Config::load_or_defaults(None).unwrap();
    let client = lightfs::client::Client::new(lightfs::client::ClientConfig {
        url: srv.url.clone(),
        metadata_ttl: None,
    });
    use lightfs::api::FsApi;
    let _ = cfg;

    client.mkdir("/streamed", 0o755).unwrap();
    let size = 16 * 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|b| (b % 253) as u8).collect();

    // streaming write
    let node = client
        .write_from_reader(
            "/streamed/big.bin",
            &mut std::io::Cursor::new(payload.clone()),
            size as u64,
            None,
            true,
        )
        .unwrap();
    assert_eq!(node.size, size as i64);

    // streaming full read: exact bytes
    let (mut reader, len) = client
        .read_stream("/streamed/big.bin", 0, size as u64)
        .unwrap();
    assert_eq!(len, size as u64);
    let mut got = Vec::new();
    std::io::copy(&mut reader, &mut got).unwrap();
    assert_eq!(got.len(), size);
    assert_eq!(got, payload);

    // streaming range read: exact slice
    let (mut reader, len) = client.read_stream("/streamed/big.bin", 1024, 4096).unwrap();
    assert_eq!(len, 4096);
    let mut got = Vec::new();
    std::io::copy(&mut reader, &mut got).unwrap();
    assert_eq!(got, &payload[1024..1024 + 4096]);

    // optimistic conflict still detected on the streaming write path
    let v = client.lookup("/streamed/big.bin").unwrap().version;
    let junk = vec![0u8; 1024];
    let _ = client
        .write_from_reader(
            "/streamed/big.bin",
            &mut std::io::Cursor::new(junk.clone()),
            1024,
            Some(v),
            false,
        )
        .unwrap();
    let err = client
        .write_from_reader(
            "/streamed/big.bin",
            &mut std::io::Cursor::new(junk),
            1024,
            Some(v),
            false,
        )
        .unwrap_err();
    assert!(matches!(err, lightfs::error::Error::Conflict { .. }));

    client.unlink("/streamed/big.bin").unwrap();
    client.rmdir("/streamed").unwrap();
}

/// Review P1 regression: a declared-length mismatch or a missing length
/// header must be rejected BEFORE anything is committed — the file's
/// content and version stay exactly as they were.
#[test]
fn write_length_mismatch_never_commits() {
    let _serial = DATA_WRITE_SERIAL.lock().unwrap();
    let srv = TestServer::start("lenmis").unwrap();
    let agent = ureq::Agent::config_builder()
        .timeout_global(Some(std::time::Duration::from_secs(60)))
        .http_status_as_error(false)
        .build()
        .new_agent();
    let url = srv.url.clone();

    assert!(srv.rpc("mkdir", json!({ "path": "/len" })).0);
    // snapshot: staging files created before this test are not its concern
    let staging_snapshot = std::time::SystemTime::now();
    let payload = b"authoritative-content";
    let mut resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/len/f")
        .header("X-LightFS-Create", "1")
        .header("X-LightFS-Length", payload.len().to_string())
        .send(payload.as_slice())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    let v0: Value = serde_json::from_str(&resp.body_mut().read_to_string().unwrap()).unwrap();
    let version0 = v0["version"].as_i64().unwrap();

    // mismatched declared length: rejected with 400...
    let resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/len/f")
        .header("X-LightFS-Create", "0")
        .header("X-LightFS-Length", "9999")
        .send(b"short".as_slice())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);

    // ...and the file must be untouched: same size, same version.
    let (ok, node) = srv.rpc("lookup", json!({ "path": "/len/f" }));
    assert!(ok);
    assert_eq!(node["size"], payload.len() as i64);
    assert_eq!(node["version"], version0);

    // missing length header: rejected with 400, still untouched
    let resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/len/f")
        .header("X-LightFS-Create", "0")
        .send(b"whatever".as_slice())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let (ok, node) = srv.rpc("lookup", json!({ "path": "/len/f" }));
    assert!(ok);
    assert_eq!(node["version"], version0);

    // a correct write afterwards still works
    let resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/len/f")
        .header("X-LightFS-Create", "0")
        .header("X-LightFS-Length", "5")
        .send(b"fixed".as_slice())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // declared length over the hard limit: rejected before any disk staging
    // (the tiny body proves the rejection is immediate, not after upload)
    let mut resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/len/f")
        .header("X-LightFS-Create", "0")
        .header("X-LightFS-Length", (268435456u64 + 1).to_string())
        .send(b"tiny".as_slice())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let v: Value = serde_json::from_str(&resp.body_mut().read_to_string().unwrap()).unwrap();
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("exceeds limit")
    );
    let (ok, node) = srv.rpc("lookup", json!({ "path": "/len/f" }));
    assert!(ok);
    assert_eq!(node["size"], 5);

    // ACTUAL length > declared length: rejected, file untouched (review P1:
    // both directions of the mismatch must refuse to commit)
    let resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/len/f")
        .header("X-LightFS-Create", "0")
        .header("X-LightFS-Length", "3")
        .send(b"this body is much longer than declared".as_slice())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);
    let (ok, node) = srv.rpc("lookup", json!({ "path": "/len/f" }));
    assert!(ok);
    assert_eq!(node["size"], 5, "actual>declared mismatch must not commit");

    // non-numeric length header: rejected
    let resp = agent
        .post(format!("{url}/data/write"))
        .header("X-LightFS-Path", "/len/f")
        .header("X-LightFS-Create", "0")
        .header("X-LightFS-Length", "not-a-number")
        .send(b"12345".as_slice())
        .unwrap();
    assert_eq!(resp.status().as_u16(), 400);

    // staging hygiene: every successful and failed write above must have
    // cleaned its temp staging file (user data must not leak into /tmp).
    // Scoped to files created BY THIS TEST: the test takes a snapshot first,
    // so pre-existing orphans from other processes do not create noise.
    // (Known limitation: a SIGKILLed server cannot run RAII cleanup and
    // leaves staging files behind — documented in the design notes.)
    let new_leaks: Vec<_> = std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.starts_with("lightfs-up-")
                && e.metadata()
                    .map(|m| {
                        m.created().unwrap_or(std::time::SystemTime::UNIX_EPOCH) > staging_snapshot
                    })
                    .unwrap_or(false)
        })
        .collect();
    assert!(
        new_leaks.is_empty(),
        "staging files leaked: {:?}",
        new_leaks
    );
}

#[test]
fn symlink_wire_semantics() {
    let srv = TestServer::start("symlink").unwrap();

    // create + readlink over the wire; target rides verbatim
    let (ok, _) = srv.rpc("mkdir", json!({ "path": "/d" }));
    assert!(ok);
    let (ok, node) = srv.rpc(
        "symlink",
        json!({ "path": "/d/link", "target": "../target-file" }),
    );
    assert!(ok, "symlink rpc failed: {node:?}");
    assert_eq!(node["kind"], "symlink");
    assert_eq!(node["target"], "../target-file");
    assert_eq!(node["size"], "../target-file".len() as i64);

    let (ok, r) = srv.rpc("readlink", json!({ "path": "/d/link" }));
    assert!(ok);
    assert_eq!(r["target"], "../target-file");

    // lstat (lookup) keeps the link node; the target field rides along
    let (ok, node) = srv.rpc("lookup", json!({ "path": "/d/link" }));
    assert!(ok);
    assert_eq!(node["kind"], "symlink");

    // dangling until the target exists
    let (ok, _) = srv.rpc("write", json!({ "path": "/target-file", "data": "aGk=" }));
    assert!(ok);
    // A dir symlink resolves as an intermediate component: readdir through
    // it lists the target directory. (Literal ".." never reaches Core -
    // path_components rejects it - because the FUSE kernel normalizes the
    // path before issuing RPCs.)
    let (ok, _) = srv.rpc("symlink", json!({ "path": "/dirlink", "target": "d" }));
    assert!(ok);
    let (ok, r) = srv.rpc("readdir", json!({ "path": "/dirlink" }));
    assert!(ok, "readdir through a dir symlink must follow it: {r:?}");
    assert!(
        r["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|n| n["name"] == "link")
    );

    // readlink on a non-symlink is a kind error, not a crash
    let (ok, _) = srv.rpc("readlink", json!({ "path": "/target-file" }));
    assert!(!ok);

    // EEXIST on occupied names, any kind
    let (ok, _) = srv.rpc("symlink", json!({ "path": "/d/link", "target": "/x" }));
    assert!(!ok);

    // a symlink counts as an occupied name for create/mkdir too
    let (ok, _) = srv.rpc("create", json!({ "path": "/d/link", "mode": 0o644 }));
    assert!(!ok);

    // delete the link; the target file is untouched
    let (ok, _) = srv.rpc("unlink", json!({ "path": "/d/link" }));
    assert!(ok);
    let (ok, node) = srv.rpc("lookup", json!({ "path": "/target-file" }));
    assert!(ok);
    assert_eq!(node["kind"], "file");

    // the escape contract over the wire: .. clamps at the root
    let (ok, _) = srv.rpc(
        "symlink",
        json!({ "path": "/esc", "target": "../../../../etc" }),
    );
    assert!(ok);
    let (ok, node) = srv.rpc("lookup", json!({ "path": "/esc" }));
    assert!(ok);
    assert_eq!(node["kind"], "symlink", "dangling inside the mount only");
}
