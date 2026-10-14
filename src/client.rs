//! Network client (Phase 7): implements FsApi over the LightFS protocol.
//!
//! - Metadata: server is the authority; this client only caches (TTL,
//!   invalidated locally on its own mutations). Cache loss is never a
//!   correctness problem (AD-009). Remote mutations by other clients become
//!   visible after the TTL.
//! - Content: buffered client-side like the embedded WriteSession, committed
//!   with one atomic `write` RPC carrying expected_version. Bytes go through
//!   the server in v1 (data-plane bypass is Phase 10 and must not change
//!   filesystem semantics).

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::api::{FsApi, WriteSession};
use crate::error::{Error, Result};
use crate::metadata::Node;

pub struct ClientConfig {
    pub url: String,
    /// None disables metadata caching entirely.
    pub metadata_ttl: Option<Duration>,
}

pub struct Client {
    url: String,
    agent: ureq::Agent,
    cache: MetaCache,
    ttl: Option<Duration>,
}

/// TTL metadata cache. Never authoritative: losing it, expiring it, or a
/// remote mutation outrunning it only changes freshness, not correctness.
#[derive(Default)]
struct MetaCache {
    nodes: Mutex<HashMap<String, (Node, Instant)>>,
    listings: Mutex<HashMap<String, (Vec<Node>, Instant)>>,
}

impl MetaCache {
    fn get_node(&self, key: &str, ttl: Option<Duration>) -> Option<Node> {
        let ttl = ttl?;
        let mut m = self.nodes.lock().unwrap();
        match m.get(key) {
            Some((n, at)) if at.elapsed() < ttl => Some(n.clone()),
            _ => {
                m.remove(key);
                None
            }
        }
    }
    fn put_node(&self, key: &str, node: &Node) {
        self.nodes
            .lock()
            .unwrap()
            .insert(key.to_string(), (node.clone(), Instant::now()));
    }
    fn get_listing(&self, dir: &str, ttl: Option<Duration>) -> Option<Vec<Node>> {
        let ttl = ttl?;
        let mut m = self.listings.lock().unwrap();
        match m.get(dir) {
            Some((l, at)) if at.elapsed() < ttl => Some(l.clone()),
            _ => {
                m.remove(dir);
                None
            }
        }
    }
    fn put_listing(&self, dir: &str, list: &[Node]) {
        self.listings
            .lock()
            .unwrap()
            .insert(dir.to_string(), (list.to_vec(), Instant::now()));
    }
    fn invalidate_node(&self, key: &str) {
        self.nodes.lock().unwrap().remove(key);
    }
    fn invalidate_listing(&self, dir: &str) {
        self.listings.lock().unwrap().remove(dir);
    }
}

/// JSON RPC responses carry base64 file data; a 10 MiB default cap would
/// make >7 MiB files unreadable through the server path.
const MAX_RESPONSE_BODY: u64 = 1024 * 1024 * 1024;

#[derive(Deserialize)]
struct RpcEnvelope {
    ok: bool,
    #[serde(default)]
    result: Value,
    #[serde(default)]
    error: Option<RpcErrorWire>,
}

#[derive(Deserialize)]
struct RpcErrorWire {
    code: String,
    message: String,
}

struct ClientWriteSession<'a> {
    client: &'a Client,
    /// STABLE node id this session is anchored on: the commit routes by
    /// id, so a rename by another client cannot detach the session from
    /// its own file (the path stays for diagnostics only).
    node_id: i64,
    path: String,
    expected_version: i64,
    chunks: BTreeMap<u64, Vec<u8>>,
}

impl WriteSession for ClientWriteSession<'_> {
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        if !data.is_empty() {
            self.chunks.insert(offset, data.to_vec());
        }
        Ok(data.len())
    }
    fn commit(self: Box<Self>) -> Result<Node> {
        let mut content: Vec<u8> = Vec::new();
        for (&off, data) in self.chunks.iter() {
            if off > content.len() as u64 {
                content.resize(off as usize, 0);
            }
            let end = (off as usize + data.len()).max(content.len());
            content.resize(end, 0);
            content[off as usize..off as usize + data.len()].copy_from_slice(data);
        }
        let node = self.client.write_node_by_id(
            self.node_id,
            &self.path,
            &content,
            Some(self.expected_version),
        )?;
        self.client.after_mutation(&self.path);
        Ok(node)
    }
}

impl Client {
    pub fn new(cfg: ClientConfig) -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_global(Some(Duration::from_secs(300)))
            .http_status_as_error(false)
            .build()
            .new_agent();
        Self {
            url: cfg.url.trim_end_matches('/').to_string(),
            agent,
            cache: MetaCache::default(),
            ttl: cfg.metadata_ttl,
        }
    }

    /// Phase 11 honesty check: the data cache is specified as optional and
    /// OFF by default; v1 does not implement one. Config with enabled=true
    /// must not silently pretend otherwise.
    pub fn warn_if_data_cache_enabled(cfg: &crate::config::Config) {
        if cfg.cache.data.enabled {
            eprintln!(
                "lightfs: config enables cache.data but v1 implements no data cache; ignoring (correctness unaffected - cache is never authoritative)"
            );
        }
    }

    fn call(&self, method: &str, params: Value) -> Result<Value> {
        let slow_t0 = std::time::Instant::now();
        // Per-method client-side timing (the aggregate "client.rpc" row of
        // a dump is the sum of these): transport + serialization + response
        // processing as seen by the caller, one attribution level above the
        // server's own srv.rpc.<method> rows.
        let out = crate::metrics::time_with_name(
            || format!("client.rpc.{method}"),
            || {
                let body = json!({"id": 1, "method": method, "params": params}).to_string();
                let mut resp = self
                    .agent
                    .post(format!("{}/rpc", self.url))
                    .header("Content-Type", "application/json")
                    .send(body.as_bytes())
                    .map_err(|e| Error::Io(format!("rpc transport: {e}")))?;
                let status = resp.status().as_u16();
                let text = resp
                    .body_mut()
                    .with_config()
                    .limit(MAX_RESPONSE_BODY)
                    .read_to_string()
                    .map_err(|e| Error::Io(e.to_string()))?;
                if status != 200 {
                    return Err(Error::Io(format!("rpc http {status}")));
                }
                let env: RpcEnvelope =
                    serde_json::from_str(&text).map_err(|e| Error::Io(format!("rpc json: {e}")))?;
                if env.ok {
                    Ok(env.result)
                } else {
                    let e = env.error.unwrap_or(RpcErrorWire {
                        code: "IO".into(),
                        message: "unknown server error".into(),
                    });
                    Err(wire_to_error(&e))
                }
            },
        );
        slowlog(method, &params, slow_t0.elapsed());
        out
    }

    fn write_node(
        &self,
        path: &str,
        data: &[u8],
        expected_version: Option<i64>,
        create_if_missing: bool,
    ) -> Result<Node> {
        // Binary data endpoint: bytes travel raw; the small result comes
        // back as node JSON. (Metadata-only ops stay on /rpc.)
        let encoded_path = crate::sigv4::uri_encode(path, true);
        let mut builder = self
            .agent
            .post(format!("{}/data/write", self.url))
            .header("X-LightFS-Path", &encoded_path)
            .header(
                "X-LightFS-Create",
                if create_if_missing { "1" } else { "0" },
            )
            .header("X-LightFS-Length", data.len().to_string());
        if let Some(v) = expected_version {
            builder = builder.header("X-LightFS-Expected-Version", v.to_string());
        }
        let mut resp = crate::metrics::time("client.data.write", || {
            builder
                .send(data)
                .map_err(|e| Error::Io(format!("data/write transport: {e}")))
        })?;
        let status = resp.status().as_u16();
        let text = resp
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BODY)
            .read_to_string()
            .map_err(|e| Error::Io(e.to_string()))?;
        if !(200..300).contains(&status) {
            return Err(error_from_body(status, &text));
        }
        let v: Value =
            serde_json::from_str(&text).map_err(|e| Error::Io(format!("node json: {e}")))?;
        node_from_json(&v)
    }

    /// Commit anchored on the STABLE node id: the server locates the node
    /// by id (never re-resolving the path), so the CAS gate protects THIS
    /// file across renames by other clients, and a deleted identity fails
    /// with IDENTITY_GONE instead of writing onto a path successor.
    fn write_node_by_id(
        &self,
        node_id: i64,
        path_hint: &str,
        data: &[u8],
        expected_version: Option<i64>,
    ) -> Result<Node> {
        let encoded_path = crate::sigv4::uri_encode(path_hint, true);
        let mut builder = self
            .agent
            .post(format!("{}/data/write", self.url))
            .header("X-LightFS-Path", &encoded_path)
            .header("X-LightFS-Create", "0")
            .header("X-LightFS-Node-Id", node_id.to_string())
            .header("X-LightFS-Length", data.len().to_string());
        if let Some(v) = expected_version {
            builder = builder.header("X-LightFS-Expected-Version", v.to_string());
        }
        let mut resp = crate::metrics::time("client.data.write", || {
            builder
                .send(data)
                .map_err(|e| Error::Io(format!("data/write transport: {e}")))
        })?;
        let status = resp.status().as_u16();
        let text = resp
            .body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BODY)
            .read_to_string()
            .map_err(|e| Error::Io(e.to_string()))?;
        if !(200..300).contains(&status) {
            return Err(error_from_body(status, &text));
        }
        let v: Value =
            serde_json::from_str(&text).map_err(|e| Error::Io(format!("node json: {e}")))?;
        node_from_json(&v)
    }

    /// Drop cached state affected by a local mutation of `path`.
    fn after_mutation(&self, path: &str) {
        if self.ttl.is_none() {
            return;
        }
        self.cache.invalidate_node(path);
        self.cache.invalidate_listing(&path_parent(path));
    }
}

fn path_parent(path: &str) -> String {
    match path.rfind('/') {
        Some(0) | None => "/".to_string(),
        Some(i) => path[..i].to_string(),
    }
}

fn wire_to_error(e: &RpcErrorWire) -> Error {
    match e.code.as_str() {
        "IDENTITY_GONE" => Error::IdentityGone(e.message.clone()),
        "NOT_FOUND" => Error::NotFound(e.message.clone()),
        "ALREADY_EXISTS" => Error::AlreadyExists(e.message.clone()),
        "CONFLICT" => Error::Conflict {
            path: e.message.clone(),
            expected: -1,
            actual: -1,
        },
        "KIND_MISMATCH" => Error::KindMismatch {
            path: e.message.clone(),
            expected: "node",
            actual: "mismatch".into(),
        },
        "DIRECTORY_NOT_EMPTY" => Error::DirectoryNotEmpty(e.message.clone()),
        "CYCLE" => Error::Cycle(e.message.clone()),
        "INVALID" => Error::InvalidArgument(e.message.clone()),
        "STORAGE" => Error::Storage(e.message.clone()),
        "OBJECT_MISSING" => Error::ObjectMissing(e.message.clone()),
        "DB" => Error::Db(e.message.clone()),
        _ => Error::Io(e.message.clone()),
    }
}

/// Parse an error body from either the JSON-RPC envelope
/// ({"error":{code,message}}) or the binary endpoints' error bodies.
fn error_from_body(status: u16, text: &str) -> Error {
    #[derive(Deserialize)]
    struct Body {
        #[serde(default)]
        error: Option<RpcErrorWire>,
    }
    match serde_json::from_str::<Body>(text) {
        Ok(b) if b.error.is_some() => wire_to_error(b.error.as_ref().unwrap()),
        _ => Error::Io(format!(
            "http {status}: {}",
            text.chars().take(200).collect::<String>()
        )),
    }
}

fn node_from_json(v: &Value) -> Result<Node> {
    let kind = match v["kind"].as_str() {
        Some("dir") => crate::metadata::NodeKind::Dir,
        Some("file") => crate::metadata::NodeKind::File,
        Some("symlink") => crate::metadata::NodeKind::Symlink,
        _ => return Err(Error::Io("bad node kind from server".into())),
    };
    Ok(Node {
        id: v["id"].as_i64().unwrap_or(0),
        parent_id: v["parent_id"].as_i64(),
        name: v["name"].as_str().unwrap_or_default().into(),
        kind,
        size: v["size"].as_i64().unwrap_or(0),
        mtime: v["mtime"].as_i64().unwrap_or(0),
        mode: v["mode"].as_u64().unwrap_or(0o644) as u32,
        object_key: v["object_key"].as_str().map(String::from),
        version: v["version"].as_i64().unwrap_or(0),
        etag: v["etag"].as_str().unwrap_or_default().into(),
        target: v["target"].as_str().map(String::from),
        pack_off: v["pack_off"].as_i64(),
        pack_len: v["pack_len"].as_i64(),
        content_hash: v["content_hash"].as_str().map(String::from),
    })
}

impl FsApi for Client {
    fn lookup(&self, path: &str) -> Result<Node> {
        if let Some(n) = self.cache.get_node(path, self.ttl) {
            return Ok(n);
        }
        let r = self.call("lookup", json!({"path": path}))?;
        let node = node_from_json(&r)?;
        self.cache.put_node(path, &node);
        Ok(node)
    }

    fn readdir(&self, path: &str) -> Result<Vec<Node>> {
        if let Some(list) = self.cache.get_listing(path, self.ttl) {
            return Ok(list);
        }
        let r = self.call("readdir", json!({"path": path}))?;
        let mut out = Vec::new();
        for item in r["nodes"]
            .as_array()
            .ok_or_else(|| Error::Io("bad readdir".into()))?
        {
            out.push(node_from_json(item)?);
        }
        self.cache.put_listing(path, &out);
        Ok(out)
    }

    fn mkdir(&self, path: &str, mode: u32) -> Result<Node> {
        let r = self.call("mkdir", json!({"path": path, "mode": mode}))?;
        let node = node_from_json(&r)?;
        self.after_mutation(path);
        Ok(node)
    }

    fn create(&self, path: &str, mode: u32) -> Result<Node> {
        let r = self.call("create", json!({"path": path, "mode": mode}))?;
        let node = node_from_json(&r)?;
        self.after_mutation(path);
        Ok(node)
    }

    fn open(&self, path: &str) -> Result<Node> {
        self.lookup(path)
    }

    fn symlink(&self, target: &str, path: &str) -> Result<Node> {
        let r = self.call("symlink", json!({"path": path, "target": target}))?;
        let node = node_from_json(&r)?;
        self.after_mutation(path);
        Ok(node)
    }

    fn readlink(&self, path: &str) -> Result<String> {
        let r = self.call("readlink", json!({"path": path}))?;
        Ok(r["target"].as_str().unwrap_or_default().to_string())
    }

    fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        // Binary data endpoint: raw bytes in the response body, no base64.
        let mut resp = crate::metrics::time("client.data.read", || {
            self.agent
                .post(format!("{}/data/read", self.url))
                .header("Content-Type", "application/json")
                .send(
                    json!({"path": path, "offset": offset, "size": size})
                        .to_string()
                        .as_bytes(),
                )
                .map_err(|e| Error::Io(format!("data/read transport: {e}")))
        })?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let text = resp
                .body_mut()
                .with_config()
                .limit(MAX_RESPONSE_BODY)
                .read_to_string()
                .unwrap_or_default();
            return Err(error_from_body(status, &text));
        }
        resp.body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BODY)
            .read_to_vec()
            .map_err(|e| Error::Io(e.to_string()))
    }

    fn open_write(
        &self,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        // Observe the version directly from the server, bypassing the cache:
        // stale expected_version would manufacture conflicts.
        let r = self.call("lookup", json!({"path": path}))?;
        let node = node_from_json(&r)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        Ok(Box::new(ClientWriteSession {
            client: self,
            node_id: node.id,
            path: path.to_string(),
            expected_version: expected_version.unwrap_or(node.version),
            chunks: BTreeMap::new(),
        }))
    }

    fn lookup_by_id(&self, node_id: i64, _path_hint: &str) -> Result<Node> {
        // Always a live RPC: the id anchor exists precisely because a
        // cached (path -> node) snapshot may lie after another client's
        // rename/unlink.
        let r = self.call("lookup", json!({"node_id": node_id}))?;
        node_from_json(&r)
    }

    fn read_by_id(
        &self,
        node_id: i64,
        _path_hint: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        let mut resp = crate::metrics::time("client.data.read", || {
            self.agent
                .post(format!("{}/data/read", self.url))
                .header("Content-Type", "application/json")
                .send(
                    json!({"node_id": node_id, "offset": offset, "size": size})
                        .to_string()
                        .as_bytes(),
                )
                .map_err(|e| Error::Io(format!("data/read transport: {e}")))
        })?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let text = resp
                .body_mut()
                .with_config()
                .limit(MAX_RESPONSE_BODY)
                .read_to_string()
                .unwrap_or_default();
            return Err(error_from_body(status, &text));
        }
        resp.body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BODY)
            .read_to_vec()
            .map_err(|e| Error::Io(e.to_string()))
    }

    fn open_write_by_id(
        &self,
        node_id: i64,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        // Live version observation through the id anchor (see lookup_by_id).
        let r = self.call("lookup", json!({"node_id": node_id}))?;
        let node = node_from_json(&r)?;
        if !node.is_file() {
            return Err(Error::KindMismatch {
                path: path.to_string(),
                expected: "file",
                actual: "dir".into(),
            });
        }
        Ok(Box::new(ClientWriteSession {
            client: self,
            node_id,
            path: path.to_string(),
            expected_version: expected_version.unwrap_or(node.version),
            chunks: BTreeMap::new(),
        }))
    }

    fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        self.call("rename", json!({"old": old_path, "new": new_path}))?;
        self.after_mutation(old_path);
        self.after_mutation(new_path);
        Ok(())
    }

    fn unlink(&self, path: &str) -> Result<()> {
        self.call("unlink", json!({"path": path}))?;
        self.after_mutation(path);
        Ok(())
    }

    fn rmdir(&self, path: &str) -> Result<()> {
        self.call("rmdir", json!({"path": path}))?;
        self.after_mutation(path);
        Ok(())
    }

    fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
        let mut params = json!({"path": path});
        if let Some(m) = mode {
            params["mode"] = json!(m);
        }
        if let Some(t) = mtime {
            params["mtime"] = json!(t);
        }
        let r = self.call("setattr", params)?;
        let node = node_from_json(&r)?;
        self.cache.invalidate_node(path);
        Ok(node)
    }
}

impl Client {
    /// Full-file convenience for scripts and tests.
    pub fn write_full(&self, path: &str, data: &[u8], create: bool) -> Result<Node> {
        let node = self.write_node(path, data, None, create)?;
        self.after_mutation(path);
        Ok(node)
    }

    /// Ask the server for a presigned GET URL of the file's current object
    /// (Phase 10 direct-data-plane read primitive; short TTL by default).
    pub fn presign(&self, path: &str, ttl_seconds: u64) -> Result<String> {
        let r = self.call("presign", json!({"path": path, "ttl_seconds": ttl_seconds}))?;
        Ok(r["url"].as_str().unwrap_or_default().to_string())
    }

    /// Stream a byte range without buffering the whole slice: the HTTP
    /// response body is handed to the caller as a reader.
    pub fn read_stream(
        &self,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let mut resp = crate::metrics::time("client.data.read", || {
            self.agent
                .post(format!("{}/data/read", self.url))
                .header("Content-Type", "application/json")
                .send(
                    json!({"path": path, "offset": offset, "size": size})
                        .to_string()
                        .as_bytes(),
                )
                .map_err(|e| Error::Io(format!("data/read transport: {e}")))
        })?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let text = resp
                .body_mut()
                .with_config()
                .limit(MAX_RESPONSE_BODY)
                .read_to_string()
                .unwrap_or_default();
            return Err(error_from_body(status, &text));
        }
        // Large responses are chunked (no Content-Length); the server
        // always states the exact length in X-LightFS-Length.
        let len: u64 = resp
            .headers()
            .get("x-lightfs-length")
            .or_else(|| resp.headers().get("content-length"))
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| Error::Io("missing X-LightFS-Length".into()))?;
        Ok((Box::new(resp.into_body().into_reader()), len))
    }

    /// Single-pass streaming write: bytes stream from `reader` through the
    /// server into a version object, then one CAS commit. No whole-file
    /// buffering on client or server.
    pub fn write_from_reader(
        &self,
        path: &str,
        reader: &mut dyn std::io::Read,
        len: u64,
        expected_version: Option<i64>,
        create: bool,
    ) -> Result<Node> {
        let encoded_path = crate::sigv4::uri_encode(path, true);
        let mut builder = self
            .agent
            .post(format!("{}/data/write", self.url))
            .header("X-LightFS-Path", &encoded_path)
            .header("X-LightFS-Create", if create { "1" } else { "0" })
            .header("X-LightFS-Length", len.to_string());
        if let Some(v) = expected_version {
            builder = builder.header("X-LightFS-Expected-Version", v.to_string());
        }
        let body = ureq::SendBody::from_reader(reader);
        // Diagnostic (Phase F): the streaming write previously had no
        // client-side timing scope; this adds the same histogram the JSON
        // write path has, plus an in-flight level sample. Metrics-gated;
        // behavior unchanged.
        static WRITE_INFLIGHT: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);
        let lvl = WRITE_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
        crate::metrics::observe(
            "client.data.write.inflight",
            std::time::Duration::from_micros(lvl as u64),
        );
        let sent = crate::metrics::time("client.data.write", || -> Result<(u16, String)> {
            let mut resp = builder
                .send(body)
                .map_err(|e| Error::Io(format!("data/write transport: {e}")))?;
            let status = resp.status().as_u16();
            let text = resp
                .body_mut()
                .with_config()
                .limit(MAX_RESPONSE_BODY)
                .read_to_string()
                .map_err(|e| Error::Io(e.to_string()))?;
            Ok((status, text))
        });
        WRITE_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        let (status, text) = sent?;
        if !(200..300).contains(&status) {
            return Err(error_from_body(status, &text));
        }
        let v: Value =
            serde_json::from_str(&text).map_err(|e| Error::Io(format!("node json: {e}")))?;
        let node = node_from_json(&v)?;
        self.after_mutation(path);
        Ok(node)
    }
}

/// Slow-RPC log (diagnosis instrument, opt-in): when LIGHTFS_RPC_SLOWLOG
/// names a file, calls slower than LIGHTFS_RPC_SLOWLOG_MS (default 10)
/// append `<unix_us> <thread> <ms> <method> <path-hint>`. Exists because
/// aggregate histograms cannot answer WHERE in wall-clock time the slow
/// calls cluster and what the other FUSE callback threads are doing at
/// that moment.
fn slowlog(method: &str, params: &Value, elapsed: std::time::Duration) {
    static CFG: std::sync::LazyLock<Option<(std::path::PathBuf, u64)>> =
        std::sync::LazyLock::new(|| {
            let path = std::env::var("LIGHTFS_RPC_SLOWLOG").ok()?;
            let ms: u64 = std::env::var("LIGHTFS_RPC_SLOWLOG_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10);
            Some((std::path::PathBuf::from(path), ms))
        });
    let Some((path, threshold_ms)) = CFG.as_ref() else {
        return;
    };
    if elapsed.as_millis() < u128::from(*threshold_ms) {
        return;
    }
    let path_hint = params
        .get("path")
        .and_then(|p| p.as_str())
        .unwrap_or("")
        .to_string();
    let us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros())
        .unwrap_or(0);
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        use std::io::Write;
        let _ = writeln!(
            f,
            "{} {:?} {} {} {}",
            us,
            std::thread::current().id(),
            elapsed.as_millis(),
            method,
            path_hint
        );
    }
}
