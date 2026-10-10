//! LightFS client protocol (Phase 6): JSON over HTTP, one endpoint.
//!
//! Request:  {"id":N,"method":"lookup","params":{...}}
//! Response: {"id":N,"ok":true,"result":...} | {"id":N,"ok":false,"error":{...}}
//!
//! The protocol never exposes SQLite/S3 internals.
//! `write` is stateless and atomic: the client buffers its WriteSession and
//! commits the whole content in one call, carrying expected_version for
//! optimistic concurrency. Server restarts therefore cannot lose sessions.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::core::Core;
use crate::error::{Error, Result};
use crate::metadata::{Node, NodeKind};

pub const PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Deserialize)]
pub struct RpcRequest {
    pub id: Value,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Serialize)]
pub struct RpcErrorBody {
    pub code: String,
    pub message: String,
}

#[derive(Debug)]
pub enum RpcResponse {
    Ok(Value),
    Err(Error),
}

impl RpcResponse {
    pub fn to_json(&self, id: &Value) -> String {
        match self {
            RpcResponse::Ok(result) => json!({"id": id, "ok": true, "result": result}).to_string(),
            RpcResponse::Err(e) => json!({
                "id": id,
                "ok": false,
                "error": RpcErrorBody { code: e.code().to_string(), message: e.to_string() }
            })
            .to_string(),
        }
    }
}

pub fn node_to_json(n: &Node) -> Value {
    json!({
        "id": n.id,
        "parent_id": n.parent_id,
        "name": n.name,
        "kind": n.kind.as_str(),
        "size": n.size,
        "mtime": n.mtime,
        "mode": n.mode,
        "object_key": n.object_key,
        "version": n.version,
        "etag": n.etag,
    })
}

/// Client-side decoder (Phase 7).
#[allow(dead_code)]
fn node_from_json(v: &Value) -> Result<Node> {
    let kind = match v["kind"].as_str() {
        Some("dir") => NodeKind::Dir,
        Some("file") => NodeKind::File,
        _ => return Err(Error::InvalidArgument("node kind".into())),
    };
    Ok(Node {
        id: v["id"]
            .as_i64()
            .ok_or_else(|| Error::InvalidArgument("id".into()))?,
        parent_id: v["parent_id"].as_i64(),
        name: v["name"].as_str().unwrap_or_default().to_string(),
        kind,
        size: v["size"].as_i64().unwrap_or(0),
        mtime: v["mtime"].as_i64().unwrap_or(0),
        mode: v["mode"].as_u64().unwrap_or(0o644) as u32,
        object_key: v["object_key"].as_str().map(String::from),
        version: v["version"].as_i64().unwrap_or(0),
        etag: v["etag"].as_str().unwrap_or_default().to_string(),
    })
}

fn p_str(params: &Value, key: &str) -> Result<String> {
    params[key]
        .as_str()
        .map(String::from)
        .ok_or_else(|| Error::InvalidArgument(format!("params.{key}")))
}

fn p_u64(params: &Value, key: &str) -> Result<u64> {
    params[key]
        .as_u64()
        .ok_or_else(|| Error::InvalidArgument(format!("params.{key}")))
}

fn p_opt_i64(params: &Value, key: &str) -> Option<i64> {
    params[key].as_i64()
}

fn p_opt_u32(params: &Value, key: &str) -> Option<u32> {
    params[key].as_u64().map(|v| v as u32)
}

/// Dispatch one parsed request against the server's Core.
pub fn dispatch(core: &Core, req: &RpcRequest) -> RpcResponse {
    match handle(core, &req.method, &req.params) {
        Ok(v) => RpcResponse::Ok(v),
        Err(e) => RpcResponse::Err(e),
    }
}

fn handle(core: &Core, method: &str, params: &Value) -> Result<Value> {
    match method {
        "ping" => Ok(json!({
            "protocol": PROTOCOL_VERSION,
            "server": format!("lightfs {}", env!("CARGO_PKG_VERSION")),
        })),
        "metrics" => {
            let m = &core.metrics;
            let g = |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed);
            Ok(json!({
                "s3_get": g(&m.s3_get), "s3_put": g(&m.s3_put),
                "s3_head": g(&m.s3_head), "s3_delete": g(&m.s3_delete),
                "s3_list": g(&m.s3_list),
                "bytes_read": g(&m.bytes_read), "bytes_written": g(&m.bytes_written),
                "conflicts": g(&m.conflicts), "metadata_tx": g(&m.metadata_tx),
            }))
        }
        "lookup" | "getattr" | "open" => {
            let path = p_str(params, "path")?;
            let node = if method == "open" {
                core.open(&path)?
            } else {
                core.lookup(&path)?
            };
            Ok(node_to_json(&node))
        }
        "readdir" => {
            let path = p_str(params, "path")?;
            let nodes = core.readdir(&path)?;
            let list: Vec<Value> = nodes.iter().map(node_to_json).collect();
            Ok(json!({ "nodes": list }))
        }
        "mkdir" => {
            let path = p_str(params, "path")?;
            let mode = p_opt_u32(params, "mode").unwrap_or(0o755);
            Ok(node_to_json(&core.mkdir(&path, mode)?))
        }
        "create" => {
            let path = p_str(params, "path")?;
            let mode = p_opt_u32(params, "mode").unwrap_or(0o644);
            Ok(node_to_json(&core.create(&path, mode)?))
        }
        "read" => {
            use base64::Engine as _;
            let path = p_str(params, "path")?;
            let offset = p_u64(params, "offset")?;
            let size = p_u64(params, "size")?;
            let data = core.read(&path, offset, size)?;
            Ok(json!({
                "offset": offset,
                "size": data.len(),
                "data": base64::engine::general_purpose::STANDARD.encode(&data),
            }))
        }
        "write" => {
            use base64::Engine as _;
            let path = p_str(params, "path")?;
            let data_b64 = p_str(params, "data")?;
            let data = base64::engine::general_purpose::STANDARD
                .decode(data_b64.as_bytes())
                .map_err(|e| Error::InvalidArgument(format!("data base64: {e}")))?;
            let create_if_missing = params["create_if_missing"].as_bool().unwrap_or(true);
            if create_if_missing {
                match core.create(&path, 0o644) {
                    Ok(_) => {}
                    Err(Error::AlreadyExists(_)) => {}
                    Err(e) => return Err(e),
                }
            }
            let handle = core.open_write(&path, p_opt_i64(params, "expected_version"))?;
            core.write(&handle, 0, &data)?;
            let node = core.commit_write(&handle)?;
            Ok(node_to_json(&node))
        }
        "presign" => {
            let path = p_str(params, "path")?;
            let node = core.lookup(&path)?;
            if !node.is_file() {
                return Err(Error::KindMismatch {
                    path: path.to_string(),
                    expected: "file",
                    actual: "dir".into(),
                });
            }
            let ttl = params["ttl_seconds"].as_u64().unwrap_or(60).min(3600);
            let key = node
                .object_key
                .clone()
                .ok_or_else(|| Error::NotFound(path.to_string()))?;
            match core.objects().presign_get(&key, ttl) {
                Some(url) => Ok(json!({"url": url, "expires_seconds": ttl, "size": node.size})),
                None => Err(Error::InvalidArgument(
                    "object store does not support presigned URLs".into(),
                )),
            }
        }
        "rename" => {
            let old = p_str(params, "old")?;
            let new = p_str(params, "new")?;
            core.rename(&old, &new)?;
            Ok(json!({"moved": [old, new]}))
        }
        "unlink" => {
            let path = p_str(params, "path")?;
            core.unlink(&path)?;
            Ok(json!({"removed": path}))
        }
        "rmdir" => {
            let path = p_str(params, "path")?;
            core.rmdir(&path)?;
            Ok(json!({"removed": path}))
        }
        "setattr" => {
            let path = p_str(params, "path")?;
            let node =
                core.setattr(&path, p_opt_u32(params, "mode"), p_opt_i64(params, "mtime"))?;
            Ok(node_to_json(&node))
        }
        other => Err(Error::InvalidArgument(format!("unknown method: {other}"))),
    }
}

/// Parse + dispatch a raw JSON body (server transport calls this).
pub fn handle_body(core: &Core, body: &[u8]) -> (Value, String) {
    let parsed: std::result::Result<RpcRequest, _> = serde_json::from_slice(body);
    match parsed {
        Ok(req) => {
            let resp = dispatch(core, &req);
            (req.id.clone(), resp.to_json(&req.id))
        }
        Err(e) => {
            let id = Value::Null;
            let resp = RpcResponse::Err(Error::InvalidArgument(format!("bad request: {e}")));
            let body = resp.to_json(&id);
            (id, body)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::sqlite::SqliteMetadataStore;
    use crate::object_store::mem::MemObjectStore;
    use std::sync::Arc;

    fn core() -> Core {
        Core::new(
            Arc::new(SqliteMetadataStore::open_in_memory().unwrap()),
            Arc::new(MemObjectStore::new()),
        )
    }

    fn call(core: &Core, id: i64, method: &str, params: Value) -> (bool, Value) {
        let body = json!({"id": id, "method": method, "params": params}).to_string();
        let (_id, resp) = handle_body(core, body.as_bytes());
        let v: Value = serde_json::from_str(&resp).unwrap();
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

    #[test]
    fn rpc_roundtrip_namespaced_ops() {
        let c = core();
        let (ok, _) = call(&c, 1, "ping", json!({}));
        assert!(ok);
        let (ok, _) = call(&c, 2, "mkdir", json!({"path": "/a", "mode": 0o755}));
        assert!(ok);
        let (ok, r) = call(&c, 3, "lookup", json!({"path": "/a"}));
        assert!(ok);
        assert_eq!(r["kind"], "dir");

        let (ok, _) = call(
            &c,
            4,
            "write",
            json!({"path": "/a/f", "data": base64_data(b"hello rpc")}),
        );
        assert!(ok);
        let (ok, r) = call(
            &c,
            5,
            "read",
            json!({"path": "/a/f", "offset": 0, "size": 100}),
        );
        assert!(ok);
        assert_eq!(read_data(&r), b"hello rpc");

        let (ok, _) = call(&c, 6, "readdir", json!({"path": "/a"}));
        assert!(ok);
        let (ok, _) = call(&c, 7, "rename", json!({"old": "/a/f", "new": "/a/g"}));
        assert!(ok);
        let (ok, _) = call(&c, 8, "unlink", json!({"path": "/a/g"}));
        assert!(ok);
        let (ok, _) = call(&c, 9, "rmdir", json!({"path": "/a"}));
        assert!(ok);
    }

    #[test]
    fn rpc_errors_are_structured() {
        let c = core();
        let (ok, e) = call(&c, 1, "lookup", json!({"path": "/missing"}));
        assert!(!ok);
        assert_eq!(e["code"], "NOT_FOUND");
        let (ok, e) = call(&c, 2, "nosuchmethod", json!({}));
        assert!(!ok);
        assert_eq!(e["code"], "INVALID");
        let (ok, e) = call(&c, 3, "lookup", json!({}));
        assert!(!ok);
        assert_eq!(e["code"], "INVALID");
    }

    #[test]
    fn rpc_write_conflict_detected() {
        let c = core();
        call(&c, 1, "create", json!({"path": "/f"}));
        let (ok, _) = call(
            &c,
            2,
            "write",
            json!({"path": "/f", "data": base64_data(b"v1"), "expected_version": 1}),
        );
        assert!(ok);
        // stale write: version is now 2
        let (ok, e) = call(
            &c,
            3,
            "write",
            json!({"path": "/f", "data": base64_data(b"v2"), "expected_version": 1, "create_if_missing": false}),
        );
        assert!(!ok);
        assert_eq!(e["code"], "CONFLICT");
        let (ok, r) = call(
            &c,
            4,
            "read",
            json!({"path": "/f", "offset": 0, "size": 10}),
        );
        assert!(ok);
        assert_eq!(read_data(&r), b"v1");
    }

    fn base64_data(d: &[u8]) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD.encode(d)
    }

    fn read_data(r: &Value) -> Vec<u8> {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(r["data"].as_str().unwrap())
            .unwrap()
    }
}
