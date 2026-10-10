#!/usr/bin/env bash
# Phase 9: multi-client correctness against a real server process.
# Uses two independent lightfs-shell processes as Client A / Client B.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build -q

DB="/tmp/lightfs-mc-$$-$(date +%s).db"
CFG="/tmp/lightfs-mc-$$-$(date +%s).toml"
PORT=18082
cat > "$CFG" <<EOF
mode = "server"
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "${LIGHTFS_S3_BUCKET:-lightfs-test}"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$DB"
[server]
bind = "127.0.0.1:$PORT"
[cache.metadata]
enabled = true
ttl_seconds = 1
[cache.data]
enabled = false
EOF

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

start_server() {
  ./target/debug/lightfs server --config "$CFG" >>/tmp/lightfs-mc-server.log 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && return 0
    sleep 0.2
  done
  echo "server did not come up" >&2; exit 1
}

A=(./target/debug/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT")
# Client B caches metadata for 1s to exercise TTL invalidation on the way.
B=(./target/debug/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT")

start_server

echo "== 1-2. A creates, B lists"
"${A[@]}" mkdir /mc
"${A[@]}" write /mc/file --string shared-data
"${B[@]}" ls /mc | grep -q "file"
echo "OK"

echo "== 3-4. A renames, B sees rename"
"${A[@]}" mv /mc/file /mc/file-renamed
"${B[@]}" cat /mc/file-renamed | grep -q "shared-data"
"${B[@]}" ls /mc | grep -q "file-renamed"
if "${B[@]}" cat /mc/file 2>/dev/null; then echo "stale path still visible" >&2; exit 1; fi
echo "OK"

echo "== 5-6. B writes, A reads"
"${B[@]}" write /mc/file-renamed --string client-B-wrote
"${A[@]}" cat /mc/file-renamed | grep -q "client-B-wrote"
echo "OK"

echo "== 7. concurrent write conflict (optimistic, no silent overwrite)"
V=$("${B[@]}" stat /mc/file-renamed | awk '/^version:/ {print $2}')
"${B[@]}" cas-write /mc/file-renamed "$V" --string newer-content
if "${A[@]}" cas-write /mc/file-renamed "$V" --string stale-content 2>/dev/null; then
  echo "conflicting write unexpectedly succeeded" >&2; exit 1
fi
"${A[@]}" cat /mc/file-renamed | grep -q "newer-content"
echo "OK (conflict rejected, B's content stands)"

echo "== 8-9. server restart; both clients reconnect"
kill "$SERVER_PID"; wait "$SERVER_PID" 2>/dev/null || true
start_server
"${A[@]}" cat /mc/file-renamed | grep -q "newer-content"
"${B[@]}" ls /mc | grep -q "file-renamed"
"${B[@]}" write /mc/after-restart --string alive
"${A[@]}" cat /mc/after-restart | grep -q "alive"
echo "OK"

# cleanup namespace
"${A[@]}" rm /mc/file-renamed >/dev/null
"${A[@]}" rm /mc/after-restart >/dev/null
"${A[@]}" script /dev/null >/dev/null 2>&1 || true
echo "MULTI-CLIENT CORRECTNESS OK"
