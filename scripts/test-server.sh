#!/usr/bin/env bash
# Server-mode acceptance: one real server process + one network client.
# (Multi-client correctness has its own deeper script: test-multiclient.sh.)
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build -q

DB="/tmp/lightfs-srvtest-$$-$(date +%s).db"
CFG="/tmp/lightfs-srvtest-$$-$(date +%s).toml"
PORT=18084
cat > "$CFG" <<EOF
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
ttl_seconds = 5
[cache.data]
enabled = false
EOF

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

./target/debug/lightfs server --config "$CFG" >>/tmp/lightfs-srvtest.log 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 50); do curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && break; sleep 0.2; done

curl -s "http://127.0.0.1:$PORT/health" | grep -q ok
curl -s -X POST "http://127.0.0.1:$PORT/rpc" -H 'Content-Type: application/json' \
  -d '{"id":1,"method":"ping","params":{}}' | grep -q '"ok":true'

SHELL="target/debug/lightfs-shell --config $CFG --server http://127.0.0.1:$PORT"
$SHELL mkdir /srv-check
$SHELL write /srv-check/file --string through-the-server
$SHELL cat /srv-check/file | grep -q through-the-server
$SHELL script tests/basic.lfs > /dev/null
$SHELL rm /srv-check/file > /dev/null

echo "SERVER TESTS OK"
