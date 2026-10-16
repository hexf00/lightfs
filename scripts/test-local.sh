#!/usr/bin/env bash
# Definition-of-Done entry point for local development:
#   core unit tests -> MinIO integration -> shell script mode -> fsck
# FUSE and multi-client live in test-fuse.sh / test-multiclient.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"

echo "== unit tests (no network, no FUSE)"
cargo test

echo "== MinIO integration (object store, engine, external modification)"
start_minio
LIGHTFS_TEST_MINIO=1 cargo test

echo "== shell script mode over real MinIO"
cargo build -q
DB="/tmp/lightfs-local-$$-$(date +%s).db"
CFG="/tmp/lightfs-local-$$-$(date +%s).toml"
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
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
EOF
cat >> "$CFG" <<EOF
[server]
bind = "127.0.0.1:18091"
EOF
LPORT=18091
SRV_PID=""
start_local_server() {
  ./target/debug/lightfs server --config "$1" >>"$2" 2>&1 &
  SRV_PID=$!
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$LPORT/health" && return 0
    sleep 0.2
  done
  echo "local server did not come up" >&2; return 1
}
stop_local_server() {
  if [[ -n "${SRV_PID:-}" ]]; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
    SRV_PID=""
  fi
}
start_local_server "$CFG" "/tmp/lightfs-local-server-$$.log"
./target/debug/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$LPORT" script tests/basic.lfs
stop_local_server

echo "== fsck on a clean namespace"
DB2="/tmp/lightfs-fscklocal-$$-$(date +%s).db"
CFG2="/tmp/lightfs-fscklocal-$$-$(date +%s).toml"
cat > "$CFG2" <<EOF
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "${LIGHTFS_FSCK_BUCKET:-lightfs-local-fsck}"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$DB2"
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
EOF
./target/debug/lightfs create-bucket --config "$CFG2"
# S4.14.1: writes go through the server (client-only shell); fsck runs
# OFFLINE — stop the server first, run it, restart for the final delete.
cat >> "$CFG2" <<EOF
[server]
bind = "127.0.0.1:18092"
EOF
LPORT=18092
start_local_server "$CFG2" "/tmp/lightfs-local-fsck-server-$$.log"
./target/debug/lightfs-shell --config "$CFG2" --server "http://127.0.0.1:$LPORT" write /fsck-me --string intact
stop_local_server
./target/debug/lightfs fsck --config "$CFG2" | grep -q clean
start_local_server "$CFG2" "/tmp/lightfs-local-fsck-server-$$.log"
./target/debug/lightfs-shell --config "$CFG2" --server "http://127.0.0.1:$LPORT" rm /fsck-me >/dev/null
stop_local_server
rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm" "$CFG2" "$DB2" "$DB2-wal" "$DB2-shm"

echo "LOCAL TESTS OK"
