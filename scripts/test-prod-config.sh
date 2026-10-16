#!/usr/bin/env bash
# Phase 14: production S3/OSS switch is a CONFIG-ONLY change.
# Simulates it by pointing the same code at a different bucket + region +
# access credentials on the local S3-compatible endpoint. No recompile, no
# application change: the entire acceptance is that these writes/reads/fsck
# succeed on the new configuration.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build -q

DB="/tmp/lightfs-prod-$$-$(date +%s).db"
CFG="/tmp/lightfs-prod-$$-$(date +%s).toml"
PROD_BUCKET="${LIGHTFS_PROD_BUCKET:-lightfs-prod-sim}"
cat > "$CFG" <<EOF
# Production-shaped config: only endpoint/bucket/credentials/region change
# when moving from MinIO to real S3/OSS.
[s3]
endpoint = "${LIGHTFS_PROD_ENDPOINT:-${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}}"
region = "${LIGHTFS_PROD_REGION:-cn-hangzhou}"
bucket = "$PROD_BUCKET"
access_key = "${LIGHTFS_PROD_ACCESS_KEY:-minioadmin}"
secret_key = "${LIGHTFS_PROD_SECRET_KEY:-minioadmin}"
path_style = true
[metadata]
path = "$DB"
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
EOF

# one-time provisioning of the (simulated) production bucket
target/debug/lightfs create-bucket --config "$CFG"

# S4.14.1: lightfs-shell is client-only — writes/reads go through a
# short-lived `lightfs server`; fsck runs OFFLINE (server stopped).
cat >> "$CFG" <<EOF
[server]
bind = "127.0.0.1:18093"
EOF
PORT=18093
SRV_PID=""
cleanup() {
  [[ -n "${SRV_PID:-}" ]] && kill "${SRV_PID:-}" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT
start_server() {
  target/debug/lightfs server --config "$CFG" >>/tmp/lightfs-prod-server-$$.log 2>&1 &
  SRV_PID=$!
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && return 0
    sleep 0.2
  done
  echo "prod-sim server did not come up" >&2; return 1
}
stop_server() {
  if [[ -n "${SRV_PID:-}" ]]; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
    SRV_PID=""
  fi
}
SHELL="target/debug/lightfs-shell --server http://127.0.0.1:$PORT"
start_server
$SHELL --config "$CFG" mkdir /prod-sim
$SHELL --config "$CFG" write /prod-sim/data --string produced-on-prod-shaped-config
$SHELL --config "$CFG" cat /prod-sim/data | grep -q "produced-on-prod-shaped-config"
stop_server
target/debug/lightfs fsck --config "$CFG" | grep -q "clean"
start_server
$SHELL --config "$CFG" rm /prod-sim/data >/dev/null
stop_server
echo "PRODUCTION CONFIG SWITCH OK (bucket=$PROD_BUCKET, region from config)"
