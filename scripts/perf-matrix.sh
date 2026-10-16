#!/usr/bin/env bash
# Layered I/O benchmark matrix (performance review harness).
# Runs lightfs-bench against MinIO + a real LightFS server, capturing wall
# time, throughput, per-request latency percentiles, and process max RSS.
#
#   scripts/perf-matrix.sh [LABEL]     # LABEL e.g. BEFORE / AFTER-binpath
# Results: /tmp/lightfs-perf/<LABEL>.txt
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

LABEL="${1:-run}"
OUT="/tmp/lightfs-perf/${LABEL}.txt"
mkdir -p /tmp/lightfs-perf
PORT=18085

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build --release -q --bin lightfs --bin lightfs-bench

DB="/tmp/lightfs-perf-server-$$-$(date +%s).db"
CFG="/tmp/lightfs-perf-$$-$(date +%s).toml"
cat > "$CFG" <<EOF
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "${LIGHTFS_S3_BUCKET:-lightfs-bench}"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$DB"
[server]
bind = "127.0.0.1:$PORT"
[cache.metadata]
enabled = false
ttl_seconds = 5
[cache.data]
enabled = false
EOF

SERVER_PID=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-perf-server.log 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && break; sleep 0.2; done

{
  echo "# LightFS layered benchmark — $LABEL"
  echo "# date: $(date)  host: $(hostname -s)  dataset: 6000 x 100KB + 256MiB (+64MiB lfs copy)"
  echo "# server: 8 worker threads, metadata cache OFF; client: metadata cache OFF"
  /usr/bin/time -l ./target/release/lightfs-bench \
    --server "http://127.0.0.1:$PORT" \
    --s3-endpoint "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}" \
    --bucket "${LIGHTFS_S3_BUCKET:-lightfs-bench}" \
    --scenario all 2>&1
} | tee "$OUT"

echo "results: $OUT"
