#!/usr/bin/env bash
# Global multipart-buffer budget validation: concurrent large writes with a
# 64 MiB part budget must keep SERVER RSS flat regardless of writer count.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

PORT=18085
OUT="/tmp/lightfs-perf/membudget.txt"
mkdir -p /tmp/lightfs-perf

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build --release -q --bin lightfs --bin lightfs-bench

DB="/tmp/lightfs-mem-server-PLACEHOLDER.db"
DB="${DB/PLACEHOLDER/$$-$(date +%s)}"
CFG="/tmp/lightfs-mem-$$-$(date +%s).toml"
cat > "$CFG" <<CFGEOF
mode = "server"
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
[cache.data]
enabled = false
CFGEOF

SERVER_PID=""
cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

start_server() {
  LIGHTFS_S3_PART_CONCURRENCY="${LIGHTFS_S3_PART_CONCURRENCY:-8}" \
  LIGHTFS_S3_MULTIPART_MAX_PARTS="${LIGHTFS_S3_MULTIPART_MAX_PARTS:-8}" \
  ./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-mem-server.log 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && return 0
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "ERROR: server died at startup" >&2; exit 1
    fi
    sleep 0.2
  done
  echo "server did not start" >&2; exit 1
}

server_rss_kb() { ps -o rss= -p "$SERVER_PID" 2>/dev/null | tr -d ' '; }

start_server
B="./target/release/lightfs-bench"
LFSARGS=(--server "http://127.0.0.1:$PORT" --s3-endpoint "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}" --bucket "${LIGHTFS_S3_BUCKET:-lightfs-bench}")

{
  echo "# concurrent-write memory budget validation - $(date)"
  echo "# part concurrency 8, global budget 8 parts (64 MiB), 128 MiB per writer"
  echo "# expected: server peak RSS stays near baseline regardless of writer count"
  echo "# baseline server RSS: $(server_rss_kb) KB"
  for W in 1 4 8 16; do
    BASE=$(server_rss_kb)
    MAX=0
    PIDS=()
    T0=$(date +%s)
    for i in $(seq 1 "$W"); do
      LIGHTFS_BENCH_PATH="/mem-w$W-$i.bin" "$B" "${LFSARGS[@]}" --scenario lfs-stream-write --lfs-large-mb 128 > "/tmp/lightfs-perf/w-$W-$i.txt" 2>&1 &
      PIDS+=($!)
    done
    while true; do
      alive=0
      for p in "${PIDS[@]}"; do kill -0 "$p" 2>/dev/null && alive=1; done
      [[ $alive -eq 0 ]] && break
      V=$(server_rss_kb)
      [[ -n "$V" && "$V" -gt "$MAX" ]] && MAX=$V
      sleep 0.1
    done
    for p in "${PIDS[@]}"; do wait "$p" 2>/dev/null || true; done
    DT=$(( $(date +%s) - T0 ))
    OK=1
    for i in $(seq 1 "$W"); do grep -q "STREAMED-WRITE" "/tmp/lightfs-perf/w-$W-$i.txt" || OK=0; done
    echo "writers=$W base_rss=${BASE}KB peak_rss=${MAX}KB delta=$(( MAX - BASE ))KB wall=${DT}s all_ok=$OK"
    for i in $(seq 1 "$W"); do grep -o "[0-9.]* MB/s" "/tmp/lightfs-perf/w-$W-$i.txt" | head -1 | xargs -I{} echo "    writer $i: {}" ; done
    # keep writer outputs for failure diagnosis
    mv /tmp/lightfs-perf/w-$W-*.txt /tmp/lightfs-perf/ 2>/dev/null || true
  done
} | tee "$OUT"

echo "results: $OUT"
