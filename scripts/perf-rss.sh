#!/usr/bin/env bash
# Memory (peak RSS) and streaming validation for large-object paths.
# For each scenario the bench binary runs alone under /usr/bin/time -l, so
# "maximum resident set size" is that scenario's client peak. Server RSS is
# sampled separately during the concurrent-large-reads run.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

LABEL="${1:-rss}"
PORT=18085
OUT="/tmp/lightfs-perf/${LABEL}.txt"
mkdir -p /tmp/lightfs-perf

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build --release -q --bin lightfs --bin lightfs-bench

DB="/tmp/lightfs-rss-server-$$-$(date +%s).db"
CFG="/tmp/lightfs-rss-$$-$(date +%s).toml"
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
[cache.data]
enabled = false
EOF

SERVER_PID=""
sample_server() {
  # sample server RSS (KB) every 0.2s while a foreground job runs
  (
    local max=0
    while kill -0 "$SERVER_PID" 2>/dev/null; do
      local v
      v=$(ps -o rss= -p "$SERVER_PID" 2>/dev/null | tr -d ' ')
      [[ -n "$v" && "$v" -gt "$max" ]] && max=$v
      sleep 0.2
    done
    echo "$max" > /tmp/lightfs-perf/server-rss-$$.txt
  ) &
  SAMPLER_PID=$!
}

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-rss-server.log 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && break; sleep 0.2; done

B="./target/release/lightfs-bench"
S3ARGS=(--s3-endpoint "http://127.0.0.1:9000" --bucket "${LIGHTFS_S3_BUCKET:-lightfs-bench}")
LFSARGS=(--server "http://127.0.0.1:$PORT" "${S3ARGS[@]}")

{
  echo "# RSS + streaming validation — $LABEL"
  echo "# (RSS = client peak via /usr/bin/time -l; server peak sampled separately)"

  echo "== [1] prepare dataset (256MiB raw + 256MiB lfs)"
  $B "${S3ARGS[@]}" --scenario prepare --large-mb 256 2>&1 | grep -v "^$"

  echo "== [2] BUFFERED large read (client.read -> whole Vec, 256MiB)"
  /usr/bin/time -l $B "${LFSARGS[@]}" --scenario lfs-large --lfs-large-mb 256 2>&1 | grep -E "^lfs|maximum resident"

  echo "== [3] STREAMED large read (read_stream -> 64KiB buffers, 256MiB)"
  /usr/bin/time -l $B "${LFSARGS[@]}" --scenario lfs-stream-large --lfs-large-mb 256 2>&1 | grep -E "^lfs|maximum resident"

  echo "== [4] STREAMED large write (write_from_reader -> staged file, 256MiB)"
  /usr/bin/time -l $B "${LFSARGS[@]}" --scenario lfs-stream-write --lfs-large-mb 256 2>&1 | grep -E "^lfs|maximum resident"

  echo "== [5] 4 concurrent STREAMED reads of 256MiB (client RSS each; server peak sampled)"
  sample_server
  PIDS=()
  for i in 1 2 3 4; do
    /usr/bin/time -l $B "${LFSARGS[@]}" --scenario lfs-stream-large --lfs-large-mb 256 > "/tmp/lightfs-perf/conc-$i.txt" 2>&1 &
    PIDS+=($!)
  done
  for p in "${PIDS[@]}"; do wait "$p"; done
  kill "$SAMPLER_PID" 2>/dev/null || true
  for i in 1 2 3 4; do
    grep -E "MB/s" "/tmp/lightfs-perf/conc-$i.txt" || true
    grep "maximum resident" "/tmp/lightfs-perf/conc-$i.txt" | awk '{print "    client peak RSS: " $1 " bytes"}'
  done
  if [[ -f /tmp/lightfs-perf/server-rss-$$.txt ]]; then
    echo "    server peak RSS during run: $(cat /tmp/lightfs-perf/server-rss-$$.txt) KB"
  fi

  echo "== [6] raw S3 256MiB full GET (reference point)"
  /usr/bin/time -l $B "${S3ARGS[@]}" --scenario raw-large --large-mb 256 2>&1 | grep -E "^raw|maximum resident"
} | tee "$OUT"

echo "results: $OUT"
