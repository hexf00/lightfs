#!/usr/bin/env bash
# Experiment suite from the performance review:
#   A. small-block random reads + metadata ops (IOPS/latency bound)
#   B. 256 MiB streaming write: multipart concurrency sweep (1/2/4/8)
#   C. minimal Node.js proxy vs LightFS server proxy (same work, same bench)
# All three share one dataset and one MinIO instance.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

PORT=18085
PROXY_PORT=18086
OUT="/tmp/lightfs-perf/compare.txt"
mkdir -p /tmp/lightfs-perf

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build --release -q --bin lightfs --bin lightfs-bench

DB="/tmp/lightfs-cmp-server-$$-$(date +%s).db"
CFG="/tmp/lightfs-cmp-$$-$(date +%s).toml"
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

SERVER_PID="" NODE_PID=""
cpu_seconds() {
  local t
  t=$(ps -o time= -p "$1" 2>/dev/null | tr -d ' ')
  if [[ -z "$t" ]]; then echo 0; return; fi
  local h=0 m=0 s=0 a b c
  IFS=: read -r a b c <<< "$t"
  if [[ -n "${c:-}" ]]; then h=$a; m=$b; s=$c; else m=$a; s=$b; fi
  "$HOME/tools/gitpy/bin/python" -c "print(round($h*3600+$m*60+$s, 3))"
}
now_ms() { "${PYTHON:-$HOME/tools/gitpy/bin/python}" -c 'import time; print(int(time.time()*1000))'; }

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  [[ -n "${NODE_PID:-}" ]] && kill "$NODE_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

start_server() {
  LIGHTFS_SERVER_WORKERS="${1:-8}" LIGHTFS_S3_PART_CONCURRENCY="${LIGHTFS_S3_PART_CONCURRENCY:-1}" ./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-cmp-server.log 2>&1 &
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

B="./target/release/lightfs-bench"
S3ARGS=(--s3-endpoint "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}" --bucket "${LIGHTFS_S3_BUCKET:-lightfs-bench}")
LFSARGS=(--server "http://127.0.0.1:$PORT" "${S3ARGS[@]}")

# dataset (raw S3 objects; also used by the node proxy)
"$B" "${S3ARGS[@]}" --scenario prepare >/dev/null

start_server 8
# one-time LightFS import of the same dataset
"$B" "${LFSARGS[@]}" --scenario lfs-serial >/dev/null

{
  echo "# review experiment suite — $(date) (loopback; server workers=8)"
  echo
  echo "===== A. small-block random reads + metadata ops ====="
  echo "-- raw S3 4KiB random (baseline):"
  "$B" "${S3ARGS[@]}" --scenario raw-4k-rand
  echo "-- LightFS 4KiB random (proxy path), 1 and 8 in-flight:"
  "$B" "${LFSARGS[@]}" --scenario lfs-4k-rand
  "$B" "${LFSARGS[@]}" --scenario lfs-4k-rand8
  echo "-- LightFS metadata (lookup) ops, 1 and 8 in-flight:"
  "$B" "${LFSARGS[@]}" --scenario lfs-meta
  "$B" "${LFSARGS[@]}" --scenario lfs-meta-par8
  echo

  echo "===== B. 256 MiB streaming write: multipart concurrency sweep ====="
  for P in 1 2 4 8; do
    # part concurrency is a SERVER-side setting: restart the server with it
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    LIGHTFS_S3_PART_CONCURRENCY=$P start_server 8
    CPU0=$(cpu_seconds "$SERVER_PID"); T0=$(now_ms)
    ./target/release/lightfs-bench --server "http://127.0.0.1:$PORT" --scenario lfs-stream-write --lfs-large-mb 256 2>&1 | sed 's/^/  /'
    CPU1=$(cpu_seconds "$SERVER_PID"); T1=$(now_ms)
    ROW=$("$HOME/tools/gitpy/bin/python" -c "
d = max($CPU1 - $CPU0, 0.0); w = max($T1 - $T0, 1) / 1000.0
print(f'  server cpu {d:.2f}s = {d/w:.2f} cores avg over {w:.1f}s')")
    echo "$ROW"
  done
  echo

  echo "===== C. minimal Node.js proxy vs LightFS proxy (same objects) ====="
  # make objects anonymously readable, start the thin node proxy
  target/release/lightfs create-bucket --config "$CFG" --insecure-public-rw
  ORIGIN_HOST=127.0.0.1 ORIGIN_PORT=9000 BUCKET="${LIGHTFS_S3_BUCKET:-lightfs-bench}" PROXY_PORT=$PROXY_PORT \
    "$HOME/tools/node/bin/node" scripts/proxy/node-proxy.js >/tmp/lightfs-cmp-node.log 2>&1 &
  NODE_PID=$!
  sleep 1
  kill -0 "$NODE_PID" 2>/dev/null || { echo "node proxy failed:"; cat /tmp/lightfs-cmp-node.log; exit 1; }
  PROXY_URL="http://127.0.0.1:$PROXY_PORT"

  CPU0=$(cpu_seconds "$NODE_PID"); T0=$(now_ms)
  LIGHTFS_PROXY_URL="$PROXY_URL" "$B" "${S3ARGS[@]}" --scenario raw-proxy-par
  LIGHTFS_PROXY_URL="$PROXY_URL" "$B" "${S3ARGS[@]}" --scenario raw-proxy-par8
  CPU1=$(cpu_seconds "$NODE_PID"); T1=$(now_ms)
  NRSS=$(ps -o rss= -p "$NODE_PID" 2>/dev/null | tr -d ' ')
  NROW=$("$HOME/tools/gitpy/bin/python" -c "
d = max($CPU1 - $CPU0, 0.0); w = max($T1 - $T0, 1) / 1000.0
print(f'  node proxy cpu {d:.2f}s = {d/w:.2f} cores avg; rss=${NRSS}KB')")
  echo "$NROW"

  CPU0=$(cpu_seconds "$SERVER_PID"); T0=$(now_ms)
  "$B" "${LFSARGS[@]}" --scenario lfs-serial
  "$B" "${LFSARGS[@]}" --scenario lfs-par8
  CPU1=$(cpu_seconds "$SERVER_PID"); T1=$(now_ms)
  SRSS=$(ps -o rss= -p "$SERVER_PID" 2>/dev/null | tr -d ' ')
  SROW=$("$HOME/tools/gitpy/bin/python" -c "
d = max($CPU1 - $CPU0, 0.0); w = max($T1 - $T0, 1) / 1000.0
print(f'  lightfs server cpu {d:.2f}s = {d/w:.2f} cores avg; rss=${SRSS}KB')")
  echo "$SROW"
} | tee "$OUT"

echo "results: $OUT"
