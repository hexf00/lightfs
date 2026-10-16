#!/usr/bin/env bash
# Server worker-count experiment: does throughput track worker count
# (concurrency-limited) or plateau with high CPU (serialization/CPU-limited)?
# Also compares presigned direct-S3 reads vs the proxied path at the same
# client concurrency.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

PORT=18085
OUT="/tmp/lightfs-perf/workers.txt"
mkdir -p /tmp/lightfs-perf

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build --release -q --bin lightfs --bin lightfs-bench

DB="/tmp/lightfs-workers-server-$$-$(date +%s).db"
CFG="/tmp/lightfs-workers-$$-$(date +%s).toml"
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

# cumulative process CPU time as seconds (macOS ps -o time= "MM:SS.ss")
cpu_seconds() {
  local t
  t=$(ps -o time= -p "$1" 2>/tmp/lightfs-ps-err.txt | tr -d ' ')
  if [[ -z "$t" ]]; then echo 0; return; fi
  local h=0 m=0 s=0
  case "$t" in
    *:*.*) ;;
    *.*) t="00:$t" ;;
  esac
  IFS=: read -r a b c <<< "$t"
  if [[ -n "${c:-}" ]]; then h=$a; m=$b; s=$c; else m=$a; s=$b; fi
  "$HOME/tools/gitpy/bin/python" -c "print(round($h*3600+$m*60+$s, 3))"
}

SERVER_PID=""
start_server() {
  LIGHTFS_SERVER_WORKERS="$1" ./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-workers-server.log 2>&1 &
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && return 0
    # A dead server pid means bind failed (port taken by a leftover
    # process) — the health check would then be answering from THAT
    # leftover and silently invalidate every measurement.
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      echo "ERROR: server pid $SERVER_PID died at startup (is port $PORT taken?)" >&2
      exit 1
    fi
    sleep 0.2
  done
  echo "server did not start" >&2; exit 1
}

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

# millisecond wall clock (macOS date lacks %N; no bc on a CLT-less mac)
now_ms() { "${PYTHON:-$HOME/tools/gitpy/bin/python}" -c 'import time; print(int(time.time()*1000))'; }
OBJECTS="${LIGHTFS_BENCH_OBJECTS:-6000}"
OBJ_SIZE="${LIGHTFS_BENCH_OBJ_SIZE:-100000}"

start_server 8
# one-time lfs dataset import
./target/release/lightfs-bench --server "http://127.0.0.1:$PORT" --scenario lfs-serial >/dev/null

{
  echo "# worker-count experiment + presign vs proxy — $(date)"
  echo "# client concurrency x server workers: aggregate MB/s (server CPU%, RSS KB)"
  for W in 8 16 32 64; do
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
    sleep 0.5
    start_server "$W"
    echo "== server workers = $W"
    echo "  (one row per scenario: raw cpu-time delta, wall, requests, bytes, avg cores, measured MB/s, and the CPU-model MB/s computed from THAT row)"
    for C in 8 16 32 64; do
      CPU0=$(cpu_seconds "$SERVER_PID"); T0=$(now_ms)
      ./target/release/lightfs-bench --server "http://127.0.0.1:$PORT" --scenario "lfs-par$C" 2>&1 | sed "s/^/    /"
      CPU1=$(cpu_seconds "$SERVER_PID"); T1=$(now_ms)
      RSS=$(ps -o rss= -p "$SERVER_PID" 2>/dev/null | tr -d ' ')
      ROW=$("$HOME/tools/gitpy/bin/python" -c "
req = $OBJECTS  # one read per file; client concurrency C only changes in-flight depth
b = req * $OBJ_SIZE
d = max($CPU1 - $CPU0, 0.0)
w_ms = max($T1 - $T0, 1); w = w_ms / 1000.0
cores = d / w
mbps = b / 1048576 / w
cpu_ms = d * 1000 / req
theo = cores * b / 1048576 / d   # cores / (cpu-seconds per byte) -> MB/s ceiling from THIS row
print(f'reqs={req} bytes={b} wall_ms={w_ms:.0f} cpu_delta_s={d:.3f} cores_avg={cores:.2f} cpu_ms_per_req={cpu_ms:.3f} measured={mbps:.1f}MB/s cpu_model_ceiling={theo:.1f}MB/s rss=${RSS}KB')")
      echo "      $ROW"
    done
  done

  echo "== presigned direct-S3 reads vs proxied (server workers = 64)"
  kill "$SERVER_PID" 2>/dev/null || true
  sleep 0.5
  start_server 64
  echo "  proxied:"
  ./target/release/lightfs-bench --server "http://127.0.0.1:$PORT" --scenario lfs-par8 2>&1 | sed "s/^/    /"
  echo "  presigned direct:"
  ./target/release/lightfs-bench --server "http://127.0.0.1:$PORT" --scenario lfs-presign-par8 2>&1 | sed "s/^/    /"
} | tee "$OUT"

echo "results: $OUT"
