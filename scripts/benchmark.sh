#!/usr/bin/env bash
# Phase 13 benchmark: 6000 files / ~600MB default (override with env:
# LIGHTFS_BENCH_COUNT, LIGHTFS_BENCH_SIZE). Measures generate, find/tree,
# read-all, rename, concurrent clients; records wall time and the server's
# S3/metadata counters (SPEC §10).
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"
COUNT="${LIGHTFS_BENCH_COUNT:-6000}"
SIZE="${LIGHTFS_BENCH_SIZE:-102400}"
PORT="${LIGHTFS_BENCH_PORT:-18083}"
start_minio
cargo build --release -q

DB="/tmp/lightfs-bench-$$-$(date +%s).db"
CFG="/tmp/lightfs-bench-$$-$(date +%s).toml"
cat > "$CFG" <<EOF
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
ttl_seconds = 5
[cache.data]
enabled = false
EOF

SHELL="target/release/lightfs-shell"
URL="http://127.0.0.1:$PORT"
SERVER_PID=""
cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"
}
trap cleanup EXIT

./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-bench-server.log 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do curl -s -m 1 -o /dev/null "$URL/health" && break; sleep 0.2; done
# dedicated bucket: metrics/orphan accounting stays clean
target/release/lightfs create-bucket --config "$CFG"

metrics() {
  curl -s -X POST "$URL/rpc" -H 'Content-Type: application/json' \
    -d '{"id":1,"method":"metrics","params":{}}'
}

# macOS date lacks %N and CLT-less Macs lack bc/python3: use the uv python.
now_ms() { "${PYTHON:-$HOME/tools/gitpy/bin/python}" -c 'import time; print(int(time.time()*1000))'; }
# bash-only ms arithmetic (no bc on a CLT-less Mac)
secs() { echo "$(( $2 - $1 ))ms"; }

echo "== dataset: $COUNT files x ~${SIZE}B (~$((COUNT * SIZE / 1048576)) MB)"
M0=$(metrics); T0=$(now_ms)
$SHELL --config "$CFG" --server "$URL" bench-generate /bench "$COUNT" "$SIZE"
T1=$(now_ms)
echo "generate wall: $(secs $T0 $T1)"

T2=$(now_ms)
$SHELL --config "$CFG" --server "$URL" tree /bench > /tmp/lightfs-bench-tree.txt
T3=$(now_ms)
echo "find/tree COLD wall (fresh process, cold connection): $(secs $T2 $T3) ($(wc -l < /tmp/lightfs-bench-tree.txt) entries)"

T2b=$(now_ms)
$SHELL --config "$CFG" --server "$URL" tree /bench > /tmp/lightfs-bench-tree2.txt
T3b=$(now_ms)
echo "find/tree WARM wall (fresh process, server warm): $(secs $T2b $T3b)"

T4=$(now_ms)
$SHELL --config "$CFG" --server "$URL" bench-read /bench
T5=$(now_ms)
echo "read-all wall SERIAL: $(secs $T4 $T5)"

T4b=$(now_ms)
$SHELL --config "$CFG" --server "$URL" bench-read /bench 8
T5b=$(now_ms)
echo "read-all wall 8 IN-FLIGHT: $(secs $T4b $T5b)"

T5c=$(now_ms)
$SHELL --config "$CFG" --server "$URL" bench-grep needle- /bench 8
T5d=$(now_ms)
echo "content-scan wall 8 IN-FLIGHT (needle- prefix, expect COUNT matches): $(secs $T5c $T5d)"

T6=$(now_ms)
$SHELL --config "$CFG" --server "$URL" mv /bench/shard-00 /bench/renamed-shard
T7=$(now_ms)
echo "dir rename wall: $(secs $T6 $T7) (metadata-only, no byte movement)"

echo "== concurrent writers: 4 x 50 files"
T8=$(now_ms)
BENCH_PIDS=()
for c in 1 2 3 4; do
  ( $SHELL --config "$CFG" --server "$URL" bench-generate /bench/concurrent-$c 50 10240 ) &
  BENCH_PIDS+=($!)
done
for pid in "${BENCH_PIDS[@]}"; do wait "$pid"; done
T9=$(now_ms)
echo "concurrent write wall: $(secs $T8 $T9)"

echo "== concurrent readers: 4 processes x full read of /bench"
T10=$(now_ms)
BENCH_PIDS=()
for c in 1 2 3 4; do
  ( $SHELL --config "$CFG" --server "$URL" bench-read /bench ) &
  BENCH_PIDS+=($!)
done
for pid in "${BENCH_PIDS[@]}"; do wait "$pid"; done
T11=$(now_ms)
echo "concurrent read wall (4x aggregate): $(secs $T10 $T11)"

echo "== server metrics delta over the whole run"
PY="${PYTHON:-$HOME/tools/gitpy/bin/python}"
"$PY" - "$M0" "$(metrics)" <<'PYEOF'
import json, sys
a, b = (json.loads(x)["result"] for x in sys.argv[1:3])
for k in sorted(b):
    print(f"  {k}: {a.get(k, 0)} -> {b[k]} (+{b[k] - a.get(k, 0)})")
PYEOF
echo "BENCHMARK OK"
