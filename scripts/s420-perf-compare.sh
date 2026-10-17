#!/usr/bin/env bash
# S4.20 §四: request-cost & performance comparison, 5 rounds per side.
# SAME machine, SAME MinIO, SAME parameters, fresh bucket+db+server per
# round (the write-side import must be exercised every round). Prints
# per-round lines: import seconds (product write path), bench rows, and
# the server's object-op counters (PUT/GET/HEAD/LIST + bytes) so the
# per-logical-write object cost is explicit. No gate — raw numbers.
#
# usage: scripts/s420-perf-compare.sh PATH_TO_BASELINE_BENCH PATH_TO_NEW_BENCH
set -euo pipefail
cd "$(dirname "$0")/.."
BASE_BENCH="${1:?path to baseline lightfs-bench}"
NEW_BENCH="${2:?path to new lightfs-bench}"
ENDPOINT="${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"

run_round() {
  local label="$1" round="$2" port="$3" binary="$4" repo="$5"
  local root bucket srv_pid="" cfg
  root="$(mktemp -d "/tmp/lightfs-perf-${label}-XXXXXX")"
  bucket="lightfs-perf-${label}-r${round}-$$-$RANDOM-$(date +%s)"
  cfg="$root/cfg.toml"
  cat > "$cfg" <<CFGEOF
[s3]
endpoint = "$ENDPOINT"
region = "us-east-1"
bucket = "$bucket"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$root/meta.db"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
[server]
bind = "127.0.0.1:$port"
CFGEOF
  # CRITICAL (S4.20 lesson): the SERVER must be the side's own binary.
  # Comparing two bench clients against one server measures nothing.
  "$repo/target/release/lightfs" create-bucket --config "$cfg" >/dev/null
  "$repo/target/release/lightfs" server --config "$cfg" >"$root/server.log" 2>&1 &
  srv_pid=$!
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$port/health" && break
    kill -0 "$srv_pid" 2>/dev/null || { tail -5 "$root/server.log" >&2; return 1; }
    sleep 0.2
  done
  local out
  out=$("$binary" --server "http://127.0.0.1:$port" \
    --objects 300 --obj-size 8192 --lfs-large-mb 8 \
    --scenario lfs-serial,lfs-large,lfs-par4 2>&1)
  local import_s
  import_s=$(echo "$out" | sed -n 's/.*product write path in \([0-9.]*\)s.*/\1/p')
  local metrics
  metrics=$(curl -s "http://127.0.0.1:$port/rpc" -X POST -d '{"id":1,"method":"metrics","params":{}}')
  echo "RESULT|$label|r$round|import_s=$import_s|$(echo "$out" | grep '^lfs ' | tr '\n' '|')|$(echo "$metrics" | python3 -c "
import sys, json
m = json.load(sys.stdin)['result']
print(f\"put={m['s3_put']} get={m['s3_get']} head={m['s3_head']} list={m['s3_list']} bytes_w={m['bytes_written']} bytes_r={m['bytes_read']}\")")"
  kill "$srv_pid" 2>/dev/null || true
  wait "$srv_pid" 2>/dev/null || true
  rm -rf "$root"
}

BASE_REPO="${BASE_REPO:?dir with the baseline repo (server binary)}"
NEW_REPO="${NEW_REPO:-$(pwd)}"
for r in 1 2 3 4 5; do
  run_round base "$r" 18093 "$BASE_BENCH" "$BASE_REPO"
  run_round s420 "$r" 18094 "$NEW_BENCH" "$NEW_REPO"
done
