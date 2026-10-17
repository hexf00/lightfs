#!/usr/bin/env bash
# S4.19 §六 performance comparison: the SAME lightfs-bench scenario set,
# same machine, same MinIO, same parameters — v0.1 (frozen main) vs vnext.
# No pass/fail gate: raw numbers for the report (the task forbids a
# mechanical 5% threshold on noisy single-run data).
#
# usage: scripts/s419-perf-compare.sh PATH_TO_V01_BENCH PATH_TO_VNEXT_BENCH
set -euo pipefail
cd "$(dirname "$0")/.."
V01_BENCH="${1:?path to v0.1 lightfs-bench}"
VNEXT_BENCH="${2:?path to vnext lightfs-bench}"
ENDPOINT="${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"

run_side() {
  local label="$1" port="$2" binary="$3"
  local root bucket srv_pid=""
  root="$(mktemp -d "/tmp/lightfs-perf-${label}-XXXXXX")"
  bucket="lightfs-perf-${label}-$$-$(date +%s)"
  local cfg="$root/cfg.toml"
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
  ./target/release/lightfs create-bucket --config "$cfg" >/dev/null
  ./target/release/lightfs server --config "$cfg" >"$root/server.log" 2>&1 &
  srv_pid=$!
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$port/health" && break
    kill -0 "$srv_pid" 2>/dev/null || { tail -5 "$root/server.log" >&2; return 1; }
    sleep 0.2
  done
  echo "===== $label ====="
  # keep stderr: the import line (product write path) is the WRITE-side
  # measurement of the comparison
  "$binary" --server "http://127.0.0.1:$port" \
    --objects 300 --obj-size 8192 --lfs-large-mb 8 \
    --scenario lfs-serial,lfs-large,lfs-par4
  kill "$srv_pid" 2>/dev/null || true
  wait "$srv_pid" 2>/dev/null || true
  rm -rf "$root"
}

run_side v01 18093 "$V01_BENCH"
run_side vnext 18094 "$VNEXT_BENCH"
