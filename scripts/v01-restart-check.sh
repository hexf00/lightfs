#!/usr/bin/env bash
# v0.1.0-experimental acceptance: the cross-process walk a first-time
# experimenter performs — initialize, start the server, real file
# operations (empty / 1 KiB / 8 MiB, overwrite), STOP the server
# process, start it AGAIN, verify every byte survived, delete, then run
# OFFLINE fsck on the stopped engine. Everything is isolated: its own
# bucket, its own metadata database, its own directories. No FUSE, no
# production resources.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh
export PATH="$HOME/.cargo/bin:$PATH"

PORT=18097
ROOT="$(mktemp -d /tmp/lightfs-v01-restart-XXXXXX)"
CFG="$ROOT/cfg.toml"
LOG="$ROOT/server.log"
BUCKET="lightfs-v01-restart-$$-$(date +%s)"
SRV_PID=""

cleanup() {
  if [[ -n "${SRV_PID:-}" ]]; then
    kill "$SRV_PID" 2>/dev/null || true
    wait "$SRV_PID" 2>/dev/null || true
    SRV_PID=""
  fi
}
trap cleanup EXIT

start_server() {
  ./target/release/lightfs server --config "$CFG" >>"$LOG" 2>&1 &
  SRV_PID=$!
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$PORT/health" && return 0
    # fail loudly if the server process died during startup
    kill -0 "$SRV_PID" 2>/dev/null || { tail -5 "$LOG" >&2; return 1; }
    sleep 0.2
  done
  echo "server did not come up" >&2
  return 1
}

stop_server() {
  kill "$SRV_PID" 2>/dev/null || true
  wait "$SRV_PID" 2>/dev/null || true
  SRV_PID=""
}

shell_cmd() { ./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" "$@" >/dev/null; }

mkdir -p "$ROOT/staging"
cat > "$CFG" <<EOF
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$ROOT/meta.db"
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
[writeback]
staging_dir = "$ROOT/staging"
[server]
bind = "127.0.0.1:$PORT"
EOF

cargo build --release -q
./target/release/lightfs create-bucket --config "$CFG" >/dev/null

echo "== start #1: initialize and operate"
start_server
printf 'mkdir /e2e\n' > "$ROOT/cmds.lfs"
shell_cmd script "$ROOT/cmds.lfs"
dd if=/dev/urandom of="$ROOT/empty.src" bs=1 count=0 status=none
: > "$ROOT/empty.src"
dd if=/dev/urandom of="$ROOT/small.src" bs=1024 count=1 status=none
dd if=/dev/urandom of="$ROOT/big.src" bs=1048576 count=8 status=none
./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" write /e2e/empty --stdin < "$ROOT/empty.src" >/dev/null
./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" write /e2e/small --stdin < "$ROOT/small.src" >/dev/null
./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" write /e2e/big --stdin < "$ROOT/big.src" >/dev/null
verify_file() {
  local path="$1" src="$2"
  ./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" cat "$path" > "$ROOT/readback"
  if [[ "$(shasum -a 256 < "$ROOT/readback" | cut -d' ' -f1)" != "$(shasum -a 256 < "$src" | cut -d' ' -f1)" ]]; then
    echo "content mismatch for $path" >&2
    return 1
  fi
}
verify_file /e2e/empty "$ROOT/empty.src"
verify_file /e2e/small "$ROOT/small.src"
verify_file /e2e/big "$ROOT/big.src"
echo "  ok: empty / 1KiB / 8MiB written and read back byte-exact"

echo "== overwrite and re-read"
dd if=/dev/urandom of="$ROOT/small2.src" bs=1024 count=1 status=none
./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" write /e2e/small --stdin < "$ROOT/small2.src" >/dev/null
verify_file /e2e/small "$ROOT/small2.src"
echo "  ok: overwrite reads back the new content"

echo "== STOP the server process, start #2, verify recovery"
stop_server
start_server
verify_file /e2e/empty "$ROOT/empty.src"
verify_file /e2e/small "$ROOT/small2.src"
verify_file /e2e/big "$ROOT/big.src"
./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" ls /e2e >/dev/null
echo "  ok: metadata and bytes survive a full process restart"

echo "== delete, then stop and run OFFLINE fsck"
printf 'rm /e2e/big\nassert-error cat /e2e/big\n' > "$ROOT/cmds.lfs"
shell_cmd script "$ROOT/cmds.lfs"
stop_server
# fsck exits 2 on any problem (set -e turns that into a failure)
./target/release/lightfs fsck --config "$CFG"
echo "  ok: delete honored, offline fsck clean"

rm -rf "$ROOT"
echo "V01 RESTART CHECK: PASSED (bucket $BUCKET kept for inspection)"
