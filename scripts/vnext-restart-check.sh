#!/usr/bin/env bash
# vnext acceptance (S4.19 §六.6): the cross-process walk a first-time
# experimenter performs — initialize, start the server, real file
# operations (empty / 1 KiB / 8 MiB, overwrite), STOP the server, start
# it AGAIN, verify every byte survived, then SIGKILL the server mid-life
# (crash, no cleanup at all) and verify a fresh process recovers
# everything. Isolated: own bucket, own metadata DB. No FUSE, no
# production resources.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh
export PATH="$HOME/.cargo/bin:$PATH"

PORT=18098
ROOT="$(mktemp -d /tmp/lightfs-vnext-restart-XXXXXX)"
CFG="$ROOT/cfg.toml"
LOG="$ROOT/server.log"
BUCKET="lightfs-vnext-restart-$$-$(date +%s)"
SRV_PID=""

cleanup() {
  if [[ -n "${SRV_PID:-}" ]]; then
    kill -9 "$SRV_PID" 2>/dev/null || true
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
    kill -0 "$SRV_PID" 2>/dev/null || { tail -5 "$LOG" >&2; return 1; }
    sleep 0.2
  done
  echo "server did not come up" >&2
  return 1
}

shell_cmd() { ./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" "$@" >/dev/null; }

verify_file() {
  local path="$1" src="$2"
  ./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" cat "$path" > "$ROOT/readback"
  if [[ "$(shasum -a 256 < "$ROOT/readback" | cut -d' ' -f1)" != "$(shasum -a 256 < "$src" | cut -d' ' -f1)" ]]; then
    echo "content mismatch for $path" >&2
    return 1
  fi
}

mkdir -p "$ROOT"
cat > "$CFG" <<CFGEOF
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
enabled = false
[cache.data]
enabled = false
[server]
bind = "127.0.0.1:$PORT"
CFGEOF

cargo build --release -q
./target/release/lightfs create-bucket --config "$CFG" >/dev/null

echo "== start #1: initialize and operate"
start_server
printf 'mkdir /e2e\n' > "$ROOT/cmds.lfs"
shell_cmd script "$ROOT/cmds.lfs"
: > "$ROOT/empty.src"
dd if=/dev/urandom of="$ROOT/small.src" bs=1024 count=1 status=none
dd if=/dev/urandom of="$ROOT/big.src" bs=1048576 count=8 status=none
S="http://127.0.0.1:$PORT"
./target/release/lightfs-shell --config "$CFG" --server "$S" write /e2e/empty --stdin < "$ROOT/empty.src" >/dev/null
./target/release/lightfs-shell --config "$CFG" --server "$S" write /e2e/small --stdin < "$ROOT/small.src" >/dev/null
./target/release/lightfs-shell --config "$CFG" --server "$S" write /e2e/big --stdin < "$ROOT/big.src" >/dev/null
verify_file /e2e/empty "$ROOT/empty.src"
verify_file /e2e/small "$ROOT/small.src"
verify_file /e2e/big "$ROOT/big.src"
echo "  ok: empty / 1KiB / 8MiB written and read back byte-exact"

echo "== overwrite and re-read"
dd if=/dev/urandom of="$ROOT/small2.src" bs=1024 count=1 status=none
./target/release/lightfs-shell --config "$CFG" --server "$S" write /e2e/small --stdin < "$ROOT/small2.src" >/dev/null
verify_file /e2e/small "$ROOT/small2.src"
echo "  ok: overwrite reads back the new content"

echo "== STOP the server, start #2, verify recovery"
kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; SRV_PID=""
start_server
verify_file /e2e/empty "$ROOT/empty.src"
verify_file /e2e/small "$ROOT/small2.src"
verify_file /e2e/big "$ROOT/big.src"
shell_cmd ls /e2e
echo "  ok: metadata and bytes survive a graceful process restart"

echo "== SIGKILL the server (crash, zero cleanup), start #3, verify recovery"
kill -9 "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; SRV_PID=""
start_server
verify_file /e2e/empty "$ROOT/empty.src"
verify_file /e2e/small "$ROOT/small2.src"
verify_file /e2e/big "$ROOT/big.src"
echo "  ok: a killed server leaves a fully recoverable engine"

echo "== delete honored across one more restart"
printf 'rm /e2e/big\n' > "$ROOT/cmds.lfs"
shell_cmd script "$ROOT/cmds.lfs"
kill "$SRV_PID" 2>/dev/null || true; wait "$SRV_PID" 2>/dev/null || true; SRV_PID=""
start_server
if ./target/release/lightfs-shell --config "$CFG" --server "$S" cat /e2e/big >/dev/null 2>&1; then
  echo "deleted file reappeared" >&2; exit 1
fi
shell_cmd ls /e2e
echo "  ok: delete is durable"

rm -rf "$ROOT"
echo "VNEXT RESTART CHECK: PASSED (bucket $BUCKET kept for inspection)"
