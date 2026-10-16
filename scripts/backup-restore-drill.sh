#!/usr/bin/env bash
# Backup / restore drill (S4.14.2 form, against the CURRENT backup format):
#
#   [backup].root = isolated directory; every published backup is
#   <root>/<id>/{meta.db, meta.db.manifest, backup.json}.
#
#   [1] build a small namespace through server + client RPC
#   [2] record the pre-backup tree and checksums; fsck runs OFFLINE
#   [3] ONLINE `lightfs backup` while the server keeps running; assert the
#       published directory contains the three required files
#   [4] mutate AFTER the snapshot: add, overwrite, delete
#   [5] restore = copy the published meta.db into an INDEPENDENT database
#       (never treat the backup directory itself as one SQLite file) and
#       serve it with its own server; tree/checksums must equal the
#       pre-backup state and the post-backup file must be absent
#   [6] fsck the restored namespace OFFLINE (restore server stopped)
#   [7] gc with BOTH servers stopped, current gc output asserted, then the
#       restored server comes back and the manifest-protected history must
#       still read byte-exact
#
# Every server gets a bounded health-check readiness wait, a reliable
# stop-and-wait, and both are stopped before the temp tree is removed.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

PORT=18087
RPORT=18090
BUCKET="lightfs-drill-$$-$(date +%s)"
OUT="/tmp/lightfs-perf/backup-drill.txt"
RUN="/tmp/lightfs-drill-$$-$(date +%s)"
BACKUPS="$RUN/backups"
mkdir -p "$RUN" "$BACKUPS" /tmp/lightfs-perf

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build --release -q --bin lightfs --bin lightfs-shell

CFG="$RUN/drill.toml"
cat > "$CFG" <<CFGEOF
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/metadata.db"
[backup]
root = "$BACKUPS"
[server]
bind = "127.0.0.1:$PORT"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
CFGEOF

MAIN_PID=""
RSRV_PID=""
stop_main() {
  if [[ -n "${MAIN_PID:-}" ]]; then
    kill "$MAIN_PID" 2>/dev/null || true
    wait "$MAIN_PID" 2>/dev/null || true
    if kill -0 "$MAIN_PID" 2>/dev/null; then
      kill -9 "$MAIN_PID" 2>/dev/null || true
      wait "$MAIN_PID" 2>/dev/null || true
    fi
    MAIN_PID=""
  fi
}
stop_rsrv() {
  if [[ -n "${RSRV_PID:-}" ]]; then
    kill "$RSRV_PID" 2>/dev/null || true
    wait "$RSRV_PID" 2>/dev/null || true
    if kill -0 "$RSRV_PID" 2>/dev/null; then
      kill -9 "$RSRV_PID" 2>/dev/null || true
      wait "$RSRV_PID" 2>/dev/null || true
    fi
    RSRV_PID=""
  fi
}
cleanup() { stop_main; stop_rsrv; rm -rf "$RUN"; }
trap cleanup EXIT

# require_port_free PORT: refuse to start if a stale server already holds the
# port (a leaked process would answer our health checks and poison the run).
require_port_free() {
  if lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | grep -q LISTEN; then
    echo "ERROR: port $1 is already in use; kill the stale process first" >&2
    lsof -nP -iTCP:"$1" -sTCP:LISTEN >&2 || true
    exit 1
  fi
}

# wait_server PORT PID LOG: bounded health-check readiness, hard-fails if
# the process dies before the endpoint answers.
wait_server() {
  local port=$1 pid=$2 log=$3
  for _ in $(seq 1 100); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$port/health" && return 0
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "ERROR: server on $port died during startup (log: $log)" >&2
      tail -5 "$log" >&2 || true
      exit 1
    fi
    sleep 0.2
  done
  echo "ERROR: server on $port never became ready" >&2
  exit 1
}
start_main() {
  require_port_free "$PORT"
  ./target/release/lightfs server --config "$CFG" >>"$RUN/main-server.log" 2>&1 &
  MAIN_PID=$!
  wait_server "$PORT" "$MAIN_PID" "$RUN/main-server.log"
}

./target/release/lightfs create-bucket --config "$CFG" >/dev/null
lsh() { ./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" "$@"; }
rsh() { ./target/release/lightfs-shell --config "$RC" --server "http://127.0.0.1:$RPORT" "$@"; }
sha() { "${SHA_TOOL:-shasum}" -a 256 "$@"; }

# The body stays in THIS shell (process substitution, not a pipe): this bash
# (macOS 3.2) does not fire EXIT traps in pipeline subshells, so `{ ... } | tee`
# would leak both servers on any mid-body failure. A top-level trap plus
# same-shell kill/wait is what makes the stops reliable.
exec > >(tee "$OUT") 2>&1

echo "# backup/restore drill - $(date) (bucket $BUCKET)"
start_main

echo "== [1] build namespace (server + client RPC)"
lsh mkdir /docs >/dev/null
printf 'contract draft v1\n' | lsh write /docs/a.txt --stdin >/dev/null
head -c 1048576 /dev/urandom > "$RUN/random.bin"
lsh write /docs/binary.bin --stdin < "$RUN/random.bin" >/dev/null
echo "  files: /docs/a.txt (18B), /docs/binary.bin (1MiB random)"

echo "== [2] record the pre-backup state; fsck OFFLINE (server stopped)"
stop_main
./target/release/lightfs fsck --config "$CFG" | grep -q clean
echo "  fsck: clean"
start_main
lsh tree / | sort > "$RUN/tree-before.txt"
lsh cat /docs/a.txt | sha > "$RUN/sum-a.txt"
lsh cat /docs/binary.bin | sha > "$RUN/sum-b.txt"

echo "== [3] ONLINE backup while the server keeps running"
PUB=$(./target/release/lightfs backup --config "$CFG" | tee /dev/stderr \
      | sed -n 's/^backup published: \([^ ]*\) .*/\1/p')
[[ -n "$PUB" ]] || { echo "  ERROR: could not parse the published backup path" >&2; exit 1; }
for f in meta.db meta.db.manifest backup.json; do
  [[ -f "$PUB/$f" ]] || { echo "  ERROR: published backup lacks $f" >&2; exit 1; }
done
echo "  published: $PUB (meta.db + manifest + backup.json present)"

echo "== [4] mutate AFTER the snapshot: add, overwrite, delete"
printf 'after-the-snapshot\n' | lsh write /docs/post.txt --stdin >/dev/null
printf 'contract draft v2 OVERWRITTEN\n' | lsh write /docs/a.txt --stdin >/dev/null
lsh rm /docs/binary.bin >/dev/null
lsh cat /docs/a.txt | grep -q OVERWRITTEN
echo "  mutations applied: post.txt added, a.txt overwritten, binary.bin deleted"

echo "== [5] restore: published meta.db -> independent db under its own server"
cp "$PUB/meta.db" "$RUN/restored.db"
RC="$RUN/restored.toml"
cat > "$RC" <<RESTOREOF
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/restored.db"
[backup]
root = "$BACKUPS"
[server]
bind = "127.0.0.1:$RPORT"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
RESTOREOF
./target/release/lightfs server --config "$RC" >>"$RUN/restored-server.log" 2>&1 &
RSRV_PID=$!
wait_server "$RPORT" "$RSRV_PID" "$RUN/restored-server.log"
rsh tree / | sort > "$RUN/tree-restored.txt"
diff "$RUN/tree-before.txt" "$RUN/tree-restored.txt" \
  && echo "  file tree: identical to the pre-backup state (post.txt absent)"
rsh cat /docs/a.txt | sha | diff "$RUN/sum-a.txt" - \
  && echo "  a.txt: OVERWRITE after backup, pre-backup bytes restored byte-exact"
rsh cat /docs/binary.bin | sha | diff "$RUN/sum-b.txt" - \
  && echo "  binary.bin: DELETE after backup, pre-backup bytes restored byte-exact"

echo "== [6] fsck the restored namespace OFFLINE (restore server stopped)"
stop_rsrv
if ./target/release/lightfs fsck --config "$RC" | tee /tmp/lightfs-perf/drill-fsck-restore.txt | grep -q "MISSING OBJECT"; then
  echo "  ERROR: restored fsck reports MISSING OBJECT - retention failed" >&2
  exit 1
fi
echo "  restored fsck: zero missing objects"
start_rsrv() {
  require_port_free "$RPORT"
  ./target/release/lightfs server --config "$RC" >>"$RUN/restored-server.log" 2>&1 &
  RSRV_PID=$!
  wait_server "$RPORT" "$RSRV_PID" "$RUN/restored-server.log"
}
start_rsrv

echo "== [7] gc with BOTH servers stopped; protected history must survive"
stop_main
stop_rsrv
GC_OUT=$(./target/release/lightfs gc --config "$RC")
echo "$GC_OUT"
echo "$GC_OUT" | grep -q "1 backup(s) verified" \
  || { echo "  ERROR: gc did not verify exactly the one backup" >&2; exit 1; }
PROT=$(echo "$GC_OUT" | grep -oE '[0-9]+ protected key' | grep -oE '^[0-9]+')
[[ -n "$PROT" && "$PROT" -gt 0 ]] \
  || { echo "  ERROR: gc reports no protected keys — the manifest was not honored" >&2; exit 1; }
start_rsrv
rsh cat /docs/a.txt | sha | diff "$RUN/sum-a.txt" - \
  && echo "  a.txt survives gc byte-exact (manifest-protected)"
rsh cat /docs/binary.bin | sha | diff "$RUN/sum-b.txt" - \
  && echo "  binary.bin survives gc byte-exact (manifest-protected)"
echo "BACKUP/RESTORE DRILL: PASSED"
