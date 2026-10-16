#!/usr/bin/env bash
# Backup / restore drill (S4.14.3 form, against the CURRENT backup format):
#
#   [backup].root = isolated directory; every published backup is
#   <root>/<id>/{meta.db, meta.db.manifest, backup.json}.
#
#   [1] build a small namespace through server + client RPC
#   [2] record the pre-backup tree and checksums; fsck runs OFFLINE
#   [3] ONLINE `lightfs backup` while the server keeps running; assert the
#       published directory contains the three required files
#   [4] mutate AFTER the snapshot: overwrite a.txt, delete binary.bin, and
#       add+delete post.txt. With retention off, every retired object
#       becomes an immediate deletion INTENT of the MAIN database — the
#       pre-backup contents are no longer referenced by the live namespace
#   [5] restore = copy the published meta.db into an INDEPENDENT database
#       (never treat the backup directory itself as one SQLite file) and
#       serve it with its own server; tree/checksums must equal the
#       pre-backup state
#   [6] fsck the restored namespace OFFLINE (restore server stopped)
#   [7] gc --config $CFG with BOTH servers stopped: the gc now operates on
#       the MUTATED MAIN database, so the two pre-backup objects are real
#       deletion candidates that only the backup manifest shields
#       (skipped-protected 2); the one unprotected object (post.txt) must
#       be reclaimed (deleted 1, failed 0) — proof gc actually reclaims in
#       the same pass. Then the restored server comes back and reads the
#       manifest-protected history byte-exact, while the post-backup file
#       stays absent.
#
# `--selftest-stop` exercises ONLY the process-stop machinery: a process
# that ignores SIGTERM must reach the SIGKILL escalation inside the bounded
# grace, and a well-behaved one must die without escalation.
#
# Every server gets a bounded health-check readiness wait, a bounded
# stop (SIGTERM grace -> SIGKILL), and both are stopped before the temp
# tree is removed.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

PORT=18087
RPORT=18090
BUCKET="lightfs-drill-$$-$(date +%s)"
OUT="/tmp/lightfs-perf/backup-drill.txt"
RUN="/tmp/lightfs-drill-$$-$(date +%s)"
BACKUPS="$RUN/backups"

# alive PID: true while the process exists and is not a reaped-pending
# zombie (a plain `kill -0` would report zombies alive forever and break
# the bounded wait below).
alive() {
  local st
  st=$(ps -o stat= -p "$1" 2>/dev/null || true)
  [[ -n "$st" && "$st" != Z* ]]
}

# stop_pid PID LABEL: bounded stop. SIGTERM, then a bounded grace polling
# for exit; only a process still alive after the grace gets SIGKILL; then
# wait reaps it. Escalation is RECORDED (STOP_ESCALATED=1 plus a stderr
# line); surviving even SIGKILL is an error.
GRACE_TICKS=50 # 50 x 0.1s = 5s bounded SIGTERM grace
STOP_ESCALATED=0
stop_pid() {
  local pid=$1 label=$2
  local n=0
  STOP_ESCALATED=0
  kill "$pid" 2>/dev/null || true
  while alive "$pid" && (( n < GRACE_TICKS )); do
    sleep 0.1
    n=$((n + 1))
  done
  if alive "$pid"; then
    echo "  stop: $label (pid $pid) ignored SIGTERM for 5s; escalating to SIGKILL" >&2
    STOP_ESCALATED=1
    kill -9 "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
  if alive "$pid"; then
    echo "ERROR: $label (pid $pid) survived SIGKILL" >&2
    return 1
  fi
}

MAIN_PID=""
RSRV_PID=""
stop_main() {
  if [[ -n "${MAIN_PID:-}" ]]; then
    stop_pid "$MAIN_PID" "main server"
    MAIN_PID=""
  fi
}
stop_rsrv() {
  if [[ -n "${RSRV_PID:-}" ]]; then
    stop_pid "$RSRV_PID" "restore server"
    RSRV_PID=""
  fi
}

# Script-level proof that the stop machinery really bounds: run it and exit.
if [[ "${1:-}" == "--selftest-stop" ]]; then
  bash -c 'trap "" TERM; exec sleep 300' &
  stubborn=$!
  # Let the child actually install its TERM trap (and exec) before we
  # signal it; otherwise we are testing a race, not the escalation.
  sleep 0.3
  stop_pid "$stubborn" "selftest/stubborn"
  [[ "$STOP_ESCALATED" == 1 ]] \
    || { echo "SELFTEST FAIL: the SIGTERM-ignorer never reached the SIGKILL escalation" >&2; exit 1; }
  sleep 300 &
  polite=$!
  stop_pid "$polite" "selftest/polite"
  [[ "$STOP_ESCALATED" == 0 ]] \
    || { echo "SELFTEST FAIL: the well-behaved process unexpectedly escalated to SIGKILL" >&2; exit 1; }
  echo "STOP-SELFTEST: PASSED (bounded grace, real SIGKILL escalation, both reaped)"
  exit 0
fi

mkdir -p "$RUN" "$BACKUPS" /tmp/lightfs-perf
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
[retention]
# retention OFF: every retire in [4] registers an immediate deletion intent
# in the MAIN database, and gc candidates qualify at once — the sharp
# scenario where only the backup manifest stands between the two pre-backup
# objects and the delete call.
superseded_secs = 0
[server]
bind = "127.0.0.1:$PORT"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
CFGEOF

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

echo "== [4] mutate AFTER the snapshot; the old contents leave the live namespace"
printf 'after-the-snapshot\n' | lsh write /docs/post.txt --stdin >/dev/null
printf 'contract draft v2 OVERWRITTEN\n' | lsh write /docs/a.txt --stdin >/dev/null
lsh rm /docs/binary.bin >/dev/null
# Control object: created AND deleted after the backup, so it is a deletion
# intent that NO manifest key protects — gc must actually reclaim it.
lsh rm /docs/post.txt >/dev/null
lsh cat /docs/a.txt | grep -q OVERWRITTEN
echo "  mutations: post.txt added, a.txt overwritten, binary.bin deleted, post.txt deleted"

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

echo "== [7] gc on the MUTATED MAIN db (both servers stopped); protection must be real"
stop_main
stop_rsrv
# The gc operates on $CFG — the database whose live namespace no longer
# references the pre-backup contents. Its own deletion intents for those
# objects exist; ONLY the backup manifest keeps them alive. Running gc
# against the restored snapshot instead would be a false positive: the
# snapshot still references everything.
GC_OUT=$(./target/release/lightfs gc --config "$CFG")
echo "$GC_OUT"
echo "$GC_OUT" | grep -q "1 backup(s) verified" \
  || { echo "  ERROR: gc did not verify exactly the one backup" >&2; exit 1; }
echo "$GC_OUT" | grep -q "2 protected key(s)" \
  || { echo "  ERROR: expected exactly the two manifest keys protected" >&2; exit 1; }
echo "$GC_OUT" | grep -q "skipped-protected 2" \
  || { echo "  ERROR: the two pre-backup objects were not deletion candidates shielded by the manifest (protection unproven)" >&2; exit 1; }
echo "$GC_OUT" | grep -q "deleted 1, failed 0" \
  || { echo "  ERROR: expected exactly the unprotected post.txt object reclaimed in the same pass" >&2; exit 1; }
echo "  gc: manifest keys were real deletion candidates (skipped-protected 2);"
echo "  gc: the unprotected control object was reclaimed (deleted 1, failed 0)"
start_rsrv
rsh tree / | sort | diff "$RUN/tree-before.txt" - \
  && echo "  restored tree after gc: identical to the pre-backup state"
rsh cat /docs/a.txt | sha | diff "$RUN/sum-a.txt" - \
  && echo "  a.txt survives gc byte-exact (main had retired it; manifest protected it)"
rsh cat /docs/binary.bin | sha | diff "$RUN/sum-b.txt" - \
  && echo "  binary.bin survives gc byte-exact (main had retired it; manifest protected it)"
if rsh cat /docs/post.txt >/dev/null 2>&1; then
  echo "  ERROR: post.txt (created after the backup) appeared in the restore" >&2
  exit 1
fi
echo "  post.txt: still absent from the restore (created after the snapshot)"
echo "BACKUP/RESTORE DRILL: PASSED"
