#!/usr/bin/env bash
# Backup / restore drill (release gate):
#   1. build a small namespace with known content through the product path
#   2. fsck (clean) + record file tree and SHA-256 of every file
#   3. lightfs backup (VACUUM INTO: consistent point-in-time snapshot)
#   4. mutate AFTER the backup (new file, overwrite, delete) to prove the
#      snapshot is point-in-time
#   5. restore: isolated copy of the metadata backup against the same bucket
#      - restored file tree/checksums equal the PRE-backup state
#      - restored fsck: ZERO missing objects (superseded-object retention
#        keeps every key the snapshot references; leftover post-backup
#        objects are reported as inert stale/orphan classes, not missing)
#   6. content-restorable proof: a file OVERWRITTEN and a file DELETED after
#      the backup both come back byte-exact after restore — the P0-3
#      acceptance. gc inside the window purges nothing.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

PORT=18087
BUCKET="lightfs-drill-$$-$(date +%s)"
OUT="/tmp/lightfs-perf/backup-drill.txt"
RUN="/tmp/lightfs-drill-$$-$(date +%s)"
mkdir -p "$RUN" /tmp/lightfs-perf

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build --release -q --bin lightfs --bin lightfs-shell

CFG="$RUN/drill.toml"
cat > "$CFG" <<CFGEOF
mode = "server"
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/metadata.db"
[server]
bind = "127.0.0.1:$PORT"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
CFGEOF

SERVER_PID=""
lsh() { ./target/release/lightfs-shell --config "$CFG" --server "http://127.0.0.1:$PORT" "$@"; }
rsh() { ./target/release/lightfs-shell --config "$RUN/restored.toml" "$@"; }
rfsck() { ./target/release/lightfs fsck --config "$RUN/restored.toml"; }
sha() { "${SHA_TOOL:-shasum}" -a 256 "$@"; }

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$RUN"
}
trap cleanup EXIT

./target/release/lightfs create-bucket --config "$CFG" >/dev/null
start_server() {
  ./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-drill-server.log 2>&1 &
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
start_server

{
  echo "# backup/restore drill - $(date) (bucket $BUCKET)"

  echo "== [1] build namespace"
  lsh mkdir /docs >/dev/null
  printf 'contract draft v1\n' | lsh write /docs/a.txt --stdin >/dev/null
  head -c 1048576 /dev/urandom > "$RUN/random.bin"
  lsh write /docs/binary.bin --stdin < "$RUN/random.bin" >/dev/null
  echo "  files: /docs/a.txt (18B), /docs/binary.bin (1MiB random)"

  echo "== [2] fsck + record pre-backup state"
  ./target/release/lightfs fsck --config "$CFG" | grep -q clean
  echo "  fsck: clean"
  lsh tree / | sort > "$RUN/tree-before.txt"
  lsh cat /docs/a.txt | sha > "$RUN/sum-a.txt"
  lsh cat /docs/binary.bin | sha > "$RUN/sum-b.txt"

  echo "== [3] backup"
  ./target/release/lightfs backup --config "$CFG" --output "$RUN/backup.db" | tee /dev/stderr | grep -q "content restorable" ||
    { echo "  ERROR: backup did not report content-restorable retention" >&2; exit 1; }
  echo "  backup reports content-level restorability (retention on)"
  ls -la "$RUN/backup.db" | awk '{print "  backup size:", $5, "bytes"}'

  echo "== [4] post-backup mutations (must NOT appear in the restore)"
  # a.txt and binary.bin are left untouched in [5]; the overwrite/delete
  # boundary is demonstrated separately in [6].
  printf 'after-the-snapshot\n' | lsh write /docs/post.txt --stdin >/dev/null
  echo "  mutations applied: post.txt added"

  echo "== [5] restore into isolation and compare"
  cp "$RUN/backup.db" "$RUN/restored.db"
  RC="$RUN/restored.toml"
  cat > "$RC" <<RESTOREOF
mode = "embedded"
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/restored.db"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
RESTOREOF
  ./target/release/lightfs-shell --config "$RC" tree / | sort > "$RUN/tree-restored.txt"
  diff "$RUN/tree-before.txt" "$RUN/tree-restored.txt" && echo "  file tree: identical to pre-backup state"
  rsh cat /docs/a.txt | sha | diff "$RUN/sum-a.txt" - && echo "  a.txt checksum: identical (pre-backup content)"
  rsh cat /docs/binary.bin | sha | diff "$RUN/sum-b.txt" - && echo "  binary.bin checksum: identical"
  # post-backup leftovers are inert classes; MISSING must not appear
  if (rfsck || true) | tee /tmp/lightfs-perf/drill-fsck-restore.txt | grep -q "MISSING OBJECT"; then
    echo "  ERROR: restored fsck reports MISSING OBJECT - retention failed" >&2
    exit 1
  fi
  echo "  restored fsck: zero missing objects (post-backup leftovers are stale/orphan classes)"
  if rsh cat /docs/post.txt >/dev/null 2>&1; then
    echo "  ERROR: post-backup file leaked into restore" >&2; exit 1
  fi
  echo "  post-backup file absent from restore: correct"

  echo "== [6] content-restorable proof: overwrite + delete AFTER backup"
  printf 'overwritten after backup\n' | lsh write /docs/a.txt --stdin >/dev/null
  lsh rm /docs/binary.bin >/dev/null
  cp "$RUN/backup.db" "$RUN/restored.db"
  rsh cat /docs/a.txt | sha | diff "$RUN/sum-a.txt" -     && echo "  a.txt: pre-backup content restored byte-exact after overwrite"
  rsh cat /docs/binary.bin | sha | diff "$RUN/sum-b.txt" -     && echo "  binary.bin: deleted-after-backup content restored byte-exact"
  if (rfsck || true) | grep -q "MISSING OBJECT"; then
    echo "  ERROR: MISSING OBJECT after restore - retention failed" >&2
    exit 1
  fi
  echo "  restored fsck: zero missing objects"

  echo "== [7] gc inside the retention window purges nothing"
  ./target/release/lightfs gc --config "$CFG" | tee /dev/stderr | grep -q "purged 0 object" ||
    { echo "  ERROR: gc purged objects inside the retention window" >&2; exit 1; }
  echo "  gc: nothing purged inside the window; history intact"
} | tee "$OUT"

echo "results: $OUT"
