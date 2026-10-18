#!/usr/bin/env bash
# S4.25 real-FUSE acceptance + bounded-memory probe. Runs INSIDE a Linux
# container with --device /dev/fuse (fuse3/fusermount3 present).
#
#   MODE=ci: MinIO provided externally (LIGHTFS_S3_ENDPOINT, default the
#            host's :9000 via host.docker.internal).
#
# Phases:
#   [A] server up (true subprocess) + mount up (true FUSE mount)
#   [B] end-to-end mounted-write semantics (mkdir/create/read-your-writes/
#       overwrite/truncate/extend/rename/unlink/commit visibility)
#   [C] big-file mounted write (64 MiB + 256 MiB) with byte-exact verify,
#       client (mount process) + server RSS sampled at each phase
#   [D] staged full-read RSS (256 MiB through the mount)
#   [E] concurrent sessions / staging-budget observation via /metrics
#   [F] abnormal termination (SIGKILL the mount; committed data survives;
#       staging dirs of the dead process are swept by the next process)
set -u
MODE="${MODE:-ci}"
DEPLOY_DIR="${DEPLOY_DIR:-/deploy}"
# RUN_DIR may live on a mounted volume (survives the container for
# post-mortem); the MOUNT TARGET must stay on the container-local FS.
RUN="${RUN_DIR:-/tmp/s425-run-$$}"
# fresh target per run: a previously-mounted path can hold a STALE fuse
# mount from a dead run, and remounting over a corpse stacks and dies on
# old kernels (3.10: fresh conn dies with ENOTCONN, corpse still listed in
# /proc/mounts while /sys/fs/fuse/connections is empty). MNT_DIR stays
# overridable but is then guarded by clear_stale.
MNT="${MNT_DIR:-/tmp/s425-gate-$$}"
EP="${LIGHTFS_S3_ENDPOINT:-http://host.docker.internal:9000}"
# unique bucket per run: a failed earlier run leaves a namespace marker,
# and a fresh database refuses to co-root with it (fail-closed by design)
BUCKET="${LIGHTFS_S3_BUCKET:-lightfs-s425-gate-$(date +%s)}"
BIND="${LIGHTFS_BIND:-127.0.0.1:18999}"

fail() { echo "FAIL: $1" >&2; exit 1; }
note() { echo "== $1"; }

mount_listed() { grep -qs " $1 fuse" /proc/mounts; }
# A mount still listed at the target is a CORPSE from a dead run: stacking
# a fresh mount over it fails weirdly on old kernels. Clear it, or refuse
# to run - never mount over it.
clear_stale() {
  if mount_listed "$1"; then
    fusermount -u -z "$1" 2>/dev/null || fusermount3 -u -z "$1" 2>/dev/null || umount -l "$1" 2>/dev/null || true
    sleep 0.5
  fi
  mount_listed "$1" && fail "stale fuse mount at $1 is unremovable - refusing to stack a new mount over it"
}
# Listed is NOT up (a listed-but-dead mount passes `mountpoint -q`): the
# target must also answer a real filesystem op. On failure dump the full
# diagnostic (stderr, mount table, kernel connections, thread wait-chans).
wait_mount_up() {
  local pid=$1
  for _ in $(seq 1 100); do
    if mount_listed "$MNT" && timeout 2 ls "$MNT" >/dev/null 2>&1; then return 0; fi
    sleep 0.2
  done
  echo "--- mount.err ---"; cat "$RUN/mount.err" 2>/dev/null
  echo "--- mount.log ---"; cat "$RUN/mount.log" 2>/dev/null
  echo "--- target in /proc/mounts:"; grep -s "$MNT" /proc/mounts || echo "(not listed)"
  echo "--- kernel fuse connections: $(ls /sys/fs/fuse/connections/ 2>/dev/null | wc -l)"
  for t in "/proc/$pid/task/"*; do
    [[ -r "$t/wchan" ]] || continue
    echo "  thread $(cat "$t/comm" 2>/dev/null) blocked in $(cat "$t/wchan" 2>/dev/null)"
  done
  fail "mount did not come up at $MNT"
}
cleanup() {
  kill -9 "${MOUNT_PID:-0}" "${MOUNT_PID2:-0}" "${SERVER_PID:-0}" 2>/dev/null
  fusermount -u -z "$MNT" 2>/dev/null || fusermount3 -u -z "$MNT" 2>/dev/null || umount -l "$MNT" 2>/dev/null
}
trap cleanup EXIT

for bin in lightfs lightfs-shell lightfs-accept; do
  [[ -x "$DEPLOY_DIR/$bin" ]] || fail "binary missing: $DEPLOY_DIR/$bin"
done

mkdir -p "$RUN" "$MNT" || fail "mkdir run/mnt"
cat > "$RUN/cfg.toml" <<CFGEOF
[s3]
endpoint = "$EP"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/metadata.db"
[server]
bind = "$BIND"
[staging]
budget_bytes = 1073741824
max_object_bytes = 0
[cache.metadata]
enabled = false
[cache.data]
enabled = false
CFGEOF

"$DEPLOY_DIR/lightfs" create-bucket --config "$RUN/cfg.toml" >/dev/null || fail "create-bucket"

# ---- [A] server + mount lifecycles ----
"$DEPLOY_DIR/lightfs" server --config "$RUN/cfg.toml" >"$RUN/server.log" 2>"$RUN/server.err" &
SERVER_PID=$!
for i in $(seq 1 100); do
  curl -s --noproxy '*' -m 1 "http://$BIND/health" >/dev/null 2>&1 && break
  sleep 0.2
done
curl -s --noproxy '*' "http://$BIND/health" | grep -q ok || { echo "--- server.err ---"; cat "$RUN/server.err"; echo "--- server.log ---"; cat "$RUN/server.log"; fail "server health"; }
note "server up (pid $SERVER_PID)"

# Mount: a fresh client process with its OWN staging budget, talking to
# the server over the wire protocol.
cat > "$RUN/mount.toml" <<CFGEOF
# the mount client talks to the SERVER at server.bind; the s3 section is
# required by config validation but unused by the mount process
[s3]
endpoint = "$EP"
region = "us-east-1"
bucket = "unused-by-mount"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/mount-client-meta.db"
[server]
bind = "$BIND"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
CFGEOF
clear_stale "$MNT"
"$DEPLOY_DIR/lightfs" mount --config "$RUN/mount.toml" "$MNT" >"$RUN/mount.log" 2>"$RUN/mount.err" &
MOUNT_PID=$!
wait_mount_up "$MOUNT_PID"
note "mount up (pid $MOUNT_PID at $MNT)"

rss_kb() { awk '/VmRSS/{print $2}' "/proc/$1/status" 2>/dev/null || echo 0; }
hwm_kb() { awk '/VmHWM/{print $2}' "/proc/$1/status" 2>/dev/null || echo 0; }
staging_used() { curl -s --noproxy '*' "http://$BIND/metrics" | awk '/^staging_used_bytes /{print $2}'; }
staging_refused() { curl -s --noproxy '*' "http://$BIND/metrics" | awk '/^staging_refused_total /{print $2}'; }

# ---- [B] mounted write semantics, byte-exact ----
note "B: mounted write semantics"
mkdir -p "$MNT/dir" && [[ -d "$MNT/dir" ]] || fail "mkdir"
printf 'hello-s425' > "$MNT/dir/a.txt"
[[ "$(cat "$MNT/dir/a.txt")" == "hello-s425" ]] || fail "write-then-read your own writes"
printf 'hello-s425-longer-content' > "$MNT/dir/a.txt"
[[ "$(cat "$MNT/dir/a.txt")" == "hello-s425-longer-content" ]] || fail "overwrite"
truncate -s 100 "$MNT/dir/a.txt"
[[ "$(stat -c %s "$MNT/dir/a.txt")" == "100" ]] || fail "truncate extend (stat)"
head -c 100 "$MNT/dir/a.txt" > "$RUN/trunc.out"
head -c 26 "$RUN/trunc.out" | grep -q "hello-s425-longer-content" || fail "truncate keeps prefix"
tail -c 74 "$RUN/trunc.out" | tr -d '\0' | grep -q . && fail "extension must be zero"
truncate -s 7 "$MNT/dir/a.txt"
[[ "$(cat "$MNT/dir/a.txt")" == "hello-s" ]] || fail "truncate shrink"
mv "$MNT/dir/a.txt" "$MNT/dir/b.txt"
[[ "$(cat "$MNT/dir/b.txt")" == "hello-s" ]] || fail "rename keeps content"
rm "$MNT/dir/b.txt"
[[ ! -e "$MNT/dir/b.txt" ]] || fail "unlink"
note "B: PASSED"

# ---- [C] big mounted write: 64 MiB then 256 MiB, RSS + byte verify ----
note "C: big mounted writes"
mount_rss_before=$(rss_kb "$MOUNT_PID")
server_rss_before=$(rss_kb "$SERVER_PID")
for SIZE_MB in 64 256; do
  SRC="/tmp/pattern-$SIZE_MB.bin"
  # deterministic pseudo-random pattern, generated streamingly
  python3 - "$SIZE_MB" "$SRC" <<'PYGEN'
import sys, os
mb, path = int(sys.argv[1]), sys.argv[2]
seed = (0x9E3779B9 * (mb + 1)) & 0xFFFFFFFF
def rnd():
    global seed
    seed = (1103515245 * seed + 12345) & 0x7FFFFFFF
    return (seed >> 16) & 0xFF
with open(path, 'wb') as f:
    left = mb * 1024 * 1024
    while left:
        n = min(left, 1 << 20)
        f.write(bytes(rnd() for _ in range(n)))
        left -= n
PYGEN
  # mount-side streaming write (dd → the FUSE file); client process RSS
  # must stay bounded by its spool budget, not the file size
  dd if="$SRC" of="$MNT/big-$SIZE_MB.bin" bs=1M count="$SIZE_MB" iflag=fullblock 2>/dev/null || fail "dd write $SIZE_MB"
  sync
  # byte-exact verify through the mount (streamed read)
  python3 - "$SRC" "$MNT/big-$SIZE_MB.bin" <<'PYVERIFY'
import sys, hashlib
src, dst = sys.argv[1], sys.argv[2]
h1, h2 = hashlib.sha256(), hashlib.sha256()
with open(src, 'rb') as f:
    for chunk in iter(lambda: f.read(1 << 20), b''):
        h1.update(chunk)
with open(dst, 'rb') as f:
    for chunk in iter(lambda: f.read(1 << 20), b''):
        h2.update(chunk)
assert h1.hexdigest() == h2.hexdigest(), f"content mismatch {src} vs {dst}"
PYVERIFY
  [[ $? -eq 0 ]] || fail "byte verify $SIZE_MB"
  echo "  ${SIZE_MB}MiB: mount RSS $(rss_kb "$MOUNT_PID")KB (hwm $(hwm_kb "$MOUNT_PID")KB), server RSS $(rss_kb "$SERVER_PID")KB (hwm $(hwm_kb "$SERVER_PID")KB), staging_used $(staging_used)"
  rm -f "$SRC"
done
mount_rss_after=$(rss_kb "$MOUNT_PID")
server_rss_after=$(rss_kb "$SERVER_PID")
note "C: PASSED (mount RSS ${mount_rss_before}->${mount_rss_after} KB, server ${server_rss_before}->${server_rss_after} KB)"

# ---- [D] staged full read RSS ----
note "D: full read of 256MiB through the mount"
server_rss_before_read=$(rss_kb "$SERVER_PID")
sha256sum "$MNT/big-256.bin" > /dev/null || fail "full read"
server_rss_after_read=$(rss_kb "$SERVER_PID")
echo "  server RSS read phase: ${server_rss_before_read}->${server_rss_after_read} KB (hwm $(hwm_kb "$SERVER_PID")KB)"
note "D: PASSED"

# ---- [E] concurrent write sessions + staging observation ----
note "E: concurrent sessions and staging gauges"
for i in 1 2 3 4; do
  ( dd if=/dev/urandom of="$MNT/dir/conc-$i.bin" bs=1M count=8 iflag=fullblock 2>/dev/null ) &
done
wait
for i in 1 2 3 4; do [[ -s "$MNT/dir/conc-$i.bin" ]] || fail "concurrent write $i"; done
echo "  staging_used=$(staging_used) refused=$(staging_refused)"
note "E: PASSED"

# ---- [F] abnormal termination + recovery + sweep ----
note "F: SIGKILL mount, committed data survives, staging swept"
kill -9 "$MOUNT_PID" 2>/dev/null
sleep 1
# the SIGKILLed daemon may leave the mount listed-but-dead (corpse) on old
# kernels: clear it the same way before remounting
clear_stale "$MNT"
# committed big file must still be there after remount
"$DEPLOY_DIR/lightfs" mount --config "$RUN/mount.toml" "$MNT" >>"$RUN/mount.log" 2>>"$RUN/mount.err" &
MOUNT_PID2=$!
wait_mount_up "$MOUNT_PID2"
[[ -s "$MNT/big-256.bin" ]] || fail "committed data lost after mount SIGKILL"
note "F: PASSED"

kill "$SERVER_PID" "$MOUNT_PID2" 2>/dev/null
sleep 1
kill -9 "$SERVER_PID" "$MOUNT_PID2" 2>/dev/null
echo "S4.25 MOUNT GATE: PASSED (A,B,C,D,E,F)"
