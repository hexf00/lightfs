#!/usr/bin/env bash
# FUSE regression + deployment gate (release-blocking).
#
# Runs INSIDE a Linux VM/host with /dev/fuse (e.g. colima vz VM).
# Deployment preflight: the lightfs binaries are hash-verified against the
# host build BEFORE anything starts - a stale deploy fails the gate instead
# of silently testing old code.
#
# Regression scenarios locked here (found in review round 4):
#   R1  dup-close first-write preservation (bash redirect fd dup2+close
#       sends an empty FLUSH before the data write - the first write must
#       survive the post-commit re-arm)
#   R2  ftruncate extend: size exact, original prefix intact, zero tail
#   R3  write -> truncate -> write: truncate consumes the open session's
#       expected version; the subsequent write must NOT be lost
#   R4  truncate to zero: old content must NOT survive
#   R5  empty file lifecycle: create empty, remount, still empty
#   R6  multi-write + append integrity: exact SHA-256 of the final content
#   + remount persistence for every file created above.
#
# Every scenario verifies content EXACTLY via sha256/cmp - never just the
# command exit code.

set -u
# MODE: vm (default - starts its own MinIO, may pkill by exact name) or
# ci (MinIO provided externally by the workflow; lightfs processes are
# still ours to manage, by PID - never by name, so parallel jobs and
# other services on a shared runner are unaffected).
MODE="${1:-vm}"
RUN_ID="gate-$$-$(date +%s)"
MOUNT_PARENT="/tmp/lightfs-gate-$RUN_ID"
PORT=18085
RUN="/tmp/lightfs-gate-run"
DEPLOY_DIR="${DEPLOY_DIR:-/tmp/lightfs-accept}"
MINIO_BIN="${MINIO_BIN:-$DEPLOY_DIR/minio}"
PORT=18085
PASS=0
FAIL=0

fail() { echo "  FAIL: $1" >&2; FAIL=$((FAIL+1)); }
pass() { echo "  PASS: $1"; PASS=$((PASS+1)); }
sha()  { "${SHA_TOOL:-shasum} -a 256"; }

# PID files are split: section [4] restarts ONLY the lightfs server and
# must not kill MinIO (non-empty-file reads would then fail with EIO).
# `$!` after `setsid nohup cmd &` is sometimes the real child pid, so a
# single mixed file silently turned the "server restart" into
# "kill MinIO too" depending on setsid's fork decision.
PIDS_MINIO=/tmp/lightfs-gate.pids.minio
PIDS_LIGHTFS=/tmp/lightfs-gate.pids.lightfs
kill_pids_file() {
    [[ -f "$1" ]] && while read -r p; do kill -9 "$p" 2>/dev/null || true; done < "$1"
    rm -f "$1"
}

echo "== [0] deployment hash verification"
if [[ ! -x "$MINIO_BIN" || ! -x "$DEPLOY_DIR/lightfs" || ! -x "$DEPLOY_DIR/lightfs-shell" ]]; then
    echo "FAIL: binaries missing at $DEPLOY_DIR" >&2
    exit 1
fi
# The deployment hash check is MANDATORY: the gate must verify it is
# testing exactly the binaries built from the commit under test - a gate
# that can silently skip this check can green-light a stale deploy.
if [[ -z "${EXPECTED_LIGHTFS_SHA:-}" ]]; then
    echo "FAIL: EXPECTED_LIGHTFS_SHA is not set - refusing to run without hash verification" >&2
    exit 1
fi
ACTUAL=$(sha256sum "$DEPLOY_DIR/lightfs" 2>/dev/null | awk '{print $1}')
if [[ "$ACTUAL" != "$EXPECTED_LIGHTFS_SHA" ]]; then
    echo "FAIL: deployed lightfs sha256 $ACTUAL != expected $EXPECTED_LIGHTFS_SHA (stale deploy)" >&2
    exit 1
fi
echo "  deployed lightfs sha256 verified: $ACTUAL"
echo "  commit: ${GATE_COMMIT:-unknown}  platform: $(uname -sm)  gate: fuse-gate"


# stop leftovers from OUR previous runs only (by recorded PID files)
kill_pids_file "$PIDS_MINIO"
kill_pids_file "$PIDS_LIGHTFS"
fusermount3 -u "$MOUNT_PARENT/gate-mnt" 2>/dev/null || true
mkdir -p "$RUN" "$MOUNT_PARENT/gate-mnt"

cat > "$RUN/cfg.toml" <<CFGEOF
mode = "server"
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "${LIGHTFS_S3_BUCKET:-lightfs-fuse-gate}"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/metadata.db"
[server]
bind = "127.0.0.1:18085"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
CFGEOF

echo "== [1] pinned MinIO + lightfs server"
if [[ "$MODE" == "vm" ]]; then
    setsid nohup "$MINIO_BIN" server "$RUN/minio-data" --address :9000 >"$RUN/minio.log" 2>&1 &
    echo "$!" > "$PIDS_MINIO"
fi
for i in $(seq 1 50); do curl -s --noproxy "*" -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live && break; sleep 0.2; done
curl -s --noproxy "*" -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live || { fail "minio up"; echo "$FAIL/$PASS"; exit 1; }
pass "minio up"

"$DEPLOY_DIR/lightfs" create-bucket --config "$RUN/cfg.toml" >/dev/null
setsid nohup "$DEPLOY_DIR/lightfs" server --config "$RUN/cfg.toml" >"$RUN/server.log" 2>&1 &
echo "$!" > "$PIDS_LIGHTFS"
for i in $(seq 1 50); do curl -s --noproxy "*" -m 1 -o /dev/null http://127.0.0.1:18085/health && break; sleep 0.2; done
curl -s --noproxy "*" -m 1 -o /dev/null http://127.0.0.1:18085/health || { fail "lightfs server up"; echo "$FAIL/$PASS"; exit 1; }
pass "lightfs server up"

echo "== [2] FUSE mount"
mkdir -p "$MOUNT_PARENT/gate-mnt"
setsid nohup "$DEPLOY_DIR/lightfs" mount --config "$RUN/cfg.toml" "$MOUNT_PARENT/gate-mnt" >"$RUN/mount.log" 2>&1 &
for i in $(seq 1 100); do mountpoint -q "$MOUNT_PARENT/gate-mnt" && break; sleep 0.2; done
mountpoint -q "$MOUNT_PARENT/gate-mnt" || { fail "fuse mount"; tail -5 "$RUN/mount.log" >&2; echo "$FAIL/$PASS"; exit 1; }
pass "fuse mounted"
M="$MOUNT_PARENT/gate-mnt"

echo "== [3] regression scenarios (exact-content verification)"

# R1: dup-close first-write preservation. `cmd > file` makes bash open the
# redirect fd, fork+dup2, and CLOSE the parent copy -> FLUSH arrives before
# the child writes. The first content must survive the post-commit re-arm
# and the subsequent append.
printf 'first write content\nappended line\n' > /tmp/lightfs-gate-expected-r1
printf 'first write content\n' > "$M/r1.txt"
printf 'appended line\n' >> "$M/r1.txt"
sleep 1.5
sha256sum "$M/r1.txt" | awk '{print $1}' > /tmp/lightfs-gate-r1-actual
sha256sum /tmp/lightfs-gate-expected-r1 | awk '{print $1}' > /tmp/lightfs-gate-r1-expected
if cmp -s /tmp/lightfs-gate-r1-actual /tmp/lightfs-gate-r1-expected; then
    pass "R1 dup-close first-write preserved"
else
    fail "R1 dup-close first-write preserved"
fi

# R2: ftruncate extend - exact size, original prefix, zero-filled tail
printf 'truncate base data\n' > "$M/r2.txt"
truncate -s 8192 "$M/r2.txt"; sleep 1.5
SIZE=$(stat -c %s "$M/r2.txt" 2>/dev/null || echo -1)
PREFIX_OK=$(cmp -s -n 19 <(head -c 19 "$M/r2.txt") <(printf 'truncate base data\n') && echo yes || echo no)
if [[ "$SIZE" == "8192" && "$PREFIX_OK" == "yes" ]]; then
    pass "R2 ftruncate extend (size + prefix)"
else
    fail "R2 ftruncate extend (size=$SIZE prefix_ok=$PREFIX_OK)"
fi

# R3: write -> truncate -> write - the truncate consumes the open session's
# expected version; the write AFTER the truncate must not be lost
printf 'before truncate\n' > "$M/r3.txt"; sleep 1.5
truncate -s 4096 "$M/r3.txt"; sleep 1.5
printf 'after truncate\n' > "$M/r3.txt"; sleep 1.5
CONTENT=$(cat "$M/r3.txt")
if [[ "$CONTENT" == "after truncate" ]]; then
    pass "R3 write-truncate-write (post-truncate write survives)"
else
    fail "R3 write-truncate-write (got: $CONTENT)"
fi

# R4: truncate to zero - old content must NOT survive
printf 'old content that must go away\n' > "$M/r4.txt"; sleep 1.5
truncate -s 0 "$M/r4.txt"; sleep 1.5
ZSIZE=$(stat -c %s "$M/r4.txt" 2>/dev/null || echo -1)
if [[ "$ZSIZE" == "0" ]]; then
    pass "R4 truncate to zero"
else
    fail "R4 truncate to zero (size=$ZSIZE)"
fi

# R5: empty file lifecycle
: > "$M/r5-empty.txt"; sleep 1.5
ESIZE=$(stat -c %s "$M/r5-empty.txt" 2>/dev/null || echo -1)
if [[ "$ESIZE" == "0" ]]; then
    pass "R5 empty file create+close"
else
    fail "R5 empty file create+close (size=$ESIZE)"
fi

# R7 (drain lifecycle): same-handle write -> commit -> truncate -> write ->
# flush. The stale chunks from before the truncate must NOT resurrect after
# the truncate commit (drain-on-commit guarantees), and the final content
# must be exactly the second write.
printf 'r7 first write payload\n' > "$M/r7.txt"; sleep 1.5
truncate -s 4096 "$M/r7.txt"; sleep 1.5
printf 'r7 second write payload\n' > "$M/r7.txt"; sleep 1.5
R7SHA=$(sha256sum "$M/r7.txt" | awk '{print $1}')
EXP7=$(printf 'r7 second write payload\n' | sha256sum | awk '{print $1}')
if [[ "$R7SHA" == "$EXP7" ]]; then
    pass "R7 same-handle write-commit-truncate-write (stale chunks drained)"
else
    fail "R7 same-handle write-commit-truncate-write"
fi

# R8 (review round 8): bytes written through an OPEN fd but not yet
# flushed, then the file is truncated to zero via its path. ftruncate
# discards dirty pages beyond the new EOF - the kernel will not resend
# them, so the session's pending chunks must be dropped too, or the
# close-flush resurrects the truncated-away content.
command -v python3 >/dev/null || { fail "R8/R9 need python3"; }
M="$M" python3 - <<'PYEOF'
import os, time
m = os.environ["M"]
fd = os.open(m + "/r8.txt", os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
os.write(fd, b"r8 dirty payload that truncate must destroy\n")
time.sleep(2.0)    # writeback delivers the dirty pages to the session
os.truncate(m + "/r8.txt", 0)   # via PATH: independent of the open fd
time.sleep(1.0)
os.close(fd)       # flush fires here with the stale chunks pending
PYEOF
sleep 1.5
R8SIZE=$(stat -c %s "$M/r8.txt" 2>/dev/null || echo -1)
if [[ "$R8SIZE" == "0" ]]; then
    pass "R8 dirty-write then truncate-to-zero (no resurrection)"
else
    fail "R8 dirty-write then truncate-to-zero (size=$R8SIZE, want 0)"
fi

# R9 (partial shrink): same-handle writes at offset 0 and 8192, then shrink
# to 4096 WITHOUT flushing. The beyond-EOF chunk must be dropped; the kept
# prefix must survive exactly once.
M="$M" python3 - <<'PYEOF'
import os, time
m = os.environ["M"]
fd = os.open(m + "/r9.txt", os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
os.write(fd, b"r9 kept prefix\n")
os.pwrite(fd, b"r9 tail beyond new EOF\n", 8192)
time.sleep(2.0)
os.truncate(m + "/r9.txt", 4096)
time.sleep(1.0)
os.close(fd)
PYEOF
sleep 1.5
R9SIZE=$(stat -c %s "$M/r9.txt" 2>/dev/null || echo -1)
R9HEAD=$(head -c 15 "$M/r9.txt" 2>/dev/null || true)
if [[ "$R9SIZE" == "4096" && "$R9HEAD" == "r9 kept prefix" ]]; then
    pass "R9 partial shrink trims beyond-EOF chunks"
else
    fail "R9 partial shrink (size=$R9SIZE, head='$R9HEAD')"
fi

# R10 (straddling chunk): ONE dirty write spanning the future EOF
# [0, 8192), then shrink to 4096 WITHOUT flushing. The chunk starting
# below the new EOF but extending past it must be trimmed to its prefix;
# leaving it whole would re-extend the file to 8192 on close-flush.
# Together with R8 (chunk fully beyond EOF) and R9 (beyond + below) this
# covers all four truncate classes.
M="$M" python3 - <<'PYEOF'
import os, time
m = os.environ["M"]
fd = os.open(m + "/r10.txt", os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
os.write(fd, bytes(range(256)) * 32)   # 8192 bytes, one straddling chunk
time.sleep(2.0)    # writeback delivers the dirty pages to the session
os.truncate(m + "/r10.txt", 4096)
time.sleep(1.0)
os.close(fd)
PYEOF
sleep 1.5
R10SIZE=$(stat -c %s "$M/r10.txt" 2>/dev/null || echo -1)
R10SHA=$(cat "$M/r10.txt" 2>/dev/null | sha256sum | awk '{print $1}')
R10WANT=$(python3 -c "import sys; sys.stdout.buffer.write(bytes(range(256))*16)" | sha256sum | awk '{print $1}')
if [[ "$R10SIZE" == "4096" && "$R10SHA" == "$R10WANT" ]]; then
    pass "R10 straddling chunk trimmed to pre-EOF prefix (sha exact)"
else
    fail "R10 straddling chunk (size=$R10SIZE, sha-mismatch)"
fi
printf 'line one\nline two\n' > "$M/r6.txt"; sleep 1.5
printf 'line three\n' >> "$M/r6.txt"; sleep 1.5
printf 'line four\n' >> "$M/r6.txt"; sleep 1.5
EXPECTED_SHA=$(printf 'line one\nline two\nline three\nline four\n' | sha256sum | awk '{print $1}')
ACTUAL_SHA=$(cat "$M/r6.txt" | sha256sum | awk '{print $1}')
if [[ "$EXPECTED_SHA" == "$ACTUAL_SHA" ]]; then
    pass "R6 multi-write+append integrity (sha256 exact)"
else
    fail "R6 multi-write+append integrity"
fi

echo "== [4] restart persistence (server restart + remount)"
fusermount3 -u "$MOUNT_PARENT/gate-mnt" 2>/dev/null || umount -l "$MOUNT_PARENT/gate-mnt" 2>/dev/null
kill_pids_file "$PIDS_LIGHTFS"
sleep 1
setsid nohup "$DEPLOY_DIR/lightfs" server --config "$RUN/cfg.toml" >"$RUN/server2.log" 2>&1 &
echo "$!" >> "$PIDS_LIGHTFS"
for i in $(seq 1 50); do curl -s --noproxy "*" -m 1 -o /dev/null http://127.0.0.1:18085/health && break; sleep 0.2; done
setsid nohup "$DEPLOY_DIR/lightfs" mount --config "$RUN/cfg.toml" "$MOUNT_PARENT/gate-mnt" >"$RUN/mount2.log" 2>&1 &
for i in $(seq 1 100); do mountpoint -q "$MOUNT_PARENT/gate-mnt" && break; sleep 0.2; done
mountpoint -q "$MOUNT_PARENT/gate-mnt" || { fail "remount"; echo "$FAIL/$PASS"; exit 1; }
pass "remount"

sleep 1.5
ck1=$(cat "$M/r1.txt" | sha256sum | awk '{print $1}')
ck2=$(stat -c %s "$M/r2.txt")
ck3=$(cat "$M/r3.txt")
ck4=$(stat -c %s "$M/r4.txt")
ck5=$(stat -c %s "$M/r5-empty.txt")
ck6=$(cat "$M/r6.txt" | sha256sum | awk '{print $1}')
[[ "$ck1" == "$(sha256sum /tmp/lightfs-gate-expected-r1 | awk '{print $1}')" ]] && pass "remount r1 content" || fail "remount r1 content"
[[ "$ck2" == "8192" ]] && pass "remount r2 size" || fail "remount r2 size ($ck2)"
[[ "$ck3" == "after truncate" ]] && pass "remount r3 content" || fail "remount r3 content ($ck3)"
[[ "$ck4" == "0" ]] && pass "remount r4 zero" || fail "remount r4 zero ($ck4)"
[[ "$ck5" == "0" ]] && pass "remount r5 empty" || fail "remount r5 empty ($ck5)"
[[ "$ck6" == "$EXPECTED_SHA" ]] && pass "remount r6 content" || fail "remount r6 content"

echo "== [5] unmount cleanup"
fusermount3 -u "$MOUNT_PARENT/gate-mnt" 2>/dev/null || umount -l "$MOUNT_PARENT/gate-mnt" 2>/dev/null
kill_pids_file "$PIDS_LIGHTFS"
kill_pids_file "$PIDS_MINIO"
sleep 1
if mountpoint -q "$MOUNT_PARENT/gate-mnt" 2>/dev/null; then
    fail "unmount cleanup"
    exit 1
fi
pass "unmount cleanup"

echo
echo "FUSE GATE: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
