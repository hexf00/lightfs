#!/usr/bin/env bash
# FUSE regression + deployment gate (release-blocking).
#
# Thin wrapper: deployment hash verification + MinIO lifecycle, then ONE
# call to the cross-platform acceptance suite (src/bin/lightfs_accept.rs)
# which owns the server/mount lifecycles and EVERY byte-exact assertion
# (sections A/B/C: write-back boundaries, flush failure injection, crash
# persistence, offline fsck). The same suite runs as the Windows WinFsp
# experimental acceptance - one harness, two platforms, no drift.
#
# Runs INSIDE a Linux VM/host with /dev/fuse.
#   MODE vm (default): starts its own MinIO (MINIO_BIN), cleans it up.
#   MODE ci:           MinIO provided externally (release-gates workflow).
set -u
MODE="${1:-vm}"
DEPLOY_DIR="${DEPLOY_DIR:-/tmp/lightfs-accept}"
MINIO_BIN="${MINIO_BIN:-$DEPLOY_DIR/minio}"
RUN="/tmp/lightfs-gate-run-$$-$(date +%s)"
MOUNT_PARENT="/tmp/lightfs-gate-mnt-$$"

fail() { echo "FAIL: $1" >&2; exit 1; }

# == [0] deployment hash verification (MANDATORY - a gate that can skip
# this check can green-light a stale deploy).
for bin in lightfs lightfs-shell lightfs-accept; do
    [[ -x "$DEPLOY_DIR/$bin" ]] || fail "binaries missing at $DEPLOY_DIR ($bin)"
done
[[ -n "${EXPECTED_LIGHTFS_SHA:-}" ]] || fail "EXPECTED_LIGHTFS_SHA is not set - refusing to run without hash verification"
ACTUAL=$(sha256sum "$DEPLOY_DIR/lightfs" 2>/dev/null | awk '{print $1}')
[[ "$ACTUAL" == "$EXPECTED_LIGHTFS_SHA" ]] || fail "deployed lightfs sha256 $ACTUAL != expected $EXPECTED_LIGHTFS_SHA (stale deploy)"
echo "deployed lightfs sha256 verified: $ACTUAL"
echo "commit: ${GATE_COMMIT:-unknown}  platform: $(uname -sm)  gate: fuse-gate (suite)"

# == [1] MinIO
if [[ "$MODE" == "vm" ]]; then
    setsid nohup "$MINIO_BIN" server "$RUN/minio-data" --address :9000 >/tmp/lightfs-gate-minio.log 2>&1 &
    echo $! > /tmp/lightfs-gate.pids.minio
fi
for i in $(seq 1 50); do curl -s --noproxy "*" -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live && break; sleep 0.2; done
curl -s --noproxy "*" -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live || fail "minio up"
echo "minio up"

# == [2] config (fresh namespace per run)
mkdir -p "$RUN"
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

# == [3] THE GATE: cross-platform acceptance suite (server/mount lifecycle
# + sections A/B/C owned by the suite; non-zero exit on any failure).
mkdir -p "$MOUNT_PARENT/gate-mnt"
"$DEPLOY_DIR/lightfs" create-bucket --config "$RUN/cfg.toml" >/dev/null
"$DEPLOY_DIR/lightfs-accept" \
    --lightfs "$DEPLOY_DIR/lightfs" \
    --shell "$DEPLOY_DIR/lightfs-shell" \
    --config "$RUN/cfg.toml" \
    --mount-target "$MOUNT_PARENT/gate-mnt"
RC=$?
[[ $RC -eq 0 ]] || fail "acceptance suite exited $RC (logs: $RUN/*.err)"

# == [4] cleanup (our MinIO only)
if [[ "$MODE" == "vm" && -f /tmp/lightfs-gate.pids.minio ]]; then
    while read -r p; do kill -9 "$p" 2>/dev/null || true; done < /tmp/lightfs-gate.pids.minio
    rm -f /tmp/lightfs-gate.pids.minio
fi
echo "FUSE GATE: PASSED (suite a,b,c)"
