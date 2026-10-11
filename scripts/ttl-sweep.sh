#!/usr/bin/env bash
# C1 TTL sweep: attribute-cache and directory-listing-cache TTLs, measured
# against a real FUSE mount + a second (out-of-band RPC) client.
#
# For each cell it reports BOTH sides of the tradeoff:
#   speed  - git status cold/warm + find on a fixed repo (mount, fresh mount per cell)
#   staleness - how long cross-client create/unlink/rename stay invisible
#               to the mount's stat and readdir (visibility probes)
#
# Knobs under test (env, read once per mount at startup):
#   LIGHTFS_FUSE_ATTR_TTL_SECS    - kernel attr/entry cache lifetime
#   LIGHTFS_FUSE_DIRENT_CACHE_SECS - mount-side full-listing cache (0=off)
#
# Matrix: ATTR in {0,1,5,30,60} with DIRENT=0, then DIRENT in {1,5,30,60}
# with ATTR=1. Every mount is verified against /proc/self/mounts.
set -uo pipefail
cd "$(dirname "$0")/.."

DEPLOY_DIR="${DEPLOY_DIR:?DEPLOY_DIR must hold lightfs+minio binaries}"
REPO_SRC="${REPO_SRC:?REPO_SRC must be a local git repo to clone}"
RUN="${RUN:-/tmp/ttl-sweep-$$}"
MNT="$RUN/mnt"
LOC="$RUN/loc"

fail() { echo "FAIL: $1" >&2; exit 1; }
for bin in lightfs minio; do [[ -x "$DEPLOY_DIR/$bin" ]] || fail "$bin missing"; done

t0() { T0="${EPOCHREALTIME:-$(date +%s)}"; }
t1() {
    local end="${EPOCHREALTIME:-$(date +%s)}" ms
    if [[ -n "${EPOCHREALTIME:-}" ]]; then
        ms=$(awk -v a="$end" -v b="$T0" 'BEGIN{printf "%.0f", (a-b)*1000}')
    else
        ms=$(( (end - T0) * 1000 ))
    fi
    echo "${ms}"
}

mkdir -p "$RUN" "$MNT" "$LOC"
setsid nohup "$DEPLOY_DIR/minio" server "$RUN/minio-data" --address :9000 \
    >"$RUN/minio.log" 2>&1 &
MINIO_PID=$!
for _ in $(seq 1 100); do
    curl -s --noproxy '*' -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live && break
    sleep 0.2
done
curl -s --noproxy '*' -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live \
    || fail "minio up"

cat > "$RUN/cfg.toml" <<CFGEOF
mode = "server"
[s3]
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
bucket = "ttl-sweep"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/metadata.db"
[server]
bind = "127.0.0.1:18095"
CFGEOF

"$DEPLOY_DIR/lightfs" create-bucket --config "$RUN/cfg.toml" >/dev/null
"$DEPLOY_DIR/lightfs" server --config "$RUN/cfg.toml" >"$RUN/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 100); do
    curl -s --noproxy '*' -m 1 -o /dev/null http://127.0.0.1:18095/health && break
    sleep 0.2
done
curl -s --noproxy '*' -m 1 -o /dev/null http://127.0.0.1:18095/health || fail "server up"

cleanup() {
    kill -9 "$SERVER_PID" "$MINIO_PID" 2>/dev/null || true
}
trap cleanup EXIT

# Seed the repo ONCE through a throwaway mount (default knobs); later
# cells just remount - the server-side namespace persists.
LIGHTFS_FUSE_DIR_SNAPSHOT_SECS=60 "$DEPLOY_DIR/lightfs" mount --config "$RUN/cfg.toml" "$MNT" \
    >"$RUN/mount-seed.log" 2>&1 &
for _ in $(seq 1 100); do grep -q " $MNT fuse" /proc/self/mounts && break; sleep 0.2; done
grep -q " $MNT fuse" /proc/self/mounts || fail "seed mount up"
git clone -q "$REPO_SRC" "$MNT/repo" || fail "seed clone"
( cd "$MNT/repo" && git status --porcelain >/dev/null )
fusermount3 -u "$MNT" 2>/dev/null || fusermount -u "$MNT" || umount -l "$MNT"
sleep 1

rpc() { # rpc <method> <json params> -> body
    curl -s --noproxy '*' -X POST http://127.0.0.1:18095/rpc \
        -H 'Content-Type: application/json' \
        -d "{\"id\":1,\"method\":\"$1\",\"params\":$2}"
}
b64() { base64 | tr -d '\n'; }

poll_until() { # poll_until <timeout_s> <shell predicate> -> elapsed ms
    local deadline=$(( SECONDS + $1 ))
    t0
    while (( SECONDS < deadline )); do
        eval "$2" && { t1; return; }
        sleep 0.02
    done
    t1
}

# One matrix cell: fresh mount with the cell's knobs, timings, probes.
run_cell() {
    local attr="$1" dirent="$2"
    local label="attr=${attr}s dirent=${dirent}s"
    fusermount3 -u "$MNT" 2>/dev/null || fusermount -u "$MNT" 2>/dev/null || umount -l "$MNT" 2>/dev/null || true
    sleep 0.5
    LIGHTFS_FUSE_ATTR_TTL_SECS="$attr" LIGHTFS_FUSE_DIRENT_CACHE_SECS="$dirent" \
        "$DEPLOY_DIR/lightfs" mount --config "$RUN/cfg.toml" "$MNT" >"$RUN/mount.log" 2>&1 &
    for _ in $(seq 1 100); do grep -q " $MNT fuse" /proc/self/mounts && break; sleep 0.2; done
    grep -q " $MNT fuse" /proc/self/mounts || fail "$label: mount up"

    local cold warm find_ms
    cold=$(t0; ( cd "$MNT/repo" && git status --porcelain >/dev/null ); t1)
    warm=$(t0; ( cd "$MNT/repo" && git status --porcelain >/dev/null ); t1)
    find_ms=$(t0; find "$MNT/repo" -type f >/dev/null; t1)

    # ---- cross-client visibility probes (second client = raw RPC) ----
    # create visible via stat
    rpc write '{"path":"/vis-create","data":"'"$(printf vis | b64)"'"}' >/dev/null
    local v_create
    v_create=$(poll_until 90 "[ -e '$MNT/vis-create' ]")
    rpc unlink '{"path":"/vis-create"}' >/dev/null
    # delete visible via stat
    ( cd "$MNT" && stat vis-del >/dev/null 2>&1 ) || true
    rpc write '{"path":"/vis-del","data":"'"$(printf del | b64)"'"}' >/dev/null
    # negative-lookup staleness: the stat above cached ENOENT, the file
    # now exists server-side - how long until stat sees it?
    local v_negcreate
    v_negcreate=$(poll_until 90 "[ -e '$MNT/vis-del' ]")
    rpc unlink '{"path":"/vis-del"}' >/dev/null
    local v_delete
    v_delete=$(poll_until 90 "[ ! -e '$MNT/vis-del' ]")
    # listing visible via readdir (a fresh dir entry)
    rpc write '{"path":"/repo/vis-listing","data":"'"$(printf l | b64)"'"}' >/dev/null
    local v_list
    v_list=$(poll_until 90 "ls '$MNT/repo' 2>/dev/null | grep -q vis-listing")
    rpc unlink '{"path":"/repo/vis-listing"}' >/dev/null

    printf '%-28s cold=%6s warm=%6s find=%6s | create=%6s neg-create=%6s delete=%6s listing=%6s\n' \
        "$label" "$cold" "$warm" "$find_ms" "$v_create" "$v_negcreate" "$v_delete" "$v_list"
}

echo "== C1 TTL sweep (times in ms; visibility = worst-case observed delay) =="
printf '%-28s %-13s %-13s %-13s | %-11s %-11s %-11s %-11s\n' \
    "cell" "status-cold" "status-warm" "find" "vis-create" "neg-create" "vis-delete" "vis-listing"
for a in 0 1 5 30 60; do run_cell "$a" 0; done
for d in 1 5 30 60; do run_cell 1 "$d"; done
echo "SWEEP DONE (run dir $RUN)"
