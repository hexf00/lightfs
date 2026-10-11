#!/usr/bin/env bash
# Application-level workload benchmark: a real git repository and a
# node_modules-shaped small-file storm, on a LightFS mount vs the local
# filesystem (baseline). Answers "what does daily app traffic cost" -
# synthetic byte-throughput numbers do not.
#
# Runs INSIDE a Linux environment with /dev/fuse (container/VM) with its
# own MinIO at 127.0.0.1:9000. Needs bash + coreutils + git + python3;
# npm is optional (the real-node_modules step is skipped when absent).
#
#   DEPLOY_DIR=/tmp/lightfs-accept bash scripts/git-workload-perf.sh
set -euo pipefail

DEPLOY_DIR="${DEPLOY_DIR:-/tmp/lightfs-accept}"
RUN="/tmp/lightfs-gitperf-$$"
MNT="$RUN/mnt"
LOC="$RUN/local"
FILES="${FILES:-1000}"     # synthetic git repo file count
NFILES="${NFILES:-3000}"   # small-file storm size
NPM_TIMEOUT="${NPM_TIMEOUT:-600}"
# Real-world inputs: GIT_REPO_SRC is any git URL/path cloned straight onto
# the mount (GIT_PROXY only wraps http(s) clones; local traffic never sees
# it). NPM_PACKAGES is the npm dependency tree to install on the mount.
GIT_REPO_SRC="${GIT_REPO_SRC:-}"
GIT_REPO_MIRROR="${GIT_REPO_MIRROR:-}"
GIT_PROXY="${GIT_PROXY:-}"
GIT_CLONE_ARGS="${GIT_CLONE_ARGS:---depth 1}"
NPM_PACKAGES="${NPM_PACKAGES:-express}"

fail() { echo "FAIL: $1" >&2; exit 1; }
for bin in lightfs lightfs-shell minio; do
    [[ -x "$DEPLOY_DIR/$bin" ]] || fail "$bin missing at $DEPLOY_DIR"
done

t0() { T0=$(date +%s%N); }
t1() { echo "  $1: $(( ($(date +%s%N) - T0) / 1000000 )) ms"; }
wait_http() {
    for _ in $(seq 1 100); do
        curl -s --noproxy '*' -m 1 -o /dev/null "$1" && return 0
        sleep 0.2
    done
    return 1
}

mkdir -p "$RUN" "$MNT" "$LOC"

setsid nohup "$DEPLOY_DIR/minio" server "$RUN/minio-data" --address :9000 \
    >"$RUN/minio.log" 2>&1 &
echo $! > "$RUN/minio.pid"
wait_http http://127.0.0.1:9000/minio/health/live || fail "minio up"

cat > "$RUN/cfg.toml" <<CFGEOF
mode = "server"
[s3]
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
bucket = "git-perf"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$RUN/metadata.db"
[server]
bind = "127.0.0.1:18095"
CFGEOF

"$DEPLOY_DIR/lightfs" create-bucket --config "$RUN/cfg.toml" >/dev/null
LIGHTFS_METRICS=1 "$DEPLOY_DIR/lightfs" server --config "$RUN/cfg.toml" >"$RUN/server.log" 2>&1 &
echo $! > "$RUN/server.pid"
wait_http http://127.0.0.1:18095/health || fail "lightfs server up"

LIGHTFS_METRICS=1 LIGHTFS_METRICS_FILE="$RUN/mount-metrics.txt" \
    "$DEPLOY_DIR/lightfs" mount --config "$RUN/cfg.toml" "$MNT" >"$RUN/mount.log" 2>&1 &
echo $! > "$RUN/mount.pid"
for _ in $(seq 1 100); do mountpoint -q "$MNT" && break; sleep 0.2; done
mountpoint -q "$MNT" || fail "lightfs mount up"

cleanup() {
    # graceful unmount FIRST: the mount process flushes its metrics file
    # in the FUSE destroy callback; kill -9 would lose it
    fusermount3 -u "$MNT" 2>/dev/null || fusermount -u "$MNT" 2>/dev/null \
        || umount -l "$MNT" 2>/dev/null || true
    sleep 0.5
    kill -9 "$(cat "$RUN/mount.pid")" 2>/dev/null || true
    kill -9 "$(cat "$RUN/server.pid")" 2>/dev/null || true
    kill -9 "$(cat "$RUN/minio.pid")" 2>/dev/null || true
}

dump_metrics() {
    # the mount writes its table only when its session ends - unmount
    # gracefully right here, then read both sides
    fusermount3 -u "$MNT" 2>/dev/null || fusermount -u "$MNT" 2>/dev/null \
        || umount -l "$MNT" 2>/dev/null || true
    sleep 1
    echo "== mount-side metrics (top 15 by total) =="
    grep -v '^op ' "$RUN/mount-metrics.txt" 2>/dev/null | head -16 || echo "  (no metrics file)"
    echo "== server-side metrics (top 11 by total) =="
    curl -s --noproxy '*' http://127.0.0.1:18095/metrics 2>/dev/null | grep -v '^op ' | head -12 || true
}
trap cleanup EXIT

export GIT_AUTHOR_NAME=perf GIT_AUTHOR_EMAIL=perf@local
export GIT_COMMITTER_NAME=perf GIT_COMMITTER_EMAIL=perf@local

run_synthetic_git() {
echo "== seed: git repo with $FILES files (on local disk) =="
SEED="/tmp/gitperf-seed-$$"
t0
git init -q "$SEED"
for d in $(seq 1 10); do
    mkdir -p "$SEED/src/dir$d"
    for f in $(seq 1 $((FILES / 10))); do
        dd if=/dev/urandom of="$SEED/src/dir$d/f$f.txt" bs=1536 count=1 \
            status=none
    done
done
git -C "$SEED" add -A
git -C "$SEED" commit -qm seed
t1 "seed repo ready (local disk)"

echo "== git workload (synthetic $FILES-file repo) on the LightFS mount =="
t0; git clone -q "file://$SEED" "$MNT/repo"; t1 "git clone -> mount"
t0; (cd "$MNT/repo" && git status --porcelain >/dev/null); t1 "git status (clean tree)"
echo delta >> "$MNT/repo/src/dir1/f1.txt"
t0; (cd "$MNT/repo" && git status --porcelain >/dev/null); t1 "git status (1 dirty file)"
t0; cat "$MNT/repo/src/dir2/"*.txt > /dev/null; t1 "read 100 small files"
(cd "$MNT/repo" && for f in src/dir2/*.txt; do echo delta >> "$f"; done)
t0; (cd "$MNT/repo" && git add -A); t1 "git add -A (100 modified)"
t0; (cd "$MNT/repo" && git commit -qm delta); t1 "git commit (100 blobs)"
t0; rm -rf "$MNT/repo"; t1 "rm -rf repo (unlink storm, $((FILES+40)) files)"
}

if [[ -z "$GIT_REPO_SRC" ]]; then
    run_synthetic_git
fi

echo "== real git repo on the LightFS mount =="
if [[ -n "$GIT_REPO_SRC" ]]; then
    DEST="$MNT/repo-real"
    t0
    # network clones flake (proxies drop packs mid-transfer); that is not
    # what this benchmark measures, so retry the fetch a few times
    for attempt in 1 2 3; do
        rm -rf "$DEST"
        if [[ -n "$GIT_PROXY" ]]; then
            git -c http.proxy="$GIT_PROXY" clone -q $GIT_CLONE_ARGS "$GIT_REPO_SRC" "$DEST" 2>"$RUN/clone.err" && break
        else
            git clone -q $GIT_CLONE_ARGS "$GIT_REPO_SRC" "$DEST" 2>"$RUN/clone.err" && break
        fi
        [[ $attempt -eq 3 ]] && break
        sleep 2
    done
    if [[ ! -d "$DEST/.git" && -n "$GIT_REPO_MIRROR" ]]; then
        echo "  direct clone failed, falling back to mirror: $GIT_REPO_MIRROR"
        rm -rf "$DEST"
        git clone -q $GIT_CLONE_ARGS "$GIT_REPO_MIRROR" "$DEST" \
            2>>"$RUN/clone.err"
    fi
    [[ -d "$DEST/.git" ]] || fail "real-repo clone failed (see $RUN/clone.err)"
    t1 "git clone real repo -> mount"
    echo "  worktree files: $(git -C "$DEST" ls-files | wc -l), pack: $(git -C "$DEST" count-objects -v | grep '^size-pack' | awk '{print $2}') KB"
    t0; (cd "$DEST" && git status --porcelain >/dev/null); t1 "git status (first run on fresh clone)"
    t0; (cd "$DEST" && git status --porcelain >/dev/null); t1 "git status (warm)"
    t0; (cd "$DEST" && git log --oneline | head -5 >/dev/null); t1 "git log"
    # head -50 closes the pipe early; under pipefail that SIGPIPEs
    # git ls-files and set -e would kill the script (known shell trap)
    (cd "$DEST" && git ls-files '*.js' | head -50 > "$RUN/js50.txt" || true)
    while read -r f; do echo delta >> "$DEST/$f"; done < "$RUN/js50.txt"
    t0; (cd "$DEST" && git add -A); t1 "git add -A (50 modified files)"
    t0; (cd "$DEST" && git commit -qm delta); t1 "git commit"
else
    echo "  skipped (GIT_REPO_SRC not set)"
fi

echo "== small-file storm, node_modules shape ($NFILES x 2KB, 200 pkgs) =="
gen_storm() {
    python3 - "$1" "$NFILES" <<'PY'
import os, sys
root, n = sys.argv[1], int(sys.argv[2])
data = b"x" * 2048
for i in range(n):
    d = os.path.join(root, "node_modules", f"pkg-{i % 200}", "lib", f"sub{i % 7}")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"file-{i}.js"), "wb") as f:
        f.write(data)
PY
}
t0; gen_storm "$MNT"; t1 "create+write+close $NFILES files (mount)"
t0; find "$MNT/node_modules" -type f | wc -l >/dev/null; t1 "find -type f (recursive enumeration, mount)"
t0; rm -rf "$MNT/node_modules"; t1 "rm -rf storm (mount)"
t0; gen_storm "$LOC"; t1 "create+write+close $NFILES files (local disk)"
t0; find "$LOC/node_modules" -type f | wc -l >/dev/null; t1 "find (local disk)"
t0; rm -rf "$LOC/node_modules"; t1 "rm -rf storm (local disk)"

echo "== optional: real npm install on the mount =="
if [[ -n "$GIT_REPO_SRC" && -d "$DEST/.git" ]]; then
    echo "== control: same real repo on the LOCAL DISK =="
    t0; rm -rf "$LOC/repo-local"; git clone -q "$GIT_REPO_SRC" "$LOC/repo-local" 2>/dev/null || git clone -q "$DEST" "$LOC/repo-local"; t1 "git clone -> local disk"
    t0; (cd "$LOC/repo-local" && git status --porcelain >/dev/null); t1 "git status (local disk)"
    t0; (cd "$LOC/repo-local" && git status --porcelain >/dev/null); t1 "git status warm (local disk)"
    rm -rf "$LOC/repo-local"
fi

echo "== attribution: op metrics =="
dump_metrics

if command -v npm >/dev/null 2>&1; then
    mkdir -p "$MNT/npmapp"
    printf '{"name":"app","version":"1.0.0"}\n' > "$MNT/npmapp/package.json"
    t0
    if (cd "$MNT/npmapp" && timeout "$NPM_TIMEOUT" npm install $NPM_PACKAGES \
        --registry=https://registry.npmmirror.com --no-audit --no-fund \
        --loglevel=error >/dev/null 2>&1); then
        t1 "npm install $NPM_PACKAGES (mount)"
        echo "  packages installed: $(find "$MNT/npmapp/node_modules" -maxdepth 1 -mindepth 1 | wc -l), files: $(find "$MNT/npmapp/node_modules" -type f | wc -l)"
    else
        echo "  npm install $NPM_PACKAGES did not finish in ${NPM_TIMEOUT}s (registry/time) - skipped"
    fi
else
    echo "  npm not installed - skipped"
fi
# same install on the LOCAL disk: separates registry/network time from
# filesystem time (same network, same package tree)
if command -v npm >/dev/null 2>&1; then
    mkdir -p "$LOC/npmapp"
    printf '{"name":"app","version":"1.0.0"}\n' > "$LOC/npmapp/package.json"
    t0
    if (cd "$LOC/npmapp" && timeout "$NPM_TIMEOUT" npm install $NPM_PACKAGES \
        --registry=https://registry.npmmirror.com --no-audit --no-fund \
        --loglevel=error >/dev/null 2>&1); then
        t1 "npm install $NPM_PACKAGES (local disk, same network)"
    else
        echo "  npm install $NPM_PACKAGES (local disk) did not finish either"
    fi
    rm -rf "$LOC/npmapp"
fi

echo "GIT/SMALLFILE WORKLOAD PERF DONE (run dir $RUN)"
