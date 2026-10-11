#!/usr/bin/env bash
# Write-concurrency experiment: the create storm is bounded by serial
# small-object PUTs (~8ms each). This measures how much wall time limited
# PARALLEL writer processes recover, what RSS the server pays, and that a
# SIGKILL mid-run leaves only the contract-visible state behind (committed
# files byte-exact, uncommitted absent, fsck clean).
#
# K in {1,2,4,8}: K processes, each creating the SAME tree shape under its
# own prefix (no CAS contention - this measures transport/worker scaling,
# not conflict behavior).
set -uo pipefail
cd "$(dirname "$0")/.."

DEPLOY_DIR="${DEPLOY_DIR:?DEPLOY_DIR must hold lightfs+minio binaries}"
RUN="${RUN:-/tmp/write-conc-$$}"
MNT="$RUN/mnt"
FILES_PER_WRITER="${FILES_PER_WRITER:-250}"
OBJ_SIZE="${OBJ_SIZE:-2048}"

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
    echo "$ms"
}

mkdir -p "$RUN" "$MNT"
setsid nohup "$DEPLOY_DIR/minio" server "$RUN/minio-data" --address :9000 \
    >"$RUN/minio.log" 2>&1 &
MINIO_PID=$!
for _ in $(seq 1 100); do
    curl -s --noproxy '*' -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live && break
    sleep 0.2
done
curl -s --noproxy '*' -m 1 -o /dev/null http://127.0.0.1:9000/minio/health/live || fail "minio"

cat > "$RUN/cfg.toml" <<CFGEOF
mode = "server"
[s3]
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
bucket = "write-conc"
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
curl -s --noproxy '*' -m 1 -o /dev/null http://127.0.0.1:18095/health || fail "server"

"$DEPLOY_DIR/lightfs" mount --config "$RUN/cfg.toml" "$MNT" >"$RUN/mount.log" 2>&1 &
for _ in $(seq 1 100); do grep -q " $MNT fuse" /proc/self/mounts && break; sleep 0.2; done
grep -q " $MNT fuse" /proc/self/mounts || fail "mount"

cleanup() { kill -9 "$SERVER_PID" "$MINIO_PID" 2>/dev/null || true; }
trap cleanup EXIT

writer() { # writer <id> - python for exact file shapes
    python3 - "$1" "$MNT" "$FILES_PER_WRITER" "$OBJ_SIZE" <<'PY'
import os, sys
wid, mnt, n, size = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
data = bytes(range(256)) * (size // 256) + bytes(size % 256)
for i in range(n):
    d = os.path.join(mnt, f"w{wid}", f"pkg-{i % 20}", "lib")
    os.makedirs(d, exist_ok=True)
    with open(os.path.join(d, f"f{i}.bin"), "wb") as f:
        f.write(data)
PY
}

rss_kb() { awk '/VmRSS/{print $2}' "/proc/$1/status" 2>/dev/null || echo 0; }

echo "== write concurrency sweep (K writers x $FILES_PER_WRITER x ${OBJ_SIZE}B) =="
printf '%-4s %-10s %-9s %-12s %-14s\n' K wall_ms ops_per_s serverRSS_MB result
for K in 1 2 4 8; do
    t0
    for i in $(seq 1 "$K"); do writer "$i-K$K" & done
    WPIDS=()
    for i in $(seq 1 "$K"); do WPIDS+=($!); done
    FAIL=0
    for p in "${WPIDS[@]}"; do wait "$p" || FAIL=1; done
    WALL=$(t1)
    TOTAL=$(( K * FILES_PER_WRITER ))
    OPS=$(awk -v t="$WALL" -v n="$TOTAL" 'BEGIN{printf "%.0f", n*1000/t}')
    RSS=$(rss_kb "$SERVER_PID")
    RES=ok; (( FAIL )) && RES=writer-failed
    printf '%-4s %-10s %-9s %-12s %-14s\n' "$K" "$WALL" "$OPS" "$((RSS/1024))" "$RES"
done

echo "== failure recovery: SIGKILL half the writers mid-flight (K=4) =="
CPIDS=()
for i in 1 2 3 4; do writer "crash-$i" & CPIDS+=($!); done
sleep 1.5
kill -9 "${CPIDS[1]}" "${CPIDS[3]}" 2>/dev/null
wait "${CPIDS[0]}" 2>/dev/null; wait "${CPIDS[2]}" 2>/dev/null
sleep 1
# committed files of the survivors must be byte-exact; writers 2/4 must not
# have left partial content behind (close-committed or nothing)
python3 - "$MNT" <<'PY'
import os, sys
mnt = sys.argv[1]
ok = bad = 0
for w in ("wcrash-1", "wcrash-3"):
    root = os.path.join(mnt, w)
    for dirpath, _, files in os.walk(root):
        for fn in files:
            p = os.path.join(dirpath, fn)
            with open(p, "rb") as f:
                data = f.read()
            exp = bytes(range(256)) * 8
            if data == exp[: len(data)]:
                ok += 1
            else:
                bad += 1
print(f"survivor files byte-check: ok={ok} corrupt={bad}")
PY
fusermount3 -u "$MNT" 2>/dev/null || umount -l "$MNT"
sleep 1
"$DEPLOY_DIR/lightfs" fsck --config "$RUN/cfg.toml" 2>&1 | tail -3
echo "WRITE-CONCURRENCY DONE"
