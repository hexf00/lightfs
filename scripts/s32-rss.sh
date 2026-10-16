#!/usr/bin/env bash
# s32-rss.sh — S3.2 supplement: server/mount RSS+CPU during seq & mt writes, A vs C.
# Correctness was proven by s32-run.sh; this only samples resource usage.
# Precise-PID lifecycle only; production :18085/:9000 untouched.
set -u
cd "$HOME/gate-s32"
DEPLOY_DIR="$HOME/gate-s32/deploy"
[[ -x "$DEPLOY_DIR/lightfs" ]] || { echo "FATAL: binary missing"; exit 1; }
[[ -x "$DEPLOY_DIR/minio" ]] || { echo "FATAL: minio missing"; exit 1; }

RUN="$HOME/gate-s32/rss-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN/tools"
MINIO_PORT=9005; CONSOLE_PORT=9006; SRV_PORT=18097
N=6000; SIZE=4096; THREADS=16
MINIO_PID=""; SRV=""; MNT_PID=""; MNT_POINT=""

log(){ echo "[$(date +%H:%M:%S)] $*"; }
fail(){ log "FATAL: $*"; cleanup; exit 1; }
port_busy(){ ss -ltn 2>/dev/null | grep -q ":$1 "; }

cleanup(){
    if [[ -n "$MNT_PID" ]]; then
        timeout 15 fusermount -u "$MNT_POINT" 2>/dev/null || timeout 15 fusermount -zu "$MNT_POINT" 2>/dev/null
        sleep 0.3; kill -9 "$MNT_PID" 2>/dev/null; MNT_PID=""
    fi
    [[ -n "$SRV" ]] && { kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""; }
    [[ -n "$MINIO_PID" ]] && { kill -9 "$MINIO_PID" 2>/dev/null; wait "$MINIO_PID" 2>/dev/null; MINIO_PID=""; }
}
trap cleanup EXIT

cat > "$RUN/tools/mtwrite.py" <<'PY'
import os, sys, time, hashlib, threading
target, total, size, threads, tag, manifest, out = sys.argv[1:8]
total, size, threads = int(total), int(size), int(threads)
os.makedirs(target, exist_ok=True)
mf = open(manifest, "a")
per = total // threads
lock = threading.Lock()
t0 = time.time()
def w(t):
    rows = []
    for i in range(t*per, (t+1)*per if t < threads-1 else total):
        data = hashlib.sha256(f"{tag}:{i}".encode()).digest() * (size // 32)
        p = os.path.join(target, f"f{i}.bin")
        with open(p, "wb") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        ms = (time.time() - t0) * 1000
        rows.append(f"{i}\t{ms:.3f}\t{hashlib.sha256(data).hexdigest()}")
    with lock:
        for r in rows: mf.write(f"{tag}\t{r}\n")
        mf.flush()
ts = [threading.Thread(target=w, args=(t,)) for t in range(threads)]
for t in ts: t.start()
for t in ts: t.join()
print(f"{time.time()-t0:.3f}")
PY

cat > "$RUN/tools/write.py" <<'PY'
import os, sys, time, hashlib
target, size, count, tag, manifest, out = sys.argv[1:7]
size, count = int(size), int(count)
os.makedirs(target, exist_ok=True)
mf = open(manifest, "a"); t0 = time.time()
for i in range(count):
    data = hashlib.sha256(f"{tag}:{i}".encode()).digest() * (size // 32)
    p = os.path.join(target, f"f{i}.bin")
    a = time.time()
    with open(p, "wb") as f:
        f.write(data); f.flush(); os.fsync(f.fileno())
    ms = (time.time() - a) * 1000
    mf.write(f"{tag}\t{i}\t{ms:.3f}\t{hashlib.sha256(data).hexdigest()}\n"); mf.flush()
print(f"{time.time()-t0:.3f}")
PY

start_sampler(){  # $1=pid $2=file
    ( while kill -0 "$1" 2>/dev/null; do
        rss=$(awk '/VmRSS/{print $2}' "/proc/$1/status" 2>/dev/null)
        cpu=$(awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null)
        echo "${cpu:-0} ${rss:-0}" >> "$2"; sleep 0.2
      done ) & SAM="$SAM $!"
}
SAM=""
stop_samplers(){ for s in $SAM; do kill "$s" 2>/dev/null; wait "$s" 2>/dev/null; done; SAM=""; }
summarize(){  # $1=file -> "cpu_s=X peak_rss_mb=Y"
    python3 - "$1" <<'PY'
import sys
jiffies = []; peak = 0
try:
    for line in open(sys.argv[1]):
        parts = line.split()
        if len(parts) < 2: continue
        jiffies.append(float(parts[0])); peak = max(peak, int(parts[1]))
except FileNotFoundError:
    pass
if not jiffies:
    print("cpu_s=0.00 peak_rss_mb=0.0"); raise SystemExit
print("cpu_s=%.2f peak_rss_mb=%.1f" % ((jiffies[-1]-jiffies[0])/100.0, peak/1024.0))
PY
}

port_busy "$MINIO_PORT" && fail "minio port $MINIO_PORT busy"
rm -rf "$RUN/minio-data"
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin2025 \
    nohup "$DEPLOY_DIR/minio" server "$RUN/minio-data" \
    --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:$CONSOLE_PORT" \
    >"$RUN/minio.log" 2>&1 &
MINIO_PID=$!
for _ in $(seq 1 100); do
    kill -0 "$MINIO_PID" 2>/dev/null || fail "minio died at startup"
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$MINIO_PORT/minio/health/live" && break
    sleep 0.2
done
curl -s -m 1 -o /dev/null "http://127.0.0.1:$MINIO_PORT/minio/health/live" || fail "minio not up"
sleep 1
log "minio up pid=$MINIO_PORT/9005"

run_group(){  # $1 = A|C
    local g=$1 GDIR="$RUN/g-$g" BUCKET="s32rss-$(echo $g | tr A-Z a-z)"
    local M="$GDIR/mnt" W="$GDIR/out"
    mkdir -p "$GDIR" "$W" "$M"
    {
        cat <<EOF
[s3]
endpoint = "http://127.0.0.1:$MINIO_PORT"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin2025"
path_style = true
service = "s3"
[metadata]
path = "$GDIR/meta.db"
[server]
bind = "127.0.0.1:$SRV_PORT"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
EOF
        if [[ "$g" == "C" ]]; then
            cat <<EOF
[commit]
mode = "writeback"
[writeback]
staging_dir = "$GDIR/wb-staging"
drain_workers = 1
batch_pack_bytes = 8388608
batch_max_wait_ms = 200
queue_max_tasks = 4096
EOF
        fi
    } > "$GDIR/cfg.toml"
    "$DEPLOY_DIR/lightfs" create-bucket --config "$GDIR/cfg.toml" >>"$RUN/exec.log" 2>&1 || fail "create-bucket $g"

    ( cd "$GDIR" && LIGHTFS_METRICS=1 nohup "$DEPLOY_DIR/lightfs" server --config "$GDIR/cfg.toml" >"$GDIR/server.log" 2>&1 & )
    SRV=$(pgrep -f "lightfs server --config $GDIR/cfg.toml" | head -1)
    [[ -n "$SRV" ]] || fail "server pid"
    for _ in $(seq 1 100); do curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" && break; sleep 0.2; done
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" || fail "server up $g"

    ( cd "$GDIR" && LIGHTFS_METRICS=1 nohup "$DEPLOY_DIR/lightfs" mount --config "$GDIR/cfg.toml" "$M" >"$GDIR/mount.log" 2>&1 & )
    for _ in $(seq 1 100); do mountpoint -q "$M" && break; sleep 0.2; done
    mountpoint -q "$M" || fail "mount up $g"
    MNT_PID=$(pgrep -f "lightfs mount --config $GDIR/cfg.toml" | head -1)
    MNT_POINT="$M"
    log "group $g: srv=$SRV mnt=$MNT_PID"
    : > "$RUN/.srv.tmp"; : > "$RUN/.mnt.tmp"
    start_sampler "$SRV" "$RUN/.srv.tmp"
    start_sampler "$MNT_PID" "$RUN/.mnt.tmp"

    local wall
    wall=$(python3 "$RUN/tools/write.py" "$M/seq" "$SIZE" "$N" seq "$GDIR/manifest.tsv" "$W/seq.tsv") || fail "seq write $g"
    sleep 3
    log "RSS group=$g phase=seq wall_s=$wall srv_$(summarize "$RUN/.srv.tmp") mnt_$(summarize "$RUN/.mnt.tmp")"
    : > "$RUN/.srv.tmp"; : > "$RUN/.mnt.tmp"

    wall=$(python3 "$RUN/tools/mtwrite.py" "$M/mt" "$N" "$SIZE" "$THREADS" mt "$GDIR/manifest.tsv" "$W/mt.tsv") || fail "mt write $g"
    sleep 3
    stop_samplers
    log "RSS group=$g phase=mt wall_s=$wall srv_$(summarize "$RUN/.srv.tmp") mnt_$(summarize "$RUN/.mnt.tmp")"
    rm -f "$RUN/.srv.tmp" "$RUN/.mnt.tmp"

    timeout 15 fusermount -u "$M" 2>/dev/null || timeout 15 fusermount -zu "$M" 2>/dev/null
    sleep 0.3; kill -9 "$MNT_PID" 2>/dev/null; MNT_PID=""
    kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""
    log "group $g done"
}

run_group A
run_group C
log "RSS SUPPLEMENT DONE -> $RUN"
