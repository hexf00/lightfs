#!/usr/bin/env bash
# S3.2: batch-pack drain for write-back — staged comparison on the real stack.
#
# Groups (write experiment):
#   A  strict per-file (main semantics)             [baseline]
#   B  write-back, per-file drain (S3.1 prototype)  [control: async only]
#   C  write-back + batch-pack drain (S3.2)         [candidate]
#
# Stages (per S3.2 process rules): smoke 100 -> medium 1000 -> full 6000 x3
# rounds. Every stage prints its own latency / request counts / file count /
# sha verdict / failure reason and the script EXITS on the first failed
# contract — no wasted downstream runtime.
#
# Read experiment (separate, after writes): R1 per-file no cache,
# R2 per-file + Q memory cache 8MiB, R3 pack + pack cache 8MiB — same
# working set each: cold read, two hot re-reads, sequential scan, and an
# over-budget scan.
#
# Remote latency: MinIO loopback only (measured baseline). ECS has no root,
# so no tc/netem injection; the OSS-latency extrapolation is done in the
# report from the measured request counts and the historical OSS PUT p50
# (~19 ms, phase B/D measurements) — explicitly labeled simulated.
#
# SAFETY (production on this host): our MinIO binds 127.0.0.1:9002
# (production holds :9000), console :9003, server :18096 (production lightfs
# holds :18085). EVERY cleanup kills an exact $PID captured at start; there
# is no `pkill` anywhere in this script. Everything lives under
# DEPLOY_DIR=~/gate-s32.

set -u

DEPLOY_DIR="${DEPLOY_DIR:-$HOME/gate-s32}"
MINIO_PORT="${MINIO_PORT:-9002}"
CONSOLE_PORT="${CONSOLE_PORT:-9003}"
SRV_PORT="${SRV_PORT:-18096}"
N_FULL="${N_FULL:-6000}"
SIZE="${SIZE:-4096}"
THREADS="${THREADS:-16}"
BATCH_BYTES=$((8 * 1024 * 1024))     # starting parameter, NOT assumed optimal
BATCH_WAIT_MS="${BATCH_WAIT_MS:-200}"
ROUNDS="${ROUNDS:-3}"
STAGES="${STAGES:-smoke med full reads}"

TS=$(date +%Y%m%d-%H%M%S)
RUN="$DEPLOY_DIR/run-$TS"
OUT="$RUN/out"
EXECLOG="$RUN/exec.log"
mkdir -p "$OUT" "$RUN/tools"

fail()  { echo "FATAL: $*" | tee -a "$EXECLOG" >&2; exit 1; }
log()   { echo "[$(date +%H:%M:%S)] $*" | tee -a "$EXECLOG"; }
note()  { echo "    . $*" | tee -a "$EXECLOG"; }
res()   { echo "$*" | tee -a "$OUT/RESULTS.md" >>"$EXECLOG"; }

# stage gate: print the verdict, abort everything on failure (S3.2 rule 6)
gate() {  # $1 = stage label, $2 = detail, $3 = 0 ok / 1 fail
    if [[ "$3" == 0 ]]; then
        res "GATE=$1 PASS $2"
        log "gate $1 PASS $2"
    else
        res "GATE=$1 FAIL $2"
        fail "stage contract failed: $1 $2 (stopping per S3.2 rule 6)"
    fi
}

for bin in python3 git fusermount curl; do
    command -v "$bin" >/dev/null || fail "$bin missing"
done
[[ -x "$DEPLOY_DIR/lightfs" ]] || fail "lightfs binary missing at $DEPLOY_DIR/lightfs"
[[ -x "$DEPLOY_DIR/minio" ]] || fail "minio binary missing at $DEPLOY_DIR/minio"
"$DEPLOY_DIR/lightfs" version 2>/dev/null | head -1 | { read -r l && log "binary: $l"; }

port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&-; return 0; } || return 1; }
port_busy "$MINIO_PORT" && fail "port $MINIO_PORT in use - refusing to share"
port_busy "$CONSOLE_PORT" && fail "port $CONSOLE_PORT in use"
port_busy "$SRV_PORT" && fail "port $SRV_PORT in use"
if pgrep -af "minio server" >"$RUN/preexisting-minio.txt" 2>/dev/null; then
    log "NOTE: pre-existing minio detected (NOT touched):"
    sed 's/^/       /' "$RUN/preexisting-minio.txt" | tee -a "$EXECLOG"
fi
log "gate-s32: run=$RUN groups=(A B C) stages=(100 1000 ${N_FULL}x${ROUNDS}) minio=:$MINIO_PORT srv=:$SRV_PORT"

##############################################################################
# lifecycle: exact PIDs only
##############################################################################
MINIO_PID=""
SRV=""; MNT_PID=""; MNT_POINT=""; MNT2_PID=""; MNT2_POINT=""
GROUP=0
GDIR=""; BUCKET=""; MANIFEST=""

stop_minio() {
    [[ -n "$MINIO_PID" ]] || return 0
    kill -9 "$MINIO_PID" 2>/dev/null; wait "$MINIO_PID" 2>/dev/null
    kill -0 "$MINIO_PID" 2>/dev/null && fail "our minio pid $MINIO_PID refuses to die"
    MINIO_PID=""
}
start_minio() {
    stop_minio
    port_busy "$MINIO_PORT" && fail "port $MINIO_PORT taken before start"
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
}

cfg_for() {  # $1 = A|B|C|QF(queue-full probe)|R1|R2|R3  -> extra TOML
    case "$1" in
        A) : ;;
        B)
            cat <<EOF
[commit]
mode = "writeback"
[writeback]
staging_dir = "$GDIR/wb-staging"
drain_workers = 1
batch_pack_bytes = 0
EOF
            ;;
        C)
            cat <<EOF
[commit]
mode = "writeback"
[writeback]
staging_dir = "$GDIR/wb-staging"
drain_workers = 1
batch_pack_bytes = $BATCH_BYTES
batch_max_wait_ms = $BATCH_WAIT_MS
queue_max_tasks = 4096
EOF
            ;;
        QF)
            cat <<EOF
[commit]
mode = "writeback"
[writeback]
staging_dir = "$GDIR/wb-staging"
drain_workers = 1
batch_pack_bytes = $BATCH_BYTES
batch_max_wait_ms = 50
queue_max_tasks = 4
EOF
            ;;
        R1) : ;;
        R2)
            cat <<EOF
[policy.limits]
memory_budget = "8MiB"

[[policy.rule]]
prefix = "/"
storage = "s3"
cache = "memory"
memory_budget = "8MiB"
EOF
            ;;
        R3)
            cat <<EOF
[commit]
mode = "writeback"
[writeback]
staging_dir = "$GDIR/wb-staging"
drain_workers = 1
batch_pack_bytes = $BATCH_BYTES
batch_max_wait_ms = $BATCH_WAIT_MS
queue_max_tasks = 4096
[pack]
enabled = true
read_cache_bytes = $((8 * 1024 * 1024))
large_file_bytes = $((1024 * 1024))
max_bytes = $BATCH_BYTES
max_age_ms = 50
EOF
            ;;
        *) fail "unknown cfg $1" ;;
    esac
}

mount_lightfs() {  # $1=cfg path $2=prefix ("" or 2)
    local cfg=$1 pre=$2
    local pid_var="MNT${pre}_PID" point_var="MNT${pre}_POINT"
    if [[ -n "${!pid_var:-}" ]]; then
        timeout 15 fusermount -u "${!point_var:-}" 2>/dev/null || timeout 15 fusermount -zu "${!point_var:-}" 2>/dev/null
        sleep 0.5
        kill -9 "${!pid_var}" 2>/dev/null
        printf -v "$pid_var" ''
    fi
    local mnt="$GDIR/mnt$pre"
    mkdir -p "$mnt"
    ( cd "$GDIR" && LIGHTFS_METRICS=1 LIGHTFS_METRICS_FILE="$GDIR/metrics-m$pre.txt" \
      nohup "$DEPLOY_DIR/lightfs" mount --config "$cfg" "$mnt" >"$GDIR/mount$pre.log" 2>&1 & )
    for _ in $(seq 1 100); do mountpoint -q "$mnt" && break; sleep 0.2; done
    mountpoint -q "$mnt" || fail "mount up ($cfg)"
    printf -v "$pid_var" '%s' "$(pgrep -f "lightfs mount --config $cfg" | head -1)"
    printf -v "$point_var" '%s' "$mnt"
}
unmount_all() {
    for pre in "" 2; do
        local pid_var="MNT${pre}_PID" point_var="MNT${pre}_POINT"
        [[ -n "${!pid_var:-}" ]] || continue
        timeout 15 fusermount -u "${!point_var}" 2>/dev/null || timeout 15 fusermount -zu "${!point_var}" 2>/dev/null
        sleep 0.3
        kill -9 "${!pid_var}" 2>/dev/null
        printf -v "$pid_var" ''
    done
}
start_server() {
    if [[ -n "${SRV:-}" ]]; then kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; fi
    ( cd "$GDIR" && LIGHTFS_METRICS=1 nohup "$DEPLOY_DIR/lightfs" server \
      --config "$GDIR/cfg.toml" >"$GDIR/server.log" 2>&1 & )
    SRV=$(pgrep -f "lightfs server --config $GDIR/cfg.toml" | head -1)
    for _ in $(seq 1 100); do
        curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" && return 0
        sleep 0.2
    done
    fail "server up ($GDIR)"
}
start_stack() {  # $1 = group label
    GROUP=$((GROUP + 1))
    GDIR="$RUN/g$GROUP-$1"
    BUCKET="s32-$(echo "$1" | tr 'A-Z' 'a-z' | tr -d '.')"
    MANIFEST="$GDIR/manifest.tsv"
    mkdir -p "$GDIR"
    {
        cat <<EOF
mode = "server"
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
        cfg_for "$1"
    } > "$GDIR/cfg.toml"
    "$DEPLOY_DIR/lightfs" create-bucket --config "$GDIR/cfg.toml" >>"$EXECLOG" 2>&1 || fail "create-bucket ($1)"
    start_server
    mount_lightfs "$GDIR/cfg.toml" ""
}
stop_stack() {
    unmount_all
    if [[ -n "${SRV:-}" ]]; then kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; unset SRV; fi
}
cleanup_all() { stop_stack 2>/dev/null; stop_minio; }
trap cleanup_all EXIT

##############################################################################
# metrics + queue + analysis helpers (same shapes as s31)
##############################################################################
rpc_metrics() { curl -s -m 2 -X POST "http://127.0.0.1:$SRV_PORT/rpc" -d '{"id":1,"method":"metrics","params":{}}'; }
metrics_snap() {
    curl -s -m 2 "http://127.0.0.1:$SRV_PORT/metrics" \
        | awk 'NR>1 && $1 ~ /^s3\./ {print $1"="$2}' > "$1"
    rpc_metrics | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)["result"]
    for k in sorted(d): print("%s=%s" % (k, d[k]))
except Exception: pass
' >> "$1"
}
metrics_delta() {
    python3 - "$1" "$2" <<'PY'
import sys
def load(p):
    d = {}
    for line in open(p):
        line = line.strip()
        if "=" in line:
            k, v = line.split("=", 1)
            try: d[k] = int(v)
            except ValueError: pass
    return d
a, b = load(sys.argv[1]), load(sys.argv[2])
keys = ["s3.put", "s3.get", "s3.delete", "s3_put", "s3_get", "s3_delete",
        "bytes_written", "bytes_read", "pack_uploads", "pack_entries",
        "pack_dead_bytes", "wb_enqueued", "wb_drained", "wb_conflicts",
        "wb_packs", "wb_overflow", "metadata_tx", "conflicts"]
out = []
for k in keys:
    d = b.get(k, 0) - a.get(k, 0)
    if d: out.append("%s=%d" % (k.replace("s3.", ""), d))
print(" ".join(out) if out else "none")
PY
}
wb_counts() {
    rpc_metrics | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["result"]
    print("%s %s" % (r.get("wb_queued", -1), r.get("wb_draining", -1)))
except Exception: print("-1 -1")
'
}
wb_wait_drained() {  # $1 = timeout_s -> stdout: seconds until queue is idle
    # idle = no queued AND no draining tasks; dead-lettered rows are
    # terminal outcomes (conflict / lost staging), not pending work
    local t0=$(date +%s) deadline=$(( $(date +%s) + $1 ))
    while (( $(date +%s) < deadline )); do
        read -r q d <<< "$(wb_counts)"
        [[ "$q" == "0" && "$d" == "0" ]] && { echo $(( $(date +%s) - t0 )); return 0; }
        sleep 0.1
    done
    echo "-1"; return 1
}
percentiles() {
    python3 - "$1" <<'PY' | tr '\n' ' '
import sys
v = []
for x in open(sys.argv[1]):
    x = x.strip().split("\t")[0]
    try: v.append(float(x))
    except ValueError: pass
v.sort()
if not v: print("n=0"); raise SystemExit
n = len(v)
print("n=%d avg=%.2f p50=%.2f p95=%.2f p99=%.2f max=%.2f" % (
    n, sum(v)/n, v[n//2], v[min(n-1, n*95//100)], v[min(n-1, n*99//100)], v[-1]))
PY
}
obj_count() {
    local dir="$RUN/minio-data/$BUCKET"
    [[ -n "$1" ]] && dir="$dir/$1"
    find "$dir" -name 'xl.meta' 2>/dev/null | wc -l
}
staging_bytes() { du -sk "$GDIR/wb-staging" 2>/dev/null | cut -f1 || echo 0; }

S_SIG=""
sample_to() {
    ( while kill -0 "$1" 2>/dev/null; do
        rss=$(awk '/VmRSS/{print $2}' "/proc/$1/status" 2>/dev/null)
        cpu=$(awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null)
        echo "${cpu:-0} ${rss:-0}" >> "$3"
        sleep 0.2
      done ) & S_SIG="$S_SIG $!"
}
sample_kill() { for s in $S_SIG; do kill "$s" 2>/dev/null; wait "$s" 2>/dev/null; done; S_SIG=""; }
summarize_sample() {
    python3 - "$1" <<'PY'
import sys
jiffies = []; peak = 0
for line in open(sys.argv[1]):
    parts = line.split()
    if len(parts) < 2: continue
    jiffies.append(float(parts[0])); peak = max(peak, int(parts[1]))
if not jiffies: print("cpu_s=0.00 peak_rss_mb=0.0"); raise SystemExit
print("cpu_s=%.2f peak_rss_mb=%.1f" % ((jiffies[-1]-jiffies[0])/100.0, peak/1024.0))
PY
}

##############################################################################
# workload tools
##############################################################################
cat > "$RUN/tools/write.py" <<'PY'
# create count files: write+fsync+close each; manifest line flushed AFTER
# close = the acked-record; per-op ms TSV; stdout wall seconds
import os, sys, time, hashlib
target, size, count, tag, manifest, out = sys.argv[1:7]
size, count = int(size), int(count)
os.makedirs(target, exist_ok=True)
mf = open(manifest, "a")
rows = []; wall0 = time.time()
for i in range(count):
    rel = "%s/f%05d.bin" % (tag, i)
    head = ("%s#%d\n" % (rel, i)).encode()
    data = (head + bytes((j * 31 + 7) % 256 for j in range(size)))[:size]
    t0 = time.time()
    fh = open(os.path.join(target, "f%05d.bin" % i), "wb")
    fh.write(data); fh.flush(); os.fsync(fh.fileno()); fh.close()
    rows.append((time.time() - t0) * 1000.0)
    mf.write("%s\t%s\n" % (rel, hashlib.sha256(data).hexdigest())); mf.flush()
mf.close()
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows: f.write("%.3f\n" % r)
print("%.3f" % (time.time() - wall0))
PY
cat > "$RUN/tools/mtwrite.py" <<'PY'
# threads writers, thread t owns files [t*per, (t+1)*per)
import os, sys, time, hashlib, threading
target, total, size, threads, tag, manifest, out = sys.argv[1:8]
total, size, threads = int(total), int(size), int(threads)
os.makedirs(target, exist_ok=True)
per = (total + threads - 1) // threads
mf = open(manifest, "a")
rows = [[] for _ in range(threads)]
barrier = threading.Barrier(threads)
wall0 = time.time()
def work(t):
    barrier.wait()
    for i in range(t * per, min((t + 1) * per, total)):
        rel = "%s/f%05d.bin" % (tag, i)
        head = ("%s#%d\n" % (rel, i)).encode()
        data = (head + bytes((j * 31 + 7) % 256 for j in range(size)))[:size]
        t0 = time.time()
        fh = open(os.path.join(target, "f%05d.bin" % i), "wb")
        fh.write(data); fh.flush(); os.fsync(fh.fileno()); fh.close()
        rows[t].append(((time.time() - t0) * 1000.0, hashlib.sha256(data).hexdigest(), rel))
ths = [threading.Thread(target=work, args=(t,)) for t in range(threads)]
for t in ths: t.start()
for t in ths: t.join()
wall = time.time() - wall0
for t in range(threads):
    for ms, sha, rel in rows[t]:
        mf.write("%s\t%s\n" % (rel, sha))
mf.close()
with open(out, "w") as f:
    f.write("op_ms\n")
    for t in range(threads):
        for ms, _, _ in rows[t]: f.write("%.3f\n" % ms)
print("%.3f" % wall)
PY
cat > "$RUN/tools/read.py" <<'PY'
# verify-read manifest slice; BAD lines to stderr; exit 2 on mismatch
import os, sys, time, hashlib
root, manifest, prefix, start, count, out = sys.argv[1:7]
start, count = int(start), int(count)
sel = [l.rstrip("\n").split("\t") for l in open(manifest) if l.split("\t")[0].startswith(prefix)]
sel = sel[start:start + count] if count >= 0 else sel[start:]
rows = []; ok = bad = 0
for rel, want in sel:
    t0 = time.time()
    try:
        with open(os.path.join(root, rel), "rb") as f:
            got = hashlib.sha256(f.read()).hexdigest()
    except OSError:
        print("BAD missing %s" % rel); bad += 1
        rows.append((time.time() - t0) * 1000.0); continue
    rows.append((time.time() - t0) * 1000.0)
    if got != want: print("BAD sha %s" % rel); bad += 1
    else: ok += 1
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows: f.write("%.3f\n" % r)
sys.stderr.write("ok=%d bad=%d\n" % (ok, bad))
sys.exit(2 if bad else 0)
PY
cat > "$RUN/tools/rewrite.py" <<'PY'
# rewrite one open file `cycles` times, fsync per cycle; manifest-append the
# final generation; stdout wall seconds; per-op ms TSV
import os, sys, time, hashlib
path, size, cycles, tag, manifest, out = sys.argv[1:7]
size, cycles = int(size), int(cycles)
data = bytes((i * 17 + 3) % 256 for i in range(size))
fh = open(path, "wb")
fh.write(data); fh.flush(); os.fsync(fh.fileno()); fh.close()
rows = []; wall0 = time.time()
for i in range(cycles):
    t0 = time.time()
    fh = open(path, "wb")
    fh.write(data); fh.flush(); os.fsync(fh.fileno()); fh.close()
    rows.append((time.time() - t0) * 1000.0)
wall = time.time() - wall0
# the last closed generation is the durable content: record it
mf = open(manifest, "a")
mf.write("%s\t%s\n" % (tag, hashlib.sha256(data).hexdigest())); mf.flush(); mf.close()
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows: f.write("%.3f\n" % r)
print("%.3f" % wall)
PY
cat > "$RUN/tools/crash_writer.py" <<'PY'
import os, sys, time, hashlib
target, size, count, tag, manifest = sys.argv[1:6]
size, count = int(size), int(count)
os.makedirs(target, exist_ok=True)
mf = open(manifest, "a")
done = 0
for i in range(count):
    rel = "%s/f%05d.bin" % (tag, i)
    head = ("%s#%d\n" % (rel, i)).encode()
    data = (head + bytes((j * 31 + 7) % 256 for j in range(size)))[:size]
    try:
        fh = open(os.path.join(target, "f%05d.bin" % i), "wb")
        fh.write(data); fh.flush(); os.fsync(fh.fileno()); fh.close()
    except OSError:
        break
    mf.write("%s\t%s\n" % (rel, hashlib.sha256(data).hexdigest()))
    mf.flush(); os.fsync(mf.fileno())
    done += 1
mf.close()
print(done)
PY

# verify acked manifest entries under root (crash contract); exit 2 on any loss
cat > "$RUN/tools/verify_acked.py" <<'PY'
import hashlib, os, sys
root, manifest, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
ok = bad = missing = 0
for line in open(manifest):
    rel, want = line.rstrip("\n").split("\t")
    if not rel.startswith(prefix): continue
    try:
        got = hashlib.sha256(open(os.path.join(root, rel), "rb").read()).hexdigest()
    except OSError:
        missing += 1; continue
    if got == want: ok += 1
    else: bad += 1
print("acked_ok=%d bad=%d missing=%d" % (ok, bad, missing))
sys.exit(2 if bad or missing else 0)
PY

##############################################################################
# one write-group pass over a fresh stack; returns via globals
##############################################################################
L=""; MNT=""
run_write_group() {  # $1=group $2=count $3=round-label  -> results under $OUT
    local g=$1 n=$2 rl=$3
    start_minio
    start_stack "$g"
    local M="$GDIR/mnt"
    local W="$OUT/w-$g-$rl"; mkdir -p "$W"

    ########## seq create + foreground latency + drain-to-remote ##########
    phase_begin "$W"
    local wall t_drain
    wall=$(python3 "$RUN/tools/write.py" "$M/seq" "$SIZE" "$n" seq "$MANIFEST" "$W/seq.tsv") || { gate "seq.write.$g.$rl" "writer crashed" 1; }
    t_drain=$(wb_wait_drained 600 || true)
    phase_end "$W" "seq.$g.$rl" "wall_s=$wall objs=$(obj_count '') packs=$(obj_count packs) staging_kb=$(staging_bytes)"
    local p; p=$(percentiles "$W/seq.tsv")
    res "FRONT=seq group=$g round=$rl wall_s=$wall drain_to_remote_s=$t_drain close_$p"
    local okv
    python3 "$RUN/tools/read.py" "$M" "$MANIFEST" seq 0 "$n" "$W/seqr.tsv" 2>"$W/seqr.err" || okv=1 || true
    [[ "${okv:-0}" == 0 ]] || gate "seq.verify.$g.$rl" "$(cat "$W/seqr.err")" 1
    gate "seq.verify.$g.$rl" "$(tail -1 "$W/seqr.err" | tr '\t' ' ')" 0

    ########## 16-thread create ##########
    phase_begin "$W"
    wall=$(python3 "$RUN/tools/mtwrite.py" "$M/mt" "$n" "$SIZE" "$THREADS" mt "$MANIFEST" "$W/mt.tsv") || { gate "mt.write.$g.$rl" "writer crashed" 1; }
    t_drain=$(wb_wait_drained 600 || true)
    phase_end "$W" "mt.$g.$rl" "wall_s=$wall objs=$(obj_count '') packs=$(obj_count packs)"
    res "FRONT=mt group=$g round=$rl wall_s=$wall drain_to_remote_s=$t_drain close_$(percentiles "$W/mt.tsv")"
    python3 "$RUN/tools/read.py" "$M" "$MANIFEST" mt 0 "$n" "$W/mtr.tsv" 2>"$W/mtr.err" \
        || gate "mt.verify.$g.$rl" "$(cat "$W/mtr.err")" 1
    gate "mt.verify.$g.$rl" "$(tail -1 "$W/mtr.err" | tr '\t' ' ')" 0

    ########## same-file rewrite x50 (expected versions + final bytes) ####
    phase_begin "$W"
    wall=$(python3 "$RUN/tools/rewrite.py" "$M/rewrite.bin" "$SIZE" 50 "rw/$g-$rl.bin" "$MANIFEST" "$W/rew.tsv") \
        || gate "rew.write.$g.$rl" "writer crashed" 1
    t_drain=$(wb_wait_drained 600 || true)
    phase_end "$W" "rew.$g.$rl" "wall_s=$wall drain_s=$t_drain"
    res "FRONT=rewrite50 group=$g round=$rl wall_s=$wall drain_to_remote_s=$t_drain cycle_$(percentiles "$W/rew.tsv")"

    ########## rename + unlink semantics ##################################
    mv "$M/seq/f00000.bin" "$M/seq/f00000-renamed.bin" 2>>"$EXECLOG" || gate "rename.$g.$rl" "mv failed" 1
    # renamed file must read back byte-correct under the new name
    python3 - "$M" "$MANIFEST" <<'PY' || gate "rename.verify.$g.$rl" "renamed bytes wrong" 1
import hashlib, sys, os
root, manifest = sys.argv[1], sys.argv[2]
for line in open(manifest):
    rel, want = line.rstrip("\n").split("\t")
    if rel == "seq/f00000.bin":
        got = hashlib.sha256(open(os.path.join(root, "seq/f00000-renamed.bin"), "rb").read()).hexdigest()
        sys.exit(0 if got == want else 1)
sys.exit(1)
PY
    gate "rename.$g.$rl" "" 0
    # unlink 50 files: gone from namespace; zero remote requests expected on B/C
    phase_begin "$W"
    for i in $(seq 1 49); do rm -f "$M/seq/$(printf 'f%05d.bin' "$i")"; done
    rm -f "$M/seq/f00000-renamed.bin"
    phase_end "$W" "unlink50.$g.$rl" ""
    [[ "$(find "$M/seq" -name 'f*.bin' | wc -l)" -eq $((n - 50)) ]] || gate "unlink.$g.$rl" "files still visible" 1
    gate "unlink.$g.$rl" "" 0

    ########## dual-mount conflict (CAS) ##################################
    cp "$GDIR/cfg.toml" "$GDIR/cfg-dual.toml"
    mount_lightfs "$GDIR/cfg-dual.toml" 2
    python3 - "$M" "${MNT2_POINT}" <<'PY' || note "conflict writer: losers expected"
import os, sys, threading
m1, m2 = sys.argv[1], sys.argv[2]
d1 = os.path.join(m1, "conf"); os.makedirs(d1, exist_ok=True)
os.makedirs(os.path.join(m2, "conf"), exist_ok=True)
def put(root, tag):
    for i in range(5):
        try:
            with open(os.path.join(root, "conf", "f%d.bin" % i), "wb") as f:
                f.write((tag + "\n").encode() * 256); f.flush(); os.fsync(f.fileno())
        except OSError:
            pass   # a loser erroring on close IS the contract (recorded)
t1 = threading.Thread(target=put, args=(m1, "one"))
t2 = threading.Thread(target=put, args=(m2, "two"))
t1.start(); t2.start(); t1.join(); t2.join()
PY
    t_drain=$(wb_wait_drained 300 || true)
    unmount_all
    mount_lightfs "$GDIR/cfg.toml" ""
    python3 - "$M" <<'PY' || gate "conflict.final.$g.$rl" "torn final state" 1
import hashlib, os, sys
d = os.path.join(sys.argv[1], "conf")
s1 = hashlib.sha256(b"one\n" * 256).hexdigest()
s2 = hashlib.sha256(b"two\n" * 256).hexdigest()
for i in range(5):
    got = hashlib.sha256(open(os.path.join(d, "f%d.bin" % i), "rb").read()).hexdigest()
    if got != s1 and got != s2: sys.exit(1)
sys.exit(0)
PY
    gate "conflict.$g.$rl" "final states untorn after remount; drain_s=$t_drain" 0

    ########## crash: kill -9 mid-write, restart, recover acked ###########
    phase_begin "$W"
    python3 "$RUN/tools/crash_writer.py" "$M/crash" "$SIZE" 400 crash "$MANIFEST" \
        > "$W/cw.out" 2>/dev/null &
    local CW=$!
    sleep 0.8
    kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; unset SRV
    sleep 1; kill -9 "$CW" 2>/dev/null; wait "$CW" 2>/dev/null
    unmount_all
    start_server
    mount_lightfs "$GDIR/cfg.toml" ""
    local rc
    t_drain=$(wb_wait_drained 600); rc=$?
    sleep 1
    local verdict
    verdict=$(python3 "$RUN/tools/verify_acked.py" "$M" "$MANIFEST" crash/ 2>&1)
    [[ $rc == 0 ]] || verdict="$verdict (DRAIN TIMEOUT)"
    phase_end "$W" "crash.$g.$rl" "recovery_drain_s=$t_drain"
    res "CRASH group=$g round=$rl $verdict recovery_drain_s=$t_drain"
    gate "crash.$g.$rl" "$verdict" $([[ "$verdict" == *"bad=0 missing=0"* ]] && echo 0 || echo 1)

    ########## fsck + unreferenced packs ##################################
    "$DEPLOY_DIR/lightfs" fsck --config "$GDIR/cfg.toml" > "$W/fsck.txt" 2>&1 || note "fsck nonzero"
    res "FSCK group=$g round=$rl $(grep -i packs "$W/fsck.txt" | head -2 | tr '\n' ' ')"

    stop_stack
    stop_minio
}

PH_BEF=""; PH_AFT=""
phase_begin() { PH_BEF="$1/bef.txt"; PH_AFT="$1/aft.txt"; metrics_snap "$PH_BEF"; }
phase_end() {  # $1=dir $2=label $3..=extras
    local dir=$1 label=$2; shift 2
    metrics_snap "$PH_AFT"
    local extra=""; for kv in "$@"; do extra="$extra $kv"; done
    res "PHASE=$label delta: $(metrics_delta "$PH_BEF" "$PH_AFT")$extra"
}

##############################################################################
# main
##############################################################################
: > "$OUT/RESULTS.md"
res "# S3.2 batch-pack drain results — $(date -u +%FT%TZ)"
res "# FRONT lines = foreground close latency (client-side ms);"
res "# drain_to_remote_s = wall time until EVERY queued write reached the store;"
res "# the two are reported separately and never substituted."

# ---------- stage 1: smoke (100 files, all contracts exercised) ----------
if [[ " $STAGES " == *" smoke "* ]]; then
log "== STAGE 1: smoke 100 files x (A B C) =="
for g in A B C; do
    run_write_group "$g" 100 smoke
done
# queue-full probe: cap 4, 20 files, all must survive (backpressure path)
start_minio
start_stack QF
python3 "$RUN/tools/write.py" "$GDIR/mnt/qf" "$SIZE" 20 qf "$MANIFEST" "$OUT/qf.tsv" >/dev/null \
    || fail "queue-full writer crashed"
t_drain=$(wb_wait_drained 300 || true)
python3 "$RUN/tools/read.py" "$GDIR/mnt" "$MANIFEST" qf 0 20 "$OUT/qfr.tsv" 2>"$OUT/qfr.err" \
    || fail "queue-full verify failed: $(cat "$OUT/qfr.err")"
QF_META=$(rpc_metrics | python3 -c '
import json, sys
r = json.load(sys.stdin)["result"]
print("overflow=%s drained=%s packs=%s" % (r.get("wb_overflow", -1), r.get("wb_drained", -1), r.get("wb_packs", -1)))
')
gate "queuefull.QF" "all 20 files byte-correct under cap=4; $QF_META drain_s=$t_drain" 0
stop_stack
stop_minio
fi

# ---------- stage 2: medium (1000 files) ----------
if [[ " $STAGES " == *" med "* ]]; then
log "== STAGE 2: medium 1000 files x (A B C) =="
for g in A B C; do
    run_write_group "$g" 1000 med
done
fi

# ---------- stage 3: full (6000 files x ROUNDS) ----------
if [[ " $STAGES " == *" full "* ]]; then
log "== STAGE 3: full $N_FULL files x (A B C) x $ROUNDS rounds =="
for r in $(seq 1 "$ROUNDS"); do
    for g in A B C; do
        run_write_group "$g" "$N_FULL" "r$r"
    done
done
fi

# ---------- read fairness experiment (S3.2 §4, independent) ----------
log "== READ fairness: R1 per-file / R2 per-file+Q cache / R3 pack+pack cache =="
read_group() {  # $1 = R1|R2|R3
    local g=$1
    start_minio
    start_stack "$g"
    local M="$GDIR/mnt"
    local W="$OUT/read-$g"; mkdir -p "$W"
    # hot set 600 x 4KiB (fits 8MiB budget) + over-budget set 6000 x 4KiB
    python3 "$RUN/tools/write.py" "$M/hot" "$SIZE" 600 hot "$MANIFEST" "$W/w1.tsv" >/dev/null
    if [[ "$g" == R3 ]]; then wb_wait_drained 300 >/dev/null; fi
    python3 "$RUN/tools/write.py" "$M/big" "$SIZE" "$N_FULL" big "$MANIFEST" "$W/w2.tsv" >/dev/null
    if [[ "$g" == R3 ]]; then wb_wait_drained 600 >/dev/null; fi
    # cold read hot set
    metrics_snap "$W/bef.txt"
    python3 "$RUN/tools/read.py" "$M" "$MANIFEST" hot 0 600 "$W/cold.tsv" 2>"$W/cold.err" \
        || gate "read.$g.cold" "$(cat "$W/cold.err")" 1
    metrics_snap "$W/aft.txt"
    res "READ=$g phase=cold600 $(metrics_delta "$W/bef.txt" "$W/aft.txt") p95=$(percentiles "$W/cold.tsv" | grep -o 'p95=[0-9.]*')"
    # two hot re-reads
    for h in 1 2; do
        metrics_snap "$W/bef.txt"
        python3 "$RUN/tools/read.py" "$M" "$MANIFEST" hot 0 600 "$W/hot$h.tsv" 2>"$W/hot$h.err" \
            || gate "read.$g.hot$h" "$(cat "$W/hot$h.err")" 1
        metrics_snap "$W/aft.txt"
        res "READ=$g phase=hot$h $(metrics_delta "$W/bef.txt" "$W/aft.txt") p95=$(percentiles "$W/hot$h.tsv" | grep -o 'p95=[0-9.]*')"
    done
    # sequential scan over the over-budget set, twice
    for p in 1 2; do
        metrics_snap "$W/bef.txt"
        python3 "$RUN/tools/read.py" "$M" "$MANIFEST" big 0 "$N_FULL" "$W/scan$p.tsv" 2>"$W/scan$p.err" \
            || gate "read.$g.scan$p" "$(cat "$W/scan$p.err")" 1
        metrics_snap "$W/aft.txt"
        res "READ=$g phase=scan$p $(metrics_delta "$W/bef.txt" "$W/aft.txt") p95=$(percentiles "$W/scan$p.tsv" | grep -o 'p95=[0-9.]*')"
    done
    # cache counters (R2: client.cache.* text rows; R3: pack_cache_* rpc)
    { curl -s -m 2 "http://127.0.0.1:$SRV_PORT/metrics" | awk 'NR>1 && $1 ~ /cache/ {print $1"="$2}'
      rpc_metrics | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["result"]
    for k in ("pack_cache_hits", "pack_cache_misses", "pack_bytes_loaded"):
        print("%s=%s" % (k, r.get(k, 0)))
except Exception: pass' ; } > "$W/cache.txt"
    res "READCACHE=$g $(tr '\n' ' ' < "$W/cache.txt")"
    gate "read.$g" "cold/hot/scan all byte-verified" 0
    stop_stack
    stop_minio
}
if [[ " $STAGES " == *" reads "* ]]; then
for g in R1 R2 R3; do read_group "$g"; done
fi

log "ALL STAGES DONE -> $OUT/RESULTS.md"
