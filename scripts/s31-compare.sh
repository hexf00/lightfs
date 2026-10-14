#!/usr/bin/env bash
# S3.1 pack/write-back prototype comparison on the real stack:
# FUSE mount -> client RPC -> server -> Core(SQLite+ObjectStore) -> MinIO.
#
# Groups (same code, same gate, same datasets; config is the only variable):
#   A   strict per-file (current main behavior)          [baseline]
#   B   write-back (durable server-side staging + drain) [prototype]
#   C   pack, seal window 50ms + 8MiB pack read cache    [prototype]
#   C2  pack, seal window 100ms + 8MiB pack read cache   [window-knob variant]
#   D   pack, 50ms window, NO read cache (Range GET)     [read-strategy control]
#   CF  pack, 50ms, no cache, retention 5s               [fragmentation/gc focus]
#
# Phases per group (all through the FUSE mount, ordinary file ops only):
#   SEQ   create+fsync+close N x 4KiB, then cold read + sha256 verify
#   MT    same, 16 concurrent writer threads, then cold read + verify
#   REW   same-file rewrite sync x100 / batch x300
#   CRTD  create-then-delete: committed (closed then unlinked) and aborted
#         (unlinked while the handle is held open)
#   SCAN  full sequential re-scan (working set > cache budget)
#   RAND  1000 random single-file reads (seeded, same picks every group)
#   SIB   consecutive reads of one write window, x2 passes (cache best case)
#   GIT   real git repo on the mount: init+commit, branch commit, checkouts
#   DUAL  two mounts (two clients): concurrent creates, cross-client
#         rename/unlink, concurrent same-file overwrite (CAS conflict)
#   COLD  re-read a cold slice after an idle gap
#   FRAG  (CF only) churn rounds -> pack/dead-byte growth -> gc -> shared-pack
#         unlink safety -> fsck
#   CRASH kill -9 the server mid-write; restart; every close-ACKED file must
#         read back byte-identical (B: after the drain queue empties)
#
# Primary outputs are request counts (server s3.* deltas + core pack/wb
# counters) and client-side per-op latency percentiles. The server runs
# LIGHTFS_METRICS=1 for the whole group (on-cost taxed equally; counts are
# the deliverable - w01 load6 precedent).
#
# SAFETY (this ECS also runs production):
#   - our MinIO listens on 127.0.0.1:9002 (production minio holds :9000);
#     console on :9003. We manage ONLY our own PID. There is no pkill of
#     minio/lightfs anywhere in this script.
#   - server bind 127.0.0.1:18096 (production lightfs holds 18085).
#   - everything lives under DEPLOY_DIR=~/gate-s31; production
#     /home/lightfs/bin/lightfs and /home/lightfs/minio-data are never
#     read, written, stopped, or counted.
#   - both ports are verified free before we bind; the script refuses to
#     start otherwise.

set -u

##############################################################################
# preflight
##############################################################################
DEPLOY_DIR="${DEPLOY_DIR:-$HOME/gate-s31}"
MINIO_PORT="${MINIO_PORT:-9002}"
CONSOLE_PORT="${CONSOLE_PORT:-9003}"
SRV_PORT="${SRV_PORT:-18096}"
S31_GROUPS="${S31_GROUPS:-A B C C2 D CF}"
N1="${N1:-6000}"            # files per dataset
SIZE1="${SIZE1:-4096}"      # bytes per file
THREADS="${THREADS:-16}"    # MT writers
PACK_MAX_BYTES=$((8 * 1024 * 1024))
PACK_CACHE_BYTES=$((8 * 1024 * 1024))
PACK_LARGE_BYTES=$((1024 * 1024))
FRAG_RETENTION="${FRAG_RETENTION:-5}"

TS=$(date +%Y%m%d-%H%M%S)
RUN="$DEPLOY_DIR/run-$TS"
OUT="$RUN/out"
EXECLOG="$RUN/exec.log"
mkdir -p "$OUT" "$RUN/tools"

fail()  { echo "FATAL: $*" | tee -a "$EXECLOG" >&2; exit 1; }
log()   { echo "[$(date +%H:%M:%S)] $*" | tee -a "$EXECLOG"; }
note()  { echo "    . $*" | tee -a "$EXECLOG"; }
res()   { echo "$*" | tee -a "$OUT/RESULTS.md" >>"$EXECLOG"; }

for bin in python3 git fusermount curl; do
    command -v "$bin" >/dev/null || fail "$bin missing"
done
[[ -x "$DEPLOY_DIR/lightfs" ]] || fail "lightfs binary missing at $DEPLOY_DIR/lightfs"
[[ -x "$DEPLOY_DIR/minio" ]] || fail "minio binary missing at $DEPLOY_DIR/minio"
"$DEPLOY_DIR/lightfs" version | head -1 | while read -r l; do log "binary: $l"; done

port_busy() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&-; return 0; } || return 1; }
port_busy "$MINIO_PORT" && fail "port $MINIO_PORT already in use - refusing to share an instance"
port_busy "$CONSOLE_PORT" && fail "port $CONSOLE_PORT already in use"
port_busy "$SRV_PORT" && fail "port $SRV_PORT already in use"
if pgrep -af "minio server" >"$RUN/preexisting-minio.txt" 2>/dev/null; then
    log "NOTE: pre-existing minio process(es) detected - NOT touched:"
    sed 's/^/       /' "$RUN/preexisting-minio.txt" | tee -a "$EXECLOG"
fi
log "gate-s31 harness: run=$RUN groups=($S31_GROUPS) minio=:$MINIO_PORT srv=:$SRV_PORT"

##############################################################################
# MinIO lifecycle - OUR pid only, never pkill
##############################################################################
MINIO_PID=""
stop_minio() {
    [[ -n "$MINIO_PID" ]] || return 0
    kill -9 "$MINIO_PID" 2>/dev/null
    wait "$MINIO_PID" 2>/dev/null
    if kill -0 "$MINIO_PID" 2>/dev/null; then
        fail "our minio pid $MINIO_PID refuses to die"
    fi
    MINIO_PID=""
}
start_minio() {
    stop_minio
    port_busy "$MINIO_PORT" && fail "port $MINIO_PORT got taken before our minio started"
    rm -rf "$RUN/minio-data"
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin2025 \
        nohup "$DEPLOY_DIR/minio" server "$RUN/minio-data" \
        --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:$CONSOLE_PORT" \
        >"$RUN/minio.log" 2>&1 &
    MINIO_PID=$!
    for _ in $(seq 1 100); do
        kill -0 "$MINIO_PID" 2>/dev/null || fail "minio died at startup (see minio.log)"
        curl -s -m 1 -o /dev/null "http://127.0.0.1:$MINIO_PORT/minio/health/live" && break
        sleep 0.2
    done
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$MINIO_PORT/minio/health/live" || fail "minio did not come up"
    sleep 1   # settle: first writes right after health can 503
}

##############################################################################
# stack lifecycle (per group)
##############################################################################
GROUP=0
GDIR=""; BUCKET=""; SRV=""; MNT_PID=""; MNT_POINT=""; MNT2_PID=""; MNT2_POINT=""
MANIFEST=""

cfg_extra() {  # $1 = group label -> extra TOML sections on stdout
    case "$1" in
        A)
            : ;;
        B)
            cat <<EOF
[commit]
mode = "writeback"
[writeback]
staging_dir = "$GDIR/wb-staging"
drain_workers = 1
EOF
            ;;
        C)
            cat <<EOF
[pack]
enabled = true
max_bytes = $PACK_MAX_BYTES
max_age_ms = 50
large_file_bytes = $PACK_LARGE_BYTES
read_cache_bytes = $PACK_CACHE_BYTES
EOF
            ;;
        C2)
            cat <<EOF
[pack]
enabled = true
max_bytes = $PACK_MAX_BYTES
max_age_ms = 100
large_file_bytes = $PACK_LARGE_BYTES
read_cache_bytes = $PACK_CACHE_BYTES
EOF
            ;;
        D)
            cat <<EOF
[pack]
enabled = true
max_bytes = $PACK_MAX_BYTES
max_age_ms = 50
large_file_bytes = $PACK_LARGE_BYTES
read_cache_bytes = 0
EOF
            ;;
        CF)
            cat <<EOF
[pack]
enabled = true
max_bytes = $PACK_MAX_BYTES
max_age_ms = 50
large_file_bytes = $PACK_LARGE_BYTES
read_cache_bytes = 0
[retention]
superseded_secs = $FRAG_RETENTION
EOF
            ;;
        *) fail "unknown group $1" ;;
    esac
}

group_desc() {
    case "$1" in
        A)  echo "strict per-file (baseline)" ;;
        B)  echo "write-back staging+drain" ;;
        C)  echo "pack 50ms + 8MiB read cache" ;;
        C2) echo "pack 100ms + 8MiB read cache" ;;
        D)  echo "pack 50ms no read cache (Range GET)" ;;
        CF) echo "pack 50ms no cache, retention ${FRAG_RETENTION}s" ;;
    esac
}

mount_lightfs() {  # $1=cfg path $2=mountpoint var-prefix ("" or 2)
    local cfg=$1 pre=$2
    local pid_var="MNT${pre}_PID" point_var="MNT${pre}_POINT"
    if [[ -n "${!pid_var:-}" ]]; then
        fusermount -u "${!point_var:-}" 2>/dev/null; sleep 0.5
        kill -9 "${!pid_var}" 2>/dev/null
        printf -v "$pid_var" ''
    fi
    local mnt="$GDIR/mnt$pre"
    mkdir -p "$mnt"
    ( cd "$GDIR" && LIGHTFS_METRICS=1 LIGHTFS_METRICS_FILE="$GDIR/metrics-m$pre.txt" \
      nohup "$DEPLOY_DIR/lightfs" mount --config "$cfg" "$mnt" \
      >"$GDIR/mount$pre.log" 2>&1 & )
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
        sleep 0.5
        kill -9 "${!pid_var}" 2>/dev/null
        printf -v "$pid_var" ''
    done
}

start_server() {  # always metrics-on: request counts are the primary output
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

start_stack() {  # $1 = group label -> fresh bucket + meta.db + server + mount
    GROUP=$((GROUP + 1))
    GDIR="$RUN/g$GROUP-$1"
    BUCKET="s31-$(echo "$1" | tr 'A-Z' 'a-z')"
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
        cfg_extra "$1"
    } > "$GDIR/cfg.toml"
    "$DEPLOY_DIR/lightfs" create-bucket --config "$GDIR/cfg.toml" >>"$EXECLOG" 2>&1 \
        || fail "create-bucket ($1)"
    start_server
    mount_lightfs "$GDIR/cfg.toml" ""
    res ""
    res "## GROUP=$1 ($(group_desc "$1")) bucket=$BUCKET cfg=$(grep -c '' "$GDIR/cfg.toml") lines"
}

stop_stack() {
    unmount_all
    if [[ -n "${SRV:-}" ]]; then kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; unset SRV; fi
}
cleanup_all() { stop_stack 2>/dev/null; stop_minio; }
trap cleanup_all EXIT

##############################################################################
# metrics: text s3.* rows + core counters via the metrics RPC
##############################################################################
rpc_metrics() {
    curl -s -m 2 -X POST "http://127.0.0.1:$SRV_PORT/rpc" \
        -d '{"id":1,"method":"metrics","params":{}}'
}
metrics_snap() {  # $1 = out file of key=value lines
    curl -s -m 2 "http://127.0.0.1:$SRV_PORT/metrics" \
        | awk 'NR>1 && $1 ~ /^s3\./ {print $1"="$2}' > "$1"
    rpc_metrics | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)["result"]
    for k in sorted(d):
        print("%s=%s" % (k, d[k]))
except Exception:
    pass
' >> "$1"
}
metrics_delta() {  # $1=before $2=after -> "put=N get=N ..." deltas
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
out = []
for k in sorted(set(a) | set(b)):
    d = b.get(k, 0) - a.get(k, 0)
    if d:
        out.append("%s=%d" % (k, d))
print(" ".join(out) if out else "none")
PY
}

wb_counts() {  # stdout: "enqueued=N drained=F conflicts=C" (or empty)
    rpc_metrics | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["result"]
    print("enqueued=%s drained=%s conflicts=%s" % (
        r.get("wb_enqueued", -1), r.get("wb_drained", -1), r.get("wb_conflicts", -1)))
except Exception:
    pass
'
}
wb_wait_drained() {  # $1 = timeout seconds; waits until drained == enqueued
    local deadline=$((SECONDS + $1))
    while (( SECONDS < deadline )); do
        local c; c=$(wb_counts)
        local enq drn
        enq=$(sed 's/enqueued=\([0-9-]*\).*/\1/' <<<"$c")
        drn=$(sed 's/.*drained=\([0-9-]*\).*/\1/' <<<"$c")
        [[ "$enq" == "$drn" && "$enq" != "-1" ]] && return 0
        sleep 0.2
    done
    return 1
}

percentiles() {  # $1 = tsv, first column -> "n=.. avg=.. p50=.. p95=.."
    python3 - "$1" <<'PY' | tr '\n' ' '
import sys
v = []
for x in open(sys.argv[1]):
    x = x.strip().split("\t")[0]
    try: v.append(float(x))
    except ValueError: pass
v.sort()
if not v:
    print("n=0"); raise SystemExit
n = len(v)
print("n=%d avg=%.2f p50=%.2f p95=%.2f p99=%.2f min=%.2f max=%.2f" % (
    n, sum(v)/n, v[n//2], v[min(n-1, n*95//100)], v[min(n-1, n*99//100)], v[0], v[-1]))
PY
}

# CPU jiffies + RSS sampler: $1=pid $2=tag $3=out-tsv
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
if not jiffies:
    print("cpu_s=0.00 peak_rss_mb=0.0"); raise SystemExit
print("cpu_s=%.2f peak_rss_mb=%.1f" % ((jiffies[-1]-jiffies[0])/100.0, peak/1024.0))
PY
}

# object count under our own minio-data only (never the production tree)
obj_count() {  # $1 = subpath filter ("" = all, "packs" = packs/ only)
    local dir="$RUN/minio-data/$BUCKET"
    [[ -n "$1" ]] && dir="$dir/$1"
    find "$dir" -name 'xl.meta' 2>/dev/null | wc -l
}

# bracket a phase: snapshot -> cmd... -> snapshot -> res line
# (GDIR-relative: resolved per call, GDIR only exists once a group started)
phase_begin() { metrics_snap "$GDIR/bef.txt"; }
phase_end() {  # $1 = label, $2.. = extra key=val notes
    local label=$1
    shift
    metrics_snap "$GDIR/aft.txt"
    local extra=""
    for kv in "$@"; do extra="$extra $kv"; done
    res "PHASE=$label group=$GROUP_DELTA_TAG delta: $(metrics_delta "$GDIR/bef.txt" "$GDIR/aft.txt")$extra"
}
GROUP_DELTA_TAG="?"

##############################################################################
# workload tools (python, FUSE-only access; manifests enable byte verify)
##############################################################################
cat > "$RUN/tools/write.py" <<'PY'
# create count files of `size` bytes under target: write+fsync+close each;
# manifest line "rel sha256" appended (and flushed) AFTER close = acked.
# stdout: wall seconds; out: per-op ms TSV
import os, sys, time, hashlib
target, size, count, tag, manifest, out = sys.argv[1:7]
size, count = int(size), int(count)
os.makedirs(target, exist_ok=True)
mf = open(manifest, "a")
rows = []
wall0 = time.time()
for i in range(count):
    rel = "%s/f%05d.bin" % (tag, i)
    head = ("%s#%d\n" % (rel, i)).encode()
    data = (head + bytes((j * 31 + 7) % 256 for j in range(size)))[:size]
    t0 = time.time()
    fh = open(os.path.join(target, "f%05d.bin" % i), "wb")
    fh.write(data)
    fh.flush()
    os.fsync(fh.fileno())
    fh.close()
    rows.append((time.time() - t0) * 1000.0)
    mf.write("%s\t%s\n" % (rel, hashlib.sha256(data).hexdigest()))
    mf.flush()
mf.close()
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows:
        f.write("%.3f\n" % r)
print("%.3f" % (time.time() - wall0))
PY

cat > "$RUN/tools/mtwrite.py" <<'PY'
# concurrent create: `threads` writers, thread t owns files [t*per, (t+1)*per)
import os, sys, time, hashlib, threading
target, total, size, threads, tag, manifest, out = sys.argv[1:8]
total, size, threads = int(total), int(size), int(threads)
os.makedirs(target, exist_ok=True)
per = (total + threads - 1) // threads
mf_lock = threading.Lock()
mf = open(manifest, "a")
rows = [[] for _ in range(threads)]
barrier = threading.Barrier(threads)
wall0 = time.time()

def work(t):
    barrier.wait()
    lo, hi = t * per, min((t + 1) * per, total)
    for i in range(lo, hi):
        rel = "%s/f%05d.bin" % (tag, i)
        head = ("%s#%d\n" % (rel, i)).encode()
        data = (head + bytes((j * 31 + 7) % 256 for j in range(size)))[:size]
        t0 = time.time()
        fh = open(os.path.join(target, "f%05d.bin" % i), "wb")
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
        fh.close()
        rows[t].append(((time.time() - t0) * 1000.0, hashlib.sha256(data).hexdigest(), rel))

ths = [threading.Thread(target=work, args=(t,)) for t in range(threads)]
for t in ths: t.start()
for t in ths: t.join()
wall = time.time() - wall0
with mf_lock:
    for t in range(threads):
        for ms, sha, rel in rows[t]:
            mf.write("%s\t%s\n" % (rel, sha))
mf.close()
with open(out, "w") as f:
    f.write("op_ms\n")
    for t in range(threads):
        for ms, _, _ in rows[t]:
            f.write("%.3f\n" % ms)
print("%.3f" % wall)
PY

cat > "$RUN/tools/read.py" <<'PY'
# verify-read manifest entries: prefix filter, [start, start+count) slice.
# per-op ms TSV; "ok=N bad=M" to stderr; exit 2 on any byte mismatch.
import os, sys, time, hashlib
root, manifest, prefix, start, count, out = sys.argv[1:7]
start, count = int(start), int(count)
sel = []
for line in open(manifest):
    rel, sha = line.rstrip("\n").split("\t")
    if rel.startswith(prefix):
        sel.append((rel, sha))
sel = sel[start:start + count] if count >= 0 else sel[start:]
rows = []
ok = bad = 0
for rel, want in sel:
    t0 = time.time()
    try:
        with open(os.path.join(root, rel), "rb") as f:
            got = hashlib.sha256(f.read()).hexdigest()
    except OSError:
        print("BAD missing %s" % rel)
        bad += 1
        rows.append((time.time() - t0) * 1000.0)
        continue
    rows.append((time.time() - t0) * 1000.0)
    if got != want:
        print("BAD sha %s" % rel)
        bad += 1
    else:
        ok += 1
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows:
        f.write("%.3f\n" % r)
sys.stderr.write("ok=%d bad=%d\n" % (ok, bad))
sys.exit(2 if bad else 0)
PY

cat > "$RUN/tools/randread.py" <<'PY'
# `k` random single-file reads over manifest prefix (seeded: identical
# picks across groups). Same output contract as read.py.
import os, sys, time, hashlib, random
root, manifest, prefix, k, seed, out = sys.argv[1:7]
k, seed = int(k), int(seed)
sel = [l.rstrip("\n").split("\t") for l in open(manifest) if l.split("\t")[0].startswith(prefix)]
rng = random.Random(seed)
picks = [sel[i] for i in (rng.randrange(len(sel)) for _ in range(k))]
rows = []
ok = bad = 0
for rel, want in picks:
    t0 = time.time()
    with open(os.path.join(root, rel), "rb") as f:
        got = hashlib.sha256(f.read()).hexdigest()
    rows.append((time.time() - t0) * 1000.0)
    if got != want: bad += 1
    else: ok += 1
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows:
        f.write("%.3f\n" % r)
sys.stderr.write("ok=%d bad=%d\n" % (ok, bad))
sys.exit(2 if bad else 0)
PY

cat > "$RUN/tools/rewrite.py" <<'PY'
# rewrite ONE open file `cycles` times; sync = fsync per cycle,
# batch = single fsync at the end. stdout: total wall seconds.
import os, sys, time
path, size, cycles, mode, out = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5]
data = bytes((i * 17 + 3) % 256 for i in range(size))
fh = open(path, "wb")
fh.write(data); fh.flush(); os.fsync(fh.fileno())   # file exists before timing
rows = []
wall0 = time.time()
for i in range(cycles):
    t0 = time.time()
    fh.seek(0)
    fh.write(data)
    fh.flush()
    if mode == "sync":
        os.fsync(fh.fileno())
        rows.append((time.time() - t0) * 1000.0)
if mode == "batch":
    t0 = time.time()
    os.fsync(fh.fileno())
    rows.append((time.time() - t0) * 1000.0)
fh.close()
wall = time.time() - wall0
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows:
        f.write("%.3f\n" % r)
print("%.3f" % wall)
PY

cat > "$RUN/tools/creatdel.py" <<'PY'
# create-then-delete; committed = write+fsync+close then unlink;
# aborted = write with the handle HELD OPEN across the unlink.
import os, sys, time
d, size, count, mode, out = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4], sys.argv[5]
os.makedirs(d, exist_ok=True)
data = bytes((i * 13 + 5) % 256 for i in range(size))
t = []
if mode == "committed":
    for i in range(count):
        t0 = time.time()
        fh = open(os.path.join(d, "f%05d.bin" % i), "wb")
        fh.write(data); fh.flush(); os.fsync(fh.fileno()); fh.close()
        t.append((time.time() - t0) * 1000.0)
    t0 = time.time()
    for i in range(count):
        os.unlink(os.path.join(d, "f%05d.bin" % i))
    t.append((time.time() - t0) * 1000.0)
else:
    for i in range(count):
        t0 = time.time()
        fh = open(os.path.join(d, "f%05d.bin" % i), "wb")
        fh.write(data)
        os.unlink(os.path.join(d, "f%05d.bin" % i))
        t.append((time.time() - t0) * 1000.0)
        fh.close()
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in t:
        f.write("%.3f\n" % r)
PY

cat > "$RUN/tools/overwrite.py" <<'PY'
# churn: rewrite each of `count` existing files once (open/write/fsync/close)
import os, sys, time
d, size, count, out = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
data = bytes((i * 23 + 11) % 256 for i in range(size))
rows = []
for i in range(count):
    t0 = time.time()
    fh = open(os.path.join(d, "f%05d.bin" % i), "wb")
    fh.write(data); fh.flush(); os.fsync(fh.fileno()); fh.close()
    rows.append((time.time() - t0) * 1000.0)
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in rows:
        f.write("%.3f\n" % r)
PY

cat > "$RUN/tools/crash_writer.py" <<'PY'
# writes count files sequentially; manifest line flushed+fsynced after each
# close = the durable acked-record. Survives the server dying underneath:
# OSError -> stop quietly (files never attempted are simply absent).
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
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
        fh.close()
    except OSError:
        break
    mf.write("%s\t%s\n" % (rel, hashlib.sha256(data).hexdigest()))
    mf.flush()
    os.fsync(mf.fileno())
    done += 1
mf.close()
print(done)
PY

##############################################################################
# per-group phase runner
##############################################################################
run_group() {  # $1 = label
    local g=$1
    start_minio
    start_stack "$g"
    GROUP_DELTA_TAG="$g"
    local MNT="$GDIR/mnt"
    local L="$OUT/g-$g"; mkdir -p "$L"

    ################ SEQ: sequential create + cold verify-read ################
    log "[$g] SEQ create $N1 x $SIZE1"
    : > "$L/sample_s.tmp"; : > "$L/sample_srv.tmp"
    sample_to "$SRV" S "$L/sample_srv.tmp"
    phase_begin
    local wall
    wall=$(python3 "$RUN/tools/write.py" "$MNT/seq" "$SIZE1" "$N1" seq "$MANIFEST" "$L/seq_write.tsv") \
        || note "[$g] SEQ write FAIL"
    sample_kill
    phase_end "SEQ.write" "wall_s=$wall objs=$(obj_count '') packs=$(obj_count packs)"
    res "SAMPLE=SEQ.write group=$g $(summarize_sample "$L/sample_srv.tmp")"
    note "[$g] SEQ write $(percentiles "$L/seq_write.tsv")"

    phase_begin
    python3 "$RUN/tools/read.py" "$MNT" "$MANIFEST" seq 0 "$N1" "$L/seq_read.tsv" 2>"$L/seq_read.err" \
        || note "[$g] SEQ READ VERIFY FAIL: $(cat "$L/seq_read.err")"
    phase_end "SEQ.coldread" "verify=$(tail -1 "$L/seq_read.err" 2>/dev/null | tr '\t' ' ')"
    note "[$g] SEQ coldread $(percentiles "$L/seq_read.tsv")"

    ################ MT: concurrent create + cold verify-read #################
    log "[$g] MT create $N1 x $SIZE1 x $THREADS threads"
    : > "$L/sample_srv2.tmp"
    sample_to "$SRV" S "$L/sample_srv2.tmp"
    phase_begin
    wall=$(python3 "$RUN/tools/mtwrite.py" "$MNT/mt" "$N1" "$SIZE1" "$THREADS" mt "$MANIFEST" "$L/mt_write.tsv") \
        || note "[$g] MT write FAIL"
    sample_kill
    phase_end "MT.write" "wall_s=$wall objs=$(obj_count '') packs=$(obj_count packs)"
    res "SAMPLE=MT.write group=$g $(summarize_sample "$L/sample_srv2.tmp")"
    note "[$g] MT write $(percentiles "$L/mt_write.tsv")"

    phase_begin
    python3 "$RUN/tools/read.py" "$MNT" "$MANIFEST" mt 0 "$N1" "$L/mt_read.tsv" 2>"$L/mt_read.err" \
        || note "[$g] MT READ VERIFY FAIL: $(cat "$L/mt_read.err")"
    phase_end "MT.coldread" "verify=$(tail -1 "$L/mt_read.err" 2>/dev/null | tr '\t' ' ')"
    note "[$g] MT coldread $(percentiles "$L/mt_read.tsv")"

    ################ B: drain queue before latency phases ####################
    if [[ "$g" == B ]]; then
        local wb0; wb0=$(wb_counts)
        local t0=$(date +%s)
        if wb_wait_drained 300; then
            res "PHASE=DRAIN group=$g start_counts=[$wb0] drained_after_s=$(( $(date +%s) - t0 ))"
        else
            res "PHASE=DRAIN group=$g TIMEOUT_300s counts=[$(wb_counts)]"
        fi
    fi

    ################ REW: same-file rewrite ##################################
    phase_begin
    wall=$(python3 "$RUN/tools/rewrite.py" "$MNT/rewrite.bin" 4096 100 sync "$L/rew_sync.tsv") \
        || note "[$g] REW sync FAIL"
    phase_end "REW.sync100" "wall_s=$wall"
    note "[$g] REW sync100 $(percentiles "$L/rew_sync.tsv")"
    phase_begin
    wall=$(python3 "$RUN/tools/rewrite.py" "$MNT/rewrite.bin" 4096 300 batch "$L/rew_batch.tsv") \
        || note "[$g] REW batch FAIL"
    phase_end "REW.batch300" "wall_s=$wall"
    # byte check: last write wins, content must equal the tool's pattern
    python3 - "$MNT/rewrite.bin" <<'PY' || note "[$g] REW BYTE CHECK FAIL"
import hashlib, sys
size = 4096
want = hashlib.sha256(bytes((i * 17 + 3) % 256 for i in range(size))).hexdigest()
got = hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest()
sys.exit(0 if got == want else 1)
PY
    phase_end "REW.verify" ""

    ################ CRTD: create-then-delete ################################
    for mode in committed aborted; do
        phase_begin
        python3 "$RUN/tools/creatdel.py" "$MNT/cd-$mode" 4096 200 "$mode" "$L/crtd_$mode.tsv" \
            || note "[$g] CRTD $mode FAIL"
        phase_end "CRTD.$mode" "objs=$(obj_count '')"
    done

    ################ SCAN + RAND #############################################
    phase_begin
    python3 "$RUN/tools/read.py" "$MNT" "$MANIFEST" seq 0 "$N1" "$L/scan_seq.tsv" 2>"$L/scan_seq.err" \
        || note "[$g] SCAN seq VERIFY FAIL"
    python3 "$RUN/tools/read.py" "$MNT" "$MANIFEST" mt 0 "$N1" "$L/scan_mt.tsv" 2>"$L/scan_mt.err" \
        || note "[$g] SCAN mt VERIFY FAIL"
    phase_end "SCAN.full" "seq=$(tail -1 "$L/scan_seq.err" 2>/dev/null | tr '\t' ' ') mt=$(tail -1 "$L/scan_mt.err" 2>/dev/null | tr '\t' ' ')"
    note "[$g] SCAN seq $(percentiles "$L/scan_seq.tsv") mt $(percentiles "$L/scan_mt.tsv")"

    phase_begin
    python3 "$RUN/tools/randread.py" "$MNT" "$MANIFEST" seq 1000 42 "$L/rand_seq.tsv" 2>"$L/rand_seq.err" \
        || note "[$g] RAND seq VERIFY FAIL"
    python3 "$RUN/tools/randread.py" "$MNT" "$MANIFEST" mt 1000 42 "$L/rand_mt.tsv" 2>"$L/rand_mt.err" \
        || note "[$g] RAND mt VERIFY FAIL"
    phase_end "RAND.1000x2" "seq=$(tail -1 "$L/rand_seq.err" 2>/dev/null | tr '\t' ' ') mt=$(tail -1 "$L/rand_mt.err" 2>/dev/null | tr '\t' ' ')"
    note "[$g] RAND seq $(percentiles "$L/rand_seq.tsv") mt $(percentiles "$L/rand_mt.tsv")"

    ################ SIB: one-window consecutive reads, x2 passes ############
    for ds in seq mt; do
        phase_begin
        python3 "$RUN/tools/read.py" "$MNT" "$MANIFEST" "$ds" 0 1000 "$L/sib_${ds}_p1.tsv" 2>"$L/sib_${ds}_p1.err" \
            || note "[$g] SIB $ds p1 VERIFY FAIL"
        python3 "$RUN/tools/read.py" "$MNT" "$MANIFEST" "$ds" 0 1000 "$L/sib_${ds}_p2.tsv" 2>"$L/sib_${ds}_p2.err" \
            || note "[$g] SIB $ds p2 VERIFY FAIL"
        phase_end "SIB.$ds" "p1=$(tail -1 "$L/sib_${ds}_p1.err" 2>/dev/null | tr '\t' ' ') p2=$(tail -1 "$L/sib_${ds}_p2.err" 2>/dev/null | tr '\t' ' ')"
    done

    ################ GIT: real repository on the mount #######################
    log "[$g] GIT workload"
    local repo="$MNT/gitrepo"
    mkdir -p "$repo"
    phase_begin
    (
        cd "$repo"
        git init -q -b master
        git config user.email s31@local
        git config user.name s31
        python3 - <<'PY'
import os
for i in range(1500):
    d = "dir%d" % (i // 100)
    os.makedirs(d, exist_ok=True)
    with open("%s/f%04d.txt" % (d, i), "w") as f:
        f.write("base %d\n" % i)
        f.write("line\n" * (i % 200 + 10))
PY
        git add -A && git commit -qm base
        git checkout -qb change
        python3 - <<'PY'
for i in range(0, 1500, 3):   # 500 files change on the branch
    with open("dir%d/f%04d.txt" % (i // 100, i), "w") as f:
        f.write("changed %d\n" % i)
        f.write("newline\n" * (i % 150 + 5))
PY
        git commit -qam change
    ) >>"$EXECLOG" 2>&1 || note "[$g] GIT setup FAIL"
    phase_end "GIT.setup" ""
    local base_branch="master"
    for c in 1 2 3; do
        phase_begin
        local t0=$(date +%s.%N)
        git -C "$repo" checkout -q "$base_branch" 2>>"$EXECLOG" || note "[$g] GIT checkout $base_branch FAIL"
        local t1=$(date +%s.%N)
        local dirty; dirty=$(git -C "$repo" status --porcelain 2>/dev/null | wc -l)
        phase_end "GIT.checkout.$base_branch.$c" "wall_s=$(python3 -c "print(f'{$t1-$t0:.3f}')") dirty=$dirty"
        t0=$(date +%s.%N)
        git -C "$repo" checkout -q change 2>>"$EXECLOG" || note "[$g] GIT checkout change FAIL"
        t1=$(date +%s.%N)
        dirty=$(git -C "$repo" status --porcelain 2>/dev/null | wc -l)
        phase_end "GIT.checkout.change.$c" "wall_s=$(python3 -c "print(f'{$t1-$t0:.3f}')") dirty=$dirty"
    done

    ################ DUAL: second mount = second client ######################
    log "[$g] DUAL two-client workload"
    cp "$GDIR/cfg.toml" "$GDIR/cfg-dual.toml"
    mount_lightfs "$GDIR/cfg-dual.toml" 2
    phase_begin
    python3 - "$MNT" "$MNT2_POINT" <<'PY' || note "[$g] DUAL concurrent FAIL"
import os, sys, threading, time
m1, m2 = sys.argv[1], sys.argv[2]
def w(root, sub):
    d = os.path.join(root, "dual", sub)
    os.makedirs(d, exist_ok=True)
    data = (sub + "\n").encode() * 512   # 2KiB, content identifies the writer
    for i in range(200):
        with open(os.path.join(d, "f%03d.bin" % i), "wb") as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
t1 = threading.Thread(target=w, args=(m1, "a"))
t2 = threading.Thread(target=w, args=(m2, "b"))
t0 = time.time(); t1.start(); t2.start(); t1.join(); t2.join()
print("dual concurrent wall %.2fs" % (time.time() - t0))
PY
    phase_end "DUAL.concurrent" ""

    # cross-client visibility of a rename done by client 1
    phase_begin
    mv "$MNT/dual/a" "$MNT/dual/a2" 2>>"$EXECLOG" || note "[$g] DUAL rename FAIL"
    phase_end "DUAL.rename" ""
    python3 - "$MNT2_POINT" <<'PY' || note "[$g] DUAL rename-visibility FAIL"
import hashlib, os, sys
d = os.path.join(sys.argv[1], "dual", "a2")
want = hashlib.sha256(("a\n" * 512).encode()).hexdigest()
bad = 0
for i in range(200):
    p = os.path.join(d, "f%03d.bin" % i)
    if hashlib.sha256(open(p, "rb").read()).hexdigest() != want:
        bad += 1
print("a2 visible from client2, bad=%d" % bad)
sys.exit(2 if bad else 0)
PY
    phase_end "DUAL.rename.verify" ""

    # cross-client unlink of the renamed dir, visibility both ways
    phase_begin
    ( for i in $(seq 0 49); do rm -f "$MNT2_POINT/dual/a2/$(printf 'f%03d.bin' "$i")"; done ) 2>>"$EXECLOG"
    phase_end "DUAL.unlink50" ""
    python3 - "$MNT" <<'PY' || note "[$g] DUAL unlink-visibility FAIL"
import os, sys
d = os.path.join(sys.argv[1], "dual", "a2")
gone = sum(os.path.exists(os.path.join(d, "f%03d.bin" % i)) is False for i in range(50))
kept = sum(os.path.exists(os.path.join(d, "f%03d.bin" % i)) for i in range(50, 200))
sys.exit(0 if gone == 50 and kept == 150 else 1)
PY
    phase_end "DUAL.unlink.verify" ""

    # concurrent same-file overwrite from both clients. Contract under test:
    # (a) the server records the CAS conflict (conflicts counter in delta);
    # (b) a loser's close reports an error - it may NOT silently "succeed";
    # (c) the committed final state is exactly one of the two patterns per
    #     file, never torn. Cross-client read visibility before remount is
    #     NOT asserted: each mount's FUSE page cache serves its own last
    #     write (K3 semantics, pre-existing). The torn-check therefore runs
    #     AFTER unmount/remount, when all client-local state is gone.
    phase_begin
    python3 - "$MNT" "$MNT2_POINT" <<'PY' || note "[$g] DUAL conflict write FAIL"
import hashlib, os, sys, threading
m1, m2 = sys.argv[1], sys.argv[2]
os.makedirs(os.path.join(m1, "dual", "conf"), exist_ok=True)
os.makedirs(os.path.join(m2, "dual", "conf"), exist_ok=True)
pat1 = (b"one\n" * 512); pat2 = (b"two\n" * 512)
losers = []
def put(tag, root, pat):
    for i in range(10):
        try:
            with open(os.path.join(root, "dual", "conf", "f%02d.bin" % i), "wb") as f:
                f.write(pat); f.flush(); os.fsync(f.fileno())
        except OSError as e:
            losers.append("%s/f%02d %s" % (tag, i, e))
t1 = threading.Thread(target=put, args=("c1", m1, pat1))
t2 = threading.Thread(target=put, args=("c2", m2, pat2))
t1.start(); t2.start(); t1.join(); t2.join()
print("conflict losers (EIO on close, expected >=1): %d" % len(losers))
for l in losers:
    print("  loser: " + l)
PY
    phase_end "DUAL.conflict" ""
    unmount_all  # client2 leaves; remount single: page caches die with the mounts
    mount_lightfs "$GDIR/cfg.toml" ""
    phase_begin
    python3 - "$MNT" <<'PY' || note "[$g] DUAL conflict FINAL-STATE FAIL (torn)"
import hashlib, os, sys
d = os.path.join(sys.argv[1], "dual", "conf")
s1 = hashlib.sha256(b"one\n" * 512).hexdigest()
s2 = hashlib.sha256(b"two\n" * 512).hexdigest()
bad = []
for i in range(10):
    got = hashlib.sha256(open(os.path.join(d, "f%02d.bin" % i), "rb").read()).hexdigest()
    if got != s1 and got != s2:
        bad.append(i)
print("final-state torn files after remount: %s" % (bad or "none"))
sys.exit(2 if bad else 0)
PY
    phase_end "DUAL.conflict.final" ""

    ################ COLD: idle gap then re-read a slice #####################
    sleep 3
    phase_begin
    python3 "$RUN/tools/read.py" "$MNT" "$MANIFEST" seq 0 100 "$L/cold.tsv" 2>"$L/cold.err" \
        || note "[$g] COLD VERIFY FAIL"
    phase_end "COLD.seq0-99" "verify=$(tail -1 "$L/cold.err" 2>/dev/null | tr '\t' ' ')"
    note "[$g] COLD $(percentiles "$L/cold.tsv")"

    ################ FRAG: CF only ###########################################
    if [[ "$g" == CF ]]; then
        log "[$g] FRAG churn + gc + shared-pack unlink safety"
        python3 - "$MNT" <<'PY'
import os, sys, threading
d = os.path.join(sys.argv[1], "frag"); os.makedirs(d, exist_ok=True)
def w(t):
    for i in range(t * 4, (t + 1) * 4):   # 25 threads x 4 = 100 files, one burst
        with open(os.path.join(d, "f%03d.bin" % i), "wb") as f:
            f.write(("frag%d\n" % i).encode() * 256); f.flush(); os.fsync(f.fileno())
ths = [threading.Thread(target=w, args=(t,)) for t in range(25)]
for t in ths: t.start()
for t in ths: t.join()
PY
        for r in $(seq 1 10); do
            phase_begin
            python3 "$RUN/tools/overwrite.py" "$MNT/frag" 1024 100 "$L/frag_r$r.tsv" \
                || note "[$g] FRAG round $r FAIL"
            phase_end "FRAG.round$r" "objs=$(obj_count '') packs=$(obj_count packs) dead_bytes_cumulative_via_deltas"
        done
        res "FRAG.trend group=$g rounds=10 packs_before=$(obj_count packs) objs_before=$(obj_count '')"
        sleep $((FRAG_RETENTION + 2))
        phase_begin
        "$DEPLOY_DIR/lightfs" gc --config "$GDIR/cfg.toml" > "$L/gc.txt" 2>&1 \
            || note "[$g] gc FAIL"
        tail -2 "$L/gc.txt" | while read -r gl; do res "GC.out group=$g $gl"; done
        phase_end "FRAG.gc" "objs_after=$(obj_count '') packs_after=$(obj_count packs)"

        # shared-pack safety: burst-created siblings survive sibling deletion + gc
        python3 - "$MNT" <<'PY'
import os, sys, threading
d = os.path.join(sys.argv[1], "shared"); os.makedirs(d, exist_ok=True)
def w(t):
    for i in range(t * 2, (t + 1) * 2):   # 25 threads x 2 = 50 files, one burst
        with open(os.path.join(d, "f%02d.bin" % i), "wb") as f:
            f.write(("shared%d\n" % i).encode() * 256); f.flush(); os.fsync(f.fileno())
ths = [threading.Thread(target=w, args=(t,)) for t in range(25)]
for t in ths: t.start()
for t in ths: t.join()
PY
        python3 - "$MNT" "$GDIR/shared.sha" <<'PY'
import hashlib, os, sys
d = os.path.join(sys.argv[1], "shared")
with open(sys.argv[2], "w") as out:
    for i in range(50):
        p = os.path.join(d, "f%02d.bin" % i)
        out.write("f%02d.bin\t%s\n" % (i, hashlib.sha256(open(p, "rb").read()).hexdigest()))
PY
        for i in $(seq 0 24); do rm -f "$MNT/shared/$(printf 'f%02d.bin' "$i")"; done
        sleep $((FRAG_RETENTION + 2))
        "$DEPLOY_DIR/lightfs" gc --config "$GDIR/cfg.toml" > "$L/gc2.txt" 2>&1 || note "[$g] gc2 FAIL"
        python3 - "$MNT" "$GDIR/shared.sha" <<'PY' || note "[$g] SHARED-PACK SURVIVAL FAIL"
import hashlib, os, sys
d = os.path.join(sys.argv[1], "shared")
bad = 0
for line in open(sys.argv[2]):
    name, want = line.rstrip("\n").split("\t")
    n = int(name[1:3])
    if n < 25:
        continue   # deleted on purpose
    got = hashlib.sha256(open(os.path.join(d, name), "rb").read()).hexdigest()
    if got != want:
        bad += 1
print("shared-pack survivors bad=%d" % bad)
sys.exit(2 if bad else 0)
PY
        phase_end "FRAG.sharedpack" ""
        "$DEPLOY_DIR/lightfs" fsck --config "$GDIR/cfg.toml" > "$L/fsck.txt" 2>&1 \
            || note "[$g] fsck FAIL"
        grep -E "pack|unreferenced|orphan|pending" "$L/fsck.txt" | head -8 | while read -r fl; do
            res "FSCK.out group=$g $fl"
        done
    fi

    ################ CRASH: kill -9 mid-write, restart, verify acked #########
    log "[$g] CRASH kill -9 mid-write + recovery"
    phase_begin
    python3 "$RUN/tools/crash_writer.py" "$MNT/crash" "$SIZE1" 600 crash "$MANIFEST" \
        > "$L/crash_writer.out" 2>"$L/crash_writer.err" &
    local CW=$!
    sleep 1.2
    kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; unset SRV
    sleep 2                       # let the writer observe EIO and exit on its own
    kill -9 "$CW" 2>/dev/null     # bounded: never block the harness on a stuck FUSE op
    wait "$CW" 2>/dev/null
    local acked; acked=$(grep -c '^crash/' "$MANIFEST" || true)
    note "[$g] CRASH acked-before-crash=$acked writer_out=$(cat "$L/crash_writer.out" 2>/dev/null)"
    unmount_all
    start_server
    mount_lightfs "$GDIR/cfg.toml" ""
    if [[ "$g" == B ]]; then
        if wb_wait_drained 300; then note "[$g] CRASH post-restart drain complete"; else
            note "[$g] CRASH post-restart drain TIMEOUT: $(wb_counts)"; fi
    fi
    sleep 1
    python3 - "$MNT" "$MANIFEST" <<'PY' > "$L/crash_verify.txt" 2>&1 || note "[$g] CRASH VERIFY FAIL (see $L/crash_verify.txt)"
import hashlib, os, sys
root, manifest = sys.argv[1], sys.argv[2]
ok = bad = missing = 0
for line in open(manifest):
    rel, want = line.rstrip("\n").split("\t")
    if not rel.startswith("crash/"):
        continue
    p = os.path.join(root, rel)
    try:
        got = hashlib.sha256(open(p, "rb").read()).hexdigest()
    except OSError:
        missing += 1
        continue
    if got == want: ok += 1
    else: bad += 1
print("CRASH acked_ok=%d bad=%d missing=%d (bad+missing>0 = CONTRACT VIOLATION)" % (ok, bad, missing))
sys.exit(2 if bad or missing else 0)
PY
    res "CRASH.verify group=$g $(cat "$L/crash_verify.txt")"
    # deltas here come from the RESTARTED server (counters reset) - may read
    # negative; the verify line above is the contract, this is bookkeeping
    phase_end "CRASH.recovery" "server_restarted=1"
    "$DEPLOY_DIR/lightfs" fsck --config "$GDIR/cfg.toml" > "$L/fsck_crash.txt" 2>&1 \
        || note "[$g] fsck(crash) FAIL"
    grep -iE "error|diverg|missing|orphan|unreferenced" "$L/fsck_crash.txt" | head -5 | while read -r fl; do
        res "FSCK.crash group=$g $fl"
    done

    stop_stack
    stop_minio
    log "[$g] group done"
}

##############################################################################
# main
##############################################################################
: > "$OUT/RESULTS.md"
res "# S3.1 comparison raw results — $(date -u +%FT%TZ)"
res "# all request counts are server-side deltas over the labeled phase;"
res "# latency percentiles are client-side (python over the FUSE mount), ms."
for g in $S31_GROUPS; do
    run_group "$g"
done
log "ALL GROUPS DONE -> $OUT/RESULTS.md"
