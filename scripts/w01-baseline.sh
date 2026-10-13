#!/usr/bin/env bash
# W0.1 pure-measurement baseline (write-back feasibility study, part B).
#
# Measures main@<commit> as-is on a LOCAL MinIO - never real OSS (policy:
# dev testing never bills cloud requests). Seven workloads; every number
# lands as TSV under $RESULTS so the report can cite raw files.
#
#   load1  small-file create   1K/4K/64K x 1000/10000, lightfs vs local FS
#   load2  same-file rewrite   10/100/1000 commits, sync-flush vs batch
#   load3  create-then-delete  commit-then-unlink vs unlink-before-commit
#   load4  git clone/checkout  60k-file repo, split fetch/checkout, x3
#   load5  npm install         fixed lockfile, download split from writes
#   load6  read-cache matrix   none vs memory (cold/warm), small-budget scan
#   load7  Windows             NOT runnable here - see report, no script
#
# Discipline: timing runs keep LIGHTFS_METRICS off (the on-cost is a
# mutex'd registry update per op); a dedicated metrics-on rep per shape
# collects PUT/GET counts (mount-side table + server /metrics delta).
# Synthetic workloads x$REPS (default 5), git x$GIT_REPS (default 3).
# Failures are logged and skipped, never silently averaged away.
# Workload groups get a FRESH MinIO data dir + meta.db + bucket: no
# cross-group warm state, disk stays bounded.
#
#   DEPLOY_DIR=~/gate-m bash scripts/w01-baseline.sh [--quick]
set -uo pipefail

DEPLOY_DIR="${DEPLOY_DIR:-$HOME/gate-m}"
RESULTS="${RESULTS:-$HOME/w01-results}"
REPS="${REPS:-5}"
GIT_REPS="${GIT_REPS:-3}"
QUICK="${QUICK:-0}"
[[ "${1:-}" == "--quick" ]] && { QUICK=1; REPS=2; GIT_REPS=1; }
NPM_REPS=$( [[ $QUICK = 1 ]] && echo 1 || echo 3 )

MINIO_PORT=9000          # our own MinIO instance; the running one is stopped
SRV_PORT=18096           # lightfs server (18085=prod, 18086/18087=other gates)
TS=$(date +%Y%m%d-%H%M%S)
COMMIT=$(cd "$(dirname "$0")/.." && git rev-parse --short HEAD 2>/dev/null || echo unknown)
OUT="$RESULTS/$TS"
RUN="$OUT/run"
mkdir -p "$RUN/tools"
EXECLOG="$OUT/exec.log"
: > "$EXECLOG"

fail() { echo "FAIL: $1" | tee -a "$EXECLOG" >&2; exit 1; }
log()  { echo "[$(date +%H:%M:%S)] $*" | tee -a "$EXECLOG"; }
note() { echo "$*" >> "$EXECLOG"; }

for bin in python3 git fusermount curl; do
    command -v "$bin" >/dev/null || fail "$bin missing"
done
[[ -x "$DEPLOY_DIR/lightfs" ]] || fail "lightfs binary missing at $DEPLOY_DIR"
[[ -x "$DEPLOY_DIR/minio" ]] || fail "minio binary missing at $DEPLOY_DIR"

MINIO_VER=$("$DEPLOY_DIR/minio" --version 2>/dev/null | head -1)
cat > "$OUT/env.txt" <<EOF
commit: $COMMIT
date: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
kernel: $(uname -r)
cpu: $(nproc) x $(grep -m1 "model name" /proc/cpuinfo | cut -d: -f2 | sed 's/^ *//')
mem_total: $(free -h | awk '/Mem:/{print $2}')
disk: $(df -h /home | tail -1 | awk '{print $2" total, "$4" free"}') on /dev/vda1
disk_layout_note: MinIO data, lightfs meta.db, and the local-FS baseline all share /dev/vda1
minio: $MINIO_VER
lightfs: $("$DEPLOY_DIR/lightfs" --version 2>/dev/null | head -1)
git: $(git --version)
python: $(python3 --version 2>&1)
node: $(node --version 2>&1)
npm: $(npm --version 2>&1)
config: fresh per group - mode=server, s3=http://127.0.0.1:$MINIO_PORT path_style=true, cache.metadata=off, cache.data=off
reps: synthetic=$REPS git=$GIT_REPS npm=$NPM_REPS quick=$QUICK
EOF

# ---------------------------------------------------------------------------
# MinIO lifecycle: the w01 run OWNS :9000 while it executes. Pattern
# "minio server" covers every invocation path (bin/, gate-m/ symlink);
# stop VERIFIES the process is gone - a survivor serving health checks
# from a deleted data dir is exactly the 503-SlowDownWrite trap.
MINIO_PID=""
stop_minio()  {
    pkill -f "minio server" 2>/dev/null
    for _ in $(seq 1 50); do
        pgrep -f "minio server" >/dev/null || break
        sleep 0.2
    done
    if pgrep -f "minio server" >/dev/null; then
        fail "a minio process refuses to die - refusing to measure against a stale instance"
    fi
    sleep 1   # let :9000 drain before the next bind
}
start_minio() {
    stop_minio
    rm -rf "$RUN/minio-data"
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin2025 \
        nohup "$DEPLOY_DIR/minio" server "$RUN/minio-data" \
        --address "127.0.0.1:$MINIO_PORT" --console-address 127.0.0.1:9001 \
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

GROUP=0
start_stack() {  # $1 = group label -> fresh bucket + meta.db + server + mount
    GROUP=$((GROUP + 1))
    GDIR="$RUN/g$GROUP-$1"
    BUCKET="w01-$1"
    mkdir -p "$GDIR/mnt"
    cat > "$GDIR/cfg.toml" <<EOF
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
    local ok=0
    for attempt in 1 2 3 4 5; do
        if "$DEPLOY_DIR/lightfs" create-bucket --config "$GDIR/cfg.toml" >>"$EXECLOG" 2>&1; then
            ok=1; break
        fi
        note "create-bucket retry $attempt ($1)"; sleep 2
    done
    [[ $ok = 1 ]] || fail "create-bucket ($1)"
    "$DEPLOY_DIR/lightfs" server --config "$GDIR/cfg.toml" >"$GDIR/server.log" 2>&1 &
    SRV=$!
    for _ in $(seq 1 100); do
        curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" && break
        sleep 0.2
    done
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" || fail "server up ($1)"
    mount_lightfs "$GDIR/cfg.toml" off ""
}
mount_lightfs() {  # $1=cfg path $2=on|off $3=metrics-file ("" = default $GDIR/metrics.txt)
    if [[ -n "${MNT_PID:-}" ]]; then
        fusermount -u "${MNT_POINT:-}" 2>/dev/null; sleep 0.5
        kill -9 "$MNT_PID" 2>/dev/null
        unset MNT_PID
    fi
    local cfg=$1 mfile=$3
    [[ -z "$mfile" ]] && mfile="$(dirname "$cfg")/metrics.txt"
    MNT_POINT="$(dirname "$cfg")/mnt"
    mkdir -p "$MNT_POINT"
    if [[ "$2" == on ]]; then
        ( cd "$(dirname "$cfg")" && LIGHTFS_METRICS=1 LIGHTFS_METRICS_FILE="$mfile" \
          nohup "$DEPLOY_DIR/lightfs" mount --config "$cfg" "$MNT_POINT" \
          >"$(dirname "$cfg")/mount.log" 2>&1 & )
    else
        ( cd "$(dirname "$cfg")" && nohup "$DEPLOY_DIR/lightfs" mount --config "$cfg" \
          "$MNT_POINT" >"$(dirname "$cfg")/mount.log" 2>&1 & )
    fi
    for _ in $(seq 1 100); do mountpoint -q "$MNT_POINT" && break; sleep 0.2; done
    mountpoint -q "$MNT_POINT" || fail "mount up ($cfg)"
    MNT_PID=$(pgrep -f "lightfs mount --config $cfg" | head -1)
}
unmount_and_keep() {  # graceful: the mount writes its metrics file on destroy
    [[ -n "${MNT_PID:-}" ]] || return 0
    fusermount -u "$MNT_POINT" 2>/dev/null; sleep 1
    kill -9 "$MNT_PID" 2>/dev/null
    unset MNT_PID
}
# server lifecycle beyond start_stack: attribution runs restart it with
# LIGHTFS_METRICS=1 (same meta.db/bucket) so /metrics carries s3.* rows;
# timing servers stay metrics-off.
start_server() {  # $1 = gdir, $2 = on|off
    if [[ -n "${SRV:-}" ]]; then kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; fi
    if [[ "$2" == on ]]; then
        ( cd "$1" && LIGHTFS_METRICS=1 nohup "$DEPLOY_DIR/lightfs" server \
          --config "$1/cfg.toml" >"$1/server.log" 2>&1 & )
    else
        ( cd "$1" && nohup "$DEPLOY_DIR/lightfs" server --config "$1/cfg.toml" \
          >"$1/server.log" 2>&1 & )
    fi
    SRV=$(pgrep -f "lightfs server --config $1/cfg.toml" | head -1)
    for _ in $(seq 1 100); do
        curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" && return 0
        sleep 0.2
    done
    fail "server restart ($2)"
}
# attribution bracket: metrics-on server + metrics-on mount around one rep
attr_begin() {
    unmount_and_keep
    start_server "$GDIR" on
    mount_lightfs "$GDIR/cfg.toml" on "$GDIR/attr-metrics.txt"
    metrics_snapshot "$GDIR/bef.txt"
}
attr_end() {  # $1 = tag line for attr.txt
    unmount_and_keep
    metrics_snapshot "$GDIR/aft.txt"
    { echo "$1 server_delta: $(metrics_delta "$GDIR/bef.txt" "$GDIR/aft.txt")"
      grep -v '^op ' "$GDIR/attr-metrics.txt" 2>/dev/null | head -18; } >> "$ATTR_FILE"
    start_server "$GDIR" off
    mount_lightfs "$GDIR/cfg.toml" off ""
}
stop_stack() {
    unmount_and_keep
    if [[ -n "${SRV:-}" ]]; then kill -9 "$SRV" 2>/dev/null; unset SRV; fi
}
cleanup_all() { stop_stack 2>/dev/null; [[ -n "$MINIO_PID" ]] && kill -9 "$MINIO_PID" 2>/dev/null; stop_minio 2>/dev/null; }
trap cleanup_all EXIT

# CPU jiffies + RSS sampler: $1=pid $2=tag $3=out-tsv (cpu_jiffies rss_kb)
SAMPLE_W_SIG=""; SAMPLE_M_SIG=""
sample_to() {
    ( while kill -0 "$1" 2>/dev/null; do
        rss=$(awk '/VmRSS/{print $2}' "/proc/$1/status" 2>/dev/null)
        cpu=$(awk '{print $14+$15}' "/proc/$1/stat" 2>/dev/null)
        echo "${cpu:-0} ${rss:-0}" >> "$3"
        sleep 0.2
      done ) & eval "SAMPLE_$2_SIG=$!"
}
sample_kill() { for s in $SAMPLE_W_SIG $SAMPLE_M_SIG; do [[ -n "$s" ]] && { kill "$s" 2>/dev/null; wait "$s" 2>/dev/null; }; done; }
# sums a sampler file -> "cpu_s=F peak_rss_mb=F"
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

percentiles() {  # $1=tsv with header -> stats of FIRST column, header skipped
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

cat > "$RUN/tools/writer.py" <<'PY'
# create count files of `size` bytes: write+fsync+close each; per-op ms TSV
import os, sys, time
target, size, count, out = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
os.makedirs(target, exist_ok=True)
data = bytes((i * 31 + 7) % 256 for i in range(size))
t_total, t_fsync = [], []
for i in range(count):
    t0 = time.time()
    fh = open(os.path.join(target, "f%05d.bin" % i), "wb")
    fh.write(data)
    fh.flush()
    t1 = time.time()
    os.fsync(fh.fileno())
    t2 = time.time()
    fh.close()
    t_total.append((t2 - t0) * 1000.0)
    t_fsync.append((t2 - t1) * 1000.0)
with open(out, "w") as fh:
    fh.write("op_ms\tfsync_ms\n")
    for a, b in zip(t_total, t_fsync):
        fh.write("%.3f\t%.3f\n" % (a, b))
PY

cat > "$RUN/tools/rewriter.py" <<'PY'
# rewrite ONE open file `cycles` times; sync = fsync per cycle,
# batch = single fsync at the end. stdout: total wall seconds.
import os, sys, time
path, size, cycles, mode = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), sys.argv[4]
out = sys.argv[5]
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
# aborted = write with the handle HELD OPEN across the unlink (the
# write session detaches and dies on close - no commit, no PUT).
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
        fh.close()
        t.append((time.time() - t0) * 1000.0)
with open(out, "w") as f:
    f.write("op_ms\n")
    for r in t:
        f.write("%.3f\n" % r)
PY

obj_count() { find "$RUN/minio-data/$1" -name 'xl.meta' 2>/dev/null | wc -l; }
metrics_get() {
    curl -s -m 2 "http://127.0.0.1:$SRV_PORT/metrics" \
        | awk 'NR>1 && $1 ~ /^s3\./ {print $1"="$2}'
}
metrics_snapshot() { metrics_get > "$1"; }
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
        out.append("%s=%d" % (k.replace("s3.", ""), d))
print(" ".join(out) if out else "none")
PY
}

# ===========================================================================
log "== load1: small-file create =="
start_minio
start_stack small
L1="$OUT/load1"; mkdir -p "$L1"
echo "size	count	target	rep	total_s	ops_per_s" > "$L1/summary.tsv"
for size in 1024 4096 65536; do
  for count in 1000 10000; do
    [[ $QUICK = 1 && $count = 10000 ]] && continue
    for target in lightfs local; do
      if [[ $target = lightfs ]]; then dir="$GDIR/mnt/l1"; else dir="$RUN/local-l1"; fi
      for rep in $(seq 1 "$REPS"); do
        rm -rf "$dir"
        t0=$(date +%s.%N)
        SAMPLE_W_SIG=""; SAMPLE_M_SIG=""
        python3 "$RUN/tools/writer.py" "$dir" "$size" "$count" "$L1/w.tsv" &
        WPID=$!
        : > "$L1/sample_w.tmp"; : > "$L1/sample_m.tmp"
        sample_to "$WPID" W "$L1/sample_w.tmp"
        [[ $target = lightfs ]] && sample_to "$MNT_PID" M "$L1/sample_m.tmp"
        wait $WPID || { note "load1 writer FAIL s$size n$count $target rep$rep"; sample_kill; continue; }
        sleep 0.3
        sample_kill
        t1=$(date +%s.%N)
        total=$(python3 -c "print(f'{$t1-$t0:.3f}')")
        ops=$(python3 -c "print(f'{$count/$total:.1f}')")
        echo -e "$size	$count	$target	$rep	$total	$ops" >> "$L1/summary.tsv"
        mv "$L1/w.tsv" "$L1/w_s${size}_n${count}_${target}_r${rep}.tsv"
        summarize_sample "$L1/sample_w.tmp" > "$L1/cpu_s${size}_n${count}_${target}_r${rep}.txt"
        [[ $target = lightfs ]] && summarize_sample "$L1/sample_m.tmp" >> "$L1/cpu_s${size}_n${count}_${target}_r${rep}.txt"
        note "load1 s$size n$count $target rep$rep total=${total}s $(cat "$L1/cpu_s${size}_n${count}_${target}_r${rep}.txt" | tr '\n' ' ')"
      done
    done
    # attribution rep: metrics ON server+mount - exact request counts
    ATTR_FILE="$L1/attr.txt"
    rm -rf "$GDIR/mnt/l1"
    attr_begin
    python3 "$RUN/tools/writer.py" "$GDIR/mnt/l1" "$size" "$count" "$L1/w.tsv"
    attr_end "s$size n$count"
    log "load1 s$size n$count done"
  done
done
stop_stack
stop_minio

# ===========================================================================
log "== load2: same-file rewrite =="
start_minio
start_stack rewrite
L2="$OUT/load2"; mkdir -p "$L2"
echo "cycles	mode	rep	wall_s" > "$L2/summary.tsv"
echo "# attr: cycles mode server_delta" > "$L2/attr.txt"
for cycles in 10 100 1000; do
  for mode in sync batch; do
    for rep in $(seq 1 "$REPS"); do
      wall=$(python3 "$RUN/tools/rewriter.py" "$GDIR/mnt/rewrite.bin" 4096 "$cycles" "$mode" "$L2/w.tsv") \
          || { note "load2 FAIL c$cycles $mode rep$rep"; continue; }
      echo -e "$cycles	$mode	$rep	$wall" >> "$L2/summary.tsv"
    done
    ATTR_FILE="$L2/attr.txt"
    attr_begin
    python3 "$RUN/tools/rewriter.py" "$GDIR/mnt/rewrite.bin" 4096 "$cycles" "$mode" "$L2/w.tsv" >/dev/null
    attr_end "c$cycles $mode"
    log "load2 c$cycles $mode done"
  done
done
stop_stack
stop_minio

# ===========================================================================
log "== load3: create-then-delete =="
start_minio
start_stack cd
L3="$OUT/load3"; mkdir -p "$L3"
echo "mode	rep	objects_immediately_after	retire_settle_60s" > "$L3/objects.tsv"
: > "$L3/attr.txt"
N3=1000
for mode in committed aborted; do
  for rep in $(seq 1 "$REPS"); do
    rm -rf "$GDIR/mnt/cd"
    ATTR_FILE="$L3/attr.txt"
    attr_begin
    python3 "$RUN/tools/creatdel.py" "$GDIR/mnt/cd" 4096 "$N3" "$mode" "$L3/w_${mode}_r${rep}.tsv" \
        || note "load3 FAIL $mode rep$rep"
    obj0=$(obj_count w01-cd)
    attr_end "$mode rep$rep"
    echo -e "$mode	$rep	$obj0" >> "$L3/objects.tsv"
    note "load3 $mode rep$rep objects_after=$obj0"
  done
  sleep 60   # retire/GC observation window
  echo -e "$mode	settle	$(obj_count w01-cd)" >> "$L3/objects.tsv"
done
note "ground truth = xl.meta file count under minio-data/w01-cd; s3.delete deltas in attr.txt"
stop_stack
stop_minio

# ===========================================================================
log "== load6: read-cache matrix =="
start_minio
start_stack rc
# metrics-on server for the WHOLE workload: GET counts per pass are the
# primary output; the on-cost taxes these read timings equally across all
# three cache configs (documented in the report)
start_server "$GDIR" on
L6="$OUT/load6"; mkdir -p "$L6"
# dataset: one 200MiB sequential target + 1000 x 4KiB targets
python3 - "$GDIR/mnt" <<'PY'
import os, sys
root = sys.argv[1]
big = bytes((i * 7 + 11) % 256 for i in range(1 << 20))
with open(os.path.join(root, "big.bin"), "wb") as f:
    for _ in range(200):
        f.write(big)
os.makedirs(os.path.join(root, "many"), exist_ok=True)
small = bytes(4096)
for i in range(1000):
    with open(os.path.join(root, "many", "f%04d.bin" % i), "wb") as f:
        f.write(small)
PY
cat > "$RUN/tools/readpass.py" <<'PY'
# full read of big.bin then the 1000 small files; per-phase seconds TSV
import os, sys, time
root, out = sys.argv[1], sys.argv[2]
t0 = time.time()
with open(os.path.join(root, "big.bin"), "rb") as f:
    while f.read(1 << 20):
        pass
t1 = time.time()
for i in range(1000):
    with open(os.path.join(root, "many", "f%04d.bin" % i), "rb") as f:
        f.read()
t2 = time.time()
with open(out, "w") as f:
    f.write("big_s	many_s\n%.3f	%.3f\n" % (t1 - t0, t2 - t1))
PY
cat > "$RUN/tools/readpass_many.py" <<'PY'
# small files ONLY, no sequential scan in between: the access pattern the
# memory cache can actually serve from (big.bin's 200MiB scan evicts
# everything under a 64MiB budget - R-audit's LRU worst case).
import os, sys, time
root, out = sys.argv[1], sys.argv[2]
t0 = time.time()
for i in range(1000):
    with open(os.path.join(root, "many", "f%04d.bin" % i), "rb") as f:
        f.read()
t1 = time.time()
with open(out, "w") as f:
    f.write("many_s\n%.3f\n" % (t1 - t0))
PY
cache_mount() {  # $1 = none|memory|small
    local kind=$1 budget=64
    [[ "$kind" == small ]] && budget=8
    local cfg="$GDIR/cfg-$kind.toml"
    if [[ "$kind" == none ]]; then
        cp "$GDIR/cfg.toml" "$cfg"
    else
        cat > "$cfg" <<EOF
mode = "server"
[s3]
endpoint = "http://127.0.0.1:$MINIO_PORT"
region = "us-east-1"
bucket = "w01-rc"
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
[policy.limits]
memory_budget = "${budget}MiB"

[[policy.rule]]
prefix = "/"
storage = "s3"
cache = "memory"
memory_budget = "${budget}MiB"
EOF
    fi
    mount_lightfs "$cfg" on "$GDIR/metrics-$kind.txt"
}
for kind in none memory small; do
    cache_mount "$kind"
    # full passes (scan + small): warm hits are expected to be ~0 for the
    # scan shape (LRU worst case) - recorded as-is
    for pass in cold warm warm2; do
        metrics_snapshot "$L6/bef.txt"
        python3 "$RUN/tools/readpass.py" "$MNT_POINT" "$L6/r_${kind}_${pass}.tsv" \
            || note "load6 FAIL $kind $pass"
        metrics_snapshot "$L6/aft.txt"
        note "load6 $kind $pass: $(tail -1 "$L6/r_${kind}_${pass}.tsv" 2>/dev/null) delta: $(metrics_delta "$L6/bef.txt" "$L6/aft.txt")"
    done
    # small-files-only repeats: the shape a memory cache CAN serve
    for pass in manywarm manywarm2; do
        metrics_snapshot "$L6/bef.txt"
        python3 "$RUN/tools/readpass_many.py" "$MNT_POINT" "$L6/r_${kind}_${pass}.tsv" \
            || note "load6 FAIL $kind $pass"
        metrics_snapshot "$L6/aft.txt"
        note "load6 $kind $pass: $(tail -1 "$L6/r_${kind}_${pass}.tsv" 2>/dev/null) delta: $(metrics_delta "$L6/bef.txt" "$L6/aft.txt")"
    done
    unmount_and_keep
    { echo "== $kind =="; grep -v '^op ' "$GDIR/metrics-$kind.txt" 2>/dev/null; } >> "$L6/metrics.txt"
done
stop_stack
stop_minio

# ===========================================================================
log "== load4: git clone/checkout (60k files) =="
start_minio
start_stack git
L4="$OUT/load4"; mkdir -p "$L4"
SEED="$RUN/git-seed"
GIT_FILES=60000
if [[ ! -d "$SEED/.git" ]]; then
    log "seeding $GIT_FILES-file repo on local disk..."
    python3 - "$SEED" <<'PY'
import os, sys
root = sys.argv[1]
data = b"payload-plaintext-16B-pad" * 64   # 1.5 KiB deterministic
for d in range(60):
    dd = os.path.join(root, "src", "dir%02d" % d)
    os.makedirs(dd, exist_ok=True)
    for f in range(1000):
        with open(os.path.join(dd, "f%04d.txt" % f), "wb") as fh:
            fh.write(data)
PY
    git init -q "$SEED"
    git -C "$SEED" add -A
    git -C "$SEED" -c user.name=perf -c user.email=perf@local commit -qm seed
    log "seed ready: $(git -C "$SEED" ls-files | wc -l) files"
fi
export GIT_TRACE_PERFORMANCE=1
echo "rep	target	fetch_unpack_s	checkout_s	total_s" > "$L4/summary.tsv"
for rep in $(seq 1 "$GIT_REPS"); do
    rm -rf "$GDIR/mnt/repo" "$RUN/repo-local"
    t0=$(date +%s.%N)
    git clone -q --no-checkout "file://$SEED" "$GDIR/mnt/repo" 2> "$L4/trace_mnt_r$rep.txt" \
        || note "load4 clone FAIL mnt rep$rep"
    t1=$(date +%s.%N)
    git -C "$GDIR/mnt/repo" checkout -q 2>> "$L4/trace_mnt_r$rep.txt" \
        || note "load4 checkout FAIL mnt rep$rep"
    t2=$(date +%s.%N)
    echo -e "$rep	lightfs	$(python3 -c "print(f'{$t1-$t0:.2f}')")	$(python3 -c "print(f'{$t2-$t1:.2f}')")	$(python3 -c "print(f'{$t2-$t0:.2f}')")" >> "$L4/summary.tsv"
    t0=$(date +%s.%N)
    git clone -q --no-checkout "file://$SEED" "$RUN/repo-local" 2> "$L4/trace_loc_r$rep.txt" \
        || note "load4 clone FAIL local rep$rep"
    t1=$(date +%s.%N)
    git -C "$RUN/repo-local" checkout -q 2>> "$L4/trace_loc_r$rep.txt" \
        || note "load4 checkout FAIL local rep$rep"
    t2=$(date +%s.%N)
    echo -e "$rep	local	$(python3 -c "print(f'{$t1-$t0:.2f}')")	$(python3 -c "print(f'{$t2-$t1:.2f}')")	$(python3 -c "print(f'{$t2-$t0:.2f}')")" >> "$L4/summary.tsv"
    rm -rf "$GDIR/mnt/repo" "$RUN/repo-local"
    note "load4 rep$rep done"
done
stop_stack
stop_minio

# ===========================================================================
log "== load5: npm install (fixed lockfile) =="
start_minio
start_stack npm
L5="$OUT/load5"; mkdir -p "$L5"
export npm_config_cache="$RUN/npm-cache"
export npm_config_registry="https://registry.npmmirror.com"
export npm_config_update_notifier=false
PJ="$RUN/npm-src"
mkdir -p "$PJ"
cat > "$PJ/package.json" <<'EOF'
{ "name": "w01-npm", "version": "1.0.0", "private": true,
  "dependencies": { "express": "4.21.2", "lodash": "4.17.21", "moment": "2.30.1", "rxjs": "7.8.1" } }
EOF
(cd "$PJ" && npm install --package-lock-only >>"$EXECLOG" 2>&1) || fail "lockfile generation"
cp "$PJ/package-lock.json" "$L5/package-lock.json"
runnpm() {  # $1=dest rest=extra npm args; prints wall seconds (or -1 on failure)
    local dest=$1; shift
    rm -rf "$dest/node_modules"
    cp "$PJ/package.json" "$PJ/package-lock.json" "$dest/"
    local t0=$(date +%s.%N)
    (cd "$dest" && npm ci --no-audit --no-fund "$@" >>"$EXECLOG" 2>&1) \
        || { note "npm ci FAIL dest=$dest args=$*"; echo "-1"; return; }
    local t1=$(date +%s.%N)
    python3 -c "print(f'{$t1-$t0:.2f}')"
}
mkdir -p "$RUN/npml" "$GDIR/mnt/npmf"
: > "$L5/a1.txt"; : > "$L5/a2.txt"; : > "$L5/a3.txt"
for rep in $(seq 1 "$NPM_REPS"); do
    rm -rf "$RUN/npm-cache"                      # A1 must be COLD every rep
    runnpm "$RUN/npml" >> "$L5/a1.txt"      # cold cache, local: download + write
    runnpm "$RUN/npml" --offline >> "$L5/a2.txt"  # warm cache, local: writes only
    runnpm "$GDIR/mnt/npmf" --offline >> "$L5/a3.txt"  # warm cache, mount: writes only
    note "load5 rep$rep done"
done
note "load5 node_modules files: $(find "$RUN/npml/node_modules" -type f 2>/dev/null | wc -l)"
stop_stack
stop_minio

# ===========================================================================
log "== summarizing =="
{
echo "=== load1 per-op stats (first column = full op incl fsync; fsync_ms col in raw TSVs) ==="
for f in "$L1"/w_*.tsv; do echo "$(basename "$f"): $(percentiles "$f")"; done
echo; echo "=== load1 attribution (exact server-side request deltas) ==="; cat "$L1/attr.txt"
echo; echo "=== load2 wall_s ==="; cat "$L2/summary.tsv"
echo "=== load2 attribution ==="; cat "$L2/attr.txt"
echo; echo "=== load3 objects ==="; cat "$L3/objects.tsv"
echo; echo "=== load4 ==="; cat "$L4/summary.tsv"
echo; echo "=== load5 (s) ==="; echo "A1 cold-cache local (download+write): $(tr '\n' ' ' < "$L5/a1.txt")"
echo "A2 warm-cache local (--offline, writes only): $(tr '\n' ' ' < "$L5/a2.txt")"
echo "A3 warm-cache mount (--offline, writes only): $(tr '\n' ' ' < "$L5/a3.txt")"
echo; echo "=== load6 read passes ==="; for f in "$L6"/r_*.tsv; do echo "$(basename "$f"): $(tail -1 "$f")"; done
echo "=== load6 per-pass server GET/HEAD deltas ==="; grep 'load6 ' "$EXECLOG" | sed 's/^\[[0-9:]*\] //'
echo "=== load6 mount metrics (cache/s3 rows) ==="; cat "$L6/metrics.txt"
} > "$OUT/summary.txt" 2>&1
log "== W0.1 COMPLETE: $OUT/summary.txt =="
