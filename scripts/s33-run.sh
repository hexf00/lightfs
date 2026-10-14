#!/usr/bin/env bash
# s33-run.sh — S3.3 local fast validation (Mac, local MinIO, RPC-driven, no FUSE).
#
# Reuses only existing entry points:
#   - workload/verify tools are heredoc Python speaking the plain HTTP client
#     protocol (/data/write, /data/read, /rpc) — the same wire contract as
#     src/client.rs (no auth on the client->server hop; SigV4 is server->S3).
#   - RTT calibration uses lightfs-bench's raw-serial scenario (real SigV4
#     client) against MinIO directly vs through the delay proxy.
#   - server binary: target/release/lightfs (macOS build; FUSE never mounted).
#
# Discipline (S3.3 charter): isolated ports/buckets/dirs, PID-level lifecycle
# (no pkill), per-scenario wall caps (stage1 <=10s, others <=60s), measured vs
# simulated latency always labeled, production ECS untouched.
set -u
cd "$(dirname "$0")/.."
REPO="$PWD"
export PATH="$HOME/tools/bin:$HOME/.local/bin:$PATH"

BIN="$REPO/target/release/lightfs"
BENCH="$REPO/target/release/lightfs-bench"
[[ -x "$BIN" ]] || { echo "FATAL: $BIN missing (cargo build --release --bin lightfs)"; exit 1; }
[[ -x "$BENCH" ]] || { echo "FATAL: $BENCH missing (cargo build --release --bin lightfs-bench)"; exit 1; }
MINIO_BIN="${MINIO_BIN:-$HOME/tools/bin/minio}"
[[ -x "$MINIO_BIN" ]] || { echo "FATAL: minio missing at $MINIO_BIN (see docs/LOCAL_ENV.md)"; exit 1; }

RUN="$REPO/.s33-run-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$RUN/tools" "$RUN/out"
OUT="$RUN/out"; RESULTS="$OUT/RESULTS.md"
EXECLOG="$RUN/exec.log"

MINIO_PORT="${MINIO_PORT:-19003}"
PROXY_PORT="${PROXY_PORT:-19004}"
SRV_PORT="${SRV_PORT:-18096}"
RTT_MS="${RTT_MS:-19}"
N_SMOKE="${N_SMOKE:-100}"
N_RTT="${N_RTT:-1000}"
SIZE="${SIZE:-4096}"
THREADS="${THREADS:-16}"
BATCH_BYTES=$((8 * 1024 * 1024))
BATCH_WAIT_MS="${BATCH_WAIT_MS:-200}"
STAGES="${STAGES:-p1 calib rtt100 rtt1000 reads}"
VERIFY_SAMPLE="${VERIFY_SAMPLE:-100}"
CAP1=10
CAP2=60

MINIO_PID=""; PROXY_PID=""; SRV=""; GDIR=""; BUCKET=""; MANIFEST=""
WATCHERS=""; S3_ENDPOINT=""
# HARD WALL CAP: one script invocation must never run longer than this (user
# directive: no test script runs past 60s). Checked synchronously at every
# stage/group boundary; the exit fires the cleanup trap.
WALL_CAP="${WALL_CAP:-60}"
# proxy parameter that realizes the target RTT (calibration feedback loop);
# rtt100/rtt1000 read this cross-invocation file instead of the nominal value.
PROXY_PARAM_FILE="$REPO/.s33-proxy-param.txt"
DEADLINE=$(( $(date +%s) + WALL_CAP ))
wall_check(){
    (( $(date +%s) >= DEADLINE )) && {
        echo "FATAL: wall cap ${WALL_CAP}s exceeded for this invocation" >&2
        exit 124
    }
    return 0
}

log(){ echo "[$(date +%H:%M:%S)] $*"; }
res(){ echo "$*" >> "$RESULTS"; }
note(){ log "NOTE: $*"; }
fail(){ log "FATAL: $*"; exit 1; }

port_busy(){ lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | grep -q LISTEN; }

with_timeout(){  # $1=cap_s, rest=cmd; kills cmd on overrun -> rc=137
    local cap=$1; shift
    "$@" 2> "$RUN/.to.err" &
    local pid=$!
    ( sleep "$cap"; kill -9 "$pid" 2>/dev/null ) >/dev/null 2>&1 & WATCHERS="$WATCHERS $!"
    wait "$pid"; local rc=$?
    kill $WATCHERS 2>/dev/null; wait $WATCHERS 2>/dev/null; WATCHERS=""
    (( rc != 0 )) && sed 's/^/    /' "$RUN/.to.err" | tail -5 >&2
    return $rc
}

stop_srv(){ [[ -n "$SRV" ]] || return 0; kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""; }
stop_proxy(){ [[ -n "$PROXY_PID" ]] || return 0; kill -9 "$PROXY_PID" 2>/dev/null; wait "$PROXY_PID" 2>/dev/null; PROXY_PID=""; }
stop_minio(){ [[ -n "$MINIO_PID" ]] || return 0; kill -9 "$MINIO_PID" 2>/dev/null; wait "$MINIO_PID" 2>/dev/null; MINIO_PID=""; }
cleanup(){
    stop_srv; stop_proxy; stop_minio
    for w in $WATCHERS; do kill "$w" 2>/dev/null; done
}
trap cleanup EXIT

start_proxy(){  # $1 = target rtt ms (SIMULATED latency)
    stop_proxy
    if port_busy "$PROXY_PORT"; then
        lsof -nP -iTCP:"$PROXY_PORT" | tail -n +2 | sed 's/^/    holder: /' >&2
        fail "proxy port $PROXY_PORT busy"
    fi
    python3 "$REPO/scripts/s33-rtt-proxy.py" \
        --listen "127.0.0.1:$PROXY_PORT" \
        --target "127.0.0.1:$MINIO_PORT" \
        --rtt-ms "$1" --stats "$OUT/proxy-stats.txt" > "$OUT/proxy.log" 2>&1 &
    PROXY_PID=$!
    sleep 0.5
    kill -0 "$PROXY_PID" 2>/dev/null || { cat "$OUT/proxy.log"; fail "rtt-proxy died"; }
    log "rtt-proxy up pid=$PROXY_PID target_rtt_ms=$1 (SIMULATED)"
}

start_minio(){
    stop_minio
    port_busy "$MINIO_PORT" && fail "port $MINIO_PORT busy - refusing to share"
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
        nohup "$MINIO_BIN" server "$RUN/minio-data" \
        --address "127.0.0.1:$MINIO_PORT" --console-address "127.0.0.1:$((MINIO_PORT+9))" \
        > "$OUT/minio.log" 2>&1 &
    MINIO_PID=$!
    for _ in $(seq 1 100); do
        kill -0 "$MINIO_PID" 2>/dev/null || fail "minio died at startup"
        curl -s -m 1 -o /dev/null "http://127.0.0.1:$MINIO_PORT/minio/health/live" && break
        sleep 0.2
    done
    curl -s -m 1 -o /dev/null "http://127.0.0.1:$MINIO_PORT/minio/health/live" || fail "minio not up"
    sleep 1
}

start_server(){
    stop_srv
    ( cd "$GDIR" && LIGHTFS_METRICS=1 nohup "$BIN" server --config "$GDIR/cfg.toml" \
        > "$GDIR/server.log" 2>&1 & )
    SRV=$(pgrep -f "lightfs server --config $GDIR/cfg.toml" | head -1)
    [[ -n "$SRV" ]] || fail "server pid"
    for _ in $(seq 1 100); do
        curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" && return 0
        sleep 0.2
    done
    fail "server up ($GDIR)"
}

rpc_mkdir(){ curl -s -m 5 -X POST "http://127.0.0.1:$SRV_PORT/rpc" \
    -d "{\"id\":1,\"method\":\"mkdir\",\"params\":{\"path\":\"$1\",\"mode\":493}}" >/dev/null; }

start_stack(){  # $1 = group label; endpoint = $S3_ENDPOINT or direct MinIO
    local label=$1 endpoint="${S3_ENDPOINT:-http://127.0.0.1:$MINIO_PORT}"
    GDIR="$RUN/g-$label"; BUCKET="s33-$(echo "$label" | tr 'A-Z' 'a-z' | tr -d '.')"
    MANIFEST="$GDIR/manifest.tsv"
    mkdir -p "$GDIR"
    {
        cat <<EOF
mode = "server"
[s3]
endpoint = "$endpoint"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
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
        cfg_for "$label"
    } > "$GDIR/cfg.toml"
    "$BIN" create-bucket --config "$GDIR/cfg.toml" >>"$EXECLOG" 2>&1 || fail "create-bucket ($label)"
    start_server
    for d in seq mt conf crash hot big; do rpc_mkdir "/$d"; done
}
stop_stack(){ stop_srv; }

cfg_for(){  # $1 = A|B|C|QF|R1|R2|R3 (same shapes as s32-run.sh)
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

rpc_metrics(){ curl -s -m 2 -X POST "http://127.0.0.1:$SRV_PORT/rpc" -d '{"id":1,"method":"metrics","params":{}}'; }
metrics_snap(){
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
metrics_delta(){
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
        "bytes_written", "bytes_read", "wb_enqueued", "wb_drained",
        "wb_conflicts", "wb_packs", "wb_overflow", "metadata_tx", "conflicts"]
out = []
for k in keys:
    d = b.get(k, 0) - a.get(k, 0)
    if d: out.append("%s=%d" % (k.replace("s3.", ""), d))
print(" ".join(out) if out else "none")
PY
}
wb_counts(){
    rpc_metrics | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["result"]
    print("%s %s" % (r.get("wb_queued", -1), r.get("wb_draining", -1)))
except Exception: print("-1 -1")
'
}
wb_wait_drained(){  # $1 = timeout_s -> seconds until queued==0 && draining==0
    local t0=$(date +%s) deadline=$(( $(date +%s) + $1 ))
    while (( $(date +%s) < deadline )); do
        read -r q d <<< "$(wb_counts)"
        [[ "$q" == "0" && "$d" == "0" ]] && { echo $(( $(date +%s) - t0 )); return 0; }
        sleep 0.1
    done
    echo "-1"; return 1
}
percentiles(){
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

# queue-depth sampler while a scenario runs: peak_queued/peak_draining/overflow
QS_OUT=""; QS_DONE=""
qsample(){  # $1 = outfile
    QS_OUT="$1"; QS_DONE="$1.done"; rm -f "$QS_DONE"
    ( local peakq=0 peakd=0 ovf=0
      while [[ ! -f "$QS_DONE" ]] && kill -0 "$SRV" 2>/dev/null; do
        read -r q d ov <<< "$(wb_counts)"
        (( q > peakq )) 2>/dev/null && peakq=$q
        (( d > peakd )) 2>/dev/null && peakd=$d
        ovf=$ov
        sleep 0.1
      done
      echo "peak_queued=$peakq peak_draining=$peakd overflow=$ovf" > "$QS_OUT" ) >/dev/null 2>&1 & QSIG="$QSIG $!"
}
qstop(){ [[ -n "$QS_DONE" ]] && touch "$QS_DONE"; for s in $QSIG; do wait "$s" 2>/dev/null; done; QSIG=""; }
QSIG=""

# macOS resource sampling: ps cputime + rss
S_SIG=""
sample_to(){
    ( while kill -0 "$1" 2>/dev/null; do
        ps -p "$1" -o cputime=,rss= 2>/dev/null >> "$2"
        sleep 0.2
      done ) >/dev/null 2>&1 & S_SIG="$S_SIG $!"
}
sample_kill(){ for s in $S_SIG; do kill "$s" 2>/dev/null; wait "$s" 2>/dev/null; done; S_SIG=""; }
summarize_sample(){
    python3 - "$1" <<'PY'
import sys
def secs(t):
    if not t or t == "0" or t == "0:00.00": return None
    t = t.replace(",", ".")
    parts = t.split(":")
    try: return sum(float(x) * 60 ** i for i, x in enumerate(reversed(parts)))
    except ValueError: return None
cpu = []; peak = 0
try:
    for line in open(sys.argv[1]):
        parts = line.split()
        if len(parts) < 2: continue
        s = secs(parts[0])
        if s is not None: cpu.append(s)
        try: peak = max(peak, int(parts[1]))
        except ValueError: pass
except FileNotFoundError:
    pass
if not cpu:
    print("cpu_s=0.00 peak_rss_mb=0.0"); raise SystemExit
print("cpu_s=%.2f peak_rss_mb=%.1f" % (max(cpu) - min(cpu), peak / 1024.0))
PY
}

##############################################################################
# workload tools (plain-HTTP client protocol; same wire contract as client.rs)
##############################################################################
cat > "$RUN/tools/w33.py" <<'PY'
# RPC-driven writers: POST /data/write per file (create+commit in one request,
# the same wire contract as Client::write_full). TSV: ms<TAB>i;
# manifest: tag<TAB>i<TAB>ms<TAB>sha ; stdout wall seconds.
import concurrent.futures, hashlib, json, sys, time, urllib.request

srv, d, count, size, threads, tag, manifest, out = sys.argv[1:9]
count, size, threads = int(count), int(size), int(threads)
per, rem = count // threads, count % threads
mf = open(manifest, "a")
lat = []
t0 = time.perf_counter()

def write_one(i):
    data = hashlib.sha256(f"{tag}:{i}".encode()).digest() * (size // 32)
    path = f"{d}/f{i}.bin"
    req = urllib.request.Request(
        f"{srv}/data/write", data=data, method="POST",
        headers={"X-LightFS-Path": path, "X-LightFS-Create": "1",
                 "X-LightFS-Length": str(len(data))})
    s = time.perf_counter()
    with urllib.request.urlopen(req, timeout=120) as r:
        node = json.loads(r.read())
    ms = (time.perf_counter() - s) * 1000.0
    assert node.get("version", 0) >= 1, f"no version for {path}"
    return ms, hashlib.sha256(data).hexdigest(), i

def work(n, base):
    return [write_one(base + k) for k in range(n)]

with concurrent.futures.ThreadPoolExecutor(threads) as ex:
    futs = []
    for t in range(threads):
        n = per + (1 if t < rem else 0)
        base = sum(per + (1 if j < rem else 0) for j in range(t))
        futs.append(ex.submit(work, n, base))
    for f in futs:
        lat.extend(f.result())   # propagate the first writer error
wall = time.perf_counter() - t0
for ms, sha, i in sorted(lat, key=lambda r: r[2]):
    mf.write(f"{tag}\t{i}\t{ms:.3f}\t{sha}\n")
    with open(out, "a") as f:
        f.write(f"{ms:.3f}\t{i}\n")
mf.flush()
print(f"{wall:.3f}")
PY

cat > "$RUN/tools/r33.py" <<'PY'
# RPC-driven verify-read over a manifest slice, following the FUSE read path:
# lookup(path) for the node id (outside the timed window, as GETATTR is cached
# by the kernel dentry cache), then POST /data/read {"node_id",...} — the same
# call the Q memory cache and the pack cache are keyed on. sha-verify per file.
# TSV: ms<TAB>i. BAD lines to stderr; exit 2 on mismatch.
import hashlib, json, sys, time, urllib.request

srv, root, manifest, prefix, start, count, size, out = sys.argv[1:9]
start, count, size = int(start), int(count), int(size)
sel = [l.rstrip("\n").split("\t") for l in open(manifest)
       if l.split("\t")[0].startswith(prefix)]
sel = sel[start:start + count] if count >= 0 else sel[start:]
if not sel:
    print("BAD empty manifest selection (nothing to verify)", file=sys.stderr)
    sys.exit(2)
rows = []; ok = bad = 0; bytes_read = 0
for tag, i, _, want in sel:
    path = f"{root}/f{i}.bin"
    body = json.dumps({"id": 1, "method": "lookup",
                       "params": {"path": path}}).encode()
    req = urllib.request.Request(f"{srv}/rpc", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=120) as r:
        node = json.loads(r.read())["result"]
    body = json.dumps({"node_id": node["id"], "offset": 0,
                       "size": size}).encode()
    req = urllib.request.Request(f"{srv}/data/read", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    s = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
    except Exception as e:
        print(f"BAD missing {path}: {e}", file=sys.stderr); bad += 1; continue
    ms = (time.perf_counter() - s) * 1000.0
    bytes_read += len(data)
    rows.append((ms, i))
    if hashlib.sha256(data).hexdigest() != want:
        print(f"BAD sha {path}", file=sys.stderr); bad += 1
    else:
        ok += 1
with open(out, "a") as f:
    for ms, i in rows:
        f.write(f"{ms:.3f}\t{i}\n")
sys.stderr.write(f"ok={ok} bad={bad} bytes_read={bytes_read}\n")
sys.exit(2 if bad else 0)
PY

cat > "$RUN/tools/conf33.py" <<'PY'
# Conflict probe: two concurrent writers over the same 10 files (last-commit-
# wins at the RPC layer), then one deliberate stale-expected CAS write.
# mode=strict: the stale write MUST be rejected synchronously (CAS gate).
# mode=wb:     the stale write is accepted at close (staged); the CAS loss
#              becomes a dead letter at drain (counted via wb_conflicts) and
#              the harness checks that counter separately.
import concurrent.futures, hashlib, json, sys, urllib.request, urllib.error

srv = sys.argv[1]; d = sys.argv[2]; mode = sys.argv[3]
def put(path, data, expected=None):
    h = {"X-LightFS-Path": path, "X-LightFS-Create": "1",
         "X-LightFS-Length": str(len(data))}
    if expected is not None:
        h["X-LightFS-Expected-Version"] = str(expected)
    req = urllib.request.Request(f"{srv}/data/write", data=data, method="POST",
                                 headers=h)
    with urllib.request.urlopen(req, timeout=120) as r:
        return json.loads(r.read())

def writer(tag):
    errs = 0
    for i in range(10):
        try:
            put(f"{d}/c{i}.bin", (tag.encode() * 256)[:4096])
        except (urllib.error.HTTPError, Exception):
            errs += 1
    return errs

with concurrent.futures.ThreadPoolExecutor(2) as ex:
    a, b = ex.submit(writer, "one"), ex.submit(writer, "two")
    errs = a.result() + b.result()

rejected = 0; stale_accepted = 0
try:
    put(f"{d}/c0.bin", b"stale-writer-must-lose" * 16, expected=0)
    stale_accepted = 1
    if mode == "strict":
        print("STALE WRITE SUCCEEDED - CAS gate broken", file=sys.stderr)
        sys.exit(2)
except urllib.error.HTTPError:
    rejected = 1

if mode == "wb":
    # WB ack-projection: pending staged content is readable before the drain
    # (close=staged). Wait for the queue to go idle so the final read observes
    # the COMMITTED terminal state, not a projection.
    import time as _t
    deadline = _t.time() + 15
    while _t.time() < deadline:
        body = json.dumps({"id": 1, "method": "metrics", "params": {}}).encode()
        req = urllib.request.Request(f"{srv}/rpc", data=body, method="POST",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            m = json.loads(r.read())["result"]
        if m.get("wb_queued", 0) == 0 and m.get("wb_draining", 0) == 0:
            break
        _t.sleep(0.1)

ok = True
h1 = hashlib.sha256((b"one" * 256)[:4096]).hexdigest()
h2 = hashlib.sha256((b"two" * 256)[:4096]).hexdigest()
body = json.dumps({"path": f"{d}/c0.bin", "offset": 0, "size": 4096}).encode()
req = urllib.request.Request(f"{srv}/data/read", data=body, method="POST",
                             headers={"Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=120) as r:
    got = hashlib.sha256(r.read()).hexdigest()
if got not in (h1, h2):
    ok = False
print(f"errors={errs} stale_rejected={rejected} stale_accepted={stale_accepted} "
      f"untorn={str(ok).lower()}")
sys.exit(0 if (ok and (rejected or stale_accepted)) else 2)
PY

cat > "$RUN/tools/crashw33.py" <<'PY'
# Crash writer: acked lines are flushed+fsynced to the manifest AFTER the
# server accepted the write; exits when the server connection breaks.
import hashlib, json, os, sys, urllib.request

srv, count, size, tag, manifest = sys.argv[1:6]
count, size = int(count), int(size)
mf = open(manifest, "a")
done = 0
for i in range(count):
    data = hashlib.sha256(f"{tag}:{i}".encode()).digest() * (size // 32)
    path = f"/crash/f{i}.bin"
    req = urllib.request.Request(
        f"{srv}/data/write", data=data, method="POST",
        headers={"X-LightFS-Path": path, "X-LightFS-Create": "1",
                 "X-LightFS-Length": str(len(data))})
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            json.loads(r.read())
    except Exception:
        break
    mf.write(f"{tag}\t{i}\t0.000\t{hashlib.sha256(data).hexdigest()}\n")
    mf.flush(); os.fsync(mf.fileno())
    done += 1
mf.close()
print(done)
PY

cat > "$RUN/tools/verify33.py" <<'PY'
# Crash contract: every acked manifest row must read back byte-identical.
import hashlib, json, sys, urllib.request

srv, manifest, tag, size = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4])
ok = bad = missing = 0
found = False
for line in open(manifest):
    t, i, _, want = line.rstrip("\n").split("\t")
    if t != tag: continue
    found = True
    path = f"/{tag}/f{i}.bin"
    body = json.dumps({"path": path, "offset": 0, "size": size}).encode()
    req = urllib.request.Request(f"{srv}/data/read", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=120) as r:
            data = r.read()
    except Exception:
        missing += 1; continue
    if hashlib.sha256(data).hexdigest() == want: ok += 1
    else: bad += 1
print(f"acked_ok={ok} bad={bad} missing={missing}")
sys.exit(2 if (bad or missing or not found) else 0)
PY

##############################################################################
# one write-group pass (A/B/C); endpoint = $S3_ENDPOINT (proxy or direct)
##############################################################################
run_group(){  # $1=group $2=count $3=label $4=cap_s
    local g=$1 n=$2 rl=$3 cap=$4
    wall_check
    start_stack "$g"
    wall_check
    local W="$OUT/w-$g-$rl"; mkdir -p "$W"
    local wall t_drain rc

    : > "$W/seq.tsv"; : > "$W/mt.tsv"
    sample_to "$SRV" "$W/srv-seq.samples"
    log "  $g/$rl: seq $n x ${SIZE}B (cap ${cap}s) endpoint=${S3_ENDPOINT:-direct}"
    metrics_snap "$W/bef.txt"
    qsample "$W/qpeak"
    wall=$(with_timeout "$cap" python3 "$RUN/tools/w33.py" \
        "http://127.0.0.1:$SRV_PORT" "/seq" "$n" "$SIZE" 1 seq "$MANIFEST" "$W/seq.tsv")
    rc=$?
    t_drain=$(wb_wait_drained "$cap" || true)
    qstop
    sample_kill
    metrics_snap "$W/aft.txt"
    [[ $rc == 0 ]] || fail "seq write $g/$rl rc=$rc (scenario exceeded cap ${cap}s?)"
    [[ "$t_drain" != "-1" ]] || fail "drain timeout $g/$rl"
    res "PHASE=seq.$g.$rl endpoint=${S3_ENDPOINT:-direct} delta: $(metrics_delta "$W/bef.txt" "$W/aft.txt") wall_s=$wall"
    res "FRONT=seq group=$g round=$rl wall_s=$wall drain_to_remote_s=$t_drain close_$(percentiles "$W/seq.tsv") $(cat "$W/qpeak")"
    # verify over a sample under simulated RTT (full-N reads would double the
    # scenario cost); full N on direct loopback.
    local vn=$n
    [[ -n "$S3_ENDPOINT" ]] && vn=$(( n > VERIFY_SAMPLE ? VERIFY_SAMPLE : n ))
    metrics_snap "$W/bef2.txt"
    python3 "$RUN/tools/r33.py" "http://127.0.0.1:$SRV_PORT" "/seq" "$MANIFEST" seq 0 "$vn" "$SIZE" "$W/seqr.tsv" 2>"$W/seqr.err" \
        || fail "seq verify $g/$rl: $(cat "$W/seqr.err")"
    metrics_snap "$W/aft2.txt"
    res "VERIFY=seq.$g.$rl n=$vn delta: $(metrics_delta "$W/bef2.txt" "$W/aft2.txt")"

    log "  $g/$rl: mt ${THREADS}t"
    sample_to "$SRV" "$W/srv-mt.samples"
    metrics_snap "$W/bef-mt.txt"
    wall=$(with_timeout "$cap" python3 "$RUN/tools/w33.py" \
        "http://127.0.0.1:$SRV_PORT" "/mt" "$n" "$SIZE" "$THREADS" mt "$MANIFEST" "$W/mt.tsv") \
        || fail "mt write $g/$rl"
    t_drain=$(wb_wait_drained "$cap" || true)
    sample_kill
    metrics_snap "$W/aft-mt.txt"
    [[ "$t_drain" != "-1" ]] || fail "drain timeout mt $g/$rl"
    res "PHASE=mt.$g.$rl endpoint=${S3_ENDPOINT:-direct} delta: $(metrics_delta "$W/bef-mt.txt" "$W/aft-mt.txt") wall_s=$wall"
    local vn=$n
    [[ -n "$S3_ENDPOINT" ]] && vn=$(( n > VERIFY_SAMPLE ? VERIFY_SAMPLE : n ))
    python3 "$RUN/tools/r33.py" "http://127.0.0.1:$SRV_PORT" "/mt" "$MANIFEST" mt 0 "$vn" "$SIZE" "$W/mtr.tsv" 2>"$W/mtr.err" \
        || fail "mt verify $g/$rl: $(cat "$W/mtr.err")"
    res "VERIFY=mt.$g.$rl n=$vn"
    res "FRONT=mt group=$g round=$rl wall_s=$wall drain_to_remote_s=$t_drain close_$(percentiles "$W/mt.tsv")"
    res "RSS group=$g round=$rl seq_srv_$(summarize_sample "$W/srv-seq.samples") mt_srv_$(summarize_sample "$W/srv-mt.samples")"
    stop_stack
}

##############################################################################
# main
##############################################################################
: > "$RESULTS"
res "# S3.3 local fast-validation results — $(date -u +%FT%TZ)"
res "# Host: local Mac, MinIO $MINIO_BIN (loopback), server target/release/lightfs."
res "# Workloads are RPC-driven plain HTTP (/data/write,/data/read): NO FUSE layer,"
res "# identical for A/B/C so group comparisons stay fair."
res "# SIMULATED = server's S3 endpoint goes through the userspace delay proxy"
res "# (scripts/s33-rtt-proxy.py, target ${RTT_MS}ms RTT, calibrated in the CALIB stage);"
res "# direct = plain loopback. drain_to_remote_s = wall until the write-back queue"
res "# is idle (queued==0 && draining==0); reported separately from FRONT."
res "#"
res "# READS scope note (structural, not a failure): the Q per-file memory cache"
res "# (R2) is installed in the MOUNT process (RoutingFs, cmd.rs), so plain-RPC"
res "# reads never traverse it; locally R2 therefore measures the same path as R1."
res "# R2's true client-cached numbers come from S3.2's ECS FUSE run (hot 0 GET)."
res "# R3's pack cache lives on the ObjectStore layer and DOES serve plain RPC"
res "# reads, which is itself a measured difference between the two caches."

# ---------- stage p1: local smoke (direct loopback) ----------
if [[ " $STAGES " == *" p1 "* ]]; then
wall_check
log "== P1: local smoke $N_SMOKE files x (A B C) + conflict + crash =="
start_minio
for g in A B C; do
    run_group "$g" "$N_SMOKE" p1 "$CAP1"
done

log "  conflict probes (stale-expected CAS on A/strict; dual writer on B/WB)"
start_stack A
conf=$(with_timeout "$CAP1" python3 "$RUN/tools/conf33.py" "http://127.0.0.1:$SRV_PORT" "/conf" strict) \
    || fail "conflict probe (strict): $conf"
[[ "$conf" == *stale_rejected=1* && "$conf" == *untorn=true* ]] || fail "conflict contract A: $conf"
res "CONFLICT A/p1 direct $conf (strict CAS rejected synchronously)"
stop_stack
start_stack B
conf=$(with_timeout "$CAP1" python3 "$RUN/tools/conf33.py" "http://127.0.0.1:$SRV_PORT" "/conf" wb) \
    || fail "conflict probe (wb): $conf"
t_drain=$(wb_wait_drained 60 || true)
wbconf=$(rpc_metrics | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["result"]
    print("%s %s" % (r.get("wb_conflicts", -1), r.get("wb_dead", -1)))
except Exception: print("-1 -1")
')
res "CONFLICT B/p1 direct $conf wb_conflicts_dead=$wbconf drain_s=$t_drain"
[[ "$conf" == *untorn=true* ]] || fail "conflict contract B: $conf"
stop_stack

log "  crash probe (kill -9 mid-write on C, restart, verify acked)"
start_stack C
python3 "$RUN/tools/crashw33.py" "http://127.0.0.1:$SRV_PORT" 400 "$SIZE" crash "$MANIFEST" \
    > "$OUT/cw.out" 2>/dev/null &
CW=$!
sleep 1.0
kill -9 "$SRV" 2>/dev/null; wait "$SRV" 2>/dev/null; SRV=""
wait "$CW" 2>/dev/null
start_server
t_drain=$(wb_wait_drained 60 || true)
verdict=$(with_timeout "$CAP1" python3 "$RUN/tools/verify33.py" "http://127.0.0.1:$SRV_PORT" "$MANIFEST" crash "$SIZE")
rc=$?
res "CRASH C/p1 direct $verdict recovery_drain_s=$t_drain"
[[ $rc == 0 ]] || fail "crash contract: $verdict"
stop_stack
stop_minio
log "== P1 done =="
fi

# ---------- stage calib: proxy delay calibration via lightfs-bench ----------
if [[ " $STAGES " == *" calib "* ]]; then
wall_check
log "== CALIB: bench raw-serial PUT direct vs proxy (target +${RTT_MS}ms RTT) =="
start_minio
"$BIN" create-bucket --config <(cat <<EOF
mode = "server"
[s3]
endpoint = "http://127.0.0.1:$MINIO_PORT"
region = "us-east-1"
bucket = "s33-calib"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
service = "s3"
[metadata]
path = "$RUN/calib-meta.db"
[server]
bind = "127.0.0.1:$SRV_PORT"
EOF
) >>"$EXECLOG" 2>&1 || note "calib bucket (may exist)"

bench_p50(){  # $1 = s3 endpoint; prints raw-serial GET p50 (ms)
    "$BENCH" --s3-endpoint "$1" --bucket s33-calib \
        --objects 30 --obj-size 4096 --large-mb 1 --scenario prepare,raw-serial \
        > "$2" 2>>"$EXECLOG" || fail "bench prepare,raw-serial ($1)"
    python3 - "$2" <<'PY'
import re, sys
txt = open(sys.argv[1]).read()
line = [l for l in txt.splitlines() if l.startswith("raw-s3 serial")]
if not line:
    print("PARSE-FAIL", file=sys.stderr); print(txt[-800:], file=sys.stderr); raise SystemExit(1)
m = re.search(r"p50\s+([0-9.]+)", line[-1])
print(m.group(1) if m else "PARSE-FAIL")
PY
}
direct_p50=$(bench_p50 "http://127.0.0.1:$MINIO_PORT" "$OUT/bench-direct.txt")
# calibration feedback loop: the proxy parameter is ADJUSTED until the
# MEASURED added round trip matches the target (the raw sleep model lands
# a few ms high on this host; the measured value is what counts).
rtt_param="$RTT_MS"
calib_line=""
for attempt in 1 2 3; do
    start_proxy "$rtt_param"
    proxy_p50=$(bench_p50 "http://127.0.0.1:$PROXY_PORT" "$OUT/bench-proxy-$attempt.txt")
    stop_proxy
    delta=$(python3 -c "print(f'{$proxy_p50-$direct_p50:.2f}')")
    ok=$(python3 -c "print(1 if abs($delta-$RTT_MS) <= max(2.0, $RTT_MS*0.15) else 0)")
    calib_line="CALIB attempt=$attempt param=${rtt_param}ms direct_get_p50=${direct_p50}ms proxy_get_p50=${proxy_p50}ms delta=${delta}ms target=${RTT_MS}ms verdict=$( [[ "$ok" == 1 ]] && echo MATCH || echo adjust )"
    log "$calib_line"
    [[ "$ok" == 1 ]] && break
    rtt_param=$(python3 -c "print(max(1.0, round($RTT_MS * $RTT_MS / $delta, 1)))")
done
[[ "$ok" == 1 ]] || fail "calibration failed to converge (last: $calib_line)"
echo "$rtt_param" > "$PROXY_PARAM_FILE"
res "$calib_line (SIMULATED delay calibration; lightfs-bench prepare,raw-serial 30 SigV4 GETs; proxy parameter locked for rtt stages)"
stop_minio
fi

# ---------- stage rtt100: 100 files through the proxy (SigV4 compat) ----------
if [[ " $STAGES " == *" rtt100 "* ]]; then
[[ -f "$PROXY_PARAM_FILE" ]] || fail "rtt100 needs the calib stage first (no $PROXY_PARAM_FILE)"
RTT_PARAM=$(cat "$PROXY_PARAM_FILE")
wall_check
log "== RTT100: $N_SMOKE files x (A B C) through proxy (SIMULATED, param ${RTT_PARAM}ms -> calibrated ~${RTT_MS}ms RTT) =="
start_minio
start_proxy "$RTT_PARAM"
S3_ENDPOINT="http://127.0.0.1:$PROXY_PORT"
for g in A B C; do
    run_group "$g" "$N_SMOKE" rtt100 30
done
S3_ENDPOINT=""
stop_proxy
stop_minio
log "== RTT100 done =="
fi

# ---------- stage rtt1000: the recorded experiment ----------
if [[ " $STAGES " == *" rtt1000 "* ]]; then
[[ -f "$PROXY_PARAM_FILE" ]] || fail "rtt1000 needs the calib stage first (no $PROXY_PARAM_FILE)"
RTT_PARAM=$(cat "$PROXY_PARAM_FILE")
wall_check
log "== RTT1000: $N_RTT files x (A B C) through proxy (SIMULATED, param ${RTT_PARAM}ms -> calibrated ~${RTT_MS}ms RTT) =="
start_minio
start_proxy "$RTT_PARAM"
S3_ENDPOINT="http://127.0.0.1:$PROXY_PORT"
for g in ${RTT_GROUPS:-A B C}; do
    run_group "$g" "$N_RTT" rtt1000 "$CAP2"
done
S3_ENDPOINT=""
stop_proxy
stop_minio
log "== RTT1000 done =="
fi

# ---------- stage reads: R1/R2/R3 fairness (direct loopback) ----------
if [[ " $STAGES " == *" reads "* ]]; then
wall_check
log "== READS: R1/R2/R3 $N_RTT files x 8MiB budget (direct) =="
start_minio
read_group(){  # $1 = R1|R2|R3
    local g=$1
    wall_check
    start_stack "$g"
    local W="$OUT/read-$g"; mkdir -p "$W"
    # dataset: 300-file hot set + $N_RTT-file scan set, written under the
    # group's own config (R3 = WB batch -> pack objects)
    python3 "$RUN/tools/w33.py" "http://127.0.0.1:$SRV_PORT" "/hot" 300 "$SIZE" 1 hot "$MANIFEST" "$W/w1.tsv" >/dev/null \
        || fail "read $g: hot write"
    python3 "$RUN/tools/w33.py" "http://127.0.0.1:$SRV_PORT" "/big" "$N_RTT" "$SIZE" 1 big "$MANIFEST" "$W/w2.tsv" >/dev/null \
        || fail "read $g: big write"
    if [[ "$g" == R3 ]]; then wb_wait_drained 300 >/dev/null || fail "read $g drain"; fi
    local ph
    for ph in cold hot1 hot2 scan1 scan2; do
        local prefix cnt
        case "$ph" in cold|hot1|hot2) prefix=hot; cnt=300 ;; *) prefix=big; cnt="$N_RTT" ;; esac
        metrics_snap "$W/bef.txt"
        python3 "$RUN/tools/r33.py" "http://127.0.0.1:$SRV_PORT" "/$prefix" "$MANIFEST" "$prefix" 0 "$cnt" "$SIZE" "$W/$ph.tsv" 2>"$W/$ph.err" \
            || fail "read $g/$ph: $(cat "$W/$ph.err")"
        metrics_snap "$W/aft.txt"
        res "READ=$g phase=$ph direct delta: $(metrics_delta "$W/bef.txt" "$W/aft.txt") p95_$(percentiles "$W/$ph.tsv")"
    done
    { curl -s -m 2 "http://127.0.0.1:$SRV_PORT/metrics" | awk 'NR>1 && $1 ~ /cache/ {print $1"="$2}'
      rpc_metrics | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["result"]
    for k in ("pack_cache_hits", "pack_cache_misses", "pack_bytes_loaded"):
        print("%s=%s" % (k, r.get(k, 0)))
except Exception: pass'; } > "$W/cache.txt"
    res "READCACHE=$g $(tr '\n' ' ' < "$W/cache.txt")"
    stop_stack
}
for g in ${READS_GROUPS:-R1 R2 R3}; do read_group "$g"; done
stop_minio
log "== READS done =="
fi

log "ALL STAGES DONE -> $RESULTS"
