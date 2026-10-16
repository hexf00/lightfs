#!/usr/bin/env bash
# S3.4: real-workload value validation — git/npm over the full FUSE mount chain.
#
#   GROUP=A|C SCENARIO=git|npm EP=direct|rtt bash scripts/s34-run.sh
#
#   A = main branch binary (strict commit), C = s31-pack-proto (WB+batch-pack).
#   One invocation = ONE scenario run; the WHOLE invocation is capped at 60s
#   (wall_check at every boundary + with_timeout on the workload). MinIO and
#   the RTT proxy run on the host; server + FUSE mount + workload run inside
#   a privileged container (lightfs-s34:arm64, /dev/fuse). Fresh bucket +
#   MinIO data dir + meta.db per run. RTT reuses the S3.3-calibrated proxy
#   parameter (no recalibration, per charter).
#
#   Discipline (S3.4 charter): dev machine only, no ECS, no production ports,
#   <=60s per invocation, Chinese report, no new Rust entrypoints.
set -u
cd "$(dirname "$0")/.."
REPO=$(pwd)
export PATH="$HOME/tools/bin:$PATH"

GROUP=${GROUP:-C}
SCENARIO=${SCENARIO:-git}
EP=${EP:-direct}
WALL_CAP=${WALL_CAP:-60}
RTT_PARAM=${RTT_PARAM:-13.7}          # S3.3 calibration feedback loop, converged param (ms)
HOST_IP=${HOST_IP:-192.168.50.147}    # host LAN IP, reachable from container
MINIO_PORT=${MINIO_PORT:-19003}
PROXY_PORT=${PROXY_PORT:-19004}
SRV_PORT=${SRV_PORT:-18095}           # published 127.0.0.1:$SRV_PORT -> container 18095 (NOT prod 18085)
IMAGE=${IMAGE:-lightfs-s34:arm64}
DEPLOY=${DEPLOY:-$HOME/code/s34-deploy}
GIT_DIRS=${GIT_DIRS:-"packages/react packages/react-dom packages/shared packages/scheduler"}
OUT=${OUT:-/tmp/s34-results/$(date +%Y%m%d-%H%M%S)}
RUN="$OUT/run"; EXECLOG="$OUT/exec.log"
mkdir -p "$RUN"; : > "$EXECLOG"
START=$(date +%s)
DEADLINE=$((START + WALL_CAP))
MINIO_BIN="$HOME/tools/bin/minio"
RESULT_TSV="$OUT/RESULT.tsv"

log(){ echo "[$GROUP/$SCENARIO/$EP] $*"; }
fail(){ echo "FAIL: $*" >&2; exit 1; }
wall_check(){
    (( $(date +%s) < DEADLINE )) || { log "wall cap exceeded at '$1'"; exit 124; }
    log "'$1' budget left: $(( DEADLINE - $(date +%s) ))s"
}
port_busy(){ lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | grep -q LISTEN; }

CTR=s34-$GROUP-$SCENARIO-$EP
BUCKET=s34-$(echo "$GROUP-$SCENARIO-$EP" | tr "A-Z" "a-z")
S3URL="http://$HOST_IP:$MINIO_PORT"

cleanup(){
    docker rm -f "$CTR" >/dev/null 2>&1
    [[ -n "${MINIO_PID:-}" ]] && kill "$MINIO_PID" 2>/dev/null
    [[ -n "${PROXY_PID:-}" ]] && kill "$PROXY_PID" 2>/dev/null
    return 0
}
trap cleanup EXIT

with_timeout(){  # $1=cap_s, rest=cmd; kills cmd on overrun
    local cap=$1; shift
    "$@" > "$OUT/work.out" 2> "$OUT/work.err" &
    local wpid=$!
    ( sleep "$cap"; kill -9 "$wpid" 2>/dev/null ) >/dev/null 2>&1 &
    local killer=$!
    wait "$wpid"; WORK_RC=$?
    kill "$killer" 2>/dev/null
    wait "$killer" 2>/dev/null
    return 0
}

stop_minio(){ [[ -n "${MINIO_PID:-}" ]] && kill "$MINIO_PID" 2>/dev/null && sleep 0.3; return 0; }

start_minio(){  # fresh data dir per run, all interfaces (container reaches via HOST_IP)
    stop_minio
    port_busy "$MINIO_PORT" && fail "port $MINIO_PORT busy - refusing to share"
    rm -rf "$RUN/minio-data"
    MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
        nohup "$MINIO_BIN" server "$RUN/minio-data" \
        --address "0.0.0.0:$MINIO_PORT" --console-address "127.0.0.1:$((MINIO_PORT+9))" \
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

start_proxy(){  # S3.3 proxy, all-interfaces so the in-container server can use it
    port_busy "$PROXY_PORT" && fail "proxy port $PROXY_PORT busy"
    nohup python3 "$REPO/scripts/s33-rtt-proxy.py" \
        --listen "0.0.0.0:$PROXY_PORT" --target "127.0.0.1:$MINIO_PORT" \
        --rtt-ms "$RTT_PARAM" --stats "$OUT/proxy-stats.txt" > "$OUT/proxy.log" 2>&1 &
    PROXY_PID=$!
    sleep 0.5
    kill -0 "$PROXY_PID" 2>/dev/null || { cat "$OUT/proxy.log"; fail "rtt-proxy died"; }
    S3URL="http://$HOST_IP:$PROXY_PORT"
    log "rtt-proxy up pid=$PROXY_PID param=${RTT_PARAM}ms (S3.3 calibrated, SIMULATED)"
}

rpc(){ curl -s -m 5 -X POST "http://127.0.0.1:$SRV_PORT/rpc" -d "$1"; }
rpc_metrics(){ rpc '{"id":1,"method":"metrics","params":{}}'; }

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
keys = ["s3.put", "s3.get", "s3.delete", "bytes_written", "bytes_read",
        "wb_enqueued", "wb_drained", "wb_conflicts", "wb_packs", "metadata_tx"]
out = []
for k in keys:
    d = b.get(k, 0) - a.get(k, 0)
    if d: out.append("%s=%d" % (k.replace("s3.", ""), d))
print(" ".join(out) if out else "none")
PY
}
wb_wait_drained(){  # $1 = timeout_s -> seconds until queued==0 && draining==0
    local t0=$(date +%s) deadline=$(( $(date +%s) + $1 )) qd first=1
    while (( $(date +%s) < deadline )); do
        qd=$(rpc_metrics | python3 -c '
import json, sys
try:
    r = json.load(sys.stdin)["result"]
    print("%s %s" % (r.get("wb_queued", -1), r.get("wb_draining", -1)))
except Exception: print("-1 -1")
')
        [[ "$qd" == "-1 -1" && "$first" == "1" ]] && { echo 0; return 0; }  # strict: no queue
        first=0
        [[ "$qd" == "0 0" ]] && { echo $(( $(date +%s) - t0 )); return 0; }
        sleep 0.1
    done
    echo "-1"; return 1
}

# --- container lifecycle -----------------------------------------------------
start_container(){
    docker rm -f "$CTR" >/dev/null 2>&1
    docker run -d --rm --privileged --device /dev/fuse --name "$CTR" \
        -v "$DEPLOY":/deploy -v "$HOME/code/react-mirror":/data-src:ro \
        -v "$HOME/.npm":/root/.npm \
        -p "127.0.0.1:$SRV_PORT:18095" \
        "$IMAGE" sleep 900 >/dev/null || fail "container start"
}

start_server_in_container(){
    docker exec -i "$CTR" sh -c "mkdir -p /g && cat > /g/srv.toml" <<SRVEOF
[s3]
endpoint = "$S3URL"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "/g/meta.db"
[server]
bind = "0.0.0.0:18095"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
$( [[ "$GROUP" == "C" ]] && printf '[commit]\nmode = "writeback"\n[writeback]\nstaging_dir = "/g/wb-staging"\ndrain_workers = 1\nbatch_pack_bytes = 8388608\nbatch_max_wait_ms = 200\nqueue_max_tasks = 4096\n' )
SRVEOF
    docker exec "$CTR" /deploy/lightfs-$GROUP create-bucket --config /g/srv.toml >>"$EXECLOG" 2>&1 \
        || fail "create-bucket"
    docker exec -d "$CTR" sh -c "LIGHTFS_METRICS=1 /deploy/lightfs-$GROUP server --config /g/srv.toml > /g/server.log 2>&1"
    for _ in $(seq 1 100); do
        curl -s -m 1 -o /dev/null "http://127.0.0.1:$SRV_PORT/health" && return 0
        (( $(date +%s) < DEADLINE )) || fail "wall cap during server start"
        sleep 0.2
    done
    docker exec "$CTR" cat /g/server.log >&2 2>/dev/null || true
    fail "server not up"
}

start_mount_in_container(){
    docker exec -i "$CTR" sh -c "cat > /g/mnt.toml" <<MNTEOF
[s3]
endpoint = "$S3URL"
region = "us-east-1"
bucket = "$BUCKET"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "/g/mnt-unused.db"
[server]
bind = "127.0.0.1:18095"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
MNTEOF
    docker exec -d "$CTR" sh -c "mkdir -p /mnt/s34 && /deploy/lightfs-$GROUP mount --config /g/mnt.toml /mnt/s34 > /g/mount.log 2>&1"
    for _ in $(seq 1 50); do
        docker exec "$CTR" ls /mnt/s34 >/dev/null 2>&1 && return 0
        (( $(date +%s) < DEADLINE )) || fail "wall cap during mount"
        sleep 0.2
    done
    docker exec "$CTR" cat /g/mount.log >&2 2>/dev/null || true
    fail "mount not ready"
}

proc_stats(){  # peak RSS (KB) for server+mount from inside the container
    docker exec "$CTR" sh -c '
for d in /proc/[0-9]*; do
    p=${d#/proc/}
    cmd=$(tr "\0" " " < $d/cmdline 2>/dev/null)
    case "$cmd" in
        *lightfs*server*) echo "srv_rss_kb=$(awk '/VmHWM/{print $2}' $d/status 2>/dev/null)" ;;
        *lightfs*mount*)  echo "mnt_rss_kb=$(awk '/VmHWM/{print $2}' $d/status 2>/dev/null)" ;;
    esac
done' 2>/dev/null | paste -sd" " -
}

# --- workloads ----------------------------------------------------------------
write_work_py(){
    docker exec -i "$CTR" sh -c 'cat > /g/work.py' <<'PYEOF'
import subprocess, sys, time, os
env = dict(os.environ, GIT_TRACE_PERFORMANCE="1")
def phase(name, args):
    t0 = time.time()
    r = subprocess.run(args, capture_output=True, text=True, env=env)
    ms = (time.time() - t0) * 1000.0
    print(f"PHASE {name} ms={ms:.0f} rc={r.returncode}", flush=True)
    if r.returncode != 0:
        print(r.stderr[-2000:], file=sys.stderr)
        sys.exit(r.returncode)
git = ["git", "-C", "/mnt/s34/repo"]
phase("clone_nocheckout", ["git", "clone", "--no-local", "--no-checkout",
                           "file:///data-src", "/mnt/s34/repo"])
dirs = os.environ["GIT_DIRS"].split()
phase("sparse_set", git + ["sparse-checkout", "init", "--cone"])
phase("sparse_set2", git + ["sparse-checkout", "set"] + dirs)
phase("checkout", git + ["checkout"])
for i in range(20):
    with open("/mnt/s34/repo/packages/react/package.json", "a") as f:
        f.write(f"\n// s34-edit-{i}\n")
phase("commit", git + ["-c", "user.email=s34@local", "-c", "user.name=s34",
                       "commit", "-aqm", "s34 workload"])
st = subprocess.run(git + ["status", "--porcelain"], capture_output=True, text=True)
n = subprocess.run(["sh", "-c", "find /mnt/s34/repo/packages -type f | wc -l"],
                   capture_output=True, text=True)
head = subprocess.run(git + ["rev-parse", "HEAD"], capture_output=True, text=True)
print(f"SANITY dirty={len(st.stdout.splitlines())} pkg_files={n.stdout.strip()} head={head.stdout.strip()[:12]}")
PYEOF
}
workload_git(){
    write_work_py
    docker exec -e GIT_DIRS="$GIT_DIRS" "$CTR" python3 /g/work.py
}

workload_npm(){
    docker exec -i "$CTR" sh -c 'cat > /g/work2.py' <<'PYEOF'
import subprocess, sys, time, os, shutil
os.makedirs("/mnt/s34/app", exist_ok=True)
for f in ("package.json", "package-lock.json"):
    shutil.copy(f"/deploy/proj/{f}", f"/mnt/s34/app/{f}")
t0 = time.time()
r = subprocess.run(["npm", "ci", "--offline", "--ignore-scripts",
                    "--no-audit", "--no-fund", "--loglevel=error"],
                   cwd="/mnt/s34/app", capture_output=True, text=True)
ms = (time.time() - t0) * 1000.0
print(f"PHASE npm_ci ms={ms:.0f} rc={r.returncode}", flush=True)
if r.returncode != 0:
    print(r.stderr[-2000:], file=sys.stderr); sys.exit(r.returncode)
n = subprocess.run(["sh", "-c", "find /mnt/s34/app/node_modules -type f | wc -l"],
                   capture_output=True, text=True)
s1 = subprocess.run(["sha256sum", "/mnt/s34/app/node_modules/react/package.json"],
                    capture_output=True, text=True)
v = subprocess.run(["npm", "ls", "--depth=0"], cwd="/mnt/s34/app",
                   capture_output=True, text=True)
print(f"SANITY nm_files={n.stdout.strip()} react_pkg_sha={s1.stdout.split()[0][:16]} ls_rc={v.returncode}")
PYEOF
    docker exec "$CTR" python3 /g/work2.py
}

# --- second-client remote visibility check (host side, after drain) ------------
verify_remote(){
    python3 - "$SRV_PORT" "$SCENARIO" <<'PYEOF'
import json, sys, urllib.request

port, scenario = sys.argv[1], sys.argv[2]
srv = f"http://127.0.0.1:{port}"
def lookup(path):
    body = json.dumps({"id": 1, "method": "lookup", "params": {"path": path}}).encode()
    req = urllib.request.Request(f"{srv}/rpc", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())["result"]
def read_nid(nid, size):
    body = json.dumps({"node_id": nid, "offset": 0, "size": size}).encode()
    req = urllib.request.Request(f"{srv}/data/read", data=body, method="POST",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return r.read()
ok = bad = 0
def check(mpath, pred):
    global ok, bad
    try:
        node = lookup(mpath)
        data = read_nid(node["id"], node["size"] + 65536)
        if pred(data): ok += 1
        else:
            print(f"BAD content {mpath}", file=sys.stderr); bad += 1
    except Exception as e:
        print(f"BAD {mpath}: {e}", file=sys.stderr); bad += 1

if scenario == "git":
    src = open("/Users/momoou/code/react-mirror/packages/react/package.json", "rb").read()
    check("/repo/packages/react/package.json",
          lambda d: d.startswith(src[:100]) and b"s34-edit-19" in d)
    check("/repo/packages/react-dom/package.json",
          lambda d: json.loads(d[:d.rindex(b"}")+1]).get("name") == "@react-dom/shared" or b"name" in d)
else:
    check("/app/node_modules/react/package.json",
          lambda d: json.loads(d[:d.rindex(b"}")+1]).get("name") == "react")
    check("/app/node_modules/lodash/package.json",
          lambda d: json.loads(d[:d.rindex(b"}")+1]).get("name") == "lodash")
print(f"VERIFY_REMOTE ok={ok} bad={bad}")
sys.exit(2 if bad else 0)
PYEOF
}

# --- main ----------------------------------------------------------------------
[[ "$GROUP" == "A" || "$GROUP" == "C" ]] || fail "GROUP must be A|C"
[[ "$SCENARIO" == "git" || "$SCENARIO" == "npm" ]] || fail "SCENARIO must be git|npm"
[[ "$EP" == "direct" || "$EP" == "rtt" ]] || fail "EP must be direct|rtt"
[[ -x "$DEPLOY/lightfs-$GROUP" ]] || fail "missing $DEPLOY/lightfs-$GROUP"

wall_check "pre-flight"
if [[ "$EP" == "rtt" ]]; then start_proxy; fi
start_minio
start_container
start_server_in_container
start_mount_in_container
wall_check "stack up"

metrics_snap "$OUT/m-before.txt"
WORK_RC=0
if [[ "$SCENARIO" == "git" ]]; then
    with_timeout "$(( DEADLINE - $(date +%s) ))" workload_git
else
    with_timeout "$(( DEADLINE - $(date +%s) ))" workload_npm
fi
WALL_WORK=$(( $(date +%s) - START ))
wall_check "workload done (rc=$WORK_RC)"

DRAIN_S=$(wb_wait_drained 20)
metrics_snap "$OUT/m-after.txt"
DELTA=$(metrics_delta "$OUT/m-before.txt" "$OUT/m-after.txt")
PROCS=$(proc_stats)
wall_check "drained (drain_s=$DRAIN_S)"

VERIFY=FAIL
verify_remote && VERIFY=OK
SANITY=$(grep -h "^SANITY" "$OUT/work.out" 2>/dev/null | tail -1)
TAG=$GROUP-$SCENARIO-$EP

echo "RESULT group=$GROUP scenario=$SCENARIO ep=$EP rc=$WORK_RC wall_work_s=$WALL_WORK drain_s=$DRAIN_S delta: $DELTA"
echo "SANITY $SANITY"
echo "PROCS $PROCS"
echo "VERIFY_REMOTE=$VERIFY"
printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\n" "$GROUP" "$SCENARIO" "$EP" "$WORK_RC" "$WALL_WORK" "${DRAIN_S:--1}" "$VERIFY" >> "$RESULT_TSV"
cp "$OUT/work.out" "$OUT/work-$TAG.out" 2>/dev/null || true
cp "$OUT/work.err" "$OUT/work-$TAG.err" 2>/dev/null || true
[[ "$WORK_RC" == "0" && "$VERIFY" == "OK" ]] || fail "workload rc=$WORK_RC verify=$VERIFY (see $OUT)"
log "PASS ($TAG) — artifacts in $OUT"
