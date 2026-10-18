#!/bin/bash
# s423-rss-probe.sh — ONE round of server peak-RSS measurement for a given
# build and payload size. A REAL server subprocess is spawned; the client
# (curl) streams the payload from DISK for the upload and streams the
# download into shasum — neither the client nor this script ever holds the
# payload in memory, so ru_maxrss(RUSAGE_CHILDREN) after the server exits
# is the server's peak RSS (curl's small buffers never dominate for the
# sizes probed).
#
# usage: s423-rss-probe.sh <build_dir> <payload_file> <round> <label>
# prints: RESULT|<label>|<round>|<rss_kb>|<put_s>|<get_s>
set -e
BUILD="$1"; PAYLOAD="$2"; ROUND="$3"; LABEL="$4"
export S423_LABEL="$LABEL" S423_ROUND="$ROUND" S423_PHASE="${S423_PHASE:-both}"
LEN=$(stat -f %z "$PAYLOAD")
EXPECT_SHA=$(cat "$PAYLOAD.sha")
B=lightfs-s423-rss-$LABEL-$ROUND-$(date +%s)
R=$(mktemp -d /tmp/s423rss.XXXX)
PORT=$((18200 + RANDOM % 300))

cat > "$R/cfg.toml" <<CFG
[s3]
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
bucket = "$B"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$R/meta.db"
[server]
bind = "127.0.0.1:$PORT"
CFG

"$BUILD/target/release/lightfs" create-bucket --config "$R/cfg.toml" >/dev/null
# The python wrapper is the server's PARENT: RUSAGE_CHILDREN after it
# reaps the killed server carries the server's peak RSS.
python3 - "$BUILD" "$R" "$PORT" "$PAYLOAD" "$LEN" "$EXPECT_SHA" "$S423_PHASE" <<'PY'
import subprocess, sys, time, os, urllib.request, signal, threading
build, r, port, payload, length, expect_sha, ph = sys.argv[1:8]
base = f"http://127.0.0.1:{port}"

def start_server(tag):
    srv = subprocess.Popen(
        [f"{build}/target/release/lightfs", "server", "--config", f"{r}/cfg.toml"],
        stdout=open(f"{r}/srv-{tag}.log", "w"), stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            urllib.request.urlopen(f"{base}/health", timeout=1).read(); return srv
        except Exception:
            assert srv.poll() is None, f"server({tag}) died on startup"
            time.sleep(0.1)
    raise SystemExit("server never became healthy")

class Sampler:
    def __init__(self, srv, name):
        self.srv, self.name, self.peak, self.stop = srv, name, 0, threading.Event()
        self.th = threading.Thread(target=self._run)
        self.th.start()
    def _run(self):
        while not self.stop.is_set():
            try:
                out = subprocess.run(["ps", "-o", "rss=", "-p", str(self.srv.pid)],
                                     capture_output=True, text=True).stdout.strip()
                kb = int(out.split()[0]) if out else 0
            except Exception:
                kb = 0
            self.peak = max(self.peak, kb)
            time.sleep(0.05)
    def done(self, wait=0.2):
        time.sleep(wait); self.stop.set(); self.th.join(); return self.peak

def upload():
    t0 = time.time()
    subprocess.run(["curl", "-s", "-X", "POST", "-T", payload,
                    "-H", "X-LightFS-Path: /big.bin", "-H", f"X-LightFS-Length: {length}",
                    "-H", "X-LightFS-Create: 1", f"{base}/data/write"],
                   check=True, capture_output=True)
    return time.time() - t0

def download():
    t1 = time.time()
    body = '{"path":"/big.bin","offset":0,"size":%s}' % length
    dl = subprocess.Popen(["curl", "-s", "-X", "POST", "-H", "Content-Type: application/json",
                           "-d", body, f"{base}/data/read"], stdout=subprocess.PIPE)
    sha = subprocess.run(["shasum", "-a", "256"], stdin=dl.stdout, capture_output=True,
                         text=True).stdout.split()[0]
    dl.wait()
    assert sha == expect_sha, f"round-trip sha mismatch: {sha} != {expect_sha}"
    return time.time() - t1

if ph == "put":
    srv = start_server("put")
    s = Sampler(srv, "put")
    put_s = upload()
    put_kb = s.done()
    srv.send_signal(signal.SIGTERM); srv.wait()
    print(f"RESULT|{os.environ.get('S423_LABEL','?')}|{os.environ.get('S423_ROUND','?')}|"
          f"put|{put_kb}|{put_s:.2f}|na")
else:
    # get phase measured on a FRESH server process: the uploader's memory
    # cannot contaminate the reading process's RSS.
    srv = start_server("warm")
    upload()
    srv.send_signal(signal.SIGTERM); srv.wait()
    srv = start_server("get")
    s = Sampler(srv, "get")
    get_s = download()
    get_kb = s.done()
    srv.send_signal(signal.SIGTERM); srv.wait()
    print(f"RESULT|{os.environ.get('S423_LABEL','?')}|{os.environ.get('S423_ROUND','?')}|"
          f"get|{get_kb}|na|{get_s:.2f}")
PY
rm -rf "$R"
# bucket cleanup (objects + bucket) via SigV4 curl
A=(-s --aws-sigv4 "aws:amz:us-east-1:s3" --user minioadmin:minioadmin)
body='<?xml version="1.0" encoding="UTF-8"?><Delete>'
curl "${A[@]}" "http://127.0.0.1:9000/$B?list-type=2&max-keys=1000" | grep -o '<Key>[^<]*</Key>' | sed 's/<[^>]*>//g' | while IFS= read -r k; do
  body="<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete><Object><Key>$k</Key></Object></Delete>"
  md5=$(printf '%s' "$body" | openssl md5 -binary | base64)
  curl "${A[@]}" -X POST -H "Content-Type: application/xml" -H "Content-MD5: $md5" \
    --data-binary "$body" "http://127.0.0.1:9000/$B?delete=" > /dev/null
done
curl "${A[@]}" -X DELETE "http://127.0.0.1:9000/$B" > /dev/null || true
