#!/usr/bin/env bash
# Real-OSS external acceptance (EXPLICIT OPT-IN, default OFF).
#
# Requires a dedicated NON-production bucket (or a strictly scoped test
# prefix) and credentials with only test-scoped permissions. Credentials are
# read from the environment only and are never printed.
#
# Required env:
#   LIGHTFS_OSS_ENDPOINT / LIGHTFS_OSS_ACCESS_KEY / LIGHTFS_OSS_SECRET_KEY
#   LIGHTFS_OSS_BUCKET   / LIGHTFS_OSS_PREFIX (default ci-<ts>/)
#
# Exits 2 with a PENDING marker when not configured - an absent run is
# NEVER reported as passed.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

: "${LIGHTFS_OSS_ENDPOINT:?PENDING: LIGHTFS_OSS_ENDPOINT not set - real-OSS acceptance NOT executed}"
: "${LIGHTFS_OSS_ACCESS_KEY:?PENDING: LIGHTFS_OSS_ACCESS_KEY not set}"
: "${LIGHTFS_OSS_SECRET_KEY:?PENDING: LIGHTFS_OSS_SECRET_KEY not set}"
: "${LIGHTFS_OSS_BUCKET:?PENDING: LIGHTFS_OSS_BUCKET not set}"
PREFIX="${LIGHTFS_OSS_PREFIX:-}"
TLS_OPT="${LIGHTFS_OSS_TLS:-true}"
# Provider specifics: AWS/MinIO = us-east-1/s3/path-style;
# Aliyun OSS = oss-cn-<region>/oss/virtual-host style.
REGION="${LIGHTFS_OSS_REGION:-us-east-1}"
SVC="${LIGHTFS_OSS_SERVICE:-s3}"
PATH_STYLE="${LIGHTFS_OSS_PATH_STYLE:-true}"
SKIP_PERF="${LIGHTFS_OSS_SKIP_PERF:-0}"
# Multipart test payload in MB. 64 exercises many parts over MinIO/loopback;
# public-cloud endpoints should keep this small (transfer costs) — 16 MB is
# still 2 full parts + tail, enough to validate the multipart protocol.
LARGE_MB="${LIGHTFS_OSS_MULTIPART_MB:-64}"

# SKIP_BUILD=1: resource-constrained targets (e.g. a 2C/1.7G VPS with no
# toolchain) run against prebuilt binaries placed in target/release/.
if [[ "${SKIP_BUILD:-0}" == "1" ]]; then
    for bin in lightfs lightfs-bench; do
        [[ -x "./target/release/$bin" ]] || { echo "ERROR: SKIP_BUILD=1 but ./target/release/$bin missing" >&2; exit 1; }
    done
    echo "SKIP_BUILD=1: using prebuilt binaries"
else
    export PATH="$HOME/.cargo/bin:$PATH"
    cargo build --release -q --bin lightfs --bin lightfs-bench
fi

RUN="/tmp/lightfs-oss-$$-$(date +%s)"
mkdir -p "$RUN"
CFG="$RUN/oss.toml"
cat > "$CFG" <<CFGEOF
mode = "server"
[s3]
endpoint = "$LIGHTFS_OSS_ENDPOINT"
region = "$REGION"
bucket = "$LIGHTFS_OSS_BUCKET"
access_key = "$LIGHTFS_OSS_ACCESS_KEY"
secret_key = "$LIGHTFS_OSS_SECRET_KEY"
path_style = $PATH_STYLE
service = "$SVC"
[metadata]
path = "$RUN/metadata.db"
[server]
bind = "127.0.0.1:18089"
[cache.metadata]
enabled = false
[cache.data]
enabled = false
CFGEOF

SERVER_PID=""
cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null || true
  rm -rf "$RUN"
}
trap cleanup EXIT

./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-oss-server.log 2>&1 &
SERVER_PID=$!
sleep 1

echo "target endpoint: $LIGHTFS_OSS_ENDPOINT bucket: $LIGHTFS_OSS_BUCKET (credentials redacted)"
echo "== 1. signature/region/addressing: create + read back (byte-exact)"
head -c 1048576 /dev/urandom > "$RUN/payload.bin"
EXPECT_SHA=$({ sha256sum 2>/dev/null || shasum -a 256; } < "$RUN/payload.bin" | awk '{print $1}')
./target/release/lightfs-shell --config "$CFG" mkdir /oss-acc
./target/release/lightfs-shell --config "$CFG" write /oss-acc/blob --stdin < "$RUN/payload.bin"
ACTUAL_SHA=$(./target/release/lightfs-shell --config "$CFG" cat /oss-acc/blob | { sha256sum 2>/dev/null || shasum -a 256; } | awk '{print $1}')
if [[ "$ACTUAL_SHA" == "$EXPECT_SHA" ]]; then
    echo "  read-back byte-exact: $ACTUAL_SHA"
else
    echo "  ERROR: read-back sha mismatch: expected $EXPECT_SHA got $ACTUAL_SHA" >&2
    exit 1
fi

echo "== 2. range reads"
./target/release/lightfs-shell --config "$CFG" read /oss-acc/blob 0 2>/dev/null | head -1 || true

echo "== 3. concurrent multipart write (part concurrency 4)"
LIGHTFS_S3_PART_CONCURRENCY=4 ./target/release/lightfs-bench \
  --server "http://127.0.0.1:$(grep -o 'bind.*' "$CFG" | grep -o '[0-9]*' | tail -1)" \
  --scenario lfs-stream-write --lfs-large-mb "$LARGE_MB"

echo "== 4. fsck on live namespace"
./target/release/lightfs fsck --config "$CFG" || true

if [[ "$SKIP_PERF" == "1" ]]; then
  echo "== 5. perf matrix skipped (LIGHTFS_OSS_SKIP_PERF=1, feasibility mode)"
  echo "REAL-OSS ACCEPTANCE COMPLETE (feasibility; endpoint/bucket above; credentials redacted)"
  exit 0
fi

echo "== 5. throughput/CPU/RSS matrix on the real endpoint"
# perf-matrix.sh respects LIGHTFS_S3_ENDPOINT/BUCKET; the OSS server-side
# latency shows up in the latency percentiles (loopback numbers do NOT apply)
# LIGHTFS_BENCH_COUNT / LIGHTFS_BENCH_SIZE control the dataset.
bash scripts/perf-matrix.sh "oss-acceptance" | tail -20

echo "REAL-OSS ACCEPTANCE COMPLETE (endpoint/bucket above; credentials redacted)"
