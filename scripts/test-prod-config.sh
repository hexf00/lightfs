#!/usr/bin/env bash
# Phase 14: production S3/OSS switch is a CONFIG-ONLY change.
# Simulates it by pointing the same code at a different bucket + region +
# access credentials on the local S3-compatible endpoint. No recompile, no
# application change: the entire acceptance is that these writes/reads/fsck
# succeed on the new configuration.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"
start_minio
cargo build -q

DB="/tmp/lightfs-prod-$$-$(date +%s).db"
CFG="/tmp/lightfs-prod-$$-$(date +%s).toml"
PROD_BUCKET="${LIGHTFS_PROD_BUCKET:-lightfs-prod-sim}"
cat > "$CFG" <<EOF
# Production-shaped config: only endpoint/bucket/credentials/region change
# when moving from MinIO to real S3/OSS.
mode = "embedded"
[s3]
endpoint = "${LIGHTFS_PROD_ENDPOINT:-${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}}"
region = "${LIGHTFS_PROD_REGION:-cn-hangzhou}"
bucket = "$PROD_BUCKET"
access_key = "${LIGHTFS_PROD_ACCESS_KEY:-minioadmin}"
secret_key = "${LIGHTFS_PROD_SECRET_KEY:-minioadmin}"
path_style = true
[metadata]
path = "$DB"
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
EOF

# one-time provisioning of the (simulated) production bucket
target/debug/lightfs create-bucket --config "$CFG"

cleanup() { rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"; }
trap cleanup EXIT

SHELL="target/debug/lightfs-shell"
$SHELL --config "$CFG" mkdir /prod-sim
$SHELL --config "$CFG" write /prod-sim/data --string produced-on-prod-shaped-config
$SHELL --config "$CFG" cat /prod-sim/data | grep -q "produced-on-prod-shaped-config"
target/debug/lightfs fsck --config "$CFG" | grep -q "clean"
$SHELL --config "$CFG" rm /prod-sim/data >/dev/null
echo "PRODUCTION CONFIG SWITCH OK (bucket=$PROD_BUCKET, region from config)"
