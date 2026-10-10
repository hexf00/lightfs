#!/usr/bin/env bash
# Definition-of-Done entry point for local development:
#   core unit tests -> MinIO integration -> shell script mode -> fsck
# FUSE and multi-client live in test-fuse.sh / test-multiclient.sh.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"

echo "== unit tests (no network, no FUSE)"
cargo test

echo "== MinIO integration (object store, embedded engine, external modification)"
start_minio
LIGHTFS_TEST_MINIO=1 cargo test

echo "== shell script mode over real MinIO"
cargo build -q
DB="/tmp/lightfs-local-$$-$(date +%s).db"
CFG="/tmp/lightfs-local-$$-$(date +%s).toml"
cat > "$CFG" <<EOF
mode = "embedded"
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "${LIGHTFS_S3_BUCKET:-lightfs-test}"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$DB"
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
EOF
./target/debug/lightfs-shell --config "$CFG" script tests/basic.lfs

echo "== fsck on a clean namespace"
DB2="/tmp/lightfs-fscklocal-$$-$(date +%s).db"
CFG2="/tmp/lightfs-fscklocal-$$-$(date +%s).toml"
cat > "$CFG2" <<EOF
mode = "embedded"
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "${LIGHTFS_FSCK_BUCKET:-lightfs-local-fsck}"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$DB2"
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
EOF
./target/debug/lightfs create-bucket --config "$CFG2"
./target/debug/lightfs-shell --config "$CFG2" write /fsck-me --string intact
./target/debug/lightfs fsck --config "$CFG2" | grep -q clean
./target/debug/lightfs-shell --config "$CFG2" rm /fsck-me >/dev/null
rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm" "$CFG2" "$DB2" "$DB2-wal" "$DB2-shm"

echo "LOCAL TESTS OK"
