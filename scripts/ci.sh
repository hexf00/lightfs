#!/usr/bin/env bash
# PR-required gate: exactly what CI runs, runnable locally with no flags.
#   1. fast unit tests (no network)
#   2. dependency gate: no Tokio in the resolved graph
#   3. MinIO integration tests (REQUIRED: fail hard if MinIO is unavailable)
# Exits non-zero on the first failure. No silent skips of required tests.
set -euo pipefail
cd "$(dirname "$0")/.."
source ./scripts/lib.sh

export PATH="$HOME/.cargo/bin:$PATH"

echo "[ci 1/4] formatting + lints (release-quality gates)"
cargo fmt --check
cargo clippy --all-targets --quiet -- -D warnings

echo "[ci 2/4] unit tests (no network)"
cargo test --quiet

echo "[ci 3/4] dependency gate"
bash scripts/check-no-tokio.sh

echo "[ci 4/4] MinIO integration tests (required; must not be skipped)"
# start_minio exits non-zero if MinIO cannot be started, so a missing
# backend can never turn the integration job green.
start_minio
LIGHTFS_TEST_MINIO=1 LIGHTFS_S3_ENDPOINT="${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}" \
  LIGHTFS_S3_BUCKET="${LIGHTFS_S3_BUCKET:-lightfs-test}" \
  cargo test --quiet

echo "CI GATE OK"
