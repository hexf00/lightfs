#!/usr/bin/env bash
# Shared helpers for test scripts. Source, don't execute.

# MinIO runs as a native binary here (no Docker on this Mac).
# Docker hosts can use: docker compose up -d
start_minio() {
  if curl -s -m 2 -o /dev/null "http://127.0.0.1:9000/minio/health/live"; then
    return 0
  fi
  local BIN="${MINIO_BIN:-$HOME/tools/bin/minio}"
  if [[ ! -x "$BIN" ]]; then
    echo "ERROR: MinIO binary not found at $BIN" >&2
    exit 1
  fi
  mkdir -p ./data/minio-data
  "$BIN" server ./data/minio-data --address :9000 --console-address :9001 \
    >/tmp/lightfs-minio.log 2>&1 &
  export MINIO_PID=$!
  for _ in $(seq 1 50); do
    curl -s -m 1 -o /dev/null "http://127.0.0.1:9000/minio/health/live" && return 0
    sleep 0.2
  done
  echo "ERROR: MinIO failed to start; see /tmp/lightfs-minio.log" >&2
  exit 1
}
