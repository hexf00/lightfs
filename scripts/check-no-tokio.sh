#!/usr/bin/env bash
set -euo pipefail
cargo tree -i tokio --quiet | grep -q . && { echo "ERROR: Tokio found" >&2; exit 1; }
echo "OK: Tokio not present"
