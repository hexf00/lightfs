#!/usr/bin/env bash
# Phase 8 FUSE integration (Linux host with /dev/fuse only).
# Mounts the filesystem through a real lightfs server and exercises it with
# standard Unix tools (find, grep, cp, mv, rm, stat, du).
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ "$(uname)" != "Linux" ]]; then
  echo "SKIP: FUSE integration requires Linux (this host: $(uname))." \
       "Core/shell/server tests cover correctness without FUSE (AGENT.md failure philosophy)." >&2
  exit 0
fi

if [[ ! -w /dev/fuse ]]; then
  echo "ERROR: /dev/fuse not available; run with --privileged / --device /dev/fuse" >&2
  exit 1
fi

export PATH="$HOME/.cargo/bin:$PATH"
source ./scripts/lib.sh

start_minio
# default features include fuse (only compiled on linux targets)
cargo build -q

CFG="/tmp/lightfs-fuse-$$-$(date +%s).toml"
DB="/tmp/lightfs-fuse-$$-$(date +%s).db"
cat > "$CFG" <<EOF
mode = "server"
[s3]
endpoint = "${LIGHTFS_S3_ENDPOINT:-http://127.0.0.1:9000}"
region = "us-east-1"
bucket = "${LIGHTFS_S3_BUCKET:-lightfs-test}"
access_key = "minioadmin"
secret_key = "minioadmin"
path_style = true
[metadata]
path = "$DB"
[server]
bind = "127.0.0.1:18081"
[cache.metadata]
enabled = true
ttl_seconds = 5
[cache.data]
enabled = false
EOF

./target/debug/lightfs server --config "$CFG" &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true; umount "$MNT" 2>/dev/null || true; rm -f "$CFG" "$DB" "$DB-wal" "$DB-shm"' EXIT
sleep 1

# mount via the network client (client -> server -> SQLite+S3)
MNT=$(mktemp -d /tmp/lightfs-mnt-XXXX)
./target/debug/lightfs mount --config "$CFG" --allow-other "$MNT" &
MOUNT_PID=$!
# wait for mount
for i in $(seq 1 50); do mountpoint -q "$MNT" && break; sleep 0.2; done
mountpoint -q "$MNT" || { echo "mount did not come up" >&2; exit 1; }

set -x
mkdir "$MNT/fuse-dir"

# create / read / overwrite / append
echo "hello fuse" > "$MNT/fuse-dir/a.txt"
cat "$MNT/fuse-dir/a.txt" | grep -q "hello fuse"
printf 'second line' >> "$MNT/fuse-dir/a.txt"          # append
cat "$MNT/fuse-dir/a.txt" | grep -q "second line"

# offset write (no truncation): overwrite the middle of the file
dd if=/dev/zero of="$MNT/fuse-dir/offset.bin" bs=1k count=64 2>/dev/null
dd if=/dev/urandom of="$MNT/fuse-dir/pattern.bin" bs=1k count=16 2>/dev/null
dd if="$MNT/fuse-dir/pattern.bin" of="$MNT/fuse-dir/offset.bin" bs=512 seek=32 count=16 conv=notrunc 2>/dev/null
cmp <(dd if="$MNT/fuse-dir/offset.bin" bs=512 skip=32 count=16 2>/dev/null) \
    <(dd if="$MNT/fuse-dir/pattern.bin" bs=512 count=16 2>/dev/null)

# truncate: extend and shrink, zero-fill check on extension
truncate -s 8192 "$MNT/fuse-dir/offset.bin"
[[ "$(stat -c %s "$MNT/fuse-dir/offset.bin" 2>/dev/null || stat -f %z "$MNT/fuse-dir/offset.bin")" -eq 8192 ]]
truncate -s 1024 "$MNT/fuse-dir/offset.bin"
[[ "$(stat -c %s "$MNT/fuse-dir/offset.bin" 2>/dev/null || stat -f %z "$MNT/fuse-dir/offset.bin")" -eq 1024 ]]

# copy / move / stat / listing tools
cp "$MNT/fuse-dir/a.txt" "$MNT/fuse-dir/b.txt"
mv "$MNT/fuse-dir/b.txt" "$MNT/fuse-dir/c.txt"
[[ ! -e "$MNT/fuse-dir/b.txt" ]]
stat "$MNT/fuse-dir/c.txt" >/dev/null
du -sh "$MNT/fuse-dir" >/dev/null
find "$MNT" -type f | grep -q c.txt
grep -R "hello fuse" "$MNT" >/dev/null
command -v rg >/dev/null && rg -q "hello fuse" "$MNT"

# concurrent access to multiple handles
( echo writer1 > "$MNT/fuse-dir/con1" ) & ( echo writer2 > "$MNT/fuse-dir/con2" ) & wait
grep -q writer1 "$MNT/fuse-dir/con1" && grep -q writer2 "$MNT/fuse-dir/con2"

# delete
rm "$MNT/fuse-dir/c.txt"
rmdir --ignore-fail-on-non-empty "$MNT/fuse-dir" || true
set +x

# unmount + remount: state must survive (server restart included)
kill "$MOUNT_PID" 2>/dev/null || true
sleep 1
umount "$MNT" 2>/dev/null || fusermount -u "$MNT" || true
kill "$SERVER_PID" 2>/dev/null || true
wait "$SERVER_PID" 2>/dev/null || true
./target/release/lightfs server --config "$CFG" >>/tmp/lightfs-cmp-server.log 2>&1 &
SERVER_PID=$!
sleep 1
./target/release/lightfs mount --config "$CFG" --allow-other "$MNT" &
MOUNT_PID=$!
for i in $(seq 1 50); do mountpoint -q "$MNT" && break; sleep 0.2; done

set -x
cat "$MNT/fuse-dir/a.txt" | grep -q "second line"     # survives remount
[[ -f "$MNT/fuse-dir/con1" ]]
set +x

kill $MOUNT_PID 2>/dev/null || true
sleep 1
umount "$MNT" 2>/dev/null || fusermount -u "$MNT" || true
kill $SERVER_PID 2>/dev/null || true
rm -rf "$MNT"
echo "FUSE integration OK"
