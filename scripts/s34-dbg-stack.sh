#!/usr/bin/env bash
# dbg stack: C-group WB+pack server+mount in a container, then run "$@" inside.
#   bash s34-dbg-stack.sh 'command string to run in container after mount ready'
# Env: BIN=A|C (default C), KEEP=1 to leave stack running (prints cleanup hint).
set -u
export PATH="$HOME/tools/bin:$PATH"
BIN=${BIN:-C}
CTR=${CTR:-s34-dbg}
HOST_IP=192.168.50.147
cleanup(){ docker rm -f "$CTR" >/dev/null 2>&1; [[ -n "${MPID:-}" ]] && kill "$MPID" 2>/dev/null; return 0; }
trap cleanup EXIT
rm -rf /tmp/s34-dbg && mkdir -p /tmp/s34-dbg
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  nohup ~/tools/bin/minio server /tmp/s34-dbg/minio --address 0.0.0.0:19003 --console-address 127.0.0.1:19012 \
  >/tmp/s34-dbg/minio.log 2>&1 & MPID=$!
sleep 2
docker rm -f "$CTR" >/dev/null 2>&1
docker run -d --rm --privileged --device /dev/fuse --name "$CTR" \
  -v "$HOME/code/s34-deploy":/deploy -v "$HOME/code/react-mirror":/data-src:ro \
  -p 127.0.0.1:18095:18095 lightfs-s34:arm64 sleep 3600 >/dev/null
docker exec -i "$CTR" sh -c 'mkdir -p /g && cat > /g/srv.toml' <<EOF
mode = "server"
[s3]
endpoint = "http://$HOST_IP:19003"
region = "us-east-1"
bucket = "s34-dbg"
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
$( [[ "$BIN" == "C" ]] && printf '[commit]\nmode = "writeback"\n[writeback]\nstaging_dir = "/g/wb"\ndrain_workers = 1\nbatch_pack_bytes = 8388608\nbatch_max_wait_ms = 200\nqueue_max_tasks = 4096\n' )
EOF
docker exec -i "$CTR" sh -c 'cat > /g/mnt.toml' <<EOF
mode = "server"
[s3]
endpoint = "http://$HOST_IP:19003"
region = "us-east-1"
bucket = "s34-dbg"
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
EOF
docker exec "$CTR" /deploy/lightfs-$BIN create-bucket --config /g/srv.toml >/dev/null
docker exec -d "$CTR" sh -c "LIGHTFS_METRICS=1 /deploy/lightfs-$BIN server --config /g/srv.toml > /g/server.log 2>&1"
for i in $(seq 1 50); do curl -s -m 1 -o /dev/null http://127.0.0.1:18095/health && break; sleep 0.2; done
curl -s -m 1 -o /dev/null http://127.0.0.1:18095/health || { echo "server not up"; exit 1; }
docker exec -d "$CTR" sh -c "mkdir -p /mnt/s34 && /deploy/lightfs-$BIN mount --config /g/mnt.toml /mnt/s34 > /g/mount.log 2>&1"
sleep 2
docker exec "$CTR" sh -c 'ls /mnt/s34 >/dev/null 2>&1' || { echo "mount not ready"; docker exec "$CTR" cat /g/mount.log; exit 1; }
echo "=== stack up (bin=$BIN container=$CTR) ==="
docker exec "$CTR" sh -c "$@"
rc=$?
echo "=== rc=$rc — logs: server=$(docker exec $CTR tail -3 /g/server.log 2>&1 | tail -1)"
