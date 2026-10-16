#!/usr/bin/env bash
# S4.3.1 acceptance: real `lightfs gc` against an ISOLATED MinIO endpoint,
# NON-empty object set. Disposable bucket + temp dirs only; never touches
# any shared/production bucket. Candidates' timestamps are pinned directly
# in the isolated metadata DB (no sleeps). Object liveness is asserted via
# real S3 HEAD status codes (SigV4), not CLI exit codes.
#
# Scenarios (per S4.3.1 §4):
#   1. superseded objects exist in MinIO and qualify (pinned timestamps)
#   2. a published backup protects them  -> gc deletes NOTHING, objects HEAD 200
#   3. backup prune --id                 -> gc deletes them, HEAD 404,
#                                           gc_deletions finalized as done
#   4. a corrupt manifest                -> whole gc pass aborts, zero deletes
set -euo pipefail

ENDPOINT="${S431_ENDPOINT:-http://127.0.0.1:9000}"
REGION="us-east-1"
AK="${S431_AK:-minioadmin}"
SK="${S431_SK:-minioadmin}"
BUCKET="lightfs-s431-e2e"
BIN="${S431_BIN:-$(dirname "$0")/../target/debug/lightfs}"

WORK="$(mktemp -d /tmp/lightfs-s431-e2e.XXXXXX)"
CFG="$WORK/lightfs.toml"
ROOT="$WORK/root"
KA="objects/700/w-s431-a.data"
KB="objects/701/w-s431-b.data"
KC="objects/702/w-s431-c.data"
BODY_A="superseded content A for real gc"
BODY_B="superseded content B for real gc"
BODY_C="superseded content C for real gc"

s3() { # s3 METHOD KEY|BUCKET-URL [body]
  local method="$1" url="$2" body="${3-}"
  # HEAD must go through `curl -I` (real HEAD mode): `curl -X HEAD` still
  # speaks the with-body request shape (Content-Length: 0 on the wire) and
  # MinIO then stalls that request for its full 30s body timeout — seen
  # live as ~30s per HEAD with every other verb sub-2ms. -I is equivalent
  # and instant.
  if [ "$method" = "HEAD" ]; then
    curl -s -I -o /dev/null -w "%{http_code}" --aws-sigv4 "aws:amz:$REGION:s3" \
      --user "$AK:$SK" "$ENDPOINT/$url"
  elif [ -n "$body" ]; then
    curl -s -o /dev/null -w "%{http_code}" -X "$method" --aws-sigv4 "aws:amz:$REGION:s3" \
      --user "$AK:$SK" --data-binary "$body" -H "Content-Type: application/octet-stream" \
      "$ENDPOINT/$url"
  else
    curl -s -o /dev/null -w "%{http_code}" -X "$method" --aws-sigv4 "aws:amz:$REGION:s3" \
      --user "$AK:$SK" "$ENDPOINT/$url"
  fi
}
expect() { # expect WANT GOT WHAT
  if [ "$1" != "$2" ]; then echo "FAIL: $3 (want $1, got $2)"; exit 1; fi
  echo "  ok: $3 = $2"
}
cleanup() {
  for k in "$KA" "$KB" "$KC"; do s3 DELETE "$BUCKET/$k" >/dev/null 2>&1 || true; done
  s3 DELETE "$BUCKET" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "== setup: disposable bucket + isolated config =="
expect 200 "$(s3 PUT "$BUCKET")" "create bucket $BUCKET"
cat > "$CFG" <<EOF
[s3]
endpoint = "$ENDPOINT"
region = "$REGION"
bucket = "$BUCKET"
access_key = "$AK"
secret_key = "$SK"
path_style = true

[metadata]
path = "$WORK/m.db"

[retention]
superseded_secs = 600

[backup]
root = "$ROOT"
lock_timeout_secs = 10
EOF
# First gc: initializes the schema through the REAL CLI chain and proves
# the empty-db path (root configured, manifest gates all pass).
"$BIN" gc --config "$CFG" | grep -q "0 backup(s) verified" 
echo "  ok: gc boots the engine chain on an empty db"

echo "== scenario 1-2: qualified candidates, protected by a real backup =="
expect 200 "$(s3 PUT "$BUCKET/$KA" "$BODY_A")" "PUT A"
expect 200 "$(s3 PUT "$BUCKET/$KB" "$BODY_B")" "PUT B"
expect 200 "$(s3 HEAD "$BUCKET/$KA")" "A exists before gc"
# Qualify both immediately: superseded_at pinned to 1 (way past any cutoff
# of now-600s). Valid rows: these keys are referenced by NO node (direct
# superseded metadata, as the task brief prescribes).
sqlite3 "$WORK/m.db" "INSERT INTO superseded_objects VALUES ('$KA', 1);
INSERT INTO superseded_objects VALUES ('$KB', 1);"
"$BIN" backup --config "$CFG" --expires-in 3600 > "$WORK/bk1.txt"
BK1=$(ls -t "$ROOT" | grep -v '^gc.lock$' | head -1)
grep -q "\"$KA\"" "$ROOT/$BK1/meta.db.manifest" && grep -q "\"$KB\"" "$ROOT/$BK1/meta.db.manifest"
echo "  ok: backup $BK1 manifest contains both keys"
OUT=$("$BIN" gc --config "$CFG")
echo "$OUT" | grep -q "protected key(s), enrolled 0 pack(s) + 0 object(s), deleted 0"
echo "  ok: gc enrolls nothing while protected ($OUT)"
expect 200 "$(s3 HEAD "$BUCKET/$KA")" "A survives gc (manifest-protected)"
expect 200 "$(s3 HEAD "$BUCKET/$KB")" "B survives gc (manifest-protected)"

echo "== scenario 3: prune releases protection, gc deletes for real =="
"$BIN" backup --config "$CFG" --expires-in 3600 > "$WORK/bk2.txt"
BK2=$(ls -t "$ROOT" | grep -v '^gc.lock$' | head -1)
"$BIN" backup prune --id "$BK1" --config "$CFG" >/dev/null
OUT=$("$BIN" gc --config "$CFG")
echo "$OUT" | grep -q "protected" && echo "  ok: gc #2 still protected by $BK2"
expect 200 "$(s3 HEAD "$BUCKET/$KA")" "A still protected by the second backup"
"$BIN" backup prune --id "$BK2" --config "$CFG" >/dev/null
OUT=$("$BIN" gc --config "$CFG")
echo "$OUT" | grep -q "enrolled 0 pack(s) + 2 object(s), deleted 2, failed 0"
echo "  ok: gc enrolls and completes 2 tasks ($OUT)"
expect 404 "$(s3 HEAD "$BUCKET/$KA")" "A actually deleted"
expect 404 "$(s3 HEAD "$BUCKET/$KB")" "B actually deleted"
DONE_ROWS=$(sqlite3 "$WORK/m.db" "SELECT count(*) FROM gc_deletions WHERE state='done';")
PENDING=$(sqlite3 "$WORK/m.db" "SELECT count(*) FROM gc_deletions WHERE state!='done';")
[ "$DONE_ROWS" = "2" ] && [ "$PENDING" = "0" ]
echo "  ok: metadata finalized (2 done, 0 outstanding); superseded rows left: $(sqlite3 "$WORK/m.db" "SELECT count(*) FROM superseded_objects;")"

echo "== scenario 4: corrupt manifest fails the WHOLE pass, zero deletes =="
expect 200 "$(s3 PUT "$BUCKET/$KC" "$BODY_C")" "PUT C"
sqlite3 "$WORK/m.db" "INSERT INTO superseded_objects VALUES ('$KC', 1);"
"$BIN" backup --config "$CFG" --expires-in 3600 > "$WORK/bk3.txt"
BK3=$(ls -t "$ROOT" | grep -v '^gc.lock$' | head -1)
printf '{ corrupted' > "$ROOT/$BK3/meta.db.manifest"
if "$BIN" gc --config "$CFG" > "$WORK/gc3.txt" 2>&1; then
  echo "FAIL: gc must fail closed on a corrupt manifest"; exit 1
fi
grep -q "fail-closed" "$WORK/gc3.txt"
echo "  ok: gc refuses: $(tail -1 "$WORK/gc3.txt")"
expect 200 "$(s3 HEAD "$BUCKET/$KC")" "C survives (zero deletes happened)"
expect 0 "$(sqlite3 "$WORK/m.db" "SELECT count(*) FROM gc_deletions WHERE key='$KC';")" "no task enrolled for C in the aborted pass"

echo "== recovery path: prune the corrupt backup, gc completes =="
"$BIN" backup prune --id "$BK3" --config "$CFG" >/dev/null
OUT=$("$BIN" gc --config "$CFG")
echo "$OUT" | grep -q "deleted 1"
expect 404 "$(s3 HEAD "$BUCKET/$KC")" "C deleted after the corrupt backup is pruned"

echo "S431-E2E: ALL PASSED"
