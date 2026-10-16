# LightFS v0.1.0-experimental

First installable experimental release. Tag: `v0.1.0-experimental`
(branch `s31-pack-proto`). This is NOT a production 1.0: it is a
reliable baseline you can deploy, exercise with real file operations,
restart, inspect, and iterate on. Everything below was executed against
the tagged commit; commands are copied from the working scripts
(`scripts/v01-restart-check.sh` is the full walk as a single runnable
artifact).

## What LightFS is

- `lightfs server` is the ONLY production runtime: one process owns the
  SQLite metadata (single-writer authority, engine lock) and talks to an
  S3-compatible object store for file bytes.
- Clients connect over HTTP JSON-RPC: `lightfs mount` (Linux FUSE;
  Windows WinFsp drive letter, experimental) or `lightfs-shell`.
- GC / FSCK / backup are offline maintenance commands; they take the
  same engine lock and refuse to run against a live server's database.

## Install from a clean machine

Requirements: Rust 1.85+ (`rustup`), an S3-compatible endpoint you can
point at (MinIO works; see Known limits for what "S3-compatible" has
actually been tested against).

```sh
git clone https://github.com/hexf00/lightfs && cd lightfs
git checkout v0.1.0-experimental
cargo build --release
```

## Initialize and start

```sh
cp config/example.toml cfg.toml   # then edit: endpoint/bucket/keys
./target/release/lightfs create-bucket --config cfg.toml
./target/release/lightfs server --config cfg.toml     # foreground; Ctrl-C to stop
```

First start creates the metadata database and initializes the schema.
The metadata database is LightFS-internal state: on the next start the
server either opens it (if it matches the current contract) or REFUSES
with an actionable error — LightFS performs no automatic schema
migration and never deletes or overwrites a rejected database. If you
see that error on an older experimental database: back up anything you
need, stop everything, delete the metadata file (and use a fresh object
namespace), then start again. External triggers are likewise refused.
Extra tables/columns/views in the database are tolerated and left
untouched.

Opening a database runs idempotent initialization: WAL journal mode (a
persistent header change), `synchronous=FULL`, the `CREATE ... IF NOT
EXISTS` schema batch (which rebuilds any missing index) and an
idempotent root-node insert. On a conforming database each step is a
no-op — but open is honestly an idempotent initialization, not a pure
read.

## Exercise it

```sh
./target/release/lightfs-shell --server http://127.0.0.1:8080 mkdir /e2e
dd if=/dev/urandom bs=1M count=8 of /tmp/blob.bin
./target/release/lightfs-shell --server http://127.0.0.1:8080 write /e2e/blob --stdin < /tmp/blob.bin
./target/release/lightfs-shell --server http://127.0.0.1:8080 cat /e2e/blob | sha256sum   # compare with sha256sum /tmp/blob.bin
./target/release/lightfs-shell --server http://127.0.0.1:8080 ls /e2e
./target/release/lightfs-shell --server http://127.0.0.1:8080 rm /e2e/blob
```

Restart the server process and re-read: committed data survives process
death (this is exactly what `scripts/v01-restart-check.sh` automates,
including an offline fsck after a clean stop).

## Backup / restore / GC / FSCK

```sh
./target/release/lightfs backup --config cfg.toml          # metadata snapshot; bytes stay in object storage
./target/release/lightfs fsck   --config cfg.toml          # offline (stop the server first); exit 2 = problems
./target/release/lightfs gc     --config cfg.toml          # offline; honors backup protection and retention
./target/release/lightfs backup prune --id <id> --config cfg.toml
```

`backup` writes a consistent metadata snapshot plus a manifest
anchoring the object set; restore = restore the snapshot file, verify
with fsck, restart. Deleted-but-retained objects survive gc until their
retention window passes OR their protecting backup is pruned. A corrupt
manifest fails the whole gc pass (fail-closed, zero deletes). All of
this is exercised end-to-end by `scripts/backup-restore-drill.sh` and
`scripts/s431-gc-e2e.sh`.

## Support matrix (this release)

| Capability | Status | Evidence / limits |
|---|---|---|
| Fresh-environment init | verified | `scripts/v01-restart-check.sh`, `scripts/test-local.sh`, `scripts/test-prod-config.sh` |
| RPC file read/write | verified | same scripts: empty / 1 KiB / 8 MiB, overwrite, delete, byte-exact readback |
| Restart recovery (process crash / clean stop) | verified | v01-restart-check (real cross-process SIGTERM/stop + re-read); `p0d_r16` real-child SIGKILL crash test |
| Strict mode (default) | verified | every suite + MinIO contract tests + fault-injection + production-config switch |
| Write-Back mode | verified (experimental, default OFF) | 100+ unit/integration tests incl. real-child crash recovery, orphan tracking, capacity ledger; NOT exercised over a real FUSE mount in this release (no /dev/fuse on the dev machine) |
| Batch-Pack drain (Write-Back sub-mode) | verified (experimental, default OFF) | batch crash-contract tests, streaming seal, orphan lifecycle; same FUSE caveat |
| Backup / restore | verified | `scripts/backup-restore-drill.sh`: byte-exact restore, gc protection, corrupt-manifest fail-closed |
| FSCK / GC | verified | drill + `scripts/s431-gc-e2e.sh` (all scenarios) + lock-protocol tests (offline commands refuse a live server) |
| Linux (x86_64/aarch64 GNU) | verified at test level | full `cargo test --lib` suite green in a Debian container; no native end-to-end run in this release |
| Windows | verified at CI level | windows-ci: unit-tests + build + WinFsp mount-smoke (experimental adapter) |
| macOS | verified | full gate chain runs on macOS arm64 (dev machine) |

## Known limits (read before experimenting)

- **No power-loss durability claim.** Process death is tested (real
  child processes killed mid-flight). Host power loss / kernel panic is
  NOT tested anywhere: `synchronous=FULL` + WAL means an acked commit
  should be in the WAL, but nobody has pulled the plug on this code.
  Do not treat crash tests or the TLA+ models as power-loss proof.
- **MinIO is the only object store exercised.** "S3-compatible" here
  means: the concrete MinIO version in CI and the local MinIO builds.
  Real cloud endpoints (AWS S3, Aliyun OSS, ...) are UNTESTED in this
  release; S3 response-semantics documentation (`docs/storage-response-
  semantics.md`) records which error paths are vouched for.
- **No migration from earlier experimental schemas.** Databases written
  by builds older than this release are rejected at open (untouched) —
  re-initialize explicitly. Future versioned migrations, if any, will
  be their own explicit contract.
- **Single server, single metadata authority.** No multi-server, no
  HA, no client/server isomorphic embedding. WinFsp adapter is
  experimental: strict coherence needs `FlushFileBuffers` (plain
  `CloseHandle` has a short commit-visibility window).
- **Empty-file writes leave one stale object behind** (reported by
  fsck as STALE VERSION, reclaimed by gc). Harmless and visible, but
  it is there.
- Write-Back's staging directory survives process death, not disk
  loss; its durability boundary is documented in the config docs.

## Verification evidence (tagged commit)

- `cargo test --lib` / `--tests`: 370 + 28 green (macOS arm64)
- `LIGHTFS_TEST_MINIO=1` MinIO contract suite: green
- `scripts/test-local.sh`, `scripts/test-prod-config.sh`,
  `scripts/s431-gc-e2e.sh`, `scripts/backup-restore-drill.sh`,
  `scripts/v01-restart-check.sh`: all green
- `cargo fmt --check`, `cargo clippy --all-targets` (0 warnings),
  `git diff --check`, `cargo check --target x86_64-pc-windows-gnu
  --tests`: clean
- windows-ci (dispatch on the tagged branch): unit-tests + build +
  mount-smoke green
- TLA+ models (`formal/`): unchanged by this release's commits; the
  models' checked-in verdicts remain the ones from S4.12–S4.16 (safety
  & liveness green, vacuity & non-nullness probes as recorded). The
  schema-admission work in this release has no state-machine surface.
