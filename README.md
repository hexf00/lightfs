# LightFS

A shared filesystem backed by S3-compatible object storage, written in Rust
with a fully synchronous core: Tokio is not used anywhere in the dependency
graph (enforced by `scripts/check-no-tokio.sh` in CI).

- S3-compatible object storage holds file bytes.
- SQLite holds the namespace/metadata; every write commits with an
  optimistic compare-and-swap, so concurrent writers can never silently
  overwrite each other.
- One `lightfs server` owns the metadata in multi-client mode; clients
  speak JSON-RPC over HTTP plus binary data endpoints.
- `lightfs mount` (Linux, FUSE) exposes the namespace to existing Unix
  tools. A Windows adapter (WinFsp, drive-letter mounts) exists and is
  exercised in CI, but is **experimental**: after a plain `CloseHandle`
  there is a short commit-visibility window (strict coherence is provided
  by `FlushFileBuffers`; see the Windows docs in the project repository).

## Build

```sh
cargo build --release
```

## Test

```sh
cargo test --release                              # network-free suites
LIGHTFS_TEST_MINIO=1 cargo test --release         # S3 integration (MinIO on 127.0.0.1:9000)
```

## Run

```sh
cp config/example.toml cfg.toml      # edit endpoint/bucket/credentials
./target/release/lightfs create-bucket --config cfg.toml
./target/release/lightfs server --config cfg.toml
./target/release/lightfs-shell --server http://127.0.0.1:8080 ls /
./target/release/lightfs mount --config cfg.toml /mnt/lightfs   # Linux + /dev/fuse
```

CLI: `lightfs {server|mount|fsck|backup [--expires-in SECS]|backup prune --id ID|gc|create-bucket|version}` plus the standalone `lightfs-shell` / `lightfs-bench` binaries.

## Backup & gc

Backups are directories under the canonical `[backup] root`: a `meta.db`
snapshot, a versioned object manifest exported from that snapshot, and a
sha256-pinned `backup.json`, published atomically. Each is protected
forever unless created with `--expires-in SECS`; remove one with
`backup prune --id ID`.

`gc` (and `fsck`) is OFFLINE maintenance (S4.14): it takes the engine
lock BEFORE it opens the metadata store, and holds it until the process
exits. Against a running server it fails at the lock and never touches
the database — stop the server first, run the maintenance pass, restart.
`gc` is the only path that deletes objects, and it is safe by gates: with
no `[backup] root` configured it refuses to delete anything; it verifies
EVERY manifest in the root first (one corrupt backup fails the whole pass,
zero deletions); keys listed in valid unexpired manifests survive.
Retention off (`superseded_secs = 0`) does not disable gc — retirement
then registers durable deletion tasks, which gc drains (candidates qualify
immediately). Configuring a root does not protect legacy backups kept
outside it — inventory and retire/re-publish them first; see the
`[backup]` section of `config/example.toml`.

## Symlink semantics

Symlink targets are stored verbatim and returned verbatim by `readlink`.
Resolution differs by access path - this is a selected design, not an
oversight:

- **FUSE mount**: the Linux kernel resolves link targets exactly as it
  does for NFS or sshfs. Absolute targets are resolved against the
  *calling process's* root, so they can and do point outside the mount
  point. A FUSE mount of LightFS is therefore **not** an isolation
  boundary - do not use it as a security mechanism.
- **RPC core**: the server-side path walker clamps resolution
  at the virtual root (chroot-style). Direct path arguments may not
  contain `..` components - an independent path-validation rule that
  applies to every RPC call. Relative targets resolve against the
  directory containing the link.

Both paths bound symlink resolution at 40 per path walk (matching the
kernel SYMLOOP_MAX bound) and fail with an error instead of hanging on
link loops. `rename` and `unlink` act on the link node itself and never
follow the final path component.

## Storage response semantics

How an object-store response (2xx / 404 / NoSuchBucket / transport error)
becomes a domain decision and a durable SQLite state transition — the
authoritative table for HEAD adjudication, idempotent deletes, strict-pack
and write-back upload-failure handling, upload-failure classification, and
gc deletion intents lives in
[docs/storage-response-semantics.md](docs/storage-response-semantics.md).

## License

Apache-2.0
