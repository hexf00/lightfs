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

CLI: `lightfs {embedded|server|mount|fsck|backup|gc|create-bucket|version}`

## License

Apache-2.0
