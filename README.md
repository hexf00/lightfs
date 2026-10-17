# LightFS

A shared filesystem backed by S3-compatible object storage, written in Rust
with a fully synchronous core: Tokio is not used anywhere in the dependency
graph (enforced by `scripts/check-no-tokio.sh` in CI).

- SQLite is the ONLY logical metadata authority: refs, commits, COW
  trees and file versions update in one transaction per commit with
  optimistic compare-and-swap, so concurrent writers can never silently
  overwrite each other. Legacy v0.1 databases are refused, never touched.
- The object store is PAYLOAD-ONLY: immutable self-describing data
  objects plus one ownership marker written at initialization. Normal
  commits write no logical metadata to the store — if the database is
  lost, the guarantee is byte-exact CONTENT salvage (read-only), not
  namespace reconstruction. See `docs/vnext-model.md`.
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

CLI: `lightfs {server|mount|create-bucket [--insecure-public-rw]|version}` plus the standalone `lightfs-shell` / `lightfs-bench` binaries.

## The vnext model (this branch)

SQLite owns the namespace: refs → commits → COW trees → file versions,
updated atomically per commit. The object store holds immutable,
self-describing data objects (format version, kind, logical length,
sha256 envelope) and NOTHING else from the commit path — bucket
identity rests on one ownership marker written at init and verified on
every open (a second database on the same bucket is refused). If the
database is lost, the guarantee is READ-ONLY content salvage: verified,
byte-exact payloads with their physical locators (diagnostic only — no
identity is parsed out of key shapes), per-object rejection reports for
anything corrupt — no paths, no structure, no committed-vs-orphan
judgment. Reachability GC runs only with SQLite
intact and deletes only objects proven unreachable. `docs/vnext-model.md`
is the authoritative description, including crash decomposition and
known limitations (no write-back mode in this slice, single-server
execution, concurrent-init caveat).

The v0.1-era offline `gc` / `fsck` / `backup` CLIs and their docs are gone
from this branch (frozen on `main`, tag `v0.1.0-experimental`).

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
became a domain decision and a durable SQLite state transition in the
v0.1 core — HEAD adjudication, idempotent deletes, strict-pack and
write-back upload-failure handling, gc deletion intents — is documented in
[docs/storage-response-semantics.md](docs/storage-response-semantics.md)
(historical: that machinery was v0.1; the vnext failure taxonomy lives in
`src/error.rs` and `docs/vnext-model.md`).

## License

Apache-2.0
