pub mod api;
pub mod client;
pub mod cmd;
pub mod config;
pub mod engine;
pub mod error;
pub(crate) mod fslock;
pub mod localfs;
pub mod routing;

// FUSE exists on Linux; mounting additionally needs /dev/fuse at runtime.
#[cfg(all(feature = "fuse", target_os = "linux"))]
pub mod fuse;

// Windows mounts through WinFsp's FUSE-compat layer in winfsp-x64.dll
// (hand-declared bindings, no external crate).
#[cfg(all(feature = "fuse", target_os = "windows"))]
pub mod winfsp;

pub mod metadata;
pub mod metrics;
pub mod object_store;
pub mod policy;
pub mod readcache;
pub mod rpc;
pub mod server;
pub mod shell;
pub mod sigv4;
pub mod vnext;
pub mod writeback;
