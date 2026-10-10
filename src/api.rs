//! FsApi: the filesystem interface adapters program against.
//! Core implements it in-process (embedded/server); the network client
//! implements it over the LightFS protocol. Adapters (shell, FUSE) never
//! see SQLite or S3.

use crate::core::{Core, WriteHandle};
use crate::error::Result;
use crate::metadata::Node;

/// One buffered write session. Commit publishes content atomically with
/// optimistic version checking.
pub trait WriteSession: Send {
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<usize>;
    fn commit(self: Box<Self>) -> Result<Node>;
}

pub trait FsApi: Send + Sync {
    fn lookup(&self, path: &str) -> Result<Node>;
    fn readdir(&self, path: &str) -> Result<Vec<Node>>;
    fn mkdir(&self, path: &str, mode: u32) -> Result<Node>;
    fn create(&self, path: &str, mode: u32) -> Result<Node>;
    fn open(&self, path: &str) -> Result<Node>;
    fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>>;
    /// Streaming read: bounded buffers; reader yields exactly `len` bytes.
    fn read_stream(
        &self,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let data = self.read(path, offset, size)?;
        let len = data.len() as u64;
        Ok((Box::new(std::io::Cursor::new(data)), len))
    }
    fn open_write(
        &self,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>>;
    fn rename(&self, old_path: &str, new_path: &str) -> Result<()>;
    fn unlink(&self, path: &str) -> Result<()>;
    fn rmdir(&self, path: &str) -> Result<()>;
    fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node>;
}

struct CoreWriteSession<'a> {
    core: &'a Core,
    handle: WriteHandle,
}

impl WriteSession for CoreWriteSession<'_> {
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        self.core.write(&self.handle, offset, data)
    }
    fn commit(self: Box<Self>) -> Result<Node> {
        self.core.commit_write(&self.handle)
    }
}

impl FsApi for Core {
    fn lookup(&self, path: &str) -> Result<Node> {
        Core::lookup(self, path)
    }
    fn readdir(&self, path: &str) -> Result<Vec<Node>> {
        Core::readdir(self, path)
    }
    fn mkdir(&self, path: &str, mode: u32) -> Result<Node> {
        Core::mkdir(self, path, mode)
    }
    fn create(&self, path: &str, mode: u32) -> Result<Node> {
        Core::create(self, path, mode)
    }
    fn open(&self, path: &str) -> Result<Node> {
        Core::open(self, path)
    }
    fn read(&self, path: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        Core::read(self, path, offset, size)
    }
    fn read_stream(
        &self,
        path: &str,
        offset: u64,
        size: u64,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        Core::open_read(self, path, offset, size)
    }
    fn open_write(
        &self,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        let handle = Core::open_write(self, path, expected_version)?;
        Ok(Box::new(CoreWriteSession { core: self, handle }))
    }
    fn rename(&self, old_path: &str, new_path: &str) -> Result<()> {
        Core::rename(self, old_path, new_path)
    }
    fn unlink(&self, path: &str) -> Result<()> {
        Core::unlink(self, path)
    }
    fn rmdir(&self, path: &str) -> Result<()> {
        Core::rmdir(self, path)
    }
    fn setattr(&self, path: &str, mode: Option<u32>, mtime: Option<i64>) -> Result<Node> {
        Core::setattr(self, path, mode, mtime)
    }
}
