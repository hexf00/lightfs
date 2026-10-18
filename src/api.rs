//! FsApi: the filesystem interface adapters program against.
//! The vnext Core implements it in-process (server); the network client
//! implements it over the LightFS protocol. Adapters (shell, FUSE) never
//! see SQLite or S3.

use crate::error::{Error, Result};
use crate::metadata::Node;
use crate::vnext::core::{Core, WriteHandle};

/// One buffered write session. Commit publishes content atomically with
/// optimistic version checking.
pub trait WriteSession: Send {
    fn write(&mut self, offset: u64, data: &[u8]) -> Result<usize>;
    fn commit(self: Box<Self>) -> Result<Node>;

    // ---- S4.25 spool-aware hooks (default impls keep simple sessions
    // working; spool-backed sessions override) ----

    /// Merge authoritative BASE content into the session spool for the
    /// commit: non-dirty ranges stream in from `base` (never materialized
    /// whole), dirty ranges keep the session's own bytes. Sessions without
    /// spool semantics refuse explicitly.
    fn overlay_base(&mut self, _base: &mut dyn std::io::Read, _base_len: u64) -> Result<()> {
        Err(Error::InvalidArgument(
            "this write session does not support base overlay".into(),
        ))
    }
    /// Highest dirty byte end (the session's visible-EOF contribution).
    fn session_dirty_end(&self) -> u64 {
        0
    }
    /// The session's high-water size (visible EOF lower bound).
    fn session_size(&self) -> u64 {
        0
    }
    /// Cumulative writes since open/reset — the commit window compares
    /// against this to detect concurrent writes.
    fn session_write_ops(&self) -> u64 {
        0
    }
    /// Release the session's staged bytes after a fully-consumed commit.
    fn session_reset(&mut self) -> Result<()> {
        Ok(())
    }

    /// Commit exactly `len` bytes streamed from `reader` as this session's
    /// content (S4.25 §三: the merged-commit upload — no whole-file
    /// materialization on a spool-backed session). The default degrades to
    /// an in-memory write+commit for sessions without a streaming path.
    fn commit_from(
        mut self: Box<Self>,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<Node> {
        let mut data = Vec::with_capacity(len.min(64 * 1024 * 1024) as usize);
        reader.read_to_end(&mut data)?;
        self.write(0, &data)?;
        self.commit()
    }
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
    /// Create a symlink at `path` whose target is stored verbatim
    /// (no-follow by definition).
    fn symlink(&self, target: &str, path: &str) -> Result<Node>;
    /// Return the stored target of the symlink at `path` (no-follow).
    fn readlink(&self, path: &str) -> Result<String>;

    // ---- stable-identity anchors (by-id protocol) ----
    //
    // These locate a node by its STABLE id instead of re-resolving the
    // path, so an open handle keeps working across renames by other
    // clients and a deleted identity fails with IdentityGone instead of
    // aliasing the new occupant of the recycled path. `path_hint` is for
    // error messages/diagnostics only - never consulted for resolution.
    // Default implementations degrade to the path form so adapters that
    // have no id anchor (WinFsp) keep their exact current behavior.

    /// stat a node by id.
    fn lookup_by_id(&self, node_id: i64, path_hint: &str) -> Result<Node> {
        let _ = node_id;
        self.lookup(path_hint)
    }

    /// read a byte range by id.
    fn read_by_id(&self, node_id: i64, path_hint: &str, offset: u64, size: u64) -> Result<Vec<u8>> {
        let _ = node_id;
        self.read(path_hint, offset, size)
    }

    /// Read a byte range by id DIRECTLY from the authority, bypassing any
    /// read cache. The vnext core has no cache; the default is the plain
    /// by-id read.
    fn read_by_id_authoritative(
        &self,
        node_id: i64,
        path_hint: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        self.read_by_id(node_id, path_hint, offset, size)
    }

    /// S4.25 §三: the file's CURRENT committed content as a STREAM, for
    /// the commit merge (base streams into the session spool; it is never
    /// materialized whole). The default materializes via the ranged read —
    /// correct, but not bounded-memory; the core and the network client
    /// override with true streaming. `ObjectMissing` propagates (a file
    /// with no committed content yet has no base).
    fn read_by_id_authoritative_stream(
        &self,
        node_id: i64,
        path_hint: &str,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        let data = self.read_by_id_authoritative(node_id, path_hint, 0, u64::MAX)?;
        let len = data.len() as u64;
        Ok((Box::new(std::io::Cursor::new(data)), len))
    }

    /// S4.25 §三: the mount adapter shares the FsApi layer's staging
    /// budget when one exists (one budget per process); None = the adapter
    /// opens its own with the bounded defaults.
    fn client_staging(&self) -> Option<std::sync::Arc<crate::staging::StagingBudget>> {
        None
    }

    /// S4.25 §三: the mounted-write COMMIT. `spool` holds the session's
    /// dirty bytes; the base streams in around them and the merged whole
    /// uploads straight out of the spool. CAS anchors on
    /// `expected_version`. The default composes the streaming read with a
    /// session commit_from; the core commits directly by id.
    fn commit_write_merged(
        &self,
        node_id: i64,
        path: &str,
        expected_version: i64,
        spool: &mut crate::staging::WriteSpool,
    ) -> Result<Node> {
        let (mut base, base_len) = self.read_by_id_authoritative_stream(node_id, path)?;
        spool.overlay_base(&mut *base, base_len)?;
        let len = spool.size();
        spool.rewind()?;
        let session = self.open_write_by_id(node_id, path, Some(expected_version))?;
        session.commit_from(spool.reader(), len)
    }

    /// start a buffered write session anchored on the id (the session's
    /// commit must then carry the id end-to-end).
    fn open_write_by_id(
        &self,
        node_id: i64,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        let _ = node_id;
        self.open_write(path, expected_version)
    }
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
    // S4.25 §三: streamed merged-commit upload (the spool is the source;
    // this commits it through the same PUT + one-transaction path).
    fn commit_from(
        self: Box<Self>,
        reader: &mut dyn std::io::Read,
        len: u64,
    ) -> Result<Node> {
        self.core.write_stream_by_id(
            self.handle.file_id,
            Some(self.handle.expected_version),
            reader,
            len,
        )
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
    fn symlink(&self, target: &str, path: &str) -> Result<Node> {
        Core::symlink(self, target, path)
    }
    fn readlink(&self, path: &str) -> Result<String> {
        Core::readlink(self, path)
    }
    fn lookup_by_id(&self, node_id: i64, _path_hint: &str) -> Result<Node> {
        Core::lookup_by_id(self, node_id)
    }
    fn read_by_id(
        &self,
        node_id: i64,
        _path_hint: &str,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>> {
        Core::read_by_id(self, node_id, offset, size)
    }
    fn open_write_by_id(
        &self,
        node_id: i64,
        path: &str,
        expected_version: Option<i64>,
    ) -> Result<Box<dyn WriteSession + '_>> {
        // Anchor the handle on the id and let commit's file-level version
        // gate do the CAS (path stays for error messages only).
        let handle = Core::open_write_id(self, node_id, path, expected_version)?;
        Ok(Box::new(CoreWriteSession { core: self, handle }))
    }
    fn read_by_id_authoritative_stream(
        &self,
        node_id: i64,
        path_hint: &str,
    ) -> Result<(Box<dyn std::io::Read + Send>, u64)> {
        // The core has no cache; this is the SAME verified streaming read
        // the whole-file path serves (footer checked before publication,
        // staging admitted against the budget).
        Core::read_stream_by_id(self, node_id, path_hint)
    }
    fn commit_write_merged(
        &self,
        node_id: i64,
        path: &str,
        expected_version: i64,
        spool: &mut crate::staging::WriteSpool,
    ) -> Result<Node> {
        // Base streams in around the session's dirty bytes; the merged
        // whole uploads straight out of the spool through the SAME
        // write_stream_by_id commit any streamed write takes (PUT, then
        // ONE SQLite transaction; failure re-registers fail-closed).
        match self.read_by_id_authoritative_stream(node_id, path) {
            Ok((mut base, base_len)) => spool.overlay_base(&mut *base, base_len)?,
            Err(Error::ObjectMissing(_)) => {} // never-committed file: no base
            Err(e) => return Err(e),
        }
        let len = spool.size();
        spool.rewind()?;
        self.write_stream_by_id(node_id, Some(expected_version), spool.reader(), len)
    }
}
