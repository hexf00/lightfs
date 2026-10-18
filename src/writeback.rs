//! Write-back session state machine shared by the FUSE (Linux) and WinFsp
//! (Windows) adapters. Platform-independent and unit-tested on every host,
//! so correctness fixes land in both adapters at once.
//!
//! Model: per-handle pending writes are kept as NON-OVERLAPPING offset
//! chunks where the LATEST write always wins. Insertion splits existing
//! chunks around the new range, so "overlay ascending by offset" is
//! equivalent to applying writes in arrival order - a plain BTreeMap<u64,
//! Vec<u8>> iterated by offset alone cannot express that (a later write at
//! a lower offset must beat an earlier write at a higher offset in the
//! overlapping region).
//!
//! Commit protocol (see adapters' commit_write): drain (take all chunks),
//! read the file's current committed content, overlay the drained chunks,
//! upload with CAS on the version captured at open. Outcomes:
//! - success: note_commit_success re-bases the version; the session only
//!   becomes "committed" when no NEW chunks arrived during the I/O window
//!   (a write-during-commit batch must be committed by the next flush).
//! - failure: restore_under puts the drained chunks back WITHOUT clobbering
//!   chunks that arrived meanwhile (newest wins), and the session re-bases
//!   to the version observed at attempt start so a retry CAS can succeed.

use std::collections::BTreeMap;

/// One pending chunk range with latest-write-wins splitting.
#[derive(Default)]
pub struct ChunkMap(BTreeMap<u64, Vec<u8>>);

impl ChunkMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Insert a write with LATEST-WINS overlap semantics: existing chunks
    /// are split/trimmed around [off, off+len) so the map stays
    /// non-overlapping and ascending-offset overlay == arrival order.
    pub fn insert(&mut self, off: u64, data: Vec<u8>) {
        if data.is_empty() {
            return;
        }
        let end = off + data.len() as u64;
        // Existing chunks are non-overlapping, so at most ONE straddles the
        // range start; the rest start inside [off, end).
        let mut keys: Vec<u64> = self
            .0
            .range(..off)
            .next_back()
            .map(|(k, _)| *k)
            .into_iter()
            .collect();
        keys.extend(self.0.range(off..end).map(|(k, _)| *k));

        let mut fragments: Vec<(u64, Vec<u8>)> = Vec::new();
        for k in keys {
            let Some((o, d)) = self.0.remove_entry(&k) else {
                continue;
            };
            let d_end = o + d.len() as u64;
            if d_end <= off || o >= end {
                // No overlap after all (straddle candidate check).
                fragments.push((o, d));
                continue;
            }
            if o < off {
                fragments.push((o, d[..(off - o) as usize].to_vec()));
            }
            if d_end > end {
                fragments.push((end, d[(end - o) as usize..].to_vec()));
            }
        }
        for (o, d) in fragments {
            if !d.is_empty() {
                self.0.insert(o, d);
            }
        }
        self.0.insert(off, data);
    }

    /// Take all pending chunks out (commit start).
    pub fn drain(&mut self) -> BTreeMap<u64, Vec<u8>> {
        std::mem::take(&mut self.0)
    }

    /// Put drained chunks back after a FAILED commit. Chunks that arrived
    /// while the commit was in flight are NEWER and win: only the
    /// uncovered fragments of the drained chunks are re-inserted.
    pub fn restore_under(&mut self, drained: &BTreeMap<u64, Vec<u8>>) {
        for (&o, d) in drained.iter() {
            let d = d.as_slice();
            if d.is_empty() {
                continue;
            }
            let end = o + d.len() as u64;
            let mut keys: Vec<u64> = self
                .0
                .range(..o)
                .next_back()
                .map(|(k, _)| *k)
                .into_iter()
                .collect();
            keys.extend(self.0.range(o..end).map(|(k, _)| *k));

            let mut cursor = o;
            for k in keys {
                let Some((ko, kd)) = self.0.get_key_value(&k) else {
                    continue;
                };
                let (ko, ke) = (*ko, ko + kd.len() as u64);
                if ke <= cursor {
                    continue;
                }
                if ko > cursor {
                    let gap_end = ko.min(end);
                    self.0.insert(
                        cursor,
                        d[(cursor - o) as usize..(gap_end - o) as usize].to_vec(),
                    );
                }
                cursor = cursor.max(ke);
                if cursor >= end {
                    break;
                }
            }
            if cursor < end {
                self.0.insert(cursor, d[(cursor - o) as usize..].to_vec());
            }
        }
    }

    /// Adjust pending chunks after a truncate to `new_size`:
    /// - chunks entirely beyond EOF: removed (the kernel discards those
    ///   dirty pages and will not resend them);
    /// - chunks straddling EOF: trimmed to the pre-EOF prefix (otherwise
    ///   the next flush re-extends the file);
    /// - chunks entirely below EOF: kept.
    pub fn trim_truncate(&mut self, new_size: u64) {
        let beyond: Vec<u64> = self.0.range(new_size..).map(|(k, _)| *k).collect();
        for k in beyond {
            self.0.remove(&k);
        }
        if let Some((k, d)) = self.0.range(..new_size).next_back() {
            let (k, d_len) = (*k, d.len() as u64);
            if k + d_len > new_size {
                if let Some(d) = self.0.get_mut(&k) {
                    d.truncate((new_size - k) as usize);
                }
            }
        }
    }

    /// Overlay non-overlapping chunks onto the file's current committed
    /// content (zero-filling gaps), producing the commit payload.
    /// Overlay the chunks intersecting `buf` (which starts at file offset
    /// `read_offset`). Read-your-writes for the same mount: POSIX requires
    /// a read (or mmap fault) after write(2) to return the written bytes
    /// even before flush. Found missing when git verified a pack file via
    /// mmap mid-write - reads served committed-only and the tail vanished.
    pub fn overlay_range(&self, buf: &mut [u8], read_offset: u64) {
        if buf.is_empty() {
            return;
        }
        let end = read_offset + buf.len() as u64;
        for (&off, data) in &self.0 {
            if off >= end {
                break;
            }
            let chunk_end = off + data.len() as u64;
            if chunk_end <= read_offset {
                continue;
            }
            let src = if read_offset > off {
                (read_offset - off) as usize
            } else {
                0
            };
            let dst = if off > read_offset {
                (off - read_offset) as usize
            } else {
                0
            };
            let n = (data.len() - src).min(buf.len() - dst);
            buf[dst..dst + n].copy_from_slice(&data[src..src + n]);
        }
    }

    /// End offset of the highest chunk (chunks are non-overlapping and the
    /// map is sorted by offset). Adapters use this to publish the in-flight
    /// EOF to getattr while a write session is open.
    pub fn max_end(&self) -> u64 {
        self.0
            .iter()
            .next_back()
            .map(|(&off, d)| off + d.len() as u64)
            .unwrap_or(0)
    }

    pub fn overlay(&self, content: &mut Vec<u8>) {
        for (&off, data) in self.0.iter() {
            let end = off as usize + data.len();
            if end > content.len() {
                content.resize(end, 0);
            }
            content[off as usize..end].copy_from_slice(data);
        }
    }
}

/// Per-handle write-back session.
/// In-flight EOF per path, published to getattr while a write session is
/// open. Two consumers: (1) appends - Windows' FUSE layer derives the
/// WriteToEndOfFile offset from our getattr, and in the write-back model
/// the committed size only advances at flush, so two appends between
/// flushes would fold onto one offset; (2) stat coherence - POSIX expects
/// fstat to reflect written bytes. Reports max(committed, in-flight), the
/// same thing the Linux page cache does for dirty writes.
///
/// Lock discipline: methods only ever take this struct's own mutex - safe
/// to call while holding an adapter's session-map lock. Callers that must
/// inspect their session map (for remaining chunk ends) do so FIRST and
/// pass the result in.
#[derive(Default)]
pub struct PendingEof {
    map: std::sync::Mutex<std::collections::HashMap<String, u64>>,
}

impl PendingEof {
    pub fn new() -> Self {
        Self {
            map: Default::default(),
        }
    }

    /// Raise the in-flight EOF to `end` (a write landed at [end-len, end)).
    pub fn advance(&self, path: &str, end: u64) {
        let mut m = self.map.lock().unwrap();
        let e = m.entry(path.to_string()).or_insert(end);
        if *e < end {
            *e = end;
        }
    }

    pub fn get(&self, path: &str) -> u64 {
        self.map.lock().unwrap().get(path).copied().unwrap_or(0)
    }

    /// After a commit: the committed size plus remaining open-session
    /// chunks defines the visible EOF. Never LOWERS an entry: a concurrent
    /// write may have advanced it after the caller inspected its sessions.
    pub fn rebase(&self, path: &str, end: u64) {
        let mut m = self.map.lock().unwrap();
        match m.get_mut(path) {
            Some(e) => {
                if end > *e {
                    *e = end;
                }
            }
            None => {
                m.insert(path.to_string(), end);
            }
        }
    }

    /// After a session went away (release): unlike rebase this may LOWER
    /// the value - a failed final commit must not leave a phantom in-flight
    /// EOF (readers would see a zero hole). `end == 0` removes the entry.
    pub fn recompute(&self, path: &str, end: u64) {
        let mut m = self.map.lock().unwrap();
        if end == 0 {
            m.remove(path);
        } else {
            m.insert(path.to_string(), end);
        }
    }

    pub fn drop_path(&self, path: &str) {
        self.map.lock().unwrap().remove(path);
    }
}

pub struct PendingWrite {
    pub path: String,
    /// STABLE node id captured when write intent started. The commit
    /// anchors on THIS id (not the path), so a rename by another client
    /// cannot detach the session from its file and a deleted identity
    /// fails explicitly instead of aliasing the path successor.
    pub node_id: i64,
    /// Version captured when write intent started; the CAS anchor.
    pub expected_version: i64,
    /// S4.25 §三: the session's unflushed bytes live in a private DISK
    /// spool inside the adapter's managed staging directory (bounded by
    /// the process staging budget) — random writes, holes, read-your-
    /// writes overlay, and a streamed commit. The commit swaps the spool
    /// out wholesale (kernel writes during the commit land in a FRESH
    /// spool), and a failed commit merges the drained batch back with
    /// `merge_under` (window writes stay newer, nothing is lost).
    pub spool: crate::staging::WriteSpool,
    /// True once a commit succeeded AND no newer chunks arrived after it.
    pub committed: bool,
    /// The name this session was opened under no longer exists (the file
    /// was unlinked, or renamed over). POSIX semantics for such a
    /// disembodied inode: writes still succeed but go nowhere, and the
    /// commit must NOT look the name up again - it may have been recycled
    /// by a brand-new file. `path` is blanked together with this flag so
    /// every path-matching scan (overlay, visible_eof, recompute) can
    /// never alias the new file.
    pub orphaned: bool,
    /// For an orphaned session: the detached entry (kernel ino) now holding
    /// this file's bytes. Writes go there and reads are served from it
    /// until the last handle closes; None on a live (non-orphaned) session.
    pub detached_ino: Option<u64>,
    /// Set when a flush/fsync reported an error to the application. The
    /// release-time best-effort commit must then STAY BEST-EFFORT-REFUSED:
    /// the app already saw the failure, and a close-time retry that flips
    /// the outcome would turn "error + not committed" into the lie
    /// "error + committed anyway". An explicit retry flush clears this by
    /// succeeding (note_commit_success).
    pub flush_failed: bool,
}

impl PendingWrite {
    pub fn new(
        path: String,
        node_id: i64,
        expected_version: i64,
        staging: &std::sync::Arc<crate::staging::StagingBudget>,
    ) -> std::result::Result<Self, crate::error::Error> {
        Ok(Self {
            path,
            node_id,
            expected_version,
            spool: crate::staging::WriteSpool::open(staging)?,
            committed: false,
            orphaned: false,
            detached_ino: None,
            flush_failed: false,
        })
    }

    /// Record a successful commit. Returns true when the session is fully
    /// committed; false when NEW chunks arrived during the commit window -
    /// those must be committed by the next flush (the version is re-based
    /// here, so that next commit's CAS succeeds).
    pub fn note_commit_success(&mut self, new_version: i64) -> bool {
        self.expected_version = new_version;
        self.flush_failed = false;
        if !self.spool.is_dirty() {
            self.committed = true;
            true
        } else {
            false
        }
    }

    /// Re-base the CAS anchor to the version observed at the start of a
    /// failed commit attempt, so a retry flush can succeed.
    pub fn rebase(&mut self, observed_version: i64) {
        self.expected_version = observed_version;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content_after(chunks: &ChunkMap, base: &[u8]) -> Vec<u8> {
        let mut v = base.to_vec();
        chunks.overlay(&mut v);
        v
    }

    #[test]
    fn same_offset_write_replaces_only_its_range() {
        let mut c = ChunkMap::new();
        c.insert(0, b"AAAA".to_vec());
        c.insert(0, b"BB".to_vec());
        // The new write only covers [0,2): pending bytes [2,4) stay "AA"
        // (pwrite semantics on the same pending batch).
        assert_eq!(content_after(&c, b""), b"BBAA");
        // A same-range overwrite replaces fully.
        c.insert(0, b"ZZZZ".to_vec());
        assert_eq!(content_after(&c, b""), b"ZZZZ");
    }

    #[test]
    fn later_lower_offset_write_wins_overlap() {
        // High range first, then a lower range overlapping it: the later
        // (lower) write must own the overlapping bytes.
        let mut c = ChunkMap::new();
        c.insert(4096, vec![b'H'; 8192]); // [4096, 12288)
        c.insert(0, vec![b'L'; 8192]); // [0, 8192) later
        let got = content_after(&c, b"");
        assert_eq!(got.len(), 12288);
        assert!(got[..8192].iter().all(|&b| b == b'L'));
        assert!(got[8192..].iter().all(|&b| b == b'H'));
    }

    #[test]
    fn later_higher_offset_write_wins_overlap() {
        let mut c = ChunkMap::new();
        c.insert(0, vec![b'L'; 8192]); // [0, 8192) first
        c.insert(4096, vec![b'H'; 8192]); // [4096, 12288) later
        let got = content_after(&c, b"");
        assert_eq!(got.len(), 12288);
        assert!(got[..4096].iter().all(|&b| b == b'L'));
        assert!(got[4096..].iter().all(|&b| b == b'H'));
    }

    #[test]
    fn middle_write_splits_existing_chunk() {
        let mut c = ChunkMap::new();
        c.insert(0, vec![b'A'; 12]); // [0, 12)
        c.insert(4, vec![b'M'; 4]); // [4, 8) later
        let got = content_after(&c, b"");
        assert_eq!(got, b"AAAAMMMMAAAA");
    }

    #[test]
    fn overlay_zero_fills_gaps() {
        let mut c = ChunkMap::new();
        c.insert(8, b"XY".to_vec());
        let got = content_after(&c, b"ab");
        assert_eq!(got, b"ab\0\0\0\0\0\0XY");
    }

    #[test]
    fn truncate_trims_straddling_chunk() {
        let mut c = ChunkMap::new();
        c.insert(0, vec![b'A'; 8192]); // [0, 8192)
        c.trim_truncate(4096);
        let got = content_after(&c, b"");
        assert_eq!(got.len(), 4096);
        assert!(got.iter().all(|&b| b == b'A'));
    }

    #[test]
    fn truncate_removes_chunks_fully_beyond_eof() {
        let mut c = ChunkMap::new();
        c.insert(0, b"keep".to_vec());
        c.insert(8192, vec![b'X'; 100]); // fully beyond 4096
        c.trim_truncate(4096);
        assert_eq!(content_after(&c, b""), b"keep");
    }

    #[test]
    fn truncate_keeps_chunks_fully_below_eof() {
        let mut c = ChunkMap::new();
        c.insert(0, b"AB".to_vec());
        c.insert(100, b"CD".to_vec());
        c.trim_truncate(4096);
        // after the truncate the committed base is 4096 bytes (zero-fill)
        let got = content_after(&c, &vec![0u8; 4096]);
        assert_eq!(got.len(), 4096);
        assert_eq!(&got[..2], b"AB");
        assert_eq!(&got[100..102], b"CD");
        assert!(got[2..100].iter().all(|&b| b == 0));
        assert!(got[102..].iter().all(|&b| b == 0));
    }

    #[test]
    fn truncate_to_zero_removes_everything() {
        let mut c = ChunkMap::new();
        c.insert(0, b"data".to_vec());
        c.insert(4096, b"tail".to_vec());
        c.trim_truncate(0);
        assert!(c.is_empty());
    }

    fn test_budget() -> std::sync::Arc<crate::staging::StagingBudget> {
        crate::staging::StagingBudget::headless(crate::staging::StagingLimits::default())
    }

    #[test]
    fn commit_success_with_new_writes_stays_uncommitted() {
        let b = test_budget();
        let mut s = PendingWrite::new("/f".into(), 77, 1, &b).unwrap();
        s.spool.write(0, b"old").unwrap();
        // the commit swaps the spool out; a write lands in the FRESH spool
        // while the commit is in flight
        let fresh = crate::staging::WriteSpool::open(&b).unwrap();
        let drained = std::mem::replace(&mut s.spool, fresh);
        s.spool.write(0, b"NEW").unwrap();
        let fully = s.note_commit_success(2);
        assert!(!fully, "new writes arrived during commit - not done");
        assert_eq!(s.expected_version, 2);
        assert!(!s.committed);
        // the next flush commits exactly the new batch
        assert_eq!(s.spool.dirty_end(), 3);
        let mut next_buf = [0u8; 3];
        s.spool.read_at(0, &mut next_buf).unwrap();
        assert_eq!(&next_buf, b"NEW");
        // the second commit uploads the fresh spool and resets it
        s.spool.reset().unwrap();
        assert!(s.note_commit_success(3));
        assert!(s.committed);
        drop(drained);
    }

    #[test]
    fn merge_under_lets_newer_writes_win() {
        let b = test_budget();
        let mut s = PendingWrite::new("/f".into(), 77, 1, &b).unwrap();
        s.spool.write(0, vec![b'O'; 8192].as_slice()).unwrap(); // old batch [0, 8192)
        let fresh = crate::staging::WriteSpool::open(&b).unwrap();
        let drained = std::mem::replace(&mut s.spool, fresh);
        // during the (failing) commit, a new write lands at [4096, 4196)
        s.spool.write(4096, vec![b'N'; 100].as_slice()).unwrap();
        s.spool.merge_under(drained).unwrap();
        let mut got = vec![0u8; 8192];
        s.spool.read_at(0, &mut got).unwrap();
        assert!(got[..4096].iter().all(|&b| b == b'O'));
        assert!(got[4096..4196].iter().all(|&b| b == b'N'), "window write must win");
        // bytes around the new write on both sides are restored old data
        assert_eq!(got[4095], b'O', "byte just before the new range");
        assert_eq!(got[4196], b'O', "byte just after the new range");
        assert_eq!(s.spool.dirty_end(), 8192);
    }

    #[test]
    fn merge_under_covers_tail_beyond_new_write() {
        let b = test_budget();
        let mut s = PendingWrite::new("/f".into(), 77, 1, &b).unwrap();
        s.spool.write(0, b"OLDOLDOLD").unwrap(); // [0, 9)
        let fresh = crate::staging::WriteSpool::open(&b).unwrap();
        let drained = std::mem::replace(&mut s.spool, fresh);
        s.spool.write(0, b"new").unwrap(); // [0, 3) newer
        s.spool.merge_under(drained).unwrap();
        let mut got = vec![0u8; 9];
        s.spool.read_at(0, &mut got).unwrap();
        assert_eq!(&got, b"newOLDOLD");
    }

    #[test]
    fn overlay_range_serves_window_and_partial_head() {
        let mut c = ChunkMap::new();
        c.insert(0, vec![b'A'; 10]); // [0, 10)
        c.insert(20, vec![b'B'; 10]); // [20, 30)
        c.insert(100, vec![b'C'; 5]); // [100, 105)
        // window fully inside a gap: untouched
        let mut buf = vec![0u8; 5];
        c.overlay_range(&mut buf, 12);
        assert_eq!(buf, vec![0u8; 5]);
        // window spanning chunk 1 and the gap+chunk 2
        let mut buf = vec![0u8; 25]; // [5, 30)
        c.overlay_range(&mut buf, 5);
        assert_eq!(&buf[..5], &[b'A'; 5]);
        assert_eq!(&buf[5..15], &[0u8; 10]);
        assert_eq!(&buf[15..25], &[b'B'; 10]);
        // window starting mid-chunk
        let mut buf = vec![0u8; 3];
        c.overlay_range(&mut buf, 23);
        assert_eq!(buf, vec![b'B', b'B', b'B']);
        // empty window: no-op
        let mut buf: Vec<u8> = Vec::new();
        c.overlay_range(&mut buf, 0);
        assert!(buf.is_empty());
    }

    #[test]
    fn pending_eof_semantics() {
        let p = PendingEof::new();
        assert_eq!(p.get("/f"), 0);
        p.advance("/f", 10);
        p.advance("/f", 4); // never lowers
        assert_eq!(p.get("/f"), 10);
        p.rebase("/f", 8); // never lowers
        assert_eq!(p.get("/f"), 10);
        p.recompute("/f", 12); // may lower/raise explicitly
        assert_eq!(p.get("/f"), 12);
        p.recompute("/f", 0); // removes
        assert_eq!(p.get("/f"), 0);
        p.advance("/f", 7);
        p.drop_path("/f");
        assert_eq!(p.get("/f"), 0);
    }

    #[test]
    fn max_end_tracks_last_chunk_end() {
        let mut c = ChunkMap::new();
        assert_eq!(c.max_end(), 0, "empty map");
        c.insert(0, vec![0; 10]); // [0, 10)
        assert_eq!(c.max_end(), 10);
        c.insert(20, vec![0; 5]); // [20, 25)
        assert_eq!(c.max_end(), 25);
        // same-offset replace must not change the end
        c.insert(20, vec![0; 3]); // [20, 23)
        assert_eq!(c.max_end(), 25);
        // hole in the middle does not matter: end is what append needs
        c.insert(5, vec![0; 40]); // [5, 45) splits [0,10), replaces [20,23)
        assert_eq!(c.max_end(), 45);
    }
}
