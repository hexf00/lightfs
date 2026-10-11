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
pub struct PendingWrite {
    pub path: String,
    /// Version captured when write intent started; the CAS anchor.
    pub expected_version: i64,
    pub chunks: ChunkMap,
    /// True once a commit succeeded AND no newer chunks arrived after it.
    pub committed: bool,
}

impl PendingWrite {
    pub fn new(path: String, expected_version: i64) -> Self {
        Self {
            path,
            expected_version,
            chunks: ChunkMap::new(),
            committed: false,
        }
    }

    /// Record a successful commit. Returns true when the session is fully
    /// committed; false when NEW chunks arrived during the commit window -
    /// those must be committed by the next flush (the version is re-based
    /// here, so that next commit's CAS succeeds).
    pub fn note_commit_success(&mut self, new_version: i64) -> bool {
        self.expected_version = new_version;
        if self.chunks.is_empty() {
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

    #[test]
    fn commit_success_with_new_chunks_stays_uncommitted() {
        let mut s = PendingWrite::new("/f".into(), 1);
        s.chunks.insert(0, b"old".to_vec());
        let drained = s.chunks.drain();
        // a write lands while the commit is in flight
        s.chunks.insert(0, b"NEW".to_vec());
        let fully = s.note_commit_success(2);
        assert!(!fully, "new chunks arrived during commit - not done");
        assert_eq!(s.expected_version, 2);
        assert!(!s.committed);
        // the next flush commits exactly the new batch
        let next = s.chunks.drain();
        assert_eq!(next.get(&0).unwrap(), b"NEW");
        assert!(s.note_commit_success(3));
        assert!(s.committed);
        let _ = drained;
    }

    #[test]
    fn restore_under_lets_newer_writes_win() {
        let mut s = PendingWrite::new("/f".into(), 1);
        s.chunks.insert(0, vec![b'O'; 8192]); // old batch [0, 8192)
        let drained = s.chunks.drain();
        // during the (failing) commit, a new write lands at [4096, 4196)
        s.chunks.insert(4096, vec![b'N'; 100]);
        s.chunks.restore_under(&drained);
        let got = content_after(&s.chunks, b"");
        assert_eq!(got.len(), 8192);
        assert!(got[..4096].iter().all(|&b| b == b'O'));
        assert!(got[4096..4196].iter().all(|&b| b == b'N'));
        // bytes around the new write on both sides are restored old data
        assert_eq!(got[4095], b'O', "byte just before the new range");
        assert_eq!(got[4196], b'O', "byte just after the new range");
    }

    #[test]
    fn restore_under_covers_tail_beyond_new_write() {
        let mut s = PendingWrite::new("/f".into(), 1);
        s.chunks.insert(0, b"OLDOLDOLD".to_vec()); // [0, 9)
        let drained = s.chunks.drain();
        s.chunks.insert(0, b"new".to_vec()); // [0, 3) newer
        s.chunks.restore_under(&drained);
        let got = content_after(&s.chunks, b"");
        assert_eq!(got, b"newOLDOLD");
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
