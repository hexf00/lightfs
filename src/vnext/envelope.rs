//! vnext object envelope (v2): the minimal self-describing wrapper for
//! every object vnext writes to the object store.
//!
//! Ownership split (S4.23 boundary): this module IS the object encoding
//! layer. It owns the wire format — magic, format version, object type,
//! encoder/cipher ids, checksum algorithm id, logical length, the
//! checksum scope and the footer position. Callers (core, salvage) never
//! do envelope byte arithmetic: they use [`encoded_len`], [`prefix_range`],
//! [`payload_range`] and [`payload_slice`]. Unknown magic, version,
//! encoder, cipher or checksum id is an explicit refusal (`Error::
//! ObjectCorrupt` naming the unsupported format) — never silent bytes.
//! Today only `enc=0` (raw) and `cipher=0` (none) exist, so the payload
//! IS the logical content; compression and AEAD slot into those ids
//! without moving this contract.
//!
//! Wire format (all integers little-endian):
//!
//! ```text
//! offset  size  field
//! 0       4     magic  "LF2\n"
//! 4       1     format version (2)
//! 5       1     object type (1 = data, 2 = tree, 3 = commit, 4 = ref,
//!                      5 = namespace marker)
//! 6       1     encoder id (0 = raw)
//! 7       1     cipher id   (0 = none)
//! 8       1     checksum algorithm id (1 = SHA-256)
//! 9       3     reserved (must be zero)
//! 12      8     payload length (bytes following the header; the only
//!                      length this format defines — see the vocabulary
//!                      note below)
//! 20      ...   payload (raw today)
//! end-32  32    FOOTER: checksum over header[0..20] || payload[0..len]
//! ```
//!
//! The v1 digest lived in the header, which forced two passes over the
//! source (hash, rewind, copy). v2 moves it to a footer so a single
//! streaming pass computes it in flight: the checksum is updated as
//! payload bytes flow through, and the footer is written once the last
//! payload byte has been seen. Total per-object overhead is unchanged
//! (20 + 32 = the old 52-byte header). The checksum covers the
//! CANONICAL HEADER (all 20 bytes, reserved included) plus the full
//! payload, so tampering with any metadata field is also detected on a
//! full read.
//!
//! Length vocabulary (S4.23 boundary, keep this distinction):
//! - LOGICAL FILE LENGTH: how long the application's file is. The
//!   envelope does NOT know it. It is metadata the caller owns (the
//!   core's entry size); it equals the payload length only because this
//!   build only writes `enc=0` (raw). A future trusted-client encoder
//!   (compression/AEAD before the RPC) makes the payload CIPHERTEXT the
//!   server never decodes: the logical length is then a property of that
//!   encoder's own format, never of this header.
//! - PAYLOAD LENGTH: the 8-byte header field — how many payload bytes
//!   follow the header. This is the ONLY length the envelope speaks, and
//!   the checksum's data scope is defined over it.
//! - PHYSICAL ENCODED LENGTH: `encoded_len()` = header + payload + footer;
//!   the on-the-wire object size, known before any upload starts.
//!   Under `enc=0` all three coincide; that identity belongs to the raw
//!   encoder id, NOT to this format.
//!
//! Checksum visibility: the footer is a digest over the bytes THE
//! ENCODING LAYER SEES (header || payload). In raw mode that is the file
//! content; under a future cipher id it is a ciphertext integrity value.
//! Nothing here claims a plaintext digest, and no key or decoder state
//! is assumed to exist server-side.
//!
//! Read guarantees by shape:
//! - FULL object read: header fields + length + footer checksum all
//!   verified ([`open`]). A damaged object is refused, never served.
//! - PARTIAL (range) read: header fields verified, payload bytes sliced
//!   out; the checksum is NOT verified (the footer was not fetched).
//!   Integrity for partial reads rests on the backend validator (etag)
//!   compared against the recorded value by the caller — the etag is the
//!   backend's object identity check, not a content hash.
//! - Objects written by a previous format (v1 `LF1\n`) are rejected with
//!   an explicit unsupported-format error. There is no legacy branch.

use crate::error::{Error, Result};
use sha2::{Digest, Sha256};
use std::io::Read;

pub const MAGIC: [u8; 4] = *b"LF2\n";
pub const FORMAT_VERSION: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjKind {
    Data = 1,
    /// Legacy (S4.19) metadata copies: this build never WRITES them, but
    /// still decodes the kind so leftovers are identified, never guessed.
    Tree = 2,
    Commit = 3,
    Ref = 4,
    /// One-per-bucket ownership marker (`namespace/marker`): written once
    /// at initialization, verified on every open. NOT part of the commit
    /// path (S4.20: no per-commit logical metadata goes to the store).
    Namespace = 5,
}

impl ObjKind {
    fn from_u8(v: u8) -> Option<ObjKind> {
        match v {
            1 => Some(ObjKind::Data),
            2 => Some(ObjKind::Tree),
            3 => Some(ObjKind::Commit),
            4 => Some(ObjKind::Ref),
            5 => Some(ObjKind::Namespace),
            _ => None,
        }
    }
}

pub const ENC_RAW: u8 = 0;
pub const CIPHER_NONE: u8 = 0;
/// Checksum id 1: SHA-256 over `header[0..20] || payload[0..logical_len]`.
pub const CHECKSUM_SHA256: u8 = 1;

pub const HEADER_LEN: usize = 20;
pub const FOOTER_LEN: usize = 32;

/// Physical object length for a payload length: header + payload + footer.
/// Known exactly before the upload starts. (Raw encoder: payload length ==
/// logical file length; the identity is the encoder's, not this function's.)
pub fn encoded_len(payload_len: u64) -> u64 {
    HEADER_LEN as u64 + payload_len + FOOTER_LEN as u64
}

/// Physical GET range for the payload window `[offset, offset+len)` —
/// offsets in PAYLOAD space. The caller's logical-to-payload mapping is
/// the identity here because the raw encoder defines it so; a future
/// encoder that breaks byte alignment owns that translation instead.
pub fn payload_range(offset: u64, len: u64) -> crate::object_store::Range {
    let start = HEADER_LEN as u64 + offset;
    crate::object_store::Range { start, len }
}

/// Physical GET range for a cold read: header and the first `payload_len`
/// payload bytes in ONE ranged request (cost contract: one GET per cold
/// read — the footer is not fetched and not needed).
pub fn prefix_range(payload_len: u64) -> crate::object_store::Range {
    crate::object_store::Range { start: 0, len: HEADER_LEN as u64 + payload_len }
}

/// The payload window of a fully fetched header+payload buffer.
pub fn payload_slice(obj: &[u8], want: usize) -> &[u8] {
    &obj[HEADER_LEN..HEADER_LEN + want]
}

/// Wrap logical content into the v2 envelope (raw encoding, no cipher).
/// Byte-for-byte identical to streaming [`seal_reader`] over the same
/// content (pinned by tests).
pub fn seal(kind: ObjKind, payload: &[u8]) -> Vec<u8> {
    let mut header = [0u8; HEADER_LEN];
    build_header(&mut header, kind, payload.len() as u64);
    let mut hasher = Sha256::new();
    hasher.update(header);
    hasher.update(payload);
    let mut out = Vec::with_capacity(encoded_len(payload.len() as u64) as usize);
    out.extend_from_slice(&header);
    out.extend_from_slice(payload);
    out.extend_from_slice(&hasher.finalize());
    out
}

fn build_header(header: &mut [u8; HEADER_LEN], kind: ObjKind, logical_len: u64) {
    header[0..4].copy_from_slice(&MAGIC);
    header[4] = FORMAT_VERSION;
    header[5] = kind as u8;
    header[6] = ENC_RAW;
    header[7] = CIPHER_NONE;
    header[8] = CHECKSUM_SHA256;
    // header[9..12] reserved: already zero
    header[12..20].copy_from_slice(&logical_len.to_le_bytes());
}

/// Open an envelope: verify every field and the footer checksum, return
/// the logical content. Any violation is `Error::ObjectCorrupt`.
pub fn open(bytes: &[u8]) -> Result<(ObjKind, Vec<u8>)> {
    if bytes.len() < HEADER_LEN + FOOTER_LEN {
        return Err(corrupt("truncated (shorter than header+footer)", bytes.len()));
    }
    let (kind, payload_len) = open_header(bytes)?;
    let payload_len = payload_len as usize;
    let expected = encoded_len(payload_len as u64) as usize;
    if bytes.len() != expected {
        return Err(corrupt(
            &format!(
                "physical length mismatch: header says {payload_len} payload bytes \
                 (object must be {expected} bytes), object is {}",
                bytes.len()
            ),
            payload_len,
        ));
    }
    let payload = &bytes[HEADER_LEN..HEADER_LEN + payload_len];
    let mut hasher = Sha256::new();
    hasher.update(&bytes[0..HEADER_LEN]);
    hasher.update(payload);
    if hasher.finalize().as_slice() != &bytes[HEADER_LEN + payload_len..] {
        return Err(corrupt("sha256 footer mismatch (object damaged)", payload_len));
    }
    Ok((kind, payload.to_vec()))
}

/// Validate ONLY the header fields (for range reads that do not fetch the
/// footer). Returns (kind, payload_len). Payload integrity for a partial
/// read rests on the caller's recorded-etag comparison; full reads must
/// use [`open`].
pub fn open_header(header: &[u8]) -> Result<(ObjKind, u64)> {
    if header.len() < HEADER_LEN {
        return Err(corrupt("truncated header", header.len()));
    }
    if header[0..4] == *b"LF1\n" {
        return Err(corrupt(
            "unsupported envelope format LF1 (pre-footer v1); this build reads v2 only",
            0,
        ));
    }
    if header[0..4] != MAGIC {
        return Err(corrupt("bad magic (not a vnext v2 object)", 0));
    }
    if header[4] != FORMAT_VERSION {
        return Err(corrupt(
            &format!("unsupported format version {} (this build reads v2 only)", header[4]),
            0,
        ));
    }
    let kind = ObjKind::from_u8(header[5])
        .ok_or_else(|| corrupt(&format!("unknown object type {}", header[5]), 0))?;
    if header[6] != ENC_RAW {
        return Err(corrupt(
            &format!("unknown encoder id {} (this build decodes raw only)", header[6]),
            0,
        ));
    }
    if header[7] != CIPHER_NONE {
        return Err(corrupt(
            &format!("unknown cipher id {} (this build decodes plaintext only)", header[7]),
            0,
        ));
    }
    if header[8] != CHECKSUM_SHA256 {
        return Err(corrupt(
            &format!("unknown checksum algorithm id {} (this build verifies sha256 only)", header[8]),
            0,
        ));
    }
    if header[9..12] != [0u8; 3] {
        return Err(corrupt("reserved header bytes not zero (not a canonical v2 object)", 0));
    }
    let logical_len = u64::from_le_bytes(header[12..20].try_into().expect("len field"));
    Ok((kind, logical_len))
}

/// Single-pass streaming encoder: a `Read` that yields
/// `header || payload || footer` over the source in bounded chunks,
/// updating the checksum as payload bytes flow through. The source is
/// read ONCE (never rewound), never buffered whole, and after the footer
/// the reader reports EOF (`Ok(0)`) on every subsequent call.
pub struct SealedReader<'a> {
    src: &'a mut dyn Read,
    header: [u8; HEADER_LEN],
    hpos: usize,
    payload_left: u64,
    hasher: Sha256,
    footer: [u8; FOOTER_LEN],
    fpos: usize,
    footer_ready: bool,
}

/// Stream-seal `payload_len` bytes from `src`. Raw encoder: the payload
/// is the logical content and the checksum covers header || payload.
pub fn seal_reader(kind: ObjKind, src: &mut dyn Read, payload_len: u64) -> SealedReader<'_> {
    let mut header = [0u8; HEADER_LEN];
    build_header(&mut header, kind, payload_len);
    let mut hasher = Sha256::new();
    hasher.update(header);
    SealedReader {
        src,
        header,
        hpos: 0,
        payload_left: payload_len,
        hasher,
        footer: [0u8; FOOTER_LEN],
        fpos: 0,
        footer_ready: false,
    }
}

impl Read for SealedReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.hpos < HEADER_LEN {
            let n = (HEADER_LEN - self.hpos).min(buf.len());
            buf[..n].copy_from_slice(&self.header[self.hpos..self.hpos + n]);
            self.hpos += n;
            return Ok(n);
        }
        if self.payload_left > 0 {
            let want = std::cmp::min(buf.len() as u64, self.payload_left) as usize;
            let got = self.src.read(&mut buf[..want])?;
            if got == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "source ended {} bytes before the declared payload length",
                        self.payload_left
                    ),
                ));
            }
            self.hasher.update(&buf[..got]);
            self.payload_left -= got as u64;
            return Ok(got);
        }
        if !self.footer_ready {
            self.footer = self.hasher.finalize_reset().into();
            self.footer_ready = true;
        }
        if self.fpos < FOOTER_LEN {
            let n = (FOOTER_LEN - self.fpos).min(buf.len());
            buf[..n].copy_from_slice(&self.footer[self.fpos..self.fpos + n]);
            self.fpos += n;
            return Ok(n);
        }
        // Declared length delivered: strict EOF, forever.
        Ok(0)
    }
}

fn corrupt(why: &str, logical_len: usize) -> Error {
    Error::ObjectCorrupt(format!(
        "vnext object unreadable ({why}); refusing to serve possibly-wrong bytes \
         (logical length {logical_len})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn stream_sealed(kind: ObjKind, data: &[u8], chunk: usize) -> Vec<u8> {
        let mut src = Cursor::new(data);
        let mut r = seal_reader(kind, &mut src, data.len() as u64);
        let mut out = Vec::new();
        let mut buf = vec![0u8; chunk];
        loop {
            let n = r.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        out
    }

    #[test]
    fn namespace_marker_roundtrip() {
        let sealed = seal(ObjKind::Namespace, b"0123456789abcdef0123456789abcdef");
        let (kind, body) = open(&sealed).unwrap();
        assert_eq!(kind, ObjKind::Namespace);
        assert_eq!(body, b"0123456789abcdef0123456789abcdef");
        // a tampered payload byte is refused like any other damage
        let mut bad = sealed.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        assert!(open(&bad).is_err());
    }

    #[test]
    fn v2_layout_and_total_overhead_matches_v1() {
        let sealed = seal(ObjKind::Data, b"hello");
        assert_eq!(sealed.len() as u64, encoded_len(5));
        assert_eq!(sealed.len(), 20 + 5 + 32, "52-byte overhead, same as v1");
        assert_eq!(&sealed[0..4], b"LF2\n");
        assert_eq!(sealed[4], 2);
        assert_eq!(sealed[5], ObjKind::Data as u8);
        assert_eq!(sealed[6], ENC_RAW);
        assert_eq!(sealed[7], CIPHER_NONE);
        assert_eq!(sealed[8], CHECKSUM_SHA256);
        assert_eq!(&sealed[9..12], &[0u8; 3]);
        assert_eq!(&sealed[12..20], &5u64.to_le_bytes());
        assert_eq!(&sealed[20..25], b"hello");
        // footer = sha256(header || payload), recomputed independently
        let mut h = Sha256::new();
        h.update(&sealed[0..20]);
        h.update(b"hello");
        assert_eq!(&sealed[25..], &h.finalize()[..]);
    }

    #[test]
    fn stream_output_is_byte_identical_to_seal() {
        for (kind, data) in [
            (ObjKind::Data, &b""[..]),
            (ObjKind::Data, b"x"),
            (ObjKind::Data, &[0u8; 8 * 1024][..]),
            (ObjKind::Data, &[0u8; 100 * 1024][..]), // crosses the 64 KiB chunk
            (ObjKind::Namespace, b"0123456789abcdef0123456789abcdef"),
        ] {
            assert_eq!(stream_sealed(kind, data, 64 * 1024), seal(kind, data));
            // tiny caller buffers exercise the short-read passthrough
            assert_eq!(stream_sealed(kind, data, 1), seal(kind, data));
            assert_eq!(stream_sealed(kind, data, 7), seal(kind, data));
        }
    }

    #[test]
    fn eof_semantics_and_empty_file() {
        let mut src = Cursor::new(b"");
        let mut r = seal_reader(ObjKind::Data, &mut src, 0);
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out.len(), HEADER_LEN + FOOTER_LEN);
        // after EOF: every further read is Ok(0), never a repeated footer
        let mut buf = [0u8; 1];
        assert_eq!(r.read(&mut buf).unwrap(), 0);
        assert_eq!(r.read(&mut buf).unwrap(), 0);
        assert_eq!(r.read(&mut buf).unwrap(), 0);
        let (kind, body) = open(&out).unwrap();
        assert_eq!(kind, ObjKind::Data);
        assert!(body.is_empty());
    }

    #[test]
    fn one_byte_at_a_time_source_is_accepted() {
        // Short reads from the source are legal; only a premature EOF is not.
        struct Trickle<'a>(&'a [u8]);
        impl Read for Trickle<'_> {
            fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
                if self.0.is_empty() || buf.is_empty() {
                    return Ok(0);
                }
                buf[0] = self.0[0];
                self.0 = &self.0[1..];
                Ok(1)
            }
        }
        let data = b"streaming works";
        let mut src = Trickle(data);
        let mut r = seal_reader(ObjKind::Data, &mut src, data.len() as u64);
        let mut out = Vec::new();
        r.read_to_end(&mut out).unwrap();
        assert_eq!(out, seal(ObjKind::Data, data));
    }

    #[test]
    fn premature_source_end_is_an_error_not_silent_truncation() {
        let mut src = Cursor::new(b"only five");
        let mut r = seal_reader(ObjKind::Data, &mut src, 100);
        let mut out = Vec::new();
        let err = r.read_to_end(&mut out).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn source_io_error_propagates() {
        struct Boom;
        impl Read for Boom {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::other("disk vanished"))
            }
        }
        let mut src = Boom;
        let mut r = seal_reader(ObjKind::Data, &mut src, 10);
        let mut buf = [0u8; 64]; // past the header, into the payload
        r.read_exact(&mut buf[..HEADER_LEN]).unwrap();
        assert!(r.read(&mut buf).is_err());
    }

    #[test]
    fn corruption_matrix_is_fail_closed() {
        let sealed = seal(ObjKind::Data, &[42u8; 1000]);
        let flip = |i: usize| {
            let mut b = sealed.clone();
            b[i] ^= 0xff;
            open(&b).err().map(|e| e.to_string()).unwrap_or_default()
        };
        // payload damage, footer damage, header field damage: all refused
        assert!(flip(30).contains("sha256 footer mismatch") || flip(30).contains("unreadable"));
        let mut bad_footer = sealed.clone();
        *bad_footer.last_mut().unwrap() ^= 0xff;
        assert!(open(&bad_footer).unwrap_err().to_string().contains("sha256 footer mismatch"));
        let mut bad_len = sealed.clone();
        bad_len[12] ^= 0xff;
        assert!(open(&bad_len).unwrap_err().to_string().contains("length mismatch"));
        let mut bad_kind = sealed.clone();
        bad_kind[5] = 99;
        assert!(open(&bad_kind).unwrap_err().to_string().contains("unknown object type"));
        let mut bad_enc = sealed.clone();
        bad_enc[6] = 3;
        assert!(open(&bad_enc).unwrap_err().to_string().contains("encoder"));
        let mut bad_cipher = sealed.clone();
        bad_cipher[7] = 1;
        assert!(open(&bad_cipher).unwrap_err().to_string().contains("cipher"));
        let mut bad_algo = sealed.clone();
        bad_algo[8] = 7;
        assert!(open(&bad_algo).unwrap_err().to_string().contains("checksum algorithm"));
        let mut bad_reserved = sealed.clone();
        bad_reserved[9] = 1;
        assert!(open(&bad_reserved).unwrap_err().to_string().contains("reserved"));
        // truncations: payload, footer, header
        assert!(open(&sealed[..sealed.len() - 1]).is_err());
        assert!(open(&sealed[..HEADER_LEN + 10]).is_err());
        assert!(open(&sealed[..10]).is_err());
        // trailing junk beyond the declared length
        let mut extra = sealed.clone();
        extra.push(0);
        assert!(open(&extra).unwrap_err().to_string().contains("length mismatch"));
        // v1 objects are named and refused, never parsed as content
        let mut v1 = seal(ObjKind::Data, b"x");
        v1[0..4].copy_from_slice(b"LF1\n");
        let msg = open(&v1).unwrap_err().to_string();
        assert!(msg.contains("unsupported envelope format LF1"), "{msg}");
        // unknown format version on the v2 magic
        let mut ver = sealed.clone();
        ver[4] = 9;
        assert!(open(&ver).unwrap_err().to_string().contains("unsupported format version 9"));
        // unknown magic
        let mut junk = sealed.clone();
        junk[0..4].copy_from_slice(&[0, 1, 2, 3]);
        assert!(open(&junk).unwrap_err().to_string().contains("bad magic"));
        // open_header agrees on a valid prefix and refuses a truncated one
        assert_eq!(open_header(&sealed).unwrap(), (ObjKind::Data, 1000));
        assert!(open_header(&sealed[..10]).is_err());
    }

    #[test]
    fn range_helpers_pin_the_layout() {
        use crate::object_store::Range;
        assert_eq!(encoded_len(0), 52);
        assert_eq!(encoded_len(1000), 1052);
        assert_eq!(prefix_range(300), Range { start: 0, len: 320 });
        assert_eq!(payload_range(40, 10), Range { start: 60, len: 10 });
        let sealed = seal(ObjKind::Data, &[7u8; 100]);
        assert_eq!(payload_slice(&sealed, 100), &[7u8; 100]);
    }
}
