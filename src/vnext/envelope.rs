//! vnext object envelope (v1): the minimal self-describing wrapper for
//! every object vnext writes to the object store.
//!
//! Contract split (S4.19 architecture boundary): the CORE owns this
//! header — magic, format version, object type, logical length and the
//! integrity digest — and refuses anything it cannot verify. The ENCODING
//! fields (`enc`, `cipher`) are declared here but INTERPRETED by the
//! storage/encoding layer, not by the core: the core treats unknown
//! values as "not readable by me" (explicit refusal, never silent bytes).
//! Today only `enc=0` (raw bytes) and `cipher=0` (none) exist, so the
//! payload after the header IS the logical content; compression
//! (per-object zstd is the leading candidate) and authenticated
//! encryption slot into those fields without moving the core contract.
//!
//! Wire format (all integers little-endian):
//!
//! ```text
//! offset  size  field
//! 0       4     magic  "LF1\n"
//! 4       1     format version (1)
//! 5       1     object type (1 = data, 2 = tree, 3 = commit, 4 = ref)
//! 6       1     encoder id (0 = raw)
//! 7       1     cipher id   (0 = none; AEAD would also pin nonce/kid refs)
//! 8       4     reserved (zero)
//! 12      8     logical length (bytes the logical content occupies)
//! 20      32    sha256 of the LOGICAL content (integrity anchor)
//! 52      ...   payload (raw today)
//! ```
//!
//! A truncated header, unknown magic/version/type, or a digest mismatch
//! is a hard read error (`ObjectCorrupt`): damaged objects are reported,
//! never served.

use crate::error::{Error, Result};

pub const MAGIC: [u8; 4] = *b"LF1\n";
pub const FORMAT_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjKind {
    Data = 1,
    Tree = 2,
    Commit = 3,
    Ref = 4,
}

impl ObjKind {
    fn from_u8(v: u8) -> Option<ObjKind> {
        match v {
            1 => Some(ObjKind::Data),
            2 => Some(ObjKind::Tree),
            3 => Some(ObjKind::Commit),
            4 => Some(ObjKind::Ref),
            _ => None,
        }
    }
}

pub const ENC_RAW: u8 = 0;
pub const CIPHER_NONE: u8 = 0;

pub const HEADER_LEN: usize = 52;

/// Wrap logical content into the v1 envelope (raw encoding, no cipher).
pub fn seal(kind: ObjKind, logical: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_LEN + logical.len());
    out.extend_from_slice(&MAGIC);
    out.push(FORMAT_VERSION);
    out.push(kind as u8);
    out.push(ENC_RAW);
    out.push(CIPHER_NONE);
    out.extend_from_slice(&[0u8; 4]);
    out.extend_from_slice(&(logical.len() as u64).to_le_bytes());
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(logical);
    out.extend_from_slice(&digest);
    out.extend_from_slice(logical);
    out
}

/// Open an envelope: verify every field the core owns and return the
/// logical content. Any violation is `Error::ObjectCorrupt`.
pub fn open(bytes: &[u8]) -> Result<(ObjKind, Vec<u8>)> {
    if bytes.len() < HEADER_LEN {
        return Err(corrupt("truncated header", bytes.len()));
    }
    if bytes[0..4] != MAGIC {
        return Err(corrupt("bad magic", 0));
    }
    if bytes[4] != FORMAT_VERSION {
        return Err(corrupt(&format!("unknown format version {}", bytes[4]), 0));
    }
    let kind = ObjKind::from_u8(bytes[5])
        .ok_or_else(|| corrupt(&format!("unknown object type {}", bytes[5]), 0))?;
    if bytes[6] != ENC_RAW {
        return Err(corrupt(
            &format!("unknown encoder id {} (this build decodes raw only)", bytes[6]),
            0,
        ));
    }
    if bytes[7] != CIPHER_NONE {
        return Err(corrupt(
            &format!("unknown cipher id {} (this build decodes plaintext only)", bytes[7]),
            0,
        ));
    }
    let logical_len = u64::from_le_bytes(
        bytes[12..20].try_into().expect("header len field"),
    ) as usize;
    let payload = &bytes[HEADER_LEN..];
    if payload.len() != logical_len {
        return Err(corrupt(
            &format!("length mismatch: header says {logical_len}, payload is {}", payload.len()),
            logical_len,
        ));
    }
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(payload);
    if digest.as_slice() != &bytes[20..52] {
        return Err(corrupt("sha256 mismatch (content damaged)", logical_len));
    }
    Ok((kind, payload.to_vec()))
}

fn corrupt(why: &str, logical_len: usize) -> Error {
    Error::ObjectCorrupt(format!(
        "vnext object unreadable ({why}); refusing to serve possibly-wrong bytes \
         (logical length {logical_len})"
    ))
}

/// Validate ONLY the header fields (for range reads that will not see
/// the whole payload). Returns (kind, logical_len). Digest verification
/// for a partial read is delegated to the recorded etag comparison done
/// by the caller; full reads must use [`open`].
pub fn open_header(header: &[u8]) -> Result<(ObjKind, u64)> {
    if header.len() < HEADER_LEN {
        return Err(corrupt("truncated header", header.len()));
    }
    if header[0..4] != MAGIC {
        return Err(corrupt("bad magic", 0));
    }
    if header[4] != FORMAT_VERSION {
        return Err(corrupt(&format!("unknown format version {}", header[4]), 0));
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
    let logical_len = u64::from_le_bytes(header[12..20].try_into().expect("len field"));
    Ok((kind, logical_len))
}

/// Stream-seal content that is already readable twice (Read + Seek, e.g.
/// the server's spooled temp file): pass one computes the digest, pass
/// two writes header + payload to `out`. Bounded memory by construction.
/// Read + Seek as a single trait object (dyn composition limitation).
pub trait ReadSeek: std::io::Read + std::io::Seek {}
impl<T: std::io::Read + std::io::Seek> ReadSeek for T {}

pub fn seal_stream(
    kind: ObjKind,
    src: &mut dyn ReadSeek,
    len: u64,
    out: &mut dyn std::io::Write,
) -> std::io::Result<()> {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    let mut copied = 0u64;
    let mut buf = [0u8; 64 * 1024];
    src.rewind()?;
    while copied < len {
        let want = std::cmp::min(buf.len() as u64, len - copied) as usize;
        let got = src.read(&mut buf[..want])?;
        if got == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "source ended before the declared length",
            ));
        }
        digest.update(&buf[..got]);
        copied += got as u64;
    }
    src.rewind()?;
    out.write_all(&MAGIC)?;
    out.write_all(&[FORMAT_VERSION, kind as u8, ENC_RAW, CIPHER_NONE])?;
    out.write_all(&[0u8; 4])?;
    out.write_all(&len.to_le_bytes())?;
    out.write_all(&digest.finalize())?;
    let mut copied = 0u64;
    while copied < len {
        let want = std::cmp::min(buf.len() as u64, len - copied) as usize;
        let got = src.read(&mut buf[..want])?;
        if got == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "source ended on the second pass",
            ));
        }
        out.write_all(&buf[..got])?;
        copied += got as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seal_open_roundtrip() {
        let env = seal(ObjKind::Data, b"hello world");
        let (kind, content) = open(&env).unwrap();
        assert_eq!(kind, ObjKind::Data);
        assert_eq!(content, b"hello world");
    }

    #[test]
    fn empty_content_roundtrip() {
        let env = seal(ObjKind::Tree, b"");
        let (kind, content) = open(&env).unwrap();
        assert_eq!(kind, ObjKind::Tree);
        assert!(content.is_empty());
    }

    #[test]
    fn corrupted_byte_is_refused() {
        let mut env = seal(ObjKind::Data, b"payload");
        let last = env.len() - 1;
        env[last] ^= 0xff;
        let err = open(&env).unwrap_err();
        assert!(matches!(err, Error::ObjectCorrupt(_)), "{err}");
        assert!(format!("{err}").contains("sha256 mismatch"));
    }

    #[test]
    fn truncated_is_refused() {
        let env = seal(ObjKind::Data, b"payload");
        let err = open(&env[..30]).unwrap_err();
        assert!(matches!(err, Error::ObjectCorrupt(_)));
    }

    #[test]
    fn unknown_encoder_is_refused_not_served() {
        let mut env = seal(ObjKind::Data, b"payload");
        env[6] = 42; // unknown encoder
        let err = open(&env).unwrap_err();
        assert!(format!("{err}").contains("unknown encoder id 42"));
    }

    #[test]
    fn unknown_version_is_refused() {
        let mut env = seal(ObjKind::Data, b"payload");
        env[4] = 9;
        assert!(open(&env).is_err());
    }
}
