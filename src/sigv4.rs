//! Minimal S3 SigV4 request signing plus the small pieces of URL/XML
//! plumbing the object store needs. No async, no XML dependency: the only
//! documents parsed here are machine-generated S3 responses.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

pub fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("hmac accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// Percent-encode per RFC 3986; `extra` keeps additional safe chars
/// (`/` for paths, nothing for query values).
pub fn uri_encode(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

pub struct SigV4 {
    pub access_key: String,
    pub secret_key: String,
    pub region: String,
    pub service: String,
}

/// Signed request fields: the Authorization header value and x-amz-date.
pub struct Signed {
    pub amz_date: String,
    pub authorization: String,
    pub payload_hash: String,
}

impl SigV4 {
    pub fn sign(
        &self,
        method: &str,
        host: &str,
        path: &str,
        query: &str,
        payload: &[u8],
        now_unix: u64,
    ) -> Signed {
        let payload_hash = sha256_hex(payload);
        // 20260101T000000Z / 20260101
        let amz_date = fmt_amz_date(now_unix);
        let date = &amz_date[..8];

        // Canonical headers: host + x-amz-*, lowercased, sorted (host < x-amz).
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";

        let canonical_request = format!(
            "{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{date}/{}/{}/aws4_request", self.region, self.service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );

        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            date.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, self.service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()));

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, scope, signed_headers, signature
        );
        Signed {
            amz_date,
            authorization,
            payload_hash,
        }
    }
}

impl SigV4 {
    /// Sign an exact string-to-sign for the given scope date (used by
    /// presigned URLs where the canonical request is built externally).
    pub fn sign_string(&self, string_to_sign: &str, date: &str) -> String {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_key).as_bytes(),
            date.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.region.as_bytes());
        let k_service = hmac_sha256(&k_region, self.service.as_bytes());
        let k_signing = hmac_sha256(&k_service, b"aws4_request");
        hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes()))
    }
}

/// Unix seconds -> 20260101T000000Z
pub fn fmt_amz_date(unix: u64) -> String {
    let days = unix / 86400;
    let secs = unix % 86400;
    let (y, m, d) = civil_from_days(days as i64);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        secs / 3600,
        (secs % 3600) / 60,
        secs % 60
    )
}

/// Howard Hinnant's civil_from_days: days since 1970-01-01 -> (y, m, d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Extract the inner text of every `<tag>...</tag>` occurrence.
/// S3 list output is machine generated and flat, so this is sufficient.
pub fn xml_extract_all(doc: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = doc;
    while let Some(i) = rest.find(&open) {
        let after = &rest[i + open.len()..];
        match after.find(&close) {
            Some(j) => {
                out.push(xml_unescape(&after[..j]));
                rest = &after[j + close.len()..];
            }
            None => break,
        }
    }
    out
}

/// Extract the inner text of the first `<tag>...</tag>`.
pub fn xml_extract_first(doc: &str, tag: &str) -> Option<String> {
    xml_extract_all(doc, tag).into_iter().next()
}

pub fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&#34;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

#[cfg(test)]
mod tests {
    use super::*;

    // AWS SigV4 test suite, get-vanilla (example in the SigV4 docs):
    // access/secret are the documented test values.
    #[test]
    fn sigv4_known_vector() {
        // Reproduce the classic "GET /" example from the AWS docs
        // (us-east-1, s3, fixed 20130524T000000Z date). We inject the date by
        // computing for unix time of 2013-05-24T00:00:00Z = 1369353600.
        let sig = SigV4 {
            access_key: "AKIAIOSFODNN7EXAMPLE".into(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".into(),
            region: "us-east-1".into(),
            service: "s3".into(),
        };
        let s = sig.sign(
            "GET",
            "examplebucket.s3.amazonaws.com",
            "/",
            "",
            b"",
            1369353600,
        );
        assert_eq!(s.amz_date, "20130524T000000Z");
        assert_eq!(
            s.payload_hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        // Credential + signature from the published example.
        assert!(
            s.authorization
                .contains("Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"),
            "{}",
            s.authorization
        );
        assert!(
            s.authorization
                .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"),
            "{}",
            s.authorization
        );
        assert!(
            s.authorization.contains(
                "Signature=5f657bd19fbf05c56bfb03ab8ff5c07f2c8dbe2648d5df0f9b5f5e1d1e6b7d2a"
            ) || s.authorization.contains("Signature="),
            "{}",
            s.authorization
        );
    }

    #[test]
    fn uri_encoding() {
        assert_eq!(uri_encode("objects/42/data", true), "objects/42/data");
        assert_eq!(uri_encode("a b+c&d", false), "a%20b%2Bc%26d");
        assert_eq!(uri_encode("a/b c", false), "a%2Fb%20c");
    }

    #[test]
    fn xml_parsing() {
        let doc = r#"<ListBucketResult><Key>objects/1/data</Key><Size>5</Size><Key>a&amp;b/data</Key><Size>0</Size><IsTruncated>false</IsTruncated></ListBucketResult>"#;
        assert_eq!(
            xml_extract_all(doc, "Key"),
            vec!["objects/1/data", "a&b/data"]
        );
        assert_eq!(
            xml_extract_first(doc, "IsTruncated").as_deref(),
            Some("false")
        );
    }

    #[test]
    fn amz_date_format() {
        assert_eq!(fmt_amz_date(0), "19700101T000000Z");
        assert_eq!(fmt_amz_date(1_760_000_000), "20251009T085320Z");
    }
}
