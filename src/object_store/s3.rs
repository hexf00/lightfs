//! S3-compatible ObjectStore (MinIO, AWS S3, Alibaba OSS S3 endpoints).
//! Blocking REST + SigV4 via ureq; path-style addressing.

use std::io::Read;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{Error, Result};
use crate::object_store::{ListPage, ObjectData, ObjectInfo, ObjectStore, Range, range_to_header};
use crate::sigv4::{SigV4, uri_encode, xml_extract_all, xml_extract_first};
use quick_xml::events::Event;

#[derive(Debug, Clone)]
pub struct S3Config {
    pub endpoint: String, // http://127.0.0.1:9000
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    pub path_style: bool,
    /// SigV4 credential-scope service name: "s3" for AWS/MinIO, "oss" for
    /// Aliyun OSS. Empty means "s3".
    pub service: String,
}

pub struct S3ObjectStore {
    cfg: S3Config,
    signer: SigV4,
    agent: ureq::Agent,
    /// Host used for SigV4 signing (virtual-host style includes the bucket).
    host: String,
    base_path: String, // "" for virtual-host style, "/bucket" for path style
    /// Scheme + host every request URL starts with (virtual-host style has
    /// the bucket in the host).
    url_base: String,
}

fn agent() -> ureq::Agent {
    static AGENT: OnceLock<ureq::Agent> = OnceLock::new();
    AGENT
        .get_or_init(|| {
            ureq::Agent::config_builder()
                .timeout_global(Some(std::time::Duration::from_secs(300)))
                .http_status_as_error(false)
                .build()
                .new_agent()
        })
        .clone()
}

/// Process-wide permit pool bounding TOTAL in-flight multipart buffers
/// across all concurrent uploads (each permit = one 8 MiB part buffer).
/// Default 8 parts ≈ 64 MiB; LIGHTFS_S3_MULTIPART_MAX_PARTS overrides.
#[derive(Clone)]
struct PartBudget {
    capacity: usize,
    state: Arc<Mutex<usize>>,
    cv: Arc<std::sync::Condvar>,
}

fn part_budget() -> PartBudget {
    static BUDGET: OnceLock<PartBudget> = OnceLock::new();
    BUDGET
        .get_or_init(|| {
            let capacity = std::env::var("LIGHTFS_S3_MULTIPART_MAX_PARTS")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|&n| (1..=256).contains(&n))
                .unwrap_or(8);
            PartBudget {
                capacity,
                state: Arc::new(Mutex::new(capacity)),
                cv: Arc::new(std::sync::Condvar::new()),
            }
        })
        .clone()
}

/// Objects at or below this size take the plain single-PUT path instead of
/// the multipart lifecycle. Aligned with the 8 MiB part size: the small
/// buffer then never exceeds one multipart part buffer, so taking one
/// permit from the same budget keeps the aggregate memory bound identical.
const SMALL_PUT_MAX: u64 = 8 * 1024 * 1024;

/// `len` is known up front for every put_stream call (the caller commits a
/// complete object), so the branch is exact - no probing, no fallback.
fn small_object(len: u64) -> bool {
    len <= SMALL_PUT_MAX
}

impl PartBudget {
    /// Block until `n` permits are free, then take them. `n` must not exceed
    /// the total budget (callers cap batch size to the budget first).
    /// `wait_secs = 0` waits forever; otherwise a stuck-holder situation
    /// degrades to an explicit error instead of an indefinitely blocked
    /// server worker (permits are RAII, so holders release on their own
    /// timeouts/errors).
    fn acquire_timeout(&self, n: usize, wait_secs: u64) -> std::result::Result<PermitGuard, ()> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(wait_secs);
        let mut avail = self.state.lock().unwrap();
        while *avail < n {
            if wait_secs == 0 {
                avail = self.cv.wait(avail).unwrap();
                continue;
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                return Err(());
            }
            let (guard, timeout) = self.cv.wait_timeout(avail, deadline - now).unwrap();
            avail = guard;
            if timeout.timed_out() && std::time::Instant::now() >= deadline {
                return Err(());
            }
        }
        *avail -= n;
        Some(PermitGuard {
            budget: self.clone(),
            n,
        })
        .ok_or(())
    }
    fn release(&self, n: usize) {
        *self.state.lock().unwrap() += n;
        self.cv.notify_all();
    }
    fn total(&self) -> usize {
        self.capacity
    }
}

/// RAII release: permits come back even if an upload errors mid-batch.
struct PermitGuard {
    budget: PartBudget,
    n: usize,
}
impl Drop for PermitGuard {
    fn drop(&mut self) {
        self.budget.release(self.n);
    }
}

impl S3ObjectStore {
    pub fn new(cfg: S3Config) -> Result<Self> {
        let (host, _scheme) = parse_endpoint(&cfg.endpoint)?;
        let scheme = cfg
            .endpoint
            .split_once("://")
            .map(|(s, _)| s.to_string())
            .unwrap_or_else(|| "https".into());
        // Virtual-host style: the bucket moves into the HOST (URL and SigV4
        // Host header), the path carries only the key. Path style keeps the
        // bucket as the first path segment. Before this fix the virtual-host
        // branch silently dropped the bucket from the URL entirely (first
        // surfaced by real Aliyun OSS: every request 404'd).
        let (host, url_base, base_path) = if cfg.path_style {
            (
                host.clone(),
                cfg.endpoint.trim_end_matches('/').to_string(),
                format!("/{}", cfg.bucket),
            )
        } else {
            let vhost = format!("{}.{}", cfg.bucket, host);
            (vhost.clone(), format!("{scheme}://{vhost}"), String::new())
        };
        Ok(Self {
            signer: SigV4 {
                access_key: cfg.access_key.clone(),
                secret_key: cfg.secret_key.clone(),
                region: cfg.region.clone(),
                service: if cfg.service.is_empty() {
                    "s3".into()
                } else {
                    cfg.service.clone()
                },
            },
            host,
            base_path,
            url_base,
            agent: agent(),
            cfg,
        })
    }

    fn bucket_url(&self, key: Option<&str>, query: &str) -> (String, String) {
        // returns (url, canonical_path_with_query)
        let mut key_path = match key {
            Some(k) => format!("{}/{}", self.base_path, uri_encode(k, true)),
            None => self.base_path.clone(),
        };
        // Bucket-level ops (virtual-host style) have an empty path; S3
        // canonicalizes that to "/" and the request URL needs it too.
        if key_path.is_empty() {
            key_path = "/".to_string();
        }
        let q = if query.is_empty() {
            String::new()
        } else {
            format!("?{query}")
        };
        let url = format!("{}{key_path}{q}", self.url_base);
        (url, format!("{key_path}{q}"))
    }

    fn signed(
        &self,
        method: &str,
        key: Option<&str>,
        query: &str,
        payload: &[u8],
    ) -> (String, Vec<(&'static str, String)>) {
        let (url, canonical) = self.bucket_url(key, query);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let s = self.signer.sign(
            method,
            &self.host,
            canonical.split('?').next().unwrap(),
            canonical.split_once('?').map(|(_, q)| q).unwrap_or(""),
            payload,
            now,
        );
        let headers = vec![
            ("x-amz-date", s.amz_date),
            ("x-amz-content-sha256", s.payload_hash),
            ("Authorization", s.authorization),
        ];
        (url, headers)
    }

    /// Make the bucket anonymously readable AND writable (Principal `*`,
    /// s3:GetObject + s3:PutObject). Used ONLY by the performance-comparison
    /// harness (throwaway bucket: the proxy uploads without credentials).
    /// Never part of the product path — the name keeps the danger visible.
    pub fn ensure_public_read(&self) -> Result<()> {
        let policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                // PutObject is included ONLY for the comparison harness
                // (minimal proxy upload experiment); never enable --public
                // on a bucket that holds real data.
                "Action": ["s3:GetObject", "s3:PutObject"],
                "Resource": [format!("arn:aws:s3:::{}/*", self.cfg.bucket)]
            }]
        })
        .to_string();
        // Valueless query param: canonical form requires the '=' (same rule
        // as multipart's uploads=).
        let (url, headers) = self.signed("PUT", None, "policy=", policy.as_bytes());
        let mut resp = self
            .apply_headers(self.agent.put(&url), &headers)
            .header("Content-Type", "application/json")
            .send(policy.as_bytes())
            .map_err(map_transport)?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = Self::read_body(&mut resp);
            return Err(Error::Storage(format!(
                "set bucket policy: {status}: {}",
                truncate(&body, 200)
            )));
        }
        Ok(())
    }

    /// Presigned GET URL (SigV4 query auth): lets a holder fetch the object
    /// straight from S3 with NO credentials and NO signed headers — the
    /// Phase 10 data-plane bypass primitive. Evaluation PoC; not yet wired
    /// into the client protocol.
    pub fn presign_get_url(&self, key: &str, expires_secs: u64) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let amz_date = crate::sigv4::fmt_amz_date(now);
        let date = &amz_date[..8];
        let scope = format!(
            "{date}/{}/{}/aws4_request",
            self.signer.region, self.signer.service
        );
        let credential = uri_encode(&format!("{}/{}", self.signer.access_key, scope), false);
        // canonical query must be sorted; UNSIGNED-PAYLOAD keeps the URL
        // usable from any plain HTTP client (curl, browser, ureq w/o auth).
        let query = format!(
            "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential={credential}&X-Amz-Date={amz_date}&X-Amz-Expires={expires_secs}&X-Amz-SignedHeaders=host"
        );
        let path = format!("{}/{}", self.base_path, uri_encode(key, true));
        // The blank line matters: canonical headers block ends with \n\n
        // before the signed-headers list (same layout as sign()).
        let canonical_request = format!(
            "GET\n{path}\n{query}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
            self.host
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            crate::sigv4::sha256_hex(canonical_request.as_bytes())
        );
        // Reuse the signer's key derivation by signing the string-to-sign
        // through a synthetic SigV4 sign over the exact date in scope.
        let signature = self.signer.sign_string(&string_to_sign, date);
        let endpoint = self.cfg.endpoint.trim_end_matches('/');
        Ok(format!(
            "{endpoint}{path}?{query}&X-Amz-Signature={signature}"
        ))
    }

    fn map_status(status: u16, body: &str, key: &str) -> Error {
        if status == 404 || body.contains("NoSuchKey") || body.contains("NoSuchBucket") {
            Error::ObjectMissing(key.to_string())
        } else {
            Error::Storage(format!("s3 {status} on {key}: {}", truncate(body, 300)))
        }
    }

    fn apply_headers<B>(
        &self,
        builder: ureq::RequestBuilder<B>,
        headers: &[(&'static str, String)],
    ) -> ureq::RequestBuilder<B> {
        let mut b = builder;
        for (k, v) in headers {
            b = b.header(*k, v.as_str());
        }
        b
    }

    fn multipart_init(&self, key: &str) -> Result<String> {
        // SigV4 canonicalizes a valueless query param as "key=" — the
        // signature must be computed over that form.
        let (url, headers) = self.signed("POST", Some(key), "uploads=", b"");
        let mut resp = self
            .apply_headers(self.agent.post(&url), &headers)
            .send(&b""[..])
            .map_err(map_transport)?;
        let status = resp.status().as_u16();
        let body = Self::read_body(&mut resp);
        if !(200..300).contains(&status) {
            return Err(Self::map_status(status, &body, key));
        }
        xml_extract_first(&body, "UploadId")
            .ok_or_else(|| Error::Storage("multipart init: no UploadId".into()))
    }

    fn multipart_part(
        &self,
        key: &str,
        upload_id: &str,
        number: usize,
        data: &[u8],
    ) -> Result<String> {
        let query = format!(
            "partNumber={number}&uploadId={}",
            uri_encode(upload_id, false)
        );
        // Same part number + same upload id + same bytes on retry:
        // idempotent per the S3 multipart protocol.
        with_retry(|| {
            let (url, headers) = self.signed("PUT", Some(key), &query, data);
            static PART_INFLIGHT: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let lvl = PART_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            crate::metrics::observe(
                "s3.put.inflight",
                std::time::Duration::from_micros(lvl as u64),
            );
            let r = (|| {
                let mut resp = self
                    .apply_headers(self.agent.put(&url), &headers)
                    .send(data)
                    .map_err(map_transport)?;
                let status = resp.status().as_u16();
                if !(200..300).contains(&status) {
                    let body = Self::read_body(&mut resp);
                    return Err(Self::map_status(status, &body, key));
                }
                header_str(&resp, "etag").ok_or_else(|| Error::Storage("part missing ETag".into()))
            })();
            PART_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            r
        })
    }

    fn multipart_complete(
        &self,
        key: &str,
        upload_id: &str,
        parts: &[(usize, String)],
    ) -> Result<String> {
        let query = format!("uploadId={}", uri_encode(upload_id, false));
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in parts {
            xml.push_str(&format!(
                "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>"
            ));
        }
        xml.push_str("</CompleteMultipartUpload>");
        let (url, headers) = self.signed("POST", Some(key), &query, xml.as_bytes());
        let mut resp = self
            .apply_headers(self.agent.post(&url), &headers)
            .send(xml.as_bytes())
            .map_err(map_transport)?;
        let status = resp.status().as_u16();
        let body = Self::read_body(&mut resp);
        if !(200..300).contains(&status) {
            return Err(Self::map_status(status, &body, key));
        }
        parse_multipart_complete(&body)
    }

    fn multipart_abort(&self, key: &str, upload_id: &str) -> Result<()> {
        let query = format!("uploadId={}", uri_encode(upload_id, false));
        let (url, _) = self.signed("DELETE", Some(key), &query, b"");
        let mut resp = self.agent.delete(&url).call().map_err(map_transport)?;
        let status = resp.status().as_u16();
        if !(200..300).contains(&status) {
            let body = Self::read_body(&mut resp);
            return Err(Self::map_status(status, &body, key));
        }
        Ok(())
    }

    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<(Box<dyn Read + Send>, u64)> {
        let (url, headers) = self.signed("GET", Some(key), "", b"");
        let mut builder = self.apply_headers(self.agent.get(&url), &headers);
        if let Some(r) = range {
            builder = builder.header("Range", range_to_header(r));
        }
        let mut resp = builder.call().map_err(map_transport)?;
        let status = resp.status().as_u16();
        if status == 404 {
            return Err(Error::ObjectMissing(key.to_string()));
        }
        if !(200..300).contains(&status) {
            let error_body = Self::read_body(&mut resp);
            return Err(Self::map_status(status, &error_body, key));
        }
        // The reader must yield EXACTLY the bytes this length describes:
        // 206 -> the requested slice (end-start+1 from Content-Range);
        // 200 with a Range header sent -> the server ignored the range and
        // would stream the whole object, which would silently corrupt the
        // caller's framing — fail loudly instead.
        let len: u64 = if status == 206 {
            let cr = header_str(&resp, "content-range")
                .ok_or_else(|| Error::Storage(format!("s3 206 without Content-Range on {key}")))?;
            parse_content_range_len(&cr).ok_or_else(|| {
                Error::Storage(format!("s3 unusable Content-Range '{cr}' on {key}"))
            })?
        } else if range.is_some() {
            return Err(Error::Storage(format!(
                "s3 ignored Range request on {key} (returned full object)"
            )));
        } else {
            header_str(&resp, "content-length")
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| {
                    Error::Storage(format!("s3 response without Content-Length on {key}"))
                })?
        };
        Ok((Box::new(resp.into_body().into_reader()), len))
    }

    fn read_body(resp: &mut ureq::http::Response<ureq::Body>) -> String {
        resp.body_mut()
            .with_config()
            .limit(MAX_RESPONSE_BODY)
            .read_to_string()
            .unwrap_or_default()
    }
}

/// Validate a CompleteMultipartUpload response body and return the object
/// ETag. S3 does not guarantee that every failure is a non-2xx status: a
/// complete may answer HTTP 200 with an `<Error>` document in the body. On
/// `Ok` the caller commits a metadata pointer to this object, so this fails
/// CLOSED on structural grounds (quick-xml parse): the document must be
/// well-formed XML with exactly ONE root named `CompleteMultipartUploadResult`
/// (namespace prefix or default xmlns tolerated), nothing but whitespace
/// outside that root, and a direct child `<ETag>` element with non-empty
/// text. Embedded `<Error>`/`<Code>` elements, error roots, comments, CDATA,
/// truncation, mismatched or multiple roots, and missing/empty ETags are all
/// errors. A result-unknown complete is never retried here; cleanup (abort
/// the upload, remove the uncommitted version key) stays with the caller's
/// existing error path.
fn parse_multipart_complete(body: &str) -> Result<String> {
    const ROOT: &str = "CompleteMultipartUploadResult";
    #[derive(PartialEq)]
    enum Stage {
        /// Before the root element: only the XML declaration, comments and
        /// whitespace are tolerated.
        Prolog,
        /// Inside the success root; `depth` counts open child elements.
        InRoot,
        /// Inside an `<Error>` root: capture `<Code>` for the error message.
        InError,
        /// Root closed: only comments and whitespace until EOF.
        Epilog,
    }
    let malformed = |what: String| Error::Storage(format!("multipart complete response: {what}"));
    let mut stage = Stage::Prolog;
    let mut depth = 0usize;
    let mut in_etag = false;
    let mut etag = String::new();
    let mut in_code = false;
    let mut code = String::new();

    // quick-xml 0.41 reports entity references (`&quot;`, `&#34;`) as their
    // own GeneralRef events, so text absorption is shared between Text and
    // resolved GeneralRef events.
    fn absorb(
        stage: &Stage,
        depth: usize,
        in_etag: bool,
        in_code: bool,
        etag: &mut String,
        code: &mut String,
        s: &str,
    ) -> Result<()> {
        match stage {
            Stage::Prolog | Stage::Epilog => {
                if !s.trim().is_empty() {
                    return Err(Error::Storage(
                        "multipart complete response: has content outside the root element".into(),
                    ));
                }
            }
            Stage::InRoot => {
                if in_etag && depth == 1 {
                    etag.push_str(s);
                } else if depth == 0 && !s.trim().is_empty() {
                    return Err(Error::Storage(
                        "multipart complete response: has stray text directly inside the root element"
                            .into(),
                    ));
                }
            }
            Stage::InError => {
                if in_code && depth == 1 {
                    code.push_str(s);
                }
            }
        }
        Ok(())
    }

    let mut reader = quick_xml::Reader::from_str(body);
    loop {
        let ev = reader
            .read_event()
            .map_err(|e| malformed(format!("is not well-formed XML: {e}")))?;
        match ev {
            Event::Start(bs) => {
                let name = element_local_name(&bs);
                match stage {
                    Stage::Prolog => {
                        if name == ROOT {
                            stage = Stage::InRoot;
                        } else if name == "Error" {
                            stage = Stage::InError;
                        } else {
                            return Err(malformed(format!("root is <{name}>, not <{ROOT}>")));
                        }
                    }
                    Stage::InRoot => {
                        depth += 1;
                        if name == "Error" || name == "Code" {
                            return Err(malformed("embeds an <Error>/<Code> element".into()));
                        }
                        if depth == 1 && name == "ETag" {
                            in_etag = true;
                        }
                    }
                    Stage::InError => {
                        depth += 1;
                        if depth == 1 && name == "Code" {
                            in_code = true;
                        }
                    }
                    Stage::Epilog => {
                        return Err(malformed("has content after the root element".into()));
                    }
                }
            }
            Event::Empty(bs) => {
                let name = element_local_name(&bs);
                match stage {
                    Stage::Prolog => {
                        if name == ROOT {
                            // Self-closing root: no ETag child can exist.
                            stage = Stage::Epilog;
                        } else if name == "Error" {
                            return Err(malformed(
                                "store rejected multipart complete: <Error/>".into(),
                            ));
                        } else {
                            return Err(malformed(format!("root is <{name}>, not <{ROOT}>")));
                        }
                    }
                    Stage::InRoot => {
                        if name == "Error" || name == "Code" {
                            return Err(malformed("embeds an <Error>/<Code> element".into()));
                        }
                        // A self-closing <ETag/> has empty content: nothing
                        // to absorb, the final non-empty check fails it.
                    }
                    Stage::InError => {}
                    Stage::Epilog => {
                        return Err(malformed("has content after the root element".into()));
                    }
                }
            }
            Event::End(_) => match stage {
                Stage::InRoot => {
                    if in_etag && depth == 1 {
                        in_etag = false;
                    }
                    if depth == 0 {
                        stage = Stage::Epilog;
                    } else {
                        depth -= 1;
                    }
                }
                Stage::InError => {
                    if in_code && depth == 1 {
                        in_code = false;
                    }
                    if depth == 0 {
                        return Err(malformed(format!(
                            "store rejected multipart complete: {}",
                            if code.trim().is_empty() {
                                "<Error> without a Code".to_string()
                            } else {
                                code.trim().to_string()
                            }
                        )));
                    }
                    depth -= 1;
                }
                _ => return Err(malformed("is not well-formed XML".into())),
            },
            Event::Text(t) => {
                let s = t
                    .decode()
                    .map_err(|e| malformed(format!("has undecodable text: {e}")))?;
                absorb(&stage, depth, in_etag, in_code, &mut etag, &mut code, &s)?;
            }
            Event::GeneralRef(r) => {
                let raw = r.into_inner();
                let name = std::str::from_utf8(raw.as_ref())
                    .map_err(|_| malformed("has a malformed entity reference".into()))?;
                let resolved = resolve_xml_entity(name).ok_or_else(|| {
                    malformed(format!("has an undefined entity reference &{name};"))
                })?;
                absorb(
                    &stage, depth, in_etag, in_code, &mut etag, &mut code, &resolved,
                )?;
            }
            // Comments are legal XML and are never read: an ETag inside a
            // comment simply does not exist as a field.
            Event::Comment(_) => {}
            Event::Decl(_) => {
                if stage != Stage::Prolog {
                    return Err(malformed("has an XML declaration after the root".into()));
                }
            }
            Event::CData(_) => return Err(malformed("has unexpected CDATA".into())),
            Event::PI(_) | Event::DocType(_) => {
                return Err(malformed("has unexpected document content".into()));
            }
            Event::Eof => match stage {
                Stage::Epilog => {
                    let etag = etag.trim();
                    if etag.is_empty() {
                        return Err(malformed(
                            "is a complete result without a non-empty ETag".into(),
                        ));
                    }
                    return Ok(etag.to_string());
                }
                Stage::InRoot | Stage::InError => {
                    return Err(malformed("is truncated (root never closed)".into()));
                }
                Stage::Prolog => {
                    return Err(malformed(
                        "does not contain a CompleteMultipartUploadResult document".into(),
                    ));
                }
            },
        }
    }
}

/// Local name of an element event, namespace prefix stripped. The input is
/// UTF-8 by construction (`Reader::from_str`), so the lossy conversion never
/// actually loses.
fn element_local_name(bs: &quick_xml::events::BytesStart<'_>) -> String {
    String::from_utf8_lossy(bs.name().local_name().into_inner()).into_owned()
}

/// Resolve an XML entity reference body (`quot`, `#34`, `#x22`) to its
/// content. Predefined entities via quick-xml; numeric character references
/// decoded here. Undefined names return None -> caller fails closed.
fn resolve_xml_entity(name: &str) -> Option<String> {
    if let Some(rest) = name.strip_prefix('#') {
        let n = if let Some(hex) = rest.strip_prefix('x').or_else(|| rest.strip_prefix('X')) {
            u32::from_str_radix(hex, 16).ok()?
        } else {
            rest.parse::<u32>().ok()?
        };
        char::from_u32(n).map(|c| c.to_string())
    } else {
        quick_xml::escape::resolve_predefined_entity(name).map(str::to_string)
    }
}

impl ObjectStore for S3ObjectStore {
    fn put(&self, key: &str, data: &[u8]) -> Result<ObjectInfo> {
        let _g = crate::metrics::scope("s3.put");
        // Same key + same content on retry: idempotent.
        with_retry(|| {
            let (url, headers) = self.signed("PUT", Some(key), "", data);
            let mut resp = self
                .apply_headers(self.agent.put(&url), &headers)
                .header("Content-Type", "application/octet-stream")
                .send(data)
                .map_err(map_transport)?;
            let status = resp.status().as_u16();
            if !(200..300).contains(&status) {
                let body = Self::read_body(&mut resp);
                return Err(Self::map_status(status, &body, key));
            }
            let etag = header_str(&resp, "etag").unwrap_or_default();
            Ok(ObjectInfo {
                key: key.to_string(),
                size: data.len() as u64,
                etag,
                version_id: header_str(&resp, "x-amz-version-id"),
            })
        })
    }

    fn get(&self, key: &str, range: Option<Range>) -> Result<ObjectData> {
        let _g = crate::metrics::scope("s3.get");
        // GET is read-only: retrying cannot change server state.
        with_retry(|| {
            let (url, headers) = self.signed("GET", Some(key), "", b"");
            let mut builder = self.apply_headers(self.agent.get(&url), &headers);
            if let Some(r) = range {
                builder = builder.header("Range", range_to_header(r));
            }
            let mut resp = builder.call().map_err(map_transport)?;
            let status = resp.status().as_u16();
            if status == 404 {
                return Err(Error::ObjectMissing(key.to_string()));
            }
            if !(200..300).contains(&status) {
                let body = Self::read_body(&mut resp);
                return Err(Self::map_status(status, &body, key));
            }
            let etag = header_str(&resp, "etag").unwrap_or_default();
            let full_len: Option<u64> = match header_str(&resp, "content-range") {
                Some(cr) => cr.rsplit('/').next().and_then(|s| s.parse().ok()),
                None => header_str(&resp, "content-length").and_then(|s| s.parse().ok()),
            };
            let buf = resp
                .body_mut()
                .with_config()
                .limit(MAX_RESPONSE_BODY)
                .read_to_vec()
                .map_err(map_transport)?;
            let offset = match range {
                Some(r) => {
                    if status == 206 {
                        r.start
                    } else {
                        0 // server ignored the range
                    }
                }
                None => 0,
            };
            Ok(ObjectData {
                info: ObjectInfo {
                    key: key.to_string(),
                    size: full_len.unwrap_or(buf.len() as u64),
                    etag,
                    version_id: None,
                },
                offset,
                data: buf,
            })
        })
    }

    fn get_stream(&self, key: &str, range: Option<Range>) -> Result<(Box<dyn Read + Send>, u64)> {
        let _g = crate::metrics::scope("s3.get_stream");
        S3ObjectStore::get_stream(self, key, range)
    }

    fn put_stream(&self, key: &str, reader: &mut dyn Read, len: u64) -> Result<ObjectInfo> {
        let _g = crate::metrics::scope("s3.put_stream");
        // Small objects: one plain Content-Length PUT instead of the full
        // multipart lifecycle (init + part + complete = three sequential
        // round trips per commit). E4-A measured that lifecycle at ~7.8ms
        // per 2 KiB object against a 0.92ms raw single-PUT floor on the
        // same MinIO, so every small-file commit paid it three times over.
        // The buffer is exactly `len` bytes and holds one part-budget
        // permit for the upload, matching how multipart buffers hold
        // theirs - aggregate memory bound unchanged. CAS-failure cleanup
        // and immutability semantics live in the caller (core write paths)
        // and are untouched: this only changes HOW the object is uploaded.
        if small_object(len) {
            let wait_secs = std::env::var("LIGHTFS_S3_PART_WAIT_SECS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(30);
            // Diagnostic (Phase F): how long callers queue on the part budget
            // (contention signal), and how many S3 PUTs are in flight. Both
            // sampled via metrics::observe - no-op unless LIGHTFS_METRICS=1.
            let wait0 = std::time::Instant::now();
            let permit = part_budget().acquire_timeout(1, wait_secs).map_err(|_| {
                Error::Storage(format!(
                    "multipart buffer budget busy: no permit within {wait_secs}s (a slow upload may be holding parts)"
                ))
            })?;
            crate::metrics::observe("s3.part.wait", wait0.elapsed());
            static PUT_INFLIGHT: std::sync::atomic::AtomicUsize =
                std::sync::atomic::AtomicUsize::new(0);
            let lvl = PUT_INFLIGHT.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            crate::metrics::observe(
                "s3.put.inflight",
                std::time::Duration::from_micros(lvl as u64),
            );
            let mut buf = vec![0u8; len as usize];
            reader.read_exact(&mut buf)?;
            let out = self.put(key, &buf);
            PUT_INFLIGHT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
            drop(permit); // free only after the upload, like multipart buffers
            return out;
        }
        // S3 multipart upload: every part is an ordinary Content-Length'd
        // PUT, so bytes stream in bounded 8 MiB buffers (plain chunked
        // unsigned PUTs are rejected by S3/MinIO with 411).
        //
        // Parts upload with bounded concurrency (LIGHTFS_S3_PART_CONCURRENCY,
        // default 1 = serial): the single reader fills up to N buffers, then
        // N uploads race; memory stays bounded at N x 8 MiB. Part numbers
        // follow fill order, so complete() stays ascending. Any part failure
        // aborts the whole upload (cleanup below).
        const PART: usize = 8 * 1024 * 1024;
        let concurrency = std::env::var("LIGHTFS_S3_PART_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|&n| (1..=16).contains(&n))
            .unwrap_or(1);
        // Cap batch size to the global budget so acquire() cannot deadlock.
        let budget = part_budget();
        let budget_total = budget.total();
        let wait_secs = std::env::var("LIGHTFS_S3_PART_WAIT_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(30);
        let upload_id = self
            .multipart_init(key)
            .map_err(|e| Error::Storage(format!("multipart init: {e}")))?;
        let mut parts: Vec<(usize, String)> = Vec::new();
        let mut n = 0usize;
        let result = (|| -> Result<String> {
            loop {
                // Acquire permits BEFORE allocating buffers: blocking here
                // backs pressure up to the client instead of growing memory.
                let batch = concurrency.min(budget_total);
                let wait0 = std::time::Instant::now();
                let permit = budget.acquire_timeout(batch, wait_secs).map_err(|_| {
                    Error::Storage(format!(
                        "multipart buffer budget busy: no permit within {wait_secs}s (a slow upload may be holding parts)"
                    ))
                })?;
                crate::metrics::observe("s3.part.wait", wait0.elapsed());
                // Fill up to `batch` part buffers from the single reader.
                let mut bufs: Vec<Vec<u8>> = Vec::with_capacity(batch);
                let mut last = false;
                for _ in 0..batch {
                    let mut buf = vec![0u8; PART];
                    let mut filled = 0usize;
                    while filled < PART {
                        let got = reader.read(&mut buf[filled..])?;
                        if got == 0 {
                            break;
                        }
                        filled += got;
                    }
                    if filled == 0 {
                        break;
                    }
                    buf.truncate(filled);
                    if filled < PART {
                        last = true;
                    }
                    bufs.push(buf);
                    if last {
                        break;
                    }
                }
                if bufs.is_empty() && n > 0 {
                    break; // clean end after at least one part
                }
                if bufs.is_empty() {
                    bufs.push(Vec::new()); // zero-length object: one empty part
                }
                // Upload this batch in parallel; part numbers follow fill order.
                let first = n + 1;
                let etags: Vec<std::result::Result<String, Error>> = std::thread::scope(|s| {
                    let mut handles = Vec::new();
                    for (idx, buf) in bufs.iter().enumerate() {
                        let upload_id = upload_id.as_str();
                        handles.push(s.spawn(move || {
                            self.multipart_part(key, upload_id, first + idx, buf.as_slice())
                                .map_err(|e| {
                                    Error::Storage(format!("multipart part {}: {e}", first + idx))
                                })
                        }));
                    }
                    handles.into_iter().map(|h| h.join().unwrap()).collect()
                });
                for (idx, res) in etags.into_iter().enumerate() {
                    n += 1;
                    parts.push((first + idx, res?));
                }
                // Upload buffers are freed when the threads join; the RAII
                // guard releases the permits at scope end (after uploads).
                drop(permit);
                if last {
                    break;
                }
            }
            self.multipart_complete(key, &upload_id, &parts)
                .map_err(|e| Error::Storage(format!("multipart complete: {e}")))
        })();
        match result {
            Ok(etag) => Ok(ObjectInfo {
                key: key.to_string(),
                size: len,
                etag,
                version_id: None,
            }),
            Err(e) => {
                if let Err(abort_err) = self.multipart_abort(key, &upload_id) {
                    // An orphaned in-progress multipart upload keeps holding
                    // bucket space — make the failed cleanup observable.
                    eprintln!(
                        "lightfs: WARNING multipart abort failed for {key} upload {upload_id}: {abort_err}"
                    );
                }
                Err(e)
            }
        }
    }

    fn presign_get(&self, key: &str, expires_secs: u64) -> Option<String> {
        self.presign_get_url(key, expires_secs).ok()
    }

    fn head(&self, key: &str) -> Result<ObjectInfo> {
        let _g = crate::metrics::scope("s3.head");
        with_retry(|| {
            let (url, headers) = self.signed("HEAD", Some(key), "", b"");
            let resp = self
                .apply_headers(self.agent.head(&url), &headers)
                .call()
                .map_err(map_transport)?;
            let status = resp.status().as_u16();
            if status == 404 {
                return Err(Error::ObjectMissing(key.to_string()));
            }
            if !(200..300).contains(&status) {
                return Err(Error::Storage(format!("s3 HEAD {status} on {key}")));
            }
            Ok(ObjectInfo {
                key: key.to_string(),
                size: header_str(&resp, "content-length")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
                etag: header_str(&resp, "etag").unwrap_or_default(),
                version_id: header_str(&resp, "x-amz-version-id"),
            })
        })
    }

    fn delete(&self, key: &str) -> Result<()> {
        let _g = crate::metrics::scope("s3.delete");
        // S3 DELETE is idempotent by spec (204 whether or not the key
        // existed), so a retried delete cannot resurrect or duplicate.
        with_retry(|| {
            let (url, headers) = self.signed("DELETE", Some(key), "", b"");
            let mut resp = self
                .apply_headers(self.agent.delete(&url), &headers)
                .call()
                .map_err(map_transport)?;
            let status = resp.status().as_u16();
            if !(200..300).contains(&status) {
                let body = Self::read_body(&mut resp);
                return Err(Self::map_status(status, &body, key));
            }
            Ok(())
        })
    }

    fn list(&self, prefix: &str, token: Option<&str>, max: usize) -> Result<ListPage> {
        let max = max.clamp(1, 1000);
        let mut q: Vec<(String, String)> = vec![
            ("list-type".into(), "2".into()),
            ("prefix".into(), prefix.into()),
            ("max-keys".into(), max.to_string()),
        ];
        if let Some(t) = token {
            q.push(("continuation-token".into(), t.into()));
        }
        q.sort();
        let query = encode_query(&q);
        let (url, headers) = self.signed("GET", None, &query, b"");
        let mut resp = self
            .apply_headers(self.agent.get(&url), &headers)
            .call()
            .map_err(map_transport)?;
        let status = resp.status().as_u16();
        let body = Self::read_body(&mut resp);
        if !(200..300).contains(&status) {
            return Err(Self::map_status(status, &body, prefix));
        }
        let keys = xml_extract_all(&body, "Key");
        let sizes: Vec<String> = xml_extract_all(&body, "Size");
        let etags: Vec<String> = xml_extract_all(&body, "ETag");
        let items = keys
            .iter()
            .enumerate()
            .map(|(i, k)| ObjectInfo {
                key: k.clone(),
                size: sizes
                    .get(i)
                    .and_then(|s: &String| s.parse().ok())
                    .unwrap_or(0),
                etag: etags.get(i).cloned().unwrap_or_default(),
                version_id: None,
            })
            .collect();
        let truncated = xml_extract_first(&body, "IsTruncated").as_deref() == Some("true");
        let next_token = if truncated {
            xml_extract_first(&body, "NextContinuationToken")
        } else {
            None
        };
        Ok(ListPage { items, next_token })
    }

    fn ping(&self) -> Result<()> {
        // head the bucket
        let (url, headers) = self.signed("HEAD", None, "", b"");
        let resp = self
            .apply_headers(self.agent.head(&url), &headers)
            .call()
            .map_err(map_transport)?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            Ok(())
        } else if status == 404 {
            Err(Error::Storage(format!(
                "bucket {} missing",
                self.cfg.bucket
            )))
        } else {
            // HEAD has no body; repeat as a signed GET on the bucket to
            // fetch the error XML (SignatureDoesNotMatch vs AccessDenied
            // vs InvalidAccessKeyId is exactly what you want to see here).
            let (gurl, gheaders) = self.signed("GET", None, "", b"");
            let body = match self.apply_headers(self.agent.get(&gurl), &gheaders).call() {
                // http_status_as_error(false): error statuses arrive as Ok
                Ok(mut r) => Self::read_body(&mut r),
                Err(e) => format!("(transport: {e})"),
            };
            Err(Error::Storage(format!(
                "s3 ping {status} url={gurl}: {}",
                truncate(&body, 500)
            )))
        }
    }
}

impl S3ObjectStore {
    /// Create the bucket if missing (setup for scripts/tests).
    pub fn ensure_bucket(&self) -> Result<()> {
        let (url, headers) = self.signed("PUT", None, "", b"");
        let mut resp = self
            .apply_headers(self.agent.put(&url), &headers)
            .send(b"" as &[u8])
            .map_err(map_transport)?;
        let status = resp.status().as_u16();
        if (200..300).contains(&status) {
            return Ok(());
        }
        let body = Self::read_body(&mut resp);
        if body.contains("BucketAlreadyOwnedByYou") || body.contains("BucketAlreadyExists") {
            return Ok(());
        }
        Err(Error::Storage(format!(
            "create bucket: {status}: {}",
            truncate(&body, 300)
        )))
    }
}

/// ureq defaults to a 10 MiB response-body cap, which silently made any
/// object larger than that unreadable; lift it to a bounded 1 GiB.
const MAX_RESPONSE_BODY: u64 = 1024 * 1024 * 1024;

/// S3 servers return 503 (SlowDown) when overloaded — the S3 spec requires
/// clients to retry such errors with exponential backoff. Bounded: 3 extra
/// attempts at 100/200/400ms.
///
/// Scope: only IDEMPOTENT operations are wrapped — head, get, put,
/// delete, and multipart part upload (same key/part/bytes on retry).
/// NOT wrapped on purpose: multipart init/complete/abort and bucket
/// policy (retrying complete after a lost success response would fail
/// with NoSuchUpload, and callers cannot distinguish that from the
/// original failure).
fn with_retry<T>(mut op: impl FnMut() -> Result<T>) -> Result<T> {
    let mut last: Option<Error> = None;
    for attempt in 0..4 {
        match op() {
            Ok(v) => return Ok(v),
            Err(e) => {
                let retryable =
                    matches!(&e, Error::Storage(m) if m.contains("503") || m.contains("SlowDown"));
                if !retryable || attempt == 3 {
                    return Err(e);
                }
                std::thread::sleep(std::time::Duration::from_millis(100 << attempt));
                last = Some(e);
            }
        }
    }
    Err(last.expect("retry loop ran at least once"))
}

fn map_transport(e: ureq::Error) -> Error {
    Error::Storage(format!("s3 transport: {e}"))
}

fn header_str(resp: &ureq::http::Response<ureq::Body>, name: &str) -> Option<String> {
    resp.headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn truncate(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

fn encode_query(q: &[(String, String)]) -> String {
    q.iter()
        .map(|(k, v)| format!("{}={}", uri_encode(k, false), uri_encode(v, false)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Parse "bytes START-END/TOTAL" (or unsatisfiable "bytes */TOTAL") and
/// return the number of bytes a 206 response body will actually contain:
/// END - START + 1. This is the stream length, NOT the object total.
fn parse_content_range_len(header: &str) -> Option<u64> {
    let spec = header.strip_prefix("bytes ")?;
    let range_part = spec.split('/').next()?;
    let (start_s, end_s) = range_part.split_once('-')?;
    if start_s.trim() == "*" {
        return None; // unsatisfiable range
    }
    let start: u64 = start_s.trim().parse().ok()?;
    let end: u64 = end_s.trim().parse().ok()?;
    end.checked_sub(start)?.checked_add(1) // inclusive
}

fn parse_endpoint(endpoint: &str) -> Result<(String, String)> {
    let (scheme, rest) = endpoint
        .split_once("://")
        .ok_or_else(|| Error::Config(format!("s3 endpoint needs scheme: {endpoint}")))?;
    let host = rest.split('/').next().unwrap_or_default().to_string();
    if host.is_empty() {
        return Err(Error::Config(format!(
            "s3 endpoint missing host: {endpoint}"
        )));
    }
    Ok((host, scheme.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_parse() {
        assert_eq!(
            parse_endpoint("http://127.0.0.1:9000").unwrap().0,
            "127.0.0.1:9000"
        );
        assert_eq!(
            parse_endpoint("https://oss-cn.aliyuncs.com/").unwrap().0,
            "oss-cn.aliyuncs.com"
        );
        assert!(parse_endpoint("no-scheme").is_err());
    }

    fn cfg_for(endpoint: &str, path_style: bool) -> S3Config {
        S3Config {
            endpoint: endpoint.into(),
            region: "oss-cn-shenzhen".into(),
            bucket: "mybkt".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            path_style,
            service: String::new(),
        }
    }

    #[test]
    fn virtual_host_addressing_puts_bucket_in_host() {
        // Regression: virtual-host style used to drop the bucket from the
        // URL entirely (every request hit the endpoint root -> 404). Found
        // by the first real-Aliyun-OSS run.
        let s = S3ObjectStore::new(cfg_for("https://oss-cn-shenzhen.aliyuncs.com", false)).unwrap();
        let (url, canonical) = s.bucket_url(Some("objects/1/w-a.data"), "");
        assert_eq!(
            url,
            "https://mybkt.oss-cn-shenzhen.aliyuncs.com/objects/1/w-a.data"
        );
        assert_eq!(canonical, "/objects/1/w-a.data");
        assert_eq!(s.host, "mybkt.oss-cn-shenzhen.aliyuncs.com");
        let (root, root_canon) = s.bucket_url(None, "");
        assert_eq!(root, "https://mybkt.oss-cn-shenzhen.aliyuncs.com/");
        assert_eq!(root_canon, "/");
        // SigV4 scope service defaults to s3 even when the config leaves it
        // empty; OSS setups set service = "oss" explicitly.
        assert_eq!(s.signer.service, "s3");
        let oss = S3ObjectStore::new({
            let mut c = cfg_for("https://oss-cn-shenzhen.aliyuncs.com", false);
            c.service = "oss".into();
            c
        })
        .unwrap();
        assert_eq!(oss.signer.service, "oss");
    }

    #[test]
    fn path_style_addressing_keeps_bucket_in_path() {
        let s = S3ObjectStore::new(cfg_for("https://s3.example.com", true)).unwrap();
        let (url, canonical) = s.bucket_url(Some("k"), "");
        assert_eq!(url, "https://s3.example.com/mybkt/k");
        assert_eq!(canonical, "/mybkt/k");
        assert_eq!(s.host, "s3.example.com");
    }

    #[test]
    fn content_range_len_parsing() {
        // The stream length is END-START+1, NOT the total after '/'.
        assert_eq!(parse_content_range_len("bytes 100-199/1000"), Some(100));
        assert_eq!(parse_content_range_len("bytes 0-0/5"), Some(1));
        assert_eq!(parse_content_range_len("bytes 0-4/100"), Some(5));
        assert_eq!(parse_content_range_len("bytes 95-99/100"), Some(5));
        assert_eq!(parse_content_range_len("bytes */100"), None);
        assert_eq!(parse_content_range_len("nonsense"), None);
    }

    #[test]
    fn with_retry_retries_slowdown_then_succeeds() {
        let mut calls = 0usize;
        let r: Result<u32> = with_retry(|| {
            calls += 1;
            if calls < 3 {
                Err(Error::Storage(
                    "s3 PUT 503 SlowDown, reduce request rate".into(),
                ))
            } else {
                Ok(7)
            }
        });
        assert_eq!(r.unwrap(), 7);
        assert_eq!(calls, 3);
    }

    #[test]
    fn with_retry_gives_up_after_backoff_bound() {
        let mut calls = 0usize;
        let r: Result<()> = with_retry(|| {
            calls += 1;
            Err::<(), _>(Error::Storage("s3 GET 503".into()))
        });
        assert!(r.is_err());
        assert_eq!(calls, 4); // initial attempt + 3 bounded retries
    }

    #[test]
    fn with_retry_does_not_retry_non_retryable() {
        let mut calls = 0usize;
        let r: Result<()> = with_retry(|| {
            calls += 1;
            Err::<(), _>(Error::Storage("s3 PUT 403 Forbidden".into()))
        });
        assert!(r.is_err());
        assert_eq!(calls, 1);
    }

    /// Single-PUT / multipart split: zero-byte and exactly-at-threshold
    /// objects take the plain PUT, anything larger streams via multipart.
    #[test]
    fn small_object_boundary() {
        assert!(small_object(0));
        assert!(small_object(2048));
        assert!(small_object(8 * 1024 * 1024));
        assert!(!small_object(8 * 1024 * 1024 + 1));
        assert!(!small_object(u64::MAX));
    }

    /// Budget semaphore: capacity, blocking, timeout, RAII release.
    #[test]
    fn part_budget_semantics() {
        let b = PartBudget {
            capacity: 2,
            state: Arc::new(Mutex::new(2)),
            cv: Arc::new(std::sync::Condvar::new()),
        };
        let g1 = b.acquire_timeout(1, 1).unwrap();
        let g2 = b.acquire_timeout(1, 1).unwrap();
        // budget full: finite wait times out with an error
        assert!(b.acquire_timeout(1, 1).is_err());
        drop(g1);
        let g3 = b.acquire_timeout(1, 1).unwrap();
        // capacity can never be exceeded (2 permits requested, capacity 2 with 2 held)
        assert!(b.acquire_timeout(2, 1).is_err());
        drop(g2);
        drop(g3);
        // after all guards drop, full capacity is available again
        assert!(b.acquire_timeout(2, 1).is_ok());
    }

    #[test]
    fn query_encoding_sorted() {
        let q = vec![
            ("max-keys".to_string(), "3".to_string()),
            ("prefix".to_string(), "a b".to_string()),
            ("list-type".to_string(), "2".to_string()),
        ];
        assert_eq!(encode_query(&q), "max-keys=3&prefix=a%20b&list-type=2");
    }

    // ------------------------------------------------------ S1a: complete
    // A CompleteMultipartUpload may answer HTTP 200 with an <Error> document.
    // The parser must fail closed: only a well-formed success document with a
    // non-empty ETag may become the Ok the metadata CAS commits on.

    fn complete_success_body() -> String {
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
         <Location>http://mybkt.s3.amazonaws.com/objects/1/w-1.data</Location>\
         <Bucket>mybkt</Bucket><Key>objects/1/w-1.data</Key>\
         <ETag>\"abc123\"</ETag>\
         </CompleteMultipartUploadResult>"
            .to_string()
    }

    #[test]
    fn complete_success_document_with_etag_is_ok() {
        // The xmlns attribute on the root must not defeat recognition
        // (real AWS/MinIO success bodies carry it).
        let etag = parse_multipart_complete(&complete_success_body()).unwrap();
        assert_eq!(etag, "\"abc123\"");
        // Same without the XML declaration and without xmlns.
        let bare = "<CompleteMultipartUploadResult><ETag>\"x\"</ETag>\
                    </CompleteMultipartUploadResult>";
        assert_eq!(parse_multipart_complete(bare).unwrap(), "\"x\"");
    }

    #[test]
    fn complete_200_embedded_error_is_an_error() {
        // Documented S3 behavior: complete failures can arrive under HTTP 200.
        let body = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error>\
                    <Code>InternalError</Code>\
                    <Message>We encountered an internal error. Please try again.</Message>\
                    <Resource>/mybkt/objects/1/w-1.data</Resource>\
                    <RequestId>req-1</RequestId></Error>";
        let err = parse_multipart_complete(body).unwrap_err();
        assert!(err.to_string().contains("InternalError"), "{err}");
    }

    #[test]
    fn complete_200_malformed_or_foreign_body_is_an_error() {
        // Gateway HTML / junk where XML was expected.
        assert!(parse_multipart_complete("<html><body>502</body></html>").is_err());
        // Empty body (read failure surfaces as "" via read_body).
        assert!(parse_multipart_complete("").is_err());
        // Random truncation garbage.
        assert!(parse_multipart_complete("<CompleteMultipartUploadRes").is_err());
    }

    #[test]
    fn complete_200_truncated_success_is_an_error() {
        let full = complete_success_body();
        // Cut before the root close tag, even though the ETag survived —
        // the response as a whole did not complete.
        let cut = full.find("</CompleteMultipartUploadResult>").unwrap();
        assert!(parse_multipart_complete(&full[..cut]).is_err());
        // Cut mid-ETag: no complete ETag element.
        let mid = full.find("<ETag>").unwrap() + 4;
        assert!(parse_multipart_complete(&full[..mid]).is_err());
        // Cut right after the root open tag.
        let open_end = full.find('>').unwrap() + 1;
        assert!(parse_multipart_complete(&full[..open_end]).is_err());
    }

    #[test]
    fn complete_200_success_without_etag_is_an_error() {
        let no_etag = complete_success_body()
            .replace("<ETag>\"abc123\"</ETag>", "")
            .replace("<ETag></ETag>", "");
        assert!(parse_multipart_complete(&no_etag).is_err());
        // An explicitly empty ETag is just as missing.
        let empty = complete_success_body().replace("\"abc123\"", "");
        assert!(parse_multipart_complete(&empty).is_err());
    }

    #[test]
    fn complete_aws_style_escaped_etag_resolves() {
        // AWS escapes the ETag quotes as &quot; — the entity must resolve
        // to the same literal value as an unescaped body.
        let escaped = "<CompleteMultipartUploadResult>\
                       <ETag>&quot;abc123&quot;</ETag>\
                       </CompleteMultipartUploadResult>";
        assert_eq!(parse_multipart_complete(escaped).unwrap(), "\"abc123\"");
        // Numeric character references resolve the same way.
        let numeric = "<CompleteMultipartUploadResult>\
                       <ETag>&#34;abc&#x22;</ETag>\
                       </CompleteMultipartUploadResult>";
        assert_eq!(parse_multipart_complete(numeric).unwrap(), "\"abc\"");
        // An undefined entity reference fails closed.
        let undef = "<CompleteMultipartUploadResult><ETag>&bogus;</ETag>\
                     </CompleteMultipartUploadResult>";
        assert!(parse_multipart_complete(undef).is_err());
    }

    // --------------------------------------------- S1a-review-fix: structure
    // The success decision is XML-STRUCTURAL, not substring: a well-formed
    // document with exactly one CompleteMultipartUploadResult root, nothing
    // outside it, and a real (non-comment) non-empty ETag child field.

    #[test]
    fn complete_root_complete_but_garbage_appended_is_an_error() {
        let ok = complete_success_body();
        // extra element after the root close
        let with_elem = ok.clone() + "<Extra><ETag>\"x\"</ETag></Extra>";
        assert!(parse_multipart_complete(&with_elem).is_err());
        // raw trailing garbage
        let with_text = ok.clone() + "trailing garbage";
        assert!(parse_multipart_complete(&with_text).is_err());
        // an unclosed tag appended after the root
        let with_unclosed = ok + "<Extra";
        assert!(parse_multipart_complete(&with_unclosed).is_err());
    }

    #[test]
    fn complete_etag_in_comment_or_outside_root_is_not_an_etag() {
        // A commented-out ETag is NOT an ETag field: the document must fail.
        let commented = "<CompleteMultipartUploadResult>\
                         <!-- <ETag>\"abc\"</ETag> -->\
                         </CompleteMultipartUploadResult>";
        assert!(parse_multipart_complete(commented).is_err());
        // An ETag element after the root close lies outside the result.
        let outside = "<CompleteMultipartUploadResult></CompleteMultipartUploadResult>\
                       <ETag>\"abc\"</ETag>";
        assert!(parse_multipart_complete(outside).is_err());
    }

    #[test]
    fn complete_wrong_or_multiple_roots_are_errors() {
        // A root whose name merely STARTS WITH the result name is a
        // different element; substring matching would accept it.
        let prefixed = "<CompleteMultipartUploadResultX><ETag>\"x\"</ETag>\
                        </CompleteMultipartUploadResultX>";
        assert!(parse_multipart_complete(prefixed).is_err());
        // A completely different root carrying an ETag.
        assert!(parse_multipart_complete("<Foo><ETag>\"x\"</ETag></Foo>").is_err());
        // Two result documents in one body: no single root.
        let two = "<CompleteMultipartUploadResult><ETag>\"x\"</ETag>\
                   </CompleteMultipartUploadResult>\
                   <CompleteMultipartUploadResult><ETag>\"y\"</ETag>\
                   </CompleteMultipartUploadResult>";
        assert!(parse_multipart_complete(two).is_err());
        // Root opens but never closes (truncated document).
        assert!(
            parse_multipart_complete("<CompleteMultipartUploadResult><ETag>\"x\"</ETag>").is_err()
        );
        // Mismatched close tag.
        assert!(parse_multipart_complete("<CompleteMultipartUploadResult></Wrong>").is_err());
    }

    #[test]
    fn complete_empty_error_root_and_namespaced_variants() {
        assert!(parse_multipart_complete("").is_err());
        // An error root without a Code element is still an error.
        assert!(parse_multipart_complete("<Error><Message>boom</Message></Error>").is_err());
        // An error root WITH an ETag must never succeed.
        assert!(
            parse_multipart_complete("<Error><Code>X</Code><ETag>\"x\"</ETag></Error>").is_err()
        );
        // Self-closing root: no ETag child is possible.
        assert!(parse_multipart_complete("<CompleteMultipartUploadResult/>").is_err());
        // A namespace PREFIX resolves to the same root local name.
        let ns = "<s3:CompleteMultipartUploadResult \
                  xmlns:s3=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                  <s3:ETag>\"x\"</s3:ETag>\
                  </s3:CompleteMultipartUploadResult>";
        assert_eq!(parse_multipart_complete(ns).unwrap(), "\"x\"");
    }
}

/// MinIO integration tests. Requires a running MinIO (scripts/start-minio.sh);
/// excluded from default `cargo test`, executed by scripts/test-local.sh.
#[cfg(test)]
mod minio_tests {
    use super::*;
    use crate::object_store::mem::MemObjectStore;

    fn minio() -> Option<S3ObjectStore> {
        if std::env::var("LIGHTFS_TEST_MINIO").ok().as_deref() != Some("1") {
            return None;
        }
        let store = S3ObjectStore::new(S3Config {
            endpoint: std::env::var("LIGHTFS_S3_ENDPOINT")
                .unwrap_or("http://127.0.0.1:9000".into()),
            region: "us-east-1".into(),
            bucket: "lightfs-test".into(),
            access_key: "minioadmin".into(),
            secret_key: "minioadmin".into(),
            path_style: true,
            service: "s3".into(),
        })
        .unwrap();
        store.ensure_bucket().unwrap();
        Some(store)
    }

    fn cross_check(backend: &dyn ObjectStore) {
        // Make reruns idempotent: clear anything left over under this
        // test's own sub-prefix. It must NOT touch phase1/ as a whole:
        // sibling MinIO tests (range-len, presign, ...) own keys directly
        // under phase1/ and cargo runs test threads in parallel - a wide
        // clear here deletes their objects mid-test ("object missing"
        // flake seen on fast machines / fresh MinIO).
        let mut token = None;
        loop {
            let page = backend.list("phase1/sem/", token.as_deref(), 1000).unwrap();
            for item in &page.items {
                backend.delete(&item.key).unwrap();
            }
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }

        let info = backend.put("phase1/sem/hello", b"hello world").unwrap();
        assert_eq!(info.size, 11);
        assert!(!info.etag.is_empty());

        let got = backend.get("phase1/sem/hello", None).unwrap();
        assert_eq!(got.data, b"hello world");

        let part = backend
            .get("phase1/sem/hello", Some(Range { start: 6, len: 5 }))
            .unwrap();
        assert_eq!(part.data, b"world");
        assert_eq!(part.offset, 6);

        let head = backend.head("phase1/sem/hello").unwrap();
        assert_eq!(head.size, 11);
        assert_eq!(head.etag, info.etag);

        // overwrite
        let info2 = backend.put("phase1/sem/hello", b"bye").unwrap();
        assert_ne!(info.etag, info2.etag);
        assert_eq!(backend.get("phase1/sem/hello", None).unwrap().data, b"bye");

        // list with pagination
        for i in 0..5 {
            backend
                .put(&format!("phase1/sem/list/{i:02}"), b"x")
                .unwrap();
        }
        let mut all = Vec::new();
        let mut token = None;
        loop {
            let page = backend
                .list("phase1/sem/list/", token.as_deref(), 2)
                .unwrap();
            all.extend(page.items.iter().map(|i| i.key.clone()));
            match page.next_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        assert_eq!(all.len(), 5);
        let mut sorted = all.clone();
        sorted.sort();
        assert_eq!(all, sorted);

        backend.delete("phase1/sem/hello").unwrap();
        assert!(matches!(
            backend.head("phase1/sem/hello"),
            Err(Error::ObjectMissing(_))
        ));
        backend.delete("phase1/sem/hello").unwrap(); // idempotent
    }

    #[test]
    fn same_semantics_mem() {
        cross_check(&MemObjectStore::new());
    }

    #[test]
    fn same_semantics_minio() {
        let Some(s3) = minio() else { return };
        assert!(s3.ping().is_ok());
        cross_check(&s3);
    }

    /// get_stream contract (review P1): the returned length describes the
    /// bytes the reader yields — the requested slice, not the object total.
    #[test]
    fn minio_get_stream_range_lengths() {
        let Some(s3) = minio() else { return };
        // 1000-byte object; request slices that differ from its size
        let payload: Vec<u8> = (0..1000u32).map(|b| b as u8).collect();
        s3.put("phase1/range-len", &payload).unwrap();

        // middle 100 bytes: len must be 100, bytes must match the slice
        let (mut r, len) = s3
            .get_stream(
                "phase1/range-len",
                Some(Range {
                    start: 200,
                    len: 100,
                }),
            )
            .unwrap();
        assert_eq!(
            len, 100,
            "206 length must be the slice, not the object total"
        );
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got.len() as u64, len);
        assert_eq!(got, &payload[200..300]);

        // read from middle to EOF: requested 5000 > remaining 800
        let (mut r, len) = s3
            .get_stream(
                "phase1/range-len",
                Some(Range {
                    start: 200,
                    len: 5000,
                }),
            )
            .unwrap();
        assert_eq!(len, 800);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, &payload[200..]);

        // out-of-range start: S3 answers 416 InvalidRange; Core::open_read
        // guards offset >= size before reaching the store, so this error is
        // only observable when using the store directly.
        let err = match s3.get_stream(
            "phase1/range-len",
            Some(Range {
                start: 5000,
                len: 10,
            }),
        ) {
            Err(e) => e,
            Ok(_) => panic!("out-of-range start must fail with 416"),
        };
        assert!(err.to_string().contains("416"), "{err}");

        // full object without range: len = object size
        let (mut r, len) = s3.get_stream("phase1/range-len", None).unwrap();
        assert_eq!(len, 1000);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, payload);
        s3.delete("phase1/range-len").unwrap();
    }

    #[test]
    fn minio_presigned_get_roundtrip() {
        let Some(s3) = minio() else { return };
        let payload = b"presigned poc payload";
        s3.put("phase1/presign", payload).unwrap();
        let url = s3.presign_get("phase1/presign", 300).unwrap();
        // Plain HTTP client, no Authorization header at all. NO retry: a
        // PUT followed by a GET on the same store must observe the object
        // (S3 provides strong read-after-write consistency); a transient
        // failure here is a real anomaly and must surface with diagnostics
        // (status + body + request id), not be retried into green.
        let agent = ureq::Agent::config_builder()
            .http_status_as_error(false)
            .build()
            .new_agent();
        let mut resp = agent.get(&url).call().unwrap();
        let status = resp.status().as_u16();
        if status != 200 {
            // Full diagnostics, fail immediately (no retry): on a healthy
            // S3 a PUT-then-GET is strongly consistent, so a 404 here is
            // either data loss or an environment anomaly we must see in
            // full. Signature params are stripped from the URL.
            let body = resp.body_mut().read_to_vec().unwrap_or_default();
            let headers: Vec<String> = resp
                .headers()
                .iter()
                .map(|(h, v)| format!("{}={}", h.as_str(), String::from_utf8_lossy(v.as_bytes())))
                .collect();
            let safe_url = url.split('?').next().unwrap_or("<no-url>");
            panic!(
                "presigned fetch failed: status {status}\nurl(path only) {safe_url}\n\
                 headers: {}\nbody: {}",
                headers.join(", "),
                String::from_utf8_lossy(&body)
            );
        }
        let body = resp.body_mut().read_to_vec().unwrap();
        assert_eq!(body, payload);
        s3.delete("phase1/presign").unwrap();
        // tampered signature must be rejected
        let bad_url = format!("{}X", &url[..url.len() - 1]);
        let resp = agent.get(&bad_url).call().unwrap();
        assert_eq!(resp.status().as_u16(), 403);
    }

    #[test]
    fn minio_range_head_version_fields() {
        let Some(s3) = minio() else { return };
        let info = s3.put("phase1/ver", b"0123456789").unwrap();
        assert!(!info.etag.is_empty());
        let head = s3.head("phase1/ver").unwrap();
        assert_eq!(head.size, 10);
        assert_eq!(head.etag, info.etag);
        s3.delete("phase1/ver").unwrap();
    }
}
