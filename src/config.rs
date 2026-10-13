//! TOML runtime configuration. Field names match config/example.toml.

use std::path::Path;

use serde::Deserialize;

use crate::error::{Error, Result};
use crate::object_store::s3::S3Config;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    /// embedded | server | client
    pub mode: String,
    pub s3: S3Section,
    pub metadata: MetadataSection,
    pub server: ServerSection,
    pub cache: CacheSection,
    pub retention: RetentionSection,
    /// Path policy (Phase P). Absent = no policy = today's routing.
    /// Strictly validated at mount: unknown fields inside are errors.
    #[serde(default)]
    pub policy: Option<crate::policy::PolicySection>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct S3Section {
    pub endpoint: String,
    pub region: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    #[serde(default = "default_true")]
    pub path_style: bool,
    /// SigV4 service scope: "s3" (AWS/MinIO) or "oss" (Aliyun OSS).
    pub service: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct MetadataSection {
    /// SQLite file path (embedded/server in v1).
    pub path: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct ServerSection {
    pub bind: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct CacheSection {
    pub metadata: MetadataCacheSection,
    pub data: DataCacheSection,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct MetadataCacheSection {
    pub enabled: bool,
    pub ttl_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct DataCacheSection {
    /// Data cache is OFF by default and never authoritative (AD-009).
    pub enabled: bool,
    pub max_size_bytes: u64,
    pub max_age_seconds: u64,
}

/// Superseded-object retention: how long replaced/deleted committed objects
/// stay in the bucket after their metadata pointer moved on. Within this
/// window a metadata snapshot (`lightfs backup`) restores point-in-time
/// CONTENT, not just names. 0 disables retention (old objects are deleted
/// at commit; backups then restore metadata only).
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct RetentionSection {
    pub superseded_secs: u64,
}

impl Default for RetentionSection {
    fn default() -> Self {
        Self {
            superseded_secs: crate::core::DEFAULT_SUPERSEDED_SECS,
        }
    }
}

fn default_true() -> bool {
    true
}

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("cannot read {path}: {e}")))?;
        let cfg: Config = toml::from_str(&text)
            .map_err(|e| Error::Config(format!("cannot parse {path}: {e}")))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Load with defaults for missing optional fields; missing file is an error
    /// unless `optional` (then defaults apply, e.g. in-memory test setups).
    pub fn load_or_defaults(path: Option<&str>) -> Result<Self> {
        match path {
            Some(p) if Path::new(p).exists() => Self::load(p),
            Some(p) => Err(Error::Config(format!("config not found: {p}"))),
            None => Ok(Config {
                mode: "embedded".into(),
                s3: S3Section {
                    endpoint: "http://127.0.0.1:9000".into(),
                    region: "us-east-1".into(),
                    bucket: "lightfs".into(),
                    access_key: "minioadmin".into(),
                    secret_key: "minioadmin".into(),
                    path_style: true,
                    service: String::new(),
                },
                metadata: MetadataSection {
                    path: "./data/lightfs.db".into(),
                },
                server: ServerSection {
                    bind: "127.0.0.1:8080".into(),
                },
                cache: CacheSection {
                    metadata: MetadataCacheSection {
                        enabled: true,
                        ttl_seconds: 5,
                    },
                    data: DataCacheSection::default(),
                },
                retention: RetentionSection::default(),
                policy: None,
            }),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.s3.endpoint.is_empty() {
            return Err(Error::Config("s3.endpoint is required".into()));
        }
        if self.s3.bucket.is_empty() {
            return Err(Error::Config("s3.bucket is required".into()));
        }
        if self.metadata.path.is_empty() {
            return Err(Error::Config("metadata.path is required".into()));
        }
        Ok(())
    }

    pub fn s3_config(&self) -> S3Config {
        S3Config {
            endpoint: self.s3.endpoint.clone(),
            region: if self.s3.region.is_empty() {
                "us-east-1".into()
            } else {
                self.s3.region.clone()
            },
            bucket: self.s3.bucket.clone(),
            access_key: self.s3.access_key.clone(),
            secret_key: self.s3.secret_key.clone(),
            path_style: self.s3.path_style,
            service: self.s3.service.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_example_toml() {
        let text = std::fs::read_to_string("config/example.toml").unwrap();
        let cfg: Config = toml::from_str(&text).unwrap();
        assert_eq!(cfg.mode, "embedded");
        assert_eq!(cfg.s3.endpoint, "http://127.0.0.1:9000");
        assert_eq!(cfg.s3.bucket, "lightfs");
        assert!(cfg.s3.path_style);
        assert_eq!(cfg.metadata.path, "./data/lightfs.db");
        assert_eq!(cfg.server.bind, "127.0.0.1:8080");
        assert_eq!(cfg.cache.metadata.ttl_seconds, 5);
        assert!(!cfg.cache.data.enabled);
    }

    #[test]
    fn defaults_fill_optional_fields() {
        let cfg = Config::load_or_defaults(None).unwrap();
        assert_eq!(cfg.mode, "embedded");
        assert_eq!(cfg.server.bind, "127.0.0.1:8080");
    }
}
