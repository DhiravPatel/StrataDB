//! Server configuration file (spec §22.2, §29.4).
//!
//! Precedence: command-line flag > environment variable (`STRATADB_*`) >
//! TOML file > built-in default. Unknown keys are rejected (a typo must not
//! silently fall back to a default). The file contains no secrets; TLS keys
//! and API tokens are referenced by path / stored separately.
//!
//! ```toml
//! config_version = 1
//! data_dir = "/var/lib/stratadb"
//! listen = "127.0.0.1:7070"
//! durability = "group_commit"          # async | sync | group_commit
//!
//! [tls]                                # optional; required for non-loopback listen
//! cert = "/etc/stratadb/tls/cert.pem"
//! key = "/etc/stratadb/tls/key.pem"
//!
//! [limits]
//! max_body_bytes = 8388608
//! max_decompressed_bytes = 33554432
//! max_concurrent_ingest = 32
//! max_concurrent_queries = 16
//! max_query_timeout_ms = 60000
//! max_query_rows = 100000
//!
//! [storage]
//! memtable_max_bytes = 67108864
//! wal_segment_max_bytes = 67108864
//! disk_reserve_bytes = 536870912
//! disk_reserve_ratio = 0.02
//! default_retention_days = 30          # omit to keep data indefinitely
//! bloom_attribute_keys = ["request_id", "user_id", "session_id", "correlation_id"]
//!
//! [backup]
//! dir = "/var/backups/stratadb"        # enables POST /api/v1/admin/backup
//!
//! [security]
//! insecure_allow_remote_without_tls = false
//! ```

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use stratadb_common::{Error, Result};
use stratadb_storage::{Durability, EngineOptions};

use crate::ServerConfig;

pub const CONFIG_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub config_version: Option<u32>,
    pub data_dir: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    pub durability: Option<String>,
    pub tls: Option<TlsSection>,
    #[serde(default)]
    pub limits: LimitsSection,
    #[serde(default)]
    pub storage: StorageSection,
    #[serde(default)]
    pub backup: BackupSection,
    #[serde(default)]
    pub security: SecuritySection,
    #[serde(default)]
    pub quotas: QuotasSection,
}

/// `[quotas]` defaults plus `[quotas.tenants.<id>]` overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotasSection {
    pub default_ingest_events_per_sec: Option<f64>,
    pub default_ingest_burst_events: Option<f64>,
    pub default_storage_bytes: Option<u64>,
    #[serde(default)]
    pub tenants: std::collections::BTreeMap<String, TenantQuotaSection>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantQuotaSection {
    pub ingest_events_per_sec: Option<f64>,
    pub ingest_burst_events: Option<f64>,
    pub storage_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsSection {
    pub cert: PathBuf,
    pub key: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitsSection {
    pub max_body_bytes: Option<usize>,
    pub max_decompressed_bytes: Option<usize>,
    pub max_concurrent_ingest: Option<usize>,
    pub max_concurrent_queries: Option<usize>,
    pub max_query_timeout_ms: Option<u64>,
    pub max_query_rows: Option<usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageSection {
    pub memtable_max_bytes: Option<usize>,
    pub wal_segment_max_bytes: Option<u64>,
    pub disk_reserve_bytes: Option<u64>,
    pub disk_reserve_ratio: Option<f64>,
    pub default_retention_days: Option<u64>,
    pub bloom_attribute_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackupSection {
    pub dir: Option<PathBuf>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecuritySection {
    pub insecure_allow_remote_without_tls: Option<bool>,
}

impl FileConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| Error::io(format!("reading config {}", path.display()), e))?;
        let cfg: FileConfig = toml::from_str(&text)
            .map_err(|e| Error::invalid_input(format!("config {}: {e}", path.display())))?;
        match cfg.config_version {
            Some(CONFIG_VERSION) => Ok(cfg),
            Some(v) => Err(Error::FormatVersion {
                what: "config",
                found: v,
                supported: CONFIG_VERSION.to_string(),
            }),
            None => Err(Error::invalid_input(format!(
                "config {}: missing `config_version = {CONFIG_VERSION}`",
                path.display()
            ))),
        }
    }
}

/// Fully resolved settings for `serve`.
#[derive(Debug, Clone)]
pub struct Resolved {
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    pub durability: Durability,
    pub tls: Option<TlsSection>,
    pub insecure_allow_remote_without_tls: bool,
    pub server: ServerConfig,
    pub engine: EngineOptions,
}

/// Values from flags/environment that override the file (None = not given).
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub data_dir: Option<PathBuf>,
    pub listen: Option<SocketAddr>,
    pub durability: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub max_body_bytes: Option<usize>,
    pub backup_dir: Option<PathBuf>,
    pub default_retention_days: Option<u64>,
    pub insecure_allow_remote_without_tls: bool,
}

pub fn parse_durability(s: &str) -> Result<Durability> {
    Ok(match s {
        "async" => Durability::Async,
        "sync" => Durability::Sync,
        "group_commit" => Durability::GroupCommit {
            max_delay: Duration::from_millis(2),
        },
        _ => {
            return Err(Error::invalid_input(format!(
                "unknown durability mode {s:?} (async | sync | group_commit)"
            )))
        }
    })
}

fn positive<T: PartialOrd + Default + Copy + std::fmt::Display>(
    name: &str,
    v: Option<T>,
) -> Result<Option<T>> {
    match v {
        Some(x) if x <= T::default() => Err(Error::invalid_input(format!(
            "{name} must be positive, got {x}"
        ))),
        other => Ok(other),
    }
}

/// Merge file + overrides and validate. Pure: no I/O except checking that
/// referenced TLS files exist.
pub fn resolve(file: &FileConfig, o: &Overrides) -> Result<Resolved> {
    let data_dir = o
        .data_dir
        .clone()
        .or_else(|| file.data_dir.clone())
        .ok_or_else(|| {
            Error::invalid_input(
                "data_dir is required (flag --data-dir, STRATADB_DATA_DIR, or config)",
            )
        })?;
    let listen = o
        .listen
        .or(file.listen)
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 7070)));
    let durability = parse_durability(
        o.durability
            .as_deref()
            .or(file.durability.as_deref())
            .unwrap_or("group_commit"),
    )?;
    let tls = match (&o.tls_cert, &o.tls_key) {
        (Some(c), Some(k)) => Some(TlsSection {
            cert: c.clone(),
            key: k.clone(),
        }),
        (None, None) => file.tls.clone(),
        _ => {
            return Err(Error::invalid_input(
                "--tls-cert and --tls-key must be given together",
            ))
        }
    };
    if let Some(t) = &tls {
        for p in [&t.cert, &t.key] {
            if !p.is_file() {
                return Err(Error::invalid_input(format!(
                    "TLS file {} does not exist",
                    p.display()
                )));
            }
        }
    }
    let insecure = o.insecure_allow_remote_without_tls
        || file
            .security
            .insecure_allow_remote_without_tls
            .unwrap_or(false);
    if !listen.ip().is_loopback() && tls.is_none() && !insecure {
        return Err(Error::invalid_input(
            "refusing to listen on a non-loopback address without TLS; configure [tls] \
             (or set insecure_allow_remote_without_tls, not recommended)",
        ));
    }

    let l = &file.limits;
    let mut server = ServerConfig::default();
    if let Some(v) = positive(
        "limits.max_body_bytes",
        o.max_body_bytes.or(l.max_body_bytes),
    )? {
        server.max_body_bytes = v;
    }
    if let Some(v) = positive("limits.max_decompressed_bytes", l.max_decompressed_bytes)? {
        server.max_decompressed_bytes = v;
    }
    if let Some(v) = positive("limits.max_concurrent_ingest", l.max_concurrent_ingest)? {
        server.max_concurrent_ingest = v;
    }
    if let Some(v) = positive("limits.max_concurrent_queries", l.max_concurrent_queries)? {
        server.max_concurrent_queries = v;
    }
    if let Some(v) = positive("limits.max_query_timeout_ms", l.max_query_timeout_ms)? {
        server.max_query_timeout = Duration::from_millis(v);
    }
    if let Some(v) = positive("limits.max_query_rows", l.max_query_rows)? {
        server.max_query_rows = v;
    }
    if server.max_decompressed_bytes < server.max_body_bytes {
        return Err(Error::invalid_input(
            "limits.max_decompressed_bytes must be >= max_body_bytes",
        ));
    }
    server.backup_root = o.backup_dir.clone().or_else(|| file.backup.dir.clone());
    let qs = &file.quotas;
    let rate_ok = |name: &str, v: Option<f64>| -> Result<Option<f64>> {
        match v {
            Some(x) if !(x.is_finite() && x > 0.0) => Err(Error::invalid_input(format!(
                "{name} must be a positive number"
            ))),
            other => Ok(other),
        }
    };
    server.quotas.default = crate::TenantQuota {
        ingest_events_per_sec: rate_ok(
            "quotas.default_ingest_events_per_sec",
            qs.default_ingest_events_per_sec,
        )?,
        ingest_burst_events: rate_ok(
            "quotas.default_ingest_burst_events",
            qs.default_ingest_burst_events,
        )?,
        storage_bytes: positive("quotas.default_storage_bytes", qs.default_storage_bytes)?,
    };
    for (t, q) in &qs.tenants {
        stratadb_common::TenantId::new(t.clone())
            .map_err(|e| Error::invalid_input(format!("quotas.tenants.{t}: {e}")))?;
        server.quotas.tenants.insert(
            t.clone(),
            crate::TenantQuota {
                ingest_events_per_sec: rate_ok("ingest_events_per_sec", q.ingest_events_per_sec)?,
                ingest_burst_events: rate_ok("ingest_burst_events", q.ingest_burst_events)?,
                storage_bytes: positive("storage_bytes", q.storage_bytes)?,
            },
        );
    }

    let s = &file.storage;
    let mut engine = EngineOptions::new(&data_dir);
    engine.wal.durability = durability;
    if let Some(v) = positive("storage.memtable_max_bytes", s.memtable_max_bytes)? {
        engine.memtable_max_bytes = v;
    }
    if let Some(v) = positive("storage.wal_segment_max_bytes", s.wal_segment_max_bytes)? {
        engine.wal.segment_max_bytes = v;
    }
    if let Some(v) = s.disk_reserve_bytes {
        engine.disk_reserve_bytes = v;
    }
    if let Some(r) = s.disk_reserve_ratio {
        if !(0.0..=0.5).contains(&r) {
            return Err(Error::invalid_input(
                "storage.disk_reserve_ratio must be within 0.0..=0.5",
            ));
        }
        engine.disk_reserve_ratio = r;
    }
    let days = positive(
        "storage.default_retention_days",
        o.default_retention_days.or(s.default_retention_days),
    )?;
    engine.default_retention = days.map(|d| Duration::from_secs(d.saturating_mul(86_400)));
    if let Some(k) = &s.bloom_attribute_keys {
        if k.iter().any(String::is_empty) {
            return Err(Error::invalid_input(
                "storage.bloom_attribute_keys must not contain empty keys",
            ));
        }
        engine.bloom_attribute_keys = k.clone();
    }
    Ok(Resolved {
        data_dir,
        listen,
        durability,
        tls,
        insecure_allow_remote_without_tls: insecure,
        server,
        engine,
    })
}

impl Resolved {
    /// Effective configuration for display (`config show`); contains no secrets.
    pub fn display(&self) -> serde_json::Value {
        serde_json::json!({
            "data_dir": self.data_dir,
            "listen": self.listen.to_string(),
            "durability": format!("{:?}", self.durability),
            "tls": self.tls.as_ref().map(|t| serde_json::json!({"cert": t.cert, "key": t.key})),
            "insecure_allow_remote_without_tls": self.insecure_allow_remote_without_tls,
            "limits": {
                "max_body_bytes": self.server.max_body_bytes,
                "max_decompressed_bytes": self.server.max_decompressed_bytes,
                "max_concurrent_ingest": self.server.max_concurrent_ingest,
                "max_concurrent_queries": self.server.max_concurrent_queries,
                "max_query_timeout_ms": self.server.max_query_timeout.as_millis() as u64,
                "max_query_rows": self.server.max_query_rows,
            },
            "storage": {
                "memtable_max_bytes": self.engine.memtable_max_bytes,
                "wal_segment_max_bytes": self.engine.wal.segment_max_bytes,
                "disk_reserve_bytes": self.engine.disk_reserve_bytes,
                "disk_reserve_ratio": self.engine.disk_reserve_ratio,
                "default_retention_days": self.engine.default_retention.map(|d| d.as_secs() / 86_400),
                "bloom_attribute_keys": self.engine.bloom_attribute_keys,
            },
            "backup_dir": self.server.backup_root,
            "quotas": {
                "default": format!("{:?}", self.server.quotas.default),
                "tenants": self.server.quotas.tenants.iter().map(|(k, v)| (k.clone(), format!("{v:?}"))).collect::<std::collections::BTreeMap<_, _>>(),
            },
            "restart_required_for_changes": true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> Result<FileConfig> {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("c.toml");
        std::fs::write(&p, s).unwrap();
        FileConfig::load(&p)
    }

    #[test]
    fn full_file_and_precedence() {
        let f = parse(
            "config_version = 1\ndata_dir = \"/d\"\nlisten = \"127.0.0.1:9000\"\ndurability = \"sync\"\n\
             [limits]\nmax_query_rows = 5\n[storage]\ndefault_retention_days = 7\nbloom_attribute_keys = [\"k\"]\n",
        )
        .unwrap();
        let r = resolve(&f, &Overrides::default()).unwrap();
        assert_eq!(r.listen.port(), 9000);
        assert_eq!(r.durability, Durability::Sync);
        assert_eq!(r.server.max_query_rows, 5);
        assert_eq!(
            r.engine.default_retention,
            Some(Duration::from_secs(7 * 86_400))
        );
        assert_eq!(r.engine.bloom_attribute_keys, vec!["k"]);
        // Flags beat the file.
        let o = Overrides {
            listen: Some("127.0.0.1:1".parse().unwrap()),
            durability: Some("async".into()),
            ..Default::default()
        };
        let r = resolve(&f, &o).unwrap();
        assert_eq!((r.listen.port(), r.durability), (1, Durability::Async));
        assert!(r.display().to_string().contains("\"max_query_rows\":5"));
    }

    #[test]
    fn quotas_section() {
        let f = parse("config_version = 1\ndata_dir = \"/d\"\n[quotas]\ndefault_ingest_events_per_sec = 100.0\ndefault_storage_bytes = 1000\n[quotas.tenants.acme]\nstorage_bytes = 5000\n").unwrap();
        let r = resolve(&f, &Overrides::default()).unwrap();
        let acme = r
            .server
            .quotas
            .for_tenant(&stratadb_common::TenantId::new("acme").unwrap());
        assert_eq!(
            (acme.ingest_events_per_sec, acme.storage_bytes),
            (Some(100.0), Some(5000))
        );
        let other = r
            .server
            .quotas
            .for_tenant(&stratadb_common::TenantId::new("x").unwrap());
        assert_eq!(other.storage_bytes, Some(1000));
        let bad = |s: &str| resolve(&parse(s).unwrap(), &Overrides::default()).is_err();
        assert!(bad("config_version = 1\ndata_dir = \"/d\"\n[quotas]\ndefault_ingest_events_per_sec = -1.0\n"));
        assert!(bad(
            "config_version = 1\ndata_dir = \"/d\"\n[quotas.tenants.\"../x\"]\nstorage_bytes = 1\n"
        ));
    }

    #[test]
    fn rejects_mistakes() {
        assert!(
            parse("config_version = 1\ndata_dirr = \"/d\"\n").is_err(),
            "typo must not be ignored"
        );
        assert!(parse("data_dir = \"/d\"\n").is_err(), "version required");
        assert_eq!(
            parse("config_version = 2\n").unwrap_err().kind(),
            stratadb_common::ErrorKind::Unsupported
        );
        let ok = |s: &str| resolve(&parse(s).unwrap(), &Overrides::default());
        assert!(ok("config_version = 1\n").is_err(), "data_dir required");
        assert!(
            ok("config_version = 1\ndata_dir = \"/d\"\nlisten = \"0.0.0.0:7070\"\n").is_err(),
            "remote without TLS"
        );
        assert!(ok("config_version = 1\ndata_dir = \"/d\"\nlisten = \"0.0.0.0:7070\"\n[security]\ninsecure_allow_remote_without_tls = true\n").is_ok());
        assert!(ok(
            "config_version = 1\ndata_dir = \"/d\"\n[tls]\ncert = \"/nope\"\nkey = \"/nope\"\n"
        )
        .is_err());
        assert!(ok("config_version = 1\ndata_dir = \"/d\"\ndurability = \"yolo\"\n").is_err());
        assert!(
            ok("config_version = 1\ndata_dir = \"/d\"\n[limits]\nmax_query_rows = 0\n").is_err()
        );
        assert!(
            ok("config_version = 1\ndata_dir = \"/d\"\n[storage]\ndisk_reserve_ratio = 0.9\n")
                .is_err()
        );
    }
}
