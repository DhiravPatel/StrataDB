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
//! scan_threads = 8          # shared scan worker pool (default: logical CPUs)
//! net_threads = 8           # async network runtime (default: logical CPUs)
//! blocking_threads = 24     # blocking request workers (default: rest of the budget)
//! # net + scan + blocking + 4 must not exceed 2 x logical CPUs + 32
//!
//! [storage]
//! memtable_max_bytes = 67108864
//! max_open_files = 4096     # segment file descriptor budget (default: 50% of RLIMIT_NOFILE)
//! group_commit_max_delay_ms = 2   # upper bound of the adaptive group-commit wait
//! group_commit_adaptive = true    # false: always wait the full delay
//! wal_segment_max_bytes = 67108864
//! disk_reserve_bytes = 536870912
//! disk_reserve_ratio = 0.02
//! default_retention_days = 30          # omit to keep data indefinitely
//! bloom_attribute_keys = ["request_id", "user_id", "session_id", "correlation_id"]
//! ngram_index = true                   # trigram filters on body for LIKE/ILIKE
//! ngram_attribute_keys = []            # string attributes to index the same way
//!
//! [backup]
//! dir = "/var/backups/stratadb"        # enables POST /api/v1/admin/backup
//!
//! [security]
//! insecure_allow_remote_without_tls = false
//!
//! [encryption]
//! key_file = "/etc/stratadb/kek.hex"    # enables encryption at rest (ADR-0024)
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
    #[serde(default)]
    pub encryption: EncryptionSection,
    #[serde(default)]
    pub observability: ObservabilitySection,
    #[serde(default)]
    pub memory: MemorySection,
}

/// `[memory]` — budget and regions (spec VIII.5.2, `memory.*` settings).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemorySection {
    /// Absolute process budget (bytes, >= 256 MiB); overrides the ratio.
    pub max_server_memory_bytes: Option<u64>,
    /// Budget as a share of the cgroup limit or physical memory (0.1..=0.95;
    /// default 0.80).
    pub max_server_memory_ratio: Option<f64>,
    /// Memtables (0.01..=0.5; default 0.10).
    pub ingest_memory_ratio: Option<f64>,
    /// Caches (0..=0.8; default 0.30).
    pub cache_memory_ratio: Option<f64>,
    /// Compaction and retention rewrites (0.01..=0.5; default 0.10).
    pub merge_memory_ratio: Option<f64>,
    /// Per-query limit (>= 64 MiB; default min(10 GiB, query pool)).
    pub max_query_memory_bytes: Option<u64>,
    /// Wait for memory before failing a query (0..=600000; default 0).
    pub memory_wait_timeout_ms: Option<u64>,
    /// Warn when untracked resident memory exceeds this share of RSS
    /// (0.01..=1; default 0.10).
    pub untracked_warn_ratio: Option<f64>,
}

/// `[observability]` — `system.query_log` (spec `obs.query_log*`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilitySection {
    /// Record every query in `system.query_log` (default true).
    pub query_log: Option<bool>,
    /// Buffering before entries are written (ms, 10..=600000; default 7500).
    pub query_log_flush_ms: Option<u64>,
    /// Retention of query log entries (days, 1..=3650; default 7).
    pub query_log_retention_days: Option<u64>,
    /// Entries buffered at most before new ones are dropped (1..=1000000;
    /// default 10000).
    pub query_log_queue: Option<usize>,
}

/// `[encryption]` — encryption at rest (ADR-0024).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EncryptionSection {
    /// File holding the 256-bit key-encryption key as 64 hex characters
    /// (mode 0600; create with `strata keys generate`).
    pub key_file: Option<PathBuf>,
}

/// `[quotas]` defaults plus `[quotas.tenants.<id>]` overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotasSection {
    pub default_ingest_events_per_sec: Option<f64>,
    pub default_ingest_burst_events: Option<f64>,
    pub default_storage_bytes: Option<u64>,
    /// Memory each tenant's running queries may hold together.
    pub default_query_memory_bytes: Option<u64>,
    #[serde(default)]
    pub tenants: std::collections::BTreeMap<String, TenantQuotaSection>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantQuotaSection {
    pub ingest_events_per_sec: Option<f64>,
    pub ingest_burst_events: Option<f64>,
    pub storage_bytes: Option<u64>,
    pub query_memory_bytes: Option<u64>,
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
    /// Threads of the shared scan worker pool (1..=256; default: logical CPUs).
    pub scan_threads: Option<usize>,
    /// Async runtime threads (default: logical CPUs).
    pub net_threads: Option<usize>,
    /// Blocking request workers (default: the rest of the thread budget, up
    /// to one per admitted ingest + query request).
    pub blocking_threads: Option<usize>,
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
    pub ngram_index: Option<bool>,
    pub ngram_attribute_keys: Option<Vec<String>>,
    /// Open segment file budget (LRU; default 50% of the soft RLIMIT_NOFILE).
    pub max_open_files: Option<usize>,
    /// I/O calls in flight per device (1..=4096; default 128;
    /// STRATA-SYS-04-008).
    pub io_max_inflight_per_device: Option<usize>,
    /// Upper bound of the group-commit wait (ms, 0..=100; default 2).
    pub group_commit_max_delay_ms: Option<u64>,
    /// Adaptive group-commit delay (default true; spec STRATA-SYS-07-004).
    pub group_commit_adaptive: Option<bool>,
    /// WAL format written: 3 (default; columnar payloads) or 2 (readable by
    /// the previous release, for a rollback window after an upgrade).
    pub wal_format_version: Option<u16>,
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
    /// Scan worker pool size (`None`: one per logical CPU).
    pub scan_threads: Option<usize>,
    /// Thread plan checked against the budget (STRATA-SYS-03-014).
    pub threads: crate::ThreadPlan,
    /// Segment file descriptor budget (`None`: 50% of RLIMIT_NOFILE).
    pub max_open_files: Option<usize>,
    /// I/O calls in flight per device (`None`: 128).
    pub io_max_inflight_per_device: Option<usize>,
    /// `system.query_log`.
    pub query_log: crate::query_log::QueryLogConfig,
    /// Memory budget settings (turned into byte limits at startup, once the
    /// host's memory is known).
    pub memory: crate::memory::MemorySettings,
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
    pub key_file: Option<PathBuf>,
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
    let scan_threads = positive("limits.scan_threads", l.scan_threads)?;
    let net_threads = positive("limits.net_threads", l.net_threads)?;
    let blocking_threads = positive("limits.blocking_threads", l.blocking_threads)?;
    if let Some(n) = scan_threads {
        if n > stratadb_query::pool::MAX_WORKERS {
            return Err(Error::invalid_input(format!(
                "limits.scan_threads must be at most {}",
                stratadb_query::pool::MAX_WORKERS
            )));
        }
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
        query_memory_bytes: positive(
            "quotas.default_query_memory_bytes",
            qs.default_query_memory_bytes,
        )?,
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
                query_memory_bytes: positive("query_memory_bytes", q.query_memory_bytes)?,
            },
        );
    }

    let s = &file.storage;
    let mut engine = EngineOptions::new(&data_dir);
    engine.wal.durability = match durability {
        Durability::GroupCommit { .. } => match s.group_commit_max_delay_ms {
            Some(ms) if ms > 100 => {
                return Err(Error::invalid_input(
                    "storage.group_commit_max_delay_ms must be at most 100",
                ))
            }
            Some(ms) => Durability::GroupCommit {
                max_delay: Duration::from_millis(ms),
            },
            None => durability,
        },
        other => other,
    };
    if let Some(a) = s.group_commit_adaptive {
        engine.wal.adaptive_group_delay = a;
    }
    if let Some(v) = s.wal_format_version {
        use stratadb_storage::{WAL_FORMAT_VERSION, WAL_MIN_WRITE_VERSION};
        if !(WAL_MIN_WRITE_VERSION..=WAL_FORMAT_VERSION).contains(&v) {
            return Err(Error::invalid_input(format!(
                "storage.wal_format_version must be {WAL_MIN_WRITE_VERSION} or {WAL_FORMAT_VERSION}"
            )));
        }
        engine.wal.format_version = v;
    }
    if let Some(k) = o
        .key_file
        .clone()
        .or_else(|| file.encryption.key_file.clone())
    {
        if !k.is_file() {
            return Err(Error::invalid_input(format!(
                "encryption key file {} does not exist",
                k.display()
            )));
        }
        engine.encryption = Some(std::sync::Arc::new(stratadb_crypto::FileKeyProvider::new(
            k,
        )));
    }
    let max_open_files = positive("storage.max_open_files", s.max_open_files)?;
    let io_max_inflight_per_device = s.io_max_inflight_per_device;
    if io_max_inflight_per_device.is_some_and(|n| !(1..=4096).contains(&n)) {
        return Err(Error::invalid_input(
            "storage.io_max_inflight_per_device must be within 1..=4096",
        ));
    }
    if max_open_files.is_some_and(|n| n < 16) {
        return Err(Error::invalid_input(
            "storage.max_open_files must be at least 16",
        ));
    }
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
    if let Some(b) = s.ngram_index {
        engine.ngram_enabled = b;
    }
    if let Some(k) = &s.ngram_attribute_keys {
        if k.iter().any(String::is_empty) {
            return Err(Error::invalid_input(
                "storage.ngram_attribute_keys must not contain empty keys",
            ));
        }
        engine.ngram_attribute_keys = k.clone();
    }
    let ob = &file.observability;
    let mut query_log = crate::query_log::QueryLogConfig::default();
    if let Some(b) = ob.query_log {
        query_log.enabled = b;
    }
    if let Some(ms) = ob.query_log_flush_ms {
        if !(10..=600_000).contains(&ms) {
            return Err(Error::invalid_input(
                "observability.query_log_flush_ms must be within 10..=600000",
            ));
        }
        query_log.flush_interval = Duration::from_millis(ms);
    }
    if let Some(d) = ob.query_log_retention_days {
        if !(1..=3650).contains(&d) {
            return Err(Error::invalid_input(
                "observability.query_log_retention_days must be within 1..=3650",
            ));
        }
        query_log.retention = Duration::from_secs(d * 86_400);
    }
    if let Some(n) = ob.query_log_queue {
        if !(1..=1_000_000).contains(&n) {
            return Err(Error::invalid_input(
                "observability.query_log_queue must be within 1..=1000000",
            ));
        }
        query_log.queue = n;
    }
    let m = &file.memory;
    let d = crate::memory::MemorySettings::default();
    let memory = crate::memory::MemorySettings {
        max_server_memory_bytes: m.max_server_memory_bytes,
        max_server_memory_ratio: m
            .max_server_memory_ratio
            .unwrap_or(d.max_server_memory_ratio),
        ingest_ratio: m.ingest_memory_ratio.unwrap_or(d.ingest_ratio),
        cache_ratio: m.cache_memory_ratio.unwrap_or(d.cache_ratio),
        merge_ratio: m.merge_memory_ratio.unwrap_or(d.merge_ratio),
        max_query_memory_bytes: m.max_query_memory_bytes,
        wait_timeout: m
            .memory_wait_timeout_ms
            .map_or(d.wait_timeout, Duration::from_millis),
        untracked_warn_ratio: m.untracked_warn_ratio.unwrap_or(d.untracked_warn_ratio),
    };
    memory.validate()?;
    Ok(Resolved {
        memory,
        query_log,
        data_dir,
        listen,
        durability,
        tls,
        insecure_allow_remote_without_tls: insecure,
        threads: {
            let plan = crate::ThreadPlan::new(
                net_threads,
                scan_threads,
                blocking_threads,
                server.max_concurrent_ingest,
                server.max_concurrent_queries,
            )?;
            server.blocking_threads = plan.blocking;
            plan
        },
        server,
        engine,
        scan_threads,
        max_open_files,
        io_max_inflight_per_device,
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
                "scan_threads": self.threads.scan,
                "net_threads": self.threads.net,
                "blocking_threads": self.threads.blocking,
                "thread_budget": self.threads.budget,
            },
            "storage": {
                "memtable_max_bytes": self.engine.memtable_max_bytes,
                "max_open_files": self.max_open_files,
                "io_max_inflight_per_device": self.io_max_inflight_per_device
                    .unwrap_or(stratadb_common::io::DEFAULT_MAX_INFLIGHT_PER_DEVICE),
                "group_commit_adaptive": self.engine.wal.adaptive_group_delay,
                "wal_format_version": self.engine.wal.format_version,
                "wal_segment_max_bytes": self.engine.wal.segment_max_bytes,
                "disk_reserve_bytes": self.engine.disk_reserve_bytes,
                "disk_reserve_ratio": self.engine.disk_reserve_ratio,
                "default_retention_days": self.engine.default_retention.map(|d| d.as_secs() / 86_400),
                "bloom_attribute_keys": self.engine.bloom_attribute_keys,
                "ngram_index": self.engine.ngram_enabled,
                "ngram_attribute_keys": self.engine.ngram_attribute_keys,
            },
            "memory": {
                "max_server_memory_bytes": self.memory.max_server_memory_bytes,
                "max_server_memory_ratio": self.memory.max_server_memory_ratio,
                "ingest_memory_ratio": self.memory.ingest_ratio,
                "cache_memory_ratio": self.memory.cache_ratio,
                "merge_memory_ratio": self.memory.merge_ratio,
                "max_query_memory_bytes": self.memory.max_query_memory_bytes,
                "memory_wait_timeout_ms": self.memory.wait_timeout.as_millis() as u64,
                "untracked_warn_ratio": self.memory.untracked_warn_ratio,
            },
            "observability": {
                "query_log": self.query_log.enabled,
                "query_log_flush_ms": self.query_log.flush_interval.as_millis() as u64,
                "query_log_retention_days": self.query_log.retention.as_secs() / 86_400,
                "query_log_queue": self.query_log.queue,
            },
            "backup_dir": self.server.backup_root,
            "encryption": self.engine.encryption.as_ref().map(|p| p.describe()),
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
             [limits]\nmax_query_rows = 5\n[storage]\ndefault_retention_days = 7\nbloom_attribute_keys = [\"k\"]\nngram_index = false\nngram_attribute_keys = [\"msg\"]\n",
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
        assert!(!r.engine.ngram_enabled);
        assert_eq!(r.engine.ngram_attribute_keys, vec!["msg"]);
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
    fn encryption_section() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("kek.hex");
        let f = parse(&format!(
            "config_version = 1\ndata_dir = \"/d\"\n[encryption]\nkey_file = \"{}\"\n",
            key.display()
        ))
        .unwrap();
        assert!(
            resolve(&f, &Overrides::default()).is_err(),
            "missing key file"
        );
        stratadb_crypto::generate_key_file(&key).unwrap();
        let r = resolve(&f, &Overrides::default()).unwrap();
        assert!(r.engine.encryption.is_some());
        let shown = r.display().to_string();
        assert!(shown.contains("kek.hex"), "{shown}");
        let secret = std::fs::read_to_string(&key).unwrap();
        assert!(
            !shown.contains(secret.trim()),
            "key material must never be displayed"
        );
    }

    #[test]
    fn thread_plan_respects_the_budget() {
        use crate::ThreadPlan;
        // 10 CPUs: budget 52; defaults 10 net + 10 scan + 6 fixed, blocking
        // fills the rest up to one per admitted request.
        let p = ThreadPlan::for_cpus(10, None, None, None, 32, 16).unwrap();
        assert_eq!((p.budget, p.net, p.scan, p.blocking), (52, 10, 10, 26));
        assert_eq!(p.total(), 52);
        let small = ThreadPlan::for_cpus(10, None, None, None, 4, 2).unwrap();
        assert_eq!(small.blocking, 6, "no more than admitted requests");
        assert!(ThreadPlan::for_cpus(10, Some(40), None, None, 32, 16).is_err());
        assert!(
            ThreadPlan::for_cpus(10, None, None, Some(1), 32, 16).is_err(),
            ">= 2 blocking"
        );
        assert!(ThreadPlan::for_cpus(10, None, None, Some(40), 32, 16).is_err());
        let one = ThreadPlan::for_cpus(1, None, None, None, 32, 16).unwrap();
        assert!(one.total() <= one.budget && one.blocking >= 2);
        let r = |s: &str| resolve(&parse(s).unwrap(), &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        let ok = r(&format!(
            "{base}[limits]\nnet_threads = 2\nscan_threads = 2\nblocking_threads = 5\n"
        ))
        .unwrap();
        assert_eq!(
            (ok.threads.net, ok.threads.scan, ok.threads.blocking),
            (2, 2, 5)
        );
        assert_eq!(ok.server.blocking_threads, 5);
        let e = r(&format!("{base}[limits]\nblocking_threads = 5000\n")).unwrap_err();
        assert!(e.to_string().contains("budget"), "{e}");
    }

    #[test]
    fn group_commit_settings() {
        let r = |s: &str| resolve(&parse(s).unwrap(), &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        let d = r(base).unwrap();
        assert!(d.engine.wal.adaptive_group_delay);
        let x = r(&format!(
            "{base}[storage]\ngroup_commit_max_delay_ms = 5\ngroup_commit_adaptive = false\n"
        ))
        .unwrap();
        assert_eq!(
            x.engine.wal.durability,
            Durability::GroupCommit {
                max_delay: Duration::from_millis(5)
            }
        );
        assert!(!x.engine.wal.adaptive_group_delay);
        assert!(r(&format!(
            "{base}[storage]\ngroup_commit_max_delay_ms = 500\n"
        ))
        .is_err());
        // Only meaningful for group_commit: other modes are unchanged.
        let s = r(&format!(
            "{base}durability = \"sync\"\n[storage]\ngroup_commit_max_delay_ms = 5\n"
        ))
        .unwrap();
        assert_eq!(s.engine.wal.durability, Durability::Sync);
    }

    #[test]
    fn io_inflight_setting() {
        let r = |s: &str| resolve(&parse(s)?, &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        assert_eq!(r(base).unwrap().io_max_inflight_per_device, None);
        assert_eq!(
            r(&format!(
                "{base}[storage]\nio_max_inflight_per_device = 32\n"
            ))
            .unwrap()
            .io_max_inflight_per_device,
            Some(32)
        );
        for bad in [0, 5000] {
            assert!(r(&format!(
                "{base}[storage]\nio_max_inflight_per_device = {bad}\n"
            ))
            .is_err());
        }
    }

    #[test]
    fn wal_format_version_setting() {
        let r = |s: &str| resolve(&parse(s)?, &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        assert_eq!(r(base).unwrap().engine.wal.format_version, 3);
        assert_eq!(
            r(&format!("{base}[storage]\nwal_format_version = 2\n"))
                .unwrap()
                .engine
                .wal
                .format_version,
            2
        );
        for bad in [1, 4] {
            assert!(r(&format!("{base}[storage]\nwal_format_version = {bad}\n")).is_err());
        }
    }

    #[test]
    fn memory_settings() {
        let r = |s: &str| resolve(&parse(s)?, &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        assert_eq!(
            r(base).unwrap().memory,
            crate::memory::MemorySettings::default()
        );
        let x = r(&format!(
            "{base}[memory]\nmax_server_memory_bytes = 2147483648\ningest_memory_ratio = 0.2\n\
             max_query_memory_bytes = 134217728\nmemory_wait_timeout_ms = 500\n\
             [quotas]\ndefault_query_memory_bytes = 1000000\n[quotas.tenants.acme]\nquery_memory_bytes = 5000000\n"
        ))
        .unwrap();
        assert_eq!(x.memory.max_server_memory_bytes, Some(2 << 30));
        assert_eq!(x.memory.ingest_ratio, 0.2);
        assert_eq!(x.memory.wait_timeout, Duration::from_millis(500));
        let acme = stratadb_common::TenantId::new("acme").unwrap();
        let other = stratadb_common::TenantId::new("other").unwrap();
        assert_eq!(
            x.server.quotas.for_tenant(&acme).query_memory_bytes,
            Some(5_000_000)
        );
        assert_eq!(
            x.server.quotas.for_tenant(&other).query_memory_bytes,
            Some(1_000_000)
        );
        for bad in [
            "cache_memory_ratio = 0.7\ningest_memory_ratio = 0.3",
            "max_query_memory_bytes = 1000",
            "max_server_memory_ratio = 1.5",
            "unknown_memory_knob = 1",
        ] {
            assert!(r(&format!("{base}[memory]\n{bad}\n")).is_err(), "{bad}");
        }
    }

    #[test]
    fn observability_settings() {
        let r = |s: &str| resolve(&parse(s)?, &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        let d = r(base).unwrap().query_log;
        assert_eq!(d, crate::query_log::QueryLogConfig::default());
        assert!(d.enabled);
        assert_eq!(d.flush_interval, Duration::from_millis(7500));
        let x = r(&format!(
            "{base}[observability]\nquery_log = false\nquery_log_flush_ms = 100\n\
             query_log_retention_days = 30\nquery_log_queue = 50\n"
        ))
        .unwrap()
        .query_log;
        assert!(!x.enabled);
        assert_eq!(x.flush_interval, Duration::from_millis(100));
        assert_eq!(x.retention, Duration::from_secs(30 * 86_400));
        assert_eq!(x.queue, 50);
        for bad in [
            "query_log_flush_ms = 0",
            "query_log_retention_days = 0",
            "query_log_queue = 0",
            "query_log_typo = 1",
        ] {
            assert!(
                r(&format!("{base}[observability]\n{bad}\n")).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn max_open_files_setting() {
        let r = |s: &str| resolve(&parse(s).unwrap(), &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        assert_eq!(r(base).unwrap().max_open_files, None);
        assert_eq!(
            r(&format!("{base}[storage]\nmax_open_files = 512\n"))
                .unwrap()
                .max_open_files,
            Some(512)
        );
        assert!(r(&format!("{base}[storage]\nmax_open_files = 4\n")).is_err());
    }

    #[test]
    fn scan_threads_setting() {
        let r = |s: &str| resolve(&parse(s).unwrap(), &Overrides::default());
        let base = "config_version = 1\ndata_dir = \"/d\"\n";
        assert_eq!(r(base).unwrap().scan_threads, None);
        let ok = r(&format!("{base}[limits]\nscan_threads = 4\n")).unwrap();
        assert_eq!(ok.scan_threads, Some(4));
        assert!(r(&format!("{base}[limits]\nscan_threads = 0\n")).is_err());
        assert!(r(&format!("{base}[limits]\nscan_threads = 100000\n")).is_err());
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
