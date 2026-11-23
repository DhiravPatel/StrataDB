//! StrataDB Rust client (spec §23.1). Blocking, connection-pooling, with
//! bounded retries that are safe because every ingest request carries an
//! idempotency key that is reused across its retries.
//!
//! ```no_run
//! use stratadb_client::{Client, ClientConfig};
//! let c = Client::new(ClientConfig::new("http://127.0.0.1:7070", "sdb_…")).unwrap();
//! let r = c.ingest(&[serde_json::json!({"service": "api", "severity": "ERROR", "body": "boom"})]).unwrap();
//! println!("committed at {} ({})", r.commit_seq, r.durability);
//! let q = c.query("SELECT count(*) AS n FROM logs").unwrap();
//! println!("{:?}", q.rows);
//! ```

use std::collections::hash_map::RandomState;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

/// Client configuration.
#[derive(Clone)]
pub struct ClientConfig {
    /// Base URL, e.g. `https://stratadb.example:7070`.
    pub endpoint: String,
    pub token: String,
    /// Per-attempt timeout (connect + request + response).
    pub timeout: Duration,
    /// Retries after the first attempt.
    pub max_retries: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    /// Gzip request bodies larger than 1 KiB.
    pub gzip: bool,
    /// Extra trusted CA certificates (PEM) in addition to webpki roots.
    pub ca_pem: Option<Vec<u8>>,
    /// Set to true to stop retrying / abort before the next attempt.
    pub cancel: Option<Arc<AtomicBool>>,
    pub metrics: Option<Arc<dyn MetricsHook>>,
}

impl fmt::Debug for ClientConfig {
    // Never print the token.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("endpoint", &self.endpoint)
            .field("timeout", &self.timeout)
            .field("max_retries", &self.max_retries)
            .field("gzip", &self.gzip)
            .finish_non_exhaustive()
    }
}

impl ClientConfig {
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into().trim_end_matches('/').to_owned(),
            token: token.into(),
            timeout: Duration::from_secs(30),
            max_retries: 5,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            gzip: true,
            ca_pem: None,
            cancel: None,
            metrics: None,
        }
    }
}

/// Observability hook: called once per HTTP attempt.
pub trait MetricsHook: Send + Sync {
    fn on_attempt(&self, operation: &str, attempt: u32, status: Option<u16>, elapsed: Duration);
}

/// Structured client errors.
#[derive(Debug, Clone, PartialEq)]
pub enum ClientError {
    /// The server answered with an error (`kind` is StrataDB's error kind).
    Server {
        status: u16,
        kind: String,
        message: String,
        retryable: bool,
    },
    /// Network/TLS/timeout failure; the request may or may not have arrived.
    Transport(String),
    /// Bad client configuration or arguments.
    InvalidInput(String),
    Cancelled,
    /// All attempts failed with retryable errors.
    RetriesExhausted {
        attempts: u32,
        last: Box<ClientError>,
    },
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Server {
                status,
                kind,
                message,
                ..
            } => write!(f, "server error {status} ({kind}): {message}"),
            ClientError::Transport(m) => write!(f, "transport error: {m}"),
            ClientError::InvalidInput(m) => write!(f, "invalid input: {m}"),
            ClientError::Cancelled => f.write_str("cancelled"),
            ClientError::RetriesExhausted { attempts, last } => {
                write!(f, "gave up after {attempts} attempts: {last}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    pub fn is_retryable(&self) -> bool {
        match self {
            ClientError::Server { retryable, .. } => *retryable,
            ClientError::Transport(_) => true,
            _ => false,
        }
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// One rejected record of an ingest request.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordError {
    pub index: u64,
    pub kind: String,
    pub message: String,
}

/// Commit receipt (see docs/api/http.md).
#[derive(Debug, Clone, PartialEq)]
pub struct IngestReceipt {
    pub accepted: u64,
    pub rejected: u64,
    pub errors: Vec<RecordError>,
    pub commit_seq: u64,
    /// `wal_synced` or `wal_written`.
    pub durability: String,
    /// True if the server recognised a retry of an already committed request.
    pub duplicate: bool,
    pub idempotency_key: String,
    pub attempts: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    /// Missing values are absent from the row objects; null is `Value::Null`.
    pub rows: Vec<Map<String, Value>>,
    pub explain: Option<String>,
    pub stats: Value,
}

/// The client. Cheap to clone; connections are pooled.
#[derive(Clone)]
pub struct Client {
    agent: ureq::Agent,
    cfg: ClientConfig,
    key_seed: u64,
    key_counter: Arc<AtomicU64>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

fn random_u64() -> u64 {
    RandomState::new().build_hasher().finish()
}

impl Client {
    pub fn new(cfg: ClientConfig) -> Result<Self> {
        if !(cfg.endpoint.starts_with("http://") || cfg.endpoint.starts_with("https://")) {
            return Err(ClientError::InvalidInput(
                "endpoint must start with http:// or https://".into(),
            ));
        }
        if cfg.token.is_empty() {
            return Err(ClientError::InvalidInput("token is empty".into()));
        }
        let mut b = ureq::Agent::config_builder()
            .timeout_global(Some(cfg.timeout))
            .http_status_as_error(false);
        if let Some(pem) = &cfg.ca_pem {
            let certs: Vec<ureq::tls::Certificate<'static>> = ureq::tls::parse_pem(pem)
                .filter_map(|item| match item {
                    Ok(ureq::tls::PemItem::Certificate(c)) => Some(Ok(c)),
                    Ok(_) => None,
                    Err(e) => Some(Err(e)),
                })
                .collect::<std::result::Result<_, _>>()
                .map_err(|e| ClientError::InvalidInput(format!("invalid CA PEM: {e}")))?;
            if certs.is_empty() {
                return Err(ClientError::InvalidInput(
                    "CA PEM contains no certificates".into(),
                ));
            }
            b = b.tls_config(
                ureq::tls::TlsConfig::builder()
                    .root_certs(ureq::tls::RootCerts::new_with_certs(&certs))
                    .build(),
            );
        }
        Ok(Self {
            agent: b.build().into(),
            cfg,
            key_seed: random_u64(),
            key_counter: Arc::new(AtomicU64::new(0)),
        })
    }

    /// A fresh idempotency key (process-random seed + counter).
    pub fn new_idempotency_key(&self) -> String {
        let n = self.key_counter.fetch_add(1, Ordering::Relaxed);
        format!("sdk-{:016x}-{:016x}-{n}", self.key_seed, random_u64())
    }

    fn cancelled(&self) -> bool {
        self.cfg
            .cancel
            .as_ref()
            .is_some_and(|c| c.load(Ordering::Relaxed))
    }

    fn backoff(&self, attempt: u32, retry_after: Option<Duration>) -> Duration {
        let cap = self.cfg.max_backoff;
        let exp = self
            .cfg
            .base_backoff
            .saturating_mul(1u32 << attempt.min(16))
            .min(cap);
        // Full jitter.
        let jittered = Duration::from_nanos(random_u64() % (exp.as_nanos() as u64).max(1));
        retry_after.map_or(jittered, |r| r.min(cap).max(jittered))
    }

    /// One logical operation with retries. `send` performs one attempt.
    fn with_retries(
        &self,
        op: &str,
        mut send: impl FnMut() -> std::result::Result<(u16, Option<Duration>, String), String>,
    ) -> Result<(String, u32)> {
        let mut attempt = 0u32;
        loop {
            if self.cancelled() {
                return Err(ClientError::Cancelled);
            }
            let start = Instant::now();
            let result = send();
            let status = result.as_ref().ok().map(|r| r.0);
            if let Some(m) = &self.cfg.metrics {
                m.on_attempt(op, attempt, status, start.elapsed());
            }
            let (err, retry_after) = match result {
                Ok((s, _, body)) if (200..300).contains(&s) => return Ok((body, attempt + 1)),
                Ok((s, ra, body)) => {
                    let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                    let retryable = v["retryable"]
                        .as_bool()
                        .unwrap_or(matches!(s, 429 | 502 | 503 | 504));
                    (
                        ClientError::Server {
                            status: s,
                            kind: v["error"].as_str().unwrap_or("unknown").to_owned(),
                            message: v["message"].as_str().unwrap_or(&body).to_owned(),
                            retryable,
                        },
                        ra,
                    )
                }
                Err(e) => (ClientError::Transport(e), None),
            };
            if !err.is_retryable() {
                return Err(err);
            }
            if attempt >= self.cfg.max_retries {
                return Err(ClientError::RetriesExhausted {
                    attempts: attempt + 1,
                    last: Box::new(err),
                });
            }
            std::thread::sleep(self.backoff(attempt, retry_after));
            attempt += 1;
        }
    }

    fn post(
        &self,
        path: &str,
        ctype: &str,
        body: &[u8],
        key: Option<&str>,
    ) -> std::result::Result<(u16, Option<Duration>, String), String> {
        let url = format!("{}{path}", self.cfg.endpoint);
        let mut req = self
            .agent
            .post(&url)
            .header("Authorization", &format!("Bearer {}", self.cfg.token))
            .header("Content-Type", ctype);
        if let Some(k) = key {
            req = req.header("Idempotency-Key", k);
        }
        let compressed;
        let payload: &[u8] = if self.cfg.gzip && body.len() > 1024 {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            e.write_all(body).map_err(|e| e.to_string())?;
            compressed = e.finish().map_err(|e| e.to_string())?;
            req = req.header("Content-Encoding", "gzip");
            &compressed
        } else {
            body
        };
        let mut resp = req.send(payload).map_err(|e| e.to_string())?;
        read_response(&mut resp)
    }

    /// Ingest events (JSON objects, see docs/api/ingestion.md) as one atomic
    /// batch with an automatically generated idempotency key.
    pub fn ingest(&self, events: &[Value]) -> Result<IngestReceipt> {
        let key = self.new_idempotency_key();
        self.ingest_with_key(events, &key)
    }

    /// Ingest with a caller-chosen idempotency key (reuse it to retry safely
    /// across process restarts within the server's dedup window).
    pub fn ingest_with_key(&self, events: &[Value], key: &str) -> Result<IngestReceipt> {
        if events.is_empty() {
            return Err(ClientError::InvalidInput("no events".into()));
        }
        let body = serde_json::to_vec(&Value::Array(events.to_vec()))
            .map_err(|e| ClientError::InvalidInput(e.to_string()))?;
        let (text, attempts) = self.with_retries("ingest", || {
            self.post("/api/v1/ingest/batch", "application/json", &body, Some(key))
        })?;
        let v: Value = serde_json::from_str(&text)
            .map_err(|e| ClientError::Transport(format!("bad response: {e}")))?;
        Ok(IngestReceipt {
            accepted: v["accepted"].as_u64().unwrap_or(0),
            rejected: v["rejected"].as_u64().unwrap_or(0),
            errors: v["errors"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .map(|e| RecordError {
                            index: e["index"].as_u64().unwrap_or(0),
                            kind: e["error"].as_str().unwrap_or_default().to_owned(),
                            message: e["message"].as_str().unwrap_or_default().to_owned(),
                        })
                        .collect()
                })
                .unwrap_or_default(),
            commit_seq: v["commit_seq"].as_u64().unwrap_or(0),
            durability: v["durability"].as_str().unwrap_or_default().to_owned(),
            duplicate: v["duplicate"].as_bool().unwrap_or(false),
            idempotency_key: key.to_owned(),
            attempts,
        })
    }

    /// Run a query. Queries are read-only and retried on retryable errors.
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        let body = json!({"query": sql}).to_string();
        let (text, _) = self.with_retries("query", || {
            self.post("/api/v1/query", "application/json", body.as_bytes(), None)
        })?;
        let v: Value = serde_json::from_str(&text)
            .map_err(|e| ClientError::Transport(format!("bad response: {e}")))?;
        Ok(QueryResult {
            columns: v["columns"]
                .as_array()
                .map(|c| {
                    c.iter()
                        .filter_map(|x| x.as_str().map(str::to_owned))
                        .collect()
                })
                .unwrap_or_default(),
            rows: v["rows"]
                .as_array()
                .map(|r| r.iter().filter_map(|x| x.as_object().cloned()).collect())
                .unwrap_or_default(),
            explain: v["explain"].as_str().map(str::to_owned),
            stats: v["stats"].clone(),
        })
    }

    /// Liveness check (no authentication needed server-side).
    pub fn health(&self) -> Result<()> {
        let url = format!("{}/api/v1/health", self.cfg.endpoint);
        let mut resp = self
            .agent
            .get(&url)
            .call()
            .map_err(|e| ClientError::Transport(e.to_string()))?;
        match read_response(&mut resp) {
            Ok((200, _, _)) => Ok(()),
            Ok((s, _, b)) => Err(ClientError::Server {
                status: s,
                kind: "unknown".into(),
                message: b,
                retryable: true,
            }),
            Err(e) => Err(ClientError::Transport(e)),
        }
    }
}

fn read_response(
    resp: &mut ureq::http::Response<ureq::Body>,
) -> std::result::Result<(u16, Option<Duration>, String), String> {
    let status = resp.status().as_u16();
    let retry_after = resp
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map(Duration::from_secs);
    let body = resp
        .body_mut()
        .read_to_string()
        .map_err(|e| e.to_string())?;
    Ok((status, retry_after, body))
}
