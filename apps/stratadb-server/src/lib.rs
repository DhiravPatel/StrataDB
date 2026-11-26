//! StrataDB HTTP API (spec §21; ADR-0018). Normative reference: docs/api/http.md.
//!
//! Every data endpoint authenticates a bearer token and derives the tenant
//! from it. The synchronous storage engine is called through
//! `spawn_blocking`, admitted by semaphores so overload yields `429` rather
//! than unbounded queueing.

pub mod audit;
pub mod auth;
pub mod config;
pub mod telemetry;
pub mod tls;

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value as J};
use stratadb_common::TenantId;
use stratadb_common::{time, Error, ErrorKind};
use stratadb_query::{execute, execute_page, Datum, PageRequest, QueryOptions};
use stratadb_schema::json::event_from_json;
use stratadb_schema::{Limits, Value};
use stratadb_storage::RetentionPolicy;
use stratadb_storage::{Engine, IdempotencyKey};
use tokio::sync::Semaphore;

use crate::auth::{Permission, Principal, TokenStore};

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub max_body_bytes: usize,
    pub max_concurrent_ingest: usize,
    pub max_concurrent_queries: usize,
    pub max_query_timeout: Duration,
    pub max_query_rows: usize,
    /// Failed authentication attempts allowed per second (global) before 429.
    pub auth_failures_per_sec: u32,
    pub limits: Limits,
    /// Maximum size of a request body after gzip decompression.
    pub max_decompressed_bytes: usize,
    /// Directory under which `POST /admin/backup` creates named backups.
    /// `None` disables HTTP-triggered backups.
    pub backup_root: Option<std::path::PathBuf>,
    /// How often the token file is checked for changes (hot reload of
    /// created/revoked/rotated tokens without restart).
    pub token_reload_interval: Duration,
    /// Per-tenant ingestion rate and storage quotas (spec §3.4).
    pub quotas: Quotas,
}

/// Limits for one tenant; `None` = unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TenantQuota {
    pub ingest_events_per_sec: Option<f64>,
    /// Bucket size; defaults to one second of `ingest_events_per_sec`.
    pub ingest_burst_events: Option<f64>,
    /// Segments + memtable bytes.
    pub storage_bytes: Option<u64>,
}

/// Default quota plus per-tenant overrides (field-wise).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Quotas {
    pub default: TenantQuota,
    pub tenants: BTreeMap<String, TenantQuota>,
}

impl Quotas {
    pub fn for_tenant(&self, t: &TenantId) -> TenantQuota {
        let d = self.default;
        match self.tenants.get(t.as_str()) {
            None => d,
            Some(o) => TenantQuota {
                ingest_events_per_sec: o.ingest_events_per_sec.or(d.ingest_events_per_sec),
                ingest_burst_events: o.ingest_burst_events.or(d.ingest_burst_events),
                storage_bytes: o.storage_bytes.or(d.storage_bytes),
            },
        }
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_body_bytes: 8 * 1024 * 1024,
            max_concurrent_ingest: 32,
            max_concurrent_queries: 16,
            max_query_timeout: Duration::from_secs(60),
            max_query_rows: 100_000,
            auth_failures_per_sec: 50,
            limits: Limits::default(),
            backup_root: None,
            max_decompressed_bytes: 32 * 1024 * 1024,
            token_reload_interval: Duration::from_secs(2),
            quotas: Quotas::default(),
        }
    }
}

struct AuthLimiter {
    window: Instant,
    failures: u32,
}

#[derive(Clone)]
pub struct AppState {
    engine: Arc<Engine>,
    tokens: Arc<std::sync::RwLock<TokenStore>>,
    token_checked: Arc<std::sync::Mutex<Instant>>,
    buckets: Arc<std::sync::Mutex<std::collections::HashMap<TenantId, (f64, Instant)>>>,
    quota_rejections: Arc<std::sync::atomic::AtomicU64>,
    cfg: Arc<ServerConfig>,
    ingest_sem: Arc<Semaphore>,
    query_sem: Arc<Semaphore>,
    running: Arc<std::sync::Mutex<std::collections::HashMap<String, Running>>>,
    auth_limiter: Arc<std::sync::Mutex<AuthLimiter>>,
    audit: Option<Arc<audit::AuditLog>>,
    metrics: Arc<telemetry::ServerMetrics>,
}

impl std::fmt::Debug for AppState {
    // Never print token material.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("cfg", &self.cfg)
            .finish_non_exhaustive()
    }
}

impl AppState {
    /// Reload the token file if it changed (checked at most every
    /// `token_reload_interval`). A failed reload keeps the previous tokens.
    fn maybe_reload_tokens(&self) {
        {
            let mut last = self.token_checked.lock().unwrap_or_else(|e| e.into_inner());
            if last.elapsed() < self.cfg.token_reload_interval {
                return;
            }
            *last = Instant::now();
        }
        let (changed, dir) = {
            let t = self.tokens.read().unwrap_or_else(|e| e.into_inner());
            (t.changed_on_disk(), t.data_dir().map(Path::to_path_buf))
        };
        if let (true, Some(dir)) = (changed, dir) {
            match TokenStore::load(&dir) {
                Ok(fresh) => {
                    *self.tokens.write().unwrap_or_else(|e| e.into_inner()) = fresh;
                    self.audit("tokens_reloaded", None, json!({}));
                }
                Err(e) => {
                    tracing::error!(component = "auth", error = %e, "token reload failed; keeping previous tokens")
                }
            }
        }
    }

    /// Enforce the tenant's storage quota and ingestion rate for `n` events.
    fn check_quota(&self, tenant: &TenantId, n: usize) -> stratadb_common::Result<()> {
        let q = self.cfg.quotas.for_tenant(tenant);
        let reject = |msg: String| {
            self.quota_rejections
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(Error::resource_exhausted(msg))
        };
        if let Some(limit) = q.storage_bytes {
            let used = self.engine.tenant_usage(tenant).total_bytes();
            if used >= limit {
                return reject(format!(
                    "storage quota exceeded ({used} of {limit} bytes); free space via retention or raise the quota"
                ));
            }
        }
        if let Some(rate) = q.ingest_events_per_sec.filter(|r| *r > 0.0) {
            let burst = q.ingest_burst_events.unwrap_or(rate).max(1.0);
            if n as f64 > burst {
                return Err(Error::invalid_input(format!(
                    "batch of {n} events exceeds this tenant's burst limit of {burst}; split the batch"
                )));
            }
            let mut b = self.buckets.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            let (tokens, last) = b.entry(tenant.clone()).or_insert((burst, now));
            *tokens = (*tokens + rate * now.duration_since(*last).as_secs_f64()).min(burst);
            *last = now;
            if *tokens < n as f64 {
                let wait = (n as f64 - *tokens) / rate;
                drop(b);
                return reject(format!(
                    "ingestion rate limit ({rate} events/s) exceeded; retry in {wait:.2} s"
                ));
            }
            *tokens -= n as f64;
        }
        Ok(())
    }

    /// Enable the audit log (spec §18.7).
    pub fn with_audit(mut self, log: audit::AuditLog) -> Self {
        self.audit = Some(Arc::new(log));
        self
    }

    /// Best-effort audit record (security events must not take the API down).
    fn audit(&self, event: &str, p: Option<&Principal>, details: J) {
        if let Err(e) = self.audit_required(event, p, details) {
            tracing::error!(component = "audit", error = %e, "audit record could not be written");
        }
    }

    /// Audit record that must be durable before an administrative action runs.
    fn audit_required(
        &self,
        event: &str,
        p: Option<&Principal>,
        details: J,
    ) -> stratadb_common::Result<()> {
        match &self.audit {
            None => Ok(()),
            Some(log) => log.record(
                event,
                p.map(|p| (p.token_id.as_str(), p.tenant.as_str())),
                details,
            ),
        }
    }

    pub fn new(engine: Arc<Engine>, tokens: TokenStore, cfg: ServerConfig) -> Self {
        Self {
            engine,
            tokens: Arc::new(std::sync::RwLock::new(tokens)),
            token_checked: Arc::new(std::sync::Mutex::new(Instant::now())),
            buckets: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            quota_rejections: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            ingest_sem: Arc::new(Semaphore::new(cfg.max_concurrent_ingest)),
            query_sem: Arc::new(Semaphore::new(cfg.max_concurrent_queries)),
            running: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            auth_limiter: Arc::new(std::sync::Mutex::new(AuthLimiter {
                window: Instant::now(),
                failures: 0,
            })),
            cfg: Arc::new(cfg),
            audit: None,
            metrics: Arc::default(),
        }
    }
}

/// Request correlation + access log (spec §25.2): assigns `x-request-id`
/// (a client-supplied id is kept if it uses `[A-Za-z0-9_-]{1,64}`), returns it
/// in the response, and logs method, path, status and latency — never
/// headers, bodies, or credentials.
async fn request_context(req: axum::extract::Request, next: axum::middleware::Next) -> Response {
    let id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| {
            !v.is_empty()
                && v.len() <= 64
                && v.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
        .map(str::to_owned)
        .unwrap_or_else(|| {
            use std::hash::{BuildHasher, Hasher};
            format!(
                "r{:016x}",
                std::collections::hash_map::RandomState::new()
                    .build_hasher()
                    .finish()
            )
        });
    let (method, path) = (req.method().clone(), req.uri().path().to_owned());
    let started = Instant::now();
    let mut resp = next.run(req).await;
    if let Ok(v) = axum::http::HeaderValue::from_str(&id) {
        resp.headers_mut().insert("x-request-id", v);
    }
    let status = resp.status().as_u16();
    let ms = started.elapsed().as_secs_f64() * 1000.0;
    if status >= 500 {
        tracing::warn!(target: "access", request_id = %id, %method, path = %path, status, elapsed_ms = ms, "request");
    } else {
        tracing::info!(target: "access", request_id = %id, %method, path = %path, status, elapsed_ms = ms, "request");
    }
    resp
}

/// Build the router.
pub fn router(state: AppState) -> Router {
    let limit = state.cfg.max_body_bytes;
    Router::new()
        .route("/api/v1/health", get(health))
        .route("/api/v1/ready", get(ready))
        .route("/api/v1/version", get(version))
        .route("/api/v1/ingest/batch", post(ingest))
        .route("/api/v1/ingest", post(ingest))
        .route("/v1/logs", post(otlp_logs))
        .route("/api/v1/query", post(query))
        .route("/api/v1/query/{id}", get(get_query).delete(cancel_query))
        .route("/api/v1/metrics", get(metrics))
        .route("/api/v1/admin/compaction", post(admin_compaction))
        .route("/api/v1/admin/backup", post(admin_backup))
        .route(
            "/api/v1/retention",
            get(get_retention)
                .put(put_retention)
                .delete(delete_retention),
        )
        .layer(axum::middleware::from_fn(request_context))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Errors

fn status_of(k: ErrorKind) -> StatusCode {
    match k {
        ErrorKind::InvalidInput | ErrorKind::Unsupported | ErrorKind::Cancelled => {
            StatusCode::BAD_REQUEST
        }
        ErrorKind::Unauthenticated => StatusCode::UNAUTHORIZED,
        ErrorKind::PermissionDenied => StatusCode::FORBIDDEN,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::AlreadyExists => StatusCode::CONFLICT,
        ErrorKind::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
        ErrorKind::Unavailable | ErrorKind::Indeterminate => StatusCode::SERVICE_UNAVAILABLE,
        ErrorKind::Timeout => StatusCode::GATEWAY_TIMEOUT,
        ErrorKind::Corruption | ErrorKind::Io | ErrorKind::Internal => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// Convert an error into a response. Internal details (paths, OS errors) are
/// logged server-side and replaced by a generic message (spec §18.9).
fn error_response(e: &Error) -> Response {
    let k = e.kind();
    let msg = match k {
        ErrorKind::Corruption | ErrorKind::Io | ErrorKind::Internal => {
            tracing::error!(component = "server", kind = k.as_str(), detail = %e, "internal error (details withheld from client)");
            "internal storage error; see server logs".to_owned()
        }
        ErrorKind::Indeterminate => {
            "the write outcome is unknown; retry with the same data (at-least-once)".to_owned()
        }
        _ => e.to_string(),
    };
    let body = Json(json!({"error": k.as_str(), "message": msg, "retryable": k.is_retryable()}));
    let status = status_of(k);
    if k.is_retryable() {
        (status, [(header::RETRY_AFTER, "1")], body).into_response()
    } else {
        (status, body).into_response()
    }
}

// Returning the ready-made error response keeps handlers simple; built at most once per request.
#[allow(clippy::result_large_err)]
fn authenticate(
    st: &AppState,
    headers: &HeaderMap,
    perm: Permission,
) -> Result<Principal, Response> {
    let presented = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let result = match presented {
        Some(tok) => {
            st.maybe_reload_tokens();
            st.tokens
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .verify(tok)
        }
        None => Err(Error::new(
            ErrorKind::Unauthenticated,
            "missing bearer token",
        )),
    };
    match result {
        Ok(p) => p.require(perm).map(|()| p.clone()).map_err(|e| {
            st.audit(
                "permission_denied",
                Some(&p),
                json!({"permission": perm.as_str()}),
            );
            error_response(&e)
        }),
        Err(e) => {
            // Never record the presented credential.
            st.audit(
                "auth_failure",
                None,
                json!({"permission": perm.as_str(), "reason": e.kind().as_str()}),
            );
            let limited = {
                let mut l = st.auth_limiter.lock().unwrap_or_else(|p| p.into_inner());
                if l.window.elapsed() >= Duration::from_secs(1) {
                    l.window = Instant::now();
                    l.failures = 0;
                }
                l.failures += 1;
                l.failures > st.cfg.auth_failures_per_sec
            };
            if limited {
                return Err(error_response(&Error::resource_exhausted(
                    "too many failed authentication attempts",
                )));
            }
            Err(error_response(&e))
        }
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> stratadb_common::Result<T> + Send + 'static,
) -> stratadb_common::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::internal(format!("worker task failed: {e}")))?
}

// ---------------------------------------------------------------------------
// Handlers

async fn health() -> Json<J> {
    Json(json!({"status": "ok"}))
}

async fn ready(State(st): State<AppState>) -> Response {
    match st.engine.failure() {
        None => Json(json!({"status": "ready"})).into_response(),
        Some(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "failed"})),
        )
            .into_response(),
    }
}

async fn version() -> Json<J> {
    Json(json!({
        "server": env!("CARGO_PKG_VERSION"),
        "api": "v1",
        "formats": {"wal": 1, "segment": 1, "manifest": 1, "event_encoding": 1},
    }))
}

/// Decode the request body: gzip (Content-Encoding) with a hard cap on the
/// decompressed size (decompression-bomb protection), then UTF-8.
#[allow(clippy::result_large_err)]
fn decode_body(headers: &HeaderMap, body: &Bytes, max: usize) -> Result<Vec<u8>, Error> {
    let enc = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("identity")
        .trim()
        .to_ascii_lowercase();
    let raw: Vec<u8> = match enc.as_str() {
        "identity" | "" => body.to_vec(),
        "gzip" => {
            use std::io::Read;
            let mut out = Vec::new();
            flate2::read::GzDecoder::new(&body[..])
                .take(max as u64 + 1)
                .read_to_end(&mut out)
                .map_err(|e| Error::invalid_input(format!("invalid gzip body: {e}")))?;
            if out.len() > max {
                // Not retryable: resending the same body cannot succeed.
                return Err(Error::invalid_input(format!(
                    "decompressed body exceeds {max} bytes"
                )));
            }
            out
        }
        other => {
            return Err(Error::unsupported(format!(
                "Content-Encoding {other:?} is not supported (use gzip)"
            )))
        }
    };
    Ok(raw)
}

#[allow(clippy::result_large_err)]
fn utf8(raw: Vec<u8>) -> Result<String, Error> {
    String::from_utf8(raw).map_err(|_| Error::invalid_input("request body is not UTF-8"))
}

#[allow(clippy::result_large_err)]
fn idempotency_key(headers: &HeaderMap, body: &[u8]) -> Result<Option<IdempotencyKey>, Error> {
    match headers.get("idempotency-key").map(|v| v.to_str()) {
        None => Ok(None),
        // The digest covers the (decompressed) body: a byte-identical retry is a
        // duplicate, anything else under the same key conflicts.
        Some(Ok(k)) => {
            use sha2::Digest;
            let digest: [u8; 32] = sha2::Sha256::digest(body).into();
            IdempotencyKey::new(k, digest).map(Some)
        }
        Some(Err(_)) => Err(Error::invalid_input("Idempotency-Key must be ASCII")),
    }
}

/// Outcome of committing the valid records of one request.
struct Committed {
    receipt: stratadb_storage::WriteReceipt,
    accepted: usize,
}

async fn commit(
    st: &AppState,
    principal: &Principal,
    permit: tokio::sync::OwnedSemaphorePermit,
    events: Vec<stratadb_schema::Event>,
    idem: Option<IdempotencyKey>,
) -> stratadb_common::Result<Committed> {
    let n = events.len();
    st.check_quota(&principal.tenant, n)?;
    let engine = Arc::clone(&st.engine);
    let tenant = principal.tenant.clone();
    let receipt = blocking(move || {
        let _permit = permit;
        engine.write_batch_idempotent(&tenant, events, idem)
    })
    .await?;
    let accepted = if receipt.duplicate {
        receipt.accepted
    } else {
        n
    };
    Ok(Committed { receipt, accepted })
}

/// Ingest with acknowledgement-latency measurement (committed requests only).
async fn ingest(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let started = Instant::now();
    let r = ingest_inner(State(st.clone()), headers, body).await;
    if r.status().is_success() {
        st.metrics
            .ingest_acked(telemetry::Endpoint::Ingest, started.elapsed());
    }
    r
}

async fn ingest_inner(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    use telemetry::InsertStage;
    let stage = Instant::now();
    let principal = match authenticate(&st, &headers, Permission::Ingest) {
        Ok(p) => p,
        Err(r) => return r,
    };
    st.metrics.insert_stage(InsertStage::Auth, stage.elapsed());
    let stage = Instant::now();
    let Ok(permit) = Arc::clone(&st.ingest_sem).try_acquire_owned() else {
        return error_response(&Error::resource_exhausted(
            "too many concurrent ingest requests",
        ));
    };
    let text = match decode_body(&headers, &body, st.cfg.max_decompressed_bytes).and_then(utf8) {
        Ok(t) => t,
        Err(e) => return error_response(&e),
    };
    let idem = match idempotency_key(&headers, text.as_bytes()) {
        Ok(k) => k,
        Err(e) => return error_response(&e),
    };
    let is_json = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    let records: Vec<Result<J, String>> = if is_json {
        match serde_json::from_str::<J>(&text) {
            Ok(J::Array(items)) => items.into_iter().map(Ok).collect(),
            Ok(obj @ J::Object(_)) => vec![Ok(obj)],
            Ok(_) => {
                return error_response(&Error::invalid_input("expected a JSON object or array"))
            }
            Err(e) => return error_response(&Error::invalid_input(format!("invalid JSON: {e}"))),
        }
    } else {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str::<J>(l).map_err(|e| format!("invalid JSON: {e}")))
            .collect()
    };
    st.metrics
        .insert_stage(InsertStage::Decode, stage.elapsed());
    let stage = Instant::now();
    let now = time::now_unix_nanos();
    let mut events = Vec::new();
    let mut errors = Vec::new();
    for (i, r) in records.into_iter().enumerate() {
        let parsed = r
            .map_err(Error::invalid_input)
            .and_then(|v| event_from_json(&v, &principal.tenant, now, &st.cfg.limits));
        match parsed {
            Ok(ev) => events.push(ev),
            Err(e) => errors
                .push(json!({"index": i, "error": e.kind().as_str(), "message": e.to_string()})),
        }
    }
    st.metrics
        .insert_stage(InsertStage::Validate, stage.elapsed());
    let rejected = errors.len();
    errors.truncate(100);
    if events.is_empty() {
        let status = if rejected == 0 {
            StatusCode::BAD_REQUEST
        } else {
            StatusCode::UNPROCESSABLE_ENTITY
        };
        return (
            status,
            Json(json!({"accepted": 0, "rejected": rejected, "errors": errors, "message": "no valid events in request"})),
        )
            .into_response();
    }
    match commit(&st, &principal, permit, events, idem).await {
        Ok(c) => {
            let stage = Instant::now();
            let r = Json(json!({
                // For a duplicate, `accepted` and `commit_seq` describe the original commit.
                "accepted": c.accepted,
                "rejected": rejected, "errors": errors,
                "duplicate": c.receipt.duplicate,
                "commit_seq": c.receipt.commit_seq,
                // Explicit durability state (spec §2.1): never "stored successfully".
                "durability": c.receipt.level.as_str(), "visible_to_queries": true,
                "flushed_to_segments": false, "replicated": false,
            }))
            .into_response();
            st.metrics.insert_stage(InsertStage::Ack, stage.elapsed());
            r
        }
        Err(e) => error_response(&e),
    }
}

/// OTLP error body: `google.rpc.Status` (protobuf or JSON, matching the request).
fn otlp_error(e: &Error, proto: bool) -> Response {
    let code = match e.kind() {
        ErrorKind::InvalidInput | ErrorKind::Unsupported => 3,
        ErrorKind::Timeout => 4,
        ErrorKind::NotFound => 5,
        ErrorKind::AlreadyExists => 6,
        ErrorKind::PermissionDenied => 7,
        ErrorKind::ResourceExhausted => 8,
        ErrorKind::Cancelled => 1,
        ErrorKind::Unavailable | ErrorKind::Indeterminate => 14,
        ErrorKind::Unauthenticated => 16,
        ErrorKind::Corruption | ErrorKind::Io | ErrorKind::Internal => 13,
    };
    let mut r = error_response(e);
    let msg = match e.kind() {
        ErrorKind::Corruption | ErrorKind::Io | ErrorKind::Internal => {
            "internal storage error".to_owned()
        }
        _ => e.to_string(),
    };
    if proto {
        *r.body_mut() =
            axum::body::Body::from(stratadb_schema::otlp_proto::encode_status(code, &msg));
        r.headers_mut().insert(
            header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/x-protobuf"),
        );
    } else {
        *r.body_mut() = axum::body::Body::from(json!({"code": code, "message": msg}).to_string());
    }
    r
}

/// `POST /v1/logs` — OTLP/HTTP logs, protobuf or JSON (spec §7.6, docs/api/otlp.md).
/// OTLP/HTTP logs with acknowledgement-latency measurement.
async fn otlp_logs(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let started = Instant::now();
    let r = otlp_logs_inner(State(st.clone()), headers, body).await;
    if r.status().is_success() {
        st.metrics
            .ingest_acked(telemetry::Endpoint::Otlp, started.elapsed());
    }
    r
}

async fn otlp_logs_inner(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let ctype = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let proto = ctype.starts_with("application/x-protobuf");
    let principal = match authenticate(&st, &headers, Permission::Ingest) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if !proto && !ctype.starts_with("application/json") {
        let e = Error::unsupported("OTLP/HTTP accepts application/x-protobuf or application/json");
        let mut r = otlp_error(&e, false);
        *r.status_mut() = StatusCode::UNSUPPORTED_MEDIA_TYPE;
        return r;
    }
    let Ok(permit) = Arc::clone(&st.ingest_sem).try_acquire_owned() else {
        return otlp_error(
            &Error::resource_exhausted("too many concurrent ingest requests"),
            proto,
        );
    };
    let raw = match decode_body(&headers, &body, st.cfg.max_decompressed_bytes) {
        Ok(t) => t,
        Err(e) => return otlp_error(&e, proto),
    };
    let idem = match idempotency_key(&headers, &raw) {
        Ok(k) => k,
        Err(e) => return otlp_error(&e, proto),
    };
    let req: J = if proto {
        match stratadb_schema::otlp_proto::decode_export_request(&raw) {
            Ok(v) => v,
            Err(e) => return otlp_error(&e, true),
        }
    } else {
        match serde_json::from_slice(&raw) {
            Ok(v) => v,
            Err(e) => {
                return otlp_error(&Error::invalid_input(format!("invalid JSON: {e}")), false)
            }
        }
    };
    let batch = match stratadb_schema::otlp::map_export_request(
        &req,
        &principal.tenant,
        time::now_unix_nanos(),
        &st.cfg.limits,
    ) {
        Ok(b) => b,
        Err(e) => return otlp_error(&e, proto),
    };
    let mut events = Vec::new();
    let mut rejected = 0usize;
    let mut first_error: Option<String> = None;
    for r in batch.records {
        match r {
            Ok(e) => events.push(e),
            Err(e) => {
                rejected += 1;
                first_error.get_or_insert_with(|| e.to_string());
            }
        }
    }
    let respond = |extra: Vec<(&'static str, String)>,
                   rejected: usize,
                   msg: Option<String>|
     -> Response {
        let mut r = if proto {
            let b = stratadb_schema::otlp_proto::encode_export_response(
                rejected as u64,
                msg.as_deref().unwrap_or(""),
            );
            ([(header::CONTENT_TYPE, "application/x-protobuf")], b).into_response()
        } else if rejected == 0 {
            Json(json!({})).into_response()
        } else {
            Json(json!({"partialSuccess": {"rejectedLogRecords": rejected.to_string(), "errorMessage": msg.unwrap_or_default()}}))
                .into_response()
        };
        for (k, v) in extra {
            if let Ok(v) = axum::http::HeaderValue::from_str(&v) {
                r.headers_mut().insert(k, v);
            }
        }
        r
    };
    if events.is_empty() {
        // Nothing valid (or an empty request): OTLP treats this as success with
        // every record rejected; the records are not retryable.
        return respond(Vec::new(), rejected, first_error);
    }
    match commit(&st, &principal, permit, events, idem).await {
        Ok(c) => respond(
            vec![
                ("x-stratadb-commit-seq", c.receipt.commit_seq.to_string()),
                ("x-stratadb-durability", c.receipt.level.as_str().to_owned()),
                ("x-stratadb-duplicate", c.receipt.duplicate.to_string()),
                ("x-stratadb-accepted", c.accepted.to_string()),
            ],
            rejected,
            first_error,
        ),
        Err(e) => otlp_error(&e, proto),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRequest {
    query: String,
    /// Only the implicit default database exists in this version.
    database: Option<String>,
    timeout_ms: Option<u64>,
    max_rows: Option<usize>,
    /// Optional client-chosen id (tenant-id alphabet) so the query can be
    /// cancelled with `DELETE /api/v1/query/{id}` while it runs.
    query_id: Option<String>,
    /// Paginate a row query (ADR-0023): at most this many rows per page.
    page_size: Option<usize>,
    /// `next_cursor` from the previous page (requires the same query text).
    cursor: Option<String>,
}

/// A running query in the registry.
#[derive(Debug)]
struct Running {
    tenant: TenantId,
    cancel: Arc<std::sync::atomic::AtomicBool>,
    started: Instant,
    sql: String,
}

/// Removes a query from the registry when it finishes (also on panic/cancel).
struct RunningGuard {
    reg: Arc<std::sync::Mutex<std::collections::HashMap<String, Running>>>,
    id: String,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.reg
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.id);
    }
}

fn to_json(v: &Value) -> J {
    match v {
        Value::Null => J::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::UInt(u) => json!(u),
        Value::Float(f) => json!(f),
        Value::String(s) => json!(s),
        Value::Bytes(b) => json!(b.iter().map(|x| format!("{x:02x}")).collect::<String>()),
        Value::Timestamp(t) => json!(time::format_rfc3339(*t)),
        Value::List(l) => J::Array(l.iter().map(to_json).collect()),
        Value::Map(m) => J::Object(m.iter().map(|(k, v)| (k.clone(), to_json(v))).collect()),
    }
}

/// Unique output keys: duplicates get `#2`, `#3`, ...
fn unique_names(cols: &[String]) -> Vec<String> {
    let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
    cols.iter()
        .map(|c| {
            let n = seen.entry(c).or_insert(0);
            *n += 1;
            if *n == 1 {
                c.clone()
            } else {
                format!("{c}#{n}")
            }
        })
        .collect()
}

fn row_object(names: &[String], row: &[Datum]) -> J {
    let mut m = Map::new();
    for (k, d) in names.iter().zip(row) {
        // Missing values are omitted; null is rendered as JSON null.
        if let Some(v) = d {
            m.insert(k.clone(), to_json(v));
        }
    }
    J::Object(m)
}

/// Query with latency and outcome measurement. Unauthenticated/unauthorized
/// requests are not counted (they are not queries).
async fn query(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let started = Instant::now();
    let r = query_inner(State(st.clone()), headers, body).await;
    let status = r.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return r;
    }
    if status.is_success() {
        st.metrics.query_done("ok", started.elapsed());
        return r;
    }
    // Error bodies are small JSON objects carrying the error kind.
    let (parts, b) = r.into_parts();
    let bytes = axum::body::to_bytes(b, 64 * 1024).await.unwrap_or_default();
    let kind = serde_json::from_slice::<J>(&bytes)
        .ok()
        .and_then(|j| j["error"].as_str().map(str::to_owned))
        .unwrap_or_default();
    st.metrics.query_done(
        telemetry::ServerMetrics::outcome_of(&kind),
        started.elapsed(),
    );
    Response::from_parts(parts, axum::body::Body::from(bytes))
}

async fn query_inner(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let principal = match authenticate(&st, &headers, Permission::Query) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let req: QueryRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(&Error::invalid_input(format!("invalid query request: {e}")))
        }
    };
    if req.database.as_deref().is_some_and(|d| d != "default") {
        return error_response(&Error::unsupported("only the `default` database exists"));
    }
    let Ok(permit) = Arc::clone(&st.query_sem).try_acquire_owned() else {
        return error_response(&Error::resource_exhausted("too many concurrent queries"));
    };
    let timeout =
        Duration::from_millis(req.timeout_ms.unwrap_or(30_000)).min(st.cfg.max_query_timeout);
    let max_rows = req.max_rows.unwrap_or(10_000).min(st.cfg.max_query_rows);
    let query_id = match req.query_id.clone() {
        Some(id) => match TenantId::new(id.clone()) {
            Ok(_) => id,
            Err(_) => {
                return error_response(&Error::invalid_input(
                    "query_id must match [A-Za-z0-9_-]{1,64}",
                ))
            }
        },
        None => format!("q{:016x}", {
            use std::hash::{BuildHasher, Hasher};
            std::collections::hash_map::RandomState::new()
                .build_hasher()
                .finish()
        }),
    };
    // Registry keys are global; scope them by tenant so ids cannot collide or leak.
    let key = format!("{}/{}", principal.tenant, query_id);
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let mut reg = st.running.lock().unwrap_or_else(|e| e.into_inner());
        if reg.contains_key(&key) {
            return error_response(&Error::new(
                ErrorKind::AlreadyExists,
                "a query with this query_id is already running",
            ));
        }
        reg.insert(
            key.clone(),
            Running {
                tenant: principal.tenant.clone(),
                cancel: Arc::clone(&cancel),
                started: Instant::now(),
                sql: req.query.chars().take(200).collect(),
            },
        );
    }
    let guard = RunningGuard {
        reg: Arc::clone(&st.running),
        id: key,
    };
    let engine = Arc::clone(&st.engine);
    let tenant = principal.tenant.clone();
    let started = Instant::now();
    let res = blocking(move || {
        let queued = started.elapsed();
        let _permit = permit;
        let _guard = guard;
        let opts = QueryOptions {
            timeout,
            max_result_rows: max_rows,
            cancel: Some(cancel),
            ..QueryOptions::default()
        };
        let snap = engine.snapshot(&tenant);
        if req.page_size.is_some() || req.cursor.is_some() {
            let page = PageRequest {
                page_size: req.page_size.unwrap_or(max_rows).min(max_rows),
                cursor: req.cursor,
            };
            execute_page(&snap, &req.query, &opts, &page).map(|p| (p.output, p.next_cursor, queued))
        } else {
            execute(&snap, &req.query, &opts).map(|o| (o, None, queued))
        }
    })
    .await;
    match res {
        Ok((out, next_cursor, queued)) => {
            use telemetry::QueryStage;
            let serializing = Instant::now();
            st.metrics.query_work(&out.stats);
            let names = unique_names(&out.columns);
            let rows: Vec<J> = out.rows.iter().map(|r| row_object(&names, r)).collect();
            let s = &out.stats;
            let us = |d: Duration| d.as_secs_f64() * 1e6;
            let r = Json(json!({
                "query_id": query_id,
                "columns": names,
                "rows": rows,
                "explain": out.explain,
                "partial": false,
                "next_cursor": next_cursor,
                "stats": {
                    "elapsed_ms": started.elapsed().as_secs_f64() * 1000.0,
                    "rows_scanned": s.rows_scanned, "rows_matched": s.rows_matched,
                    "segment_bytes_read": s.bytes_read, "segments_total": s.segments_total,
                    "segments_scanned": s.segments_scanned,
                    "segments_pruned": s.segments_pruned_time + s.segments_pruned_keys + s.segments_pruned_severity + s.segments_pruned_bloom + s.segments_pruned_ngram + s.segments_pruned_cursor,
                    "indexes_used": s.indexes_used,
                    // Phase decomposition (spec VIII.1.4, STRATA-SYS-01-007):
                    // queue + parse + plan + exec (+ serialize) ≈ elapsed.
                    "queue_us": us(queued), "parse_us": us(s.parse),
                    "plan_us": us(s.plan), "exec_us": us(s.elapsed),
                },
            }))
            .into_response();
            let serialize = serializing.elapsed();
            for (stage, d) in [
                (QueryStage::Queue, queued),
                (QueryStage::Parse, s.parse),
                (QueryStage::Plan, s.plan),
                (QueryStage::Exec, s.elapsed),
                (QueryStage::Serialize, serialize),
            ] {
                st.metrics.query_stage(stage, d);
            }
            // Query log (no SQL text or values: they may carry tenant data).
            tracing::info!(
                target: "query", query_id = %query_id, tenant = %principal.tenant,
                queue_us = us(queued) as u64, parse_us = us(s.parse) as u64,
                plan_us = us(s.plan) as u64, exec_us = us(s.elapsed) as u64,
                serialize_us = us(serialize) as u64, rows_scanned = s.rows_scanned,
                rows_returned = rows.len(), segment_bytes_read = s.bytes_read, "query"
            );
            r
        }
        Err(e) => error_response(&e),
    }
}

async fn metrics(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = authenticate(&st, &headers, Permission::ClusterRead) {
        return r;
    }
    let s = st.engine.stats();
    let mut out = String::new();
    let mut m = |name: &str, kind: &str, help: &str, v: f64| {
        out.push_str(&format!(
            "# HELP stratadb_{name} {help}\n# TYPE stratadb_{name} {kind}\nstratadb_{name} {v}\n"
        ));
    };
    m(
        "events_written_total",
        "counter",
        "Events committed.",
        s.events_written as f64,
    );
    m(
        "batches_written_total",
        "counter",
        "Batches committed.",
        s.batches_written as f64,
    );
    m(
        "flushes_total",
        "counter",
        "Successful flushes.",
        s.flushes as f64,
    );
    m(
        "flush_failures_total",
        "counter",
        "Failed flush attempts.",
        s.flush_failures as f64,
    );
    m(
        "write_stalls_total",
        "counter",
        "Writes that waited for flush backlog.",
        s.write_stalls as f64,
    );
    m(
        "write_rejections_total",
        "counter",
        "Writes rejected by backpressure.",
        s.write_rejections as f64,
    );
    m(
        "duplicates_suppressed_total",
        "counter",
        "Idempotent retries answered from the dedup index.",
        s.duplicates_suppressed as f64,
    );
    m(
        "idempotency_keys",
        "gauge",
        "Idempotency keys currently remembered.",
        s.idempotency_keys as f64,
    );
    m(
        "idempotency_evictions_total",
        "counter",
        "Keys evicted early by the idempotency capacity bound.",
        s.idempotency_evictions as f64,
    );
    m(
        "quota_rejections_total",
        "counter",
        "Writes refused by per-tenant rate or storage quotas.",
        st.quota_rejections
            .load(std::sync::atomic::Ordering::Relaxed) as f64,
    );
    m(
        "disk_free_bytes",
        "gauge",
        "Free bytes on the data volume.",
        s.disk_free_bytes as f64,
    );
    m(
        "disk_write_rejections_total",
        "counter",
        "Writes refused by the disk reserve.",
        s.disk_write_rejections as f64,
    );
    m(
        "compactions_total",
        "counter",
        "Compaction merges.",
        s.compactions as f64,
    );
    m(
        "segments_deleted_total",
        "counter",
        "Obsolete segment files deleted.",
        s.segments_deleted as f64,
    );
    m(
        "retention_rows_deleted_total",
        "counter",
        "Rows deleted by retention.",
        s.retention_rows_deleted as f64,
    );
    m(
        "obsolete_segments",
        "gauge",
        "Obsolete segments pinned by snapshots.",
        s.obsolete_segments as f64,
    );
    m(
        "memtable_rows",
        "gauge",
        "Rows in the active memtable.",
        s.active_rows as f64,
    );
    m(
        "memtable_bytes",
        "gauge",
        "Approximate active memtable bytes.",
        s.active_bytes as f64,
    );
    m(
        "frozen_memtables",
        "gauge",
        "Memtables awaiting flush.",
        s.frozen_memtables as f64,
    );
    m(
        "segments",
        "gauge",
        "Committed segments.",
        s.segments as f64,
    );
    m(
        "segment_bytes",
        "gauge",
        "Bytes in committed segments.",
        s.segment_bytes as f64,
    );
    m(
        "wal_segments",
        "gauge",
        "WAL segment files.",
        s.wal.segments as f64,
    );
    m(
        "wal_last_seq",
        "gauge",
        "Last WAL sequence written.",
        s.wal.last_seq as f64,
    );
    m(
        "wal_durable_seq",
        "gauge",
        "Last WAL sequence fsynced.",
        s.wal.durable_seq as f64,
    );
    m(
        "engine_failed",
        "gauge",
        "1 if the engine is in the failed state.",
        f64::from(u8::from(s.failed.is_some())),
    );
    s.wal_metrics.sync_seconds.render(
        &mut out,
        "stratadb_wal_sync_seconds",
        "WAL fsync latency (group commits and segment rotations).",
        "",
        true,
    );
    s.wal_metrics.group_size.render(
        &mut out,
        "stratadb_wal_group_commit_records",
        "Records made durable by one WAL fsync (group-commit size).",
        "",
        true,
    );
    s.flush_seconds.render(
        &mut out,
        "stratadb_flush_seconds",
        "Duration of memtable flushes that wrote segments.",
        "",
        true,
    );
    st.metrics.render(&mut out);
    st.metrics.render_stages(&mut out, &s.insert_stages);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        out,
    )
        .into_response()
}

/// Parse a comma-separated permission list.
pub fn parse_permissions(s: &str) -> stratadb_common::Result<BTreeSet<Permission>> {
    s.split(',')
        .filter(|p| !p.trim().is_empty())
        .map(Permission::parse)
        .collect()
}

// ---------------------------------------------------------------------------
// Administration (spec §2.4, §16, §19)

async fn admin_compaction(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let p = match authenticate(&st, &headers, Permission::ClusterAdmin) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if let Err(e) = st.audit_required("admin_compaction", Some(&p), json!({})) {
        return error_response(&Error::unavailable(format!("audit log unavailable: {e}")));
    }
    let engine = Arc::clone(&st.engine);
    let res = blocking(move || {
        let (mut groups, mut inputs, mut deleted) = (0, 0, 0);
        // Bounded number of passes; each pass merges at most one group per tenant.
        for _ in 0..100 {
            let r = engine.compact()?;
            groups += r.groups;
            inputs += r.inputs;
            deleted += r.obsolete_deleted;
            if r.groups == 0 {
                break;
            }
        }
        Ok(
            json!({"groups": groups, "inputs_merged": inputs, "obsolete_deleted": deleted,
                  "segments_now": engine.stats().segments}),
        )
    })
    .await;
    match res {
        Ok(v) => Json(v).into_response(),
        Err(e) => error_response(&e),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BackupRequest {
    /// Backup name: same alphabet as tenant ids; created under the server's backup root.
    name: String,
}

async fn admin_backup(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let principal = match authenticate(&st, &headers, Permission::BackupCreate) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let Some(root) = st.cfg.backup_root.clone() else {
        return error_response(&Error::unsupported(
            "HTTP backups are disabled; start the server with --backup-dir",
        ));
    };
    let req: BackupRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(&Error::invalid_input(format!(
                "invalid backup request: {e}"
            )))
        }
    };
    // Reuse the tenant-id alphabet: no '/', '.', or leading '-' → no traversal.
    if let Err(e) = TenantId::new(req.name.clone()) {
        return error_response(&Error::invalid_input(format!("invalid backup name: {e}")));
    }
    if let Err(e) = st.audit_required("backup_create", Some(&principal), json!({"name": req.name}))
    {
        return error_response(&Error::unavailable(format!("audit log unavailable: {e}")));
    }
    let engine = Arc::clone(&st.engine);
    let res = blocking(move || engine.backup(&root.join(&req.name))).await;
    match res {
        Ok(r) => Json(json!({
            "name": r.path.file_name().map(|n| n.to_string_lossy().into_owned()),
            "manifest_version": r.manifest_version, "flushed_through_seq": r.flushed_through_seq,
            "segments": r.segments, "rows": r.rows, "bytes": r.bytes,
            "elapsed_ms": r.elapsed.as_millis() as u64,
        }))
        .into_response(),
        Err(e) => error_response(&e),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionRequest {
    max_age_days: u64,
    #[serde(default)]
    hold: bool,
}

/// Retention endpoints act on the caller's own tenant only.
async fn get_retention(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let p = match authenticate(&st, &headers, Permission::RetentionManage) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let policy = st.engine.retention_policies().get(&p.tenant).copied();
    Json(
        json!({"tenant": p.tenant.as_str(), "policy": policy.map(|r| json!({
        "max_age_ms": r.max_age_ms, "hold": r.hold}))}),
    )
    .into_response()
}

async fn put_retention(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let p = match authenticate(&st, &headers, Permission::RetentionManage) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let req: RetentionRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(&Error::invalid_input(format!(
                "invalid retention request: {e}"
            )))
        }
    };
    let Some(ms) = i64::try_from(req.max_age_days)
        .ok()
        .and_then(|d| d.checked_mul(86_400_000))
        .filter(|m| *m > 0)
    else {
        return error_response(&Error::invalid_input(
            "max_age_days must be between 1 and 10^11",
        ));
    };
    if let Err(e) = st.audit_required(
        "retention_set",
        Some(&p),
        json!({"max_age_ms": ms, "hold": req.hold}),
    ) {
        return error_response(&Error::unavailable(format!("audit log unavailable: {e}")));
    }
    let engine = Arc::clone(&st.engine);
    let tenant = p.tenant.clone();
    let policy = RetentionPolicy {
        max_age_ms: ms,
        hold: req.hold,
    };
    match blocking(move || engine.set_retention(&tenant, Some(policy))).await {
        Ok(()) => Json(
            json!({"tenant": p.tenant.as_str(), "policy": {"max_age_ms": ms, "hold": req.hold},
                              "note": "applied by the background retention task"}),
        )
        .into_response(),
        Err(e) => error_response(&e),
    }
}

async fn delete_retention(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let p = match authenticate(&st, &headers, Permission::RetentionManage) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if let Err(e) = st.audit_required("retention_clear", Some(&p), json!({})) {
        return error_response(&Error::unavailable(format!("audit log unavailable: {e}")));
    }
    let engine = Arc::clone(&st.engine);
    let tenant = p.tenant.clone();
    match blocking(move || engine.set_retention(&tenant, None)).await {
        Ok(()) => Json(json!({"tenant": p.tenant.as_str(), "policy": null})).into_response(),
        Err(e) => error_response(&e),
    }
}

/// `GET /api/v1/query/{id}` — status of a running query of the caller's tenant.
async fn get_query(
    State(st): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    let p = match authenticate(&st, &headers, Permission::Query) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let reg = st.running.lock().unwrap_or_else(|e| e.into_inner());
    match reg
        .get(&format!("{}/{}", p.tenant, id))
        .filter(|r| r.tenant == p.tenant)
    {
        Some(r) => Json(json!({"query_id": id, "state": "running",
            "elapsed_ms": r.started.elapsed().as_millis() as u64, "query": r.sql,
            "cancel_requested": r.cancel.load(std::sync::atomic::Ordering::Relaxed)}))
        .into_response(),
        // Finished, never existed, or another tenant's: indistinguishable by design.
        None => error_response(&Error::not_found("no running query with this id")),
    }
}

/// `DELETE /api/v1/query/{id}` — request cancellation (checked by the
/// executor at batch boundaries; the query then fails with `cancelled`).
async fn cancel_query(
    State(st): State<AppState>,
    headers: HeaderMap,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Response {
    let p = match authenticate(&st, &headers, Permission::Query) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let reg = st.running.lock().unwrap_or_else(|e| e.into_inner());
    match reg
        .get(&format!("{}/{}", p.tenant, id))
        .filter(|r| r.tenant == p.tenant)
    {
        Some(r) => {
            r.cancel.store(true, std::sync::atomic::Ordering::Relaxed);
            (
                StatusCode::ACCEPTED,
                Json(json!({"query_id": id, "state": "cancelling"})),
            )
                .into_response()
        }
        None => error_response(&Error::not_found("no running query with this id")),
    }
}
