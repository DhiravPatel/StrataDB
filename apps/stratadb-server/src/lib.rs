//! StrataDB HTTP API (spec §21; ADR-0018). Normative reference: docs/api/http.md.
//!
//! Every data endpoint authenticates a bearer token and derives the tenant
//! from it. The synchronous storage engine is called on the bounded `sx-blk`
//! pool ([`blocking`]), admitted by semaphores so overload yields `429` rather
//! than unbounded queueing. All engine threads follow a budgeted plan
//! ([`ThreadPlan`]) and are listed at `GET /api/v1/admin/threads`.

pub mod admission;
pub mod audit;
pub mod auth;
pub mod blocking;
pub mod config;
pub mod json_out;
pub mod lifecycle;
pub mod memory;
pub mod net;
pub mod plan_cache;
pub mod query_log;
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
use serde_json::{json, Value as J};
use stratadb_common::TenantId;
use stratadb_common::{time, Error, ErrorKind};
use stratadb_query::{
    execute_page_prepared, execute_prepared, Catalog, PageRequest, QueryOptions, Table,
};
use stratadb_schema::json::event_from_json;
use stratadb_schema::Limits;
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
    /// Threads of the `sx-blk` pool that runs blocking request work
    /// (commits, queries, admin operations); counted in the thread budget.
    pub blocking_threads: usize,
    /// Query classes, slots, queueing and the AIMD controller (spec VIII.11).
    pub workload: admission::WorkloadSettings,
    /// Plan cache bounds (0 disables it; spec VIII.16).
    pub plan_cache_entries: usize,
    pub plan_cache_bytes: usize,
}

/// Limits for one tenant; `None` = unlimited.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TenantQuota {
    pub ingest_events_per_sec: Option<f64>,
    /// Bucket size; defaults to one second of `ingest_events_per_sec`.
    pub ingest_burst_events: Option<f64>,
    /// Segments + memtable bytes.
    pub storage_bytes: Option<u64>,
    /// Memory all of the tenant's running queries may hold together (its
    /// node in the query pool, spec VIII.5.2).
    pub query_memory_bytes: Option<u64>,
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
                query_memory_bytes: o.query_memory_bytes.or(d.query_memory_bytes),
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
            blocking_threads: ThreadPlan::default_for(32, 16).blocking,
            workload: admission::WorkloadSettings::default(),
            plan_cache_entries: 10_000,
            plan_cache_bytes: 64 << 20,
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
    /// Query admission: classes, slots and the fair queue (spec VIII.11).
    admission: Arc<admission::Admission>,
    /// Parsed and planned queries by text (spec VIII.16).
    plan_cache: Arc<plan_cache::PlanCache>,
    running: Arc<std::sync::Mutex<std::collections::HashMap<String, Running>>>,
    auth_limiter: Arc<std::sync::Mutex<AuthLimiter>>,
    audit: Option<Arc<audit::AuditLog>>,
    metrics: Arc<telemetry::ServerMetrics>,
    blk: Arc<blocking::BlockingPool>,
    /// `system.query_log` (None: disabled).
    query_log: Option<Arc<query_log::QueryLog>>,
    /// Memory accounting tree (None: queries are accounted on their own).
    memory: Option<Arc<memory::ServerMemory>>,
    /// Server/table/device lifecycles (None: not tracked, e.g. in tests).
    lifecycle: Option<Arc<lifecycle::Lifecycle>>,
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
            admission: admission::Admission::new(
                cfg.max_concurrent_queries,
                cfg.workload.clone(),
                None,
            ),
            plan_cache: Arc::new(plan_cache::PlanCache::new(
                cfg.plan_cache_entries,
                cfg.plan_cache_bytes,
            )),
            running: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            auth_limiter: Arc::new(std::sync::Mutex::new(AuthLimiter {
                window: Instant::now(),
                failures: 0,
            })),
            blk: Arc::new(blocking::BlockingPool::start(
                cfg.blocking_threads,
                // Every admitted request fits, plus headroom for admin calls.
                cfg.max_concurrent_ingest + cfg.max_concurrent_queries + 16,
            )),
            cfg: Arc::new(cfg),
            audit: None,
            metrics: Arc::default(),
            query_log: None,
            memory: None,
            lifecycle: None,
        }
    }

    /// Report the server lifecycle in `/api/v1/ready`.
    pub fn with_lifecycle(mut self, l: Arc<lifecycle::Lifecycle>) -> Self {
        self.lifecycle = Some(l);
        self
    }

    /// Account queries in the server's memory tree (tenant and pool limits);
    /// admission then also requires headroom in the query pool.
    pub fn with_memory(mut self, m: Arc<memory::ServerMemory>) -> Self {
        self.admission = admission::Admission::new(
            self.cfg.max_concurrent_queries,
            self.cfg.workload.clone(),
            Some(Arc::clone(&m.query)),
        );
        self.memory = Some(m);
        self
    }

    /// The query admission controller (its `tick` drives the AIMD
    /// controller; the server calls it every `controller_interval`).
    pub fn admission(&self) -> Arc<admission::Admission> {
        Arc::clone(&self.admission)
    }

    /// Record every query in `system.query_log`.
    pub fn with_query_log(mut self, log: Arc<query_log::QueryLog>) -> Self {
        self.query_log = Some(log);
        self
    }
}

/// The tables of one tenant: `logs` and, when enabled, `system.query_log`.
struct TenantCatalog {
    tenant: TenantId,
    engine: Arc<Engine>,
    query_log: Option<Arc<query_log::QueryLog>>,
    /// Shown as the holder of the snapshots this query takes
    /// (`GET /api/v1/admin/snapshots`, STRATA-SYS-17-005).
    holder: Option<String>,
}

impl Catalog for TenantCatalog {
    fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    fn snapshot(
        &self,
        table: Table,
    ) -> stratadb_common::Result<std::borrow::Cow<'_, stratadb_storage::Snapshot>> {
        use std::borrow::Cow;
        let held = |s: stratadb_storage::Snapshot| {
            if let Some(h) = &self.holder {
                s.set_holder(h.clone());
            }
            Cow::Owned(s)
        };
        match table {
            Table::Logs => Ok(held(self.engine.snapshot(&self.tenant))),
            Table::QueryLog => match &self.query_log {
                Some(q) => Ok(held(q.engine().snapshot(&self.tenant))),
                None => Err(Error::unsupported(
                    "system.query_log is disabled ([observability] query_log = false)",
                )),
            },
        }
    }
}

/// Microseconds, for headers and logs.
fn micros(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Query identity and timing headers (STRATA-SYS-11-006: `queue_us` in the
/// response, not only the body).
fn with_query_headers(
    mut r: Response,
    query_id: &str,
    fp: &stratadb_query::Fingerprint,
    timings: &[(&'static str, Duration)],
) -> Response {
    let h = r.headers_mut();
    let mut put = |name: &'static str, v: String| {
        if let Ok(v) = header::HeaderValue::from_str(&v) {
            h.insert(name, v);
        }
    };
    put("x-stratadb-query-id", query_id.to_owned());
    put("x-stratadb-fingerprint", fp.hex());
    for (name, d) in timings {
        put(name, micros(*d).to_string());
    }
    r
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
        .route("/api/v1/admin/backup", post(admin_backup).get(list_backups))
        .route("/api/v1/admin/threads", get(admin_threads))
        .route("/api/v1/admin/memory", get(admin_memory))
        .route("/api/v1/admin/workload", get(admin_workload))
        .route("/api/v1/admin/snapshots", get(admin_snapshots))
        .route("/api/v1/admin/caches", get(admin_caches))
        .route("/api/v1/whoami", get(whoami))
        .route("/api/v1/schema", get(schema))
        .route("/api/v1/queries", get(list_queries))
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

/// Response for a refused admission (spec STRATA-SYS-11-004):
/// `TOO_MANY_QUERIES` → 429, `SERVER_OVERLOADED` → 503, both with
/// `retry_after_ms` and `Retry-After`; a query cancelled while queued
/// fails as `cancelled`.
fn refusal_response(r: &admission::Refusal) -> Response {
    let (status, kind, retry, detail) = match r {
        admission::Refusal::TooManyQueries {
            retry_after,
            detail,
        } => (
            StatusCode::TOO_MANY_REQUESTS,
            ErrorKind::ResourceExhausted,
            *retry_after,
            detail.as_str(),
        ),
        admission::Refusal::Overloaded {
            retry_after,
            detail,
        } => (
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorKind::Unavailable,
            *retry_after,
            detail.as_str(),
        ),
        admission::Refusal::Cancelled => {
            return error_response(&Error::new(
                ErrorKind::Cancelled,
                "query cancelled while queued",
            ));
        }
    };
    let ms = retry.as_millis() as u64;
    let body = Json(json!({
        "error": kind.as_str(),
        "code": r.code(),
        "message": detail,
        "retryable": true,
        "retry_after_ms": ms,
    }));
    let secs = ms.div_ceil(1000).max(1).to_string();
    (status, [(header::RETRY_AFTER, secs)], body).into_response()
}

// Returning the ready-made error response keeps handlers simple; built at most once per request.
#[allow(clippy::result_large_err)]
fn authenticate(
    st: &AppState,
    headers: &HeaderMap,
    perm: Permission,
) -> Result<Principal, Response> {
    authenticate_with(st, headers, Some(perm))
}

/// Authenticate, requiring `perm` when given (`None`: any valid token).
#[allow(clippy::result_large_err)]
fn authenticate_with(
    st: &AppState,
    headers: &HeaderMap,
    perm: Option<Permission>,
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
    let perm_name = perm.map_or("any", |p| p.as_str());
    match result {
        Ok(p) => match perm {
            None => Ok(p.clone()),
            Some(perm) => p.require(perm).map(|()| p.clone()).map_err(|e| {
                st.audit(
                    "permission_denied",
                    Some(&p),
                    json!({"permission": perm.as_str()}),
                );
                error_response(&e)
            }),
        },
        Err(e) => {
            // Never record the presented credential.
            st.audit(
                "auth_failure",
                None,
                json!({"permission": perm_name, "reason": e.kind().as_str()}),
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

/// The engine threads a server runs, checked against the budget
/// `2 × logical_cpus + 32` (spec STRATA-SYS-03-014).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThreadPlan {
    pub logical_cpus: usize,
    pub budget: usize,
    /// Async runtime threads (`sx-net-NN`).
    pub net: usize,
    /// Scan workers (`sx-work-NN`).
    pub scan: usize,
    /// Blocking request workers (`sx-blk-NN`).
    pub blocking: usize,
}

/// Threads outside the configurable pools: the main thread, the engine's
/// background thread (`sx-maint-00`), the query log engine's background
/// thread and writer (`sx-maint-01`, `-02`; counted even when the query log
/// is disabled), and up to 2 tokio blocking threads (`max_blocking_threads`;
/// StrataDB itself runs blocking work on `sx-blk`).
pub const FIXED_THREADS: usize = 6;

/// tokio blocking-pool threads allowed (counted in [`FIXED_THREADS`]).
pub const TOKIO_BLOCKING_THREADS: usize = 2;

impl ThreadPlan {
    /// Total engine threads of this plan.
    pub fn total(&self) -> usize {
        FIXED_THREADS + self.net + self.scan + self.blocking
    }

    /// Plan for this host. Defaults: one net and one scan thread per logical
    /// CPU; blocking threads fill the rest of the budget, up to one per
    /// admitted ingest + query request. Fails if the result exceeds the
    /// budget or leaves fewer than 2 blocking threads.
    pub fn new(
        net: Option<usize>,
        scan: Option<usize>,
        blocking: Option<usize>,
        max_ingest: usize,
        max_queries: usize,
    ) -> stratadb_common::Result<Self> {
        Self::for_cpus(
            stratadb_common::threads::logical_cpus(),
            net,
            scan,
            blocking,
            max_ingest,
            max_queries,
        )
    }

    pub fn for_cpus(
        cpus: usize,
        net: Option<usize>,
        scan: Option<usize>,
        blocking: Option<usize>,
        max_ingest: usize,
        max_queries: usize,
    ) -> stratadb_common::Result<Self> {
        let budget = stratadb_common::threads::thread_budget(cpus);
        let net = net.unwrap_or(cpus);
        let scan = scan.unwrap_or(cpus);
        let room = budget.saturating_sub(FIXED_THREADS + net + scan);
        let blocking = blocking.unwrap_or_else(|| room.min(max_ingest + max_queries).max(2));
        let plan = Self {
            logical_cpus: cpus,
            budget,
            net,
            scan,
            blocking,
        };
        if blocking < 2 || plan.total() > budget {
            return Err(Error::invalid_input(format!(
                "thread plan exceeds the budget of {budget} threads for {cpus} logical CPUs \
                 (2 x cpus + 32): {} total = {FIXED_THREADS} fixed + {net} net + {scan} scan + \
                 {blocking} blocking (at least 2); lower limits.net_threads, \
                 limits.scan_threads or limits.blocking_threads",
                plan.total()
            )));
        }
        Ok(plan)
    }

    /// Defaults for this host, falling back to 2 blocking threads if the
    /// budget is already exhausted (only possible with explicit settings).
    pub fn default_for(max_ingest: usize, max_queries: usize) -> Self {
        Self::new(None, None, None, max_ingest, max_queries).unwrap_or(Self {
            logical_cpus: stratadb_common::threads::logical_cpus(),
            budget: stratadb_common::threads::thread_budget(
                stratadb_common::threads::logical_cpus(),
            ),
            net: 1,
            scan: 1,
            blocking: 2,
        })
    }
}

// ---------------------------------------------------------------------------
// Handlers

async fn health() -> Json<J> {
    Json(json!({"status": "ok"}))
}

async fn ready(State(st): State<AppState>) -> Response {
    use stratadb_common::state::{ServerState as S, State as _};
    let server = st.lifecycle.as_ref().map(|l| l.server());
    let mut body = match st.engine.failure() {
        None => json!({"status": "ready"}),
        Some(_) => json!({"status": "failed"}),
    };
    if let Some(s) = server {
        body["state"] = json!(s.name());
    }
    // A draining server takes no new work (spec IX.22.7); a degraded one
    // still serves reads, so it stays ready unless the engine has failed.
    let up = st.engine.failure().is_none()
        && !matches!(
            server,
            Some(S::Starting | S::Recovering | S::Warming | S::Draining | S::Stopped)
        );
    if up {
        Json(body).into_response()
    } else {
        if st.engine.failure().is_none() {
            body["status"] = json!("unavailable");
        }
        (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response()
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
    /// In `VISIBLE`; the caller acknowledges.
    life: stratadb_common::state::Tracker<stratadb_common::state::InsertState>,
}

/// Commit a validated request (`life` in `VALIDATED`), replaying the
/// engine's measured phases into the insert lifecycle (spec IX.22.2).
async fn commit(
    st: &AppState,
    principal: &Principal,
    permit: tokio::sync::OwnedSemaphorePermit,
    events: Vec<stratadb_schema::Event>,
    idem: Option<IdempotencyKey>,
    mut life: stratadb_common::state::Tracker<stratadb_common::state::InsertState>,
) -> stratadb_common::Result<Committed> {
    use stratadb_common::state::InsertState as I;
    let n = events.len();
    // A quota refusal has no spec transition: the entity ends in VALIDATED.
    st.check_quota(&principal.tenant, n)?;
    let engine = Arc::clone(&st.engine);
    let tenant = principal.tenant.clone();
    let handed = Instant::now();
    let res = st
        .blk
        .run(move || {
            let waited = handed.elapsed();
            let _permit = permit;
            engine
                .write_batch_idempotent(&tenant, events, idem)
                .map(|r| (r, waited))
        })
        .await;
    let (receipt, waited) = match res {
        Ok(v) => v,
        Err(e) => {
            if !matches!(
                e.kind(),
                ErrorKind::ResourceExhausted | ErrorKind::InvalidInput
            ) {
                // The write reached the WAL path and failed there.
                life.to(I::Logged, "WAL append");
                life.to(I::Failed, "WAL failed");
            }
            return Err(e);
        }
    };
    let t = receipt.timings;
    let before_wal = waited + t.encode;
    if t.throttled > Duration::ZERO {
        life.to_after(I::Throttled, before_wal, "write stall");
        life.to_after(I::Validated, t.throttled, "stall over");
        life.to_after(I::Logged, t.wal_write, "WAL append");
    } else {
        life.to_after(I::Logged, before_wal + t.wal_write, "WAL append");
    }
    life.to_after(I::Durable, t.durable_wait, "group fsync");
    life.to_after(I::Visible, t.publish, "memtable published");
    let accepted = if receipt.duplicate {
        receipt.accepted
    } else {
        n
    };
    Ok(Committed {
        receipt,
        accepted,
        life,
    })
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
    use stratadb_common::state::{InsertState as I, Tracker};
    // Insert lifecycle (spec IX.22.2) from admission on.
    let mut life = Tracker::new(I::Received);
    let text = match decode_body(&headers, &body, st.cfg.max_decompressed_bytes).and_then(utf8) {
        Ok(t) => t,
        Err(e) => return error_response(&e),
    };
    life.to(I::Decoded, "body decoded");
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
                life.to(I::Rejected, "not a JSON object or array");
                return error_response(&Error::invalid_input("expected a JSON object or array"));
            }
            Err(e) => {
                life.to(I::Rejected, "invalid JSON");
                return error_response(&Error::invalid_input(format!("invalid JSON: {e}")));
            }
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
        life.to(I::Rejected, "no valid events");
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
    life.to(I::Validated, "schema checks ok");
    match commit(&st, &principal, permit, events, idem, life).await {
        Ok(mut c) => {
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
            c.life.to(I::Acked, "response sent");
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
    use stratadb_common::state::{InsertState as I, Tracker};
    let mut life = Tracker::new(I::Received);
    let raw = match decode_body(&headers, &body, st.cfg.max_decompressed_bytes) {
        Ok(t) => t,
        Err(e) => return otlp_error(&e, proto),
    };
    life.to(I::Decoded, "body decoded");
    let idem = match idempotency_key(&headers, &raw) {
        Ok(k) => k,
        Err(e) => return otlp_error(&e, proto),
    };
    let req: J = if proto {
        match stratadb_schema::otlp_proto::decode_export_request(&raw) {
            Ok(v) => v,
            Err(e) => {
                life.to(I::Rejected, "invalid protobuf");
                return otlp_error(&e, true);
            }
        }
    } else {
        match serde_json::from_slice(&raw) {
            Ok(v) => v,
            Err(e) => {
                life.to(I::Rejected, "invalid JSON");
                return otlp_error(&Error::invalid_input(format!("invalid JSON: {e}")), false);
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
        Err(e) => {
            life.to(I::Rejected, "invalid export request");
            return otlp_error(&e, proto);
        }
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
        life.to(I::Rejected, "no valid records");
        return respond(Vec::new(), rejected, first_error);
    }
    life.to(I::Validated, "schema checks ok");
    match commit(&st, &principal, permit, events, idem, life).await {
        Ok(mut c) => {
            let r = respond(
                vec![
                    ("x-stratadb-commit-seq", c.receipt.commit_seq.to_string()),
                    ("x-stratadb-durability", c.receipt.level.as_str().to_owned()),
                    ("x-stratadb-duplicate", c.receipt.duplicate.to_string()),
                    ("x-stratadb-accepted", c.accepted.to_string()),
                ],
                rejected,
                first_error,
            );
            c.life.to(I::Acked, "response sent");
            r
        }
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
    /// Lower the query's work class (`normal` or `batch`); a request can
    /// never raise it (spec VIII.11.1).
    workload_class: Option<String>,
}

/// A running query in the registry.
#[derive(Debug)]
struct Running {
    tenant: TenantId,
    cancel: Arc<std::sync::atomic::AtomicBool>,
    started: Instant,
    sql: String,
    class: stratadb_common::workload::WorkClass,
    /// Set once admitted; until then the query is queued.
    admitted: Arc<std::sync::atomic::AtomicBool>,
    fingerprint: String,
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

/// Result rows encoded as JSON on the blocking pool, with their unique
/// column names and the time it took.
struct EncodedRows {
    names: Vec<String>,
    rows_json: Vec<u8>,
    took: Duration,
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
    use stratadb_common::state::{QueryState as Q, Tracker};
    let arrived = Instant::now();
    let principal = match authenticate(&st, &headers, Permission::Query) {
        Ok(p) => p,
        Err(r) => return r,
    };
    // Query lifecycle (spec IX.22.1). Unauthenticated requests are not
    // queries. Shared with the worker that runs it.
    let life = Arc::new(std::sync::Mutex::new(Tracker::new(Q::Received)));
    let step = |to: Q, why: &str| {
        life.lock().unwrap_or_else(|e| e.into_inner()).to(to, why);
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
    // From here every outcome is a query: it is recorded in system.query_log
    // (STRATA-SYS-13-009) and its response carries the timing headers.
    let fp = stratadb_query::fingerprint(&req.query);
    let arrived_ns = time::now_unix_nanos();
    // The plan-time estimate (rows, CPU ms), once made.
    let estimate: std::sync::OnceLock<(u64, f64)> = std::sync::OnceLock::new();
    let record = |status: &str,
                  queue: Duration,
                  serialize: Duration,
                  stats: Option<&stratadb_query::QueryStats>,
                  rows: u64| {
        if let Some(q) = &st.query_log {
            q.record(&query_log::Entry {
                tenant: &principal.tenant,
                started_unix_ns: arrived_ns,
                query_id: &query_id,
                fingerprint: &fp,
                status,
                queue,
                elapsed: arrived.elapsed(),
                serialize,
                stats,
                result_rows: rows,
                estimate: estimate.get().copied(),
            });
        }
    };
    let fail = |e: &Error, queue: Option<Duration>| {
        record(
            e.kind().as_str(),
            queue.unwrap_or_default(),
            Duration::ZERO,
            None,
            0,
        );
        let t: Vec<(&'static str, Duration)> = queue
            .map(|q| ("x-stratadb-queue-us", q))
            .into_iter()
            .collect();
        with_query_headers(error_response(e), &query_id, &fp, &t)
    };
    let requested = match req.workload_class.as_deref() {
        None => None,
        Some(c) => match stratadb_common::workload::WorkClass::parse(c)
            .filter(|c| stratadb_common::workload::WorkClass::QUERY.contains(c))
        {
            Some(c) => Some(c),
            None => {
                step(Q::Failed, "bad workload_class");
                return fail(
                    &Error::invalid_input("workload_class must be interactive, normal or batch"),
                    None,
                );
            }
        },
    };
    // Parse and plan before admission: malformed queries fail without
    // taking a slot.
    let cached = st.plan_cache.get(&req.query, &fp);
    let plan_cache_hit = cached.is_some();
    let prepared = match cached {
        Some(p) => {
            step(Q::Parsed, "plan cache hit");
            step(Q::Planned, "plan cache hit");
            p
        }
        None => {
            let parse_opts = QueryOptions::default();
            let parsed = match stratadb_query::parse(&req.query, &parse_opts) {
                Ok(p) => p,
                Err(e) => {
                    step(Q::Failed, "syntax error");
                    return fail(&e, None);
                }
            };
            step(Q::Parsed, "parse ok");
            let prepared = match parsed.plan() {
                Ok(p) => p,
                Err(e) => {
                    step(Q::Failed, "semantic error");
                    return fail(&e, None);
                }
            };
            step(Q::Planned, "plan ok");
            st.plan_cache.insert(&req.query, &prepared);
            prepared
        }
    };
    // Classify from the plan-time estimate (metadata only, STRATA-SYS-11-003).
    let catalog = TenantCatalog {
        tenant: principal.tenant.clone(),
        engine: Arc::clone(&st.engine),
        query_log: st.query_log.clone(),
        holder: Some(format!("query:{query_id}")),
    };
    // Waiting for a slot is QUEUED; anything refused from here on is
    // QUEUED -> REJECTED.
    step(Q::Queued, "admission");
    let est = match prepared.estimate(&catalog) {
        Ok(e) => {
            let _ = estimate.set((e.rows + e.memory_rows, e.est_cpu_ms));
            e.est_cpu_ms
        }
        Err(e) => {
            step(Q::Rejected, "estimate failed");
            return fail(&e, None);
        }
    };
    let class = st
        .admission
        .classify(principal.tenant.as_str(), est, requested);
    // Registry keys are global; scope them by tenant so ids cannot collide or leak.
    let key = format!("{}/{}", principal.tenant, query_id);
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let admitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    {
        let mut reg = st.running.lock().unwrap_or_else(|e| e.into_inner());
        if reg.contains_key(&key) {
            drop(reg);
            step(Q::Rejected, "duplicate query_id");
            return fail(
                &Error::new(
                    ErrorKind::AlreadyExists,
                    "a query with this query_id is already running",
                ),
                None,
            );
        }
        reg.insert(
            key.clone(),
            Running {
                tenant: principal.tenant.clone(),
                cancel: Arc::clone(&cancel),
                started: Instant::now(),
                sql: req.query.chars().take(200).collect(),
                class,
                admitted: Arc::clone(&admitted),
                fingerprint: fp.hex(),
            },
        );
    }
    let guard = RunningGuard {
        reg: Arc::clone(&st.running),
        id: key.clone(),
    };
    // Admission (spec VIII.11.2): run now, wait in the fair queue, or be
    // refused.
    let admitting = Instant::now();
    let mut permit = match st
        .admission
        .admit(admission::Ask {
            class,
            tenant: stratadb_query::tenant_key(&principal.tenant),
            est_cpu_ms: est,
            max_wait: timeout,
            cancel: Some(Arc::clone(&cancel)),
            key,
        })
        .await
    {
        Ok(p) => p,
        Err(r) => {
            step(Q::Rejected, r.code());
            let waited = admitting.elapsed();
            let status = match r {
                admission::Refusal::Cancelled => "cancelled",
                admission::Refusal::Overloaded { .. } => "unavailable",
                admission::Refusal::TooManyQueries { .. } => "resource_exhausted",
            };
            record(status, waited, Duration::ZERO, None, 0);
            return with_query_headers(
                refusal_response(&r),
                &query_id,
                &fp,
                &[("x-stratadb-queue-us", waited)],
            );
        }
    };
    admitted.store(true, std::sync::atomic::Ordering::Relaxed);
    let admission_wait = permit.waited;
    let started = Instant::now();
    let worker_life = Arc::clone(&life);
    let memory = st.memory.as_ref().map(|m| {
        (
            m.query_parent(&principal.tenant, &query_id),
            m.plan.per_query,
            m.plan.wait,
        )
    });
    let res = st
        .blk
        .run(move || {
            let queued = admission_wait + started.elapsed();
            worker_life
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .to(Q::Running, "worker started");
            let _guard = guard;
            let mut opts = QueryOptions {
                timeout,
                max_result_rows: max_rows,
                cancel: Some(cancel),
                class: Some(Arc::clone(&permit.cell)),
                // Degree of parallelism per class (STRATA-SYS-11-008):
                // 5 ms of estimated CPU per worker, 50 ms for batch.
                target_ms_per_worker: if permit.class()
                    == stratadb_common::workload::WorkClass::Batch
                {
                    50.0
                } else {
                    stratadb_query::TARGET_MS_PER_WORKER
                },
                ..QueryOptions::default()
            };
            if let Some((parent, limit, wait)) = memory {
                opts.memory = Some(parent);
                opts.max_memory_bytes = usize::try_from(limit).unwrap_or(usize::MAX);
                opts.memory_wait = wait;
            }
            // Aggregates and EXPLAIN have bounded results: a page size is
            // ignored ("paginated": false), a cursor is still an error.
            let paginate =
                req.cursor.is_some() || (req.page_size.is_some() && prepared.paginable());
            let out = if paginate {
                let page = PageRequest {
                    page_size: req.page_size.unwrap_or(max_rows).min(max_rows),
                    cursor: req.cursor,
                };
                execute_page_prepared(&catalog, &prepared, &opts, &page)
                    .map(|p| (p.output, p.next_cursor))
            } else {
                execute_prepared(&catalog, &prepared, &opts).map(|o| (o, None))
            };
            if let Ok((o, _)) = &out {
                permit.finish(o.stats.cpu);
            }
            let class = permit.class();
            drop(permit); // frees the slot before the response is built
                          // Encode the rows here, on the blocking pool, so a large result
                          // never occupies an async reactor thread (spec
                          // STRATA-SYS-06-003, ADR-0044).
            let out = out.map(|(o, c)| {
                let started = Instant::now();
                let names = unique_names(&o.columns);
                let mut rows_json = Vec::new();
                json_out::write_rows(&names, &o.rows, &mut rows_json);
                let enc = EncodedRows {
                    names,
                    rows_json,
                    took: started.elapsed(),
                };
                (o, c, paginate, enc)
            });
            Ok((queued, class, out))
        })
        .await;
    let (queued, ran_class, res) = match res {
        Ok((queued, c, r)) => (Some(queued), Some(c), r),
        Err(e) => (None, None, Err(e)),
    };
    let res = res.map(|(o, c, paginated, enc)| ((o, c), paginated, enc));
    let ran_class = ran_class.unwrap_or(class);
    match res {
        Ok(((out, next_cursor), paginated, enc)) => {
            let queued = queued.unwrap_or_default();
            use telemetry::QueryStage;
            step(Q::Streaming, "result ready");
            let serializing = Instant::now();
            st.metrics.query_work(&out.stats);
            let q_error = estimate
                .get()
                .map(|(rows, _)| telemetry::q_error(*rows, out.stats.rows_scanned));
            if let Some(q) = q_error {
                st.metrics.estimate(q);
            }
            let EncodedRows {
                names,
                rows_json,
                took: encode_took,
            } = enc;
            let s = &out.stats;
            let us = |d: Duration| d.as_secs_f64() * 1e6;
            let envelope = json!({
                "query_id": query_id,
                "columns": names,
                "explain": out.explain,
                "partial": false,
                "next_cursor": next_cursor,
                "paginated": paginated,
                "stats": {
                    // From arrival: parse, plan and admission happen before
                    // the worker starts, so queue + parse + plan + exec <=
                    // elapsed holds.
                    "elapsed_ms": arrived.elapsed().as_secs_f64() * 1000.0,
                    "rows_scanned": s.rows_scanned, "rows_matched": s.rows_matched,
                    "segment_bytes_read": s.bytes_read, "segments_total": s.segments_total,
                    "segments_scanned": s.segments_scanned,
                    "segments_pruned": s.segments_pruned_time + s.segments_pruned_keys + s.segments_pruned_severity + s.segments_pruned_bloom + s.segments_pruned_ngram + s.segments_pruned_cursor,
                    "indexes_used": s.indexes_used,
                    // Segments/windows answered from metadata (STRATA-SYS-09-006).
                    "sources_from_metadata": s.sources_from_metadata,
                    "plan_cache": if plan_cache_hit { "hit" } else { "miss" },
                    "column_cache_hits": s.column_cache_hits,
                    // Plan-time estimate vs actual (STRATA-SYS-12-004).
                    "est_rows": estimate.get().map(|e| e.0),
                    "q_error": q_error,
                    // Sources skipped by an ordered ORDER BY event_time LIMIT scan.
                    "sources_skipped_by_order": s.sources_skipped_by_order,
                    // Sorting (STRATA-SYS-14-001/003): segment runs merged,
                    // rows skipped against the shared top-k bound.
                    "sort_runs_merged": s.sort_runs_merged,
                    "topk_rows_skipped": s.topk_rows_skipped,
                    // Phase decomposition (spec VIII.1.4, STRATA-SYS-01-007):
                    // queue + parse + plan + exec (+ serialize) ≈ elapsed.
                    "queue_us": us(queued), "parse_us": us(s.parse),
                    "plan_us": us(s.plan), "exec_us": us(s.elapsed),
                    // Degree of parallelism (STRATA-SYS-11-008).
                    "scan_workers": s.scan_workers, "est_cpu_ms": s.est_cpu_ms,
                    // Work class at admission and when it finished (a
                    // demotion lowers it; spec VIII.11.1).
                    "class": class.name(), "final_class": ran_class.name(),
                    "cpu_us": us(s.cpu),
                },
                "fingerprint": fp.hex(),
            });
            // The envelope is small; the rows are already JSON. The body is
            // sent as prefix + rows + suffix with an exact length, so the
            // encoded rows are never copied; "rows" goes where serde_json
            // would put it, so the bytes are unchanged.
            let parts = match &envelope {
                J::Object(env) => json_out::envelope_parts(env, "rows"),
                _ => Err(serde::ser::Error::custom("envelope is not an object")),
            };
            let (prefix, suffix) = match parts {
                Ok(p) => p,
                Err(e) => {
                    return fail(
                        &Error::internal(format!("response encoding: {e}")),
                        Some(queued),
                    )
                }
            };
            let body = json_out::ChunkedBody::new(vec![prefix, rows_json, suffix]);
            let r = (
                [(header::CONTENT_TYPE, "application/json")],
                axum::body::Body::new(body),
            )
                .into_response();
            let serialize = serializing.elapsed() + encode_took;
            for (stage, d) in [
                (QueryStage::Queue, queued),
                (QueryStage::Parse, s.parse),
                (QueryStage::Plan, s.plan),
                (QueryStage::Exec, s.elapsed),
                (QueryStage::Serialize, serialize),
            ] {
                st.metrics.query_stage(stage, d);
            }
            // Query log line (no SQL text or values: they may carry tenant
            // data; the fingerprint identifies the query shape).
            tracing::info!(
                target: "query", query_id = %query_id, tenant = %principal.tenant,
                fingerprint = %fp.hex(),
                queue_us = us(queued) as u64, parse_us = us(s.parse) as u64,
                plan_us = us(s.plan) as u64, exec_us = us(s.elapsed) as u64,
                cpu_us = us(s.cpu) as u64,
                serialize_us = us(serialize) as u64, rows_scanned = s.rows_scanned,
                rows_returned = out.rows.len(), segment_bytes_read = s.bytes_read, "query"
            );
            record("ok", queued, serialize, Some(s), out.rows.len() as u64);
            step(Q::Finished, "response built");
            with_query_headers(
                r,
                &query_id,
                &fp,
                &[
                    ("x-stratadb-queue-us", queued),
                    ("x-stratadb-parse-us", s.parse),
                    ("x-stratadb-plan-us", s.plan),
                    ("x-stratadb-exec-us", s.elapsed),
                    ("x-stratadb-cpu-us", s.cpu),
                ],
            )
        }
        Err(e) => {
            let mut t = life.lock().unwrap_or_else(|e| e.into_inner());
            match (t.state(), e.kind()) {
                // A blocking-pool rejection: the query never started.
                (Q::Queued, _) => {
                    t.to(Q::Rejected, "worker pool full");
                }
                (_, ErrorKind::Cancelled | ErrorKind::Timeout) => {
                    t.to(Q::Cancelling, "cancel requested or time limit");
                    t.to(Q::Cancelled, "operators released");
                }
                _ => {
                    t.to(Q::Failed, "runtime error");
                }
            }
            drop(t);
            fail(&e, queued)
        }
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
    s.wal_metrics.group_delay_seconds.render(
        &mut out,
        "strata_wal_group_delay_seconds",
        "Time the WAL sync leader waited to gather a group commit (adaptive, spec STRATA-SYS-07-004).",
        "",
        true,
    );
    s.wal_metrics.commit_wake_seconds.render(
        &mut out,
        "strata_wal_commit_wake_seconds",
        "From a group commit's durability being published to a covered writer resuming (spec STRATA-SYS-07-006).",
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
    {
        use stratadb_common::threads::{self, Role};
        let inv = threads::inventory();
        out.push_str(&format!(
            "# HELP strata_thread_budget Engine thread budget (2 x logical CPUs + 32).\n\
             # TYPE strata_thread_budget gauge\nstrata_thread_budget {}\n\
             # HELP strata_threads Registered engine threads by role.\n# TYPE strata_threads gauge\n",
            threads::thread_budget(threads::logical_cpus())
        ));
        for r in Role::ALL {
            out.push_str(&format!(
                "strata_threads{{role=\"{}\"}} {}\n",
                r.as_str(),
                inv.iter().filter(|t| t.role == r).count()
            ));
        }
        out.push_str("# HELP strata_thread_cpu_seconds CPU time of live registered threads by role, as last recorded by each thread.\n# TYPE strata_thread_cpu_seconds gauge\n");
        for r in Role::ALL {
            out.push_str(&format!(
                "strata_thread_cpu_seconds{{role=\"{}\"}} {}\n",
                r.as_str(),
                inv.iter()
                    .filter(|t| t.role == r)
                    .map(|t| t.cpu_seconds)
                    .sum::<f64>()
            ));
        }
        let b = st.blk.stats();
        out.push_str(&format!(
            "# HELP strata_blocking_pool_workers Started sx-blk threads (grow on demand up to the maximum).\n\
             # TYPE strata_blocking_pool_workers gauge\nstrata_blocking_pool_workers {}\n\
             # HELP strata_blocking_pool_max_workers Maximum sx-blk threads (thread plan).\n\
             # TYPE strata_blocking_pool_max_workers gauge\nstrata_blocking_pool_max_workers {}\n\
             # HELP strata_blocking_pool_busy Blocking jobs running.\n\
             # TYPE strata_blocking_pool_busy gauge\nstrata_blocking_pool_busy {}\n\
             # HELP strata_blocking_pool_queued Blocking jobs waiting for a thread.\n\
             # TYPE strata_blocking_pool_queued gauge\nstrata_blocking_pool_queued {}\n\
             # HELP strata_blocking_pool_rejected_total Blocking jobs refused because the queue was full.\n\
             # TYPE strata_blocking_pool_rejected_total counter\nstrata_blocking_pool_rejected_total {}\n",
            b.workers, b.max_workers, b.busy, b.queued, b.rejected
        ));
    }
    if let Some(m) = &st.memory {
        m.render(&mut out);
    }
    {
        let w = &st.engine.stats().wal;
        out.push_str(&format!(
            "# HELP strata_wal_bytes_total WAL frame bytes appended since start (headers + payloads).\n\
             # TYPE strata_wal_bytes_total counter\nstrata_wal_bytes_total {}\n\
             # HELP strata_wal_records_total WAL records appended since start.\n\
             # TYPE strata_wal_records_total counter\nstrata_wal_records_total {}\n",
            w.bytes_appended, w.records_appended
        ));
    }
    if let Some(q) = &st.query_log {
        use std::sync::atomic::Ordering::Relaxed;
        let c = q.counters();
        for (name, help, v) in [
            (
                "query_log_entries_total",
                "Queries recorded for system.query_log.",
                c.recorded.load(Relaxed),
            ),
            (
                "query_log_written_total",
                "Query log entries written to storage.",
                c.written.load(Relaxed),
            ),
            (
                "query_log_dropped_total",
                "Query log entries dropped because the queue was full.",
                c.dropped.load(Relaxed),
            ),
            (
                "query_log_write_errors_total",
                "Query log entries lost to storage write errors.",
                c.write_errors.load(Relaxed),
            ),
        ] {
            out.push_str(&format!(
                "# HELP strata_{name} {help}\n# TYPE strata_{name} counter\nstrata_{name} {v}\n"
            ));
        }
    }
    let io = st.engine.io_stats();
    for (name, kind, help, v) in [
        (
            "io_max_open_files",
            "gauge",
            "Segment file descriptor budget (LRU).",
            io.max_open_files,
        ),
        (
            "io_files_open",
            "gauge",
            "Segment files held open by the I/O engine.",
            io.files_open,
        ),
        (
            "io_reads_total",
            "counter",
            "Storage reads completed.",
            io.reads,
        ),
        (
            "io_read_bytes_total",
            "counter",
            "Bytes read from storage.",
            io.bytes_read,
        ),
        (
            "io_short_reads_total",
            "counter",
            "Short reads whose remainder was resubmitted.",
            io.short_reads_resubmitted,
        ),
        (
            "io_read_retries_total",
            "counter",
            "Failed reads retried once.",
            io.read_retries,
        ),
        (
            "io_read_errors_total",
            "counter",
            "Reads that failed after the retry (queries failed).",
            io.read_errors,
        ),
        (
            "io_file_opens_total",
            "counter",
            "Segment file opens (including reopens after eviction).",
            io.opens,
        ),
        (
            "io_file_evictions_total",
            "counter",
            "Files closed to stay within the descriptor budget.",
            io.evictions,
        ),
        (
            "io_emfile_recoveries_total",
            "counter",
            "EMFILE on open recovered by closing cold files.",
            io.emfile_recoveries,
        ),
        (
            "io_max_inflight_per_device",
            "gauge",
            "I/O calls allowed in flight per device (STRATA-SYS-04-008).",
            io.max_inflight_per_device,
        ),
        (
            "io_inflight",
            "gauge",
            "I/O calls in flight now, all devices.",
            io.inflight,
        ),
        (
            "io_inflight_peak",
            "gauge",
            "Highest number of I/O calls in flight on one device since start.",
            io.inflight_peak,
        ),
        (
            "io_inflight_waits_total",
            "counter",
            "I/O calls that waited because their device was at its in-flight limit.",
            io.inflight_waits,
        ),
    ] {
        out.push_str(&format!(
            "# HELP strata_{name} {help}\n# TYPE strata_{name} {kind}\nstrata_{name} {v}\n"
        ));
    }
    let p = stratadb_query::scan_pool().stats();
    out.push_str(&format!(
        "# HELP strata_scan_pool_workers Threads of the shared scan worker pool.\n\
         # TYPE strata_scan_pool_workers gauge\nstrata_scan_pool_workers {}\n\
         # HELP strata_scan_pool_active_tasks Scan tasks queued or running (at most 2 x workers).\n\
         # TYPE strata_scan_pool_active_tasks gauge\nstrata_scan_pool_active_tasks {}\n\
         # HELP strata_scan_pool_tasks_total Scan tasks offered to the pool, by result (rejected = run by the query thread because the pool was busy).\n\
         # TYPE strata_scan_pool_tasks_total counter\n\
         strata_scan_pool_tasks_total{{result=\"accepted\"}} {}\n\
         strata_scan_pool_tasks_total{{result=\"rejected\"}} {}\n",
        p.workers, p.active_tasks, p.submitted, p.rejected
    ));
    // Time in state per lifecycle (spec IX.22, STRATA-ARCH-22-001..008).
    stratadb_common::state::render(&mut out);
    // Workload classes and admission (spec VIII.11).
    st.admission.render(&mut out);
    // Plan cache and decoded column cache (spec VIII.16).
    st.plan_cache.render(&mut out);
    if let Some(c) = &s.column_cache {
        for (name, kind, help, v) in [
            (
                "hits_total",
                "counter",
                "Segment columns served decoded from the cache.",
                c.hits as f64,
            ),
            (
                "misses_total",
                "counter",
                "Segment columns read and decoded.",
                c.misses as f64,
            ),
            (
                "insertions_total",
                "counter",
                "Decoded columns added.",
                c.insertions as f64,
            ),
            (
                "evictions_total",
                "counter",
                "Decoded columns evicted (SLRU).",
                c.evictions as f64,
            ),
            (
                "bytes",
                "gauge",
                "Bytes of decoded columns held (charged to the cache region).",
                c.bytes as f64,
            ),
            (
                "protected_entries",
                "gauge",
                "Columns in the protected segment (used at least twice).",
                c.protected_entries as f64,
            ),
        ] {
            out.push_str(&format!(
                "# HELP strata_column_cache_{name} {help}\n# TYPE strata_column_cache_{name} {kind}\nstrata_column_cache_{name} {v}\n"
            ));
        }
        c.hit_seconds.render(
            &mut out,
            "strata_column_cache_hit_seconds",
            "Lookup latency of column cache hits.",
            "",
            true,
        );
    }
    // Snapshots and retained obsolete segments (spec STRATA-SYS-17-005).
    let snaps = st.engine.snapshots();
    let oldest = snaps.open.iter().map(|s| s.age_ms).max().unwrap_or(0) as f64 / 1000.0;
    for (name, kind, help, v) in [
        (
            "snapshots_open",
            "gauge",
            "Open engine snapshots (queries holding a consistent view).",
            snaps.open.len() as f64,
        ),
        (
            "snapshot_oldest_age_seconds",
            "gauge",
            "Age of the oldest open snapshot.",
            oldest,
        ),
        (
            "obsolete_segments",
            "gauge",
            "Replaced segments whose files still exist (pinned or within old_segments_lifetime).",
            snaps.obsolete_segments as f64,
        ),
        (
            "obsolete_retained_bytes",
            "gauge",
            "Bytes of replaced segments still on disk.",
            snaps.obsolete_bytes as f64,
        ),
        (
            "obsolete_retained_ratio",
            "gauge",
            "Retained obsolete bytes / disk size (warned above 0.10).",
            snaps.obsolete_ratio,
        ),
    ] {
        out.push_str(&format!(
            "# HELP strata_{name} {help}\n# TYPE strata_{name} {kind}\nstrata_{name} {v}\n"
        ));
    }
    out.push_str("# HELP strata_scan_pool_class_cpu_seconds_total Scan-pool CPU time by work class (fair scheduling, STRATA-SYS-11-001).\n# TYPE strata_scan_pool_class_cpu_seconds_total counter\n");
    for (c, u) in stratadb_query::scan_pool().class_usage() {
        if stratadb_common::workload::WorkClass::QUERY.contains(&c) {
            out.push_str(&format!(
                "strata_scan_pool_class_cpu_seconds_total{{class=\"{}\"}} {}\n",
                c.name(),
                u.cpu.as_secs_f64()
            ));
        }
    }
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        out,
    )
        .into_response()
}

/// `GET /api/v1/admin/workload` — per-class slots, queue, arrival rate,
/// service time, utilization and queue-wait p99 (spec VIII.11.4,
/// STRATA-SYS-11-010).
async fn admin_workload(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = authenticate(&st, &headers, Permission::ClusterRead) {
        return r;
    }
    Json(st.admission.stats()).into_response()
}

/// `GET /api/v1/admin/snapshots` — open snapshots with their holders and
/// the obsolete segment bytes they keep on disk (spec STRATA-SYS-17-005,
/// the equivalent of `system.snapshots`). Tenants appear by name, so it
/// needs an operator token.
async fn admin_snapshots(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = authenticate(&st, &headers, Permission::ClusterRead) {
        return r;
    }
    let logs = st.engine.snapshots();
    let qlog = st.query_log.as_ref().map(|q| q.engine().snapshots());
    Json(json!({"logs": logs, "query_log": qlog})).into_response()
}

/// `GET /api/v1/admin/caches` — the plan cache: hits, misses, insertions,
/// evictions, entries, bytes, hit latency, and the 100 most looked-up
/// fingerprints with their hit ratios (spec STRATA-SYS-16-004).
async fn admin_caches(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = authenticate(&st, &headers, Permission::ClusterRead) {
        return r;
    }
    let s = st.plan_cache.stats();
    Json(json!({
        "plan_cache": {
            "enabled": st.plan_cache.enabled(),
            "hits": s.hits, "misses": s.misses, "insertions": s.insertions,
            "evictions": s.evictions, "entries": s.entries, "bytes": s.bytes,
            "capacity_entries": s.capacity_entries, "capacity_bytes": s.capacity_bytes,
            "hit_p99_us": s.hit_seconds.quantile_upper_bound(0.99).map(|x| x * 1e6),
            "top_fingerprints": st.plan_cache.top(100),
        },
        "column_cache": st.engine.stats().column_cache.map(|c| json!({
            "hits": c.hits, "misses": c.misses, "insertions": c.insertions,
            "evictions": c.evictions, "entries": c.entries, "bytes": c.bytes,
            "protected_entries": c.protected_entries,
            "capacity_bytes": c.capacity_bytes,
            "hit_p99_us": c.hit_seconds.quantile_upper_bound(0.99).map(|x| x * 1e6),
        })),
    }))
    .into_response()
}

/// `GET /api/v1/whoami` (any valid token) — the caller's tenant, token id,
/// permissions and expiry. Never the secret.
async fn whoami(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let p = match authenticate_with(&st, &headers, None) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let expires_ms = st
        .tokens
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .list()
        .into_iter()
        .find(|t| t.id == p.token_id)
        .and_then(|t| t.expires_ms);
    let perms: Vec<&str> = p.permissions.iter().map(|x| x.as_str()).collect();
    Json(json!({
        "tenant": p.tenant.as_str(),
        "token_id": p.token_id,
        "permissions": perms,
        "expires_ms": expires_ms,
    }))
    .into_response()
}

/// Attribute keys listed per scope by `GET /api/v1/schema`.
const SCHEMA_MAX_KEYS: usize = 500;

/// `GET /api/v1/schema` (query, caller's tenant) — tables, their columns, and
/// the attribute / resource keys present in the tenant's segments with the
/// rows of the segments holding them (from segment metadata, never a scan;
/// memtable rows are not counted). At most 500 keys per scope, most common
/// first.
async fn schema(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let p = match authenticate(&st, &headers, Permission::Query) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let columns = json!([
        {"name": "event_time", "type": "timestamp"},
        {"name": "ingest_time", "type": "timestamp"},
        {"name": "service", "type": "string"},
        {"name": "severity", "type": "string"},
        {"name": "severity_number", "type": "int"},
        {"name": "severity_text", "type": "string"},
        {"name": "body", "type": "value"},
        {"name": "trace_id", "type": "string"},
        {"name": "span_id", "type": "string"},
        {"name": "event_id", "type": "string"},
        {"name": "schema_version", "type": "int"},
        {"name": "tenant_id", "type": "string"},
        {"name": "attributes", "type": "map"},
        {"name": "resource_attributes", "type": "map"},
    ]);
    let keys_of = |engine: &Engine| {
        let (counts, mut truncated) = engine.snapshot(&p.tenant).attribute_key_rows();
        let mut keys: Vec<((&'static str, String), u64)> = counts.into_iter().collect();
        keys.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        let mut per_scope: BTreeMap<&'static str, usize> = BTreeMap::new();
        let list: Vec<J> = keys
            .into_iter()
            .filter(|((scope, _), _)| {
                let n = per_scope.entry(scope).or_default();
                *n += 1;
                if *n > SCHEMA_MAX_KEYS {
                    truncated = true;
                    false
                } else {
                    true
                }
            })
            .map(|((scope, key), rows)| json!({"key": key, "scope": scope, "rows": rows}))
            .collect();
        (list, truncated)
    };
    let (keys, truncated) = keys_of(&st.engine);
    let mut tables = vec![json!({
        "name": "logs", "enabled": true, "columns": columns,
        "attribute_keys": keys, "attribute_keys_truncated": truncated,
    })];
    match &st.query_log {
        Some(q) => {
            let (keys, truncated) = keys_of(q.engine());
            tables.push(json!({
                "name": "system.query_log", "enabled": true, "columns": columns,
                "attribute_keys": keys, "attribute_keys_truncated": truncated,
            }));
        }
        None => {
            tables.push(json!({"name": "system.query_log", "enabled": false, "columns": columns}))
        }
    }
    Json(json!({"tables": tables})).into_response()
}

/// `GET /api/v1/queries` (query, caller's tenant) — the tenant's queued and
/// running queries.
async fn list_queries(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let p = match authenticate(&st, &headers, Permission::Query) {
        Ok(p) => p,
        Err(r) => return r,
    };
    let prefix = format!("{}/", p.tenant);
    let entries: Vec<(String, J)> = {
        let reg = st.running.lock().unwrap_or_else(|e| e.into_inner());
        reg.iter()
            .filter(|(k, r)| r.tenant == p.tenant && k.starts_with(&prefix))
            .map(|(k, r)| {
                let running = r.admitted.load(std::sync::atomic::Ordering::Relaxed);
                (
                    k.clone(),
                    json!({
                        "query_id": &k[prefix.len()..],
                        "state": if running { "running" } else { "queued" },
                        "class": r.class.name(),
                        "elapsed_ms": r.started.elapsed().as_millis() as u64,
                        "fingerprint": r.fingerprint,
                        "cancel_requested": r.cancel.load(std::sync::atomic::Ordering::Relaxed),
                    }),
                )
            })
            .collect()
    };
    let queries: Vec<J> = entries
        .into_iter()
        .map(|(key, mut v)| {
            if v["state"] == "queued" {
                v["queue_position"] = json!(st.admission.position(&key));
            }
            v
        })
        .collect();
    Json(json!({"queries": queries})).into_response()
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

/// `GET /api/v1/admin/memory` (cluster.read): the memory budget, its
/// regions and the accounting tree with live usage (spec VIII.5.2).
async fn admin_memory(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = authenticate(&st, &headers, Permission::ClusterRead) {
        return r;
    }
    let Some(m) = &st.memory else {
        return error_response(&Error::unsupported("memory accounting is not enabled"));
    };
    Json(json!({
        "plan": m.plan,
        "tracked_bytes": m.tracked(),
        "tree": m.snapshot(),
    }))
    .into_response()
}

/// `GET /api/v1/admin/threads` (cluster.read): the thread inventory, the
/// spec's `system.threads` (STRATA-SYS-03-014). It lists every registered
/// engine thread with role, name, CPU time (as last recorded by the thread
/// itself) and affinity, the budget `2 × logical_cpus + 32`, and on Linux
/// the OS thread count, so untracked threads are visible.
async fn admin_threads(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = authenticate(&st, &headers, Permission::ClusterRead) {
        return r;
    }
    use stratadb_common::threads::{self, Role};
    let inv = threads::inventory();
    let cpus = threads::logical_cpus();
    let budget = threads::thread_budget(cpus);
    let os = threads::os_thread_count();
    let mut by_role = serde_json::Map::new();
    for r in Role::ALL {
        let n = inv.iter().filter(|t| t.role == r).count();
        by_role.insert(r.as_str().to_owned(), json!(n));
    }
    let scan = stratadb_query::scan_pool().stats();
    let blk = st.blk.stats();
    Json(json!({
        "logical_cpus": cpus,
        "budget": budget,
        "registered": inv.len(),
        "os_threads": os,
        "untracked": os.map(|o| o.saturating_sub(inv.len())),
        "within_budget": os.unwrap_or(inv.len()) <= budget,
        "by_role": by_role,
        "pools": {
            "scan": {"workers": scan.workers, "active_tasks": scan.active_tasks},
            "blocking": {"workers": blk.workers, "max_workers": blk.max_workers,
                         "busy": blk.busy, "queued": blk.queued,
                         "queue_capacity": blk.queue_capacity, "rejected": blk.rejected},
        },
        "threads": inv.iter().map(|t| json!({
            "name": t.name, "role": t.role.as_str(), "cpu_seconds": t.cpu_seconds,
            "cpu_age_ms": t.cpu_age_ms, "affinity": t.affinity,
        })).collect::<Vec<_>>(),
    }))
    .into_response()
}

async fn admin_compaction(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let p = match authenticate(&st, &headers, Permission::ClusterAdmin) {
        Ok(p) => p,
        Err(r) => return r,
    };
    if let Err(e) = st.audit_required("admin_compaction", Some(&p), json!({})) {
        return error_response(&Error::unavailable(format!("audit log unavailable: {e}")));
    }
    let engine = Arc::clone(&st.engine);
    let res = st
        .blk
        .run(move || {
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

/// Backups listed by `GET /api/v1/admin/backup`.
const MAX_LISTED_BACKUPS: usize = 200;

/// `GET /api/v1/admin/backup` (backup.create) — backups under `--backup-dir`,
/// newest first (at most 200): name, creation time, bytes and rows from each
/// backup's manifest. Directories without a readable manifest are skipped.
async fn list_backups(State(st): State<AppState>, headers: HeaderMap) -> Response {
    if let Err(r) = authenticate(&st, &headers, Permission::BackupCreate) {
        return r;
    }
    let Some(root) = st.cfg.backup_root.clone() else {
        return Json(json!({"enabled": false, "backups": [], "truncated": false})).into_response();
    };
    let res = st
        .blk
        .run(move || -> stratadb_common::Result<(Vec<J>, bool)> {
            let mut all: Vec<(i64, J)> = Vec::new();
            let entries = match std::fs::read_dir(&root) {
                Ok(d) => d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Ok((Vec::new(), false))
                }
                Err(e) => return Err(Error::io(format!("listing {}", root.display()), e)),
            };
            for entry in entries.flatten().take(10_000) {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let Ok(bm) = stratadb_storage::read_backup_manifest(&path) else {
                    continue;
                };
                let name = entry.file_name().to_string_lossy().into_owned();
                let bytes: u64 = bm.files.iter().map(|f| f.bytes).sum();
                all.push((
                    bm.created_ms,
                    json!({"name": name, "created_unix_ms": bm.created_ms, "bytes": bytes,
                           "rows": bm.rows, "segments": bm.segments}),
                ));
            }
            all.sort_by_key(|x| std::cmp::Reverse(x.0));
            let truncated = all.len() > MAX_LISTED_BACKUPS;
            all.truncate(MAX_LISTED_BACKUPS);
            Ok((all.into_iter().map(|(_, v)| v).collect(), truncated))
        })
        .await;
    match res {
        Ok((backups, truncated)) => {
            Json(json!({"enabled": true, "backups": backups, "truncated": truncated}))
                .into_response()
        }
        Err(e) => error_response(&e),
    }
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
    let res = st
        .blk
        .run(move || engine.backup(&root.join(&req.name)))
        .await;
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
    match st
        .blk
        .run(move || engine.set_retention(&tenant, Some(policy)))
        .await
    {
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
    match st
        .blk
        .run(move || engine.set_retention(&tenant, None))
        .await
    {
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
        Some(r) => {
            let running = r.admitted.load(std::sync::atomic::Ordering::Relaxed);
            let key = format!("{}/{}", p.tenant, id);
            // Queued queries report their approximate position (0 = next).
            let position = (!running).then(|| st.admission.position(&key)).flatten();
            Json(
                json!({"query_id": id, "state": if running { "running" } else { "queued" },
                "queue_position": position, "class": r.class.name(),
                "elapsed_ms": r.started.elapsed().as_millis() as u64, "query": r.sql,
                "cancel_requested": r.cancel.load(std::sync::atomic::Ordering::Relaxed)}),
            )
            .into_response()
        }
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
