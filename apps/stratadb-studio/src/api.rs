//! The studio's JSON API (`/api/...`), used by the embedded UI.
//!
//! Connection management works on the local registry; everything under
//! `/api/servers/{name}/...` is proxied to that StrataDB server's HTTP API v1
//! with the connection's token. See docs/studio/README.md for the routes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::auth;
use crate::error::{Result, StudioError};
use crate::metrics;
use crate::registry::{self, Connection, ConnectionPatch};
use crate::upstream::{Method, Response as UpResponse, Upstream};
use crate::AppState;

/// Who made a request, for audit logs: `local` (no studio auth) or a
/// non-secret session label.
#[derive(Debug, Clone)]
pub struct Actor(pub String);

const MAX_QUERY_TEXT: usize = 64 * 1024;
const MAX_CURSOR: usize = 4096;
const DEFAULT_QUERY_TIMEOUT_MS: u64 = 60_000;
/// Extra time given to the upstream call beyond the query's own timeout,
/// so the server's `timeout` error arrives before the studio gives up.
const QUERY_TIMEOUT_MARGIN: Duration = Duration::from_secs(5);
const MAX_ERROR_MESSAGE: usize = 2048;

fn parse_json<T: DeserializeOwned>(body: &Bytes) -> Result<T> {
    if body.is_empty() {
        return Err(StudioError::invalid("request body must be a JSON object"));
    }
    serde_json::from_slice(body)
        .map_err(|e| StudioError::invalid(format!("invalid request body: {e}")))
}

fn audit(action: &str, actor: &Actor, connection: Option<&str>, outcome: u16) {
    tracing::info!(
        target: "audit",
        action,
        actor = %actor.0,
        connection = connection.unwrap_or("-"),
        outcome,
        "studio admin action"
    );
}

fn connection(st: &AppState, name: &str) -> Result<Connection> {
    registry::validate_name(name)?;
    st.registry()
        .get(name)
        .ok_or_else(|| StudioError::not_found(format!("no connection named {name:?}")))
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// The JSON error body for a non-2xx upstream response: StrataDB's own
/// `error`/`message`/`retryable`/`code`/`retry_after_ms`, plus
/// `upstream_status`, `source: "upstream"` and, for queries, the query id
/// and fingerprint headers.
pub fn upstream_error_body(r: &UpResponse) -> Value {
    let parsed = serde_json::from_slice::<Value>(&r.body)
        .ok()
        .filter(Value::is_object)
        .unwrap_or(Value::Null);
    let default_kind = match r.status {
        400 => "invalid_input",
        401 => "unauthenticated",
        403 => "permission_denied",
        404 => "not_found",
        409 => "already_exists",
        413 => "payload_too_large",
        422 => "invalid_input",
        429 => "resource_exhausted",
        503 => "unavailable",
        504 => "timeout",
        _ => "internal",
    };
    let message = match parsed.get("message").and_then(Value::as_str) {
        Some(m) => truncate(m, MAX_ERROR_MESSAGE),
        None => {
            let text = String::from_utf8_lossy(&r.body);
            let t = text.trim();
            if t.is_empty() {
                format!("the server answered HTTP {}", r.status)
            } else {
                truncate(t, MAX_ERROR_MESSAGE)
            }
        }
    };
    let retry_after_ms = parsed
        .get("retry_after_ms")
        .and_then(Value::as_u64)
        .or_else(|| {
            r.header("retry-after")
                .and_then(|v| v.trim().parse::<u64>().ok())
                .map(|s| s.saturating_mul(1000))
        });
    let mut out = Map::new();
    out.insert(
        "error".into(),
        parsed
            .get("error")
            .and_then(Value::as_str)
            .map_or_else(|| json!(default_kind), |s| json!(truncate(s, 64))),
    );
    out.insert("message".into(), json!(message));
    out.insert(
        "retryable".into(),
        parsed
            .get("retryable")
            .and_then(Value::as_bool)
            .map_or_else(|| json!(matches!(r.status, 429 | 503)), |b| json!(b)),
    );
    if let Some(code) = parsed.get("code").and_then(Value::as_str) {
        out.insert("code".into(), json!(truncate(code, 64)));
    }
    if let Some(ms) = retry_after_ms {
        out.insert("retry_after_ms".into(), json!(ms));
    }
    out.insert("upstream_status".into(), json!(r.status));
    out.insert("source".into(), json!("upstream"));
    for (h, k) in [
        ("x-stratadb-query-id", "query_id"),
        ("x-stratadb-fingerprint", "fingerprint"),
        ("x-request-id", "request_id"),
    ] {
        if let Some(v) = r.header(h) {
            out.insert(k.into(), json!(truncate(v, 128)));
        }
    }
    Value::Object(out)
}

/// Relay an upstream response to the browser: a JSON success body as is,
/// an error normalized by [`upstream_error_body`] with the same status.
pub fn relay(r: UpResponse) -> Response {
    if r.is_success() {
        let json_ct = r
            .header("content-type")
            .is_some_and(|ct| ct.starts_with("application/json"));
        if !json_ct || serde_json::from_slice::<serde::de::IgnoredAny>(&r.body).is_err() {
            return StudioError::bad_gateway("the server sent a non-JSON response").into_response();
        }
        let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::OK);
        return (status, [(header::CONTENT_TYPE, "application/json")], r.body).into_response();
    }
    let status = StatusCode::from_u16(r.status)
        .ok()
        .filter(|s| s.is_client_error() || s.is_server_error())
        .unwrap_or(StatusCode::BAD_GATEWAY);
    let mut resp = (status, Json(upstream_error_body(&r))).into_response();
    if let Some(v) = r
        .header("retry-after")
        .and_then(|v| HeaderValue::from_str(v).ok())
    {
        resp.headers_mut().insert(header::RETRY_AFTER, v);
    }
    resp
}

// ---------------------------------------------------------------------------
// Session

pub async fn session(State(st): State<AppState>, headers: HeaderMap) -> Response {
    let authenticated = !st.config().auth_required()
        || auth::session_cookie(&headers)
            .and_then(|id| st.sessions().touch(&id, Instant::now()))
            .is_some();
    Json(json!({
        "auth_required": st.config().auth_required(),
        "authenticated": authenticated,
        "version": env!("CARGO_PKG_VERSION"),
        "csrf_header": auth::CSRF_HEADER,
        "limits": {
            "max_response_bytes": st.config().limits.max_response_bytes,
            "max_query_timeout_ms": st.config().limits.max_query_timeout.as_millis() as u64,
        },
    }))
    .into_response()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Login {
    password: String,
}

pub async fn login(State(st): State<AppState>, body: Bytes) -> Response {
    let Some(secret) = st.config().password.clone() else {
        return StudioError::invalid("this studio has no login (no password configured)")
            .into_response();
    };
    let now = Instant::now();
    if let Err(e) = st.sessions().check_throttle(now) {
        tracing::warn!(target: "audit", action = "login", outcome = 429u16, "login throttled");
        return e.into_response();
    }
    let req: Login = match parse_json(&body) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    if !auth::password_matches(&secret, &req.password) {
        st.sessions().record_failure(now);
        tracing::warn!(target: "audit", action = "login", outcome = 401u16, "login failed");
        return StudioError::unauthenticated("wrong password").into_response();
    }
    let s = match st.sessions().create(now) {
        Ok(s) => s,
        Err(e) => return e.into_response(),
    };
    let label = st
        .sessions()
        .touch(&s.id, now)
        .unwrap_or_else(|| "s:?".into());
    tracing::info!(target: "audit", action = "login", actor = %label, outcome = 200u16, "login");
    let cookie = auth::set_cookie(&s.id, st.sessions().ttl(), st.secure_cookie());
    match HeaderValue::from_str(&cookie) {
        Ok(v) => (
            [(header::SET_COOKIE, v)],
            Json(json!({"authenticated": true})),
        )
            .into_response(),
        Err(_) => StudioError::internal("could not build the session cookie").into_response(),
    }
}

pub async fn logout(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    headers: HeaderMap,
) -> Response {
    if let Some(id) = auth::session_cookie(&headers) {
        st.sessions().remove(&id);
    }
    audit("logout", &actor, None, 200);
    let cookie = auth::clear_cookie(st.secure_cookie());
    match HeaderValue::from_str(&cookie) {
        Ok(v) => (
            [(header::SET_COOKIE, v)],
            Json(json!({"authenticated": false})),
        )
            .into_response(),
        Err(_) => StudioError::internal("could not build the session cookie").into_response(),
    }
}

// ---------------------------------------------------------------------------
// Connections

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NewConnection {
    name: String,
    url: String,
    token: String,
    #[serde(default = "yes")]
    tls_verify: bool,
    #[serde(default)]
    ca_file: Option<String>,
}

fn yes() -> bool {
    true
}

impl NewConnection {
    fn build(self) -> Result<Connection> {
        let ca = self
            .ca_file
            .filter(|s| !s.trim().is_empty())
            .map(std::path::PathBuf::from);
        Connection::new(&self.name, &self.url, &self.token, self.tls_verify, ca)
    }
}

pub async fn list_connections(State(st): State<AppState>) -> Response {
    Json(json!({"connections": st.registry().list()})).into_response()
}

pub async fn get_connection(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    match registry::validate_name(&name).and_then(|()| {
        st.registry()
            .view(&name)
            .ok_or_else(|| StudioError::not_found(format!("no connection named {name:?}")))
    }) {
        Ok(v) => Json(v).into_response(),
        Err(e) => e.into_response(),
    }
}

pub async fn add_connection(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Response {
    let res = parse_json::<NewConnection>(&body)
        .and_then(NewConnection::build)
        .and_then(|c| {
            let name = c.name.clone();
            st.registry().add(c).map(|v| (name, v))
        });
    match res {
        Ok((name, v)) => {
            audit("connection_add", &actor, Some(&name), 201);
            (StatusCode::CREATED, Json(v)).into_response()
        }
        Err(e) => {
            audit("connection_add", &actor, None, e.status.as_u16());
            e.into_response()
        }
    }
}

pub async fn update_connection(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let res = registry::validate_name(&name)
        .and_then(|()| parse_json::<ConnectionPatch>(&body))
        .and_then(|p| st.registry().update(&name, p));
    match res {
        Ok(v) => {
            st.upstream().forget(&name);
            st.tracker().forget(&name);
            audit("connection_update", &actor, Some(&name), 200);
            Json(v).into_response()
        }
        Err(e) => {
            audit("connection_update", &actor, Some(&name), e.status.as_u16());
            e.into_response()
        }
    }
}

pub async fn remove_connection(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(name): Path<String>,
) -> Response {
    let res = registry::validate_name(&name).and_then(|()| st.registry().remove(&name));
    match res {
        Ok(()) => {
            st.upstream().forget(&name);
            st.tracker().forget(&name);
            audit("connection_remove", &actor, Some(&name), 200);
            Json(json!({"removed": name})).into_response()
        }
        Err(e) => {
            audit("connection_remove", &actor, Some(&name), e.status.as_u16());
            e.into_response()
        }
    }
}

fn status_of(r: &Result<UpResponse>) -> Option<u16> {
    r.as_ref().ok().map(|r| r.status)
}

fn json_of(r: &Result<UpResponse>) -> Value {
    match r {
        Ok(r) if r.is_success() => serde_json::from_slice(&r.body).unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

/// Probe a server: reachability, versions, readiness, and what the token
/// may do (without side effects: `GET /query/<unknown id>` answers 404 to a
/// valid token with `query`, 403 without it and 401 for a bad token).
pub async fn probe(up: &Upstream, c: &Connection) -> Value {
    let t = up.limits().request_timeout.min(Duration::from_secs(10));
    let started = Instant::now();
    let (health, version, ready, query, cluster, retention) = tokio::join!(
        up.call(c, Method::Get, "/api/v1/health", None, t),
        up.call(c, Method::Get, "/api/v1/version", None, t),
        up.call(c, Method::Get, "/api/v1/ready", None, t),
        up.call(c, Method::Get, "/api/v1/query/studio-probe", None, t),
        up.call(c, Method::Get, "/api/v1/admin/workload", None, t),
        up.call(c, Method::Get, "/api/v1/retention", None, t),
    );
    let reachable = status_of(&health) == Some(200);
    let error = health
        .as_ref()
        .err()
        .map(|e| e.message.clone())
        .or_else(|| {
            (!reachable).then(|| format!("health check answered HTTP {:?}", status_of(&health)))
        });
    let perm = |r: &Result<UpResponse>| match status_of(r) {
        Some(s) if (200..300).contains(&s) || s == 404 => json!(true),
        Some(403) => json!(false),
        _ => Value::Null,
    };
    let token_valid = match status_of(&query) {
        Some(401) => json!(false),
        Some(403 | 404) => json!(true),
        Some(s) if (200..300).contains(&s) => json!(true),
        _ => Value::Null,
    };
    let ok = reachable && token_valid == json!(true);
    json!({
        "ok": ok,
        "url": c.url,
        "reachable": reachable,
        "error": error,
        "token_valid": token_valid,
        "health_status": status_of(&health),
        "ready_status": status_of(&ready),
        "ready": match &ready {
            Ok(r) => serde_json::from_slice::<Value>(&r.body).unwrap_or(Value::Null),
            Err(_) => Value::Null,
        },
        "version": json_of(&version),
        "permissions": {
            "query": perm(&query),
            "cluster.read": perm(&cluster),
            "retention.manage": perm(&retention),
        },
        "elapsed_ms": started.elapsed().as_millis() as u64,
    })
}

pub async fn test_connection(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(name): Path<String>,
) -> Response {
    let c = match connection(&st, &name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let v = probe(st.upstream(), &c).await;
    audit("connection_test", &actor, Some(&name), 200);
    Json(v).into_response()
}

/// Test connection settings before saving them (nothing is stored).
pub async fn test_unsaved(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    body: Bytes,
) -> Response {
    let c = match parse_json::<NewConnection>(&body).and_then(NewConnection::build) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    // A distinct cache key so the probe never reuses a saved agent.
    let probe_conn = Connection {
        name: format!("probe:{}", c.name),
        ..c
    };
    let v = probe(st.upstream(), &probe_conn).await;
    st.upstream().forget(&probe_conn.name);
    audit("connection_test", &actor, None, 200);
    Json(v).into_response()
}

// ---------------------------------------------------------------------------
// Simple proxies

async fn get_relay(st: &AppState, name: &str, path: &str) -> Response {
    let c = match connection(st, name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let t = st.config().limits.request_timeout;
    match st.upstream().call(&c, Method::Get, path, None, t).await {
        Ok(r) => relay(r),
        Err(e) => e.into_response(),
    }
}

pub async fn health(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    get_relay(&st, &name, "/api/v1/health").await
}

pub async fn ready(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    // /ready answers 503 with a JSON status while starting or draining: the
    // dashboard wants that body, so relay it as data rather than an error.
    let c = match connection(&st, &name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let t = st.config().limits.request_timeout;
    match st
        .upstream()
        .call(&c, Method::Get, "/api/v1/ready", None, t)
        .await
    {
        Ok(r) if r.status == 503 => {
            let body = serde_json::from_slice::<Value>(&r.body).unwrap_or(Value::Null);
            Json(json!({"ready": false, "upstream_status": 503, "body": body})).into_response()
        }
        Ok(r) if r.is_success() => {
            let body = serde_json::from_slice::<Value>(&r.body).unwrap_or(Value::Null);
            Json(json!({"ready": true, "upstream_status": r.status, "body": body})).into_response()
        }
        Ok(r) => relay(r),
        Err(e) => e.into_response(),
    }
}

pub async fn version(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    get_relay(&st, &name, "/api/v1/version").await
}

/// Health, readiness and version in one call (tree status and dashboard header).
pub async fn status(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    let c = match connection(&st, &name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let up = st.upstream();
    let t = st
        .config()
        .limits
        .request_timeout
        .min(Duration::from_secs(10));
    let started = Instant::now();
    let (h, r, v) = tokio::join!(
        up.call(&c, Method::Get, "/api/v1/health", None, t),
        up.call(&c, Method::Get, "/api/v1/ready", None, t),
        up.call(&c, Method::Get, "/api/v1/version", None, t),
    );
    let ready_body = match &r {
        Ok(x) => serde_json::from_slice::<Value>(&x.body).unwrap_or(Value::Null),
        Err(_) => Value::Null,
    };
    Json(json!({
        "name": name,
        "url": c.url,
        "reachable": status_of(&h) == Some(200),
        "error": h.as_ref().err().map(|e| e.message.clone()),
        "health": json_of(&h),
        "ready": status_of(&r).map(|s| s == 200),
        "ready_status": status_of(&r),
        "state": ready_body.get("state").cloned().unwrap_or(Value::Null),
        "engine": ready_body.get("status").cloned().unwrap_or(Value::Null),
        "version": json_of(&v),
        "elapsed_ms": started.elapsed().as_millis() as u64,
    }))
    .into_response()
}

// ---------------------------------------------------------------------------
// Queries

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryBody {
    query: String,
    #[serde(default)]
    query_id: Option<String>,
    #[serde(default)]
    page_size: Option<u64>,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    workload_class: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    max_rows: Option<u64>,
}

/// Query ids use StrataDB's tenant-id alphabet.
pub fn validate_query_id(id: &str) -> Result<()> {
    let ok = !id.is_empty()
        && id.len() <= 64
        && !id.starts_with('-')
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(StudioError::invalid(
            "query_id must be 1-64 characters of [A-Za-z0-9_-] and not start with '-'",
        ))
    }
}

fn new_query_id() -> String {
    let mut b = [0u8; 8];
    if getrandom::fill(&mut b).is_err() {
        // Not secret; uniqueness within a tenant is all that matters.
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        return format!("ss{n:x}");
    }
    format!(
        "ss{}",
        b.iter().map(|x| format!("{x:02x}")).collect::<String>()
    )
}

/// Cancels the upstream query if the browser goes away before it finishes
/// (the handler future is dropped), so abandoned queries do not keep
/// running on the server.
struct CancelOnDrop {
    armed: bool,
    upstream: Arc<Upstream>,
    conn: Connection,
    id: String,
    timeout: Duration,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let (up, conn, id, t) = (
            Arc::clone(&self.upstream),
            self.conn.clone(),
            self.id.clone(),
            self.timeout,
        );
        handle.spawn(async move {
            let path = format!("/api/v1/query/{id}");
            let r = up.call(&conn, Method::Delete, &path, None, t).await;
            tracing::info!(
                target: "audit",
                action = "query_cancel_on_disconnect",
                connection = %conn.name,
                query_id = %id,
                outcome = r.map(|r| r.status).unwrap_or(0),
                "cancelled a query whose browser request went away"
            );
        });
    }
}

pub async fn query(State(st): State<AppState>, Path(name): Path<String>, body: Bytes) -> Response {
    let c = match connection(&st, &name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let q: QueryBody = match parse_json(&body) {
        Ok(q) => q,
        Err(e) => return e.into_response(),
    };
    if q.query.trim().is_empty() || q.query.len() > MAX_QUERY_TEXT {
        return StudioError::invalid(format!("query must be 1..={MAX_QUERY_TEXT} bytes"))
            .into_response();
    }
    if q.cursor.as_ref().is_some_and(|c| c.len() > MAX_CURSOR) {
        return StudioError::invalid("cursor is too long").into_response();
    }
    if let Some(w) = &q.workload_class {
        if !matches!(w.as_str(), "interactive" | "normal" | "batch") {
            return StudioError::invalid("workload_class must be normal or batch").into_response();
        }
    }
    let id = match q.query_id {
        Some(id) => match validate_query_id(&id) {
            Ok(()) => id,
            Err(e) => return e.into_response(),
        },
        None => new_query_id(),
    };
    let mut fwd = Map::new();
    fwd.insert("query".into(), json!(q.query));
    fwd.insert("query_id".into(), json!(id));
    for (k, v) in [
        ("page_size", q.page_size.map(Value::from)),
        ("cursor", q.cursor.map(Value::from)),
        ("workload_class", q.workload_class.map(Value::from)),
        ("timeout_ms", q.timeout_ms.map(Value::from)),
        ("max_rows", q.max_rows.map(Value::from)),
    ] {
        if let Some(v) = v {
            fwd.insert(k.into(), v);
        }
    }
    let limits = &st.config().limits;
    let timeout = Duration::from_millis(q.timeout_ms.unwrap_or(DEFAULT_QUERY_TIMEOUT_MS))
        .saturating_add(QUERY_TIMEOUT_MARGIN)
        .min(limits.max_query_timeout);
    let body = match serde_json::to_vec(&Value::Object(fwd)) {
        Ok(b) => b,
        Err(e) => return StudioError::internal(format!("encoding query: {e}")).into_response(),
    };
    let mut guard = CancelOnDrop {
        armed: true,
        upstream: Arc::clone(st.upstream_arc()),
        conn: c.clone(),
        id: id.clone(),
        timeout: limits.request_timeout,
    };
    let started = Instant::now();
    let res = st
        .upstream()
        .call(&c, Method::Post, "/api/v1/query", Some(body), timeout)
        .await;
    guard.armed = false;
    tracing::debug!(
        component = "proxy",
        connection = %name,
        query_id = %id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        status = status_of(&res).unwrap_or(0),
        "query relayed"
    );
    match res {
        Ok(r) => relay(r),
        Err(e) => {
            let mut v = e.body();
            v["query_id"] = json!(id);
            (e.status, Json(v)).into_response()
        }
    }
}

pub async fn query_status(
    State(st): State<AppState>,
    Path((name, id)): Path<(String, String)>,
) -> Response {
    if let Err(e) = validate_query_id(&id) {
        return e.into_response();
    }
    get_relay(&st, &name, &format!("/api/v1/query/{id}")).await
}

pub async fn cancel_query(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path((name, id)): Path<(String, String)>,
) -> Response {
    let c = match validate_query_id(&id).and_then(|()| connection(&st, &name)) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let t = st.config().limits.request_timeout;
    let path = format!("/api/v1/query/{id}");
    match st.upstream().call(&c, Method::Delete, &path, None, t).await {
        Ok(r) => {
            audit("query_cancel", &actor, Some(&name), r.status);
            relay(r)
        }
        Err(e) => {
            audit("query_cancel", &actor, Some(&name), e.status.as_u16());
            e.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Metrics

async fn scrape(st: &AppState, name: &str) -> Result<metrics::Scrape> {
    let c = connection(st, name)?;
    let t = st.config().limits.request_timeout;
    let r = st
        .upstream()
        .call(&c, Method::Get, "/api/v1/metrics", None, t)
        .await?;
    if !r.is_success() {
        let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::BAD_GATEWAY);
        let b = upstream_error_body(&r);
        return Err(StudioError {
            status,
            kind: "upstream",
            message: b["message"].as_str().unwrap_or("metrics failed").to_owned(),
        });
    }
    let text = String::from_utf8(r.body)
        .map_err(|_| StudioError::bad_gateway("metrics are not UTF-8 text"))?;
    // Parsing is CPU work proportional to the (capped) body size.
    tokio::task::spawn_blocking(move || metrics::parse(&text))
        .await
        .map_err(|e| StudioError::internal(format!("metrics parser: {e}")))?
        .map_err(|e| StudioError::bad_gateway(format!("invalid metrics from the server: {e}")))
}

fn metrics_error(e: StudioError) -> Response {
    if e.kind == "upstream" {
        let mut v = json!({
            "error": match e.status.as_u16() { 401 => "unauthenticated", 403 => "permission_denied", _ => "unavailable" },
            "message": e.message,
            "retryable": false,
            "source": "upstream",
            "upstream_status": e.status.as_u16(),
        });
        if e.status == StatusCode::FORBIDDEN {
            v["message"] = json!("the connection's token lacks cluster.read (needed for metrics)");
        }
        return (e.status, Json(v)).into_response();
    }
    e.into_response()
}

/// `GET /api/servers/{name}/metrics`: the dashboard summary with rates and
/// interval latencies since the previous call for this connection.
pub async fn metrics_summary(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    match scrape(&st, &name).await {
        Ok(s) => Json(st.tracker().observe(&name, s)).into_response(),
        Err(e) => metrics_error(e),
    }
}

/// `GET /api/servers/{name}/metrics/families`: every parsed family.
pub async fn metrics_families(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    match scrape(&st, &name).await {
        Ok(s) => Json(s).into_response(),
        Err(e) => metrics_error(e),
    }
}

// ---------------------------------------------------------------------------
// Admin

pub async fn admin_get(
    State(st): State<AppState>,
    Path((name, which)): Path<(String, String)>,
) -> Response {
    let path = match which.as_str() {
        "workload" => "/api/v1/admin/workload",
        "memory" => "/api/v1/admin/memory",
        "threads" => "/api/v1/admin/threads",
        "snapshots" => "/api/v1/admin/snapshots",
        _ => return StudioError::not_found("unknown admin view").into_response(),
    };
    get_relay(&st, &name, path).await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Backup {
    name: String,
}

pub async fn admin_post(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path((name, which)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let c = match connection(&st, &name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let (action, path, fwd) = match which.as_str() {
        "compaction" => ("compaction", "/api/v1/admin/compaction", None),
        "backup" => {
            let b: Backup = match parse_json(&body) {
                Ok(b) => b,
                Err(e) => return e.into_response(),
            };
            if registry::validate_name(&b.name).is_err() {
                return StudioError::invalid(
                    "backup name must be 1-64 characters of [A-Za-z0-9_-] and not start with '-'",
                )
                .into_response();
            }
            let v = json!({"name": b.name}).to_string().into_bytes();
            ("backup", "/api/v1/admin/backup", Some(v))
        }
        _ => return StudioError::not_found("unknown admin action").into_response(),
    };
    let t = st.config().limits.admin_timeout;
    match st.upstream().call(&c, Method::Post, path, fwd, t).await {
        Ok(r) => {
            audit(action, &actor, Some(&name), r.status);
            relay(r)
        }
        Err(e) => {
            audit(action, &actor, Some(&name), e.status.as_u16());
            e.into_response()
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Retention {
    max_age_days: u64,
    #[serde(default)]
    hold: bool,
}

pub async fn get_retention(State(st): State<AppState>, Path(name): Path<String>) -> Response {
    get_relay(&st, &name, "/api/v1/retention").await
}

pub async fn put_retention(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    let c = match connection(&st, &name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let r: Retention = match parse_json(&body) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    if r.max_age_days == 0 {
        return StudioError::invalid("max_age_days must be at least 1").into_response();
    }
    let fwd = json!({"max_age_days": r.max_age_days, "hold": r.hold})
        .to_string()
        .into_bytes();
    let t = st.config().limits.request_timeout;
    match st
        .upstream()
        .call(&c, Method::Put, "/api/v1/retention", Some(fwd), t)
        .await
    {
        Ok(resp) => {
            audit("retention_set", &actor, Some(&name), resp.status);
            relay(resp)
        }
        Err(e) => {
            audit("retention_set", &actor, Some(&name), e.status.as_u16());
            e.into_response()
        }
    }
}

pub async fn delete_retention(
    State(st): State<AppState>,
    Extension(actor): Extension<Actor>,
    Path(name): Path<String>,
) -> Response {
    let c = match connection(&st, &name) {
        Ok(c) => c,
        Err(e) => return e.into_response(),
    };
    let t = st.config().limits.request_timeout;
    match st
        .upstream()
        .call(&c, Method::Delete, "/api/v1/retention", None, t)
        .await
    {
        Ok(resp) => {
            audit("retention_clear", &actor, Some(&name), resp.status);
            relay(resp)
        }
        Err(e) => {
            audit("retention_clear", &actor, Some(&name), e.status.as_u16());
            e.into_response()
        }
    }
}

pub async fn api_not_found() -> Response {
    StudioError::not_found("no such studio API route").into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn resp(status: u16, headers: &[(&str, &str)], body: &str) -> UpResponse {
        UpResponse {
            status,
            headers: headers
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn upstream_errors_keep_code_and_retry_hint() {
        let r = resp(
            429,
            &[("retry-after", "2"), ("x-stratadb-query-id", "q1")],
            r#"{"error":"resource_exhausted","code":"TOO_MANY_QUERIES","message":"queue full","retryable":true,"retry_after_ms":1500}"#,
        );
        let b = upstream_error_body(&r);
        assert_eq!(b["error"], "resource_exhausted");
        assert_eq!(b["code"], "TOO_MANY_QUERIES");
        assert_eq!(b["retry_after_ms"], 1500);
        assert_eq!(b["upstream_status"], 429);
        assert_eq!(b["source"], "upstream");
        assert_eq!(b["query_id"], "q1");
        // Retry-After header used when the body has no hint.
        let r = resp(503, &[("retry-after", "3")], "not json");
        let b = upstream_error_body(&r);
        assert_eq!(
            (
                b["error"].clone(),
                b["retry_after_ms"].clone(),
                b["message"].clone()
            ),
            (json!("unavailable"), json!(3000), json!("not json"))
        );
        assert_eq!(b["retryable"], true);
    }

    #[test]
    fn query_ids_are_validated() {
        assert!(validate_query_id("ss0123abcd").is_ok());
        for bad in ["", "-x", "a/b", "a b", "../x", &"x".repeat(65)] {
            assert!(validate_query_id(bad).is_err(), "{bad}");
        }
        let id = new_query_id();
        assert!(validate_query_id(&id).is_ok(), "{id}");
    }
}
