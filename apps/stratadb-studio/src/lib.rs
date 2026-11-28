//! StrataStudio: a web administration and query tool for StrataDB
//! (ADR-0041, docs/studio/README.md).
//!
//! One process serves an embedded single-page UI and a JSON API. The API
//! proxies to configured StrataDB servers over their HTTP API v1, so the
//! browser never talks to StrataDB directly and API tokens never leave the
//! studio process.

pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod metrics;
pub mod registry;
pub mod tls;
pub mod upstream;
pub mod web;

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{DefaultBodyLimit, Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;

use crate::config::StudioConfig;
use crate::error::StudioError;
use crate::registry::Registry;
use crate::upstream::Upstream;

#[derive(Debug)]
struct Inner {
    cfg: StudioConfig,
    registry: Registry,
    upstream: Arc<Upstream>,
    sessions: auth::Sessions,
    tracker: metrics::Tracker,
    loopback: bool,
    secure_cookie: bool,
}

/// Shared state of the studio app.
#[derive(Debug, Clone)]
pub struct AppState(Arc<Inner>);

impl AppState {
    /// Validate the configuration and open the connection registry.
    pub fn new(cfg: StudioConfig) -> Result<Self, StudioError> {
        cfg.validate().map_err(StudioError::invalid)?;
        let registry = Registry::open(&cfg.data_dir)?;
        Ok(Self(Arc::new(Inner {
            upstream: Arc::new(Upstream::new(cfg.limits.clone())),
            sessions: auth::Sessions::new(cfg.limits.session_ttl, cfg.limits.max_sessions),
            tracker: metrics::Tracker::new(registry::MAX_CONNECTIONS),
            loopback: cfg.listen.ip().is_loopback(),
            secure_cookie: cfg.tls.is_some(),
            registry,
            cfg,
        })))
    }

    pub fn config(&self) -> &StudioConfig {
        &self.0.cfg
    }

    pub fn registry(&self) -> &Registry {
        &self.0.registry
    }

    pub fn upstream(&self) -> &Upstream {
        &self.0.upstream
    }

    pub fn upstream_arc(&self) -> &Arc<Upstream> {
        &self.0.upstream
    }

    pub fn sessions(&self) -> &auth::Sessions {
        &self.0.sessions
    }

    pub fn tracker(&self) -> &metrics::Tracker {
        &self.0.tracker
    }

    pub fn secure_cookie(&self) -> bool {
        self.0.secure_cookie
    }
}

/// Host check on every request; on `/api/`: CSRF check of state-changing
/// methods and the session check (when studio auth is on).
async fn guard(State(st): State<AppState>, mut req: Request, next: Next) -> Response {
    if let Err(e) = auth::check_host(req.headers(), st.0.loopback, &st.0.cfg.allowed_hosts) {
        return e.into_response();
    }
    let path = req.uri().path();
    if path.starts_with("/api/") {
        if auth::is_state_changing(req.method()) {
            if let Err(e) = auth::check_csrf(req.headers()) {
                tracing::warn!(target: "audit", action = "csrf_refused", path = %path, "refused a request failing the CSRF check");
                return e.into_response();
            }
        }
        let public = matches!(path, "/api/session" | "/api/auth/login");
        let label = auth::session_cookie(req.headers())
            .and_then(|id| st.sessions().touch(&id, Instant::now()));
        let actor = match (st.config().auth_required(), label) {
            (_, Some(l)) => l,
            (false, None) => "local".to_owned(),
            (true, None) if public => "anonymous".to_owned(),
            (true, None) => {
                return StudioError::unauthenticated("log in to the studio first").into_response()
            }
        };
        req.extensions_mut().insert(api::Actor(actor));
    }
    next.run(req).await
}

/// The studio's router: UI, JSON API, guard and security headers.
pub fn router(st: AppState) -> Router {
    let hsts = st.secure_cookie();
    let limit = st.config().limits.max_body_bytes;
    let srv = "/api/servers/{name}";
    Router::new()
        .route("/", get(web::index))
        .route("/index.html", get(web::index))
        .route("/assets/{*path}", get(web::asset))
        .route("/api/session", get(api::session))
        .route("/api/auth/login", post(api::login))
        .route("/api/auth/logout", post(api::logout))
        .route(
            "/api/connections",
            get(api::list_connections).post(api::add_connection),
        )
        .route("/api/connection-test", post(api::test_unsaved))
        .route(
            "/api/connections/{name}",
            get(api::get_connection)
                .put(api::update_connection)
                .delete(api::remove_connection),
        )
        .route("/api/connections/{name}/test", post(api::test_connection))
        .route(&format!("{srv}/status"), get(api::status))
        .route(&format!("{srv}/health"), get(api::health))
        .route(&format!("{srv}/ready"), get(api::ready))
        .route(&format!("{srv}/version"), get(api::version))
        .route(&format!("{srv}/query"), post(api::query))
        .route(
            &format!("{srv}/query/{{id}}"),
            get(api::query_status).delete(api::cancel_query),
        )
        .route(&format!("{srv}/metrics"), get(api::metrics_summary))
        .route(
            &format!("{srv}/metrics/families"),
            get(api::metrics_families),
        )
        .route(
            &format!("{srv}/admin/{{which}}"),
            get(api::admin_get).post(api::admin_post),
        )
        .route(
            &format!("{srv}/retention"),
            get(api::get_retention)
                .put(api::put_retention)
                .delete(api::delete_retention),
        )
        .route("/api/{*rest}", axum::routing::any(api::api_not_found))
        .layer(axum::middleware::from_fn_with_state(st.clone(), guard))
        .layer(axum::middleware::from_fn(move |req, next| {
            web::security_headers(hsts, req, next)
        }))
        .layer(DefaultBodyLimit::max(limit))
        .with_state(st)
}

/// Serve the studio until `shutdown` resolves.
pub async fn serve(
    cfg: StudioConfig,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), String> {
    let tls_files = cfg.tls.clone();
    let listen = cfg.listen;
    let auth_required = cfg.auth_required();
    let state = AppState::new(cfg).map_err(|e| e.to_string())?;
    let app = router(state.clone());
    let tcp = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|e| format!("binding {listen}: {e}"))?;
    let local = tcp
        .local_addr()
        .map_err(|e| format!("local address: {e}"))?;
    let scheme = if tls_files.is_some() { "https" } else { "http" };
    tracing::info!(
        component = "studio",
        address = %local,
        tls = tls_files.is_some(),
        auth = auth_required,
        connections = state.registry().list().len(),
        registry = %state.registry().path().display(),
        "StrataStudio listening on {scheme}://{local}/"
    );
    match tls_files {
        Some(t) => {
            let sc = tls::load_server_config(&t.cert, &t.key)?;
            let l = tls::TlsListener::new(tcp, sc).map_err(|e| format!("TLS listener: {e}"))?;
            axum::serve(l, app)
                .with_graceful_shutdown(shutdown)
                .await
                .map_err(|e| format!("server: {e}"))
        }
        None => axum::serve(tcp, app)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|e| format!("server: {e}")),
    }
}
