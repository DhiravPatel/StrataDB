//! Calls to StrataDB servers on behalf of the browser.
//!
//! - **Bounded:** at most `max_concurrent_upstream` calls run at once (each
//!   on a blocking thread, since `ureq` is synchronous); a request waits at
//!   most `upstream_queue_timeout` for a slot, then gets `503`. Every call
//!   has a global timeout, and response bodies are read up to
//!   `max_response_bytes`.
//! - **Credentials:** the token is sent only in the `Authorization` header,
//!   never in a URL; redirects are not followed (a redirect could forward
//!   the request elsewhere) and environment proxies are ignored.
//! - **TLS:** WebPKI roots by default, a connection's CA file instead when
//!   set, or no verification when the operator turned it off.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;
use ureq::tls::{Certificate, RootCerts, TlsConfig, TlsProvider};

use crate::config::Limits;
use crate::error::{Result, StudioError};
use crate::registry::Connection;

/// Response headers worth surfacing to the browser.
const KEPT_HEADERS: &[&str] = &[
    "retry-after",
    "content-type",
    "x-request-id",
    "x-stratadb-query-id",
    "x-stratadb-fingerprint",
    "x-stratadb-queue-us",
    "x-stratadb-parse-us",
    "x-stratadb-plan-us",
    "x-stratadb-exec-us",
    "x-stratadb-cpu-us",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
        }
    }
}

/// An upstream response (any status).
#[derive(Debug, Clone)]
pub struct Response {
    pub status: u16,
    /// Lower-cased names from [`KEPT_HEADERS`].
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Agents are cached per connection and rebuilt when its URL or TLS
/// settings change (the cache key).
type AgentCache = HashMap<String, (String, ureq::Agent)>;

#[derive(Debug)]
pub struct Upstream {
    limits: Limits,
    slots: Arc<Semaphore>,
    agents: Mutex<AgentCache>,
}

fn load_ca(path: &std::path::Path) -> Result<RootCerts> {
    let pem = std::fs::read(path)
        .map_err(|e| StudioError::invalid(format!("reading CA file {}: {e}", path.display())))?;
    let mut certs = Vec::new();
    for item in ureq::tls::parse_pem(&pem) {
        match item {
            Ok(ureq::tls::PemItem::Certificate(c)) => certs.push(c),
            Ok(_) => {}
            Err(e) => {
                return Err(StudioError::invalid(format!(
                    "parsing CA file {}: {e}",
                    path.display()
                )))
            }
        }
    }
    if certs.is_empty() {
        return Err(StudioError::invalid(format!(
            "no certificates in CA file {}",
            path.display()
        )));
    }
    let certs: Vec<Certificate<'static>> = certs;
    Ok(RootCerts::from(certs))
}

fn build_agent(c: &Connection) -> Result<ureq::Agent> {
    let roots = match &c.ca_file {
        Some(p) => load_ca(p)?,
        None => RootCerts::WebPki,
    };
    let tls = TlsConfig::builder()
        .provider(TlsProvider::Rustls)
        .root_certs(roots)
        .disable_verification(!c.tls_verify)
        .build();
    let cfg = ureq::Agent::config_builder()
        .http_status_as_error(false)
        .max_redirects(0)
        .proxy(None)
        .timeout_connect(Some(Duration::from_secs(5)))
        .user_agent(concat!("strata-studio/", env!("CARGO_PKG_VERSION")))
        .max_response_header_size(64 * 1024)
        .tls_config(tls)
        .build();
    Ok(ureq::Agent::new_with_config(cfg))
}

fn map_err(e: ureq::Error, url: &str) -> StudioError {
    match e {
        ureq::Error::Timeout(_) => {
            StudioError::timeout(format!("the server at {url} did not answer in time"))
        }
        ureq::Error::BodyExceedsLimit(n) => StudioError::new(
            axum::http::StatusCode::BAD_GATEWAY,
            "resource_exhausted",
            format!("the server's response exceeds the studio's limit of {n} bytes (lower page_size or LIMIT)"),
        ),
        ureq::Error::Tls(m) => StudioError::bad_gateway(format!("TLS error talking to {url}: {m}")),
        ureq::Error::Rustls(m) => {
            StudioError::bad_gateway(format!("TLS error talking to {url}: {m}"))
        }
        ureq::Error::HostNotFound => StudioError::bad_gateway(format!("cannot resolve {url}")),
        ureq::Error::ConnectionFailed => {
            StudioError::bad_gateway(format!("cannot connect to {url}"))
        }
        ureq::Error::Io(io) => StudioError::bad_gateway(format!("cannot reach {url}: {io}")),
        other => StudioError::bad_gateway(format!("request to {url} failed: {other}")),
    }
}

impl Upstream {
    pub fn new(limits: Limits) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(limits.max_concurrent_upstream)),
            limits,
            agents: Mutex::new(HashMap::new()),
        }
    }

    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    fn agent(&self, c: &Connection) -> Result<ureq::Agent> {
        let key = format!("{}|{}|{:?}", c.url, c.tls_verify, c.ca_file);
        let mut g = self.agents.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((k, a)) = g.get(&c.name) {
            if *k == key {
                return Ok(a.clone());
            }
        }
        let a = build_agent(c)?;
        // One entry per connection name; the registry bounds the count.
        if g.len() >= crate::registry::MAX_CONNECTIONS {
            g.clear();
        }
        g.insert(c.name.clone(), (key, a.clone()));
        Ok(a)
    }

    /// Drop the cached agent of a removed or edited connection.
    pub fn forget(&self, name: &str) {
        self.agents
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(name);
    }

    /// Call `method path` (path below the server root, e.g. `/api/v1/health`).
    pub async fn call(
        &self,
        c: &Connection,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
        timeout: Duration,
    ) -> Result<Response> {
        debug_assert!(path.starts_with('/'));
        let permit = tokio::time::timeout(
            self.limits.upstream_queue_timeout,
            Arc::clone(&self.slots).acquire_owned(),
        )
        .await
        .map_err(|_| {
            StudioError::unavailable("the studio is at its limit of concurrent server calls; retry")
        })?
        .map_err(|_| StudioError::unavailable("the studio is shutting down"))?;
        let agent = self.agent(c)?;
        let url = format!("{}{path}", c.url);
        let auth = format!("Bearer {}", c.token);
        let max = self.limits.max_response_bytes as u64;
        let base = c.url.clone();
        let join = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let res = match (method, body) {
                (Method::Get, _) => agent
                    .get(&url)
                    .header("Authorization", &auth)
                    .config()
                    .timeout_global(Some(timeout))
                    .build()
                    .call(),
                (Method::Delete, _) => agent
                    .delete(&url)
                    .header("Authorization", &auth)
                    .config()
                    .timeout_global(Some(timeout))
                    .build()
                    .call(),
                (Method::Post, b) => agent
                    .post(&url)
                    .header("Authorization", &auth)
                    .header("Content-Type", "application/json")
                    .config()
                    .timeout_global(Some(timeout))
                    .build()
                    .send(b.unwrap_or_default().as_slice()),
                (Method::Put, b) => agent
                    .put(&url)
                    .header("Authorization", &auth)
                    .header("Content-Type", "application/json")
                    .config()
                    .timeout_global(Some(timeout))
                    .build()
                    .send(b.unwrap_or_default().as_slice()),
            };
            let mut resp = res.map_err(|e| map_err(e, &base))?;
            let status = resp.status().as_u16();
            let headers = KEPT_HEADERS
                .iter()
                .filter_map(|h| {
                    resp.headers()
                        .get(*h)
                        .and_then(|v| v.to_str().ok())
                        .map(|v| ((*h).to_owned(), v.to_owned()))
                })
                .collect();
            let body = resp
                .body_mut()
                .with_config()
                .limit(max)
                .read_to_vec()
                .map_err(|e| map_err(e, &base))?;
            Ok(Response {
                status,
                headers,
                body,
            })
        });
        match join.await {
            Ok(r) => r,
            Err(e) => Err(StudioError::internal(format!("upstream call failed: {e}"))),
        }
    }
}
