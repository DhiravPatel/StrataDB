//! Studio authentication, sessions and request checks (ADR-0041).
//!
//! - **Login:** when a password is configured, `POST /api/auth/login`
//!   compares it in constant time (SHA-256 digests compared with `subtle`)
//!   and sets a random 256-bit session cookie: `HttpOnly`, `SameSite=Strict`,
//!   `Path=/`, plus `Secure` when the studio serves TLS. The server keeps
//!   only the SHA-256 of each session id; sessions expire after
//!   `session_ttl` idle and are bounded in number (oldest evicted).
//! - **Throttling:** at most [`MAX_FAILED_LOGINS`] failed logins per
//!   [`FAILED_LOGIN_WINDOW`], globally; beyond that, `429`.
//! - **CSRF:** every state-changing request (`POST`/`PUT`/`DELETE`) must
//!   carry `X-Strata-Studio: 1` (a custom header cannot be sent cross-origin
//!   without a CORS preflight, which the studio never grants), and its
//!   `Origin` (when present) must name the request's own host;
//!   `Sec-Fetch-Site`, when present, must be `same-origin` or `none`.
//! - **Host check:** on a loopback listener the `Host` header must be a
//!   loopback name (or an `allowed_hosts` entry), which defeats DNS
//!   rebinding against an unauthenticated local studio.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, Method};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::config::Secret;
use crate::error::{Result, StudioError};

pub const COOKIE_NAME: &str = "strata_studio_session";
pub const CSRF_HEADER: &str = "x-strata-studio";
pub const MAX_FAILED_LOGINS: usize = 20;
pub const FAILED_LOGIN_WINDOW: Duration = Duration::from_secs(60);

fn digest(s: &[u8]) -> [u8; 32] {
    Sha256::digest(s).into()
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Constant-time password check.
pub fn password_matches(configured: &Secret, offered: &str) -> bool {
    let a = digest(configured.expose().as_bytes());
    let b = digest(offered.as_bytes());
    a.ct_eq(&b).into()
}

#[derive(Debug, Clone, Copy)]
struct Session {
    last_seen: Instant,
}

/// Live sessions, keyed by the SHA-256 of the session id.
#[derive(Debug)]
pub struct Sessions {
    ttl: Duration,
    max: usize,
    map: Mutex<HashMap<[u8; 32], Session>>,
    failures: Mutex<Vec<Instant>>,
}

/// A successful login: the cookie value (only ever sent in `Set-Cookie`).
#[derive(Clone)]
pub struct NewSession {
    pub id: String,
}

impl std::fmt::Debug for NewSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NewSession(***)")
    }
}

impl Sessions {
    pub fn new(ttl: Duration, max: usize) -> Self {
        Self {
            ttl,
            max: max.max(1),
            map: Mutex::new(HashMap::new()),
            failures: Mutex::new(Vec::new()),
        }
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Record a failed login; `Err` once the window's budget is spent.
    pub fn check_throttle(&self, now: Instant) -> Result<()> {
        let mut f = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        f.retain(|t| now.saturating_duration_since(*t) < FAILED_LOGIN_WINDOW);
        if f.len() >= MAX_FAILED_LOGINS {
            return Err(StudioError::exhausted(
                "too many failed logins; wait a minute and retry",
            ));
        }
        Ok(())
    }

    pub fn record_failure(&self, now: Instant) {
        let mut f = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        f.retain(|t| now.saturating_duration_since(*t) < FAILED_LOGIN_WINDOW);
        if f.len() < MAX_FAILED_LOGINS {
            f.push(now);
        }
    }

    /// Create a session.
    pub fn create(&self, now: Instant) -> Result<NewSession> {
        let mut raw = [0u8; 32];
        getrandom::fill(&mut raw)
            .map_err(|e| StudioError::internal(format!("random session id: {e}")))?;
        let id = hex(&raw);
        let mut g = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let ttl = self.ttl;
        g.retain(|_, s| now.saturating_duration_since(s.last_seen) < ttl);
        if g.len() >= self.max {
            if let Some(oldest) = g.iter().min_by_key(|(_, s)| s.last_seen).map(|(k, _)| *k) {
                g.remove(&oldest);
            }
        }
        g.insert(digest(id.as_bytes()), Session { last_seen: now });
        Ok(NewSession { id })
    }

    /// Validate a session id and refresh its idle timer. Returns a short,
    /// non-secret label for audit logs (a prefix of the id's digest).
    pub fn touch(&self, id: &str, now: Instant) -> Option<String> {
        if id.len() != 64 || !id.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        let key = digest(id.as_bytes());
        let mut g = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let s = g.get_mut(&key)?;
        if now.saturating_duration_since(s.last_seen) >= self.ttl {
            g.remove(&key);
            return None;
        }
        s.last_seen = now;
        Some(format!("s:{}", &hex(&key)[..12]))
    }

    pub fn remove(&self, id: &str) {
        self.map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&digest(id.as_bytes()));
    }

    pub fn len(&self) -> usize {
        self.map.lock().unwrap_or_else(|e| e.into_inner()).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The session cookie's value, if the request has one.
pub fn session_cookie(headers: &HeaderMap) -> Option<String> {
    for v in headers.get_all(axum::http::header::COOKIE) {
        let Ok(s) = v.to_str() else { continue };
        for part in s.split(';') {
            if let Some((k, val)) = part.trim().split_once('=') {
                if k == COOKIE_NAME {
                    return Some(val.to_owned());
                }
            }
        }
    }
    None
}

/// `Set-Cookie` for a new session.
pub fn set_cookie(id: &str, ttl: Duration, secure: bool) -> String {
    format!(
        "{COOKIE_NAME}={id}; Path=/; HttpOnly; SameSite=Strict; Max-Age={}{}",
        ttl.as_secs(),
        if secure { "; Secure" } else { "" }
    )
}

/// `Set-Cookie` that deletes the session cookie.
pub fn clear_cookie(secure: bool) -> String {
    format!(
        "{COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0{}",
        if secure { "; Secure" } else { "" }
    )
}

/// Host part of a `Host` header or of an origin's authority, lower-cased,
/// without the port (`[::1]:7171` → `[::1]`).
fn host_only(authority: &str) -> String {
    let a = authority.trim().to_ascii_lowercase();
    if let Some(rest) = a.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return format!("[{}]", &rest[..end]);
        }
    }
    match a.rsplit_once(':') {
        Some((h, p)) if p.bytes().all(|b| b.is_ascii_digit()) => h.to_owned(),
        _ => a,
    }
}

fn is_loopback_name(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]") || host.ends_with(".localhost")
}

/// Check the `Host` header (DNS-rebinding defence). `loopback`: the
/// listener is a loopback address. `allowed`: extra accepted host names.
pub fn check_host(headers: &HeaderMap, loopback: bool, allowed: &[String]) -> Result<()> {
    let Some(host) = headers
        .get(axum::http::header::HOST)
        .and_then(|v| v.to_str().ok())
    else {
        return Err(StudioError::invalid("missing Host header"));
    };
    let h = host_only(host);
    if allowed.iter().any(|a| a.eq_ignore_ascii_case(&h)) {
        return Ok(());
    }
    if loopback && !is_loopback_name(&h) {
        return Err(StudioError::forbidden(format!(
            "host {h:?} is not allowed (a loopback studio answers only to localhost; add it to allowed_hosts)"
        )));
    }
    Ok(())
}

/// Whether `method` changes state (CSRF-checked).
pub fn is_state_changing(method: &Method) -> bool {
    !matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
}

/// CSRF check for a state-changing request.
pub fn check_csrf(headers: &HeaderMap) -> Result<()> {
    let marker = headers.get(CSRF_HEADER).and_then(|v| v.to_str().ok());
    if marker != Some("1") {
        return Err(StudioError::forbidden(format!(
            "missing {CSRF_HEADER}: 1 header (CSRF protection)"
        )));
    }
    if let Some(site) = headers.get("sec-fetch-site").and_then(|v| v.to_str().ok()) {
        if !matches!(site, "same-origin" | "none") {
            return Err(StudioError::forbidden(
                "cross-site request refused (CSRF protection)",
            ));
        }
    }
    if let Some(origin) = headers.get(axum::http::header::ORIGIN) {
        let origin = origin
            .to_str()
            .map_err(|_| StudioError::forbidden("invalid Origin header"))?;
        let host = headers
            .get(axum::http::header::HOST)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let origin_authority = origin
            .strip_prefix("https://")
            .or_else(|| origin.strip_prefix("http://"))
            .unwrap_or("");
        if origin == "null"
            || origin_authority.is_empty()
            || !origin_authority.eq_ignore_ascii_case(host.trim())
        {
            return Err(StudioError::forbidden(
                "Origin does not match the studio's host (CSRF protection)",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use axum::http::HeaderValue;

    fn h(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.append(*k, HeaderValue::from_str(v).unwrap());
        }
        m
    }

    #[test]
    fn passwords_compare_by_digest() {
        let s = Secret::new("correct horse battery");
        assert!(password_matches(&s, "correct horse battery"));
        assert!(!password_matches(&s, "correct horse batter"));
        assert!(!password_matches(&s, ""));
    }

    #[test]
    fn sessions_expire_and_are_bounded() {
        let s = Sessions::new(Duration::from_secs(10), 2);
        let t0 = Instant::now();
        let a = s.create(t0).unwrap();
        assert_eq!(a.id.len(), 64);
        assert!(s.touch(&a.id, t0 + Duration::from_secs(5)).is_some());
        // Idle timer refreshed by the touch above.
        assert!(s.touch(&a.id, t0 + Duration::from_secs(14)).is_some());
        assert!(s.touch(&a.id, t0 + Duration::from_secs(30)).is_none());
        let b = s.create(t0 + Duration::from_secs(31)).unwrap();
        let c = s.create(t0 + Duration::from_secs(32)).unwrap();
        let d = s.create(t0 + Duration::from_secs(33)).unwrap();
        assert_eq!(s.len(), 2);
        assert!(
            s.touch(&b.id, t0 + Duration::from_secs(34)).is_none(),
            "oldest evicted"
        );
        assert!(s.touch(&c.id, t0 + Duration::from_secs(34)).is_some());
        s.remove(&d.id);
        assert!(s.touch(&d.id, t0 + Duration::from_secs(34)).is_none());
        assert!(s.touch("not-hex", t0).is_none());
    }

    #[test]
    fn failed_logins_are_throttled() {
        let s = Sessions::new(Duration::from_secs(10), 2);
        let t0 = Instant::now();
        for _ in 0..MAX_FAILED_LOGINS {
            s.check_throttle(t0).unwrap();
            s.record_failure(t0);
        }
        assert!(s.check_throttle(t0).is_err());
        assert!(s.check_throttle(t0 + FAILED_LOGIN_WINDOW).is_ok());
    }

    #[test]
    fn cookies_are_parsed_and_flagged() {
        let m = h(&[("cookie", "a=b; strata_studio_session=abc; c=d")]);
        assert_eq!(session_cookie(&m).as_deref(), Some("abc"));
        assert_eq!(session_cookie(&h(&[("cookie", "x=y")])), None);
        let c = set_cookie("abc", Duration::from_secs(60), true);
        for part in [
            "HttpOnly",
            "SameSite=Strict",
            "Path=/",
            "Max-Age=60",
            "Secure",
        ] {
            assert!(c.contains(part), "{c}");
        }
        assert!(!set_cookie("abc", Duration::from_secs(60), false).contains("Secure"));
    }

    #[test]
    fn csrf_requires_marker_and_same_origin() {
        assert!(check_csrf(&h(&[("host", "localhost:7171")])).is_err());
        let ok = h(&[
            ("host", "localhost:7171"),
            (CSRF_HEADER, "1"),
            ("origin", "http://localhost:7171"),
            ("sec-fetch-site", "same-origin"),
        ]);
        assert!(check_csrf(&ok).is_ok());
        // No Origin (non-browser client) is accepted with the marker.
        assert!(check_csrf(&h(&[("host", "localhost:7171"), (CSRF_HEADER, "1")])).is_ok());
        for (k, v) in [
            ("origin", "http://evil.example"),
            ("origin", "null"),
            ("origin", "http://localhost:7172"),
            ("sec-fetch-site", "cross-site"),
        ] {
            let m = h(&[("host", "localhost:7171"), (CSRF_HEADER, "1"), (k, v)]);
            assert!(check_csrf(&m).is_err(), "{k}: {v}");
        }
    }

    #[test]
    fn host_check_defeats_rebinding_on_loopback() {
        for ok in [
            "localhost:7171",
            "127.0.0.1:7171",
            "[::1]:7171",
            "LOCALHOST",
        ] {
            assert!(check_host(&h(&[("host", ok)]), true, &[]).is_ok(), "{ok}");
        }
        assert!(check_host(&h(&[("host", "evil.example:7171")]), true, &[]).is_err());
        assert!(check_host(
            &h(&[("host", "studio.lan:7171")]),
            true,
            &["studio.lan".to_owned()]
        )
        .is_ok());
        assert!(check_host(&h(&[("host", "studio.lan")]), false, &[]).is_ok());
        assert!(check_host(&HeaderMap::new(), true, &[]).is_err());
    }
}
