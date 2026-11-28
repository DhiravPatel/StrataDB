//! The embedded single-page UI and the security headers on every response.
//!
//! The UI is plain HTML, CSS and JavaScript ES modules compiled into the
//! binary with `include_str!`; there is no build step and no external
//! resource. The Content-Security-Policy allows scripts, styles and
//! connections from the studio's own origin only: no inline script, no
//! `eval`, no framing.

use axum::extract::Request;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

pub const CSP: &str = "default-src 'none'; script-src 'self'; style-src 'self'; \
img-src 'self' data:; connect-src 'self'; font-src 'self'; manifest-src 'self'; \
base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

/// (published path, content type, contents)
pub const ASSETS: &[(&str, &str, &str)] = &[
    (
        "/assets/app.css",
        "text/css; charset=utf-8",
        include_str!("../ui/app.css"),
    ),
    (
        "/assets/favicon.svg",
        "image/svg+xml",
        include_str!("../ui/favicon.svg"),
    ),
    (
        "/assets/js/main.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/main.js"),
    ),
    (
        "/assets/js/api.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/api.js"),
    ),
    (
        "/assets/js/dom.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/dom.js"),
    ),
    (
        "/assets/js/modal.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/modal.js"),
    ),
    (
        "/assets/js/schema.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/schema.js"),
    ),
    (
        "/assets/js/tree.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/tree.js"),
    ),
    (
        "/assets/js/connections.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/connections.js"),
    ),
    (
        "/assets/js/editor.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/editor.js"),
    ),
    (
        "/assets/js/query.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/query.js"),
    ),
    (
        "/assets/js/export.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/export.js"),
    ),
    (
        "/assets/js/dashboard.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/dashboard.js"),
    ),
    (
        "/assets/js/admin.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/admin.js"),
    ),
    (
        "/assets/js/querylog.js",
        "text/javascript; charset=utf-8",
        include_str!("../ui/js/querylog.js"),
    ),
];

pub const INDEX_HTML: &str = include_str!("../ui/index.html");

/// `GET /` and `GET /index.html`.
pub async fn index() -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
        ],
        INDEX_HTML,
    )
        .into_response()
}

/// `GET /assets/...`.
pub async fn asset(uri: axum::http::Uri) -> Response {
    match ASSETS.iter().find(|(p, _, _)| *p == uri.path()) {
        Some((_, ct, body)) => (
            [
                (header::CONTENT_TYPE, *ct),
                (header::CACHE_CONTROL, "no-cache"),
            ],
            *body,
        )
            .into_response(),
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

/// Adds the security headers to every response. `hsts`: the studio serves TLS.
pub async fn security_headers(hsts: bool, req: Request, next: Next) -> Response {
    let api = req.uri().path().starts_with("/api/");
    let mut resp = next.run(req).await;
    let h = resp.headers_mut();
    let set = |h: &mut axum::http::HeaderMap, k: &'static str, v: &'static str| {
        h.insert(k, HeaderValue::from_static(v));
    };
    set(h, "content-security-policy", CSP);
    set(h, "x-content-type-options", "nosniff");
    set(h, "x-frame-options", "DENY");
    set(h, "referrer-policy", "no-referrer");
    set(h, "cross-origin-opener-policy", "same-origin");
    set(h, "cross-origin-resource-policy", "same-origin");
    set(
        h,
        "permissions-policy",
        "camera=(), microphone=(), geolocation=(), payment=(), usb=()",
    );
    if hsts {
        set(h, "strict-transport-security", "max-age=31536000");
    }
    if api {
        set(h, "cache-control", "no-store");
    }
    resp
}
