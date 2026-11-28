//! StrataStudio end to end: a real in-process StrataDB server and the studio
//! app on ephemeral ports. Covers the connection registry (masking,
//! persistence, file permissions), the proxy (query, pagination, cancel,
//! metrics JSON, errors and refusals), admin actions, studio auth and CSRF,
//! and the embedded UI with its security headers.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use stratadb_common::workload::WorkClass;
use stratadb_common::TenantId;
use stratadb_server::admission::{Admission, Ask, WorkloadSettings};
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::query_log::{QueryLog, QueryLogConfig};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Durability, Engine, EngineOptions};
use stratadb_studio::config::{Secret, StudioConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// ---------------------------------------------------------------------------
// HTTP helpers (raw HTTP/1.1, one request per connection)

#[derive(Debug)]
struct Resp {
    status: u16,
    /// Lower-cased header block.
    head: String,
    body: String,
}

impl Resp {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|e| panic!("not JSON ({e}): {} {}", self.status, self.body))
    }

    fn header(&self, name: &str) -> Option<String> {
        self.head.lines().find_map(|l| {
            l.split_once(':')
                .filter(|(k, _)| k.trim() == name)
                .map(|(_, v)| v.trim().to_owned())
        })
    }
}

async fn call(
    addr: SocketAddr,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Resp {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut msg = format!("{method} {path} HTTP/1.1\r\nConnection: close\r\n");
    if !headers.iter().any(|(k, _)| k.eq_ignore_ascii_case("host")) {
        msg.push_str(&format!("Host: localhost:{}\r\n", addr.port()));
    }
    for (k, v) in headers {
        msg.push_str(&format!("{k}: {v}\r\n"));
    }
    if !body.is_empty() || method != "GET" {
        msg.push_str(&format!(
            "Content-Type: application/json\r\nContent-Length: {}\r\n",
            body.len()
        ));
    }
    msg.push_str("\r\n");
    msg.push_str(body);
    c.write_all(msg.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    c.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text[9..12].parse().unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let body = if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        dechunk(body)
    } else {
        body.to_owned()
    };
    Resp {
        status,
        head: head.to_ascii_lowercase(),
        body,
    }
}

fn dechunk(mut s: &str) -> String {
    let mut out = String::new();
    while let Some((len, rest)) = s.split_once("\r\n") {
        let n = usize::from_str_radix(len.trim(), 16).unwrap_or(0);
        if n == 0 {
            break;
        }
        out.push_str(&rest[..n]);
        s = &rest[n + 2..];
    }
    out
}

const CSRF: (&str, &str) = ("X-Strata-Studio", "1");

async fn get(addr: SocketAddr, path: &str) -> Resp {
    call(addr, "GET", path, &[], "").await
}

async fn send(addr: SocketAddr, method: &str, path: &str, body: Value) -> Resp {
    let b = if body.is_null() {
        String::new()
    } else {
        body.to_string()
    };
    call(addr, method, path, &[CSRF], &b).await
}

// ---------------------------------------------------------------------------
// A StrataDB server

struct Db {
    addr: SocketAddr,
    admission: Arc<Admission>,
    /// Every permission the studio uses.
    all: String,
    /// `query` only.
    query_only: String,
}

async fn start_db(dir: &Path, cfg: ServerConfig) -> Db {
    let mut store = TokenStore::load(dir).unwrap();
    let mut mk = |p: &[Permission]| {
        store
            .create(
                TenantId::new("acme").unwrap(),
                p.iter().copied().collect::<BTreeSet<_>>(),
                None,
                None,
            )
            .unwrap()
            .1
    };
    let all = mk(&[
        Permission::Ingest,
        Permission::Query,
        Permission::ClusterRead,
        Permission::ClusterAdmin,
        Permission::RetentionManage,
        Permission::BackupCreate,
    ]);
    let query_only = mk(&[Permission::Query]);
    let data = dir.join("data");
    let mut o = EngineOptions::new(&data);
    o.wal.durability = Durability::Async;
    o.background = false;
    let log = QueryLog::open(
        &data,
        &o,
        &QueryLogConfig {
            flush_interval: Duration::from_millis(20),
            ..QueryLogConfig::default()
        },
    )
    .unwrap();
    let engine = Arc::new(Engine::open(o).unwrap());
    let state =
        AppState::new(engine, TokenStore::load(dir).unwrap(), cfg).with_query_log(Arc::new(log));
    let admission = state.admission();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Db {
        addr,
        admission,
        all,
        query_only,
    }
}

async fn ingest(db: &Db, n: usize) {
    let lines: Vec<String> = (0..n)
        .map(|i| {
            json!({"service": "api", "severity": if i % 2 == 0 { "INFO" } else { "ERROR" },
                   "body": format!("event {i}"), "attributes": {"i": i}})
            .to_string()
        })
        .collect();
    let body = lines.join("\n");
    let mut c = tokio::net::TcpStream::connect(db.addr).await.unwrap();
    let msg = format!(
        "POST /api/v1/ingest HTTP/1.1\r\nHost: x\r\nConnection: close\r\nAuthorization: Bearer {}\r\nContent-Length: {}\r\n\r\n{body}",
        db.all,
        body.len()
    );
    c.write_all(msg.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    c.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
}

// ---------------------------------------------------------------------------
// The studio

async fn start_studio(data_dir: &Path, password: Option<&str>) -> SocketAddr {
    let mut cfg = StudioConfig::local(data_dir.to_owned());
    cfg.password = password.map(Secret::new);
    cfg.limits.request_timeout = Duration::from_secs(5);
    cfg.limits.upstream_queue_timeout = Duration::from_secs(2);
    let state = stratadb_studio::AppState::new(cfg).unwrap();
    let app = stratadb_studio::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn add_conn(studio: SocketAddr, name: &str, url: &str, token: &str) -> Resp {
    send(
        studio,
        "POST",
        "/api/connections",
        json!({"name": name, "url": url, "token": token}),
    )
    .await
}

fn q(sql: &str, extra: Value) -> Value {
    let mut v = json!({"query": sql});
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v
}

/// What ui/js/querylog.js builds for "recent, failed, min 0 ms".
const QUERY_LOG_RECENT: &str = "SELECT event_time, attributes['query_id'] AS query_id, attributes['fingerprint'] AS fingerprint, attributes['status'] AS status,
       attributes['elapsed_us'] AS elapsed_us, attributes['queue_us'] AS queue_us, attributes['exec_us'] AS exec_us, attributes['cpu_us'] AS cpu_us,
       attributes['rows_scanned'] AS rows_scanned, attributes['result_rows'] AS result_rows, attributes['table'] AS source_table, body
FROM system.query_log
WHERE event_time >= TIMESTAMP '2000-01-01T00:00:00.000Z'
  AND attributes['status'] != 'ok'
  AND attributes['elapsed_us'] >= 0
ORDER BY event_time DESC
LIMIT 100";

/// What ui/js/querylog.js builds for "top fingerprints by CPU".
const QUERY_LOG_TOP: &str = "SELECT attributes['fingerprint'] AS fingerprint, count(*) AS n,
       sum(attributes['cpu_us']) AS cpu_us, avg(attributes['elapsed_us']) AS avg_elapsed_us,
       max(attributes['elapsed_us']) AS max_elapsed_us, sum(attributes['rows_scanned']) AS rows_scanned
FROM system.query_log
WHERE event_time >= TIMESTAMP '2000-01-01T00:00:00.000Z'
GROUP BY attributes['fingerprint']
ORDER BY cpu_us DESC
LIMIT 20";

// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn embedded_ui_is_served_with_security_headers() {
    let dir = tempfile::tempdir().unwrap();
    let s = start_studio(dir.path(), None).await;

    let r = get(s, "/").await;
    assert_eq!(r.status, 200);
    assert!(r.header("content-type").unwrap().starts_with("text/html"));
    let csp = r.header("content-security-policy").unwrap();
    for part in [
        "default-src 'none'",
        "script-src 'self'",
        "frame-ancestors 'none'",
        "base-uri 'none'",
    ] {
        assert!(csp.contains(part), "{csp}");
    }
    assert!(
        !csp.contains("unsafe-inline") && !csp.contains("unsafe-eval"),
        "{csp}"
    );
    assert_eq!(
        r.header("x-content-type-options").as_deref(),
        Some("nosniff")
    );
    assert_eq!(r.header("x-frame-options").as_deref(), Some("deny"));
    assert_eq!(r.header("referrer-policy").as_deref(), Some("no-referrer"));
    // No inline script: the only <script> loads a module from 'self'.
    assert_eq!(r.body.matches("<script").count(), 1, "{}", r.body);
    assert!(r
        .body
        .contains(r#"<script type="module" src="/assets/js/main.js"></script>"#));

    // Every embedded asset is served with its type; ES module imports all
    // resolve to embedded files; nothing loads from another origin.
    let names: Vec<&str> = stratadb_studio::web::ASSETS
        .iter()
        .map(|(p, _, _)| *p)
        .collect();
    for (path, ct, body) in stratadb_studio::web::ASSETS {
        let r = get(s, path).await;
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(r.header("content-type").as_deref(), Some(*ct), "{path}");
        assert!(r.header("content-security-policy").is_some());
        for line in body.lines().filter(|l| l.starts_with("import ")) {
            let target = line
                .rsplit_once("'./")
                .map(|(_, t)| t.trim_end_matches("';"));
            let target = target.unwrap_or_else(|| panic!("{path}: unexpected import {line}"));
            assert!(
                names.contains(&format!("/assets/js/{target}").as_str()),
                "{path} imports a missing module: {line}"
            );
        }
        // No resource from another origin (CDN): no absolute URL in a
        // src/href/url()/import/fetch position.
        let compact = body.replace(' ', "");
        for needle in [
            "src=\"http",
            "href=\"http",
            "url(http",
            "url('http",
            "url(\"http",
            "from'http",
            "from\"http",
            "import('http",
            "fetch('http",
            "fetch(\"http",
            "fetch(`http",
        ] {
            assert!(
                !compact.contains(needle),
                "{path} loads from another origin ({needle})"
            );
        }
        assert!(!body.contains("eval("), "{path} uses eval");
        assert!(!body.contains("innerHTML"), "{path} uses innerHTML");
    }
    assert!(!stratadb_studio::web::INDEX_HTML.contains("://"));
    assert_eq!(get(s, "/assets/js/nope.js").await.status, 404);

    // API responses are JSON, never cached, with the same headers.
    let r = get(s, "/api/session").await;
    assert_eq!(r.status, 200);
    assert_eq!(r.header("cache-control").as_deref(), Some("no-store"));
    assert!(r.header("content-security-policy").is_some());
    assert_eq!(r.json()["auth_required"], false);
    let r = get(s, "/api/nope").await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (404, json!("studio"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn host_check_and_csrf_protect_state_changes() {
    let dir = tempfile::tempdir().unwrap();
    let s = start_studio(dir.path(), None).await;
    // DNS rebinding: a loopback studio answers only to loopback names.
    let r = call(
        s,
        "GET",
        "/api/connections",
        &[("Host", "evil.example")],
        "",
    )
    .await;
    assert_eq!(r.status, 403, "{}", r.body);
    let r = call(s, "GET", "/", &[("Host", "127.0.0.1")], "").await;
    assert_eq!(r.status, 200);

    let body = json!({"name": "x", "url": "http://127.0.0.1:1", "token": "t"}).to_string();
    // No custom header.
    let r = call(s, "POST", "/api/connections", &[], &body).await;
    assert_eq!(r.status, 403, "{}", r.body);
    assert!(r.body.contains("CSRF"), "{}", r.body);
    // Cross-origin Origin, or a cross-site fetch.
    let host = format!("localhost:{}", s.port());
    for extra in [
        ("Origin", "http://evil.example".to_owned()),
        ("Origin", "null".to_owned()),
        ("Sec-Fetch-Site", "cross-site".to_owned()),
    ] {
        let r = call(
            s,
            "POST",
            "/api/connections",
            &[CSRF, (extra.0, extra.1.as_str())],
            &body,
        )
        .await;
        assert_eq!(r.status, 403, "{extra:?}: {}", r.body);
    }
    // Same origin with the header is accepted.
    let origin = format!("http://{host}");
    let r = call(
        s,
        "POST",
        "/api/connections",
        &[CSRF, ("Origin", &origin), ("Sec-Fetch-Site", "same-origin")],
        &body,
    )
    .await;
    assert_eq!(r.status, 201, "{}", r.body);
    // Bodies are bounded (1 MiB by default). The body is only slightly
    // larger, so it is fully sent before the server refuses it.
    let big = format!("{{\"name\":\"{}\"}}", "x".repeat((1 << 20) + 16));
    let r = tokio::time::timeout(
        Duration::from_secs(10),
        call(s, "POST", "/api/connections", &[CSRF], &big),
    )
    .await
    .expect("no response to an oversized body");
    assert_eq!(r.status, 413, "{}", r.body);
}

#[tokio::test(flavor = "multi_thread")]
async fn studio_login_sessions_and_throttling() {
    let dir = tempfile::tempdir().unwrap();
    let s = start_studio(dir.path(), Some("correct horse battery staple")).await;
    let r = get(s, "/api/session").await;
    assert_eq!(
        (
            r.json()["auth_required"].clone(),
            r.json()["authenticated"].clone()
        ),
        (json!(true), json!(false))
    );
    let r = get(s, "/api/connections").await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (401, json!("studio"))
    );
    // The UI itself loads without a session (it shows the login form).
    assert_eq!(get(s, "/").await.status, 200);

    let r = send(s, "POST", "/api/auth/login", json!({"password": "nope"})).await;
    assert_eq!(r.status, 401);
    assert!(r.header("set-cookie").is_none());
    // Login is CSRF-checked too.
    let r = call(
        s,
        "POST",
        "/api/auth/login",
        &[],
        &json!({"password": "correct horse battery staple"}).to_string(),
    )
    .await;
    assert_eq!(r.status, 403);

    let r = send(
        s,
        "POST",
        "/api/auth/login",
        json!({"password": "correct horse battery staple"}),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let cookie = r.header("set-cookie").unwrap();
    for part in ["httponly", "samesite=strict", "path=/"] {
        assert!(cookie.contains(part), "{cookie}");
    }
    assert!(
        !cookie.contains("secure"),
        "no TLS: no Secure flag ({cookie})"
    );
    let pair = cookie.split(';').next().unwrap().to_owned();
    // The header block is lower-cased; the id is hex, so it is unchanged.
    let r = call(s, "GET", "/api/connections", &[("Cookie", &pair)], "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = call(s, "GET", "/api/session", &[("Cookie", &pair)], "").await;
    assert_eq!(r.json()["authenticated"], true);
    // A forged cookie is not a session.
    let forged = format!("strata_studio_session={}", "a".repeat(64));
    let r = call(s, "GET", "/api/connections", &[("Cookie", &forged)], "").await;
    assert_eq!(r.status, 401);

    let r = call(
        s,
        "POST",
        "/api/auth/logout",
        &[CSRF, ("Cookie", &pair)],
        "",
    )
    .await;
    assert_eq!(r.status, 200);
    assert!(r.header("set-cookie").unwrap().contains("max-age=0"));
    let r = call(s, "GET", "/api/connections", &[("Cookie", &pair)], "").await;
    assert_eq!(r.status, 401, "logged-out session must be dead");

    // Failed logins are throttled.
    let mut statuses = Vec::new();
    for _ in 0..25 {
        statuses.push(
            send(s, "POST", "/api/auth/login", json!({"password": "x"}))
                .await
                .status,
        );
    }
    assert!(statuses.contains(&429), "{statuses:?}");

    // A non-loopback listener refuses to start without a password.
    let mut cfg = StudioConfig::local(dir.path().join("other"));
    cfg.listen = "0.0.0.0:7171".parse().unwrap();
    let e = stratadb_studio::AppState::new(cfg).unwrap_err();
    assert!(e.message.contains("requires studio authentication"), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_registry_masks_tokens_and_persists_privately() {
    let dir = tempfile::tempdir().unwrap();
    let db = start_db(&dir.path().join("db"), ServerConfig::default()).await;
    let data = dir.path().join("studio");
    let s = start_studio(&data, None).await;
    let url = format!("http://{}", db.addr);

    let r = add_conn(s, "local", &url, &db.all).await;
    assert_eq!(r.status, 201, "{}", r.body);
    assert!(!r.body.contains(&db.all), "token echoed: {}", r.body);
    assert_eq!(r.json()["token_set"], true);
    assert_eq!(add_conn(s, "local", &url, &db.all).await.status, 409);
    for (name, u, t) in [
        ("bad name", url.as_str(), "t"),
        ("ok", "ftp://x", "t"),
        ("ok", "http://user:pw@h:1", "t"),
        ("ok", url.as_str(), "has space"),
    ] {
        let r = add_conn(s, name, u, t).await;
        assert_eq!(r.status, 400, "{name} {u}: {}", r.body);
    }
    let r = send(
        s,
        "POST",
        "/api/connections",
        json!({"name": "x", "url": url, "token": "t", "extra": 1}),
    )
    .await;
    assert_eq!(r.status, 400, "unknown fields are refused");

    let list = get(s, "/api/connections").await;
    assert_eq!(list.status, 200);
    assert!(!list.body.contains(&db.all), "{}", list.body);
    assert_eq!(list.json()["connections"][0]["url"], url);
    let one = get(s, "/api/connections/local").await;
    assert!(!one.body.contains(&db.all) && one.json()["name"] == "local");

    // The file is 0600 in a 0700 directory and holds the token.
    let file = data.join("connections.toml");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let m = std::fs::metadata(&file).unwrap().permissions().mode();
        assert_eq!(m & 0o777, 0o600);
        let d = std::fs::metadata(&data).unwrap().permissions().mode();
        assert_eq!(d & 0o777, 0o700);
    }
    assert!(std::fs::read_to_string(&file).unwrap().contains(&db.all));

    // Edit without a token keeps the saved one (the proxy still works).
    let r = send(
        s,
        "PUT",
        "/api/connections/local",
        json!({"url": format!("{url}/"), "tls_verify": false}),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.json()["tls_verify"], false);
    let r = send(
        s,
        "POST",
        "/api/servers/local/query",
        q("SELECT count(*) FROM logs", json!({})),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);

    // A new studio process on the same data dir sees the same registry.
    let s2 = start_studio(&data, None).await;
    let r = get(s2, "/api/connections").await;
    assert_eq!(r.json()["connections"][0]["name"], "local");
    let r = send(
        s2,
        "POST",
        "/api/servers/local/query",
        q("SELECT count(*) FROM logs", json!({})),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);

    // Remove.
    assert_eq!(
        send(s, "DELETE", "/api/connections/local", Value::Null)
            .await
            .status,
        200
    );
    assert_eq!(
        send(s, "DELETE", "/api/connections/local", Value::Null)
            .await
            .status,
        404
    );
    assert!(!std::fs::read_to_string(&file).unwrap().contains(&db.all));
    let r = send(
        s,
        "POST",
        "/api/servers/local/query",
        q("SELECT 1 FROM logs", json!({})),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (404, json!("studio"))
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn connection_test_probes_reachability_token_and_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let db = start_db(dir.path(), ServerConfig::default()).await;
    let s = start_studio(&dir.path().join("studio"), None).await;
    let url = format!("http://{}", db.addr);
    add_conn(s, "full", &url, &db.all).await;
    add_conn(s, "limited", &url, &db.query_only).await;
    add_conn(s, "badtoken", &url, "sdb_0000_nope").await;
    add_conn(s, "down", "http://127.0.0.1:1", "t").await;

    let r = send(s, "POST", "/api/connections/full/test", Value::Null)
        .await
        .json();
    assert_eq!(r["ok"], true, "{r}");
    assert_eq!(
        r["permissions"],
        json!({"query": true, "cluster.read": true, "retention.manage": true})
    );
    assert_eq!(r["version"]["api"], "v1");
    let r = send(s, "POST", "/api/connections/limited/test", Value::Null)
        .await
        .json();
    assert_eq!(r["ok"], true, "{r}");
    assert_eq!(
        r["permissions"],
        json!({"query": true, "cluster.read": false, "retention.manage": false})
    );
    let r = send(s, "POST", "/api/connections/badtoken/test", Value::Null)
        .await
        .json();
    assert_eq!(
        (
            r["ok"].clone(),
            r["reachable"].clone(),
            r["token_valid"].clone()
        ),
        (json!(false), json!(true), json!(false))
    );
    let r = send(s, "POST", "/api/connections/down/test", Value::Null)
        .await
        .json();
    assert_eq!(
        (r["ok"].clone(), r["reachable"].clone()),
        (json!(false), json!(false))
    );
    assert!(r["error"].as_str().unwrap().contains("127.0.0.1:1"), "{r}");
    // Unsaved settings can be tested before saving; nothing is stored.
    let r = send(
        s,
        "POST",
        "/api/connection-test",
        json!({"name": "draft", "url": url, "token": db.all}),
    )
    .await;
    assert_eq!(r.json()["ok"], true, "{}", r.body);
    assert_eq!(get(s, "/api/connections/draft").await.status, 404);
    // Status for the tree and dashboard header.
    let st = get(s, "/api/servers/full/status").await.json();
    assert_eq!(
        (st["reachable"].clone(), st["ready"].clone()),
        (json!(true), json!(true))
    );
    let st = get(s, "/api/servers/down/status").await.json();
    assert_eq!(st["reachable"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn proxied_queries_paginate_explain_and_report_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = start_db(dir.path(), ServerConfig::default()).await;
    ingest(&db, 5).await;
    let s = start_studio(&dir.path().join("studio"), None).await;
    let url = format!("http://{}", db.addr);
    add_conn(s, "local", &url, &db.all).await;
    let path = "/api/servers/local/query";
    let sql = "SELECT event_time, body FROM logs ORDER BY event_time";

    // Pages of 2: 2 + 2 + 1, the last without a cursor.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for page in 0..3 {
        let mut body = q(
            sql,
            json!({"page_size": 2, "query_id": format!("pg{page}")}),
        );
        if let Some(c) = &cursor {
            body["cursor"] = json!(c);
        }
        let r = send(s, "POST", path, body).await;
        assert_eq!(r.status, 200, "{}", r.body);
        let v = r.json();
        assert_eq!(v["query_id"], format!("pg{page}"));
        for k in [
            "queue_us",
            "parse_us",
            "plan_us",
            "exec_us",
            "rows_scanned",
            "class",
        ] {
            assert!(!v["stats"][k].is_null(), "missing stats.{k}: {v}");
        }
        for row in v["rows"].as_array().unwrap() {
            seen.push(row["body"].as_str().unwrap().to_owned());
        }
        cursor = v["next_cursor"].as_str().map(str::to_owned);
        if page < 2 {
            assert!(cursor.is_some(), "page {page}: {v}");
        }
    }
    assert!(cursor.is_none());
    assert_eq!(
        seen,
        (0..5).map(|i| format!("event {i}")).collect::<Vec<_>>()
    );

    // A generated query id when the browser sends none.
    let v = send(
        s,
        "POST",
        path,
        q("SELECT count(*) AS n FROM logs", json!({})),
    )
    .await
    .json();
    assert_eq!(v["rows"][0]["n"], 5);
    assert!(v["query_id"].as_str().unwrap().starts_with("ss"), "{v}");
    // workload_class is forwarded (lowering works).
    let v = send(
        s,
        "POST",
        path,
        q(
            "SELECT count(*) FROM logs",
            json!({"workload_class": "batch"}),
        ),
    )
    .await
    .json();
    assert_eq!(v["stats"]["class"], "batch", "{v}");

    // EXPLAIN and EXPLAIN ANALYZE.
    let v = send(s, "POST", path, q("EXPLAIN SELECT * FROM logs", json!({})))
        .await
        .json();
    assert!(v["explain"].as_str().unwrap().contains("Scan: logs"), "{v}");
    let v = send(
        s,
        "POST",
        path,
        q("EXPLAIN ANALYZE SELECT count(*) FROM logs", json!({})),
    )
    .await
    .json();
    assert!(v["explain"].as_str().unwrap().contains("Actual:"), "{v}");

    // StrataDB errors come back structured, with the upstream status.
    let r = send(
        s,
        "POST",
        path,
        q("SELEC nope", json!({"query_id": "bad1"})),
    )
    .await;
    assert_eq!(r.status, 400);
    let e = r.json();
    assert_eq!(
        (
            e["error"].clone(),
            e["source"].clone(),
            e["upstream_status"].clone(),
            e["query_id"].clone()
        ),
        (
            json!("invalid_input"),
            json!("upstream"),
            json!(400),
            json!("bad1")
        )
    );
    assert!(
        e["message"].as_str().unwrap().contains("syntax error"),
        "{e}"
    );
    let r = send(
        s,
        "POST",
        path,
        q("SELECT count(*) FROM logs", json!({"page_size": 10})),
    )
    .await;
    // The server ignores page_size for aggregates (STATUS.md request 4).
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["paginated"], false);
    // Studio-side validation.
    for bad in [
        q("SELECT 1 FROM logs", json!({"query_id": "../x"})),
        q("SELECT 1 FROM logs", json!({"workload_class": "urgent"})),
        q("", json!({})),
        json!({"query": "SELECT 1 FROM logs", "unknown": true}),
    ] {
        let r = send(s, "POST", path, bad.clone()).await;
        assert_eq!(
            (r.status, r.json()["source"].clone()),
            (400, json!("studio")),
            "{bad}"
        );
    }
    // A wrong token: StrataDB's 401, marked as upstream (not a studio logout).
    add_conn(s, "badtoken", &url, "sdb_0000_nope").await;
    let r = send(
        s,
        "POST",
        "/api/servers/badtoken/query",
        q("SELECT 1 FROM logs", json!({})),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (401, json!("upstream"))
    );
    // An unreachable server: 502 from the studio.
    add_conn(s, "down", "http://127.0.0.1:1", "t").await;
    let r = send(
        s,
        "POST",
        "/api/servers/down/query",
        q("SELECT 1 FROM logs", json!({})),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (502, json!("studio")),
        "{}",
        r.body
    );

    // The Query Log browser's two queries (as ui/js/querylog.js builds
    // them) run against a real system.query_log.
    let mut found = false;
    for _ in 0..200 {
        let r = send(s, "POST", path, q(QUERY_LOG_RECENT, json!({}))).await;
        assert_eq!(r.status, 200, "{}", r.body);
        let v = r.json();
        if let Some(row) = v["rows"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["query_id"] == "bad1")
        {
            assert_eq!(row["status"], "invalid_input", "{row}");
            assert!(row["elapsed_us"].is_number(), "{row}");
            found = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(found, "the failed query never appeared in system.query_log");
    // The text filter's LIKE escaping (likeEscape) is valid StrataQL.
    let r = send(
        s,
        "POST",
        path,
        q(
            "SELECT body FROM system.query_log WHERE body ILIKE '%a\\_b\\%c\\\\d%' LIMIT 1",
            json!({}),
        ),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = send(s, "POST", path, q(QUERY_LOG_TOP, json!({}))).await;
    assert_eq!(r.status, 200, "{}", r.body);
    let v = r.json();
    assert!(
        v["rows"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["n"].as_u64().unwrap_or(0) >= 1),
        "{v}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn queued_queries_show_status_cancel_and_refusals_carry_retry_hints() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ServerConfig {
        max_concurrent_queries: 1,
        workload: WorkloadSettings {
            queue_max: 1,
            queue_timeout: Duration::from_secs(20),
            ..WorkloadSettings::default()
        },
        ..ServerConfig::default()
    };
    let db = start_db(dir.path(), cfg).await;
    ingest(&db, 1).await;
    let s = start_studio(&dir.path().join("studio"), None).await;
    add_conn(s, "local", &format!("http://{}", db.addr), &db.all).await;
    let held = db
        .admission
        .admit(Ask {
            class: WorkClass::Normal,
            tenant: 99,
            est_cpu_ms: 1.0,
            max_wait: Duration::from_secs(1),
            cancel: None,
            key: "held".into(),
        })
        .await
        .unwrap();

    let waiting = tokio::spawn(async move {
        send(
            s,
            "POST",
            "/api/servers/local/query",
            q("SELECT count(*) FROM logs", json!({"query_id": "w1"})),
        )
        .await
    });
    let mut status = Value::Null;
    for _ in 0..300 {
        let r = get(s, "/api/servers/local/query/w1").await;
        if r.status == 200 {
            status = r.json();
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(status["state"], "queued", "{status}");
    assert_eq!(status["queue_position"], 0);

    // The queue is full: 429 TOO_MANY_QUERIES with the retry hint, relayed.
    let r = send(
        s,
        "POST",
        "/api/servers/local/query",
        q("SELECT count(*) FROM logs", json!({})),
    )
    .await;
    assert_eq!(r.status, 429, "{}", r.body);
    let e = r.json();
    assert_eq!(e["code"], "TOO_MANY_QUERIES");
    assert_eq!(e["source"], "upstream");
    assert!(e["retry_after_ms"].as_u64().unwrap() >= 100, "{e}");
    assert!(r.header("retry-after").is_some(), "{}", r.head);

    // Cancel through the studio.
    let r = send(s, "DELETE", "/api/servers/local/query/w1", Value::Null).await;
    assert_eq!(r.status, 202, "{}", r.body);
    assert_eq!(r.json()["state"], "cancelling");
    let r = waiting.await.unwrap();
    assert_eq!(r.json()["error"], "cancelled", "{} {}", r.status, r.body);
    // Gone afterwards.
    assert_eq!(get(s, "/api/servers/local/query/w1").await.status, 404);
    assert_eq!(
        send(s, "DELETE", "/api/servers/local/query/w1", Value::Null)
            .await
            .status,
        404
    );

    // A browser that goes away cancels its query: drop the connection
    // while the query is queued, and the server forgets it.
    let mut c = tokio::net::TcpStream::connect(s).await.unwrap();
    let body = q("SELECT count(*) FROM logs", json!({"query_id": "gone1"})).to_string();
    let msg = format!(
        "POST /api/servers/local/query HTTP/1.1\r\nHost: localhost:{}\r\nX-Strata-Studio: 1\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        s.port(),
        body.len()
    );
    c.write_all(msg.as_bytes()).await.unwrap();
    let mut queued = false;
    for _ in 0..300 {
        if get(s, "/api/servers/local/query/gone1").await.status == 200 {
            queued = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(queued);
    drop(c);
    let mut forgotten = false;
    for _ in 0..500 {
        if get(s, "/api/servers/local/query/gone1").await.status == 404 {
            forgotten = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(forgotten, "abandoned query still registered on the server");
    drop(held);
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_are_parsed_into_dashboard_json() {
    let dir = tempfile::tempdir().unwrap();
    let db = start_db(dir.path(), ServerConfig::default()).await;
    ingest(&db, 3).await;
    let s = start_studio(&dir.path().join("studio"), None).await;
    let url = format!("http://{}", db.addr);
    add_conn(s, "local", &url, &db.all).await;
    add_conn(s, "limited", &url, &db.query_only).await;
    send(
        s,
        "POST",
        "/api/servers/local/query",
        q("SELECT count(*) FROM logs", json!({})),
    )
    .await;

    let r = get(s, "/api/servers/local/metrics").await;
    assert_eq!(r.status, 200, "{}", r.body);
    let m = r.json();
    assert_eq!(m["gauges"]["events_written_total"], 3.0, "{m}");
    assert_eq!(m["gauges"]["engine_failed"], 0.0);
    assert!(
        m["queries_by_outcome"]["ok"].as_f64().unwrap() >= 1.0,
        "{m}"
    );
    assert!(
        m["latency"]["lifetime"]["query"]["count"].as_f64().unwrap() >= 1.0,
        "{m}"
    );
    assert!(
        m["latency"]["lifetime"]["ingest"]["p99_ms"].is_number(),
        "{m}"
    );
    assert_eq!(m["interval_s"], Value::Null, "first scrape has no interval");
    // Memory accounting is not enabled on this in-process server.
    assert!(m["memory_regions"].is_array(), "{m}");
    assert!(
        m["workload"]["slots"]["interactive"].as_f64().is_some(),
        "{m}"
    );

    let f = get(s, "/api/servers/local/metrics/families").await.json();
    let fams = f["families"].as_array().unwrap();
    let q = fams
        .iter()
        .find(|x| x["name"] == "stratadb_queries_total")
        .unwrap();
    assert_eq!(q["type"], "counter");
    let h = fams
        .iter()
        .find(|x| x["name"] == "stratadb_query_seconds")
        .unwrap();
    assert_eq!(h["type"], "histogram");
    assert!(h["samples"]
        .as_array()
        .unwrap()
        .iter()
        .any(|x| x["labels"]["le"] == "+Inf"));

    // Without cluster.read: a clear 403.
    let r = get(s, "/api/servers/limited/metrics").await;
    assert_eq!(r.status, 403);
    assert!(r.json()["message"]
        .as_str()
        .unwrap()
        .contains("cluster.read"));
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_views_and_actions_are_proxied() {
    let dir = tempfile::tempdir().unwrap();
    let backups = dir.path().join("backups");
    std::fs::create_dir_all(&backups).unwrap();
    let cfg = ServerConfig {
        backup_root: Some(backups.clone()),
        ..ServerConfig::default()
    };
    let db = start_db(&dir.path().join("db"), cfg).await;
    let db2 = start_db(&dir.path().join("db2"), ServerConfig::default()).await;
    ingest(&db, 2).await;
    let s = start_studio(&dir.path().join("studio"), None).await;
    add_conn(s, "local", &format!("http://{}", db.addr), &db.all).await;
    add_conn(s, "nobackup", &format!("http://{}", db2.addr), &db2.all).await;
    add_conn(s, "limited", &format!("http://{}", db.addr), &db.query_only).await;

    for which in ["workload", "threads", "snapshots"] {
        let r = get(s, &format!("/api/servers/local/admin/{which}")).await;
        assert_eq!(r.status, 200, "{which}: {}", r.body);
        assert!(r.json().is_object());
    }
    // Memory accounting is off on this in-process server: StrataDB's
    // `unsupported` comes back structured.
    let r = get(s, "/api/servers/local/admin/memory").await;
    assert_eq!(
        (
            r.status,
            r.json()["error"].clone(),
            r.json()["source"].clone()
        ),
        (400, json!("unsupported"), json!("upstream"))
    );
    assert_eq!(get(s, "/api/servers/local/admin/secrets").await.status, 404);
    let w = get(s, "/api/servers/local/admin/workload").await.json();
    assert_eq!(w["classes"].as_array().unwrap().len(), 3);

    // Compaction.
    let r = send(
        s,
        "POST",
        "/api/servers/local/admin/compaction",
        Value::Null,
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(r.json()["segments_now"].is_number());
    let r = send(
        s,
        "POST",
        "/api/servers/limited/admin/compaction",
        Value::Null,
    )
    .await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (403, json!("upstream"))
    );

    // Backup: created, then a duplicate name is a conflict; a server without
    // --backup-dir says so.
    let r = send(
        s,
        "POST",
        "/api/servers/local/admin/backup",
        json!({"name": "b1"}),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.json()["name"], "b1");
    assert!(backups.join("b1").exists());
    let r = send(
        s,
        "POST",
        "/api/servers/local/admin/backup",
        json!({"name": "b1"}),
    )
    .await;
    assert_eq!(r.status, 409, "{}", r.body);
    let r = send(
        s,
        "POST",
        "/api/servers/nobackup/admin/backup",
        json!({"name": "b1"}),
    )
    .await;
    assert_eq!(r.status, 400, "{}", r.body);
    assert!(r.json()["message"].as_str().unwrap().contains("backup-dir"));
    let r = send(
        s,
        "POST",
        "/api/servers/local/admin/backup",
        json!({"name": "../etc"}),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (400, json!("studio"))
    );

    // Retention: get, set, clear.
    let r = get(s, "/api/servers/local/retention").await.json();
    assert_eq!(
        (r["tenant"].clone(), r["policy"].clone()),
        (json!("acme"), Value::Null)
    );
    let r = send(
        s,
        "PUT",
        "/api/servers/local/retention",
        json!({"max_age_days": 7, "hold": true}),
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.body);
    let r = get(s, "/api/servers/local/retention").await.json();
    assert_eq!(
        r["policy"],
        json!({"max_age_ms": 7 * 86_400_000_i64, "hold": true})
    );
    let r = send(
        s,
        "PUT",
        "/api/servers/local/retention",
        json!({"max_age_days": 0}),
    )
    .await;
    assert_eq!(
        (r.status, r.json()["source"].clone()),
        (400, json!("studio"))
    );
    let r = send(s, "DELETE", "/api/servers/local/retention", Value::Null).await;
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(
        get(s, "/api/servers/local/retention").await.json()["policy"],
        Value::Null
    );
    assert_eq!(get(s, "/api/servers/limited/retention").await.status, 403);

    // Health, ready, version.
    assert_eq!(
        get(s, "/api/servers/local/health").await.json()["status"],
        "ok"
    );
    assert_eq!(
        get(s, "/api/servers/local/ready").await.json()["ready"],
        true
    );
    assert_eq!(
        get(s, "/api/servers/local/version").await.json()["api"],
        "v1"
    );
}
