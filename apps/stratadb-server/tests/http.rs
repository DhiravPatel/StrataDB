//! HTTP API security and behaviour tests against a real in-process server.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{json, Value};
use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Durability, Engine, EngineOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Srv {
    addr: std::net::SocketAddr,
    a_rw: String,
    a_ingest_only: String,
    b_rw: String,
    metrics: String,
    admin: String,
    a_retention: String,
    _dir: tempfile::TempDir,
}

async fn start(cfg: ServerConfig) -> Srv {
    let dir = tempfile::tempdir().unwrap();
    let mut store = TokenStore::load(dir.path()).unwrap();
    let mut mk = |t: &str, p: &[Permission]| {
        store
            .create(
                TenantId::new(t).unwrap(),
                p.iter().copied().collect::<BTreeSet<_>>(),
                None,
                None,
            )
            .unwrap()
            .1
    };
    let a_rw = mk("tenant-a", &[Permission::Ingest, Permission::Query]);
    let a_ingest_only = mk("tenant-a", &[Permission::Ingest]);
    let b_rw = mk("tenant-b", &[Permission::Ingest, Permission::Query]);
    let metrics = mk("ops", &[Permission::ClusterRead]);
    let admin = mk("ops", &[Permission::ClusterAdmin, Permission::BackupCreate]);
    let a_retention = mk("tenant-a", &[Permission::RetentionManage]);
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Sync;
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let app = router(
        AppState::new(engine, TokenStore::load(dir.path()).unwrap(), cfg)
            .with_audit(stratadb_server::audit::AuditLog::open(dir.path()).unwrap()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Srv {
        addr,
        a_rw,
        a_ingest_only,
        b_rw,
        metrics,
        admin,
        a_retention,
        _dir: dir,
    }
}

/// Send raw body bytes with extra headers; returns (status, headers+body text).
async fn req_raw(s: &Srv, path: &str, token: &str, headers: &str, body: &[u8]) -> (u16, String) {
    let mut c = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let head = format!(
        "POST {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nAuthorization: Bearer {token}\r\n{headers}Content-Length: {}\r\n\r\n",
        body.len()
    );
    c.write_all(head.as_bytes()).await.unwrap();
    c.write_all(body).await.unwrap();
    let mut buf = Vec::new();
    c.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    (text[9..12].parse().unwrap(), text)
}

fn gzip(b: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(b).unwrap();
    e.finish().unwrap()
}

/// Minimal HTTP/1.1 client: returns (status, body).
async fn req(
    s: &Srv,
    method: &str,
    path: &str,
    token: Option<&str>,
    ctype: &str,
    body: &str,
) -> (u16, String) {
    req_h(s, method, path, token, ctype, body, "").await
}

/// Like [`req`] with extra raw header lines (each ending in `\r\n`).
async fn req_h(
    s: &Srv,
    method: &str,
    path: &str,
    token: Option<&str>,
    ctype: &str,
    body: &str,
    extra: &str,
) -> (u16, String) {
    let mut c = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let auth = token
        .map(|t| format!("Authorization: Bearer {t}\r\n"))
        .unwrap_or_default();
    let msg = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n{auth}{extra}Content-Type: {ctype}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    c.write_all(msg.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    c.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text[9..12].parse().unwrap();
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

fn j(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|e| panic!("not JSON ({e}): {s}"))
}

async fn query(s: &Srv, token: &str, sql: &str) -> (u16, Value) {
    let (st, b) = req(
        s,
        "POST",
        "/api/v1/query",
        Some(token),
        "application/json",
        &json!({"query": sql}).to_string(),
    )
    .await;
    (st, j(&b))
}

#[tokio::test(flavor = "multi_thread")]
async fn health_ready_version_need_no_auth() {
    let s = start(ServerConfig::default()).await;
    assert_eq!(
        req(&s, "GET", "/api/v1/health", None, "text/plain", "")
            .await
            .0,
        200
    );
    assert_eq!(
        req(&s, "GET", "/api/v1/ready", None, "text/plain", "")
            .await
            .0,
        200
    );
    let (st, b) = req(&s, "GET", "/api/v1/version", None, "text/plain", "").await;
    assert_eq!(st, 200);
    assert_eq!(j(&b)["api"], "v1");
}

#[tokio::test(flavor = "multi_thread")]
async fn authentication_and_authorization_are_enforced() {
    let s = start(ServerConfig::default()).await;
    let ev = r#"{"body":"x"}"#;
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest/batch",
            None,
            "application/x-ndjson",
            ev
        )
        .await
        .0,
        401
    );
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest/batch",
            Some("sdb_0000_bad"),
            "application/x-ndjson",
            ev
        )
        .await
        .0,
        401
    );
    // Flip the final hex digit (always differs from the original; a fixed
    // replacement digit made this test fail 1 time in 16).
    let last = if s.a_rw.ends_with('0') { '1' } else { '0' };
    let forged = format!("{}{last}", &s.a_rw[..s.a_rw.len() - 1]);
    assert_ne!(forged, s.a_rw);
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest/batch",
            Some(&forged),
            "application/x-ndjson",
            ev
        )
        .await
        .0,
        401
    );
    // Ingest-only token cannot query; query token required for metrics fails.
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest/batch",
            Some(&s.a_ingest_only),
            "application/x-ndjson",
            ev
        )
        .await
        .0,
        200
    );
    assert_eq!(
        query(&s, &s.a_ingest_only, "SELECT count(*) FROM logs")
            .await
            .0,
        403
    );
    assert_eq!(
        req(
            &s,
            "GET",
            "/api/v1/metrics",
            Some(&s.a_rw),
            "text/plain",
            ""
        )
        .await
        .0,
        403
    );
    let (st, body) = req(
        &s,
        "GET",
        "/api/v1/metrics",
        Some(&s.metrics),
        "text/plain",
        "",
    )
    .await;
    assert_eq!(st, 200);
    assert!(body.contains("stratadb_events_written_total 1"), "{body}");
}

#[tokio::test(flavor = "multi_thread")]
async fn ingest_partial_batch_and_receipt() {
    let s = start(ServerConfig::default()).await;
    let body = "{\"service\":\"api\",\"severity\":\"ERROR\",\"body\":\"one\"}\n{not json\n{\"ingest_time\":5}\n{\"body\":\"two\",\"tenant_id\":\"tenant-b\"}\n";
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/ingest/batch",
        Some(&s.a_rw),
        "application/x-ndjson",
        body,
    )
    .await;
    assert_eq!(st, 200, "{b}");
    let r = j(&b);
    assert_eq!(r["accepted"], 2);
    assert_eq!(r["rejected"], 2);
    assert_eq!(r["errors"][0]["index"], 1);
    assert_eq!(r["errors"][1]["index"], 2);
    assert_eq!(r["durability"], "wal_synced");
    assert_eq!(r["visible_to_queries"], true);
    assert!(r["commit_seq"].as_u64().unwrap() >= 1);
    // JSON array form.
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/ingest",
        Some(&s.a_rw),
        "application/json",
        r#"[{"body":"three"}]"#,
    )
    .await;
    assert_eq!((st, j(&b)["accepted"].clone()), (200, json!(1)));
    // Nothing valid → 422 with per-record errors.
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/ingest",
        Some(&s.a_rw),
        "application/json",
        r#"[{"ingest_time":1}]"#,
    )
    .await;
    assert_eq!(st, 422, "{b}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_isolation_across_the_api() {
    let s = start(ServerConfig::default()).await;
    // tenant-a writes an event that *claims* tenant-b in its payload.
    req(
        &s,
        "POST",
        "/api/v1/ingest",
        Some(&s.a_rw),
        "application/json",
        r#"{"body":"secret-a","tenant_id":"tenant-b"}"#,
    )
    .await;
    req(
        &s,
        "POST",
        "/api/v1/ingest",
        Some(&s.b_rw),
        "application/json",
        r#"{"body":"data-b"}"#,
    )
    .await;
    let (_, a) = query(&s, &s.a_rw, "SELECT body, tenant_id FROM logs").await;
    assert_eq!(
        a["rows"],
        json!([{"body": "secret-a", "tenant_id": "tenant-a"}])
    );
    let (_, b) = query(&s, &s.b_rw, "SELECT body FROM logs").await;
    assert_eq!(b["rows"], json!([{"body": "data-b"}]));
    // Attempts to widen scope from tenant-b.
    let (_, b) = query(
        &s,
        &s.b_rw,
        "SELECT count(*) AS n FROM logs WHERE tenant_id = 'tenant-a'",
    )
    .await;
    assert_eq!(b["rows"][0]["n"], 0);
    let (st, _) = req(
        &s,
        "POST",
        "/api/v1/query",
        Some(&s.b_rw),
        "application/json",
        r#"{"query":"SELECT * FROM logs","tenant":"tenant-a"}"#,
    )
    .await;
    assert_eq!(st, 400, "client-supplied tenant fields are rejected");
    let (st, _) = req(
        &s,
        "POST",
        "/api/v1/query",
        Some(&s.b_rw),
        "application/json",
        r#"{"query":"SELECT * FROM logs","database":"tenant-a"}"#,
    )
    .await;
    assert_eq!(st, 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn query_errors_limits_and_body_cap() {
    let s = start(ServerConfig {
        max_body_bytes: 1024,
        ..ServerConfig::default()
    })
    .await;
    let (st, b) = query(&s, &s.a_rw, "SELECT * FROM elsewhere").await;
    assert_eq!(st, 400);
    assert_eq!(b["error"], "invalid_input");
    assert_eq!(b["retryable"], false);
    let big = format!("{{\"body\":\"{}\"}}", "x".repeat(2000));
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest",
            Some(&s.a_rw),
            "application/json",
            &big
        )
        .await
        .0,
        413
    );
    let (st, b) = query(&s, &s.a_rw, "EXPLAIN SELECT count(*) FROM logs").await;
    assert_eq!(st, 200);
    assert!(b["explain"].as_str().unwrap().contains("Scan: logs"));
}

#[tokio::test(flavor = "multi_thread")]
async fn idempotent_ingest_over_http() {
    let s = start(ServerConfig::default()).await;
    let body = "{\"body\":\"once\"}\n{\"body\":\"twice\"}\n";
    let k = "Idempotency-Key: order-42\r\n";
    let path = "/api/v1/ingest/batch";
    let (st, b1) = req_h(
        &s,
        "POST",
        path,
        Some(&s.a_rw),
        "application/x-ndjson",
        body,
        k,
    )
    .await;
    assert_eq!(st, 200, "{b1}");
    let r1 = j(&b1);
    assert_eq!(r1["duplicate"], false);
    let (st, b2) = req_h(
        &s,
        "POST",
        path,
        Some(&s.a_rw),
        "application/x-ndjson",
        body,
        k,
    )
    .await;
    assert_eq!(st, 200);
    let r2 = j(&b2);
    assert_eq!(r2["duplicate"], true);
    assert_eq!(r2["commit_seq"], r1["commit_seq"]);
    assert_eq!(r2["accepted"], 2);
    // Same key, different body → 409 (not retryable).
    let (st, b3) = req_h(
        &s,
        "POST",
        path,
        Some(&s.a_rw),
        "application/x-ndjson",
        "{\"body\":\"other\"}",
        k,
    )
    .await;
    assert_eq!(st, 409, "{b3}");
    assert_eq!(j(&b3)["retryable"], false);
    // The same key is independent per tenant.
    let (st, b4) = req_h(
        &s,
        "POST",
        path,
        Some(&s.b_rw),
        "application/x-ndjson",
        body,
        k,
    )
    .await;
    assert_eq!((st, j(&b4)["duplicate"].clone()), (200, json!(false)));
    let (_, q) = query(&s, &s.a_rw, "SELECT count(*) AS n FROM logs").await;
    assert_eq!(q["rows"][0]["n"], 2, "retry must not duplicate rows");
    // Invalid keys are rejected before anything is written.
    let (st, _) = req_h(
        &s,
        "POST",
        path,
        Some(&s.a_rw),
        "application/x-ndjson",
        body,
        "Idempotency-Key: has space\r\n",
    )
    .await;
    assert_eq!(st, 400);
}

#[tokio::test(flavor = "multi_thread")]
async fn admin_endpoints_require_privileges_and_are_safe() {
    let root = tempfile::tempdir().unwrap();
    let cfg = ServerConfig {
        backup_root: Some(root.path().to_path_buf()),
        ..ServerConfig::default()
    };
    let s = start(cfg).await;
    for _ in 0..4 {
        req(
            &s,
            "POST",
            "/api/v1/ingest",
            Some(&s.a_rw),
            "application/json",
            r#"{"body":"x"}"#,
        )
        .await;
        // Each ingest goes to the memtable; compaction needs flushed segments,
        // which backups create (backup flushes first).
        req(
            &s,
            "POST",
            "/api/v1/admin/backup",
            Some(&s.admin),
            "application/json",
            &format!("{{\"name\":\"warm{}\"}}", rand_suffix()),
        )
        .await;
    }
    // Non-admin tokens are refused.
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/admin/compaction",
            Some(&s.a_rw),
            "application/json",
            ""
        )
        .await
        .0,
        403
    );
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/admin/backup",
            Some(&s.a_rw),
            "application/json",
            r#"{"name":"x"}"#
        )
        .await
        .0,
        403
    );
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/admin/compaction",
        Some(&s.admin),
        "application/json",
        "",
    )
    .await;
    assert_eq!(st, 200, "{b}");
    assert_eq!(j(&b)["segments_now"], 1);
    // Backup names cannot escape the backup root.
    for bad in ["../evil", "a/b", ".hidden", ""] {
        let (st, _) = req(
            &s,
            "POST",
            "/api/v1/admin/backup",
            Some(&s.admin),
            "application/json",
            &json!({"name": bad}).to_string(),
        )
        .await;
        assert_eq!(st, 400, "{bad:?} accepted");
    }
    assert!(!root.path().parent().unwrap().join("evil").exists());
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/admin/backup",
        Some(&s.admin),
        "application/json",
        r#"{"name":"nightly-1"}"#,
    )
    .await;
    assert_eq!(st, 200, "{b}");
    stratadb_storage::verify_backup(&root.path().join("nightly-1"), None).unwrap();
    // Same name again → conflict, original untouched.
    let (st, _) = req(
        &s,
        "POST",
        "/api/v1/admin/backup",
        Some(&s.admin),
        "application/json",
        r#"{"name":"nightly-1"}"#,
    )
    .await;
    assert_eq!(st, 409);
}

fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    N.fetch_add(1, Ordering::Relaxed)
}

#[tokio::test(flavor = "multi_thread")]
async fn backups_disabled_without_root_and_retention_is_tenant_scoped() {
    let s = start(ServerConfig::default()).await;
    let (st, _) = req(
        &s,
        "POST",
        "/api/v1/admin/backup",
        Some(&s.admin),
        "application/json",
        r#"{"name":"x"}"#,
    )
    .await;
    assert_eq!(st, 400);
    // Query-only token cannot manage retention.
    assert_eq!(
        req(
            &s,
            "GET",
            "/api/v1/retention",
            Some(&s.a_rw),
            "text/plain",
            ""
        )
        .await
        .0,
        403
    );
    let (st, b) = req(
        &s,
        "PUT",
        "/api/v1/retention",
        Some(&s.a_retention),
        "application/json",
        r#"{"max_age_days": 7, "hold": true}"#,
    )
    .await;
    assert_eq!(st, 200, "{b}");
    let (_, b) = req(
        &s,
        "GET",
        "/api/v1/retention",
        Some(&s.a_retention),
        "text/plain",
        "",
    )
    .await;
    let v = j(&b);
    assert_eq!(v["tenant"], "tenant-a");
    assert_eq!(v["policy"]["max_age_ms"], 7 * 86_400_000i64);
    assert_eq!(v["policy"]["hold"], true);
    // A body cannot redirect the policy to another tenant.
    let (st, _) = req(
        &s,
        "PUT",
        "/api/v1/retention",
        Some(&s.a_retention),
        "application/json",
        r#"{"max_age_days": 1, "tenant": "tenant-b"}"#,
    )
    .await;
    assert_eq!(st, 400);
    assert_eq!(
        req(
            &s,
            "PUT",
            "/api/v1/retention",
            Some(&s.a_retention),
            "application/json",
            r#"{"max_age_days": 0}"#
        )
        .await
        .0,
        400
    );
    let (st, b) = req(
        &s,
        "DELETE",
        "/api/v1/retention",
        Some(&s.a_retention),
        "text/plain",
        "",
    )
    .await;
    assert_eq!((st, j(&b)["policy"].clone()), (200, Value::Null));
}

#[tokio::test(flavor = "multi_thread")]
async fn security_events_are_audited_without_secrets() {
    let s = start(ServerConfig::default()).await;
    req(
        &s,
        "POST",
        "/api/v1/query",
        Some("sdb_bogus_token"),
        "application/json",
        "{}",
    )
    .await;
    query(&s, &s.a_ingest_only, "SELECT 1 FROM logs").await; // permission denied
    req(
        &s,
        "POST",
        "/api/v1/admin/compaction",
        Some(&s.admin),
        "application/json",
        "",
    )
    .await;
    req(
        &s,
        "PUT",
        "/api/v1/retention",
        Some(&s.a_retention),
        "application/json",
        r#"{"max_age_days": 3}"#,
    )
    .await;
    let n = stratadb_server::audit::verify(s._dir.path()).unwrap();
    assert!(n >= 4, "{n}");
    let log =
        std::fs::read_to_string(stratadb_server::audit::AuditLog::path_for(s._dir.path())).unwrap();
    for ev in [
        "auth_failure",
        "permission_denied",
        "admin_compaction",
        "retention_set",
    ] {
        assert!(log.contains(ev), "missing {ev}");
    }
    // Real tokens: their 64-hex secret must never appear. The bogus
    // credential must not appear verbatim either.
    for tok in [&s.a_rw, &s.admin, &s.a_ingest_only, &s.a_retention] {
        let secret = tok.rsplit('_').next().unwrap();
        assert_eq!(secret.len(), 64);
        assert!(
            !log.contains(secret),
            "audit log leaked the secret of {tok}"
        );
    }
    assert!(
        !log.contains("sdb_bogus_token"),
        "audit log leaked a presented credential"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn otlp_http_json_logs() {
    let s = start(ServerConfig::default()).await;
    let req = json!({"resourceLogs": [{
    "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "checkout"}}]},
    "scopeLogs": [{"scope": {"name": "app"}, "logRecords": [
        {"severityNumber": 17, "severityText": "ERROR", "body": {"stringValue": "card declined"},
         "traceId": "5B8EFFF798038103D269B633813FC60C", "spanId": "EEE19B7EC3C1B174",
         "attributes": [{"key": "http.status_code", "value": {"intValue": "402"}}]},
        {"traceId": "not-hex"}
    ]}]}]});
    let body = req.to_string();
    let (st, raw) = req_raw(
        &s,
        "/v1/logs",
        &s.a_rw,
        "Content-Type: application/json\r\n",
        body.as_bytes(),
    )
    .await;
    assert_eq!(st, 200, "{raw}");
    assert!(raw.contains("x-stratadb-durability: wal_synced"), "{raw}");
    let resp = j(raw.split_once("\r\n\r\n").unwrap().1);
    assert_eq!(resp["partialSuccess"]["rejectedLogRecords"], "1");
    let (_, q) = query(&s, &s.a_rw,
        "SELECT service, severity, body, trace_id, attributes['http.status_code'] AS code, attributes['otel.scope.name'] AS scope FROM logs").await;
    assert_eq!(
        q["rows"],
        json!([{"service": "checkout", "severity": "ERROR", "body": "card declined",
        "trace_id": "5b8efff798038103d269b633813fc60c", "code": 402, "scope": "app"}])
    );
    // gzip, as the OpenTelemetry Collector sends by default.
    let (st, raw) = req_raw(
        &s,
        "/v1/logs",
        &s.a_rw,
        "Content-Type: application/json\r\nContent-Encoding: gzip\r\n",
        &gzip(body.as_bytes()),
    )
    .await;
    assert_eq!(st, 200, "{raw}");
    // OTLP/HTTP protobuf (binary produced by `protoc --encode`, tests/fixtures/otlp).
    let pb = std::fs::read(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../tests/fixtures/otlp/request.binpb"
    ))
    .unwrap();
    let (st, raw) = req_raw(
        &s,
        "/v1/logs",
        &s.a_rw,
        "Content-Type: application/x-protobuf\r\n",
        &pb,
    )
    .await;
    assert_eq!(st, 200, "{raw}");
    assert!(
        raw.contains("content-type: application/x-protobuf"),
        "{raw}"
    );
    assert!(raw.contains("x-stratadb-accepted: 2"), "{raw}");
    let (_, q) = query(
        &s,
        &s.a_rw,
        "SELECT count(*) AS n FROM logs WHERE service = 'checkout'",
    )
    .await;
    assert_eq!(
        q["rows"][0]["n"], 4,
        "1 JSON + 1 gzip JSON + 2 records in the protobuf fixture"
    );
    let (_, q) = query(&s, &s.a_rw, "SELECT trace_id, attributes['otel.event_name'] AS ev FROM logs WHERE attributes['otel.event_name'] = 'payment.failed'").await;
    assert_eq!(
        q["rows"],
        json!([{"trace_id": "5b8efff798038103d269b633813fc60c", "ev": "payment.failed"}])
    );
    // Malformed protobuf → 400 with a protobuf google.rpc.Status (code 3).
    let (st, raw) = req_raw(
        &s,
        "/v1/logs",
        &s.a_rw,
        "Content-Type: application/x-protobuf\r\n",
        b"\x0a\x50\x01",
    )
    .await;
    assert_eq!(st, 400, "{raw}");
    assert!(raw.contains("content-type: application/x-protobuf"));
    // Other media types → 415.
    let (st, _) = req_raw(
        &s,
        "/v1/logs",
        &s.a_rw,
        "Content-Type: text/plain\r\n",
        b"x",
    )
    .await;
    assert_eq!(st, 415);
    // Wrong tenant token cannot see it; unauthenticated OTLP is refused.
    let (_, q) = query(&s, &s.b_rw, "SELECT count(*) AS n FROM logs").await;
    assert_eq!(q["rows"][0]["n"], 0);
    let (st, _) = req_raw(
        &s,
        "/v1/logs",
        "sdb_x_y",
        "Content-Type: application/json\r\n",
        b"{}",
    )
    .await;
    assert_eq!(st, 401);
}

#[tokio::test(flavor = "multi_thread")]
async fn gzip_ingest_and_decompression_bomb() {
    let cfg = ServerConfig {
        max_decompressed_bytes: 1 << 20,
        ..ServerConfig::default()
    };
    let s = start(cfg).await;
    let nd = (0..50)
        .map(|i| format!("{{\"body\":\"m{i}\"}}\n"))
        .collect::<String>();
    let (st, raw) = req_raw(
        &s,
        "/api/v1/ingest/batch",
        &s.a_rw,
        "Content-Type: application/x-ndjson\r\nContent-Encoding: gzip\r\n",
        &gzip(nd.as_bytes()),
    )
    .await;
    assert_eq!(st, 200, "{raw}");
    assert!(raw.contains("\"accepted\":50"));
    // 64 MiB of zeros compresses to ~64 KiB: must be refused, not inflated.
    let bomb = gzip(&vec![b' '; 64 << 20]);
    assert!(bomb.len() < 1 << 20);
    let (st, raw) = req_raw(
        &s,
        "/api/v1/ingest/batch",
        &s.a_rw,
        "Content-Type: application/x-ndjson\r\nContent-Encoding: gzip\r\n",
        &bomb,
    )
    .await;
    assert_eq!(st, 400, "{raw}");
    assert!(raw.contains("\"retryable\":false"));
    let (st, _) = req_raw(
        &s,
        "/api/v1/ingest/batch",
        &s.a_rw,
        "Content-Type: application/x-ndjson\r\nContent-Encoding: br\r\n",
        b"x",
    )
    .await;
    assert_eq!(st, 400);
    let (st, _) = req_raw(
        &s,
        "/api/v1/ingest/batch",
        &s.a_rw,
        "Content-Type: application/x-ndjson\r\nContent-Encoding: gzip\r\n",
        b"not gzip",
    )
    .await;
    assert_eq!(st, 400);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_queries_can_be_inspected_and_cancelled_by_their_tenant_only() {
    let s = Arc::new(start(ServerConfig::default()).await);
    // Unknown / finished ids are 404 for everyone.
    assert_eq!(
        req(
            &s,
            "DELETE",
            "/api/v1/query/nope",
            Some(&s.a_rw),
            "text/plain",
            ""
        )
        .await
        .0,
        404
    );
    let mut rows = 0;
    for round in 0..4 {
        // Load more data each round until the query is slow enough to catch.
        let batch: String = (0..20_000)
            .map(|i| format!("{{\"body\":\"{} {i} {round}\"}}\n", "abcdefghij".repeat(30)))
            .collect();
        for chunk in batch.as_bytes().chunks(4 << 20) {
            req(
                &s,
                "POST",
                "/api/v1/ingest/batch",
                Some(&s.a_rw),
                "application/x-ndjson",
                std::str::from_utf8(chunk).unwrap_or(""),
            )
            .await;
        }
        rows += 20_000;
        let id = format!("slow{round}");
        let body = json!({"query": "SELECT count(*) AS n FROM logs WHERE body LIKE '%a%b%c%d%e%f%g%h%i%z%'", "query_id": id}).to_string();
        let s2 = Arc::clone(&s);
        let running = tokio::spawn(async move {
            req(
                &s2,
                "POST",
                "/api/v1/query",
                Some(&s2.a_rw),
                "application/json",
                &body,
            )
            .await
        });
        // Wait until the server reports it running (or it already finished).
        let path = format!("/api/v1/query/{id}");
        let mut seen = false;
        for _ in 0..200 {
            let (st, _) = req(&s, "GET", &path, Some(&s.a_rw), "text/plain", "").await;
            if st == 200 {
                seen = true;
                break;
            }
            if running.is_finished() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        if seen {
            // Another tenant can neither see nor cancel it.
            assert_eq!(
                req(&s, "GET", &path, Some(&s.b_rw), "text/plain", "")
                    .await
                    .0,
                404
            );
            assert_eq!(
                req(&s, "DELETE", &path, Some(&s.b_rw), "text/plain", "")
                    .await
                    .0,
                404
            );
            // A second query with the same id is refused while it runs.
            let dup = json!({"query": "SELECT 1 FROM logs", "query_id": id}).to_string();
            let (dst, _) = req(
                &s,
                "POST",
                "/api/v1/query",
                Some(&s.a_rw),
                "application/json",
                &dup,
            )
            .await;
            let (cst, _) = req(&s, "DELETE", &path, Some(&s.a_rw), "text/plain", "").await;
            let (qst, qb) = running.await.unwrap();
            if cst == 202 {
                assert_eq!(dst, 409, "duplicate running query_id must be refused");
                assert_eq!(qst, 400, "{qb}");
                assert_eq!(j(&qb)["error"], "cancelled");
                // Finished queries leave the registry.
                assert_eq!(
                    req(&s, "GET", &path, Some(&s.a_rw), "text/plain", "")
                        .await
                        .0,
                    404
                );
                return;
            }
        } else {
            let _ = running.await;
        }
    }
    panic!("query over {rows} rows always finished before it could be cancelled");
}

/// Every path+method in docs/api/openapi.json is routed (no 404/405), so the
/// published API description cannot silently drift from the server.
#[tokio::test(flavor = "multi_thread")]
async fn openapi_document_matches_router() {
    let s = start(ServerConfig::default()).await;
    let spec: Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../docs/api/openapi.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let mut checked = 0;
    for (path, item) in spec["paths"].as_object().unwrap() {
        for method in ["get", "post", "put", "delete"] {
            if item.get(method).is_none() {
                continue;
            }
            let concrete = path.replace("{id}", "some-id");
            let (st, b) = req(
                &s,
                &method.to_uppercase(),
                &concrete,
                Some(&s.admin),
                "application/json",
                "{}",
            )
            .await;
            assert!(
                st != 404 || b.contains("\"error\""),
                "{method} {path}: not routed ({st})"
            );
            assert_ne!(st, 405, "{method} {path}: method not allowed");
            checked += 1;
        }
    }
    assert!(checked >= 15, "{checked}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tokens_hot_reload_without_restart() {
    let s = start(ServerConfig {
        token_reload_interval: std::time::Duration::ZERO,
        ..ServerConfig::default()
    })
    .await;
    // A token created after the server started works immediately.
    let mut store = TokenStore::load(s._dir.path()).unwrap();
    let (rec, fresh) = store
        .create(
            TenantId::new("tenant-a").unwrap(),
            BTreeSet::from([Permission::Query]),
            None,
            None,
        )
        .unwrap();
    let (st, _) = query(&s, &fresh, "SELECT count(*) FROM logs").await;
    assert_eq!(st, 200);
    // Revocation takes effect without restart.
    store.revoke(&rec.id).unwrap();
    let (st, _) = query(&s, &fresh, "SELECT count(*) FROM logs").await;
    assert_eq!(st, 401);
    // A corrupt token file is not applied: existing tokens keep working.
    std::fs::write(TokenStore::path_for(s._dir.path()), b"{garbage").unwrap();
    let (st, _) = query(&s, &s.a_rw, "SELECT count(*) FROM logs").await;
    assert_eq!(st, 200, "a failed reload must keep the previous tokens");
}

#[tokio::test(flavor = "multi_thread")]
async fn per_tenant_rate_and_storage_quotas() {
    let mut quotas = stratadb_server::Quotas {
        default: stratadb_server::TenantQuota {
            ingest_events_per_sec: Some(5.0),
            ingest_burst_events: Some(10.0),
            storage_bytes: None,
        },
        ..Default::default()
    };
    quotas.tenants.insert(
        "tenant-b".into(),
        stratadb_server::TenantQuota {
            storage_bytes: Some(1),
            ..Default::default()
        },
    );
    let s = start(ServerConfig {
        quotas,
        ..ServerConfig::default()
    })
    .await;
    let five: String = (0..5).map(|i| format!("{{\"body\":\"{i}\"}}\n")).collect();
    // Burst of 10 events: two batches of 5 pass, the third is rate limited.
    for _ in 0..2 {
        assert_eq!(
            req(
                &s,
                "POST",
                "/api/v1/ingest",
                Some(&s.a_rw),
                "application/x-ndjson",
                &five
            )
            .await
            .0,
            200
        );
    }
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/ingest",
        Some(&s.a_rw),
        "application/x-ndjson",
        &five,
    )
    .await;
    assert_eq!(st, 429, "{b}");
    assert_eq!(j(&b)["retryable"], true);
    // A single batch larger than the burst can never succeed → 400, not 429.
    let big: String = (0..11).map(|i| format!("{{\"body\":\"{i}\"}}\n")).collect();
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest",
            Some(&s.a_rw),
            "application/x-ndjson",
            &big
        )
        .await
        .0,
        400
    );
    // Tokens refill at 5 events/s.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest",
            Some(&s.a_rw),
            "application/x-ndjson",
            &five
        )
        .await
        .0,
        200
    );
    // tenant-b: storage quota of 1 byte — the first write fills it, the next is refused.
    assert_eq!(
        req(
            &s,
            "POST",
            "/api/v1/ingest",
            Some(&s.b_rw),
            "application/json",
            r#"{"body":"x"}"#
        )
        .await
        .0,
        200
    );
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/ingest",
        Some(&s.b_rw),
        "application/json",
        r#"{"body":"y"}"#,
    )
    .await;
    assert_eq!(st, 429, "{b}");
    assert!(b.contains("storage quota"), "{b}");
    let (_, m) = req(
        &s,
        "GET",
        "/api/v1/metrics",
        Some(&s.metrics),
        "text/plain",
        "",
    )
    .await;
    assert!(m.contains("stratadb_quota_rejections_total 2"), "{m}");
}

#[tokio::test(flavor = "multi_thread")]
async fn request_ids_are_propagated_or_generated_safely() {
    let s = start(ServerConfig::default()).await;
    let (_, raw) = req_raw(
        &s,
        "/api/v1/ingest",
        &s.a_rw,
        "Content-Type: application/json\r\nX-Request-Id: client-42_x\r\n",
        br#"{"body":"x"}"#,
    )
    .await;
    assert!(
        raw.to_ascii_lowercase()
            .contains("x-request-id: client-42_x"),
        "{raw}"
    );
    // Unsafe ids (header injection / log forging attempts) are replaced.
    let (_, raw) = req_raw(
        &s,
        "/api/v1/ingest",
        &s.a_rw,
        "Content-Type: application/json\r\nX-Request-Id: bad id\"}\r\n",
        br#"{"body":"x"}"#,
    )
    .await;
    let line = raw
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("x-request-id:"))
        .unwrap();
    assert!(!line.contains("bad"), "{line}");
    assert!(line.len() > "x-request-id: r".len());
}

#[tokio::test(flavor = "multi_thread")]
async fn query_pagination_over_http() {
    let s = start(ServerConfig::default()).await;
    // 45 events in three requests; several share a timestamp.
    for chunk in 0..3 {
        let body: String = (0..15)
            .map(|i| {
                let n = chunk * 15 + i;
                format!(
                    "{}\n",
                    json!({"event_time": format!("2026-11-20T10:00:{:02}Z", n % 10),
                           "service": "pg", "body": format!("m{n:03}")})
                )
            })
            .collect();
        let (st, _) = req(
            &s,
            "POST",
            "/api/v1/ingest",
            Some(&s.a_rw),
            "application/x-ndjson",
            &body,
        )
        .await;
        assert_eq!(st, 200);
    }
    let sql = "SELECT body FROM logs ORDER BY event_time DESC";
    let page = |cursor: Option<String>, size: usize| {
        let mut b = json!({"query": sql, "page_size": size});
        if let Some(c) = cursor {
            b["cursor"] = json!(c);
        }
        b.to_string()
    };
    let mut seen = Vec::new();
    let mut cursor = None;
    let mut pages = 0;
    loop {
        let (st, b) = req(
            &s,
            "POST",
            "/api/v1/query",
            Some(&s.a_rw),
            "application/json",
            &page(cursor.clone(), 10),
        )
        .await;
        assert_eq!(st, 200, "{b}");
        let b = j(&b);
        pages += 1;
        for r in b["rows"].as_array().unwrap() {
            seen.push(r["body"].as_str().unwrap().to_owned());
        }
        if pages == 1 {
            // Written after the first page: must not appear in later pages.
            req(
                &s,
                "POST",
                "/api/v1/ingest",
                Some(&s.a_rw),
                "application/json",
                r#"{"event_time":"2026-11-20T10:00:05Z","body":"late"}"#,
            )
            .await;
        }
        match b["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_owned()),
            None => break,
        }
    }
    assert_eq!(pages, 5);
    assert_eq!(seen.len(), 45);
    let uniq: std::collections::BTreeSet<_> = seen.iter().collect();
    assert_eq!(uniq.len(), 45);
    assert!(!seen.iter().any(|b| b == "late"));
    // Unpaginated responses carry next_cursor: null.
    let (_, b) = query(&s, &s.a_rw, sql).await;
    assert!(b["next_cursor"].is_null());
    assert_eq!(b["rows"].as_array().unwrap().len(), 46);
    // A cursor is bound to its tenant and query.
    let (_, b) = req(
        &s,
        "POST",
        "/api/v1/query",
        Some(&s.a_rw),
        "application/json",
        &page(None, 5),
    )
    .await;
    let c = j(&b)["next_cursor"].as_str().unwrap().to_owned();
    let other = json!({"query": sql, "page_size": 5, "cursor": c}).to_string();
    let (st, b) = req(
        &s,
        "POST",
        "/api/v1/query",
        Some(&s.b_rw),
        "application/json",
        &other,
    )
    .await;
    assert_eq!(st, 400, "{b}");
    assert_eq!(j(&b)["error"], "invalid_input");
    let wrong = json!({"query": "SELECT body FROM logs", "cursor": c}).to_string();
    let (st, _) = req(
        &s,
        "POST",
        "/api/v1/query",
        Some(&s.a_rw),
        "application/json",
        &wrong,
    )
    .await;
    assert_eq!(st, 400);
    // Aggregates cannot be paginated; page_size 0 is invalid.
    for body in [
        json!({"query": "SELECT count(*) FROM logs", "page_size": 5}),
        json!({"query": sql, "page_size": 0}),
    ] {
        let (st, _) = req(
            &s,
            "POST",
            "/api/v1/query",
            Some(&s.a_rw),
            "application/json",
            &body.to_string(),
        )
        .await;
        assert_eq!(st, 400, "{body}");
    }
}

/// `/metrics` exposes latency histograms and query outcomes in valid
/// Prometheus text format, consistent with the requests made, without tenant
/// names (spec §2.5, §3.2, §25).
#[tokio::test(flavor = "multi_thread")]
async fn metrics_histograms_and_query_outcomes() {
    let s = start(ServerConfig::default()).await;
    for i in 0..5 {
        let (st, _) = req(
            &s,
            "POST",
            "/api/v1/ingest",
            Some(&s.a_rw),
            "application/json",
            &format!(r#"{{"service":"m","body":"metric {i}"}}"#),
        )
        .await;
        assert_eq!(st, 200);
    }
    for _ in 0..3 {
        let (st, body) = query(&s, &s.a_rw, "SELECT count(*) FROM logs").await;
        assert_eq!(st, 200);
        // Phase decomposition is reported per query (STRATA-SYS-01-007).
        let stats = &body["stats"];
        for k in ["queue_us", "parse_us", "plan_us", "exec_us"] {
            assert!(stats[k].as_f64().is_some_and(|v| v >= 0.0), "{k}: {stats}");
        }
        assert!(stats["exec_us"].as_f64().unwrap() > 0.0);
        assert!(stats["scan_workers"].as_u64().is_some() && stats["est_cpu_ms"].as_f64().is_some());
        let parts = ["queue_us", "parse_us", "plan_us", "exec_us"]
            .iter()
            .map(|k| stats[k].as_f64().unwrap())
            .sum::<f64>();
        assert!(
            parts <= stats["elapsed_ms"].as_f64().unwrap() * 1000.0 + 1.0,
            "{stats}"
        );
    }
    assert_eq!(query(&s, &s.a_rw, "SELECT nope FROM logs").await.0, 400);
    // Unauthenticated requests are not queries.
    assert_eq!(
        query(&s, "sdb_bad_token", "SELECT 1 FROM logs").await.0,
        401
    );
    let (st, text) = req(
        &s,
        "GET",
        "/api/v1/metrics",
        Some(&s.metrics),
        "text/plain",
        "",
    )
    .await;
    assert_eq!(st, 200);

    // Every sample line is `name{labels} value` with a declared family.
    let mut families = std::collections::BTreeSet::new();
    let mut samples: Vec<(String, String, f64)> = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("# TYPE ") {
            families.insert(rest.split(' ').next().unwrap().to_owned());
            continue;
        }
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let (series, value) = line
            .rsplit_once(' ')
            .unwrap_or_else(|| panic!("bad line {line:?}"));
        let v: f64 = value
            .parse()
            .unwrap_or_else(|_| panic!("bad value in {line:?}"));
        let (name, labels) = match series.split_once('{') {
            Some((n, l)) => (n.to_owned(), l.trim_end_matches('}').to_owned()),
            None => (series.to_owned(), String::new()),
        };
        let family = ["_bucket", "_sum", "_count"]
            .iter()
            .find_map(|suf| name.strip_suffix(suf).filter(|f| families.contains(*f)))
            .unwrap_or(&name)
            .to_owned();
        assert!(families.contains(&family), "undeclared family for {line:?}");
        samples.push((name, labels, v));
    }
    let get = |name: &str, labels: &str| {
        samples
            .iter()
            .find(|(n, l, _)| n == name && l == labels)
            .map(|(_, _, v)| *v)
            .unwrap_or_else(|| panic!("missing {name}{{{labels}}}"))
    };
    assert_eq!(
        get("stratadb_ingest_ack_seconds_count", "endpoint=\"ingest\""),
        5.0
    );
    assert_eq!(
        get("stratadb_ingest_ack_seconds_count", "endpoint=\"otlp\""),
        0.0
    );
    assert_eq!(get("stratadb_queries_total", "outcome=\"ok\""), 3.0);
    assert_eq!(get("stratadb_queries_total", "outcome=\"invalid\""), 1.0);
    assert_eq!(get("stratadb_query_seconds_count", ""), 4.0);
    assert!(get("stratadb_query_rows_scanned_total", "") >= 15.0);
    assert!(
        get("stratadb_wal_sync_seconds_count", "") >= 5.0,
        "group commit fsyncs"
    );
    assert!(get("stratadb_wal_group_commit_records_count", "") >= 1.0);
    // Histograms: cumulative buckets, +Inf bucket == count.
    for (fam, labels) in [
        ("stratadb_ingest_ack_seconds", "endpoint=\"ingest\","),
        ("stratadb_query_seconds", ""),
        ("stratadb_wal_sync_seconds", ""),
    ] {
        let buckets: Vec<f64> = samples
            .iter()
            .filter(|(n, l, _)| n == &format!("{fam}_bucket") && l.starts_with(labels))
            .map(|(_, _, v)| *v)
            .collect();
        assert!(
            buckets.len() > 5 && buckets.windows(2).all(|w| w[0] <= w[1]),
            "{fam}"
        );
        let count_labels = labels.trim_end_matches(',');
        assert_eq!(
            *buckets.last().unwrap(),
            get(&format!("{fam}_count"), count_labels),
            "{fam}"
        );
    }
    // INSERT stage family (STRATA-SYS-01-001): every stage, pipeline order,
    // one observation per committed batch; µs-resolution buckets.
    let stages = [
        "auth",
        "decode",
        "validate",
        "admission",
        "wal_encode",
        "wal_append",
        "group_commit_wait",
        "memtable_insert",
        "ack",
    ];
    let order: Vec<&str> = text
        .lines()
        .filter_map(|l| l.strip_prefix("strata_insert_stage_seconds_count{stage=\""))
        .map(|l| l.split('"').next().unwrap())
        .collect();
    assert_eq!(order, stages, "stage order");
    for st in stages {
        assert_eq!(
            get(
                "strata_insert_stage_seconds_count",
                &format!("stage=\"{st}\"")
            ),
            5.0,
            "{st}"
        );
        assert_eq!(
            get(
                "strata_insert_stage_seconds_bucket",
                &format!("stage=\"{st}\",le=\"+Inf\"")
            ),
            5.0
        );
    }
    assert!(
        text.contains("strata_insert_stage_seconds_bucket{stage=\"wal_append\",le=\"0.000001\"}")
    );
    // Group commit waits for an fsync: never zero.
    assert!(
        get(
            "strata_insert_stage_seconds_sum",
            "stage=\"group_commit_wait\""
        ) > 0.0
    );
    // Shared scan pool (STRATA-SYS-01-010): fixed workers, bounded tasks.
    assert!(get("strata_scan_pool_workers", "") >= 1.0);
    assert!(get("strata_scan_pool_active_tasks", "") <= 2.0 * get("strata_scan_pool_workers", ""));
    get("strata_scan_pool_tasks_total", "result=\"accepted\"");
    get("strata_scan_pool_tasks_total", "result=\"rejected\"");
    for st in ["queue", "parse", "plan", "exec", "serialize"] {
        assert_eq!(
            get(
                "strata_query_stage_seconds_count",
                &format!("stage=\"{st}\"")
            ),
            3.0,
            "{st}: successful queries only"
        );
    }
    assert!(
        !text.contains("tenant-a") && !text.contains("tenant-b"),
        "no tenant labels"
    );
}
