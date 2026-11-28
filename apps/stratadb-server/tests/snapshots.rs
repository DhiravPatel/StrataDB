//! `GET /api/v1/admin/snapshots` and the snapshot metrics (spec
//! STRATA-SYS-17-005): operators see open snapshots and the obsolete bytes
//! retained; tenants cannot.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::Value;
use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Durability, Engine, EngineOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// (status, headers, body)
async fn req(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: &str,
    body: &str,
) -> (u16, String, String) {
    let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
    let msg = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    c.write_all(msg.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    c.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text[9..12].parse().unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    (status, head.to_ascii_lowercase(), body.to_owned())
}

#[tokio::test(flavor = "multi_thread")]
async fn operators_see_open_snapshots_and_retained_bytes() {
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
    let rw = mk("t", &[Permission::Ingest, Permission::Query]);
    let ops = mk("ops", &[Permission::ClusterRead]);
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Async;
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    // A snapshot held outside the API shows up with its holder.
    let held = engine.snapshot(&TenantId::new("t").unwrap());
    held.set_holder("query:report-1");
    let state = AppState::new(
        Arc::clone(&engine),
        TokenStore::load(dir.path()).unwrap(),
        ServerConfig::default(),
    );
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let (st, _, _) = req(addr, "GET", "/api/v1/admin/snapshots", &rw, "").await;
    assert_eq!(st, 403, "tenants cannot list other tenants' snapshots");
    let (st, _, b) = req(addr, "GET", "/api/v1/admin/snapshots", &ops, "").await;
    assert_eq!(st, 200, "{b}");
    let v: Value = serde_json::from_str(&b).unwrap();
    let open = v["logs"]["open"].as_array().unwrap();
    assert_eq!(open.len(), 1, "{v}");
    assert_eq!(open[0]["holder"], "query:report-1");
    assert_eq!(open[0]["tenant"], "t");
    for k in [
        "obsolete_segments",
        "obsolete_bytes",
        "disk_total_bytes",
        "obsolete_ratio",
        "warn",
    ] {
        assert!(!v["logs"][k].is_null(), "{k} in {v}");
    }
    let (_, _, text) = req(addr, "GET", "/api/v1/metrics", &ops, "").await;
    assert!(text.contains("strata_snapshots_open 1"), "metrics");
    assert!(text.contains("strata_obsolete_retained_bytes 0"));
    drop(held);
    let (_, _, text) = req(addr, "GET", "/api/v1/metrics", &ops, "").await;
    assert!(text.contains("strata_snapshots_open 0"));
}

#[tokio::test(flavor = "multi_thread")]
async fn repeated_queries_are_served_from_the_plan_cache() {
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
    let a = mk("a", &[Permission::Ingest, Permission::Query]);
    let b = mk("b", &[Permission::Ingest, Permission::Query]);
    let ops = mk("ops", &[Permission::ClusterRead]);
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Async;
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let app = router(AppState::new(
        engine,
        TokenStore::load(dir.path()).unwrap(),
        ServerConfig::default(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for (tok, n) in [(&a, 3), (&b, 1)] {
        for i in 0..n {
            let (st, _, _) = req(
                addr,
                "POST",
                "/api/v1/ingest",
                tok,
                &format!(r#"{{"body":"x{i}"}}"#),
            )
            .await;
            assert_eq!(st, 200);
        }
    }
    let q = r#"{"query":"SELECT count(body) FROM logs"}"#;
    let run = |tok: String| async move {
        let (st, _, body) = req(addr, "POST", "/api/v1/query", &tok, q).await;
        assert_eq!(st, 200, "{body}");
        serde_json::from_str::<Value>(&body).unwrap()
    };
    let first = run(a.clone()).await;
    assert_eq!(first["stats"]["plan_cache"], "miss");
    let second = run(a.clone()).await;
    assert_eq!(second["stats"]["plan_cache"], "hit");
    assert_eq!(second["stats"]["plan_us"], 0.0);
    // Shared across tenants, but each runs on its own snapshot.
    let other = run(b.clone()).await;
    assert_eq!(other["stats"]["plan_cache"], "hit");
    assert_eq!(first["rows"][0]["count(body)"], 3);
    assert_eq!(other["rows"][0]["count(body)"], 1);
    let (st, _, body) = req(addr, "GET", "/api/v1/admin/caches", &ops, "").await;
    assert_eq!(st, 200, "{body}");
    let c: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        (
            c["plan_cache"]["hits"].as_u64(),
            c["plan_cache"]["misses"].as_u64()
        ),
        (Some(2), Some(1))
    );
    assert_eq!(c["plan_cache"]["top_fingerprints"][0]["hits"], 2);
    let (_, _, text) = req(addr, "GET", "/api/v1/metrics", &ops, "").await;
    assert!(text.contains("strata_plan_cache_hits_total 2"));
    // Estimated vs actual rows (STRATA-SYS-12-004): 3 memtable rows,
    // estimated exactly.
    assert_eq!(first["stats"]["est_rows"], 3);
    assert_eq!(first["stats"]["q_error"], 1.0);
    assert!(text.contains("strata_query_estimate_q_error_count 3"));
}

/// `[limits] partial_cache_bytes` (ADR-0045): a repeated aggregate is
/// answered from cached per-segment partials, with identical results, and
/// the cache is visible in `/admin/caches` and the metrics.
#[tokio::test(flavor = "multi_thread")]
async fn repeated_aggregates_reuse_cached_segment_partials() {
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
    let rw = mk("t", &[Permission::Ingest, Permission::Query]);
    let ops = mk("ops", &[Permission::ClusterRead]);
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Async;
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let cfg = ServerConfig {
        partial_cache_bytes: 16 << 20,
        ..ServerConfig::default()
    };
    let state = AppState::new(
        Arc::clone(&engine),
        TokenStore::load(dir.path()).unwrap(),
        cfg,
    );
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for k in 0..3 {
        let lines: Vec<String> = (0..200)
            .map(|i| format!(r#"{{"body":"b{i}","service":"s{}"}}"#, (i + k) % 4))
            .collect();
        let body = format!("[{}]", lines.join(","));
        let (st, _, b) = req(addr, "POST", "/api/v1/ingest", &rw, &body).await;
        assert_eq!(st, 200, "{b}");
        engine.flush().unwrap();
    }
    let q = r#"{"query":"SELECT service, count(*) FROM logs GROUP BY service ORDER BY service"}"#;
    let (_, _, first) = req(addr, "POST", "/api/v1/query", &rw, q).await;
    let (_, _, second) = req(addr, "POST", "/api/v1/query", &rw, q).await;
    let (a, b): (Value, Value) = (
        serde_json::from_str(&first).unwrap(),
        serde_json::from_str(&second).unwrap(),
    );
    assert_eq!(a["rows"], b["rows"]);
    assert_eq!(a["stats"]["partials_from_cache"], 0, "{a}");
    assert_eq!(b["stats"]["partials_from_cache"], 3, "{b}");
    assert_eq!(b["stats"]["rows_from_cache"], 600);
    let (_, _, caches) = req(addr, "GET", "/api/v1/admin/caches", &ops, "").await;
    let c: Value = serde_json::from_str(&caches).unwrap();
    assert_eq!(c["partial_cache"]["entries"], 3, "{c}");
    assert!(c["partial_cache"]["hits"].as_u64().unwrap() >= 3);
    let (_, _, text) = req(addr, "GET", "/api/v1/metrics", &ops, "").await;
    assert!(text.contains("strata_partial_cache_entries 3"), "metrics");
}
