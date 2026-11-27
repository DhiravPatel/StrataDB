//! Server memory accounting over HTTP (spec VIII.5.2, STRATA-SYS-05-007/009,
//! ARCH-01-006): per-tenant query memory quotas fail queries with
//! `MEMORY_LIMIT_EXCEEDED` without naming other tenants, reservations return
//! to zero after every query, ingest memory is tracked in its own region,
//! and the tree, budget and RSS gap are visible to operators.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{json, Value};
use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::memory::{MemorySettings, ServerMemory};
use stratadb_server::{router, AppState, Quotas, ServerConfig, TenantQuota};
use stratadb_storage::{Durability, Engine, EngineOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Srv {
    addr: std::net::SocketAddr,
    small: String,
    big: String,
    ops: String,
    mem: Arc<ServerMemory>,
    _dir: tempfile::TempDir,
}

async fn start() -> Srv {
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
    let small = mk("small", &[Permission::Ingest, Permission::Query]);
    let big = mk("big", &[Permission::Ingest, Permission::Query]);
    let ops = mk("ops", &[Permission::ClusterRead]);
    let mut quotas = Quotas::default();
    quotas.tenants.insert(
        "small".into(),
        TenantQuota {
            query_memory_bytes: Some(32 << 10),
            ..Default::default()
        },
    );
    let plan = MemorySettings {
        max_server_memory_bytes: Some(1 << 30),
        ..MemorySettings::default()
    }
    .plan(None)
    .unwrap();
    let mem = ServerMemory::new(plan, quotas.clone());
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Sync;
    o.background = false;
    o.memory = Some(Arc::clone(&mem.ingest));
    o.cache_memory = Some(Arc::clone(&mem.cache));
    o.background_memory = Some(Arc::clone(&mem.background));
    let engine = Arc::new(Engine::open(o).unwrap());
    let cfg = ServerConfig {
        quotas,
        ..ServerConfig::default()
    };
    let app = router(
        AppState::new(engine, TokenStore::load(dir.path()).unwrap(), cfg)
            .with_memory(Arc::clone(&mem)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Srv {
        addr,
        small,
        big,
        ops,
        mem,
        _dir: dir,
    }
}

async fn http(
    s: &Srv,
    method: &str,
    path: &str,
    token: &str,
    ctype: &str,
    body: &str,
) -> (u16, String) {
    let mut c = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let msg = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    c.write_all(msg.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    c.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (text[9..12].parse().unwrap(), body)
}

async fn query(s: &Srv, token: &str, sql: &str) -> (u16, Value) {
    let (st, b) = http(
        s,
        "POST",
        "/api/v1/query",
        token,
        "application/json",
        &json!({"query": sql}).to_string(),
    )
    .await;
    (
        st,
        serde_json::from_str(&b).unwrap_or_else(|e| panic!("{e}: {b}")),
    )
}

async fn ingest(s: &Srv, token: &str, n: usize) {
    let body: String = (0..n)
        .map(|i| json!({"service": "api", "body": format!("request {i} handled with a fairly long message body")}).to_string() + "\n")
        .collect();
    let (st, b) = http(
        s,
        "POST",
        "/api/v1/ingest/batch",
        token,
        "application/x-ndjson",
        &body,
    )
    .await;
    assert_eq!(st, 200, "{b}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tenant_quotas_regions_and_visibility() {
    let s = start().await;
    ingest(&s, &s.small, 2000).await;
    ingest(&s, &s.big, 2000).await;
    // Ingest memory is held in its own region until flushed.
    assert!(s.mem.ingest.used() > 0);
    assert_eq!(s.mem.query.used(), 0);

    // The big tenant's query fits; the small tenant's quota (32 KiB) does not
    // cover a body batch of 2000 rows.
    let (st, v) = query(&s, &s.big, "SELECT count(*) FROM logs WHERE body = 'x'").await;
    assert_eq!(st, 200, "{v}");
    let (st, v) = query(&s, &s.small, "SELECT count(*) FROM logs WHERE body = 'x'").await;
    assert_eq!(st, 429, "{v}");
    assert_eq!(v["error"], "resource_exhausted");
    let msg = v["message"].as_str().unwrap();
    assert!(
        msg.contains(
            "MEMORY_LIMIT_EXCEEDED: process/query/tenant:small would exceed its limit of 32.0 KiB"
        ),
        "{msg}"
    );
    assert!(!msg.contains("tenant:big"), "{msg}");
    // Every reservation was returned.
    assert_eq!(s.mem.query.used(), 0);
    // A query without a body column fits the small quota.
    let (st, v) = query(&s, &s.small, "SELECT count(*) FROM logs").await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(v["rows"][0]["count(*)"], 2000);

    // Operators see the budget and the tree (tenants included).
    let (st, b) = http(
        &s,
        "GET",
        "/api/v1/admin/memory",
        &s.ops,
        "application/json",
        "",
    )
    .await;
    assert_eq!(st, 200, "{b}");
    let m: Value = serde_json::from_str(&b).unwrap();
    assert_eq!(m["plan"]["budget"], 1u64 << 30);
    let regions: Vec<&str> = m["tree"]["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    for r in ["control", "ingest", "cache", "background", "query"] {
        assert!(regions.contains(&r), "{regions:?}");
    }
    let pool = m["tree"]["children"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "query")
        .unwrap();
    assert!(pool["peak"].as_u64().unwrap() > 0);
    let tenants: Vec<&str> = pool["children"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(
        tenants.contains(&"tenant:small") && tenants.contains(&"tenant:big"),
        "{tenants:?}"
    );
    // Tenants cannot read it.
    let (st, _) = http(
        &s,
        "GET",
        "/api/v1/admin/memory",
        &s.small,
        "application/json",
        "",
    )
    .await;
    assert_eq!(st, 403);

    // Metrics.
    s.mem.sample();
    let (st, text) = http(&s, "GET", "/api/v1/metrics", &s.ops, "text/plain", "").await;
    assert_eq!(st, 200);
    for series in [
        "strata_memory_tracked_bytes{region=\"ingest\"}",
        "strata_memory_limit_bytes{region=\"query\"}",
        "strata_memory_budget_bytes 1073741824",
        "strata_memory_resident_bytes ",
        "strata_memory_untracked_bytes ",
    ] {
        assert!(text.contains(series), "missing {series}");
    }
    let resident: u64 = text
        .lines()
        .find_map(|l| l.strip_prefix("strata_memory_resident_bytes "))
        .unwrap()
        .parse()
        .unwrap();
    assert!(resident > 0);
}
