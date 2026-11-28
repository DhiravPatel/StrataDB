//! System state machines over HTTP (spec IX.22, STRATA-ARCH-22-001..008):
//! queries and inserts walk their lifecycles, server/table/device states
//! follow health, and `/metrics` exports time in each state.
//!
//! One test per binary: the state counters are process-wide.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};
use stratadb_common::state::{DeviceState, ServerState, TableState};
use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::lifecycle::Lifecycle;
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Durability, Engine, EngineOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn req(
    addr: std::net::SocketAddr,
    method: &str,
    path: &str,
    token: &str,
    body: &str,
) -> (u16, String) {
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
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

/// `(machine, state) -> value` for one `strata_state_*` sample family.
fn family(text: &str, name: &str) -> BTreeMap<(String, String), f64> {
    let mut out = BTreeMap::new();
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(name).and_then(|r| r.strip_prefix('{')) else {
            continue;
        };
        let (labels, v) = rest.split_once("} ").unwrap();
        let get = |k: &str| {
            labels
                .split(',')
                .find_map(|kv| kv.strip_prefix(&format!("{k}=\"")))
                .map(|v| v.trim_end_matches('"').to_owned())
                .unwrap_or_default()
        };
        out.insert((get("machine"), get("state")), v.parse().unwrap());
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycles_are_tracked_and_exported() {
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

    // Server lifecycle as `serve` drives it.
    let life = Arc::new(Lifecycle::default());
    assert_eq!(life.server(), ServerState::Starting);
    life.recovering();
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Sync;
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    life.warming();
    assert_eq!(life.table(), Some(TableState::Active));
    let app = router(
        AppState::new(
            Arc::clone(&engine),
            TokenStore::load(dir.path()).unwrap(),
            ServerConfig::default(),
        )
        .with_lifecycle(Arc::clone(&life)),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    life.ready();
    let (st, b) = req(addr, "GET", "/api/v1/ready", "", "").await;
    assert_eq!(
        (st, serde_json::from_str::<Value>(&b).unwrap()),
        (200, json!({"status": "ready", "state": "READY"}))
    );

    // Inserts: 3 accepted requests, 1 rejected (nothing valid).
    for i in 0..3 {
        let (st, b) = req(
            addr,
            "POST",
            "/api/v1/ingest",
            &rw,
            &format!(r#"{{"body":"e{i}"}}"#),
        )
        .await;
        assert_eq!(st, 200, "{b}");
    }
    let (st, _) = req(
        addr,
        "POST",
        "/api/v1/ingest",
        &rw,
        r#"[{"ingest_time":1}]"#,
    )
    .await;
    assert_eq!(st, 422);
    // Queries: 2 finished, 1 syntax error, 1 unknown column.
    for _ in 0..2 {
        let (st, b) = req(
            addr,
            "POST",
            "/api/v1/query",
            &rw,
            &json!({"query": "SELECT count(*) FROM logs"}).to_string(),
        )
        .await;
        assert_eq!(st, 200, "{b}");
    }
    let (st, _) = req(
        addr,
        "POST",
        "/api/v1/query",
        &rw,
        &json!({"query": "SELEC 1"}).to_string(),
    )
    .await;
    assert_eq!(st, 400);
    let (st, _) = req(
        addr,
        "POST",
        "/api/v1/query",
        &rw,
        &json!({"query": "SELECT nope FROM logs"}).to_string(),
    )
    .await;
    assert_eq!(st, 400);

    // Health tick: nothing wrong, too few fsyncs for a device baseline.
    life.tick(&engine, Instant::now());
    assert_eq!(life.server(), ServerState::Ready);
    assert_eq!(life.device(), DeviceState::Unknown);

    let (st, text) = req(addr, "GET", "/api/v1/metrics", &ops, "").await;
    assert_eq!(st, 200);
    let count = family(&text, "strata_state_seconds_count");
    let entities = family(&text, "strata_state_entities");
    let c = |m: &str, s: &str| {
        count
            .get(&(m.to_owned(), s.to_owned()))
            .copied()
            .unwrap_or(-1.0)
    };
    let n = |m: &str, s: &str| {
        entities
            .get(&(m.to_owned(), s.to_owned()))
            .copied()
            .unwrap_or(-1.0)
    };
    // Every machine is exported, including those with no entity yet.
    for m in [
        "query_lifecycle",
        "insert_lifecycle",
        "wal_segment",
        "merge_task",
        "connection",
        "table",
        "server",
        "device",
    ] {
        assert!(
            count.keys().any(|(mm, _)| mm == m),
            "{m} missing from /metrics"
        );
    }
    // Queries: 4 left RECEIVED (2 failed there: the parser also resolves
    // names), 2 planned, ran and streamed. /metrics itself is not a query.
    assert_eq!(c("query_lifecycle", "RECEIVED"), 4.0);
    assert_eq!(c("query_lifecycle", "PARSED"), 2.0);
    assert_eq!(c("query_lifecycle", "PLANNED"), 2.0);
    assert_eq!(c("query_lifecycle", "QUEUED"), 2.0);
    assert_eq!(c("query_lifecycle", "RUNNING"), 2.0);
    assert_eq!(c("query_lifecycle", "STREAMING"), 2.0);
    for s in ["RECEIVED", "PARSED", "RUNNING", "FINISHED", "FAILED"] {
        assert_eq!(n("query_lifecycle", s), 0.0, "query entities in {s}");
    }
    // Inserts: 4 decoded; 3 validated, logged, durable, visible.
    assert_eq!(c("insert_lifecycle", "RECEIVED"), 4.0);
    assert_eq!(c("insert_lifecycle", "DECODED"), 4.0);
    for s in ["VALIDATED", "LOGGED", "DURABLE", "VISIBLE"] {
        assert_eq!(c("insert_lifecycle", s), 3.0, "insert {s}");
    }
    // The WAL's segment is ACTIVE; server READY, table ACTIVE, device UNKNOWN.
    assert_eq!(n("wal_segment", "ACTIVE"), 1.0);
    assert_eq!(n("server", "READY"), 1.0);
    assert_eq!(n("table", "ACTIVE"), 1.0);
    assert_eq!(n("device", "UNKNOWN"), 1.0);
    assert_eq!(c("server", "STARTING"), 1.0);
    assert_eq!(c("table", "CREATING"), 1.0);
    assert!(text.contains("strata_state_illegal_transitions_total{machine=\"query_lifecycle\"} 0"));
    assert!(text.contains("strata_state_illegal_transitions_total{machine=\"insert_lifecycle\"} 0"));

    // Disk below the reserve: the table is READ_ONLY and the server DEGRADED,
    // still ready for reads; a healthy engine clears both.
    let mut o = EngineOptions::new(dir.path().join("full"));
    o.background = false;
    o.disk_reserve_ratio = 1.0;
    let full = Engine::open(o).unwrap();
    assert!(full.disk_below_reserve() && !engine.disk_below_reserve());
    life.tick(&full, Instant::now());
    assert_eq!(
        (life.table(), life.server()),
        (Some(TableState::ReadOnly), ServerState::Degraded)
    );
    let (st, b) = req(addr, "GET", "/api/v1/ready", "", "").await;
    assert_eq!(
        (
            st,
            serde_json::from_str::<Value>(&b).unwrap()["state"].clone()
        ),
        (200, json!("DEGRADED"))
    );
    life.tick(&engine, Instant::now());
    assert_eq!(
        (life.table(), life.server()),
        (Some(TableState::Active), ServerState::Ready)
    );

    // SIGTERM: draining servers are not ready.
    life.draining();
    let (st, b) = req(addr, "GET", "/api/v1/ready", "", "").await;
    assert_eq!(
        (st, serde_json::from_str::<Value>(&b).unwrap()),
        (503, json!({"status": "unavailable", "state": "DRAINING"}))
    );
    life.stopped();
    assert_eq!(life.server(), ServerState::Stopped);
    let (_, text) = req(addr, "GET", "/api/v1/metrics", &ops, "").await;
    let count = family(&text, "strata_state_seconds_count");
    for s in ["STARTING", "RECOVERING", "WARMING", "DRAINING"] {
        assert_eq!(
            count[&("server".to_owned(), s.to_owned())],
            1.0,
            "server {s}"
        );
    }
    assert_eq!(count[&("server".to_owned(), "READY".to_owned())], 2.0);
    assert_eq!(count[&("table".to_owned(), "READ_ONLY".to_owned())], 1.0);
}
