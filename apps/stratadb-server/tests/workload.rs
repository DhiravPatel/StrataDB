//! Workload classes and admission over HTTP (spec VIII.11,
//! STRATA-SYS-11-001/003/004/010): queued queries show their position and
//! can be cancelled, a full queue is refused with `TOO_MANY_QUERIES` and a
//! retry hint, classes come from the cost estimate and can only be lowered
//! by the request, and the controller is visible in `/metrics` and
//! `/api/v1/admin/workload`.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use stratadb_common::workload::WorkClass;
use stratadb_common::TenantId;
use stratadb_server::admission::{Ask, WorkloadSettings};
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

fn j(s: &str) -> Value {
    serde_json::from_str(s).unwrap_or_else(|e| panic!("not JSON ({e}): {s}"))
}

fn q(sql: &str, extra: Value) -> String {
    let mut v = json!({"query": sql});
    for (k, x) in extra.as_object().unwrap() {
        v[k] = x.clone();
    }
    v.to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_are_classified_queued_and_refused_over_http() {
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
    let tok = mk("t", &[Permission::Query]);
    let ops = mk("ops", &[Permission::ClusterRead]);
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Async;
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let cfg = ServerConfig {
        max_concurrent_queries: 1,
        workload: WorkloadSettings {
            queue_max: 1,
            queue_timeout: Duration::from_secs(20),
            ..WorkloadSettings::default()
        },
        ..ServerConfig::default()
    };
    let state = AppState::new(engine, TokenStore::load(dir.path()).unwrap(), cfg);
    let adm = state.admission();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (st, _, _) = req(addr, "POST", "/api/v1/ingest", &rw, r#"{"body":"x"}"#).await;
    assert_eq!(st, 200);

    // An empty table is cheap: interactive. A request may lower its class.
    let (st, _, b) = req(
        addr,
        "POST",
        "/api/v1/query",
        &rw,
        &q("SELECT count(*) FROM logs", json!({})),
    )
    .await;
    assert_eq!(st, 200, "{b}");
    assert_eq!(j(&b)["stats"]["class"], "interactive");
    let (st, _, b) = req(
        addr,
        "POST",
        "/api/v1/query",
        &rw,
        &q(
            "SELECT count(*) FROM logs",
            json!({"workload_class": "batch"}),
        ),
    )
    .await;
    assert_eq!((st, j(&b)["stats"]["class"].clone()), (200, json!("batch")));
    let (st, _, b) = req(
        addr,
        "POST",
        "/api/v1/query",
        &rw,
        &q("SELECT 1 FROM logs", json!({"workload_class": "urgent"})),
    )
    .await;
    assert_eq!(st, 400, "{b}");

    // Occupy the only slot, then queue a query.
    let held = adm
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
    let rw2 = rw.clone();
    let waiting = tokio::spawn(async move {
        req(
            addr,
            "POST",
            "/api/v1/query",
            &rw2,
            &q("SELECT count(*) FROM logs", json!({"query_id": "w1"})),
        )
        .await
    });
    let mut status = Value::Null;
    for _ in 0..200 {
        let (st, _, b) = req(addr, "GET", "/api/v1/query/w1", &tok, "").await;
        if st == 200 {
            status = j(&b);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(status["state"], "queued", "{status}");
    assert_eq!(status["queue_position"], 0);
    assert_eq!(status["class"], "interactive");

    // The queue (1) is full: refused at once with a retry hint.
    let (st, head, b) = req(
        addr,
        "POST",
        "/api/v1/query",
        &tok,
        &q("SELECT count(*) FROM logs", json!({})),
    )
    .await;
    assert_eq!(st, 429, "{b}");
    let e = j(&b);
    assert_eq!(e["code"], "TOO_MANY_QUERIES");
    assert!(e["retry_after_ms"].as_u64().unwrap() >= 100, "{e}");
    assert!(head.contains("retry-after:"), "{head}");

    // Free the slot: the queued query runs and reports its wait.
    drop(held);
    let (st, head, b) = waiting.await.unwrap();
    assert_eq!(st, 200, "{b}");
    assert!(j(&b)["stats"]["queue_us"].as_f64().unwrap() > 0.0, "{b}");
    assert!(head.contains("x-stratadb-queue-us"), "{head}");

    // Cancel while queued.
    let held = adm
        .admit(Ask {
            class: WorkClass::Normal,
            tenant: 99,
            est_cpu_ms: 1.0,
            max_wait: Duration::from_secs(1),
            cancel: None,
            key: "held2".into(),
        })
        .await
        .unwrap();
    let tok2 = tok.clone();
    let cancelled = tokio::spawn(async move {
        req(
            addr,
            "POST",
            "/api/v1/query",
            &tok2,
            &q("SELECT count(*) FROM logs", json!({"query_id": "w2"})),
        )
        .await
    });
    for _ in 0..200 {
        let (st, _, _) = req(addr, "GET", "/api/v1/query/w2", &tok, "").await;
        if st == 200 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let (st, _, _) = req(addr, "DELETE", "/api/v1/query/w2", &tok, "").await;
    assert_eq!(st, 202);
    let (st, _, b) = cancelled.await.unwrap();
    assert_eq!(j(&b)["error"], "cancelled", "{st} {b}");
    drop(held);

    // Visible to operators.
    let (st, _, b) = req(addr, "GET", "/api/v1/admin/workload", &ops, "").await;
    assert_eq!(st, 200, "{b}");
    let w = j(&b);
    assert_eq!(w["max_concurrent_queries"], 1);
    let classes = w["classes"].as_array().unwrap();
    assert_eq!(classes.len(), 3);
    let inter = classes
        .iter()
        .find(|c| c["class"] == "interactive")
        .unwrap();
    assert!(inter["admitted_total"].as_u64().unwrap() >= 2, "{inter}");
    assert_eq!(inter["rejected_queue_full_total"], 1);
    assert_eq!(inter["cancelled_queued_total"], 1);
    assert!(inter["queue_wait_p99_ms"].as_f64().is_some(), "{inter}");
    let (_, _, text) = req(addr, "GET", "/api/v1/metrics", &ops, "").await;
    for needle in [
        "strata_workload_slots{class=\"batch\"}",
        "strata_workload_queries_total{class=\"interactive\",outcome=\"queue_full\"} 1",
        "strata_workload_queue_wait_seconds_count{class=\"interactive\"}",
        "strata_scan_pool_class_cpu_seconds_total{class=\"normal\"}",
    ] {
        assert!(text.contains(needle), "missing {needle}");
    }
}
