//! `system.query_log` over HTTP (spec STRATA-SYS-01-007, -11-006, -13-009):
//! every query is recorded with its fingerprint, queue/plan/exec/CPU times
//! and work counters; each tenant reads only its own entries; top-N by CPU
//! per fingerprint works with the normal executor; failures are recorded;
//! timing headers are returned; literal values are not stored.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::query_log::{QueryLog, QueryLogConfig};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Durability, Engine, EngineOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Srv {
    addr: std::net::SocketAddr,
    a: String,
    b: String,
    ops: String,
    log: Option<Arc<QueryLog>>,
    task: tokio::task::JoinHandle<()>,
    _dir: tempfile::TempDir,
}

async fn start(enabled: bool, dir: Option<tempfile::TempDir>) -> Srv {
    let dir = dir.unwrap_or_else(|| tempfile::tempdir().unwrap());
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
    let a = mk("tenant-a", &[Permission::Ingest, Permission::Query]);
    let b = mk("tenant-b", &[Permission::Ingest, Permission::Query]);
    let ops = mk("ops", &[Permission::ClusterRead]);
    let data = dir.path().join("data");
    let mut o = EngineOptions::new(&data);
    o.wal.durability = Durability::Sync;
    o.background = false;
    let log = enabled.then(|| {
        let cfg = QueryLogConfig {
            flush_interval: Duration::from_millis(20),
            ..QueryLogConfig::default()
        };
        Arc::new(QueryLog::open(&data, &o, &cfg).unwrap())
    });
    let engine = Arc::new(Engine::open(o).unwrap());
    let mut st = AppState::new(
        engine,
        TokenStore::load(dir.path()).unwrap(),
        ServerConfig::default(),
    );
    if let Some(l) = &log {
        st = st.with_query_log(Arc::clone(l));
    }
    let app = router(st);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Srv {
        addr,
        a,
        b,
        ops,
        log,
        task,
        _dir: dir,
    }
}

/// (status, response head, body).
async fn http(
    s: &Srv,
    method: &str,
    path: &str,
    token: &str,
    ctype: &str,
    body: &str,
) -> (u16, String, String) {
    let mut c = tokio::net::TcpStream::connect(s.addr).await.unwrap();
    let msg = format!(
        "{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nAuthorization: Bearer {token}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    c.write_all(msg.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    c.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    (
        text[9..12].parse().unwrap(),
        head.to_ascii_lowercase(),
        body.to_owned(),
    )
}

async fn query(s: &Srv, token: &str, sql: &str) -> (u16, String, Value) {
    let (st, head, body) = http(
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
        head,
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("{e}: {body}")),
    )
}

fn header(head: &str, name: &str) -> Option<String> {
    head.lines()
        .find_map(|l| l.strip_prefix(&format!("{name}: ")).map(str::to_owned))
}

/// Query `system.query_log` until it has at least `n` rows (the writer
/// flushes every 20 ms here).
async fn log_rows(s: &Srv, token: &str, sql: &str, n: usize) -> Value {
    for _ in 0..200 {
        let (st, _, v) = query(s, token, sql).await;
        assert_eq!(st, 200, "{v}");
        if v["rows"].as_array().unwrap().len() >= n {
            return v;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("query log never reached {n} rows for {sql}");
}

#[tokio::test(flavor = "multi_thread")]
async fn queries_are_logged_per_tenant_with_fingerprints_and_timings() {
    let s = start(true, None).await;
    let events: String = (0..300)
        .map(|i| json!({"service": format!("svc{}", i % 3), "body": format!("event {i}"), "attributes": {"secret_code": format!("c{i}")}}).to_string() + "\n")
        .collect();
    let (st, _, b) = http(
        &s,
        "POST",
        "/api/v1/ingest/batch",
        &s.a,
        "application/x-ndjson",
        &events,
    )
    .await;
    assert_eq!(st, 200, "{b}");

    // Same shape, different literals: one fingerprint.
    let mut fps = BTreeSet::new();
    for svc in ["svc0", "svc1", "svc2"] {
        let (st, head, v) = query(
            &s,
            &s.a,
            &format!("SELECT count(*) FROM logs WHERE service = '{svc}'"),
        )
        .await;
        assert_eq!(st, 200, "{v}");
        let fp = header(&head, "x-stratadb-fingerprint").unwrap();
        assert_eq!(fp, v["fingerprint"].as_str().unwrap());
        assert_eq!(
            header(&head, "x-stratadb-query-id").unwrap(),
            v["query_id"].as_str().unwrap()
        );
        for h in [
            "x-stratadb-queue-us",
            "x-stratadb-plan-us",
            "x-stratadb-exec-us",
            "x-stratadb-cpu-us",
        ] {
            header(&head, h)
                .unwrap()
                .parse::<u64>()
                .unwrap_or_else(|e| panic!("{h}: {e}"));
        }
        fps.insert(fp);
    }
    assert_eq!(fps.len(), 1);
    // A heavier, different shape.
    let (st, _, v) = query(
        &s,
        &s.a,
        "SELECT service, count(*) FROM logs GROUP BY service ORDER BY service",
    )
    .await;
    assert_eq!(st, 200, "{v}");
    let heavy = v["fingerprint"].as_str().unwrap().to_owned();
    assert_ne!(fps.first().unwrap(), &heavy);
    // A failing query is logged too, with its error kind.
    let (st, head, v) = query(&s, &s.a, "SELECT nope FROM logs WHERE service = 'svc0'").await;
    assert_eq!(st, 400, "{v}");
    assert!(header(&head, "x-stratadb-fingerprint").is_some());
    // Tenant b runs one query of its own.
    let (st, _, _) = query(&s, &s.b, "SELECT count(*) FROM logs").await;
    assert_eq!(st, 200);

    // Top-N by total CPU per fingerprint (spec Part IV runbook query).
    let top = log_rows(
        &s,
        &s.a,
        "SELECT attributes['fingerprint'] AS fp, count(*) AS n, sum(attributes['cpu_us']) AS cpu \
         FROM system.query_log WHERE attributes['table'] = 'logs' \
         GROUP BY attributes['fingerprint'] ORDER BY n DESC",
        2,
    )
    .await;
    let rows = top["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "{top}");
    assert_eq!(rows[0]["fp"], json!(fps.first().unwrap()));
    assert_eq!(rows[0]["n"], 3);
    assert!(rows[0]["cpu"].as_i64().unwrap() > 0, "{top}");
    assert_eq!(rows[1]["fp"], json!(heavy));

    // One entry per query, with the separated phase timings and work.
    let all = log_rows(
        &s,
        &s.a,
        "SELECT event_id, severity, body, attributes FROM system.query_log \
         WHERE attributes['table'] = 'logs' OR attributes['status'] != 'ok' ORDER BY event_time",
        5,
    )
    .await;
    let rows = all["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5, "{all}");
    for r in &rows[..4] {
        let a = &r["attributes"];
        assert_eq!(a["status"], "ok");
        for k in [
            "queue_us",
            "parse_us",
            "plan_us",
            "exec_us",
            "cpu_us",
            "elapsed_us",
            "rows_scanned",
            "result_rows",
        ] {
            assert!(a[k].is_i64(), "{k} missing: {r}");
        }
        assert_eq!(a["rows_scanned"], 300);
    }
    let failed = &rows[4];
    assert_eq!(failed["severity"], "ERROR");
    assert_eq!(failed["attributes"]["status"], "invalid_input");
    assert!(failed["attributes"].get("exec_us").is_none());
    // Literals are not stored: neither the filtered value nor any data.
    let text = all.to_string();
    assert!(!text.contains("svc0") && !text.contains("svc1"), "{text}");
    assert_eq!(
        rows[0]["body"],
        "select count(*) from logs where service = ?"
    );

    // Tenant b sees only its own query (plus the query log reads it makes).
    let b_rows = log_rows(
        &s,
        &s.b,
        "SELECT attributes['table'] AS t FROM system.query_log",
        1,
    )
    .await;
    let b_rows = b_rows["rows"].as_array().unwrap();
    assert!(
        b_rows.iter().filter(|r| r["t"] == "logs").count() == 1,
        "{b_rows:?}"
    );

    // Counters.
    let (st, _, m) = http(&s, "GET", "/api/v1/metrics", &s.ops, "text/plain", "").await;
    assert_eq!(st, 200);
    let recorded: u64 = m
        .lines()
        .find_map(|l| l.strip_prefix("strata_query_log_entries_total "))
        .unwrap()
        .parse()
        .unwrap();
    assert!(recorded >= 8, "{recorded}");
    assert!(m.contains("strata_query_log_dropped_total 0"), "{m}");
}

#[tokio::test(flavor = "multi_thread")]
async fn disabled_query_log_and_restart() {
    let s = start(false, None).await;
    let (st, _, v) = query(&s, &s.a, "SELECT count(*) FROM system.query_log").await;
    assert_eq!(st, 400, "{v}");
    assert!(v["message"].as_str().unwrap().contains("disabled"), "{v}");
    // Unknown tables are rejected by the parser.
    let (st, _, v) = query(&s, &s.a, "SELECT * FROM system.nothing").await;
    assert_eq!(st, 400, "{v}");

    // Entries survive a restart (written and fsynced by the writer).
    let s = start(true, None).await;
    let (st, _, _) = query(&s, &s.a, "SELECT count(*) FROM logs").await;
    assert_eq!(st, 200);
    log_rows(
        &s,
        &s.a,
        "SELECT event_id FROM system.query_log WHERE attributes['table'] = 'logs'",
        1,
    )
    .await;
    // Stop the server (its state holds the query log), then close the log.
    let Srv {
        _dir: dir,
        task,
        log,
        ..
    } = s;
    task.abort();
    let _ = task.await;
    Arc::into_inner(log.unwrap()).unwrap().close().unwrap();
    let s = start(true, Some(dir)).await;
    let v = log_rows(
        &s,
        &s.a,
        "SELECT event_id FROM system.query_log WHERE attributes['table'] = 'logs'",
        1,
    )
    .await;
    assert_eq!(v["rows"].as_array().unwrap().len(), 1, "{v}");
}
