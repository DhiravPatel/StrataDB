//! Server endpoints requested by StrataStudio (docs/studio/STATUS.md):
//! `whoami`, `schema`, `queries`, the backup listing, and `page_size`
//! ignored for aggregate and EXPLAIN queries.

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

#[tokio::test(flavor = "multi_thread")]
async fn studio_endpoints() {
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
    let rw = mk("s", &[Permission::Ingest, Permission::Query]);
    let other = mk("o", &[Permission::Ingest, Permission::Query]);
    let backup = mk("ops", &[Permission::BackupCreate]);
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.wal.durability = Durability::Async;
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let cfg = ServerConfig {
        max_concurrent_queries: 1,
        backup_root: Some(dir.path().join("backups")),
        workload: WorkloadSettings {
            queue_timeout: Duration::from_secs(20),
            ..WorkloadSettings::default()
        },
        ..ServerConfig::default()
    };
    let state = AppState::new(
        Arc::clone(&engine),
        TokenStore::load(dir.path()).unwrap(),
        cfg,
    );
    let adm = state.admission();
    let app = router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // whoami: any valid token, never the secret.
    let (st, _, b) = req(addr, "GET", "/api/v1/whoami", &rw, "").await;
    assert_eq!(st, 200, "{b}");
    let w = j(&b);
    assert_eq!(w["tenant"], "s");
    assert_eq!(w["permissions"], json!(["ingest", "query"]));
    assert!(!b.contains(&rw), "the secret is not echoed");
    assert_eq!(
        req(addr, "GET", "/api/v1/whoami", "sdb_bad", "").await.0,
        401
    );

    // schema: attribute keys from segment metadata, per tenant.
    for (tok, body) in [
        (
            &rw,
            r#"[{"body":"a","attributes":{"region":"eu","user":"u1"}},{"body":"b","attributes":{"region":"us"}}]"#,
        ),
        (&other, r#"{"body":"x","attributes":{"secret_key":"1"}}"#),
    ] {
        assert_eq!(req(addr, "POST", "/api/v1/ingest", tok, body).await.0, 200);
    }
    engine.flush().unwrap();
    let (st, _, b) = req(addr, "GET", "/api/v1/schema", &rw, "").await;
    assert_eq!(st, 200, "{b}");
    let sch = j(&b);
    let logs = &sch["tables"][0];
    assert_eq!(logs["name"], "logs");
    assert!(logs["columns"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["name"] == "event_time"));
    let keys: Vec<(String, u64)> = logs["attribute_keys"]
        .as_array()
        .unwrap()
        .iter()
        .map(|k| {
            (
                k["key"].as_str().unwrap().to_owned(),
                k["rows"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        keys,
        vec![("region".to_owned(), 2), ("user".to_owned(), 2)],
        "{b}"
    );
    assert!(
        !b.contains("secret_key"),
        "another tenant's keys never appear"
    );
    assert_eq!(sch["tables"][1]["enabled"], false);

    // page_size is ignored for aggregates and EXPLAIN.
    let (st, _, b) = req(
        addr,
        "POST",
        "/api/v1/query",
        &rw,
        r#"{"query":"SELECT count(*) FROM logs","page_size":10}"#,
    )
    .await;
    assert_eq!(st, 200, "{b}");
    assert_eq!(
        (j(&b)["paginated"].clone(), j(&b)["next_cursor"].clone()),
        (json!(false), Value::Null)
    );
    let (st, _, b) = req(
        addr,
        "POST",
        "/api/v1/query",
        &rw,
        r#"{"query":"SELECT body FROM logs","page_size":1}"#,
    )
    .await;
    assert_eq!((st, j(&b)["paginated"].clone()), (200, json!(true)));

    // queries: the caller's queued query, with its position.
    let held = adm
        .admit(Ask {
            class: WorkClass::Normal,
            tenant: 9,
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
            r#"{"query":"SELECT count(*) FROM logs","query_id":"q-listed"}"#,
        )
        .await
    });
    let mut listed = Value::Null;
    for _ in 0..200 {
        let (_, _, b) = req(addr, "GET", "/api/v1/queries", &rw, "").await;
        let v = j(&b);
        if !v["queries"].as_array().unwrap().is_empty() {
            listed = v;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let q = &listed["queries"][0];
    assert_eq!(
        (
            q["query_id"].clone(),
            q["state"].clone(),
            q["queue_position"].clone()
        ),
        (json!("q-listed"), json!("queued"), json!(0)),
        "{listed}"
    );
    assert!(q["fingerprint"].as_str().is_some_and(|f| f.len() == 16));
    let (_, _, b) = req(addr, "GET", "/api/v1/queries", &other, "").await;
    assert_eq!(j(&b)["queries"], json!([]), "other tenants see nothing");
    drop(held);
    assert_eq!(waiting.await.unwrap().0, 200);

    // Backups: listed newest first.
    let (st, _, b) = req(addr, "GET", "/api/v1/admin/backup", &backup, "").await;
    assert_eq!((st, j(&b)["backups"].clone()), (200, json!([])), "{b}");
    for name in ["b1", "b2"] {
        let (st, _, b) = req(
            addr,
            "POST",
            "/api/v1/admin/backup",
            &backup,
            &json!({"name": name}).to_string(),
        )
        .await;
        assert_eq!(st, 200, "{b}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let (_, _, b) = req(addr, "GET", "/api/v1/admin/backup", &backup, "").await;
    let l = j(&b);
    assert_eq!(l["enabled"], true);
    let names: Vec<&str> = l["backups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["b2", "b1"]);
    assert!(l["backups"][0]["bytes"].as_u64().unwrap() > 0);
    assert_eq!(
        req(addr, "GET", "/api/v1/admin/backup", &rw, "").await.0,
        403
    );
}
