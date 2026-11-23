//! Interoperability with the official OpenTelemetry Go SDK OTLP/HTTP log
//! exporter (protobuf + gzip + batching), via tools/otlp-interop.
//! Skipped with a notice when Go or the cached modules are unavailable
//! (built offline: GOPROXY=off). CI downloads the modules first.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;

use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Engine, EngineOptions};

#[tokio::test(flavor = "multi_thread")]
async fn official_otel_go_exporter_interop() {
    let tool = concat!(env!("CARGO_MANIFEST_DIR"), "/../../tools/otlp-interop");
    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("otlp-interop");
    let built = std::process::Command::new("go")
        .args(["build", "-o"])
        .arg(&bin)
        .arg(".")
        .current_dir(tool)
        .env("GOPROXY", "off")
        .env("GOFLAGS", "-mod=mod")
        .output();
    match built {
        Ok(o) if o.status.success() => {}
        Ok(o) => {
            eprintln!(
                "skipping OTLP interop: cannot build offline:\n{}",
                String::from_utf8_lossy(&o.stderr)
            );
            return;
        }
        Err(_) => {
            eprintln!("skipping OTLP interop: go not installed");
            return;
        }
    }
    let mut store = TokenStore::load(dir.path()).unwrap();
    let (_, token) = store
        .create(
            TenantId::new("otel").unwrap(),
            BTreeSet::from([Permission::Ingest]),
            None,
            None,
        )
        .unwrap();
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let app = router(AppState::new(
        Arc::clone(&engine),
        TokenStore::load(dir.path()).unwrap(),
        ServerConfig::default(),
    ));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new(bin)
            .args(["-endpoint", &addr, "-token", &token, "-n", "75"])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        out.status.success(),
        "exporter failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let snap = engine.snapshot(&TenantId::new("otel").unwrap());
    let rows = snap.scan_all().unwrap();
    assert_eq!(rows.len(), 75);
    let e = rows
        .iter()
        .map(|(_, _, e)| e)
        .find(|e| e.attributes.get("i") == Some(&stratadb_schema::Value::Int(7)))
        .unwrap();
    assert_eq!(e.service.as_deref(), Some("interop-svc"));
    assert_eq!(e.severity, stratadb_schema::Severity::Error);
    assert_eq!(
        e.body,
        Some(stratadb_schema::Value::from("interop message 7"))
    );
    assert_eq!(
        e.attributes["request_id"],
        stratadb_schema::Value::from("req-7")
    );
    assert_eq!(e.attributes["ok"], stratadb_schema::Value::Bool(false));
    assert_eq!(
        e.attributes["otel.scope.name"],
        stratadb_schema::Value::from("interop.scope")
    );
    assert_eq!(
        e.attributes["otel.event_name"],
        stratadb_schema::Value::from("interop.event")
    );
}
