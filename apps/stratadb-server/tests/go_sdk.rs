//! Runs the Go SDK test suite (sdk/go) against a real server.
//! Skipped (with a notice) when `go` is not installed.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;

use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Engine, EngineOptions};

#[tokio::test(flavor = "multi_thread")]
async fn go_sdk_conformance() {
    if std::process::Command::new("go")
        .arg("version")
        .output()
        .is_err()
    {
        eprintln!("go not found; skipping Go SDK conformance");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut store = TokenStore::load(dir.path()).unwrap();
    let (_, token) = store
        .create(
            TenantId::new("go").unwrap(),
            BTreeSet::from([Permission::Ingest, Permission::Query]),
            None,
            None,
        )
        .unwrap();
    let mut o = EngineOptions::new(dir.path().join("data"));
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let app = router(AppState::new(
        engine,
        TokenStore::load(dir.path()).unwrap(),
        ServerConfig::default(),
    ));
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
    let sdk = concat!(env!("CARGO_MANIFEST_DIR"), "/../../sdk/go");
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new("go")
            .args(["test", "-count=1", "-v", "./..."])
            .current_dir(sdk)
            .env("STRATADB_URL", url)
            .env("STRATADB_TOKEN", token)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let log = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "Go SDK tests failed:\n{log}");
    assert!(
        log.contains("--- PASS: TestConformanceAgainstServer"),
        "conformance test did not run:\n{log}"
    );
}
