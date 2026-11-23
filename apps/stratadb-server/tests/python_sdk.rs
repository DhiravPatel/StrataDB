//! Runs the Python SDK conformance suite (sdk/python) against a real server.
//! Skipped (with a notice) when `python3` is not installed.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::sync::Arc;

use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Engine, EngineOptions};

#[tokio::test(flavor = "multi_thread")]
async fn python_sdk_conformance() {
    if std::process::Command::new("python3")
        .arg("--version")
        .output()
        .is_err()
    {
        eprintln!("python3 not found; skipping Python SDK conformance");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let mut store = TokenStore::load(dir.path()).unwrap();
    let (_, token) = store
        .create(
            TenantId::new("py").unwrap(),
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
    let script = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../sdk/python/tests/test_conformance.py"
    );
    let out = tokio::task::spawn_blocking(move || {
        std::process::Command::new("python3")
            .arg("-B")
            .arg(script)
            .env("STRATADB_URL", url)
            .env("STRATADB_TOKEN", token)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    let log = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "Python SDK conformance failed:\n{log}"
    );
    assert!(log.contains("OK") && !log.contains("skipped"), "{log}");
}
