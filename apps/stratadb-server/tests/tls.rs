//! TLS listener: real handshake with a generated certificate.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::sync::Arc;

use stratadb_server::auth::TokenStore;
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Engine, EngineOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test(flavor = "multi_thread")]
async fn serves_https_and_rejects_plaintext() {
    let dir = tempfile::tempdir().unwrap();
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let (cert, key) = (dir.path().join("c.pem"), dir.path().join("k.pem"));
    std::fs::write(&cert, ck.cert.pem()).unwrap();
    std::fs::write(&key, ck.signing_key.serialize_pem()).unwrap();
    let cfg = stratadb_server::tls::load_server_config(&cert, &key).unwrap();

    let mut o = EngineOptions::new(dir.path().join("data"));
    o.background = false;
    let engine = Arc::new(Engine::open(o).unwrap());
    let app = router(AppState::new(
        engine,
        TokenStore::load(dir.path()).unwrap(),
        ServerConfig::default(),
    ));
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = tcp.local_addr().unwrap();
    let l = stratadb_server::tls::TlsListener::new(tcp, cfg).unwrap();
    tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

    // TLS client trusting only the generated certificate.
    let mut roots = rustls::RootCertStore::empty();
    roots.add(ck.cert.der().clone()).unwrap();
    let ccfg = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(ccfg));
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut tls = connector.connect(name, tcp).await.unwrap();
    tls.write_all(b"GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = tls.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("\"status\":\"ok\""));

    // Plain HTTP to the TLS port gets no HTTP response.
    let mut plain = tokio::net::TcpStream::connect(addr).await.unwrap();
    plain
        .write_all(b"GET /api/v1/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        plain.read_to_end(&mut buf),
    )
    .await;
    assert!(!String::from_utf8_lossy(&buf).contains("HTTP/1.1 200"));

    // Mismatched key/cert is rejected at startup.
    let other = rcgen::generate_simple_self_signed(vec!["x".to_owned()]).unwrap();
    std::fs::write(&key, other.signing_key.serialize_pem()).unwrap();
    assert!(stratadb_server::tls::load_server_config(&cert, &key).is_err());
}
