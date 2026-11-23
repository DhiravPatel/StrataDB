//! SDK conformance against a real StrataDB server, plus retry behaviour
//! against a scripted mock server.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use stratadb_client::{Client, ClientConfig, ClientError};
use stratadb_common::TenantId;
use stratadb_server::auth::{Permission, TokenStore};
use stratadb_server::{router, AppState, ServerConfig};
use stratadb_storage::{Engine, EngineOptions};

struct Server {
    url: String,
    token: String,
    _dir: tempfile::TempDir,
    _rt: tokio::runtime::Runtime,
}

fn server(tls: Option<(std::path::PathBuf, std::path::PathBuf)>) -> Server {
    let dir = tempfile::tempdir().unwrap();
    let mut store = TokenStore::load(dir.path()).unwrap();
    let (_, token) = store
        .create(
            TenantId::new("sdk").unwrap(),
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
    let scheme = if tls.is_some() { "https" } else { "http" };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let addr = rt.block_on(async {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        match tls {
            Some((c, k)) => {
                let cfg = stratadb_server::tls::load_server_config(&c, &k).unwrap();
                let tl = stratadb_server::tls::TlsListener::new(l, cfg).unwrap();
                tokio::spawn(async move { axum::serve(tl, app).await.unwrap() });
            }
            None => {
                tokio::spawn(async move { axum::serve(l, app).await.unwrap() });
            }
        }
        addr
    });
    Server {
        url: format!("{scheme}://localhost:{}", addr.port()),
        token,
        _dir: dir,
        _rt: rt,
    }
}

fn http_server() -> Server {
    server(None)
}

#[test]
fn ingest_query_dedup_and_errors_against_real_server() {
    let s = http_server();
    let c = Client::new(ClientConfig::new(&s.url, &s.token)).unwrap();
    c.health().unwrap();
    let events = vec![
        json!({"service": "api", "severity": "ERROR", "body": "boom", "attributes": {"request_id": "r1"}}),
        json!({"service": "api", "severity": "INFO", "body": "ok"}),
        json!({"ingest_time": 1}), // invalid: reserved field
    ];
    let r = c.ingest(&events).unwrap();
    assert_eq!(
        (r.accepted, r.rejected, r.duplicate, r.attempts),
        (2, 1, false, 1)
    );
    assert_eq!(r.errors[0].index, 2);
    assert_eq!(r.durability, "wal_synced");
    // Explicit retry with the same key is a duplicate.
    let again = c.ingest_with_key(&events, &r.idempotency_key).unwrap();
    assert!(again.duplicate);
    assert_eq!(again.commit_seq, r.commit_seq);
    // Large batch is gzip-compressed transparently.
    let big: Vec<_> = (0..500)
        .map(|i| json!({"body": format!("bulk event {i} {}", "x".repeat(50))}))
        .collect();
    assert_eq!(c.ingest(&big).unwrap().accepted, 500);
    let q = c
        .query("SELECT severity, count(*) AS n FROM logs GROUP BY severity ORDER BY n DESC")
        .unwrap();
    assert_eq!(q.columns, vec!["severity", "n"]);
    // 500 bulk events have no severity (UNSPECIFIED), plus one ERROR and one INFO.
    assert_eq!(q.rows[0]["severity"], "UNSPECIFIED");
    assert_eq!(q.rows[0]["n"], 500);
    let total: u64 = q.rows.iter().map(|r| r["n"].as_u64().unwrap()).sum();
    assert_eq!(total, 502);
    // Server errors are structured and non-retryable ones are not retried.
    match c.query("SELECT * FROM nowhere").unwrap_err() {
        ClientError::Server {
            status: 400,
            kind,
            retryable: false,
            ..
        } => assert_eq!(kind, "invalid_input"),
        e => panic!("unexpected {e:?}"),
    }
    let bad = Client::new(ClientConfig::new(&s.url, "sdb_nope_nope")).unwrap();
    assert!(matches!(
        bad.query("SELECT 1 FROM logs").unwrap_err(),
        ClientError::Server { status: 401, .. }
    ));
    // Config validation and token hygiene.
    assert!(Client::new(ClientConfig::new("ftp://x", "t")).is_err());
    assert!(!format!("{:?}", ClientConfig::new(&s.url, &s.token)).contains(&s.token));
}

#[test]
fn tls_with_custom_ca() {
    let dir = tempfile::tempdir().unwrap();
    let ck = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let (cert, key) = (dir.path().join("c.pem"), dir.path().join("k.pem"));
    std::fs::write(&cert, ck.cert.pem()).unwrap();
    std::fs::write(&key, ck.signing_key.serialize_pem()).unwrap();
    let s = server(Some((cert, key)));
    // Untrusted certificate → transport error (not retried forever).
    let mut cfg = ClientConfig::new(&s.url, &s.token);
    cfg.max_retries = 0;
    assert!(matches!(
        Client::new(cfg).unwrap().health().unwrap_err(),
        ClientError::Transport(_)
    ));
    let mut cfg = ClientConfig::new(&s.url, &s.token);
    cfg.ca_pem = Some(ck.cert.pem().into_bytes());
    let c = Client::new(cfg).unwrap();
    c.health().unwrap();
    assert_eq!(
        c.ingest(&[json!({"body": "over tls"})]).unwrap().accepted,
        1
    );
}

/// Scripted HTTP server: responds with `script[i]` to the i-th request and
/// records each request's Idempotency-Key header.
fn mock(
    script: Vec<(u16, &'static str, &'static str)>,
) -> (String, Arc<Mutex<Vec<Option<String>>>>) {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen2 = Arc::clone(&seen);
    std::thread::spawn(move || {
        for (status, extra, body) in script {
            let Ok((mut s, _)) = l.accept() else { return };
            let mut r = BufReader::new(s.try_clone().unwrap());
            let mut key = None;
            let mut len = 0usize;
            loop {
                let mut line = String::new();
                r.read_line(&mut line).unwrap();
                let l = line.trim_end().to_owned();
                if l.is_empty() {
                    break;
                }
                let lower = l.to_ascii_lowercase();
                if let Some(v) = lower.strip_prefix("idempotency-key: ") {
                    key = Some(v.to_owned());
                }
                if let Some(v) = lower.strip_prefix("content-length: ") {
                    len = v.parse().unwrap();
                }
            }
            let mut b = vec![0u8; len];
            r.read_exact(&mut b).unwrap();
            seen2.lock().unwrap().push(key);
            let resp = format!(
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nConnection: close\r\n{extra}Content-Length: {}\r\n\r\n{body}",
                body.len()
            );
            s.write_all(resp.as_bytes()).unwrap();
        }
    });
    (url, seen)
}

const BUSY: &str = r#"{"error":"resource_exhausted","message":"busy","retryable":true}"#;
const OK: &str = r#"{"accepted":1,"rejected":0,"errors":[],"commit_seq":7,"durability":"wal_synced","duplicate":false}"#;

fn fast(url: &str) -> ClientConfig {
    let mut c = ClientConfig::new(url, "t");
    c.base_backoff = Duration::from_millis(1);
    c.max_backoff = Duration::from_millis(5);
    c
}

#[test]
fn retries_reuse_the_idempotency_key_and_honour_limits() {
    let (url, seen) = mock(vec![
        (503, "Retry-After: 0\r\n", BUSY),
        (429, "", BUSY),
        (200, "", OK),
    ]);
    let r = Client::new(fast(&url))
        .unwrap()
        .ingest(&[json!({"body": "x"})])
        .unwrap();
    assert_eq!((r.attempts, r.commit_seq), (3, 7));
    let keys = seen.lock().unwrap().clone();
    assert_eq!(keys.len(), 3);
    assert!(
        keys[0].is_some() && keys.iter().all(|k| k == &keys[0]),
        "{keys:?}"
    );

    // Non-retryable errors are returned immediately.
    let (url, seen) = mock(vec![(
        400,
        "",
        r#"{"error":"invalid_input","message":"bad","retryable":false}"#,
    )]);
    let err = Client::new(fast(&url))
        .unwrap()
        .ingest(&[json!({})])
        .unwrap_err();
    assert!(matches!(err, ClientError::Server { status: 400, .. }));
    assert_eq!(seen.lock().unwrap().len(), 1);

    // Bounded retries.
    let (url, _) = mock(vec![(503, "", BUSY), (503, "", BUSY), (503, "", BUSY)]);
    let mut cfg = fast(&url);
    cfg.max_retries = 2;
    match Client::new(cfg).unwrap().ingest(&[json!({})]).unwrap_err() {
        ClientError::RetriesExhausted { attempts: 3, .. } => {}
        e => panic!("unexpected {e:?}"),
    }

    // Cancellation stops before the next attempt.
    let mut cfg = fast("http://127.0.0.1:9");
    cfg.cancel = Some(Arc::new(AtomicBool::new(true)));
    assert_eq!(
        Client::new(cfg).unwrap().ingest(&[json!({})]).unwrap_err(),
        ClientError::Cancelled
    );
}
