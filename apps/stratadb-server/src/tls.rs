//! TLS for the external listener (spec §18.5, ticket H-003).
//!
//! rustls (memory-safe TLS) with the `ring` provider; TLS 1.2 and 1.3 with
//! rustls' default (modern AEAD-only) cipher suites. Handshakes run in their
//! own tasks with a timeout and a concurrency bound, so slow or malicious
//! clients cannot stall the accept loop.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use stratadb_common::{Error, Result};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Semaphore};
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

pub const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
pub const MAX_CONCURRENT_HANDSHAKES: usize = 256;

/// Load a PEM certificate chain and private key into a server config.
pub fn load_server_config(cert: &Path, key: &Path) -> Result<Arc<rustls::ServerConfig>> {
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(cert)
        .map_err(|e| {
            Error::invalid_input(format!("reading TLS certificate {}: {e}", cert.display()))
        })?
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| {
            Error::invalid_input(format!("parsing TLS certificate {}: {e}", cert.display()))
        })?;
    if certs.is_empty() {
        return Err(Error::invalid_input(format!(
            "no certificates in {}",
            cert.display()
        )));
    }
    let key = PrivateKeyDer::from_pem_file(key).map_err(|e| {
        Error::invalid_input(format!("reading TLS private key {}: {e}", key.display()))
    })?;
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| Error::internal(format!("TLS protocol setup: {e}")))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            Error::invalid_input(format!(
                "TLS certificate/key mismatch or unsupported key: {e}"
            ))
        })?;
    cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(cfg))
}

/// DER bytes of the first (leaf) certificate in a PEM file (diagnostics).
pub fn read_leaf_certificate(cert: &Path) -> Result<Vec<u8>> {
    CertificateDer::pem_file_iter(cert)
        .map_err(|e| {
            Error::invalid_input(format!("reading TLS certificate {}: {e}", cert.display()))
        })?
        .next()
        .ok_or_else(|| Error::invalid_input(format!("no certificates in {}", cert.display())))?
        .map(|c| c.to_vec())
        .map_err(|e| {
            Error::invalid_input(format!("parsing TLS certificate {}: {e}", cert.display()))
        })
}

/// An `axum::serve::Listener` yielding completed TLS connections.
#[derive(Debug)]
pub struct TlsListener {
    rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local: SocketAddr,
}

impl TlsListener {
    pub fn new(tcp: TcpListener, cfg: Arc<rustls::ServerConfig>) -> std::io::Result<Self> {
        let local = tcp.local_addr()?;
        let (tx, rx) = mpsc::channel(64);
        let acceptor = TlsAcceptor::from(cfg);
        let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
        tokio::spawn(async move {
            loop {
                let (stream, addr) = match tcp.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        // e.g. EMFILE: back off instead of spinning.
                        tracing::warn!(component = "tls", error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
                    drop(stream); // shed load: too many handshakes in progress
                    continue;
                };
                let (acceptor, conn_tx) = (acceptor.clone(), tx.clone());
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok(tls)) =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
                    {
                        let _ = conn_tx.send((tls, addr)).await;
                    }
                });
                if tx.is_closed() {
                    return;
                }
            }
        });
        Ok(Self { rx, local })
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.rx.recv().await {
            Some(conn) => conn,
            // The acceptor task only stops when the listener is dropped.
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> std::io::Result<Self::Addr> {
        Ok(self.local)
    }
}
