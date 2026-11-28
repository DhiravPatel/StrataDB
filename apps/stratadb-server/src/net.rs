//! Socket options for accepted client connections (spec VIII.6,
//! STRATA-SYS-06-009/012).
//!
//! - **`TCP_NODELAY`:** responses are written whole by the HTTP layer, so
//!   Nagle's algorithm would only add latency.
//! - **TCP keepalive:** after 60 s idle, probes every 10 s, and 6 failed
//!   probes close the connection, so a dead client stops holding its query
//!   resources.

use std::time::Duration;

use tokio::net::TcpStream;

/// Keepalive idle time before the first probe.
pub const KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
/// Interval between probes.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// Unanswered probes before the connection is dropped.
pub const KEEPALIVE_RETRIES: u32 = 6;

/// Apply the client-connection options. Failures are logged, not fatal: the
/// connection still works without them.
pub fn tune(stream: &TcpStream) {
    if let Err(e) = stream.set_nodelay(true) {
        tracing::debug!(component = "net", error = %e, "TCP_NODELAY not set");
    }
    let ka = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL);
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
    let ka = ka.with_retries(KEEPALIVE_RETRIES);
    if let Err(e) = socket2::SockRef::from(stream).set_tcp_keepalive(&ka) {
        tracing::debug!(component = "net", error = %e, "TCP keepalive not set");
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[tokio::test]
    async fn accepted_connections_get_nodelay_and_keepalive() {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let client = tokio::spawn(async move { TcpStream::connect(addr).await.unwrap() });
        let (server, _) = l.accept().await.unwrap();
        tune(&server);
        assert!(server.nodelay().unwrap());
        let s = socket2::SockRef::from(&server);
        assert!(s.keepalive().unwrap());
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            assert_eq!(s.tcp_keepalive_time().unwrap(), KEEPALIVE_IDLE);
            assert_eq!(s.tcp_keepalive_interval().unwrap(), KEEPALIVE_INTERVAL);
            assert_eq!(s.tcp_keepalive_retries().unwrap(), KEEPALIVE_RETRIES);
        }
        drop(client.await.unwrap());
    }
}
