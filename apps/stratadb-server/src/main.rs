//! `strata-server`: run the HTTP API, manage API tokens, validate configuration.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use serde_json::json;
use stratadb_common::{time, Error, Result, TenantId};
use stratadb_server::audit::AuditLog;
use stratadb_server::auth::TokenStore;
use stratadb_server::config::{resolve, FileConfig, Overrides, Resolved};
use stratadb_server::{parse_permissions, router, AppState};
use stratadb_storage::Engine;

#[derive(Parser)]
#[command(name = "strata-server", version, about = "StrataDB server")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

/// Settings shared by `serve` and `config`. Precedence: flag > env > file > default.
#[derive(Args, Clone)]
struct ServeOpts {
    /// TOML configuration file (see docs/operations/configuration.md).
    #[arg(long, env = "STRATADB_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, env = "STRATADB_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Default 127.0.0.1:7070.
    #[arg(long, env = "STRATADB_LISTEN")]
    listen: Option<SocketAddr>,
    /// async | sync | group_commit (default group_commit).
    #[arg(long, env = "STRATADB_DURABILITY")]
    durability: Option<String>,
    /// Allow a non-loopback address without TLS (not recommended).
    #[arg(long)]
    insecure_allow_remote_without_tls: bool,
    #[arg(long)]
    max_body_bytes: Option<usize>,
    /// Enable `POST /api/v1/admin/backup`, writing named backups here.
    #[arg(long, env = "STRATADB_BACKUP_DIR")]
    backup_dir: Option<PathBuf>,
    /// Default retention for tenants without a policy (days; omit = keep forever).
    #[arg(long)]
    default_retention_days: Option<u64>,
    /// PEM certificate chain; enables TLS (requires --tls-key).
    #[arg(long, env = "STRATADB_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for --tls-cert.
    #[arg(long, env = "STRATADB_TLS_KEY")]
    tls_key: Option<PathBuf>,
    /// Key-encryption key file: enables encryption at rest (ADR-0024).
    #[arg(long, env = "STRATADB_KEY_FILE")]
    key_file: Option<PathBuf>,
}

impl ServeOpts {
    fn resolve(&self) -> Result<Resolved> {
        let file = match &self.config {
            Some(p) => FileConfig::load(p)?,
            None => FileConfig::default(),
        };
        resolve(
            &file,
            &Overrides {
                data_dir: self.data_dir.clone(),
                listen: self.listen,
                durability: self.durability.clone(),
                tls_cert: self.tls_cert.clone(),
                tls_key: self.tls_key.clone(),
                max_body_bytes: self.max_body_bytes,
                backup_dir: self.backup_dir.clone(),
                default_retention_days: self.default_retention_days,
                insecure_allow_remote_without_tls: self.insecure_allow_remote_without_tls,
                key_file: self.key_file.clone(),
            },
        )
    }
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the HTTP API.
    Serve(ServeOpts),
    /// Validate configuration and print the effective (secret-free) settings.
    Config(ServeOpts),
    /// Exit 0 if the server at --addr answers GET /api/v1/health (plain HTTP)
    /// or, with --tcp-only, accepts TCP connections (use for TLS listeners).
    Healthcheck {
        #[arg(long, default_value = "127.0.0.1:7070")]
        addr: SocketAddr,
        #[arg(long)]
        tcp_only: bool,
    },
    /// Manage API tokens (server restart required to pick up changes).
    Token {
        #[command(subcommand)]
        cmd: TokenCmd,
    },
    /// Verify the audit log hash chain.
    AuditVerify {
        #[arg(long)]
        data_dir: PathBuf,
    },
}

#[derive(Subcommand)]
enum TokenCmd {
    /// Create a token; the plaintext is printed once and never stored.
    Create {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        tenant: String,
        /// Comma-separated, e.g. `ingest,query`.
        #[arg(long)]
        permissions: String,
        #[arg(long)]
        ttl_days: Option<u64>,
        #[arg(long)]
        description: Option<String>,
    },
    List {
        #[arg(long)]
        data_dir: PathBuf,
    },
    Revoke {
        #[arg(long)]
        data_dir: PathBuf,
        #[arg(long)]
        id: String,
    },
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(t) => t,
                Err(_) => {
                    let _ = ctrl_c.await;
                    return;
                }
            };
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    let _ = ctrl_c.await;
}

/// JSON logs on stderr; level via `STRATADB_LOG` (e.g. `info`, `debug`,
/// `stratadb_storage=debug,access=warn`). Default `info`.
fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_env("STRATADB_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_target(true)
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn serve(r: Resolved) -> Result<()> {
    let tls = match &r.tls {
        Some(t) => Some(stratadb_server::tls::load_server_config(&t.cert, &t.key)?),
        None => None,
    };
    let tokens = TokenStore::load(&r.data_dir)?;
    if tokens.list().iter().all(|t| t.revoked) {
        tracing::warn!(
            component = "auth",
            "no active API tokens; create one with `strata-server token create`"
        );
    }
    // Thread plan (STRATA-SYS-03-014/015): every engine thread is named
    // `sx-*` and registered in the inventory (GET /api/v1/admin/threads).
    let _main_thread = stratadb_common::threads::register(stratadb_common::threads::Role::Main);
    let plan = r.threads;
    // Host facts at startup (spec STRATA-SYS-02-003, partial); the full
    // readiness check is `strata doctor host`.
    let read = |p: &str| std::fs::read_to_string(p).ok().map(|s| s.trim().to_owned());
    tracing::info!(
        component = "server",
        os = std::env::consts::OS,
        arch = std::env::consts::ARCH,
        kernel = read("/proc/sys/kernel/osrelease").unwrap_or_else(|| "unknown".into()),
        logical_cpus = stratadb_common::threads::logical_cpus(),
        thp =
            read("/sys/kernel/mm/transparent_hugepage/enabled").unwrap_or_else(|| "unknown".into()),
        clocksource = read("/sys/devices/system/clocksource/clocksource0/current_clocksource")
            .unwrap_or_else(|| "unknown".into()),
        "host (run `strata doctor host` for the readiness report)"
    );
    // File descriptors (STRATA-SYS-02-036/037): raise the soft limit to the
    // hard limit, then size the segment file cache (half of it by default).
    match stratadb_common::io::raise_nofile_limit() {
        Ok((old, new)) if new > old => {
            tracing::info!(
                component = "server",
                old,
                new,
                "raised RLIMIT_NOFILE soft limit"
            )
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(component = "server", error = %e, "could not raise RLIMIT_NOFILE"),
    }
    if let Some(n) = r.max_open_files {
        stratadb_common::io::configure_max_open_files(n)?;
    }
    let io = stratadb_common::io::default_io();
    tracing::info!(
        component = "server",
        max_open_files = io.stats().max_open_files,
        backend = io.name(),
        "storage I/O engine"
    );
    stratadb_query::configure_scan_pool(plan.scan)?;
    tracing::info!(
        component = "server",
        logical_cpus = plan.logical_cpus,
        budget = plan.budget,
        net = plan.net,
        scan = plan.scan,
        blocking = plan.blocking,
        total = plan.total(),
        "thread plan"
    );
    let pool = stratadb_query::scan_pool();
    tracing::info!(
        component = "query",
        scan_threads = pool.workers(),
        "scan worker pool started"
    );
    let engine = Arc::new(Engine::open(r.engine.clone())?);
    let audit = AuditLog::open(&r.data_dir)?;
    audit.record(
        "server_start",
        None,
        json!({"listen": r.listen.to_string(), "tls": tls.is_some()}),
    )?;
    let app = router(
        AppState::new(Arc::clone(&engine), tokens, r.server.clone())
            .with_audit(AuditLog::open(&r.data_dir)?),
    );
    let net_id = std::sync::atomic::AtomicUsize::new(0);
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(plan.net)
        // StrataDB runs blocking work on its own sx-blk pool; tokio's
        // blocking pool is only a small reserve (counted in the plan).
        .max_blocking_threads(stratadb_server::TOKIO_BLOCKING_THREADS)
        .thread_name_fn(move || {
            let n = net_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            stratadb_common::threads::Role::Net.thread_name(n)
        })
        .on_thread_start(|| {
            stratadb_common::threads::register_current(stratadb_common::threads::Role::Net)
        })
        .on_thread_park(stratadb_common::threads::update_current_cpu)
        .build()
        .map_err(|e| Error::io("starting runtime", e))?;
    let listen = r.listen;
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(|e| Error::io(format!("binding {listen}"), e))?;
        tracing::info!(component = "server", addr = %listen, tls = tls.is_some(), "listening");
        match tls {
            Some(cfg) => {
                let l = stratadb_server::tls::TlsListener::new(listener, cfg)
                    .map_err(|e| Error::io("starting TLS listener", e))?;
                axum::serve(l, app)
                    .with_graceful_shutdown(shutdown_signal())
                    .await
                    .map_err(|e| Error::io("serving", e))
            }
            None => axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .map_err(|e| Error::io("serving", e)),
        }
    })?;
    tracing::info!(component = "server", "shutting down; flushing");
    if let Err(e) = audit.record("server_stop", None, json!({})) {
        tracing::error!(component = "audit", error = %e, "audit record could not be written");
    }
    match Arc::try_unwrap(engine) {
        Ok(e) => e.close(),
        Err(_) => Ok(()), // remaining references drop → WAL synced in Drop; recovery covers the rest
    }
}

fn healthcheck(addr: SocketAddr, tcp_only: bool) -> Result<()> {
    let mut s = TcpStream::connect_timeout(&addr, Duration::from_secs(3))
        .map_err(|e| Error::unavailable(format!("cannot connect to {addr}: {e}")))?;
    if tcp_only {
        return Ok(());
    }
    s.set_read_timeout(Some(Duration::from_secs(3)))
        .map_err(|e| Error::io("socket", e))?;
    s.write_all(b"GET /api/v1/health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .map_err(|e| Error::io("sending healthcheck", e))?;
    let mut buf = String::new();
    let _ = s.read_to_string(&mut buf);
    if buf.starts_with("HTTP/1.1 200") {
        Ok(())
    } else {
        Err(Error::unavailable(format!(
            "unhealthy response: {}",
            buf.lines().next().unwrap_or("<none>")
        )))
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.cmd {
        Cmd::Serve(o) => {
            init_logging();
            serve(o.resolve()?)
        }
        Cmd::Config(o) => {
            let r = o.resolve()?;
            println!(
                "{}",
                serde_json::to_string_pretty(&r.display()).unwrap_or_default()
            );
            Ok(())
        }
        Cmd::Healthcheck { addr, tcp_only } => healthcheck(addr, tcp_only),
        Cmd::Token {
            cmd:
                TokenCmd::Create {
                    data_dir,
                    tenant,
                    permissions,
                    ttl_days,
                    description,
                },
        } => {
            let mut store = TokenStore::load(&data_dir)?;
            let expires = ttl_days
                .map(|d| {
                    i64::try_from(d)
                        .ok()
                        .and_then(|d| d.checked_mul(86_400_000))
                })
                .map(|ms| ms.map(|ms| time::now_unix_millis().saturating_add(ms)))
                .map(|v| v.ok_or_else(|| Error::invalid_input("ttl-days too large")))
                .transpose()?;
            let (rec, token) = store.create(
                TenantId::new(tenant)?,
                parse_permissions(&permissions)?,
                expires,
                description,
            )?;
            AuditLog::open(&data_dir)?.record(
                "token_create",
                Some((rec.id.as_str(), rec.tenant.as_str())),
                json!({"permissions": rec.permissions.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
                       "expires_ms": rec.expires_ms, "source": "cli"}),
            )?;
            println!(
                "{}",
                json!({"id": rec.id, "tenant": rec.tenant.as_str(),
                "permissions": rec.permissions.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
                "expires_ms": rec.expires_ms, "token": token,
                "note": "store this token now; it cannot be shown again"})
            );
            Ok(())
        }
        Cmd::Token {
            cmd: TokenCmd::List { data_dir },
        } => {
            for r in TokenStore::load(&data_dir)?.list() {
                println!(
                    "{}",
                    json!({"id": r.id, "tenant": r.tenant.as_str(),
                    "permissions": r.permissions.iter().map(|p| p.as_str()).collect::<Vec<_>>(),
                    "revoked": r.revoked, "expires_ms": r.expires_ms, "description": r.description})
                );
            }
            Ok(())
        }
        Cmd::Token {
            cmd: TokenCmd::Revoke { data_dir, id },
        } => {
            TokenStore::load(&data_dir)?.revoke(&id)?;
            AuditLog::open(&data_dir)?.record(
                "token_revoke",
                None,
                json!({"id": id, "source": "cli"}),
            )?;
            println!("{}", json!({"revoked": id}));
            Ok(())
        }
        Cmd::AuditVerify { data_dir } => {
            let n = stratadb_server::audit::verify(&data_dir)?;
            println!("{}", json!({"result": "ok", "records": n}));
            Ok(())
        }
    }
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!(
                "{}",
                json!({"error": e.kind().as_str(), "message": e.to_string()})
            );
            ExitCode::FAILURE
        }
    }
}
