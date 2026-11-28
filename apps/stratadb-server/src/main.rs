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
    // Server lifecycle (spec IX.22.7): STARTING until the config is applied.
    let life = Arc::new(stratadb_server::lifecycle::Lifecycle::default());
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
    if let Some(n) = r.io_max_inflight_per_device {
        stratadb_common::io::configure_max_inflight_per_device(n)?;
    }
    let io = stratadb_common::io::default_io();
    tracing::info!(
        component = "server",
        max_open_files = io.stats().max_open_files,
        max_inflight_per_device = io.stats().max_inflight_per_device,
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
    // Memory budget and regions (spec VIII.5.2, STRATA-SYS-02-029).
    let detected = stratadb_common::memory::detect_memory_limit();
    let mplan = r.memory.plan(detected)?;
    let mem = stratadb_server::memory::ServerMemory::new(mplan.clone(), r.server.quotas.clone());
    let mut eopts = r.engine.clone();
    eopts.memory = Some(Arc::clone(&mem.ingest));
    eopts.cache_memory = Some(Arc::clone(&mem.cache));
    eopts.background_memory = Some(Arc::clone(&mem.background));
    // The decoded column cache lives in the cache region.
    if eopts.column_cache_bytes as u64 > mplan.cache {
        tracing::warn!(
            component = "memory",
            configured = eopts.column_cache_bytes,
            capped = mplan.cache,
            "column_cache_bytes capped to the cache memory region"
        );
        eopts.column_cache_bytes = usize::try_from(mplan.cache).unwrap_or(usize::MAX);
    }
    // Flush well before the ingest region fills (active + frozen memtables).
    let cap = usize::try_from(mplan.ingest / 4)
        .unwrap_or(usize::MAX)
        .max(1 << 20);
    if eopts.memtable_max_bytes > cap {
        tracing::info!(
            component = "memory",
            configured = eopts.memtable_max_bytes,
            capped = cap,
            "memtable_max_bytes capped to a quarter of the ingest memory region"
        );
        eopts.memtable_max_bytes = cap;
    }
    tracing::info!(
        component = "memory",
        budget = mplan.budget,
        source = mplan.source,
        detected = detected.map(|d| d.0),
        ingest = mplan.ingest,
        cache = mplan.cache,
        background = mplan.background,
        query_pool = mplan.query,
        per_query = mplan.per_query,
        "memory budget"
    );
    life.recovering();
    let engine = Arc::new(Engine::open(eopts.clone())?);
    life.warming();
    let query_log = if r.query_log.enabled {
        let q = stratadb_server::query_log::QueryLog::open(&r.data_dir, &eopts, &r.query_log)?;
        tracing::info!(
            component = "query_log",
            dir = %stratadb_server::query_log::dir(&r.data_dir).display(),
            flush_ms = r.query_log.flush_interval.as_millis() as u64,
            retention_days = r.query_log.retention.as_secs() / 86_400,
            "system.query_log enabled"
        );
        Some(Arc::new(q))
    } else {
        None
    };
    let audit = AuditLog::open(&r.data_dir)?;
    audit.record(
        "server_start",
        None,
        json!({"listen": r.listen.to_string(), "tls": tls.is_some()}),
    )?;
    let mut state = AppState::new(Arc::clone(&engine), tokens, r.server.clone())
        .with_audit(AuditLog::open(&r.data_dir)?)
        .with_memory(Arc::clone(&mem))
        .with_lifecycle(Arc::clone(&life));
    if let Some(q) = &query_log {
        state = state.with_query_log(Arc::clone(q));
    }
    let admission = state.admission();
    let app = router(state);
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
    let (life_rt, health_engine) = (Arc::clone(&life), Arc::downgrade(&engine));
    rt.block_on(async move {
        let life = life_rt;
        // RSS vs tracked memory every 10 s (STRATA-SYS-05-009), off the
        // async workers since reading RSS may spawn `ps` outside Linux.
        // The same tick re-evaluates device/table/server health (it reads
        // disk space).
        let sampler = Arc::clone(&mem);
        // A weak reference, so shutdown can still take sole ownership of the
        // engine to close it.
        let health_life = Arc::clone(&life);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let m = Arc::clone(&sampler);
                let (l, e) = (Arc::clone(&health_life), health_engine.clone());
                let _ = tokio::task::spawn_blocking(move || {
                    m.sample();
                    if let Some(e) = e.upgrade() {
                        l.tick(&e, std::time::Instant::now());
                    }
                })
                .await;
            }
        });
        // Admission controller (spec STRATA-SYS-11-005): AIMD on batch
        // slots, pressure readings, demotions. Reads /proc, so it runs off
        // the async workers.
        let interval = admission.settings().controller_interval;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(interval);
            loop {
                tick.tick().await;
                let a = Arc::clone(&admission);
                let _ = tokio::task::spawn_blocking(move || a.tick()).await;
            }
        });
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(|e| Error::io(format!("binding {listen}"), e))?;
        tracing::info!(component = "server", addr = %listen, tls = tls.is_some(), "listening");
        life.ready();
        let signal_life = Arc::clone(&life);
        let on_signal = move || {
            let l = Arc::clone(&signal_life);
            async move {
                shutdown_signal().await;
                l.draining();
            }
        };
        match tls {
            Some(cfg) => {
                let l = stratadb_server::tls::TlsListener::new(listener, cfg)
                    .map_err(|e| Error::io("starting TLS listener", e))?;
                axum::serve(l, app)
                    .with_graceful_shutdown(on_signal())
                    .await
                    .map_err(|e| Error::io("serving", e))
            }
            None => axum::serve(
                axum::serve::ListenerExt::tap_io(listener, |s: &mut tokio::net::TcpStream| {
                    stratadb_server::net::tune(s)
                }),
                app,
            )
            .with_graceful_shutdown(on_signal())
            .await
            .map_err(|e| Error::io("serving", e)),
        }
    })?;
    tracing::info!(component = "server", "shutting down; flushing");
    if let Err(e) = audit.record("server_stop", None, json!({})) {
        tracing::error!(component = "audit", error = %e, "audit record could not be written");
    }
    if let Some(q) = query_log {
        // Writes the buffered entries; a failure loses only query log entries.
        q.shutdown();
        if let Ok(q) = Arc::try_unwrap(q) {
            if let Err(e) = q.close() {
                tracing::warn!(component = "query_log", error = %e, "closing the query log");
            }
        }
    }
    let closed = match Arc::try_unwrap(engine) {
        Ok(e) => e.close(),
        Err(_) => Ok(()), // remaining references drop → WAL synced in Drop; recovery covers the rest
    };
    life.stopped();
    closed
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
