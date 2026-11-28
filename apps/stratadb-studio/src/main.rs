//! `strata-studio`: web administration and query tool for StrataDB.
//!
//! ```text
//! strata-studio serve [--listen ADDR] [--data-dir DIR] [--config FILE]
//! strata-studio connection add --name N --url URL [--no-tls-verify] [--ca-file PEM]
//! strata-studio connection list | remove --name N | test --name N
//! ```
//! Secrets never go on the command line: the studio password comes from
//! `STRATA_STUDIO_PASSWORD` or the config file, and `connection add` reads
//! the API token from `STRATA_STUDIO_TOKEN` or the first line of stdin.

use std::io::{BufRead, IsTerminal, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};
use stratadb_studio::config::{self, Overrides, StudioConfig};
use stratadb_studio::registry::{Connection, Registry};
use stratadb_studio::upstream::Upstream;

/// Environment variable read by `connection add` for the API token.
const TOKEN_ENV: &str = "STRATA_STUDIO_TOKEN";

#[derive(Parser)]
#[command(
    name = "strata-studio",
    version,
    about = "StrataStudio: web admin and query tool for StrataDB"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Args, Clone)]
struct Common {
    /// Studio TOML configuration file (docs/studio/README.md).
    #[arg(long, env = "STRATA_STUDIO_CONFIG")]
    config: Option<PathBuf>,
    /// Where the connection registry lives (default ~/.strata-studio).
    #[arg(long, env = "STRATA_STUDIO_DATA_DIR")]
    data_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
enum Cmd {
    /// Serve the web UI and API (default 127.0.0.1:7171).
    Serve {
        #[command(flatten)]
        common: Common,
        /// Listen address. A non-loopback address requires studio
        /// authentication (STRATA_STUDIO_PASSWORD or [auth] in the config).
        #[arg(long, env = "STRATA_STUDIO_LISTEN")]
        listen: Option<SocketAddr>,
    },
    /// Manage the StrataDB servers the studio connects to.
    Connection {
        #[command(subcommand)]
        cmd: ConnCmd,
    },
}

#[derive(Subcommand)]
enum ConnCmd {
    /// Add a server. The API token is read from STRATA_STUDIO_TOKEN or stdin.
    Add {
        #[command(flatten)]
        common: Common,
        #[arg(long)]
        name: String,
        /// Base URL, e.g. http://127.0.0.1:7070.
        #[arg(long)]
        url: String,
        /// Do not verify the server's TLS certificate (not recommended).
        #[arg(long)]
        no_tls_verify: bool,
        /// PEM file of CA certificates to trust for this server.
        #[arg(long)]
        ca_file: Option<PathBuf>,
    },
    /// List servers (tokens are never shown).
    List {
        #[command(flatten)]
        common: Common,
        /// Print JSON.
        #[arg(long)]
        json: bool,
    },
    /// Remove a server.
    Remove {
        #[command(flatten)]
        common: Common,
        #[arg(long)]
        name: String,
    },
    /// Check that a server answers and the token works.
    Test {
        #[command(flatten)]
        common: Common,
        #[arg(long)]
        name: String,
    },
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_env("STRATA_STUDIO_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn home() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .filter(|h| !h.is_empty())
        .map(PathBuf::from)
}

fn resolve(
    common: &Common,
    listen: Option<SocketAddr>,
    with_password: bool,
) -> Result<StudioConfig, String> {
    let env_pw = if with_password {
        std::env::var(config::PASSWORD_ENV).ok()
    } else {
        None
    };
    let o = Overrides {
        config: common.config.clone(),
        // Connection commands do not listen: skip the listener policy.
        listen: if with_password {
            listen
        } else {
            Some(SocketAddr::from(([127, 0, 0, 1], 0)))
        },
        data_dir: common.data_dir.clone(),
    };
    config::resolve(&o, env_pw, home().as_deref())
}

fn read_token() -> Result<String, String> {
    if let Ok(t) = std::env::var(TOKEN_ENV) {
        if !t.is_empty() {
            return Ok(t);
        }
    }
    let stdin = std::io::stdin();
    if stdin.is_terminal() {
        eprint!("API token (input is visible; prefer {TOKEN_ENV}): ");
        let _ = std::io::stderr().flush();
    }
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|e| format!("reading the token from stdin: {e}"))?;
    let t = line.trim().to_owned();
    if t.is_empty() {
        return Err(format!("no API token: set {TOKEN_ENV} or pipe it on stdin"));
    }
    Ok(t)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = term => {},
    }
    tracing::info!(component = "studio", "shutting down");
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .max_blocking_threads(1100)
        .enable_all()
        .build()
        .map_err(|e| format!("starting the runtime: {e}"))
}

fn run(cli: Cli) -> Result<ExitCode, String> {
    match cli.cmd {
        Cmd::Serve { common, listen } => {
            let cfg = resolve(&common, listen, true)?;
            runtime()?.block_on(stratadb_studio::serve(cfg, shutdown_signal()))?;
            Ok(ExitCode::SUCCESS)
        }
        Cmd::Connection { cmd } => match cmd {
            ConnCmd::Add {
                common,
                name,
                url,
                no_tls_verify,
                ca_file,
            } => {
                let cfg = resolve(&common, None, false)?;
                let reg = Registry::open(&cfg.data_dir).map_err(|e| e.to_string())?;
                let token = read_token()?;
                let c = Connection::new(&name, &url, &token, !no_tls_verify, ca_file)
                    .map_err(|e| e.to_string())?;
                let v = reg.add(c).map_err(|e| e.to_string())?;
                tracing::info!(target: "audit", action = "connection_add", actor = "cli", connection = %v.name, outcome = 201u16, "studio admin action");
                println!("added {} ({}) to {}", v.name, v.url, reg.path().display());
                Ok(ExitCode::SUCCESS)
            }
            ConnCmd::List { common, json } => {
                let cfg = resolve(&common, None, false)?;
                let reg = Registry::open(&cfg.data_dir).map_err(|e| e.to_string())?;
                let list = reg.list();
                if json {
                    let s = serde_json::to_string_pretty(&list).map_err(|e| e.to_string())?;
                    println!("{s}");
                } else if list.is_empty() {
                    println!("no connections in {}", reg.path().display());
                } else {
                    for c in list {
                        println!(
                            "{}\t{}\ttls_verify={}{}",
                            c.name,
                            c.url,
                            c.tls_verify,
                            c.ca_file
                                .map(|p| format!("\tca_file={}", p.display()))
                                .unwrap_or_default()
                        );
                    }
                }
                Ok(ExitCode::SUCCESS)
            }
            ConnCmd::Remove { common, name } => {
                let cfg = resolve(&common, None, false)?;
                let reg = Registry::open(&cfg.data_dir).map_err(|e| e.to_string())?;
                reg.remove(&name).map_err(|e| e.to_string())?;
                tracing::info!(target: "audit", action = "connection_remove", actor = "cli", connection = %name, outcome = 200u16, "studio admin action");
                println!("removed {name}");
                Ok(ExitCode::SUCCESS)
            }
            ConnCmd::Test { common, name } => {
                let cfg = resolve(&common, None, false)?;
                let reg = Registry::open(&cfg.data_dir).map_err(|e| e.to_string())?;
                let c = reg
                    .get(&name)
                    .ok_or_else(|| format!("no connection named {name:?}"))?;
                let up = Upstream::new(cfg.limits.clone());
                let v = runtime()?.block_on(stratadb_studio::api::probe(&up, &c));
                let s = serde_json::to_string_pretty(&v).map_err(|e| e.to_string())?;
                println!("{s}");
                Ok(if v["ok"] == serde_json::Value::Bool(true) {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                })
            }
        },
    }
}

fn main() -> ExitCode {
    init_logging();
    let cli = Cli::parse();
    match run(cli) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("strata-studio: {e}");
            ExitCode::from(2)
        }
    }
}
