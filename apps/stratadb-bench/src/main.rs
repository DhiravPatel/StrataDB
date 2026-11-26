//! `strata-bench`: StrataDB benchmark harness (spec §27, VIII.19).
//!
//! * `strata-bench [flags]` — the fixed query suite (raw JSON, unchanged).
//! * `strata-bench scenario FILE --out DIR` — a TOML scenario: manifest, raw
//!   samples, summaries with bootstrap CIs, plan assertions, open-loop load.
//! * `strata-bench compare CANDIDATE --baseline DIR` — regression detection
//!   (Mann-Whitney U, p < 0.01, ≥ 3% median change).
//! * `strata-bench ledger [--write]` — operator CPU-cost ledger.
//!
//! Output never contains comparisons with other systems or capacity claims;
//! reports follow docs/benchmarks/methodology.md.

mod compare;
mod ledger;
mod manifest;
mod openloop;
mod scenario;
mod stats;
mod suite;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde_json::json;
use stratadb_common::Error;

#[derive(Parser)]
#[command(
    name = "strata-bench",
    about = "StrataDB benchmark harness",
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,
    #[command(flatten)]
    suite: suite::Args,
}

/// Hardware profile label (spec VIII.27). Laptops and other unclassified
/// machines are HW-REF-7 and are never compared with server results.
const DEFAULT_HW: &str = "HW-REF-7";

#[derive(Subcommand)]
enum Cmd {
    /// Run a TOML scenario; writes manifest.json and results.json to --out.
    Scenario {
        file: PathBuf,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value = DEFAULT_HW)]
        hw_profile: String,
        /// Engine data directory (default: a temporary directory, removed).
        #[arg(long)]
        data_dir: Option<PathBuf>,
    },
    /// Compare a results.json with the rolling baseline of earlier runs.
    Compare {
        candidate: PathBuf,
        #[arg(long)]
        baseline: PathBuf,
        #[arg(long, default_value_t = 10)]
        window: usize,
        #[arg(long, default_value_t = 0.01)]
        alpha: f64,
        #[arg(long, default_value_t = 0.03)]
        min_effect: f64,
        /// Write the JSON report here (default: stdout).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Measure the operator cost ledger; check it (default) or --write it.
    Ledger {
        #[arg(long, default_value = "perf/cpr_ledger.toml")]
        path: PathBuf,
        #[arg(long)]
        write: bool,
        #[arg(long, default_value_t = 20)]
        runs: usize,
        #[arg(long, default_value = DEFAULT_HW)]
        hw_profile: String,
    },
}

/// Physical memory in bytes (Linux `/proc/meminfo`, macOS `sysctl`).
pub fn host_memory() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
        let kb: u64 = s
            .lines()
            .find_map(|l| l.strip_prefix("MemTotal:"))?
            .split_whitespace()
            .next()?
            .parse()
            .ok()?;
        return kb.checked_mul(1024);
    }
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

fn fail(e: &Error) -> ExitCode {
    eprintln!(
        "{}",
        json!({"error": e.kind().as_str(), "message": e.to_string()})
    );
    ExitCode::FAILURE
}

fn emit(text: &str, out: Option<&PathBuf>) -> ExitCode {
    match out {
        Some(p) => match std::fs::write(p, text) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("writing {}: {e}", p.display());
                ExitCode::FAILURE
            }
        },
        None => {
            println!("{text}");
            ExitCode::SUCCESS
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.cmd {
        None => {
            let out = cli.suite.out.clone();
            match suite::run(cli.suite) {
                Ok(j) => emit(
                    &serde_json::to_string_pretty(&j).unwrap_or_default(),
                    out.as_ref(),
                ),
                Err(e) => fail(&e),
            }
        }
        Some(Cmd::Scenario {
            file,
            out,
            hw_profile,
            data_dir,
        }) => {
            let text = match std::fs::read_to_string(&file) {
                Ok(t) => t,
                Err(e) => {
                    return fail(&Error::invalid_input(format!(
                        "reading {}: {e}",
                        file.display()
                    )))
                }
            };
            match scenario::run(&text, &out, &hw_profile, data_dir) {
                Ok(r) => {
                    let v = &r["verdict"];
                    eprintln!(
                        "scenario {}: ok={} failures={} flags={} -> {}",
                        r["scenario"],
                        v["ok"],
                        v["failures"],
                        v["flags"],
                        out.join("results.json").display()
                    );
                    if v["ok"].as_bool() == Some(true) {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::from(2)
                    }
                }
                Err(e) => fail(&e),
            }
        }
        Some(Cmd::Compare {
            candidate,
            baseline,
            window,
            alpha,
            min_effect,
            out,
        }) => {
            let p = compare::Params {
                window,
                alpha,
                min_effect,
            };
            match compare::compare(&candidate, &baseline, p) {
                Ok(r) => {
                    for m in &r.metrics {
                        eprintln!(
                            "{:<11} {:<48} {:>10.3} -> {:>10.3} ({:+6.1}%, p={:.2e})",
                            m.verdict,
                            m.metric,
                            m.baseline_median,
                            m.candidate_median,
                            m.change * 100.0,
                            m.p_value
                        );
                    }
                    let code = emit(
                        &serde_json::to_string_pretty(&r).unwrap_or_default(),
                        out.as_ref(),
                    );
                    if r.regressions > 0 {
                        ExitCode::from(2)
                    } else {
                        code
                    }
                }
                Err(e) => fail(&e),
            }
        }
        Some(Cmd::Ledger {
            path,
            write,
            runs,
            hw_profile,
        }) => match ledger::run(&path, write, runs, &hw_profile) {
            Ok((report, ok)) => {
                println!("{report}");
                if ok {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::from(2)
                }
            }
            Err(e) => fail(&e),
        },
    }
}
