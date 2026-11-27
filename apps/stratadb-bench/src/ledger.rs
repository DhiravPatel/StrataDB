//! Operator cost ledger (spec VIII.1.8, STRATA-SYS-01-014/015).
//!
//! The spec's ledger records *cycles* per row measured with hardware
//! performance counters on an isolated core. This harness has no access to
//! `perf_event_open` (and the development machine is not Linux), so the
//! ledger records **CPU nanoseconds per unit** from the process CPU-time
//! clock instead, and says so in every entry. The regression rule is the
//! spec's: an entry regresses when its median exceeds the recorded value by
//! more than 5% beyond the noise band established over the baseline runs
//! (default 20). Entries from another hardware profile are not compared.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use stratadb_common::cpu::CpuTimer;
use stratadb_common::{Error, Result, TenantId};
use stratadb_query::{execute, QueryOptions};
use stratadb_schema::codec::{decode_batch, encode_batch, DecodeLimits};
use stratadb_storage::{Durability, Engine, EngineOptions};
use stratadb_testkit::{Generator, Profile};

use crate::stats::{quantile, summarize};

/// Allowed regression beyond the noise band (STRATA-SYS-01-015).
pub const REGRESSION_TOLERANCE: f64 = 0.05;

#[derive(Debug, Clone, PartialEq)]
pub struct Measured {
    pub name: &'static str,
    pub unit: &'static str,
    pub description: &'static str,
    /// Median over runs.
    pub median: f64,
    /// Relative half-width of the 10–90% range of the runs.
    pub noise_band: f64,
    pub runs: usize,
}

struct Op {
    name: &'static str,
    unit: &'static str,
    description: &'static str,
    /// Returns (CPU nanoseconds, units of work) for one run.
    run: Box<dyn Fn() -> Result<(f64, f64)>>,
}

const EVENTS: usize = 100_000;

/// Minimum CPU time per ledger sample (50 ms).
const MIN_SAMPLE_CPU_NS: f64 = 50e6;

fn ops(
    engine: Arc<Engine>,
    tenant: TenantId,
    batch: Arc<Vec<u8>>,
    sample: Arc<Vec<stratadb_schema::Event>>,
) -> Vec<Op> {
    let columnar = Arc::new(stratadb_schema::columnar::encode_batch(&sample).unwrap_or_default());
    let q = |sql: &'static str| {
        let (engine, tenant) = (Arc::clone(&engine), tenant.clone());
        Box::new(move || {
            let opts = QueryOptions {
                parallelism: 1,
                ..QueryOptions::default()
            };
            let snap = engine.snapshot(&tenant);
            let t = CpuTimer::start();
            let out = execute(&snap, sql, &opts)?;
            let s = t.stop();
            Ok((
                s.process_cpu.as_secs_f64() * 1e9,
                out.stats.rows_scanned.max(1) as f64,
            ))
        }) as Box<dyn Fn() -> Result<(f64, f64)>>
    };
    let crc_buf: Vec<u8> = (0..1 << 20)
        .map(|i: u32| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
        .collect();
    vec![
        Op {
            name: "crc32c",
            unit: "cpu_ns_per_kib",
            description: "CRC-32C over 1 MiB (WAL frames, segment chunks)",
            run: Box::new(move || {
                let t = CpuTimer::start();
                let mut acc = 0u32;
                for _ in 0..16 {
                    acc ^= stratadb_common::checksum::crc32c(std::hint::black_box(&crc_buf));
                }
                std::hint::black_box(acc);
                Ok((t.stop().thread_cpu.as_secs_f64() * 1e9, 16.0 * 1024.0))
            }),
        },
        Op {
            name: "event_batch_decode",
            unit: "cpu_ns_per_row",
            description: "decode a 1,000-event WAL batch payload (WAL replay path)",
            run: Box::new(move || {
                let lim = DecodeLimits::default();
                let t = CpuTimer::start();
                let mut rows = 0usize;
                for _ in 0..10 {
                    rows += decode_batch(std::hint::black_box(&batch), &lim)?.len();
                }
                Ok((t.stop().thread_cpu.as_secs_f64() * 1e9, rows as f64))
            }),
        },
        Op {
            name: "columnar_batch_decode",
            unit: "cpu_ns_per_row",
            description: "decode a 1,000-event columnar WAL payload (WAL v3 replay path, LZ4)",
            run: Box::new(move || {
                let lim = DecodeLimits::default();
                let t = CpuTimer::start();
                let mut rows = 0usize;
                for _ in 0..10 {
                    rows += stratadb_schema::columnar::decode_batch(std::hint::black_box(&columnar), &lim)?
                        .len();
                }
                Ok((t.stop().thread_cpu.as_secs_f64() * 1e9, rows as f64))
            }),
        },
        Op {
            name: "event_batch_encode",
            unit: "cpu_ns_per_row",
            description: "encode 1,000 events as a row WAL payload (WAL v2 write path)",
            run: {
                let sample = Arc::clone(&sample);
                Box::new(move || {
                    let t = CpuTimer::start();
                    let mut bytes = 0usize;
                    for _ in 0..10 {
                        bytes += encode_batch(std::hint::black_box(&sample)).len();
                    }
                    std::hint::black_box(bytes);
                    Ok((t.stop().thread_cpu.as_secs_f64() * 1e9, 10.0 * sample.len() as f64))
                })
            },
        },
        Op {
            name: "columnar_batch_encode",
            unit: "cpu_ns_per_row",
            description: "encode 1,000 events as a columnar WAL payload (WAL v3 write path, LZ4)",
            run: {
                let sample = Arc::clone(&sample);
                Box::new(move || {
                    let t = CpuTimer::start();
                    let mut bytes = 0usize;
                    for _ in 0..10 {
                        bytes += stratadb_schema::columnar::encode_batch(std::hint::black_box(&sample))?.len();
                    }
                    std::hint::black_box(bytes);
                    Ok((t.stop().thread_cpu.as_secs_f64() * 1e9, 10.0 * sample.len() as f64))
                })
            },
        },
        Op {
            name: "scan_filter_count",
            unit: "cpu_ns_per_row_scanned",
            description: "SELECT count(*) WHERE severity = 'ERROR' (1 thread)",
            run: q("SELECT count(*) FROM logs WHERE severity = 'ERROR'"),
        },
        Op {
            name: "like_substring_count",
            unit: "cpu_ns_per_row_scanned",
            description: "SELECT count(*) WHERE body LIKE '%TIMEOUT%' (1 thread)",
            run: q("SELECT count(*) FROM logs WHERE body LIKE '%TIMEOUT%'"),
        },
        Op {
            name: "group_by_high_cardinality",
            unit: "cpu_ns_per_row_scanned",
            description: "GROUP BY attributes['user_id'] ORDER BY count DESC LIMIT 10 (1 thread)",
            run: q("SELECT attributes['user_id'] AS u, count(*) AS n FROM logs GROUP BY attributes['user_id'] ORDER BY n DESC LIMIT 10"),
        },
        Op {
            name: "group_by_low_cardinality",
            unit: "cpu_ns_per_row_scanned",
            description: "GROUP BY severity (1 thread)",
            run: q("SELECT severity, count(*) FROM logs GROUP BY severity"),
        },
        Op {
            name: "top_k_order_by_time",
            unit: "cpu_ns_per_row_scanned",
            description: "ORDER BY event_time DESC LIMIT 10 (1 thread)",
            run: q("SELECT event_time, service FROM logs ORDER BY event_time DESC LIMIT 10"),
        },
    ]
}

/// Measure every ledger operator `runs` times (after one warmup run).
pub fn measure(runs: usize) -> Result<Vec<Measured>> {
    let runs = runs.max(5);
    let dir = tempfile_dir()?;
    let mut opts = EngineOptions::new(&dir);
    opts.wal.durability = Durability::Async;
    opts.background = false;
    let engine = Arc::new(Engine::open(opts)?);
    let mut gen = Generator::new(Profile::ALL[0], 1, 1);
    let tenant = gen.tenants()[0].clone();
    let mut sample = Vec::new();
    let mut written = 0;
    while written < EVENTS {
        let evs = gen.batch(1000);
        if sample.is_empty() {
            sample = evs.clone();
        }
        written += evs.len();
        engine.write_batch(&tenant, evs)?;
    }
    engine.flush()?;
    let batch = Arc::new(encode_batch(&sample));
    let mut out = Vec::new();
    for op in ops(Arc::clone(&engine), tenant, batch, Arc::new(sample.clone())) {
        (op.run)()?; // warmup
        let mut per = Vec::with_capacity(runs);
        for _ in 0..runs {
            // One sample = repetitions until ≥ MIN_SAMPLE_CPU_NS of CPU, so
            // clock granularity and scheduling noise stay small relative to it.
            let (mut ns, mut units, mut reps) = (0.0, 0.0, 0);
            while ns < MIN_SAMPLE_CPU_NS && reps < 10_000 {
                let (n, u) = (op.run)()?;
                ns += n;
                units += u;
                reps += 1;
            }
            per.push(ns / units);
        }
        let s = summarize(&per);
        let mut v = per.clone();
        v.sort_by(f64::total_cmp);
        let band = (quantile(&v, 0.9) - quantile(&v, 0.1)) / 2.0 / s.median.max(1e-12);
        out.push(Measured {
            name: op.name,
            unit: op.unit,
            description: op.description,
            median: s.median,
            noise_band: band,
            runs,
        });
    }
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
    Ok(out)
}

fn tempfile_dir() -> Result<std::path::PathBuf> {
    let d = crate::scenario::unique_temp("strata-ledger");
    std::fs::create_dir_all(&d).map_err(|e| Error::invalid_input(e.to_string()))?;
    Ok(d)
}

/// The ledger file text for `m` measured on `hw_profile`.
pub fn render(m: &[Measured], hw_profile: &str, cpu_model: &str, load: &str) -> String {
    let mut s = String::from(
        "# Operator cost ledger (spec VIII.1.8). Generated by `strata-bench ledger --write`.\n\
         #\n\
         # UNITS: CPU nanoseconds per unit of work from the process CPU-time clock —\n\
         # NOT cycles per row. STRATA-SYS-01-014 requires hardware counters\n\
         # (perf_event_open) on an isolated core of a HW-REF-1 runner; this ledger is\n\
         # the CPU-time stand-in until one exists (ADR-0026). Values are only\n\
         # comparable on the same hardware profile and CPU model.\n\
         #\n\
         # Regression rule (STRATA-SYS-01-015): `strata-bench ledger --check` fails when an\n\
         # entry's median exceeds `measured * (1 + 0.05 + noise_band)`. On shared,\n\
         # frequency-scaled machines (HW-REF-7) session-to-session drift exceeds\n\
         # that limit, so the check is informational there and not run in CI\n\
         # (docs/benchmarks/harness.md).\n\n",
    );
    s.push_str(&format!(
        "[meta]\nhw_profile = {hw_profile:?}\ncpu_model = {cpu_model:?}\nload_average_at_write = {load:?}\nruns = {}\nunit_kind = \"cpu_ns\"\n",
        m.first().map_or(0, |x| x.runs)
    ));
    for x in m {
        s.push_str(&format!(
            "\n[operator.{}]\nunit = {:?}\ndescription = {:?}\nmeasured = {:.4}\nnoise_band = {:.4}\n",
            x.name, x.unit, x.description, x.median, x.noise_band
        ));
    }
    s
}

#[derive(Debug, Deserialize)]
struct LedgerFile {
    meta: Meta,
    operator: BTreeMap<String, Entry>,
}

#[derive(Debug, Deserialize)]
struct Meta {
    hw_profile: String,
    cpu_model: String,
}

#[derive(Debug, Deserialize)]
struct Entry {
    measured: f64,
    noise_band: f64,
}

/// Outcome of checking measurements against a ledger file.
#[derive(Debug, PartialEq)]
pub enum CheckOutcome {
    /// Different hardware: values are not comparable (reported, not failed).
    NotComparable(String),
    Checked {
        regressions: Vec<String>,
        report: Vec<String>,
    },
}

pub fn check(
    ledger_text: &str,
    m: &[Measured],
    hw_profile: &str,
    cpu_model: &str,
) -> Result<CheckOutcome> {
    let l: LedgerFile =
        toml::from_str(ledger_text).map_err(|e| Error::invalid_input(format!("ledger: {e}")))?;
    if l.meta.hw_profile != hw_profile || l.meta.cpu_model != cpu_model {
        return Ok(CheckOutcome::NotComparable(format!(
            "ledger was recorded on {} ({}), this is {hw_profile} ({cpu_model})",
            l.meta.hw_profile, l.meta.cpu_model
        )));
    }
    let mut regressions = Vec::new();
    let mut report = Vec::new();
    for x in m {
        let Some(e) = l.operator.get(x.name) else {
            report.push(format!(
                "{}: not in ledger (new operator; run --write)",
                x.name
            ));
            continue;
        };
        let limit = e.measured * (1.0 + REGRESSION_TOLERANCE + e.noise_band);
        let change = x.median / e.measured - 1.0;
        let line = format!(
            "{}: {:.3} vs ledger {:.3} ({:+.1}%, limit {:.3})",
            x.name,
            x.median,
            e.measured,
            change * 100.0,
            limit
        );
        if x.median > limit {
            regressions.push(line.clone());
        }
        report.push(line);
    }
    Ok(CheckOutcome::Checked {
        regressions,
        report,
    })
}

/// `strata-bench ledger`: measure; then write or check `path`.
pub fn run(path: &Path, write: bool, runs: usize, hw_profile: &str) -> Result<(String, bool)> {
    let cpu_model = crate::manifest::cpu_model().unwrap_or_else(|| "unknown".to_owned());
    let m = measure(runs)?;
    if write {
        let load = crate::manifest::load_average().unwrap_or_else(|| "unknown".to_owned());
        std::fs::write(path, render(&m, hw_profile, &cpu_model, &load))
            .map_err(|e| Error::invalid_input(format!("writing {}: {e}", path.display())))?;
        return Ok((
            format!("wrote {} entries to {}", m.len(), path.display()),
            true,
        ));
    }
    let text = std::fs::read_to_string(path)
        .map_err(|e| Error::invalid_input(format!("reading {}: {e}", path.display())))?;
    Ok(match check(&text, &m, hw_profile, &cpu_model)? {
        CheckOutcome::NotComparable(why) => (format!("not comparable: {why}"), true),
        CheckOutcome::Checked {
            regressions,
            report,
        } => {
            let ok = regressions.is_empty();
            let mut s = report.join("\n");
            if !ok {
                s.push_str(&format!("\nREGRESSIONS:\n{}", regressions.join("\n")));
            }
            (s, ok)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(name: &'static str, median: f64) -> Measured {
        Measured {
            name,
            unit: "cpu_ns_per_row",
            description: "d",
            median,
            noise_band: 0.02,
            runs: 20,
        }
    }

    #[test]
    fn render_parse_and_regression_rule() {
        let text = render(&[m("a", 10.0), m("b", 4.0)], "HW-REF-7", "cpu x", "1.0");
        assert!(text.contains("NOT cycles per row"));
        // Within 5% + noise (7%): ok. Beyond: regression. Faster: ok.
        let cur = [m("a", 10.69), m("b", 4.29), m("c", 1.0)];
        match check(&text, &cur, "HW-REF-7", "cpu x").unwrap() {
            CheckOutcome::Checked {
                regressions,
                report,
            } => {
                assert_eq!(regressions.len(), 1, "{report:?}");
                assert!(regressions[0].starts_with("b:"));
                assert!(report.iter().any(|r| r.contains("c: not in ledger")));
            }
            o => panic!("{o:?}"),
        }
        // Other hardware: not compared.
        assert!(matches!(
            check(&text, &cur, "HW-REF-1", "cpu x").unwrap(),
            CheckOutcome::NotComparable(_)
        ));
        assert!(check("not toml [", &cur, "HW-REF-7", "cpu x").is_err());
    }

    #[test]
    fn measures_every_operator() {
        let got = measure(5).unwrap();
        let names: Vec<&str> = got.iter().map(|x| x.name).collect();
        assert_eq!(names.len(), 10);
        for op in [
            "group_by_high_cardinality",
            "event_batch_encode",
            "columnar_batch_encode",
            "columnar_batch_decode",
        ] {
            assert!(names.contains(&op), "{op} missing from {names:?}");
        }
        for x in &got {
            assert!(x.median > 0.0 && x.median.is_finite(), "{x:?}");
            assert!(x.noise_band >= 0.0);
        }
    }
}
