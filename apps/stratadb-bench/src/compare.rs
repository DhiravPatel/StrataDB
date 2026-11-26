//! Regression detection (spec VIII.19, STRATA-SYS-19-005).
//!
//! A candidate run is compared against a rolling baseline: the last `window`
//! runs of the same scenario on the same runner (`runner_fingerprint`) with
//! the same durability mode, excluding runs flagged `CLIENT_BOUND`. For
//! every metric the candidate's raw samples are tested against the pooled
//! baseline samples with a two-sided Mann-Whitney U test; a change is
//! reported only when it is statistically significant (`p < alpha`) *and*
//! the median moved by at least `min_effect` — so neither noise nor tiny
//! real changes raise alerts.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value as J;
use stratadb_common::{Error, Result};

use crate::stats::{mann_whitney, quantile};

#[derive(Debug, Clone, Copy)]
pub struct Params {
    pub window: usize,
    pub alpha: f64,
    pub min_effect: f64,
}

impl Default for Params {
    fn default() -> Self {
        Self {
            window: 10,
            alpha: 0.01,
            min_effect: 0.03,
        }
    }
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct MetricVerdict {
    pub metric: String,
    pub baseline_median: f64,
    pub candidate_median: f64,
    /// (candidate − baseline) / baseline.
    pub change: f64,
    pub p_value: f64,
    pub baseline_samples: usize,
    pub candidate_samples: usize,
    /// Range of the per-run medians of the baseline runs: run-to-run
    /// variation that the pooled test cannot see. On shared machines it is
    /// often larger than the change being tested (docs/benchmarks/harness.md).
    pub baseline_run_medians: [f64; 2],
    /// `regression` | `improvement` | `unchanged`
    pub verdict: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Report {
    pub scenario: String,
    pub runner_fingerprint: String,
    pub baseline_runs: Vec<String>,
    pub metrics: Vec<MetricVerdict>,
    pub regressions: usize,
}

fn read(p: &Path) -> Result<J> {
    let text = std::fs::read_to_string(p)
        .map_err(|e| Error::invalid_input(format!("reading {}: {e}", p.display())))?;
    serde_json::from_str(&text)
        .map_err(|e| Error::invalid_input(format!("{}: invalid JSON: {e}", p.display())))
}

/// All `results.json` files under `dir` (bounded depth).
fn find_results(dir: &Path, depth: usize, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let p = e.path();
        if p.is_dir() && depth > 0 {
            find_results(&p, depth - 1, out);
        } else if p.file_name().is_some_and(|n| n == "results.json") {
            out.push(p);
        }
    }
}

/// Lower-is-better sample sets of one results document: per-query wall and
/// process CPU time, per-load latency.
fn metrics(r: &J) -> Vec<(String, Vec<f64>)> {
    let nums = |v: &J| -> Vec<f64> {
        v.as_array()
            .map(|a| a.iter().filter_map(J::as_f64).collect())
            .unwrap_or_default()
    };
    let mut out = Vec::new();
    for q in r["queries"].as_array().into_iter().flatten() {
        let id = q["id"].as_str().unwrap_or("?");
        out.push((format!("query.{id}.wall_ms"), nums(&q["wall_ms"])));
        out.push((
            format!("query.{id}.process_cpu_ms"),
            nums(&q["process_cpu_ms"]),
        ));
    }
    for l in r["loads"].as_array().into_iter().flatten() {
        let id = l["id"].as_str().unwrap_or("?");
        out.push((
            format!("load.{id}.latency_ms"),
            nums(&l["report"]["latency_ms_samples"]),
        ));
    }
    out
}

fn median(xs: &[f64]) -> f64 {
    let mut v = xs.to_vec();
    v.sort_by(f64::total_cmp);
    quantile(&v, 0.5)
}

pub fn compare_docs(candidate: &J, baseline: &[(String, J)], p: Params) -> Report {
    let mut metrics_out = Vec::new();
    for (name, cand) in metrics(candidate) {
        let per_run: Vec<Vec<f64>> = baseline
            .iter()
            .filter_map(|(_, b)| {
                metrics(b)
                    .into_iter()
                    .find(|(n, _)| n == &name)
                    .map(|(_, v)| v)
            })
            .filter(|v| !v.is_empty())
            .collect();
        let run_medians: Vec<f64> = per_run.iter().map(|v| median(v)).collect();
        let base: Vec<f64> = per_run.into_iter().flatten().collect();
        if cand.is_empty() || base.is_empty() {
            continue;
        }
        let (bm, cm) = (median(&base), median(&cand));
        let change = if bm > 0.0 { (cm - bm) / bm } else { 0.0 };
        let t = mann_whitney(&cand, &base);
        let verdict = if t.p < p.alpha && change >= p.min_effect {
            "regression"
        } else if t.p < p.alpha && change <= -p.min_effect {
            "improvement"
        } else {
            "unchanged"
        };
        metrics_out.push(MetricVerdict {
            metric: name,
            baseline_median: bm,
            candidate_median: cm,
            change,
            p_value: t.p,
            baseline_samples: base.len(),
            candidate_samples: cand.len(),
            baseline_run_medians: [
                run_medians.iter().copied().fold(f64::INFINITY, f64::min),
                run_medians
                    .iter()
                    .copied()
                    .fold(f64::NEG_INFINITY, f64::max),
            ],
            verdict,
        });
    }
    let regressions = metrics_out
        .iter()
        .filter(|m| m.verdict == "regression")
        .count();
    Report {
        scenario: candidate["scenario"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        runner_fingerprint: candidate["runner_fingerprint"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        baseline_runs: baseline.iter().map(|(p, _)| p.clone()).collect(),
        metrics: metrics_out,
        regressions,
    }
}

/// Compare `candidate` (a results.json) with the baseline runs under `dir`.
pub fn compare(candidate: &Path, dir: &Path, p: Params) -> Result<Report> {
    let cand = read(candidate)?;
    if cand["verdict"]["excluded_client_bound"].as_bool() == Some(true) {
        return Err(Error::invalid_input(
            "candidate run is CLIENT_BOUND and excluded from comparisons (STRATA-SYS-19-003)",
        ));
    }
    let mut files = Vec::new();
    find_results(dir, 3, &mut files);
    let cand_canon = std::fs::canonicalize(candidate).ok();
    let same = |b: &J, k: &str| b[k] == cand[k];
    let mut runs: Vec<(String, J)> = Vec::new();
    for f in files {
        if std::fs::canonicalize(&f).ok() == cand_canon {
            continue;
        }
        let b = read(&f)?;
        if same(&b, "scenario")
            && same(&b, "runner_fingerprint")
            && same(&b, "durability")
            && b["verdict"]["excluded_client_bound"].as_bool() != Some(true)
        {
            runs.push((f.display().to_string(), b));
        }
    }
    runs.sort_by(|a, b| {
        a.1["started_at"]
            .as_str()
            .unwrap_or_default()
            .cmp(b.1["started_at"].as_str().unwrap_or_default())
    });
    let keep = runs.len().saturating_sub(p.window);
    let runs = runs.split_off(keep);
    if runs.is_empty() {
        return Err(Error::invalid_input(format!(
            "no comparable baseline runs under {} (same scenario, runner and durability)",
            dir.display()
        )));
    }
    Ok(compare_docs(&cand, &runs, p))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(started: &str, wall: Vec<f64>, fp: &str) -> J {
        json!({
            "scenario": "s", "runner_fingerprint": fp, "durability": "async",
            "started_at": started, "verdict": {"excluded_client_bound": false},
            "queries": [{"id": "q", "wall_ms": wall, "process_cpu_ms": [1.0, 1.0, 1.0, 1.0, 1.0]}],
            "loads": [],
        })
    }

    fn noisy(center: f64, n: usize, seed: u64) -> Vec<f64> {
        let mut r = crate::stats::Rng::new(seed);
        (0..n)
            .map(|_| center * (1.0 + ((r.next_u64() % 1000) as f64 / 1000.0 - 0.5) * 0.02))
            .collect()
    }

    #[test]
    fn detects_regressions_and_ignores_noise_and_tiny_changes() {
        let base: Vec<(String, J)> = (0..10)
            .map(|i| (format!("b{i}"), doc("2026", noisy(10.0, 10, i), "fp")))
            .collect();
        let p = Params::default();
        let wall = |r: &Report| {
            r.metrics
                .iter()
                .find(|m| m.metric == "query.q.wall_ms")
                .cloned()
                .unwrap()
        };
        // +20%: significant and above the effect floor.
        let slow = compare_docs(&doc("2027", noisy(12.0, 10, 99), "fp"), &base, p);
        assert_eq!(wall(&slow).verdict, "regression");
        assert_eq!(slow.regressions, 1);
        // -20%: improvement, not a regression.
        let fast = compare_docs(&doc("2027", noisy(8.0, 10, 98), "fp"), &base, p);
        assert_eq!(wall(&fast).verdict, "improvement");
        // Same distribution: unchanged.
        let same = compare_docs(&doc("2027", noisy(10.0, 10, 97), "fp"), &base, p);
        assert_eq!(wall(&same).verdict, "unchanged");
        // +1.5%: may be significant with many samples, but below the 3% floor.
        let tiny = compare_docs(&doc("2027", noisy(10.15, 10, 96), "fp"), &base, p);
        assert_eq!(wall(&tiny).verdict, "unchanged", "{:?}", wall(&tiny));
        // Constant CPU samples: no change.
        assert!(same
            .metrics
            .iter()
            .any(|m| m.metric == "query.q.process_cpu_ms" && m.verdict == "unchanged"));
    }

    #[test]
    fn baseline_selection_uses_runner_window_and_excludes_client_bound() {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, d: &J| {
            let p = dir.path().join(name);
            std::fs::create_dir_all(&p).unwrap();
            std::fs::write(p.join("results.json"), d.to_string()).unwrap();
        };
        for i in 0..12 {
            write(
                &format!("run{i:02}"),
                &doc(&format!("2026-01-{:02}", i + 1), noisy(10.0, 10, i), "fp"),
            );
        }
        write(
            "other-runner",
            &doc("2026-02-01", noisy(50.0, 10, 1), "other"),
        );
        let mut bound = doc("2026-02-02", noisy(50.0, 10, 2), "fp");
        bound["verdict"]["excluded_client_bound"] = json!(true);
        write("client-bound", &bound);
        let cand = dir.path().join("cand.json");
        std::fs::write(
            &cand,
            doc("2026-03-01", noisy(10.0, 10, 50), "fp").to_string(),
        )
        .unwrap();
        let r = compare(&cand, dir.path(), Params::default()).unwrap();
        assert_eq!(r.baseline_runs.len(), 10, "window");
        assert!(r
            .baseline_runs
            .iter()
            .all(|p| !p.contains("run00") && !p.contains("run01")));
        assert!(r
            .baseline_runs
            .iter()
            .all(|p| !p.contains("other") && !p.contains("client-bound")));
        assert_eq!(r.regressions, 0);
        // A client-bound candidate is refused.
        std::fs::write(&cand, bound.to_string()).unwrap();
        assert!(compare(&cand, dir.path(), Params::default()).is_err());
        // No comparable runs → explicit error.
        let empty = tempfile::tempdir().unwrap();
        std::fs::write(&cand, doc("2026", vec![1.0], "fp").to_string()).unwrap();
        assert!(compare(&cand, empty.path(), Params::default()).is_err());
    }
}
