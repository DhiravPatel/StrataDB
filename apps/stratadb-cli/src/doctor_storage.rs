//! `strata doctor storage`: commit-path device latency (spec VIII.7,
//! STRATA-SYS-07-008).
//!
//! Each iteration writes 4 KiB and calls `fdatasync`, through the same call
//! the WAL uses for every group commit (`fsutil::sync_file`, data only), on a
//! temporary file in the target directory. The p50 latency bounds a
//! single writer's commit rate (`1 / p50`). Group commit multiplies that by
//! the group size; the adaptive delay (STRATA-SYS-07-004) waits at most
//! about one fsync latency to gather a group.

use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use serde_json::{json, Value as J};
use stratadb_common::{fsutil, Error, IoResultExt, Result};

pub const MAX_ITERATIONS: usize = 100_000;

fn quantile(sorted: &[Duration], q: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = ((sorted.len() - 1) as f64 * q).round() as usize;
    sorted[i.min(sorted.len() - 1)]
}

/// Measure `iterations` × (4 KiB write + fdatasync) in `dir`.
pub fn measure(dir: &Path, iterations: usize) -> Result<J> {
    if !(1..=MAX_ITERATIONS).contains(&iterations) {
        return Err(Error::invalid_input(format!(
            "--iterations must be in 1..={MAX_ITERATIONS}"
        )));
    }
    if !dir.is_dir() {
        return Err(Error::invalid_input(format!(
            "{} is not a directory",
            dir.display()
        )));
    }
    let path = dir.join(format!(".strata-doctor-fsync-{}", std::process::id()));
    let mut f = fsutil::create_new_file(&path)?;
    let block = vec![0xA5u8; 4096];
    let mut samples = Vec::with_capacity(iterations);
    let res: Result<()> = (|| {
        for _ in 0..iterations {
            let t = Instant::now();
            f.write_all(&block)
                .ctx(|| format!("writing {}", path.display()))?;
            fsutil::sync_file(&f, &path, true).ctx(|| format!("fdatasync {}", path.display()))?;
            samples.push(t.elapsed());
        }
        Ok(())
    })();
    drop(f);
    let _ = std::fs::remove_file(&path);
    res?;
    samples.sort();
    let us = |d: Duration| d.as_secs_f64() * 1e6;
    let (p50, p99) = (quantile(&samples, 0.5), quantile(&samples, 0.99));
    let max = samples.last().copied().unwrap_or_default();
    let rate = if p50.is_zero() {
        f64::INFINITY
    } else {
        1.0 / p50.as_secs_f64()
    };
    Ok(json!({
        "dir": dir.display().to_string(),
        "iterations": iterations,
        "write_bytes": 4096,
        "fdatasync_us": {"p50": us(p50), "p99": us(p99), "max": us(max),
                         "mean": us(samples.iter().sum::<Duration>() / iterations as u32)},
        "implied_max_commits_per_s_single_writer": rate,
        "note": "a group commit makes N writers durable with one fsync, so throughput scales with group size; the adaptive group delay waits at most min(fsync latency, max_delay)",
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn measures_and_cleans_up() {
        let d = tempfile::tempdir().unwrap();
        let r = measure(d.path(), 20).unwrap();
        assert_eq!(r["iterations"], 20);
        let p50 = r["fdatasync_us"]["p50"].as_f64().unwrap();
        let p99 = r["fdatasync_us"]["p99"].as_f64().unwrap();
        assert!(p50 > 0.0 && p99 >= p50);
        assert!(
            r["implied_max_commits_per_s_single_writer"]
                .as_f64()
                .unwrap()
                > 0.0
        );
        assert_eq!(
            std::fs::read_dir(d.path()).unwrap().count(),
            0,
            "temp file removed"
        );
        assert!(measure(d.path(), 0).is_err());
        assert!(measure(&d.path().join("missing"), 5).is_err());
    }
}
