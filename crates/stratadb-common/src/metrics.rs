//! Lock-free fixed-bucket histograms rendered in the Prometheus text format
//! (spec §2.5, §3.2, §25).
//!
//! Buckets are fixed at construction (no allocation while observing), so a
//! histogram costs a handful of relaxed atomic increments per observation and
//! its memory is bounded. Rendering produces cumulative `_bucket{le=..}`,
//! `_sum` and `_count` series.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Latency bucket upper bounds in seconds (0.25 ms … 30 s).
pub const LATENCY_BUCKETS: &[f64] = &[
    0.000_25, 0.000_5, 0.001, 0.002_5, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
    10.0, 30.0,
];

/// Pipeline-stage bucket upper bounds in seconds (1 µs … 10 s): the INSERT and
/// SELECT stage budgets of spec VIII.1 are 1–1500 µs, below the resolution of
/// [`LATENCY_BUCKETS`].
pub const STAGE_BUCKETS: &[f64] = &[
    0.000_001,
    0.000_002_5,
    0.000_005,
    0.000_01,
    0.000_025,
    0.000_05,
    0.000_1,
    0.000_25,
    0.000_5,
    0.001,
    0.002_5,
    0.005,
    0.01,
    0.025,
    0.1,
    0.5,
    2.5,
    10.0,
];

/// Size bucket upper bounds (counts: records per group commit, rows, …).
pub const SIZE_BUCKETS: &[f64] = &[
    1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0, 4096.0, 16384.0,
];

/// A histogram over fixed bucket bounds; `observe` is wait-free.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    /// One counter per bound plus the `+Inf` overflow bucket (not cumulative).
    buckets: Vec<AtomicU64>,
    count: AtomicU64,
    /// Sum in units of `1 / scale` (µs for latencies, ns for stages), so it
    /// stays an integer.
    sum_scaled: AtomicU64,
    scale: f64,
}

/// A point-in-time copy of a histogram.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSnapshot {
    pub bounds: &'static [f64],
    /// Non-cumulative counts per bound, then `+Inf`.
    pub buckets: Vec<u64>,
    pub count: u64,
    pub sum: f64,
}

impl Histogram {
    pub fn new(bounds: &'static [f64]) -> Self {
        Self::with_scale(bounds, 1e6)
    }

    /// A histogram of plain values (not durations) with these bounds.
    pub fn with_bounds(bounds: &'static [f64]) -> Self {
        Self::with_scale(bounds, 1e3)
    }

    fn with_scale(bounds: &'static [f64], scale: f64) -> Self {
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            count: AtomicU64::new(0),
            sum_scaled: AtomicU64::new(0),
            scale,
        }
    }

    /// Pipeline-stage latencies ([`STAGE_BUCKETS`], sum kept in nanoseconds).
    pub fn stage() -> Self {
        Self::with_scale(STAGE_BUCKETS, 1e9)
    }

    pub fn latency() -> Self {
        Self::new(LATENCY_BUCKETS)
    }

    pub fn sizes() -> Self {
        Self::new(SIZE_BUCKETS)
    }

    /// Record one value.
    pub fn observe(&self, v: f64) {
        let v = if v.is_finite() && v > 0.0 { v } else { 0.0 };
        let i = self.bounds.partition_point(|b| *b < v);
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_scaled
            .fetch_add((v * self.scale).round() as u64, Ordering::Relaxed);
    }

    /// Record a duration in seconds.
    pub fn observe_duration(&self, d: Duration) {
        self.observe(d.as_secs_f64());
    }

    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            bounds: self.bounds,
            buckets: self
                .buckets
                .iter()
                .map(|b| b.load(Ordering::Relaxed))
                .collect(),
            count: self.count.load(Ordering::Relaxed),
            sum: self.sum_scaled.load(Ordering::Relaxed) as f64 / self.scale,
        }
    }
}

impl HistogramSnapshot {
    /// Append the Prometheus text exposition of this histogram as metric
    /// `name` with constant `labels` (e.g. `endpoint="ingest"`; may be empty).
    /// `HELP`/`TYPE` lines are written only when `header` is true (once per
    /// metric family).
    pub fn render(&self, out: &mut String, name: &str, help: &str, labels: &str, header: bool) {
        if header {
            let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} histogram");
        }
        let sep = if labels.is_empty() { "" } else { "," };
        let mut cum = 0u64;
        for (i, b) in self.bounds.iter().enumerate() {
            cum += self.buckets[i];
            let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"{b}\"}} {cum}");
        }
        cum += self.buckets[self.bounds.len()];
        let _ = writeln!(out, "{name}_bucket{{{labels}{sep}le=\"+Inf\"}} {cum}");
        let braces = |s: &str| {
            if s.is_empty() {
                String::new()
            } else {
                format!("{{{s}}}")
            }
        };
        let _ = writeln!(out, "{name}_sum{} {}", braces(labels), self.sum);
        let _ = writeln!(out, "{name}_count{} {}", braces(labels), self.count);
    }

    /// Approximate quantile (upper bound of the bucket holding it).
    pub fn quantile_upper_bound(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let target = (q.clamp(0.0, 1.0) * self.count as f64).ceil().max(1.0) as u64;
        let mut cum = 0;
        for (i, c) in self.buckets.iter().enumerate() {
            cum += c;
            if cum >= target {
                return Some(self.bounds.get(i).copied().unwrap_or(f64::INFINITY));
            }
        }
        Some(f64::INFINITY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_cumulative_rendering_and_quantiles() {
        let h = Histogram::latency();
        for ms in [0.1, 0.9, 1.0, 3.0, 40.0, 60_000.0] {
            h.observe(ms / 1000.0);
        }
        h.observe(-1.0); // clamped to 0
        h.observe(f64::NAN);
        let s = h.snapshot();
        assert_eq!(s.count, 8);
        assert_eq!(s.buckets.iter().sum::<u64>(), 8);
        // 0.1 ms, -1, NaN → ≤ 0.25 ms; 0.9 ms and 1.0 ms → ≤ 1 ms (bounds inclusive).
        assert_eq!(s.buckets[0], 3);
        assert_eq!(s.buckets[2], 2);
        assert_eq!(*s.buckets.last().unwrap_or(&0), 1, "60 s overflows to +Inf");
        let mut out = String::new();
        s.render(&mut out, "x_seconds", "help", "endpoint=\"a\"", true);
        assert!(out.starts_with("# HELP x_seconds help\n# TYPE x_seconds histogram\n"));
        assert!(
            out.contains("x_seconds_bucket{endpoint=\"a\",le=\"0.001\"} 5\n"),
            "{out}"
        );
        assert!(out.contains("x_seconds_bucket{endpoint=\"a\",le=\"+Inf\"} 8\n"));
        assert!(out.contains("x_seconds_count{endpoint=\"a\"} 8\n"));
        // Cumulative counts never decrease.
        let cums: Vec<u64> = out
            .lines()
            .filter(|l| l.contains("_bucket"))
            .map(|l| l.rsplit(' ').next().unwrap_or("0").parse().unwrap_or(0))
            .collect();
        assert!(cums.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(s.quantile_upper_bound(0.5), Some(0.001));
        assert_eq!(s.quantile_upper_bound(1.0), Some(f64::INFINITY));
        let mut plain = String::new();
        s.render(&mut plain, "y", "h", "", false);
        assert!(plain.contains("y_bucket{le=\"+Inf\"} 8\n") && plain.contains("y_count 8\n"));
    }

    #[test]
    fn stage_histograms_resolve_microseconds() {
        let h = Histogram::stage();
        for us in [0.4, 3.0, 3.0, 40.0, 900.0] {
            h.observe(us / 1e6);
        }
        let s = h.snapshot();
        assert_eq!(s.count, 5);
        assert_eq!(s.buckets[0], 1, "0.4 µs <= 1 µs");
        assert_eq!(s.buckets[2], 2, "3 µs <= 5 µs");
        assert!((s.sum - 946.4e-6).abs() < 1e-9, "{}", s.sum);
        assert_eq!(s.quantile_upper_bound(0.5), Some(0.000_005));
    }

    #[test]
    fn concurrent_observations_are_not_lost() {
        let h = std::sync::Arc::new(Histogram::sizes());
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let h = std::sync::Arc::clone(&h);
                std::thread::spawn(move || {
                    for i in 0..10_000 {
                        h.observe(f64::from((i + t) % 100));
                    }
                })
            })
            .collect();
        for t in threads {
            let _ = t.join();
        }
        let s = h.snapshot();
        assert_eq!(s.count, 80_000);
        assert_eq!(s.buckets.iter().sum::<u64>(), 80_000);
    }
}
