//! Sample statistics for benchmark reports (spec VIII.19): order statistics,
//! moments, bootstrap confidence intervals and the Mann-Whitney U test.
//!
//! All randomness is from a fixed-seed generator so reports are reproducible.

use serde::Serialize;

/// Summary of one sample set (any unit; callers name it).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Summary {
    pub n: usize,
    pub min: f64,
    pub max: f64,
    pub mean: f64,
    pub stddev: f64,
    pub median: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    /// 95% bootstrap confidence interval of the median.
    pub median_ci95: [f64; 2],
}

/// Linear-interpolation quantile (type 7) of sorted samples.
pub fn quantile(sorted: &[f64], q: f64) -> f64 {
    match sorted.len() {
        0 => f64::NAN,
        1 => sorted[0],
        n => {
            let h = (n - 1) as f64 * q.clamp(0.0, 1.0);
            let lo = h.floor() as usize;
            let hi = (lo + 1).min(n - 1);
            sorted[lo] + (h - lo as f64) * (sorted[hi] - sorted[lo])
        }
    }
}

fn sorted(xs: &[f64]) -> Vec<f64> {
    let mut v: Vec<f64> = xs.iter().copied().filter(|x| x.is_finite()).collect();
    v.sort_by(f64::total_cmp);
    v
}

/// Deterministic SplitMix64 (bootstrap resampling only; not cryptographic).
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (`n > 0`), via 128-bit multiply (no modulo bias
    /// worth measuring at bootstrap sample sizes).
    pub fn below(&mut self, n: usize) -> usize {
        ((u128::from(self.next_u64()) * n as u128) >> 64) as usize
    }
}

/// Above this many samples the median CI uses order statistics instead of
/// resampling (bootstrap cost is `resamples × n log n`).
pub const BOOTSTRAP_MAX_N: usize = 2_000;

/// Distribution-free 95% CI of the median from order statistics: ranks
/// `n/2 ∓ 1.96·√n/2` (normal approximation to the binomial), valid for the
/// large `n` where it is used.
pub fn order_statistic_median_ci(sorted: &[f64]) -> [f64; 2] {
    let n = sorted.len();
    if n == 0 {
        return [f64::NAN, f64::NAN];
    }
    let half = 1.959_964 * (n as f64).sqrt() / 2.0;
    let lo = ((n as f64 / 2.0 - half).floor().max(0.0)) as usize;
    let hi = ((n as f64 / 2.0 + half).ceil() as usize).min(n - 1);
    [sorted[lo], sorted[hi]]
}

/// 95% CI of the median: percentile bootstrap (`resamples` draws) up to
/// [`BOOTSTRAP_MAX_N`] samples, order statistics beyond.
pub fn bootstrap_median_ci(xs: &[f64], resamples: usize, seed: u64) -> [f64; 2] {
    let xs = sorted(xs);
    if xs.len() < 2 {
        let v = xs.first().copied().unwrap_or(f64::NAN);
        return [v, v];
    }
    if xs.len() > BOOTSTRAP_MAX_N {
        return order_statistic_median_ci(&xs);
    }
    let mut rng = Rng::new(seed);
    let mut medians = Vec::with_capacity(resamples);
    let mut buf = vec![0.0; xs.len()];
    for _ in 0..resamples {
        for b in buf.iter_mut() {
            *b = xs[rng.below(xs.len())];
        }
        buf.sort_by(f64::total_cmp);
        medians.push(quantile(&buf, 0.5));
    }
    medians.sort_by(f64::total_cmp);
    [quantile(&medians, 0.025), quantile(&medians, 0.975)]
}

pub fn summarize(xs: &[f64]) -> Summary {
    let v = sorted(xs);
    let n = v.len();
    let mean = if n == 0 {
        f64::NAN
    } else {
        v.iter().sum::<f64>() / n as f64
    };
    let stddev = if n < 2 {
        0.0
    } else {
        (v.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64).sqrt()
    };
    Summary {
        n,
        min: v.first().copied().unwrap_or(f64::NAN),
        max: v.last().copied().unwrap_or(f64::NAN),
        mean,
        stddev,
        median: quantile(&v, 0.5),
        p95: quantile(&v, 0.95),
        p99: quantile(&v, 0.99),
        p999: quantile(&v, 0.999),
        median_ci95: bootstrap_median_ci(&v, 2_000, 0x5eed),
    }
}

/// Result of a two-sided Mann-Whitney U test of `a` vs `b`.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct MannWhitney {
    /// U statistic of `a`.
    pub u: f64,
    /// Standardized statistic (tie- and continuity-corrected).
    pub z: f64,
    /// Two-sided p-value (normal approximation).
    pub p: f64,
}

/// Two-sided Mann-Whitney U test with the normal approximation, tie
/// correction and continuity correction. Adequate for the sample sizes the
/// harness uses (≥ 5 per side); exact for no sample size.
pub fn mann_whitney(a: &[f64], b: &[f64]) -> MannWhitney {
    let (n1, n2) = (a.len() as f64, b.len() as f64);
    if a.is_empty() || b.is_empty() {
        return MannWhitney {
            u: 0.0,
            z: 0.0,
            p: 1.0,
        };
    }
    let mut all: Vec<(f64, bool)> = a
        .iter()
        .map(|&x| (x, true))
        .chain(b.iter().map(|&x| (x, false)))
        .collect();
    all.sort_by(|x, y| x.0.total_cmp(&y.0));
    // Mid-ranks for ties; accumulate the tie correction term Σ(t³ − t).
    let mut rank_sum_a = 0.0;
    let mut ties = 0.0;
    let mut i = 0;
    while i < all.len() {
        let mut j = i;
        while j + 1 < all.len() && all[j + 1].0 == all[i].0 {
            j += 1;
        }
        let t = (j - i + 1) as f64;
        let mid = (i + j) as f64 / 2.0 + 1.0;
        rank_sum_a += all[i..=j].iter().filter(|x| x.1).count() as f64 * mid;
        ties += t * t * t - t;
        i = j + 1;
    }
    let u = rank_sum_a - n1 * (n1 + 1.0) / 2.0;
    let n = n1 + n2;
    let mean = n1 * n2 / 2.0;
    let var = n1 * n2 / 12.0 * ((n + 1.0) - ties / (n * (n - 1.0)));
    if var <= 0.0 {
        return MannWhitney { u, z: 0.0, p: 1.0 };
    }
    let diff = u - mean;
    let corrected = (diff.abs() - 0.5).max(0.0) * diff.signum();
    let z = corrected / var.sqrt();
    MannWhitney {
        u,
        z,
        p: (2.0 * normal_sf(z.abs())).min(1.0),
    }
}

/// Upper tail of the standard normal, `P(Z > x)`, via erfc (Numerical
/// Recipes `erfcc`, relative error < 1.2e-7).
pub fn normal_sf(x: f64) -> f64 {
    0.5 * erfc(x / std::f64::consts::SQRT_2)
}

fn erfc(x: f64) -> f64 {
    let z = x.abs();
    let t = 1.0 / (1.0 + 0.5 * z);
    let r = t
        * (-z * z - 1.265_512_23
            + t * (1.000_023_68
                + t * (0.374_091_96
                    + t * (0.096_784_18
                        + t * (-0.186_288_06
                            + t * (0.278_868_07
                                + t * (-1.135_203_98
                                    + t * (1.488_515_87
                                        + t * (-0.822_152_23 + t * 0.170_872_77)))))))))
            .exp();
    if x >= 0.0 {
        r
    } else {
        2.0 - r
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_statistics_and_moments() {
        let s = summarize(&[4.0, 1.0, 3.0, 2.0, 5.0, f64::NAN]);
        assert_eq!((s.n, s.min, s.max, s.median), (5, 1.0, 5.0, 3.0));
        assert!((s.mean - 3.0).abs() < 1e-12);
        assert!((s.stddev - 2.5f64.sqrt()).abs() < 1e-12, "sample stddev");
        assert!((s.p95 - 4.8).abs() < 1e-12, "type-7 interpolation");
        assert!(s.median_ci95[0] <= s.median && s.median <= s.median_ci95[1]);
        assert!(quantile(&[], 0.5).is_nan());
        let one = summarize(&[7.0]);
        assert_eq!(
            (one.median, one.stddev, one.median_ci95),
            (7.0, 0.0, [7.0, 7.0])
        );
    }

    #[test]
    fn bootstrap_is_deterministic_and_narrows_with_n() {
        let mut r = Rng::new(1);
        let small: Vec<f64> = (0..10).map(|_| (r.next_u64() % 1000) as f64).collect();
        let big: Vec<f64> = (0..1000).map(|_| (r.next_u64() % 1000) as f64).collect();
        assert_eq!(
            bootstrap_median_ci(&small, 500, 9),
            bootstrap_median_ci(&small, 500, 9)
        );
        let w = |c: [f64; 2]| c[1] - c[0];
        assert!(w(bootstrap_median_ci(&big, 500, 9)) < w(bootstrap_median_ci(&small, 500, 9)));
    }

    #[test]
    fn large_samples_use_order_statistics_quickly() {
        let xs: Vec<f64> = (0..1_000_000).map(f64::from).collect();
        let t = std::time::Instant::now();
        let s = summarize(&xs);
        assert!(t.elapsed() < std::time::Duration::from_secs(5));
        // ±1.96·√n/2 = ±980 ranks around the median.
        assert_eq!(s.median_ci95, [499_020.0, 500_980.0]);
    }

    #[test]
    fn normal_tail_matches_tables() {
        for (x, want) in [
            (0.0, 0.5),
            (1.0, 0.158_655_25),
            (1.959_964, 0.025),
            (3.0, 0.001_349_9),
        ] {
            assert!((normal_sf(x) - want).abs() < 1e-6, "{x}");
        }
        assert!((normal_sf(-1.0) - 0.841_344_75).abs() < 1e-6);
    }

    #[test]
    fn mann_whitney_known_values() {
        // Fully separated samples of 10 vs 10: U = 0; z = -(50 - 0.5)/sqrt(175).
        let a: Vec<f64> = (0..10).map(f64::from).collect();
        let b: Vec<f64> = (10..20).map(f64::from).collect();
        let r = mann_whitney(&a, &b);
        assert_eq!(r.u, 0.0);
        assert!((r.z + 49.5 / 175f64.sqrt()).abs() < 1e-9, "{}", r.z);
        assert!(r.p < 0.001, "{}", r.p); // exact p = 1.08e-5; normal approx ≈ 1.8e-4
                                         // Identical distributions: no evidence.
        let same = mann_whitney(&a, &a);
        assert!(same.p > 0.9, "{}", same.p);
        // Symmetry: swapping sides flips z, keeps p.
        let rev = mann_whitney(&b, &a);
        assert!((rev.z + r.z).abs() < 1e-12 && (rev.p - r.p).abs() < 1e-12);
        assert_eq!(rev.u, 100.0);
        // All values tied: variance 0 → p = 1, not NaN.
        assert_eq!(mann_whitney(&[1.0; 5], &[1.0; 5]).p, 1.0);
        assert_eq!(mann_whitney(&[], &[1.0]).p, 1.0);
        // Ties get mid-ranks: [1,2,2] vs [2,3]: ranks 1,3,3 | 3,5 → U_a = 7 - 6 = 1.
        assert_eq!(mann_whitney(&[1.0, 2.0, 2.0], &[2.0, 3.0]).u, 1.0);
    }
}
