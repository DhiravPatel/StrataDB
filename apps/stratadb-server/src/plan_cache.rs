//! Query plan cache (spec VIII.16, STRATA-SYS-16-001/004): parsed and
//! planned queries keyed by their exact text, so a repeated query skips
//! parsing and planning.
//!
//! - **Store.** A [`ShardedLru`] (64 shards, a lock per shard) bounded by
//!   entries and bytes. Texts above [`MAX_TEXT_BYTES`] are not cached.
//! - **Why the text is a safe key.** A plan depends only on the query text:
//!   tenant scoping is applied when the plan runs, through the tenant's
//!   snapshot, so cached plans are shared across tenants without leaking
//!   data.
//! - **Per-fingerprint hit ratios** (top 100 by lookups) for operators.
//!   Counters are sharded by fingerprint and bounded per shard, so a hit
//!   takes no global lock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stratadb_common::cache::ShardedLru;
use stratadb_query::{Fingerprint, Prepared};

/// Longer query texts are planned every time (not cached).
pub const MAX_TEXT_BYTES: usize = 64 * 1024;
const FP_SHARDS: usize = 64;
/// Fingerprints tracked per shard (4,096 in all).
const FP_PER_SHARD: usize = 64;

#[derive(Debug, Clone)]
struct FpStat {
    normalized: String,
    hits: u64,
    misses: u64,
}

/// One fingerprint in [`PlanCache::top`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct FingerprintHits {
    pub fingerprint: String,
    pub normalized: String,
    pub hits: u64,
    pub misses: u64,
    pub hit_ratio: f64,
}

/// The server's plan cache.
#[derive(Debug)]
pub struct PlanCache {
    lru: ShardedLru<String, Arc<Prepared>>,
    fps: Vec<Mutex<HashMap<u64, FpStat>>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl PlanCache {
    /// At most `entries` plans and `bytes` bytes; zero disables caching.
    pub fn new(entries: usize, bytes: usize) -> Self {
        Self {
            lru: ShardedLru::new(entries, bytes),
            fps: (0..FP_SHARDS).map(|_| Mutex::new(HashMap::new())).collect(),
        }
    }

    pub fn enabled(&self) -> bool {
        self.lru.enabled()
    }

    fn count(&self, fp: &Fingerprint, hit: bool) {
        let mut m = lock(&self.fps[(fp.hash as usize) % FP_SHARDS]);
        if !m.contains_key(&fp.hash) && m.len() >= FP_PER_SHARD {
            // Make room: forget the least-used fingerprint of this shard.
            if let Some(k) = m
                .iter()
                .min_by_key(|(_, s)| s.hits + s.misses)
                .map(|(k, _)| *k)
            {
                m.remove(&k);
            }
        }
        let e = m.entry(fp.hash).or_insert_with(|| FpStat {
            normalized: fp.normalized.chars().take(200).collect(),
            hits: 0,
            misses: 0,
        });
        if hit {
            e.hits += 1;
        } else {
            e.misses += 1;
        }
    }

    /// The cached plan of `sql`, timed as a parse of `lookup` duration.
    pub fn get(&self, sql: &str, fp: &Fingerprint) -> Option<Prepared> {
        if !self.enabled() {
            return None;
        }
        let started = Instant::now();
        let hit = self.lru.get(sql);
        self.count(fp, hit.is_some());
        hit.map(|p| p.cached(started.elapsed()))
    }

    /// Remember a freshly planned query.
    pub fn insert(&self, sql: &str, prepared: &Prepared) {
        if self.enabled() && sql.len() <= MAX_TEXT_BYTES {
            let bytes = prepared.approx_bytes() + sql.len();
            self.lru.insert(
                sql.to_owned(),
                Arc::new(prepared.cached(Duration::ZERO)),
                bytes,
            );
        }
    }

    pub fn stats(&self) -> stratadb_common::cache::CacheStats {
        self.lru.stats()
    }

    /// The `n` most looked-up fingerprints with their hit ratios.
    pub fn top(&self, n: usize) -> Vec<FingerprintHits> {
        let mut all: Vec<FingerprintHits> = self
            .fps
            .iter()
            .flat_map(|m| {
                lock(m)
                    .iter()
                    .map(|(k, s)| FingerprintHits {
                        fingerprint: format!("{k:016x}"),
                        normalized: s.normalized.clone(),
                        hits: s.hits,
                        misses: s.misses,
                        hit_ratio: s.hits as f64 / (s.hits + s.misses).max(1) as f64,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        all.sort_by(|a, b| {
            (b.hits + b.misses)
                .cmp(&(a.hits + a.misses))
                .then(a.fingerprint.cmp(&b.fingerprint))
        });
        all.truncate(n);
        all
    }

    /// Prometheus text (`strata_plan_cache_*`).
    pub fn render(&self, out: &mut String) {
        use std::fmt::Write as _;
        let s = self.stats();
        for (name, kind, help, v) in [
            (
                "hits_total",
                "counter",
                "Plan cache lookups that found a plan.",
                s.hits as f64,
            ),
            (
                "misses_total",
                "counter",
                "Plan cache lookups that parsed and planned.",
                s.misses as f64,
            ),
            (
                "insertions_total",
                "counter",
                "Plans added to the cache.",
                s.insertions as f64,
            ),
            (
                "evictions_total",
                "counter",
                "Plans evicted (LRU within a shard).",
                s.evictions as f64,
            ),
            ("entries", "gauge", "Plans cached.", s.entries as f64),
            (
                "bytes",
                "gauge",
                "Approximate bytes of cached plans.",
                s.bytes as f64,
            ),
        ] {
            let _ = writeln!(out, "# HELP strata_plan_cache_{name} {help}\n# TYPE strata_plan_cache_{name} {kind}\nstrata_plan_cache_{name} {v}");
        }
        s.hit_seconds.render(
            out,
            "strata_plan_cache_hit_seconds",
            "Lookup latency of plan cache hits.",
            "",
            true,
        );
        let _ = writeln!(out, "# HELP strata_plan_cache_fingerprint_hit_ratio Hit ratio of the 100 most looked-up query fingerprints.\n# TYPE strata_plan_cache_fingerprint_hit_ratio gauge");
        for f in self.top(100) {
            let _ = writeln!(
                out,
                "strata_plan_cache_fingerprint_hit_ratio{{fingerprint=\"{}\"}} {}",
                f.fingerprint, f.hit_ratio
            );
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn prep(sql: &str) -> Prepared {
        stratadb_query::parse(sql, &stratadb_query::QueryOptions::default())
            .unwrap()
            .plan()
            .unwrap()
    }

    #[test]
    fn repeated_queries_hit_and_fingerprints_are_ranked() {
        let c = PlanCache::new(1000, 1 << 20);
        let sql = "SELECT count(*) FROM logs WHERE severity = 'ERROR'";
        let fp = stratadb_query::fingerprint(sql);
        assert!(c.get(sql, &fp).is_none());
        c.insert(sql, &prep(sql));
        for _ in 0..3 {
            assert!(c.get(sql, &fp).is_some());
        }
        let other = "SELECT count(*) FROM logs WHERE severity = 'WARN'";
        // Same shape, different literal: same fingerprint, a separate plan.
        let fp2 = stratadb_query::fingerprint(other);
        assert_eq!(fp.hash, fp2.hash);
        assert!(c.get(other, &fp2).is_none());
        let s = c.stats();
        assert_eq!((s.hits, s.misses, s.entries), (3, 2, 1));
        let top = c.top(10);
        assert_eq!(top.len(), 1);
        assert_eq!((top[0].hits, top[0].misses), (3, 2));
        let mut text = String::new();
        c.render(&mut text);
        assert!(text.contains("strata_plan_cache_hits_total 3"));
        assert!(text.contains("strata_plan_cache_fingerprint_hit_ratio{fingerprint="));
    }

    #[test]
    fn disabled_and_oversized() {
        let off = PlanCache::new(0, 0);
        let sql = "SELECT 1 FROM logs";
        off.insert(sql, &prep(sql));
        assert!(off.get(sql, &stratadb_query::fingerprint(sql)).is_none());
        let c = PlanCache::new(10, 1 << 20);
        // (The parser refuses texts this long anyway; the cache must not
        // keep one either.)
        let long = "x".repeat(MAX_TEXT_BYTES + 1);
        c.insert(&long, &prep(sql));
        assert_eq!(c.stats().entries, 0);
    }
}
