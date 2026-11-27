//! The server's memory budget and accounting tree (spec VIII.5.2;
//! STRATA-SYS-02-029/035, -05-005..009, ARCH-01-006).
//!
//! The budget is `max_server_memory` (default 0.80 of the cgroup limit, or
//! of physical memory without one). It is split into regions:
//! - control: a fixed 2%;
//! - ingest: memtables;
//! - cache: decoded index filters;
//! - background: compaction and retention rewrites;
//! - query: the remainder, as a pool shared by all queries.
//!
//! Each tenant gets a private node under the query pool, limited by its
//! `query_memory_bytes` quota. Each query gets a node under its tenant,
//! limited by `max_query_memory_bytes`. Because the regions are separate
//! subtrees, queries cannot take memory that ingest needs.
//!
//! Every 10 s, resident memory (RSS) is compared with tracked memory
//! (SYS-05-009). The gap is exported as `strata_memory_untracked_bytes`, and a
//! warning is logged at most every 5 minutes when it exceeds
//! `untracked_warn_ratio` of RSS (with a 64 MiB floor) or 4 GiB.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use stratadb_common::memory::{self, MemoryTracker, TrackerNode};
use stratadb_common::{Error, Result, TenantId};

use crate::Quotas;

/// Fixed share of the control region (catalog, sessions, metadata).
pub const CONTROL_RATIO: f64 = 0.02;

/// Smallest per-query limit accepted (spec `memory.max_memory_usage`).
pub const MIN_QUERY_MEMORY: u64 = 64 << 20;

/// Validated `[memory]` settings (no host facts yet).
#[derive(Debug, Clone, PartialEq)]
pub struct MemorySettings {
    /// Absolute budget; overrides `max_server_memory_ratio`.
    pub max_server_memory_bytes: Option<u64>,
    pub max_server_memory_ratio: f64,
    pub ingest_ratio: f64,
    pub cache_ratio: f64,
    pub merge_ratio: f64,
    /// `None`: min(10 GiB, the query pool).
    pub max_query_memory_bytes: Option<u64>,
    pub wait_timeout: Duration,
    pub untracked_warn_ratio: f64,
}

impl Default for MemorySettings {
    fn default() -> Self {
        Self {
            max_server_memory_bytes: None,
            max_server_memory_ratio: 0.80,
            ingest_ratio: 0.10,
            cache_ratio: 0.30,
            merge_ratio: 0.10,
            max_query_memory_bytes: None,
            wait_timeout: Duration::ZERO,
            untracked_warn_ratio: 0.10,
        }
    }
}

impl MemorySettings {
    /// Check ranges (spec VIII.5 settings table) and that the query pool
    /// keeps at least 10% of the budget.
    pub fn validate(&self) -> Result<()> {
        let range = |name: &str, v: f64, lo: f64, hi: f64| {
            if v.is_finite() && (lo..=hi).contains(&v) {
                Ok(())
            } else {
                Err(Error::invalid_input(format!(
                    "memory.{name} must be within {lo}..={hi}, got {v}"
                )))
            }
        };
        range(
            "max_server_memory_ratio",
            self.max_server_memory_ratio,
            0.1,
            0.95,
        )?;
        range("ingest_memory_ratio", self.ingest_ratio, 0.01, 0.5)?;
        range("cache_memory_ratio", self.cache_ratio, 0.0, 0.8)?;
        range("merge_memory_ratio", self.merge_ratio, 0.01, 0.5)?;
        range("untracked_warn_ratio", self.untracked_warn_ratio, 0.01, 1.0)?;
        let shares = CONTROL_RATIO + self.ingest_ratio + self.cache_ratio + self.merge_ratio;
        if shares > 0.9 {
            return Err(Error::invalid_input(format!(
                "memory ratios leave {:.0}% for queries; ingest + cache + merge must be at most {:.2}",
                (1.0 - shares) * 100.0,
                0.9 - CONTROL_RATIO
            )));
        }
        if let Some(b) = self.max_server_memory_bytes {
            if b < 256 << 20 {
                return Err(Error::invalid_input(
                    "memory.max_server_memory_bytes must be at least 256 MiB",
                ));
            }
        }
        if let Some(q) = self.max_query_memory_bytes {
            if q < MIN_QUERY_MEMORY {
                return Err(Error::invalid_input(
                    "memory.max_query_memory_bytes must be at least 64 MiB",
                ));
            }
        }
        if self.wait_timeout > Duration::from_secs(600) {
            return Err(Error::invalid_input(
                "memory.memory_wait_timeout_ms must be at most 600000",
            ));
        }
        Ok(())
    }

    /// The byte budget for a host with `detected` memory available.
    pub fn plan(&self, detected: Option<(u64, &'static str)>) -> Result<MemoryPlan> {
        let (budget, source) = match (self.max_server_memory_bytes, detected) {
            (Some(b), _) => (b, "max_server_memory_bytes"),
            (None, Some((limit, src))) => {
                ((limit as f64 * self.max_server_memory_ratio) as u64, src)
            }
            (None, None) => {
                return Err(Error::invalid_input(
                    "cannot detect the host's memory; set memory.max_server_memory_bytes",
                ))
            }
        };
        let share = |r: f64| (budget as f64 * r) as u64;
        let (control, ingest, cache, background) = (
            share(CONTROL_RATIO),
            share(self.ingest_ratio),
            share(self.cache_ratio),
            share(self.merge_ratio),
        );
        let query = budget.saturating_sub(control + ingest + cache + background);
        let per_query = self
            .max_query_memory_bytes
            .unwrap_or(10 << 30)
            .min(query)
            .max(1);
        Ok(MemoryPlan {
            budget,
            source,
            control,
            ingest,
            cache,
            background,
            query,
            per_query,
            wait: self.wait_timeout,
            untracked_warn_ratio: self.untracked_warn_ratio,
        })
    }
}

/// Byte limits of every region.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct MemoryPlan {
    pub budget: u64,
    /// Where the budget came from: `cgroup`, `physical` or the setting.
    pub source: &'static str,
    pub control: u64,
    pub ingest: u64,
    pub cache: u64,
    pub background: u64,
    pub query: u64,
    pub per_query: u64,
    #[serde(skip)]
    pub wait: Duration,
    pub untracked_warn_ratio: f64,
}

/// The live tree.
#[derive(Debug)]
pub struct ServerMemory {
    pub plan: MemoryPlan,
    pub root: Arc<MemoryTracker>,
    pub control: Arc<MemoryTracker>,
    pub ingest: Arc<MemoryTracker>,
    /// Cache region. Filters cannot be evicted yet, so it is tracked
    /// without enforcing its limit.
    pub cache: Arc<MemoryTracker>,
    pub background: Arc<MemoryTracker>,
    pub query: Arc<MemoryTracker>,
    tenants: Mutex<HashMap<TenantId, Arc<MemoryTracker>>>,
    quotas: Quotas,
    resident: AtomicU64,
    untracked: AtomicU64,
    last_warning: Mutex<Option<Instant>>,
}

impl ServerMemory {
    pub fn new(plan: MemoryPlan, quotas: Quotas) -> Arc<Self> {
        let root = MemoryTracker::root("process", plan.budget);
        Arc::new(Self {
            control: root.child("control", plan.control),
            ingest: root.child("ingest", plan.ingest),
            cache: root.child("cache", 0),
            background: root.child("background", plan.background),
            query: root.child("query", plan.query),
            root,
            plan,
            tenants: Mutex::new(HashMap::new()),
            quotas,
            resident: AtomicU64::new(0),
            untracked: AtomicU64::new(0),
            last_warning: Mutex::new(None),
        })
    }

    /// The tenant's node in the query pool (limit: its `query_memory_bytes`
    /// quota). Tenants are named only to themselves in errors.
    pub fn tenant(&self, t: &TenantId) -> Arc<MemoryTracker> {
        let mut m = self.tenants.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(m.entry(t.clone()).or_insert_with(|| {
            let limit = self.quotas.for_tenant(t).query_memory_bytes.unwrap_or(0);
            self.query.private_child(format!("tenant:{t}"), limit)
        }))
    }

    /// Parent node for one query (the executor adds its `exec` node with the
    /// per-query limit below it).
    pub fn query_parent(&self, t: &TenantId, query_id: &str) -> Arc<MemoryTracker> {
        self.tenant(t).child(format!("query:{query_id}"), 0)
    }

    /// Bytes tracked across all regions.
    pub fn tracked(&self) -> u64 {
        self.root.used()
    }

    /// Compare RSS with tracked memory; warn when the gap is alarming.
    /// Returns (resident, untracked).
    pub fn sample(&self) -> Option<(u64, u64)> {
        let rss = memory::resident_bytes()?;
        let tracked = self.tracked();
        let gap = memory::untracked(rss, tracked);
        self.resident.store(rss, Ordering::Relaxed);
        self.untracked.store(gap, Ordering::Relaxed);
        if memory::untracked_is_alarming(rss, tracked, self.plan.untracked_warn_ratio, 64 << 20) {
            let mut last = self.last_warning.lock().unwrap_or_else(|e| e.into_inner());
            if last.is_none_or(|t| t.elapsed() >= Duration::from_secs(300)) {
                *last = Some(Instant::now());
                tracing::warn!(
                    component = "memory",
                    resident_bytes = rss,
                    tracked_bytes = tracked,
                    untracked_bytes = gap,
                    "resident memory is well above tracked memory (untracked allocations risk OOM)"
                );
            }
        }
        Some((rss, gap))
    }

    /// The tree, for `GET /api/v1/admin/memory`.
    pub fn snapshot(&self) -> TrackerNode {
        self.root.snapshot()
    }

    /// Prometheus text for `/metrics`.
    pub fn render(&self, out: &mut String) {
        use std::fmt::Write as _;
        let regions = [
            ("control", &self.control),
            ("ingest", &self.ingest),
            ("cache", &self.cache),
            ("background", &self.background),
            ("query", &self.query),
        ];
        let limit = |r: &str, t: &MemoryTracker| match r {
            "cache" => self.plan.cache,
            _ => t.limit(),
        };
        let _ = writeln!(out, "# HELP strata_memory_tracked_bytes Memory reserved on the accounting tree, by region.\n# TYPE strata_memory_tracked_bytes gauge");
        for (r, t) in regions {
            let _ = writeln!(
                out,
                "strata_memory_tracked_bytes{{region=\"{r}\"}} {}",
                t.used()
            );
        }
        let _ = writeln!(out, "# HELP strata_memory_limit_bytes Limit of each region (cache: target share, not enforced).\n# TYPE strata_memory_limit_bytes gauge");
        for (r, t) in regions {
            let _ = writeln!(
                out,
                "strata_memory_limit_bytes{{region=\"{r}\"}} {}",
                limit(r, t)
            );
        }
        let _ = writeln!(out, "# HELP strata_memory_peak_bytes Peak reserved memory by region since start.\n# TYPE strata_memory_peak_bytes gauge");
        for (r, t) in regions {
            let _ = writeln!(
                out,
                "strata_memory_peak_bytes{{region=\"{r}\"}} {}",
                t.peak()
            );
        }
        let _ = writeln!(
            out,
            "# HELP strata_memory_budget_bytes Process memory budget (max_server_memory).\n# TYPE strata_memory_budget_bytes gauge\nstrata_memory_budget_bytes {}",
            self.plan.budget
        );
        let _ = writeln!(
            out,
            "# HELP strata_memory_resident_bytes Resident set size at the last 10 s sample.\n# TYPE strata_memory_resident_bytes gauge\nstrata_memory_resident_bytes {}",
            self.resident.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP strata_memory_untracked_bytes Resident memory not covered by tracked memory (spec STRATA-SYS-05-009).\n# TYPE strata_memory_untracked_bytes gauge\nstrata_memory_untracked_bytes {}",
            self.untracked.load(Ordering::Relaxed)
        );
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn budget_and_regions() {
        let s = MemorySettings::default();
        s.validate().unwrap();
        let p = s.plan(Some((10_000 << 20, "physical"))).unwrap();
        assert_eq!(p.budget, 8_000 << 20);
        assert_eq!(p.source, "physical");
        assert_eq!(p.ingest, 800 << 20);
        assert_eq!(p.cache, 2_400 << 20);
        assert_eq!(
            p.query,
            p.budget - p.control - p.ingest - p.cache - p.background
        );
        assert_eq!(p.per_query, p.query, "10 GiB default capped at the pool");
        let fixed = MemorySettings {
            max_server_memory_bytes: Some(1 << 30),
            max_query_memory_bytes: Some(100 << 20),
            ..s.clone()
        };
        let p = fixed.plan(None).unwrap();
        assert_eq!(
            (p.budget, p.per_query, p.source),
            (1 << 30, 100 << 20, "max_server_memory_bytes")
        );
        assert!(
            s.plan(None).is_err(),
            "no budget without detection or setting"
        );
        for bad in [
            MemorySettings {
                ingest_ratio: 0.5,
                cache_ratio: 0.4,
                ..s.clone()
            },
            MemorySettings {
                max_server_memory_ratio: 0.99,
                ..s.clone()
            },
            MemorySettings {
                max_query_memory_bytes: Some(1 << 20),
                ..s.clone()
            },
            MemorySettings {
                max_server_memory_bytes: Some(1 << 20),
                ..s.clone()
            },
            MemorySettings {
                wait_timeout: Duration::from_secs(601),
                ..s.clone()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn tenants_and_queries_get_nodes_with_quota_limits() {
        let mut quotas = Quotas::default();
        quotas.tenants.insert(
            "small".into(),
            crate::TenantQuota {
                query_memory_bytes: Some(1 << 20),
                ..Default::default()
            },
        );
        let m = ServerMemory::new(
            MemorySettings::default()
                .plan(Some((1 << 34, "physical")))
                .unwrap(),
            quotas,
        );
        let small = TenantId::new("small").unwrap();
        let other = TenantId::new("other").unwrap();
        assert_eq!(m.tenant(&small).limit(), 1 << 20);
        assert_eq!(m.tenant(&other).limit(), 0);
        assert!(Arc::ptr_eq(&m.tenant(&small), &m.tenant(&small)));
        let q = m.query_parent(&small, "q1");
        assert_eq!(q.path(), "process/query/tenant:small/query:q1");
        q.try_reserve(512 << 10).unwrap();
        assert!(q.try_reserve(1 << 20).is_err());
        assert_eq!(m.tracked(), 512 << 10);
        q.release(512 << 10);
        let mut text = String::new();
        m.sample();
        m.render(&mut text);
        assert!(
            text.contains("strata_memory_tracked_bytes{region=\"ingest\"} 0"),
            "{text}"
        );
        assert!(text.contains("strata_memory_resident_bytes "), "{text}");
        assert_eq!(m.snapshot().children.len(), 5);
    }
}
