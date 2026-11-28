//! Server, table and device lifecycles (spec IX.22.6–IX.22.8,
//! STRATA-ARCH-22-006/007/008), exported as `strata_state_*`.
//!
//! - **Server:** `STARTING → RECOVERING` (config loaded) `→ WARMING` (engine
//!   open: WAL replayed, catalog loaded) `→ READY` (listener bound) `↔ DEGRADED`
//!   (device degraded or failed, or table read-only) `→ DRAINING` (SIGTERM)
//!   `→ STOPPED` (engine closed).
//! - **Table** `logs`: `CREATING → ACTIVE` once the engine is open, and
//!   `ACTIVE ↔ READ_ONLY` while the engine is failed (WAL or manifest I/O) or
//!   disk space is below the reserve.
//! - **Device** (the data directory's device): a [`DeviceMonitor`] over the
//!   WAL fsync latency histogram. See its docs for the thresholds.
//!
//! [`Lifecycle::tick`] re-evaluates the conditions; the server calls it every
//! 10 s from a blocking task (it reads disk space).

use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use stratadb_common::metrics::HistogramSnapshot;
use stratadb_common::state::{
    self, ConnectionState, DeviceState, InsertState, MergeState, QueryState, ServerState,
    TableState, Tracker, WalSegmentState,
};
use stratadb_storage::Engine;

/// Thresholds of the device health monitor.
#[derive(Debug, Clone, Copy)]
pub struct DeviceThresholds {
    /// fsyncs needed before the baseline p99 is trusted (`UNKNOWN → HEALTHY`).
    pub baseline_samples: u64,
    /// fsyncs a window needs before its p99 counts as evidence.
    pub window_samples: u64,
    /// A window is slow when its p99 exceeds `factor ×` the baseline p99.
    pub factor: f64,
    /// Slow windows for this long: `HEALTHY → DEGRADED` (spec: 30 s).
    pub degrade_after: Duration,
    /// No slow window for this long: `DEGRADED → HEALTHY` (spec: 5 min).
    pub recover_after: Duration,
}

impl Default for DeviceThresholds {
    fn default() -> Self {
        Self {
            baseline_samples: 64,
            window_samples: 16,
            factor: 4.0,
            degrade_after: Duration::from_secs(30),
            recover_after: Duration::from_secs(300),
        }
    }
}

/// Device health from WAL fsync latency (spec IX.22.8).
///
/// - The **baseline** is the p99 of the first `baseline_samples` fsyncs,
///   then follows healthy windows slowly (EWMA, weight 1/8) so a device that
///   is gradually busier is not called degraded.
/// - Each tick's **window** is the difference of two histogram snapshots;
///   windows with too few fsyncs carry no evidence either way.
/// - An I/O failure (the engine is fail-stopped) moves a known device to
///   `FAILED`, which only an operator clears (a restart starts `UNKNOWN`).
///
/// p99 values are histogram bucket upper bounds, so `factor` should be at
/// least the bucket ratio (the WAL buckets are ~2–2.5× apart).
#[derive(Debug)]
pub struct DeviceMonitor {
    th: DeviceThresholds,
    life: Tracker<DeviceState>,
    last: Option<HistogramSnapshot>,
    baseline: Option<f64>,
    slow_since: Option<Instant>,
    normal_since: Option<Instant>,
}

fn diff(now: &HistogramSnapshot, before: Option<&HistogramSnapshot>) -> HistogramSnapshot {
    let mut w = now.clone();
    if let Some(b) = before {
        for (x, y) in w.buckets.iter_mut().zip(&b.buckets) {
            *x = x.saturating_sub(*y);
        }
        w.count = w.count.saturating_sub(b.count);
        w.sum = (w.sum - b.sum).max(0.0);
    }
    w
}

/// p99 as a finite bucket bound (`+Inf` maps to the largest finite bound).
fn p99(h: &HistogramSnapshot) -> Option<f64> {
    let q = h.quantile_upper_bound(0.99)?;
    Some(if q.is_finite() {
        q
    } else {
        h.bounds.last().copied().unwrap_or(f64::MAX) * 2.0
    })
}

impl DeviceMonitor {
    pub fn new(th: DeviceThresholds) -> Self {
        Self {
            th,
            life: Tracker::new(DeviceState::Unknown),
            last: None,
            baseline: None,
            slow_since: None,
            normal_since: None,
        }
    }

    pub fn state(&self) -> DeviceState {
        self.life.state()
    }

    /// Baseline fsync p99 in seconds, once measured.
    pub fn baseline(&self) -> Option<f64> {
        self.baseline
    }

    /// Feed the cumulative fsync histogram and whether the engine has failed
    /// with an I/O error. Returns the state after the observation.
    pub fn observe(
        &mut self,
        now: Instant,
        fsync: &HistogramSnapshot,
        io_failed: bool,
    ) -> DeviceState {
        let window = diff(fsync, self.last.as_ref());
        self.last = Some(fsync.clone());
        if io_failed {
            if matches!(self.state(), DeviceState::Healthy | DeviceState::Degraded) {
                self.life.to(DeviceState::Failed, "storage I/O failure");
                tracing::error!(component = "device", "device FAILED: storage I/O failure");
            }
            return self.state();
        }
        match self.state() {
            DeviceState::Unknown => {
                if fsync.count >= self.th.baseline_samples {
                    self.baseline = p99(fsync);
                    self.life
                        .to(DeviceState::Healthy, "baseline latency measured");
                    tracing::info!(
                        component = "device",
                        baseline_p99_s = self.baseline,
                        "device HEALTHY: fsync baseline measured"
                    );
                }
            }
            DeviceState::Healthy | DeviceState::Degraded => {
                let (Some(base), true) = (self.baseline, window.count >= self.th.window_samples)
                else {
                    // No evidence: an idle device counts as normal.
                    self.slow_since = None;
                    self.normal_since.get_or_insert(now);
                    return self.settle(now);
                };
                let wp = p99(&window).unwrap_or(0.0);
                if wp > self.th.factor * base {
                    self.normal_since = None;
                    self.slow_since.get_or_insert(now);
                } else {
                    self.slow_since = None;
                    self.normal_since.get_or_insert(now);
                    if self.state() == DeviceState::Healthy {
                        self.baseline = Some(base + (wp - base) / 8.0);
                    }
                }
                return self.settle(now);
            }
            DeviceState::Failed => {}
        }
        self.state()
    }

    fn settle(&mut self, now: Instant) -> DeviceState {
        let for_at_least =
            |since: Option<Instant>, d: Duration| since.is_some_and(|t| now.duration_since(t) >= d);
        match self.state() {
            DeviceState::Healthy if for_at_least(self.slow_since, self.th.degrade_after) => {
                self.life
                    .to(DeviceState::Degraded, "fsync p99 above factor × baseline");
                tracing::warn!(
                    component = "device",
                    baseline_p99_s = self.baseline,
                    "device DEGRADED: fsync p99 above {}× baseline for {:?}",
                    self.th.factor,
                    self.th.degrade_after
                );
            }
            DeviceState::Degraded if for_at_least(self.normal_since, self.th.recover_after) => {
                self.life.to(DeviceState::Healthy, "latency normal");
                tracing::info!(component = "device", "device HEALTHY again");
            }
            _ => {}
        }
        self.state()
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A panic while holding a tracker leaves it consistent (moves are single
    // assignments), so poisoning is ignored.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// The process-level lifecycles: one server, the `logs` table, one device.
#[derive(Debug)]
pub struct Lifecycle {
    /// Last warning about retained obsolete bytes (rate limit).
    retained_warned: Mutex<Option<Instant>>,
    server: Mutex<Tracker<ServerState>>,
    table: Mutex<Option<Tracker<TableState>>>,
    device: Mutex<DeviceMonitor>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new(DeviceThresholds::default())
    }
}

impl Lifecycle {
    /// The server is `STARTING`. Also makes every machine's series appear in
    /// `/metrics` before its first entity.
    pub fn new(th: DeviceThresholds) -> Self {
        state::touch::<QueryState>();
        state::touch::<InsertState>();
        state::touch::<WalSegmentState>();
        state::touch::<MergeState>();
        state::touch::<ConnectionState>();
        state::touch::<TableState>();
        Self {
            retained_warned: Mutex::new(None),
            server: Mutex::new(Tracker::new(ServerState::Starting)),
            table: Mutex::new(None),
            device: Mutex::new(DeviceMonitor::new(th)),
        }
    }

    pub fn server(&self) -> ServerState {
        lock(&self.server).state()
    }

    pub fn table(&self) -> Option<TableState> {
        lock(&self.table).as_ref().map(Tracker::state)
    }

    pub fn device(&self) -> DeviceState {
        lock(&self.device).state()
    }

    /// Configuration loaded; opening the engine (WAL replay) next.
    pub fn recovering(&self) {
        lock(&self.server).to(ServerState::Recovering, "config loaded");
    }

    /// The engine is open: the table's metadata is published.
    pub fn warming(&self) {
        lock(&self.server).to(ServerState::Warming, "WAL replayed, catalog loaded");
        let mut t = Tracker::new(TableState::Creating);
        t.to(TableState::Active, "metadata published");
        *lock(&self.table) = Some(t);
    }

    /// Serving: the listener is bound.
    pub fn ready(&self) {
        lock(&self.server).to(ServerState::Ready, "listener bound");
    }

    /// SIGTERM. The spec has no `DEGRADED → DRAINING` edge, so a degraded
    /// server stops from `DEGRADED` (its time there is still recorded).
    pub fn draining(&self) {
        let mut s = lock(&self.server);
        if s.state() == ServerState::Ready {
            s.to(ServerState::Draining, "SIGTERM");
        }
    }

    /// Queries finished and memtables flushed (the engine is closed).
    pub fn stopped(&self) {
        let mut s = lock(&self.server);
        if s.state() == ServerState::Draining {
            s.to(ServerState::Stopped, "drained");
        }
    }

    /// Re-evaluate device, table and server health.
    pub fn tick(&self, engine: &Engine, now: Instant) {
        // Retained obsolete segments above 10% of the disk (spec
        // STRATA-SYS-17-005): warn, at most every 5 minutes, naming the
        // oldest holders.
        let snaps = engine.snapshots();
        if snaps.warn {
            let mut last = lock(&self.retained_warned);
            if last.is_none_or(|t| now.saturating_duration_since(t) >= Duration::from_secs(300)) {
                let holders: Vec<String> = snaps
                    .open
                    .iter()
                    .filter(|s| s.obsolete_segments_pinned > 0)
                    .take(5)
                    .map(|s| format!("{}({} ms)", s.holder.as_deref().unwrap_or("?"), s.age_ms))
                    .collect();
                tracing::warn!(
                    component = "storage",
                    retained_bytes = snaps.obsolete_bytes,
                    ratio = snaps.obsolete_ratio,
                    holders = ?holders,
                    "replaced segments retained on disk exceed 10% of the volume; long-running queries pin them (GET /api/v1/admin/snapshots)"
                );
                *last = Some(now);
            }
        }
        let failed = engine.failure();
        let fsync = engine.stats().wal_metrics.sync_seconds;
        let device = lock(&self.device).observe(now, &fsync, failed.is_some());
        let read_only = failed.is_some() || engine.disk_below_reserve();
        let table_ro = {
            let mut g = lock(&self.table);
            if let Some(t) = g.as_mut() {
                match (t.state(), read_only) {
                    (TableState::Active, true) => {
                        let why = match &failed {
                            Some(f) => format!("engine failed: {f}"),
                            None => "disk space below the reserve".to_owned(),
                        };
                        tracing::warn!(component = "server", reason = %why, "table logs READ_ONLY");
                        t.to(TableState::ReadOnly, &why);
                    }
                    (TableState::ReadOnly, false) => {
                        tracing::info!(component = "server", "table logs ACTIVE again");
                        t.to(TableState::Active, "condition cleared");
                    }
                    _ => {}
                }
            }
            g.as_ref()
                .is_some_and(|t| t.state() == TableState::ReadOnly)
        };
        let degraded = table_ro || matches!(device, DeviceState::Degraded | DeviceState::Failed);
        let mut s = lock(&self.server);
        match (s.state(), degraded) {
            (ServerState::Ready, true) => {
                tracing::warn!(component = "server", "server DEGRADED");
                s.to(ServerState::Degraded, "device degraded or table read-only");
            }
            (ServerState::Degraded, false) => {
                tracing::info!(component = "server", "server READY again");
                s.to(ServerState::Ready, "condition cleared");
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use stratadb_common::metrics::Histogram;

    const B: &[f64] = &[
        0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.1,
    ];

    #[test]
    fn device_degrades_after_sustained_slow_windows_and_recovers() {
        let h = Histogram::new(B);
        let mut m = DeviceMonitor::new(DeviceThresholds::default());
        let t0 = Instant::now();
        let at = |s: u64| t0 + Duration::from_secs(s);
        let feed = |n: usize, v: f64| (0..n).for_each(|_| h.observe(v));
        assert_eq!(m.observe(at(0), &h.snapshot(), false), DeviceState::Unknown);
        feed(100, 0.0004);
        assert_eq!(
            m.observe(at(10), &h.snapshot(), false),
            DeviceState::Healthy
        );
        assert_eq!(m.baseline(), Some(0.0005));
        // Slow (25 ms vs 0.5 ms) but not for 30 s yet.
        feed(50, 0.02);
        assert_eq!(
            m.observe(at(20), &h.snapshot(), false),
            DeviceState::Healthy
        );
        // One normal window resets the clock.
        feed(50, 0.0004);
        assert_eq!(
            m.observe(at(30), &h.snapshot(), false),
            DeviceState::Healthy
        );
        for (i, s) in [40, 50, 60].into_iter().enumerate() {
            feed(50, 0.02);
            let st = m.observe(at(s), &h.snapshot(), false);
            assert_eq!(st, DeviceState::Healthy, "window {i}");
        }
        feed(50, 0.02);
        assert_eq!(
            m.observe(at(70), &h.snapshot(), false),
            DeviceState::Degraded
        );
        // Too-small windows are no evidence; normal latency for 5 min recovers.
        feed(50, 0.0004);
        assert_eq!(
            m.observe(at(80), &h.snapshot(), false),
            DeviceState::Degraded
        );
        feed(3, 0.02);
        assert_eq!(
            m.observe(at(200), &h.snapshot(), false),
            DeviceState::Degraded
        );
        feed(50, 0.0004);
        assert_eq!(
            m.observe(at(380), &h.snapshot(), false),
            DeviceState::Healthy
        );
        // I/O failure.
        assert_eq!(m.observe(at(390), &h.snapshot(), true), DeviceState::Failed);
        feed(100, 0.0004);
        assert_eq!(
            m.observe(at(400), &h.snapshot(), false),
            DeviceState::Failed
        );
    }

    #[test]
    fn unknown_device_needs_a_baseline_and_cannot_fail() {
        let h = Histogram::new(B);
        let mut m = DeviceMonitor::new(DeviceThresholds::default());
        (0..10).for_each(|_| h.observe(0.001));
        let t = Instant::now();
        assert_eq!(m.observe(t, &h.snapshot(), false), DeviceState::Unknown);
        // The spec has no UNKNOWN → FAILED edge.
        assert_eq!(m.observe(t, &h.snapshot(), true), DeviceState::Unknown);
        assert_eq!(m.baseline(), None);
    }
}
