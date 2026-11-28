//! Query admission and workload classes (spec VIII.11, STRATA-SYS-11-001,
//! 11-003, 11-004, 11-005, 11-010).
//!
//! - **Classes.** A query is classified at plan time from its metadata cost
//!   estimate: `interactive` (< 100 ms of estimated CPU), `normal`, or
//!   `batch` (≥ 10 s). An operator can set a tenant's class in the config;
//!   a request may only lower its own class.
//! - **Slots.** Each class has concurrent slots under the global
//!   `max_concurrent_queries`. A query that cannot run now waits in a
//!   bounded queue. When a slot frees, the next waiter is chosen by a
//!   [`FairQueue`]: stride scheduling by class weight over measured CPU
//!   time, then per tenant, then FIFO, with aging.
//! - **Admission** also requires memory headroom in the query pool and no
//!   severe memory pressure (Linux PSI). Refusals carry a code and a retry
//!   hint: `TOO_MANY_QUERIES` (queue full or queue timeout, HTTP 429) or
//!   `SERVER_OVERLOADED` (pressure, HTTP 503).
//! - **Demotion.** A running query past twice its class budget moves down
//!   one class: its slot moves with it, and its scan tasks are scheduled in
//!   the lower class from their next morsel.
//! - **AIMD.** Every interval, if interactive p95 latency is above target
//!   while CPU utilization is above 90%, batch slots shrink by one; when
//!   latency is below target and utilization is below 70%, they grow by one
//!   back towards the configured value.
//!
//! The controller holds one mutex. Nothing under it does I/O: pressure
//! readings are taken outside it and stored.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use stratadb_common::memory::MemoryTracker;
use stratadb_common::metrics::Histogram;
use stratadb_common::workload::{ClassCell, Classifier, FairQueue, WorkClass};
use tokio::sync::oneshot;

/// Workload settings (`[workload]` in the config file).
#[derive(Debug, Clone, PartialEq)]
pub struct WorkloadSettings {
    /// Slots per query class (each also bounded by the global
    /// `max_concurrent_queries`). `None`: derived from the global limit.
    pub interactive_slots: Option<usize>,
    pub normal_slots: Option<usize>,
    pub batch_slots: Option<usize>,
    /// The AIMD controller never takes batch below this.
    pub batch_slots_min: usize,
    /// Queries waiting for a slot, all classes together.
    pub queue_max: usize,
    /// Longest wait in the queue (also bounded by the query timeout).
    pub queue_timeout: Duration,
    pub classifier: Classifier,
    /// Interactive p95 latency target of the AIMD controller.
    pub latency_target: Duration,
    pub controller_interval: Duration,
    /// Aging interval of the fair queue (+1 priority per interval).
    pub aging: Duration,
    /// Memory the query pool must have free to admit a query.
    pub memory_floor_bytes: u64,
    /// PSI `memory full avg10` (%) above which only interactive queries are
    /// admitted.
    pub overload_memory_full_pct: f64,
    /// Operator-assigned class per tenant.
    pub tenant_classes: std::collections::BTreeMap<String, WorkClass>,
}

impl Default for WorkloadSettings {
    fn default() -> Self {
        Self {
            interactive_slots: None,
            normal_slots: None,
            batch_slots: None,
            batch_slots_min: 1,
            queue_max: 64,
            queue_timeout: Duration::from_secs(5),
            classifier: Classifier::default(),
            latency_target: Duration::from_millis(250),
            controller_interval: Duration::from_secs(1),
            aging: Duration::from_millis(500),
            memory_floor_bytes: 1 << 20,
            overload_memory_full_pct: 10.0,
            tenant_classes: Default::default(),
        }
    }
}

impl WorkloadSettings {
    /// Slots per class for a global limit of `total`.
    pub fn slots(&self, total: usize) -> [usize; 6] {
        let total = total.max(1);
        let mut s = [0; 6];
        s[WorkClass::Interactive.index()] = self.interactive_slots.unwrap_or(total).clamp(1, total);
        s[WorkClass::Normal.index()] = self
            .normal_slots
            .unwrap_or((total * 3 / 4).max(1))
            .clamp(1, total);
        s[WorkClass::Batch.index()] = self
            .batch_slots
            .unwrap_or((total / 4).max(1))
            .clamp(1, total);
        s
    }

    pub fn validate(&self) -> stratadb_common::Result<()> {
        let bad = |m: &str| Err(stratadb_common::Error::invalid_input(m.to_owned()));
        if self.queue_max == 0 || self.queue_max > 100_000 {
            return bad("workload.queue_max must be in 1..=100000");
        }
        if [self.interactive_slots, self.normal_slots, self.batch_slots].contains(&Some(0)) {
            return bad("workload slots must be at least 1");
        }
        if self.batch_slots_min == 0 {
            return bad("workload.batch_slots_min must be at least 1");
        }
        if !(self.classifier.interactive_ms > 0.0
            && self.classifier.batch_ms > self.classifier.interactive_ms)
        {
            return bad("workload: need 0 < interactive_ms < batch_ms");
        }
        if self.controller_interval < Duration::from_millis(10) {
            return bad("workload.controller_interval_ms must be at least 10");
        }
        if self
            .tenant_classes
            .values()
            .any(|c| !WorkClass::QUERY.contains(c))
        {
            return bad("workload.tenant_classes values must be interactive, normal or batch");
        }
        Ok(())
    }
}

/// Why a query was not admitted.
#[derive(Debug, Clone, PartialEq)]
pub enum Refusal {
    /// `TOO_MANY_QUERIES`: the queue is full or the wait timed out.
    TooManyQueries {
        retry_after: Duration,
        detail: String,
    },
    /// `SERVER_OVERLOADED`: resource pressure.
    Overloaded {
        retry_after: Duration,
        detail: String,
    },
    /// Cancelled while queued.
    Cancelled,
}

impl Refusal {
    pub fn code(&self) -> &'static str {
        match self {
            Refusal::TooManyQueries { .. } => "TOO_MANY_QUERIES",
            Refusal::Overloaded { .. } => "SERVER_OVERLOADED",
            Refusal::Cancelled => "CANCELLED",
        }
    }
}

/// Pressure stall information (Linux PSI, `avg10` in percent).
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Pressure {
    pub cpu_some: Option<f64>,
    pub memory_some: Option<f64>,
    pub memory_full: Option<f64>,
}

impl Pressure {
    /// Read `/proc/pressure/{cpu,memory}`; fields are `None` where PSI is
    /// unavailable (other systems, older kernels).
    pub fn read() -> Self {
        fn avg10(path: &str, kind: &str) -> Option<f64> {
            let text = std::fs::read_to_string(path).ok()?;
            parse_avg10(&text, kind)
        }
        Self {
            cpu_some: avg10("/proc/pressure/cpu", "some"),
            memory_some: avg10("/proc/pressure/memory", "some"),
            memory_full: avg10("/proc/pressure/memory", "full"),
        }
    }
}

/// `avg10` of the `kind` (`some`/`full`) line of a PSI file.
pub fn parse_avg10(text: &str, kind: &str) -> Option<f64> {
    text.lines()
        .find(|l| l.starts_with(kind))?
        .split_whitespace()
        .find_map(|f| f.strip_prefix("avg10="))?
        .parse()
        .ok()
}

/// Per-class counters.
#[derive(Debug, Default)]
struct ClassStats {
    arrivals: u64,
    admitted: u64,
    rejected_full: u64,
    rejected_overloaded: u64,
    timeouts: u64,
    cancelled: u64,
    demoted: u64,
    completed: u64,
    service: Duration,
    cpu: Duration,
}

struct Waiter {
    id: u64,
    /// The server's key for the query (status requests show its position).
    key: String,
    tx: oneshot::Sender<Grant>,
}

/// What the dispatcher hands a waiter.
struct Grant {
    popped_class: WorkClass,
    waited: Duration,
}

struct Running {
    cell: Arc<ClassCell>,
    /// Fair-queue identity: the class it was dequeued from and its tenant.
    popped: WorkClass,
    tenant: u64,
    charged_ns: f64,
    /// The class whose slot it holds.
    slot: WorkClass,
    started: Instant,
}

struct Core {
    queue: FairQueue<Waiter>,
    limits: [usize; 6],
    configured: [usize; 6],
    total: usize,
    running: [usize; 6],
    running_total: usize,
    runs: HashMap<u64, Running>,
    stats: [ClassStats; 6],
    pressure: Pressure,
    /// Interactive latencies since the last controller tick (bounded).
    window: Vec<Duration>,
    last_tick: Instant,
    last_cpu: Duration,
    last_arrivals: [u64; 6],
    arrival_rate: [f64; 6],
    utilization: f64,
    adjustments: u64,
}

/// The admission controller.
pub struct Admission {
    core: Mutex<Core>,
    s: WorkloadSettings,
    memory: Option<Arc<MemoryTracker>>,
    next_id: AtomicU64,
    wait_seconds: Vec<Histogram>,
    cpus: usize,
}

impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission").finish_non_exhaustive()
    }
}

/// Bounds the latency window between controller ticks.
const WINDOW_MAX: usize = 4096;

fn lock(m: &Mutex<Core>) -> MutexGuard<'_, Core> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// A query's admission request.
#[derive(Debug, Clone)]
pub struct Ask {
    pub class: WorkClass,
    pub tenant: u64,
    /// Estimated CPU (charged until the measured CPU is known).
    pub est_cpu_ms: f64,
    /// Longest wait (the query timeout; also capped by the queue timeout).
    pub max_wait: Duration,
    /// Set by `DELETE /api/v1/query/{id}` while the query waits.
    pub cancel: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// The server's key for the query (see [`Admission::position`]).
    pub key: String,
}

/// An admitted query's slot. Dropping it frees the slot and lets the next
/// waiter in; call [`Permit::finish`] first to record its CPU time.
pub struct Permit {
    adm: Arc<Admission>,
    id: u64,
    pub cell: Arc<ClassCell>,
    /// Time spent queued.
    pub waited: Duration,
    cpu: Duration,
}

impl std::fmt::Debug for Permit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Permit")
            .field("class", &self.cell.get())
            .field("waited", &self.waited)
            .finish_non_exhaustive()
    }
}

impl Permit {
    /// The query's current class (after any demotion).
    pub fn class(&self) -> WorkClass {
        self.cell.get()
    }

    /// Record the query's measured CPU time (charged to its class and
    /// tenant when the permit drops).
    pub fn finish(&mut self, cpu: Duration) {
        self.cpu = cpu;
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.adm.release(self.id, self.cpu);
    }
}

/// One class in [`Admission::stats`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ClassReport {
    pub class: &'static str,
    pub slots: usize,
    pub configured_slots: usize,
    pub running: usize,
    pub queued: usize,
    pub arrivals_total: u64,
    pub admitted_total: u64,
    pub rejected_queue_full_total: u64,
    pub rejected_overloaded_total: u64,
    pub queue_timeouts_total: u64,
    pub cancelled_queued_total: u64,
    pub demoted_total: u64,
    /// Arrivals per second over the last controller interval.
    pub arrival_rate: f64,
    pub mean_service_ms: f64,
    /// `arrival_rate × mean_service / slots` (Little's law, spec VIII.11.4).
    pub utilization: f64,
    pub queue_wait_p99_ms: Option<f64>,
    pub cpu_seconds_total: f64,
}

/// [`Admission::stats`].
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct WorkloadReport {
    pub max_concurrent_queries: usize,
    pub running: usize,
    pub queued: usize,
    pub queue_max: usize,
    pub cpu_utilization: f64,
    pub batch_adjustments_total: u64,
    pub pressure_cpu_some_avg10: Option<f64>,
    pub pressure_memory_full_avg10: Option<f64>,
    pub classes: Vec<ClassReport>,
}

impl Admission {
    /// A controller for `total` concurrent queries.
    pub fn new(total: usize, s: WorkloadSettings, memory: Option<Arc<MemoryTracker>>) -> Arc<Self> {
        let limits = s.slots(total);
        let now = Instant::now();
        Arc::new(Self {
            core: Mutex::new(Core {
                queue: FairQueue::new(s.aging),
                limits,
                configured: limits,
                total: total.max(1),
                running: [0; 6],
                running_total: 0,
                runs: HashMap::new(),
                stats: Default::default(),
                pressure: Pressure::default(),
                window: Vec::new(),
                last_tick: now,
                last_cpu: stratadb_common::cpu::process_cpu_time(),
                last_arrivals: [0; 6],
                arrival_rate: [0.0; 6],
                utilization: 0.0,
                adjustments: 0,
            }),
            s,
            memory,
            next_id: AtomicU64::new(1),
            wait_seconds: WorkClass::ALL
                .iter()
                .map(|_| Histogram::latency())
                .collect(),
            cpus: stratadb_common::threads::logical_cpus().max(1),
        })
    }

    pub fn settings(&self) -> &WorkloadSettings {
        &self.s
    }

    /// The class for a query: the operator's tenant class if set, else the
    /// plan-time estimate; a request may lower it (`requested`), never
    /// raise it.
    pub fn classify(
        &self,
        tenant: &str,
        est_cpu_ms: f64,
        requested: Option<WorkClass>,
    ) -> WorkClass {
        let base = self
            .s
            .tenant_classes
            .get(tenant)
            .copied()
            .unwrap_or_else(|| self.s.classifier.classify(est_cpu_ms));
        match requested {
            Some(r) if WorkClass::QUERY.contains(&r) && r > base => r,
            _ => base,
        }
    }

    fn memory_ok(&self) -> bool {
        self.memory.as_ref().is_none_or(|m| {
            m.limit() == 0 || m.used().saturating_add(self.s.memory_floor_bytes) <= m.limit()
        })
    }

    /// Hand free slots to waiters in fair order.
    fn dispatch(&self, c: &mut Core) {
        let memory_ok = self.memory_ok();
        let now = Instant::now();
        loop {
            let limits = c.limits;
            let (running, total, running_total) = (c.running, c.total, c.running_total);
            let eligible = |k: WorkClass| {
                memory_ok && running_total < total && running[k.index()] < limits[k.index()]
            };
            let Some(p) = c.queue.pop_where(now, eligible) else {
                return;
            };
            c.running[p.class.index()] += 1;
            c.running_total += 1;
            let grant = Grant {
                popped_class: p.class,
                waited: p.waited,
            };
            let id = p.item.id;
            if p.item.tx.send(grant).is_err() {
                // The waiter is gone (client disconnected): undo.
                c.running[p.class.index()] -= 1;
                c.running_total -= 1;
                c.queue.settle(p.class, p.tenant, p.charged_ns, 0.0);
                c.stats[p.class.index()].cancelled += 1;
                continue;
            }
            c.runs.insert(
                id,
                Running {
                    cell: Arc::new(ClassCell::new(p.class)),
                    popped: p.class,
                    tenant: p.tenant,
                    charged_ns: p.charged_ns,
                    slot: p.class,
                    started: now,
                },
            );
            c.stats[p.class.index()].admitted += 1;
            self.wait_seconds[p.class.index()].observe_duration(p.waited);
        }
    }

    /// Demote running queries past twice their class budget.
    fn demote_overdue(&self, c: &mut Core, now: Instant) {
        let mut moved = Vec::new();
        for r in c.runs.values_mut() {
            let class = r.cell.get();
            let Some(budget) = self.s.classifier.budget(class) else {
                continue;
            };
            if now.saturating_duration_since(r.started) > budget * 2 {
                let lower = class.demoted();
                r.cell.set(lower);
                moved.push((r.slot, lower));
                r.slot = lower;
            }
        }
        for (from, to) in moved {
            c.running[from.index()] = c.running[from.index()].saturating_sub(1);
            c.running[to.index()] += 1;
            c.stats[from.index()].demoted += 1;
        }
    }

    /// Admit, queue or refuse a query. Resolves when it may run.
    pub async fn admit(self: &Arc<Self>, ask: Ask) -> Result<Permit, Refusal> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let class = ask.class;
        let rx = {
            let mut c = lock(&self.core);
            let now = Instant::now();
            self.demote_overdue(&mut c, now);
            c.stats[class.index()].arrivals += 1;
            if class != WorkClass::Interactive
                && c.pressure
                    .memory_full
                    .is_some_and(|p| p > self.s.overload_memory_full_pct)
            {
                c.stats[class.index()].rejected_overloaded += 1;
                return Err(Refusal::Overloaded {
                    retry_after: Duration::from_secs(1),
                    detail: "memory pressure is high; only interactive queries are admitted".into(),
                });
            }
            if c.queue.len() >= self.s.queue_max {
                c.stats[class.index()].rejected_full += 1;
                return Err(Refusal::TooManyQueries {
                    retry_after: self.retry_after(&c, class),
                    detail: format!("the query queue is full ({} waiting)", c.queue.len()),
                });
            }
            let (tx, rx) = oneshot::channel();
            let est_ns = (ask.est_cpu_ms * 1e6).max(1e5);
            c.queue.push(
                class,
                ask.tenant,
                Waiter {
                    id,
                    key: ask.key.clone(),
                    tx,
                },
                est_ns,
                now,
            );
            self.dispatch(&mut c);
            rx
        };
        let wait = ask.max_wait.min(self.s.queue_timeout);
        let deadline = tokio::time::Instant::now() + wait;
        let mut rx = rx;
        loop {
            let tick = tokio::time::sleep(Duration::from_millis(50));
            tokio::select! {
                g = &mut rx => {
                    return match g {
                        Ok(g) => Ok(self.permit(id, g)),
                        // The sender is only dropped after removal below.
                        Err(_) => Err(Refusal::Cancelled),
                    };
                }
                () = tick => {}
            }
            let cancelled = ask
                .cancel
                .as_ref()
                .is_some_and(|f| f.load(Ordering::Relaxed));
            if cancelled || tokio::time::Instant::now() >= deadline {
                let mut c = lock(&self.core);
                if c.queue.remove(|w| w.id == id).is_some() {
                    let st = &mut c.stats[class.index()];
                    if cancelled {
                        st.cancelled += 1;
                        return Err(Refusal::Cancelled);
                    }
                    st.timeouts += 1;
                    return Err(Refusal::TooManyQueries {
                        retry_after: self.retry_after(&c, class),
                        detail: format!(
                            "waited {} ms in the queue without a free slot",
                            wait.as_millis()
                        ),
                    });
                }
                drop(c);
                // Granted meanwhile: take the slot (and release it at once
                // if the query was cancelled).
                if let Ok(g) = rx.try_recv() {
                    let p = self.permit(id, g);
                    if cancelled {
                        drop(p);
                        return Err(Refusal::Cancelled);
                    }
                    return Ok(p);
                }
            }
        }
    }

    fn permit(self: &Arc<Self>, id: u64, g: Grant) -> Permit {
        let cell = lock(&self.core)
            .runs
            .get(&id)
            .map(|r| Arc::clone(&r.cell))
            .unwrap_or_else(|| Arc::new(ClassCell::new(g.popped_class)));
        Permit {
            adm: Arc::clone(self),
            id,
            cell,
            waited: g.waited,
            cpu: Duration::ZERO,
        }
    }

    /// Approximate queue position (0 = next) of a waiting query.
    pub fn position(&self, key: &str) -> Option<usize> {
        lock(&self.core).queue.position(|w| w.key == key)
    }

    fn release(&self, id: u64, cpu: Duration) {
        let mut c = lock(&self.core);
        let Some(r) = c.runs.remove(&id) else {
            return;
        };
        let elapsed = r.started.elapsed();
        c.running[r.slot.index()] = c.running[r.slot.index()].saturating_sub(1);
        c.running_total = c.running_total.saturating_sub(1);
        // Settle against the class and tenant it was dequeued from (the fair
        // queue's accounting); the CPU is reported under the class it ended
        // in.
        c.queue
            .settle(r.popped, r.tenant, r.charged_ns, cpu.as_nanos() as f64);
        let st = &mut c.stats[r.slot.index()];
        st.completed += 1;
        st.service += elapsed;
        st.cpu += cpu;
        if r.popped == WorkClass::Interactive && c.window.len() < WINDOW_MAX {
            c.window.push(elapsed);
        }
        self.demote_overdue(&mut c, Instant::now());
        self.dispatch(&mut c);
    }

    fn retry_after(&self, c: &Core, class: WorkClass) -> Duration {
        let st = &c.stats[class.index()];
        let mean_ms = if st.completed > 0 {
            st.service.as_secs_f64() * 1000.0 / st.completed as f64
        } else {
            1000.0
        };
        let slots = c.limits[class.index()].max(1) as f64;
        let ms = mean_ms * (c.queue.len_of(class) + 1) as f64 / slots;
        Duration::from_millis(ms.clamp(100.0, 30_000.0) as u64)
    }

    /// Controller tick (every `controller_interval`): read pressure, adapt
    /// batch slots (AIMD), update rates, demote, dispatch.
    pub fn tick(&self) {
        let pressure = Pressure::read(); // file reads outside the lock
        let cpu_now = stratadb_common::cpu::process_cpu_time();
        let mut c = lock(&self.core);
        let now = Instant::now();
        let wall = now.saturating_duration_since(c.last_tick);
        if wall.is_zero() {
            return;
        }
        let util = (cpu_now.saturating_sub(c.last_cpu).as_secs_f64()
            / (wall.as_secs_f64() * self.cpus as f64))
            .min(1.0);
        let p95 = p95(&mut c.window);
        self.adjust(&mut c, p95, util);
        for k in WorkClass::ALL {
            let a = c.stats[k.index()].arrivals;
            c.arrival_rate[k.index()] =
                (a - c.last_arrivals[k.index()]) as f64 / wall.as_secs_f64();
            c.last_arrivals[k.index()] = a;
        }
        c.utilization = util;
        c.pressure = pressure;
        c.last_tick = now;
        c.last_cpu = cpu_now;
        c.window.clear();
        self.demote_overdue(&mut c, now);
        self.dispatch(&mut c);
    }

    /// One AIMD step on the batch slots (STRATA-SYS-11-005).
    fn adjust(&self, c: &mut Core, interactive_p95: Option<Duration>, util: f64) {
        let b = WorkClass::Batch.index();
        let over = interactive_p95.is_some_and(|p| p > self.s.latency_target);
        if over && util > 0.9 {
            let next = c.limits[b].saturating_sub(1).max(self.s.batch_slots_min);
            if next != c.limits[b] {
                tracing::info!(
                    component = "admission",
                    batch_slots = next,
                    p95_ms = interactive_p95.map(|d| d.as_secs_f64() * 1e3),
                    util,
                    "interactive latency over target under load: fewer batch slots"
                );
                c.limits[b] = next;
                c.adjustments += 1;
            }
        } else if !over && util < 0.7 && c.limits[b] < c.configured[b] {
            c.limits[b] += 1;
            c.adjustments += 1;
        }
    }

    /// Test hook: run the AIMD step with given observations.
    #[doc(hidden)]
    pub fn adjust_for_test(&self, interactive_p95: Option<Duration>, util: f64) -> usize {
        let mut c = lock(&self.core);
        self.adjust(&mut c, interactive_p95, util);
        self.dispatch(&mut c);
        c.limits[WorkClass::Batch.index()]
    }

    /// Test hook: pretend PSI reported `memory_full`.
    #[doc(hidden)]
    pub fn set_pressure_for_test(&self, p: Pressure) {
        lock(&self.core).pressure = p;
    }

    pub fn stats(&self) -> WorkloadReport {
        let c = lock(&self.core);
        let classes = WorkClass::QUERY
            .iter()
            .map(|&k| {
                let st = &c.stats[k.index()];
                let mean_ms = if st.completed > 0 {
                    st.service.as_secs_f64() * 1000.0 / st.completed as f64
                } else {
                    0.0
                };
                let slots = c.limits[k.index()];
                let rate = c.arrival_rate[k.index()];
                ClassReport {
                    class: k.name(),
                    slots,
                    configured_slots: c.configured[k.index()],
                    running: c.running[k.index()],
                    queued: c.queue.len_of(k),
                    arrivals_total: st.arrivals,
                    admitted_total: st.admitted,
                    rejected_queue_full_total: st.rejected_full,
                    rejected_overloaded_total: st.rejected_overloaded,
                    queue_timeouts_total: st.timeouts,
                    cancelled_queued_total: st.cancelled,
                    demoted_total: st.demoted,
                    arrival_rate: rate,
                    mean_service_ms: mean_ms,
                    utilization: rate * mean_ms / 1000.0 / slots.max(1) as f64,
                    queue_wait_p99_ms: self.wait_seconds[k.index()]
                        .snapshot()
                        .quantile_upper_bound(0.99)
                        .map(|s| s * 1000.0),
                    cpu_seconds_total: st.cpu.as_secs_f64(),
                }
            })
            .collect();
        WorkloadReport {
            max_concurrent_queries: c.total,
            running: c.running_total,
            queued: c.queue.len(),
            queue_max: self.s.queue_max,
            cpu_utilization: c.utilization,
            batch_adjustments_total: c.adjustments,
            pressure_cpu_some_avg10: c.pressure.cpu_some,
            pressure_memory_full_avg10: c.pressure.memory_full,
            classes,
        }
    }

    /// Prometheus text (`strata_workload_*`).
    pub fn render(&self, out: &mut String) {
        use std::fmt::Write as _;
        let r = self.stats();
        let fam = |out: &mut String, name: &str, kind: &str, help: &str| {
            let _ = writeln!(
                out,
                "# HELP strata_workload_{name} {help}\n# TYPE strata_workload_{name} {kind}"
            );
        };
        fam(
            out,
            "slots",
            "gauge",
            "Concurrent query slots per class (batch adapts, AIMD).",
        );
        for c in &r.classes {
            let _ = writeln!(
                out,
                "strata_workload_slots{{class=\"{}\"}} {}",
                c.class, c.slots
            );
        }
        fam(out, "running", "gauge", "Queries running per class.");
        for c in &r.classes {
            let _ = writeln!(
                out,
                "strata_workload_running{{class=\"{}\"}} {}",
                c.class, c.running
            );
        }
        fam(
            out,
            "queued",
            "gauge",
            "Queries waiting for a slot per class.",
        );
        for c in &r.classes {
            let _ = writeln!(
                out,
                "strata_workload_queued{{class=\"{}\"}} {}",
                c.class, c.queued
            );
        }
        fam(
            out,
            "queries_total",
            "counter",
            "Queries by class and admission outcome.",
        );
        for c in &r.classes {
            for (o, v) in [
                ("admitted", c.admitted_total),
                ("queue_full", c.rejected_queue_full_total),
                ("overloaded", c.rejected_overloaded_total),
                ("queue_timeout", c.queue_timeouts_total),
                ("cancelled_queued", c.cancelled_queued_total),
            ] {
                let _ = writeln!(
                    out,
                    "strata_workload_queries_total{{class=\"{}\",outcome=\"{o}\"}} {v}",
                    c.class
                );
            }
        }
        fam(
            out,
            "demotions_total",
            "counter",
            "Running queries demoted out of the class after 2x its time budget.",
        );
        for c in &r.classes {
            let _ = writeln!(
                out,
                "strata_workload_demotions_total{{class=\"{}\"}} {}",
                c.class, c.demoted_total
            );
        }
        fam(
            out,
            "cpu_seconds_total",
            "counter",
            "CPU time of finished queries by the class they ended in.",
        );
        for c in &r.classes {
            let _ = writeln!(
                out,
                "strata_workload_cpu_seconds_total{{class=\"{}\"}} {}",
                c.class, c.cpu_seconds_total
            );
        }
        let _ = writeln!(out, "# HELP strata_workload_queue_wait_seconds Time queries waited for a slot.\n# TYPE strata_workload_queue_wait_seconds histogram");
        for k in WorkClass::QUERY {
            self.wait_seconds[k.index()].snapshot().render(
                out,
                "strata_workload_queue_wait_seconds",
                "",
                &format!("class=\"{}\"", k.name()),
                false,
            );
        }
    }
}

/// p95 of the window (sorted in place); `None` when empty.
fn p95(v: &mut [Duration]) -> Option<Duration> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let i = ((v.len() as f64 * 0.95).ceil() as usize).clamp(1, v.len()) - 1;
    Some(v[i])
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn settings() -> WorkloadSettings {
        WorkloadSettings {
            queue_timeout: Duration::from_secs(10),
            ..WorkloadSettings::default()
        }
    }

    fn ask(class: WorkClass, tenant: u64, key: &str) -> Ask {
        Ask {
            class,
            tenant,
            est_cpu_ms: 1.0,
            max_wait: Duration::from_secs(10),
            cancel: None,
            key: key.to_owned(),
        }
    }

    /// Spawn an admission that reports its key when granted.
    fn spawn_waiter(
        a: &Arc<Admission>,
        x: Ask,
        tx: tokio::sync::mpsc::UnboundedSender<(String, Permit)>,
    ) -> tokio::task::JoinHandle<Result<(), Refusal>> {
        let a = Arc::clone(a);
        tokio::spawn(async move {
            let key = x.key.clone();
            let p = a.admit(x).await?;
            let _ = tx.send((key, p));
            Ok(())
        })
    }

    async fn queued(a: &Admission, n: usize) {
        for _ in 0..400 {
            if a.stats().queued == n {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("expected {n} queued, have {:?}", a.stats());
    }

    #[tokio::test]
    async fn waiters_are_served_by_class_then_tenant_then_fifo() {
        let a = Admission::new(1, settings(), None);
        let mut first = a.admit(ask(WorkClass::Batch, 1, "running")).await.unwrap();
        // Every query reports CPU equal to its estimate (1 ms).
        first.finish(Duration::from_millis(1));
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        // Arrival order: two batch queries of tenant 1, one of tenant 2, then
        // an interactive one.
        for (c, t, k) in [
            (WorkClass::Batch, 1, "b1"),
            (WorkClass::Batch, 1, "b2"),
            (WorkClass::Batch, 2, "c1"),
            (WorkClass::Interactive, 3, "i1"),
        ] {
            spawn_waiter(&a, ask(c, t, k), tx.clone());
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        queued(&a, 4).await;
        assert_eq!(a.position("i1"), Some(0));
        drop(first);
        let mut order = Vec::new();
        for _ in 0..4 {
            let (k, mut p) = rx.recv().await.unwrap();
            order.push(k);
            p.finish(Duration::from_millis(1));
            drop(p);
        }
        // Interactive first; tenants alternate within batch; FIFO per tenant.
        assert_eq!(order, ["i1", "b1", "c1", "b2"]);
        let r = a.stats();
        assert_eq!((r.running, r.queued), (0, 0));
    }

    #[tokio::test]
    async fn class_slots_bound_each_class_under_the_global_limit() {
        let s = WorkloadSettings {
            batch_slots: Some(1),
            ..settings()
        };
        let a = Admission::new(3, s, None);
        let b = a.admit(ask(WorkClass::Batch, 1, "b")).await.unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_waiter(&a, ask(WorkClass::Batch, 1, "b2"), tx.clone());
        queued(&a, 1).await;
        // A free global slot does not admit a second batch query, but an
        // interactive one runs at once.
        let i = a.admit(ask(WorkClass::Interactive, 1, "i")).await.unwrap();
        assert_eq!(a.stats().running, 2);
        drop(b);
        let (k, _p) = rx.recv().await.unwrap();
        assert_eq!(k, "b2");
        drop(i);
    }

    #[tokio::test]
    async fn full_queue_timeout_and_cancel_are_refused() {
        let s = WorkloadSettings {
            queue_max: 1,
            queue_timeout: Duration::from_millis(120),
            ..settings()
        };
        let a = Admission::new(1, s, None);
        let held = a.admit(ask(WorkClass::Normal, 1, "held")).await.unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let waiting = spawn_waiter(&a, ask(WorkClass::Normal, 1, "w"), tx.clone());
        queued(&a, 1).await;
        match a.admit(ask(WorkClass::Normal, 2, "x")).await.unwrap_err() {
            Refusal::TooManyQueries {
                retry_after,
                detail,
            } => {
                assert!(retry_after >= Duration::from_millis(100), "{retry_after:?}");
                assert!(detail.contains("queue is full"), "{detail}");
            }
            r => panic!("{r:?}"),
        }
        // The waiter times out after 120 ms.
        match waiting.await.unwrap().unwrap_err() {
            Refusal::TooManyQueries { detail, .. } => {
                assert!(detail.contains("waited"), "{detail}")
            }
            r => panic!("{r:?}"),
        }
        // Cancelled while queued.
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut x = ask(WorkClass::Normal, 1, "c");
        x.cancel = Some(Arc::clone(&flag));
        let a2 = Arc::clone(&a);
        let h = tokio::spawn(async move { a2.admit(x).await.map(|_| ()) });
        queued(&a, 1).await;
        flag.store(true, Ordering::Relaxed);
        assert_eq!(h.await.unwrap().unwrap_err(), Refusal::Cancelled);
        let r = a.stats();
        let n = &r.classes[1];
        assert_eq!(
            (
                n.rejected_queue_full_total,
                n.queue_timeouts_total,
                n.cancelled_queued_total
            ),
            (1, 1, 1)
        );
        drop(held);
        assert_eq!(a.stats().running, 0);
    }

    #[tokio::test]
    async fn a_waiter_that_goes_away_does_not_leak_its_slot() {
        let a = Admission::new(1, settings(), None);
        let held = a.admit(ask(WorkClass::Normal, 1, "held")).await.unwrap();
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let gone = spawn_waiter(&a, ask(WorkClass::Normal, 1, "gone"), tx.clone());
        queued(&a, 1).await;
        gone.abort(); // client disconnected
        let _ = gone.await;
        drop(held); // the slot goes to the departed waiter, then comes back
        let p = tokio::time::timeout(
            Duration::from_secs(2),
            a.admit(ask(WorkClass::Normal, 1, "next")),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(a.stats().running, 1);
        drop(p);
        assert_eq!(a.stats().running, 0);
    }

    #[tokio::test]
    async fn overdue_queries_are_demoted_and_free_their_slot() {
        let s = WorkloadSettings {
            interactive_slots: Some(1),
            classifier: Classifier {
                interactive_ms: 10.0,
                batch_ms: 10_000.0,
            },
            ..settings()
        };
        let a = Admission::new(4, s, None);
        let long = a
            .admit(ask(WorkClass::Interactive, 1, "long"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await; // > 2 × 10 ms
        a.tick();
        assert_eq!(long.class(), WorkClass::Normal);
        // Its interactive slot moved to normal: another interactive query runs.
        let short = tokio::time::timeout(
            Duration::from_secs(1),
            a.admit(ask(WorkClass::Interactive, 2, "short")),
        )
        .await
        .unwrap()
        .unwrap();
        let r = a.stats();
        assert_eq!(r.classes[0].demoted_total, 1);
        assert_eq!((r.classes[0].running, r.classes[1].running), (1, 1));
        drop((long, short));
        assert_eq!(a.stats().running, 0);
    }

    #[test]
    fn aimd_moves_batch_slots_between_min_and_configured() {
        let s = WorkloadSettings {
            batch_slots: Some(4),
            batch_slots_min: 2,
            ..settings()
        };
        let a = Admission::new(16, s, None);
        let slow = Some(Duration::from_secs(1));
        assert_eq!(a.adjust_for_test(slow, 0.95), 3);
        assert_eq!(a.adjust_for_test(slow, 0.95), 2);
        assert_eq!(a.adjust_for_test(slow, 0.95), 2, "floor");
        // Over target but CPU not saturated: hold.
        assert_eq!(a.adjust_for_test(slow, 0.8), 2);
        // Under target and idle: additive increase back to configured.
        let fast = Some(Duration::from_millis(5));
        assert_eq!(a.adjust_for_test(fast, 0.5), 3);
        assert_eq!(a.adjust_for_test(None, 0.5), 4);
        assert_eq!(a.adjust_for_test(fast, 0.5), 4, "ceiling");
        assert_eq!(a.stats().batch_adjustments_total, 4);
    }

    #[tokio::test]
    async fn memory_pressure_and_headroom_gate_admission() {
        let pool = MemoryTracker::root("query", 4 << 20);
        let a = Admission::new(4, settings(), Some(Arc::clone(&pool)));
        a.set_pressure_for_test(Pressure {
            memory_full: Some(50.0),
            ..Pressure::default()
        });
        match a.admit(ask(WorkClass::Batch, 1, "b")).await.unwrap_err() {
            Refusal::Overloaded { .. } => {}
            r => panic!("{r:?}"),
        }
        let i = a.admit(ask(WorkClass::Interactive, 1, "i")).await.unwrap();
        drop(i);
        a.set_pressure_for_test(Pressure::default());
        // Less than the 1 MiB floor free: queued until memory is released.
        let held = stratadb_common::memory::Reservation::new(
            &pool,
            (4 << 20) - (512 << 10),
            Duration::ZERO,
        )
        .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        spawn_waiter(&a, ask(WorkClass::Normal, 1, "m"), tx);
        queued(&a, 1).await;
        drop(held);
        a.tick(); // the controller re-dispatches when memory frees up
        let (k, _p) = tokio::time::timeout(Duration::from_secs(2), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(k, "m");
    }

    #[test]
    fn classification_respects_tenant_classes_and_never_raises() {
        let mut s = settings();
        s.tenant_classes.insert("reports".into(), WorkClass::Batch);
        let a = Admission::new(4, s, None);
        assert_eq!(a.classify("t", 1.0, None), WorkClass::Interactive);
        assert_eq!(a.classify("t", 500.0, None), WorkClass::Normal);
        assert_eq!(a.classify("t", 50_000.0, None), WorkClass::Batch);
        assert_eq!(
            a.classify("t", 1.0, Some(WorkClass::Batch)),
            WorkClass::Batch
        );
        assert_eq!(
            a.classify("t", 50_000.0, Some(WorkClass::Interactive)),
            WorkClass::Batch,
            "a request cannot raise its class"
        );
        assert_eq!(a.classify("reports", 1.0, None), WorkClass::Batch);
    }

    #[test]
    fn psi_lines_parse_and_settings_validate() {
        let text = "some avg10=1.50 avg60=0.40 avg300=0.10 total=123\nfull avg10=0.25 avg60=0.00 avg300=0.00 total=9\n";
        assert_eq!(parse_avg10(text, "some"), Some(1.5));
        assert_eq!(parse_avg10(text, "full"), Some(0.25));
        assert_eq!(parse_avg10("garbage", "some"), None);
        assert!(WorkloadSettings::default().validate().is_ok());
        let bad = WorkloadSettings {
            queue_max: 0,
            ..WorkloadSettings::default()
        };
        assert!(bad.validate().is_err());
        let s = WorkloadSettings::default().slots(16);
        assert_eq!([s[1], s[2], s[4]], [16, 12, 4]);
        let s = WorkloadSettings::default().slots(1);
        assert_eq!([s[1], s[2], s[4]], [1, 1, 1]);
    }
}
