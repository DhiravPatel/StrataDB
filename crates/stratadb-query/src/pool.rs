//! The process-wide scan worker pool (spec VIII.3, VIII.11.3).
//!
//! A fixed set of named threads (`sx-work-NN`, STRATA-SYS-03-015) executes
//! scan tasks of every query. No thread is created per query
//! (STRATA-SYS-01-010), and the thread count is fixed when the pool starts.
//! Tasks queued plus running never exceed 2 × workers across all queries
//! (STRATA-SYS-11-008); `normal` work may fill 3/4 of that and `batch` work
//! half, so higher classes always find room ([`class_cap`]). When the pool
//! is full for a class,
//! [`ScanPool::try_submit`] hands the task back and the submitting query
//! runs that share of its scan itself. Queries therefore never block on the
//! pool, and a saturated pool degrades to sequential scans instead of
//! queueing without bound.
//!
//! Tasks own their inputs (`'static`). A task that starts after its query
//! has finished finds no work left and returns immediately.
//!
//! **Fair scheduling (STRATA-SYS-11-001).** Tasks run one step (one morsel,
//! a segment) at a time. Between steps the worker settles the step's
//! measured CPU time into a [`FairQueue`]: stride scheduling by class
//! weight (interactive 8, normal 4, batch 1), then by tenant, then FIFO,
//! with aging. A task with more work is queued again under its query's
//! *current* class, so a demoted query loses priority at its next morsel
//! boundary, and interactive work preempts batch work at morsel
//! boundaries.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use stratadb_common::workload::{ClassCell, FairQueue, Popped, WorkClass};
use stratadb_common::{Error, Result};

type Job = Box<dyn FnOnce() + Send + 'static>;

/// What a task step reports back to the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// More work remains: queue the task again (under its current class).
    Again,
    Done,
}

/// A task run one step (one morsel) at a time.
pub type Task = Box<dyn FnMut() -> Step + Send + 'static>;

/// Who a task works for: its query's live class and its tenant.
#[derive(Debug, Clone)]
pub struct Tag {
    pub class: Arc<ClassCell>,
    /// Tenant key (see [`tenant_key`]).
    pub tenant: u64,
    /// Estimated CPU nanoseconds of one step (corrected by measurement).
    pub est_step_ns: f64,
}

impl Tag {
    /// A `normal` task of no particular tenant.
    pub fn normal() -> Self {
        Self {
            class: Arc::new(ClassCell::new(WorkClass::Normal)),
            tenant: 0,
            est_step_ns: 1e6,
        }
    }
}

/// A stable 64-bit key for a tenant id.
pub fn tenant_key(t: &stratadb_common::TenantId) -> u64 {
    use std::hash::{Hash, Hasher};
    // SipHash with fixed keys: stable within and across processes.
    let mut h = std::hash::DefaultHasher::new();
    t.as_str().hash(&mut h);
    h.finish()
}

/// Snapshot of pool state (metrics, EXPLAIN, tests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    pub workers: usize,
    /// Tasks queued or running.
    pub active_tasks: usize,
    /// Tasks ever submitted to the pool.
    pub submitted: usize,
    /// Tasks the pool could not take (queue full); run by the submitter.
    pub rejected: usize,
    /// Tasks that panicked (debug/test builds; release builds abort).
    pub panicked: usize,
    /// Task steps run (one morsel each).
    pub steps: usize,
}

/// Per-class step counts and CPU time (metrics).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClassUsage {
    pub steps: u64,
    pub cpu: Duration,
}

struct Queued {
    task: Task,
    tag: Tag,
}

struct State {
    queue: FairQueue<Queued>,
    shutdown: bool,
    usage: [ClassUsage; 6],
}

struct Inner {
    /// Queries executing per class (on any thread; see [`ScanPool::enter`]).
    executing: [AtomicUsize; 6],
    state: Mutex<State>,
    ready: Condvar,
    active: AtomicUsize,
    panicked: AtomicUsize,
    steps: AtomicUsize,
}

pub struct ScanPool {
    inner: Arc<Inner>,
    workers: usize,
    submitted: AtomicUsize,
    rejected: AtomicUsize,
}

impl std::fmt::Debug for ScanPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanPool")
            .field("workers", &self.workers)
            .finish_non_exhaustive()
    }
}

/// Upper bound on pool threads (STRATA-SYS-03-014 caps all engine threads at
/// `2 × logical_cpus + 32`).
pub const MAX_WORKERS: usize = 256;

/// Default aging interval (STRATA-SYS-11-001: +1 priority per 500 ms).
pub const AGING: Duration = Duration::from_millis(500);

static POOL: OnceLock<ScanPool> = OnceLock::new();
static CONFIGURED: OnceLock<usize> = OnceLock::new();

fn default_workers() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

/// Set the number of pool threads. Must be called before the first parallel
/// query (for example at server start); later calls fail.
pub fn configure_scan_pool(workers: usize) -> Result<()> {
    if !(1..=MAX_WORKERS).contains(&workers) {
        return Err(Error::invalid_input(format!(
            "scan pool workers must be in 1..={MAX_WORKERS}"
        )));
    }
    if POOL.get().is_some() {
        return Err(Error::invalid_input(
            "scan pool already started; configure it before the first query",
        ));
    }
    CONFIGURED
        .set(workers)
        .map_err(|_| Error::invalid_input("scan pool already configured"))
}

/// The process-wide pool (started on first use).
pub fn scan_pool() -> &'static ScanPool {
    POOL.get_or_init(|| {
        let n = CONFIGURED
            .get()
            .copied()
            .unwrap_or_else(default_workers)
            .clamp(1, MAX_WORKERS);
        ScanPool::start(n)
    })
}

/// Active tasks (queued + running) up to which a class may submit. Lower
/// classes stop short of the pool-wide 2 × workers, so a batch flood never
/// fills the queue: interactive work always finds room, then overtakes at
/// the next morsel boundary.
pub fn class_cap(workers: usize, class: WorkClass) -> usize {
    let all = 2 * workers;
    match class {
        WorkClass::System | WorkClass::Interactive => all,
        WorkClass::Normal | WorkClass::Ingest => (all * 3 / 4).max(1),
        WorkClass::Batch | WorkClass::Background => workers.max(1),
    }
}

fn lock(m: &Mutex<State>) -> std::sync::MutexGuard<'_, State> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl ScanPool {
    pub(crate) fn start(workers: usize) -> Self {
        let inner = Arc::new(Inner {
            executing: Default::default(),
            state: Mutex::new(State {
                queue: FairQueue::new(AGING),
                shutdown: false,
                usage: [ClassUsage::default(); 6],
            }),
            ready: Condvar::new(),
            active: AtomicUsize::new(0),
            panicked: AtomicUsize::new(0),
            steps: AtomicUsize::new(0),
        });
        let mut started = 0;
        for i in 0..workers {
            let inner = Arc::clone(&inner);
            let spawned = std::thread::Builder::new()
                .name(stratadb_common::threads::Role::Work.thread_name(i))
                .spawn(move || worker(&inner));
            if spawned.is_ok() {
                started += 1;
            }
        }
        // A pool whose threads could not start still works: every task is
        // rejected and runs on the submitting thread.
        Self {
            inner,
            workers: started,
            submitted: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
        }
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Tasks the pool can still take without exceeding 2 × workers active.
    pub fn free_slots(&self) -> usize {
        (2 * self.workers).saturating_sub(self.inner.active.load(Ordering::Relaxed))
    }

    /// Tasks a query of `class` can still submit (see [`class_cap`]).
    pub fn free_slots_for(&self, class: WorkClass) -> usize {
        class_cap(self.workers, class).saturating_sub(self.inner.active.load(Ordering::Relaxed))
    }

    /// Queue a stepped task, or hand it back if the pool is saturated (the
    /// caller then runs it itself).
    pub fn try_submit_task(&self, tag: Tag, task: Task) -> std::result::Result<(), Task> {
        let cap = class_cap(self.workers, tag.class.get());
        let admitted = self
            .inner
            .active
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |a| {
                (a < cap).then_some(a + 1)
            })
            .is_ok();
        if !admitted {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(task);
        }
        {
            let mut st = lock(&self.inner.state);
            let class = tag.class.get();
            let (tenant, est) = (tag.tenant, tag.est_step_ns);
            st.queue
                .push(class, tenant, Queued { task, tag }, est, Instant::now());
        }
        self.inner.ready.notify_one();
        self.submitted.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Queue a one-shot `normal` job (see [`ScanPool::try_submit_task`]).
    pub fn try_submit(&self, job: Job) -> std::result::Result<(), Job> {
        let cell = Arc::new(Mutex::new(Some(job)));
        let run = Arc::clone(&cell);
        let task: Task = Box::new(move || {
            let j = run.lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Some(j) = j {
                j();
            }
            Step::Done
        });
        match self.try_submit_task(Tag::normal(), task) {
            Ok(()) => Ok(()),
            Err(_) => {
                let j = cell.lock().unwrap_or_else(|e| e.into_inner()).take();
                // The task never ran, so the job is still there.
                Err(j.unwrap_or_else(|| Box::new(|| {})))
            }
        }
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            workers: self.workers,
            active_tasks: self.inner.active.load(Ordering::Relaxed),
            submitted: self.submitted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            panicked: self.inner.panicked.load(Ordering::Relaxed),
            steps: self.inner.steps.load(Ordering::Relaxed),
        }
    }

    /// Steps and CPU time run per class so far.
    pub fn class_usage(&self) -> Vec<(WorkClass, ClassUsage)> {
        let st = lock(&self.inner.state);
        WorkClass::ALL
            .iter()
            .map(|c| (*c, st.usage[c.index()]))
            .collect()
    }

    /// Tasks waiting (not running) per class.
    pub fn queued_by_class(&self) -> Vec<(WorkClass, usize)> {
        let st = lock(&self.inner.state);
        WorkClass::ALL
            .iter()
            .map(|c| (*c, st.queue.len_of(*c)))
            .collect()
    }
}

/// See [`ScanPool::enter`]. Counted in the class the query had when its
/// scan started.
pub struct Executing {
    inner: Arc<Inner>,
    counted: WorkClass,
}

impl std::fmt::Debug for Executing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Executing")
            .field("class", &self.counted)
            .finish()
    }
}

impl Drop for Executing {
    fn drop(&mut self) {
        self.inner.executing[self.counted.index()].fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for ScanPool {
    fn drop(&mut self) {
        lock(&self.inner.state).shutdown = true;
        self.inner.ready.notify_all();
    }
}

/// Run one popped step and put the task back (more work) or retire it.
/// Called without the state lock; returns with it held.
fn run_step<'a>(
    inner: &'a Inner,
    popped: Popped<Queued>,
) -> (std::sync::MutexGuard<'a, State>, Duration) {
    let (class, tenant, charged) = (popped.class, popped.tenant, popped.charged_ns);
    let Queued { mut task, tag } = popped.item;
    let cpu0 = stratadb_common::cpu::thread_cpu_time();
    // Keep the thread alive if a task panics (test/debug builds; release
    // builds abort on panic). The task itself reports its failure.
    let step = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(&mut task)) {
        Ok(s) => s,
        Err(_) => {
            inner.panicked.fetch_add(1, Ordering::Relaxed);
            Step::Done
        }
    };
    let cpu = stratadb_common::cpu::thread_cpu_time().saturating_sub(cpu0);
    inner.steps.fetch_add(1, Ordering::Relaxed);
    let mut st = lock(&inner.state);
    let ns = cpu.as_nanos() as f64;
    let u = &mut st.usage[class.index()];
    u.steps += 1;
    u.cpu += cpu;
    match step {
        Step::Again => {
            // Morsel boundary: requeue under the query's current class (it
            // may have been demoted) and let the fair queue choose. Requeue
            // before settling, so the task's class and tenant stay active
            // and keep their virtual time.
            let now_class = tag.class.get();
            let est = ns.max(1.0);
            st.queue.push(
                now_class,
                tag.tenant,
                Queued { task, tag },
                est,
                Instant::now(),
            );
            st.queue.settle(class, tenant, charged, ns);
            drop(st);
            // Another worker may be idle while this one picks something else.
            inner.ready.notify_one();
            (lock(&inner.state), cpu)
        }
        Step::Done => {
            st.queue.settle(class, tenant, charged, ns);
            inner.active.fetch_sub(1, Ordering::Relaxed);
            // Drop outside the lock: the task may hold the last reference
            // to segment readers.
            drop(st);
            drop(task);
            (lock(&inner.state), cpu)
        }
    }
}

fn worker(inner: &Inner) {
    let me = stratadb_common::threads::register(stratadb_common::threads::Role::Work);
    let mut st = lock(&inner.state);
    loop {
        let popped = loop {
            if st.shutdown {
                return;
            }
            if let Some(p) = st.queue.pop(Instant::now()) {
                break p;
            }
            st = inner.ready.wait(st).unwrap_or_else(|e| e.into_inner());
        };
        drop(st);
        st = run_step(inner, popped).0;
        me.update_cpu();
    }
}

impl ScanPool {
    /// True while a query of a class served before `class` (higher
    /// weight) is executing, on its own thread or in the pool.
    pub fn higher_busy(&self, class: WorkClass) -> bool {
        let w = class.weight();
        WorkClass::ALL
            .iter()
            .filter(|c| c.weight() > w)
            .any(|c| self.inner.executing[c.index()].load(Ordering::Relaxed) > 0)
    }

    /// Count a query of `class` as executing until the guard drops (the
    /// executor holds one around a query's scan).
    pub fn enter(&self, class: Arc<ClassCell>) -> Executing {
        let c = class.get();
        self.inner.executing[c.index()].fetch_add(1, Ordering::Relaxed);
        Executing {
            inner: Arc::clone(&self.inner),
            counted: c,
        }
    }

    /// Morsel-boundary preemption for work outside the pool: a query of
    /// `class` running on its own thread calls this between its morsels. If
    /// a step of a class served before it (higher weight) is waiting, run
    /// it here. Lower-class threads thereby lend their CPU
    /// to higher-class work instead of competing with it. Returns the CPU
    /// time lent (not the caller's own work), or `None` if nothing waited.
    pub fn help_higher(&self, class: WorkClass) -> Option<Duration> {
        let w = class.weight();
        let popped = {
            let mut st = lock(&self.inner.state);
            if st.queue.is_empty() {
                return None;
            }
            st.queue.pop_where(Instant::now(), |c| c.weight() > w)
        };
        let (st, cpu) = run_step(&self.inner, popped?);
        drop(st);
        Some(cpu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn runs_tasks_on_named_threads_and_rejects_when_full() {
        let pool = ScanPool::start(2);
        assert_eq!(pool.workers(), 2);
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        // Block both workers, then fill the queue (capacity 2).
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let mut accepted = 0;
        let mut rejected = 0;
        for _ in 0..10 {
            let (gate, done) = (Arc::clone(&gate), done_tx.clone());
            let job: Job = Box::new(move || {
                let name = std::thread::current().name().unwrap_or("").to_owned();
                let (m, cv) = &*gate;
                let mut open = m.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
                let _ = done.send(name);
            });
            match pool.try_submit(job) {
                Ok(()) => accepted += 1,
                Err(_) => rejected += 1,
            }
        }
        // 2 running + 2 queued at most (2 × workers); the rest is handed back.
        assert!(accepted <= 4 && rejected >= 6, "{accepted}/{rejected}");
        assert_eq!(pool.stats().rejected, rejected);
        assert_eq!(pool.free_slots(), 4 - accepted);
        {
            let (m, cv) = &*gate;
            *m.lock().unwrap() = true;
            cv.notify_all();
        }
        for _ in 0..accepted {
            let name = done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(name.starts_with("sx-work-"), "{name}");
        }
        // All tasks finished: active count drains to zero.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while pool.stats().active_tasks > 0 && std::time::Instant::now() < deadline {
            std::thread::yield_now();
        }
        assert_eq!(pool.stats().active_tasks, 0);
        assert_eq!(pool.free_slots(), 4);
    }

    #[test]
    fn a_panicking_task_does_not_kill_the_worker() {
        let pool = ScanPool::start(1);
        let _ = pool.try_submit(Box::new(|| panic!("task bug")));
        let (tx, rx) = std::sync::mpsc::channel();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut job: Job = Box::new(move || {
            let _ = tx.send(());
        });
        // The queue may still hold the panicking task; retry until accepted.
        loop {
            match pool.try_submit(job) {
                Ok(()) => break,
                Err(j) => {
                    assert!(std::time::Instant::now() < deadline);
                    job = j;
                    std::thread::yield_now();
                }
            }
        }
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert_eq!(pool.stats().panicked, 1);
    }

    /// Burn about `us` microseconds of CPU on this thread.
    fn burn(us: u64) {
        let t = stratadb_common::cpu::thread_cpu_time();
        while stratadb_common::cpu::thread_cpu_time().saturating_sub(t) < Duration::from_micros(us)
        {
            std::hint::black_box(0u64);
        }
    }

    /// A task of `steps` morsels that logs its class letter per step.
    fn stepper(steps: usize, mark: char, log: &Arc<Mutex<String>>) -> Task {
        stepper_gated(steps, mark, log, None)
    }

    type Gate = Arc<(Mutex<bool>, std::sync::Condvar)>;

    fn gate() -> Gate {
        Arc::new((Mutex::new(false), std::sync::Condvar::new()))
    }

    fn open(g: &Gate) {
        *g.0.lock().unwrap() = true;
        g.1.notify_all();
    }

    /// Like [`stepper`]; with `Some((k, gate))` the step after `k` logged
    /// steps waits for the gate (the test then acts at a known point).
    fn stepper_gated(
        steps: usize,
        mark: char,
        log: &Arc<Mutex<String>>,
        at: Option<(usize, Gate)>,
    ) -> Task {
        let log = Arc::clone(log);
        let mut left = steps;
        let mut done = 0;
        Box::new(move || {
            if let Some((k, g)) = &at {
                if done == *k {
                    let mut o = g.0.lock().unwrap();
                    while !*o {
                        o = g.1.wait(o).unwrap();
                    }
                }
            }
            done += 1;
            burn(300);
            log.lock().unwrap().push(mark);
            left -= 1;
            if left == 0 {
                Step::Done
            } else {
                Step::Again
            }
        })
    }

    fn tag(c: WorkClass, tenant: u64) -> Tag {
        Tag {
            class: Arc::new(ClassCell::new(c)),
            tenant,
            est_step_ns: 3e5,
        }
    }

    fn wait_idle(pool: &ScanPool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        while pool.stats().active_tasks > 0 {
            assert!(Instant::now() < deadline, "pool did not drain");
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    #[test]
    fn interactive_work_preempts_batch_at_morsel_boundaries() {
        let pool = ScanPool::start(1);
        let log = Arc::new(Mutex::new(String::new()));
        // Batch runs 3 steps alone, then waits inside its 4th until the
        // interactive task is queued.
        let g = gate();
        pool.try_submit_task(
            tag(WorkClass::Batch, 1),
            stepper_gated(60, 'b', &log, Some((3, Arc::clone(&g)))),
        )
        .ok()
        .unwrap();
        while log.lock().unwrap().len() < 3 {
            std::thread::yield_now();
        }
        pool.try_submit_task(tag(WorkClass::Interactive, 2), stepper(40, 'i', &log))
            .ok()
            .unwrap();
        open(&g);
        wait_idle(&pool);
        let s = log.lock().unwrap().clone();
        // While both were runnable (until interactive finished), batch got
        // about 1/9 of the steps (weights 8:1), not all of them first.
        let last_i = s.rfind('i').unwrap();
        let first_i = s.find('i').unwrap();
        assert!(first_i <= 5, "interactive waited for batch: {s}");
        let window = &s[first_i..=last_i];
        let b = window.matches('b').count();
        assert!(
            (2..=10).contains(&b),
            "batch steps while interactive ran: {b} in {window}"
        );
        let u = pool.class_usage();
        assert_eq!(u[WorkClass::Interactive.index()].1.steps, 40);
        assert_eq!(u[WorkClass::Batch.index()].1.steps, 60);
    }

    #[test]
    fn a_demoted_task_runs_its_remaining_steps_in_the_lower_class() {
        let pool = ScanPool::start(1);
        let log = Arc::new(Mutex::new(String::new()));
        let t = tag(WorkClass::Interactive, 1);
        let cell = Arc::clone(&t.class);
        // 5 steps as interactive; the 6th waits until the demotion.
        let g = gate();
        pool.try_submit_task(t, stepper_gated(20, 'x', &log, Some((5, Arc::clone(&g)))))
            .ok()
            .unwrap();
        while log.lock().unwrap().len() < 5 {
            std::thread::yield_now();
        }
        cell.set(WorkClass::Normal);
        open(&g);
        wait_idle(&pool);
        let u = pool.class_usage();
        let (i, n) = (
            u[WorkClass::Interactive.index()].1.steps,
            u[WorkClass::Normal.index()].1.steps,
        );
        assert_eq!(i + n, 20);
        assert!(i >= 5 && n >= 1, "interactive {i}, normal {n}");
    }

    #[test]
    fn tenants_of_one_class_alternate() {
        let pool = ScanPool::start(1);
        let log = Arc::new(Mutex::new(String::new()));
        pool.try_submit_task(tag(WorkClass::Interactive, 1), stepper(10, 'a', &log))
            .ok()
            .unwrap();
        pool.try_submit_task(tag(WorkClass::Interactive, 2), stepper(10, 'z', &log))
            .ok()
            .unwrap();
        wait_idle(&pool);
        let s = log.lock().unwrap().clone();
        // Equal CPU per step: neither tenant runs more than a couple of
        // steps in a row.
        assert!(!s.contains("aaaa") && !s.contains("zzzz"), "{s}");
    }

    #[test]
    fn batch_work_leaves_room_for_interactive_work() {
        let pool = ScanPool::start(2);
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let blocker = |g: &Arc<(Mutex<bool>, std::sync::Condvar)>| -> Task {
            let g = Arc::clone(g);
            Box::new(move || {
                let (m, cv) = &*g;
                let mut open = m.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
                Step::Done
            })
        };
        // Batch may hold `workers` (2) active tasks, no more.
        let mut accepted = 0;
        for _ in 0..6 {
            if pool
                .try_submit_task(tag(WorkClass::Batch, 1), blocker(&gate))
                .is_ok()
            {
                accepted += 1;
            }
        }
        assert_eq!(accepted, 2);
        assert_eq!(pool.free_slots_for(WorkClass::Batch), 0);
        // Normal fills to 3/4 of 2 × workers (3), interactive to 4.
        assert!(pool
            .try_submit_task(tag(WorkClass::Normal, 1), blocker(&gate))
            .is_ok());
        assert!(pool
            .try_submit_task(tag(WorkClass::Normal, 1), blocker(&gate))
            .is_err());
        assert!(pool
            .try_submit_task(tag(WorkClass::Interactive, 1), blocker(&gate))
            .is_ok());
        assert!(pool
            .try_submit_task(tag(WorkClass::Interactive, 1), blocker(&gate))
            .is_err());
        let (m, cv) = &*gate;
        *m.lock().unwrap() = true;
        cv.notify_all();
        wait_idle(&pool);
        assert_eq!(class_cap(1, WorkClass::Normal), 1);
    }

    #[test]
    fn configuration_is_validated() {
        assert!(configure_scan_pool(0).is_err());
        assert!(configure_scan_pool(MAX_WORKERS + 1).is_err());
    }
}
