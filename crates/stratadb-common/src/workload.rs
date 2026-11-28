//! Work classes (spec VIII.11.1, STRATA-SYS-11-001..003).
//!
//! | Class | Weight | Notes |
//! |---|---|---|
//! | `system` | absolute | never preempted (catalog publish, WAL leader) |
//! | `interactive` | 8 | estimated < 100 ms; preempts lower at morsel boundaries |
//! | `normal` | 4 | ad-hoc analytics |
//! | `ingest` | 4 | min share 20% |
//! | `batch` | 1 | large reports, exports |
//! | `background` | 1 | merges, TTL; min share 10% |
//!
//! Queries are `interactive`, `normal` or `batch`. A [`ClassCell`] holds a
//! query's current class so that a demotion at run time (2× the class time
//! budget, STRATA-SYS-11-003) is seen by work that is already queued.

use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

/// A scheduling class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WorkClass {
    System = 0,
    Interactive = 1,
    Normal = 2,
    Ingest = 3,
    Batch = 4,
    Background = 5,
}

impl WorkClass {
    pub const ALL: [WorkClass; 6] = [
        WorkClass::System,
        WorkClass::Interactive,
        WorkClass::Normal,
        WorkClass::Ingest,
        WorkClass::Batch,
        WorkClass::Background,
    ];
    /// The classes a query can have, highest first.
    pub const QUERY: [WorkClass; 3] = [WorkClass::Interactive, WorkClass::Normal, WorkClass::Batch];

    pub fn name(self) -> &'static str {
        match self {
            WorkClass::System => "system",
            WorkClass::Interactive => "interactive",
            WorkClass::Normal => "normal",
            WorkClass::Ingest => "ingest",
            WorkClass::Batch => "batch",
            WorkClass::Background => "background",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.name() == s)
    }

    pub fn index(self) -> usize {
        self as usize
    }

    fn from_index(i: u8) -> Self {
        Self::ALL
            .get(usize::from(i))
            .copied()
            .unwrap_or(WorkClass::Normal)
    }

    /// Fair-share weight. `system` is absolute (always first); it is given
    /// a large weight for the cases where it competes in a fair queue.
    pub fn weight(self) -> u64 {
        match self {
            WorkClass::System => 1_000,
            WorkClass::Interactive => 8,
            WorkClass::Normal | WorkClass::Ingest => 4,
            WorkClass::Batch | WorkClass::Background => 1,
        }
    }

    /// The class a query is demoted to after exceeding twice its time
    /// budget (`batch` stays `batch`).
    pub fn demoted(self) -> Self {
        match self {
            WorkClass::Interactive => WorkClass::Normal,
            WorkClass::Normal => WorkClass::Batch,
            c => c,
        }
    }
}

/// Plan-time classification thresholds and class time budgets
/// (STRATA-SYS-11-003).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Classifier {
    /// Estimated single-core CPU below this is `interactive` (spec: 100 ms).
    pub interactive_ms: f64,
    /// Estimated CPU at or above this is `batch`.
    pub batch_ms: f64,
}

impl Default for Classifier {
    fn default() -> Self {
        Self {
            interactive_ms: 100.0,
            batch_ms: 10_000.0,
        }
    }
}

impl Classifier {
    pub fn classify(&self, est_cpu_ms: f64) -> WorkClass {
        if est_cpu_ms < self.interactive_ms {
            WorkClass::Interactive
        } else if est_cpu_ms < self.batch_ms {
            WorkClass::Normal
        } else {
            WorkClass::Batch
        }
    }

    /// Wall-time budget of a query class; a query running longer than twice
    /// its budget is demoted. `None` for classes that are never demoted.
    pub fn budget(&self, c: WorkClass) -> Option<Duration> {
        let ms = match c {
            WorkClass::Interactive => self.interactive_ms,
            WorkClass::Normal => self.batch_ms,
            _ => return None,
        };
        Some(Duration::from_secs_f64(ms.max(0.0) / 1000.0))
    }
}

/// A query's current class, shared with the work it has queued.
#[derive(Debug)]
pub struct ClassCell(AtomicU8);

impl ClassCell {
    pub fn new(c: WorkClass) -> Self {
        Self(AtomicU8::new(c as u8))
    }

    pub fn get(&self) -> WorkClass {
        WorkClass::from_index(self.0.load(Ordering::Relaxed))
    }

    pub fn set(&self, c: WorkClass) {
        self.0.store(c as u8, Ordering::Relaxed);
    }
}

/// Weighted fair queue (STRATA-SYS-11-001): stride scheduling over CPU time
/// per class, then per tenant within the class, then FIFO per tenant.
///
/// - **Virtual time.** Each class has a virtual time advanced by
///   `cpu_ns / weight` of the work it ran; the non-empty class with the
///   smallest virtual time goes next (`system` always goes first). Within a
///   class, tenants have their own virtual time (`cpu_ns`).
/// - **No credit hoarding.** A class or tenant that becomes non-empty starts
///   at the smallest virtual time among those already waiting (or the last
///   one dispatched), so idleness is not saved up for a burst.
/// - **Charging.** [`FairQueue::pop`] charges the item's estimated cost, so
///   many dispatches before any completion still alternate fairly;
///   [`FairQueue::settle`] corrects by the measured CPU time.
/// - **Aging.** A class's head that has waited `n × aging` gets `n` steps
///   of priority (each worth 1 ms of virtual time), so nothing starves even
///   behind a heavier class.
///
/// The structure holds no lock and does no I/O; callers serialize access and
/// bound its size.
#[derive(Debug)]
pub struct FairQueue<T> {
    classes: Vec<ClassQ<T>>,
    /// Virtual time of the last dispatch (start point when all are idle).
    vclock: f64,
    aging: Duration,
    len: usize,
}

#[derive(Debug)]
struct TenantQ<T> {
    key: u64,
    vtime: f64,
    /// (item, enqueued, estimated ns)
    q: std::collections::VecDeque<(T, std::time::Instant, f64)>,
    /// Popped and not yet settled.
    running: usize,
}

#[derive(Debug)]
struct ClassQ<T> {
    vtime: f64,
    tclock: f64,
    tenants: Vec<TenantQ<T>>,
    queued: usize,
    running: usize,
}

impl<T> ClassQ<T> {
    /// Waiting or running: an active class keeps its virtual time.
    fn active(&self) -> bool {
        self.queued > 0 || self.running > 0
    }
}

/// One aging step, in virtual nanoseconds.
const AGING_STEP_NS: f64 = 1e6;

/// What [`FairQueue::pop`] returns.
#[derive(Debug)]
pub struct Popped<T> {
    pub class: WorkClass,
    pub tenant: u64,
    pub item: T,
    pub waited: Duration,
    /// The estimate charged; pass it to [`FairQueue::settle`].
    pub charged_ns: f64,
}

impl<T> FairQueue<T> {
    pub fn new(aging: Duration) -> Self {
        Self {
            classes: WorkClass::ALL
                .iter()
                .map(|_| ClassQ {
                    vtime: 0.0,
                    tclock: 0.0,
                    tenants: Vec::new(),
                    queued: 0,
                    running: 0,
                })
                .collect(),
            vclock: 0.0,
            aging,
            len: 0,
        }
    }

    /// Items waiting (not running).
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Items waiting in `class`.
    pub fn len_of(&self, class: WorkClass) -> usize {
        self.classes[class.index()].queued
    }

    /// Items of `class` popped and not yet settled.
    pub fn running_of(&self, class: WorkClass) -> usize {
        self.classes[class.index()].running
    }

    /// Enqueue `item` of `tenant` in `class` with an estimated cost. Work
    /// that was running and comes back (a stepped task) keeps its virtual
    /// time; a class or tenant that was idle starts at the current minimum.
    pub fn push(
        &mut self,
        class: WorkClass,
        tenant: u64,
        item: T,
        est_ns: f64,
        now: std::time::Instant,
    ) {
        let vmin = self
            .classes
            .iter()
            .enumerate()
            .filter(|(i, c)| *i != class.index() && c.active())
            .map(|(_, c)| c.vtime)
            .fold(f64::INFINITY, f64::min);
        let vmin = if vmin.is_finite() { vmin } else { self.vclock };
        let c = &mut self.classes[class.index()];
        if !c.active() {
            c.vtime = c.vtime.max(vmin);
        }
        match c.tenants.iter_mut().find(|t| t.key == tenant) {
            Some(t) => t.q.push_back((item, now, est_ns)),
            None => {
                let tmin = c
                    .tenants
                    .iter()
                    .map(|t| t.vtime)
                    .fold(f64::INFINITY, f64::min);
                let start = if tmin.is_finite() { tmin } else { c.tclock };
                let mut q = std::collections::VecDeque::new();
                q.push_back((item, now, est_ns));
                c.tenants.push(TenantQ {
                    key: tenant,
                    vtime: start,
                    q,
                    running: 0,
                });
            }
        }
        c.queued += 1;
        self.len += 1;
    }

    /// Dequeue the next item by class, tenant and arrival order. `eligible`
    /// filters classes (e.g. classes without a free slot). Every popped item
    /// must be [`settle`](FairQueue::settle)d once.
    pub fn pop_where(
        &mut self,
        now: std::time::Instant,
        eligible: impl Fn(WorkClass) -> bool,
    ) -> Option<Popped<T>> {
        let aging_ms = self.aging.as_secs_f64() * 1000.0;
        let mut best: Option<(usize, f64)> = None;
        for (i, c) in self.classes.iter().enumerate() {
            let class = WorkClass::ALL[i];
            if c.queued == 0 || !eligible(class) {
                continue;
            }
            if class == WorkClass::System {
                best = Some((i, f64::NEG_INFINITY));
                break;
            }
            let oldest = c
                .tenants
                .iter()
                .filter_map(|t| t.q.front().map(|h| h.1))
                .min()
                .unwrap_or(now);
            let waited_ms = now.saturating_duration_since(oldest).as_secs_f64() * 1000.0;
            let steps = if aging_ms > 0.0 {
                (waited_ms / aging_ms).floor()
            } else {
                0.0
            };
            let key = c.vtime - steps * AGING_STEP_NS;
            if best.is_none_or(|(_, k)| key < k) {
                best = Some((i, key));
            }
        }
        let (ci, _) = best?;
        let class = WorkClass::ALL[ci];
        let c = &mut self.classes[ci];
        let ti = c
            .tenants
            .iter()
            .enumerate()
            .filter(|(_, t)| !t.q.is_empty())
            .min_by(|a, b| a.1.vtime.total_cmp(&b.1.vtime))
            .map(|(i, _)| i)?;
        let t = &mut c.tenants[ti];
        let (item, at, est) = t.q.pop_front()?;
        t.vtime += est;
        t.running += 1;
        let tenant = t.key;
        c.tclock = c.tclock.max(t.vtime);
        c.queued -= 1;
        c.running += 1;
        c.vtime += est / class.weight() as f64;
        self.vclock = self.vclock.max(c.vtime);
        self.len -= 1;
        Some(Popped {
            class,
            tenant,
            item,
            waited: now.saturating_duration_since(at),
            charged_ns: est,
        })
    }

    pub fn pop(&mut self, now: std::time::Instant) -> Option<Popped<T>> {
        self.pop_where(now, |_| true)
    }

    /// Finish a popped item: correct its charge by the measured CPU time
    /// (`actual_ns`) and stop counting it as running.
    pub fn settle(&mut self, class: WorkClass, tenant: u64, charged_ns: f64, actual_ns: f64) {
        let delta = actual_ns - charged_ns;
        let c = &mut self.classes[class.index()];
        c.vtime += delta / class.weight() as f64;
        c.running = c.running.saturating_sub(1);
        if let Some(ti) = c.tenants.iter().position(|t| t.key == tenant) {
            let t = &mut c.tenants[ti];
            t.vtime += delta;
            t.running = t.running.saturating_sub(1);
            if t.q.is_empty() && t.running == 0 {
                c.tenants.swap_remove(ti);
            }
        }
    }

    /// Remove the first waiting item matching `f` (e.g. a cancelled query).
    pub fn remove(&mut self, mut f: impl FnMut(&T) -> bool) -> Option<T> {
        for c in &mut self.classes {
            for ti in 0..c.tenants.len() {
                if let Some(pos) = c.tenants[ti].q.iter().position(|x| f(&x.0)) {
                    let item = c.tenants[ti].q.remove(pos).map(|x| x.0);
                    if c.tenants[ti].q.is_empty() && c.tenants[ti].running == 0 {
                        c.tenants.swap_remove(ti);
                    }
                    c.queued -= 1;
                    self.len -= 1;
                    return item;
                }
            }
        }
        None
    }

    /// Approximate number of items served before the first one matching `f`
    /// (shown to clients as the queue position): everything in classes
    /// listed before its class, the round-robin share of other tenants in
    /// its class, and its own tenant's earlier items. Weighted sharing
    /// between classes can serve it sooner.
    pub fn position(&self, mut f: impl FnMut(&T) -> bool) -> Option<usize> {
        for (ci, c) in self.classes.iter().enumerate() {
            for (ti, t) in c.tenants.iter().enumerate() {
                if let Some(pos) = t.q.iter().position(|x| f(&x.0)) {
                    let higher: usize = self.classes[..ci].iter().map(|c| c.queued).sum();
                    // Tenants of the same class alternate: each other tenant
                    // is served about `pos + 1` times first.
                    let peers: usize = c
                        .tenants
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != ti)
                        .map(|(_, o)| o.q.len().min(pos + 1))
                        .sum();
                    return Some(higher + peers + pos);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_and_demotion() {
        let c = Classifier::default();
        assert_eq!(c.classify(0.0), WorkClass::Interactive);
        assert_eq!(c.classify(99.9), WorkClass::Interactive);
        assert_eq!(c.classify(100.0), WorkClass::Normal);
        assert_eq!(c.classify(10_000.0), WorkClass::Batch);
        assert_eq!(
            c.budget(WorkClass::Interactive),
            Some(Duration::from_millis(100))
        );
        assert_eq!(c.budget(WorkClass::Batch), None);
        assert_eq!(WorkClass::Interactive.demoted(), WorkClass::Normal);
        assert_eq!(WorkClass::Normal.demoted(), WorkClass::Batch);
        assert_eq!(WorkClass::Batch.demoted(), WorkClass::Batch);
        for w in WorkClass::ALL {
            assert_eq!(WorkClass::parse(w.name()), Some(w));
            assert_eq!(WorkClass::ALL[w.index()], w);
        }
        let cell = ClassCell::new(WorkClass::Interactive);
        cell.set(cell.get().demoted());
        assert_eq!(cell.get(), WorkClass::Normal);
    }

    use std::time::Instant;

    /// Serve `n` items, always settling at the true cost, and count how many
    /// each class got.
    fn serve(
        q: &mut FairQueue<u32>,
        n: usize,
        cost: impl Fn(WorkClass) -> f64,
        refill: bool,
        now: Instant,
    ) -> [usize; 6] {
        let mut got = [0; 6];
        for _ in 0..n {
            let Some(p) = q.pop(now) else { break };
            got[p.class.index()] += 1;
            q.settle(p.class, p.tenant, p.charged_ns, cost(p.class));
            if refill {
                q.push(p.class, p.tenant, p.item, 1e6, now);
            }
        }
        got
    }

    #[test]
    fn classes_share_cpu_by_weight() {
        let now = Instant::now();
        let mut q = FairQueue::new(Duration::ZERO);
        for c in [WorkClass::Interactive, WorkClass::Normal, WorkClass::Batch] {
            for i in 0..4 {
                q.push(c, 1, i, 1e6, now);
            }
        }
        // Equal per-item cost: dispatch counts follow the weights 8:4:1.
        let got = serve(&mut q, 13_000, |_| 1e6, true, now);
        let (i, n, b) = (got[1] as f64, got[2] as f64, got[4] as f64);
        assert!(
            (i / b - 8.0).abs() < 0.1 && (n / b - 4.0).abs() < 0.1,
            "{got:?}"
        );
        // Unequal cost: CPU time (count × cost) follows the weights.
        let mut q = FairQueue::new(Duration::ZERO);
        for c in [WorkClass::Interactive, WorkClass::Batch] {
            q.push(c, 1, 0, 1e6, now);
        }
        let cost = |c: WorkClass| if c == WorkClass::Batch { 1e6 } else { 4e6 };
        let got = serve(&mut q, 9_000, cost, true, now);
        let ratio = got[1] as f64 * 4.0 / got[4] as f64;
        assert!((ratio - 8.0).abs() < 0.2, "{got:?}");
    }

    #[test]
    fn tenants_share_within_a_class_and_fifo_within_a_tenant() {
        let now = Instant::now();
        let mut q = FairQueue::new(Duration::ZERO);
        // Tenant 1 floods; tenant 2 has two items. They alternate, and each
        // tenant's items come out in arrival order.
        for i in 0..10 {
            q.push(WorkClass::Normal, 1, i, 1e6, now);
        }
        q.push(WorkClass::Normal, 2, 100, 1e6, now);
        q.push(WorkClass::Normal, 2, 101, 1e6, now);
        let order: Vec<u32> = std::iter::from_fn(|| {
            let p = q.pop(now)?;
            q.settle(p.class, p.tenant, p.charged_ns, p.charged_ns);
            Some(p.item)
        })
        .collect();
        assert_eq!(&order[..5], &[0, 100, 1, 101, 2]);
        assert_eq!(&order[5..], &[3, 4, 5, 6, 7, 8, 9]);
        assert!(q.is_empty());
    }

    #[test]
    fn idle_classes_do_not_hoard_credit_and_system_goes_first() {
        let now = Instant::now();
        let mut q = FairQueue::new(Duration::ZERO);
        q.push(WorkClass::Batch, 1, 0, 1e6, now);
        let _ = serve(&mut q, 1_000, |_| 1e6, true, now);
        // Interactive arrives after batch ran alone for a long time: it does
        // not get 1,000 items in a row, it shares 8:1 from now on.
        q.push(WorkClass::Interactive, 1, 0, 1e6, now);
        let got = serve(&mut q, 90, |_| 1e6, true, now);
        assert!(got[4] >= 9, "{got:?}");
        q.push(WorkClass::System, 9, 7, 1e9, now);
        assert_eq!(q.pop(now).unwrap().class, WorkClass::System);
    }

    #[test]
    fn aging_lets_a_starved_class_through() {
        let t0 = Instant::now();
        let mut q = FairQueue::new(Duration::from_millis(500));
        // Batch is far behind in virtual time (it just ran a lot of CPU).
        q.push(WorkClass::Batch, 1, 0, 1e6, t0);
        let p = q.pop(t0).unwrap();
        q.settle(p.class, p.tenant, p.charged_ns, 50e6);
        q.push(WorkClass::Batch, 1, 1, 1e6, t0);
        q.push(WorkClass::Interactive, 1, 2, 1e6, t0);
        assert_eq!(q.pop(t0).unwrap().class, WorkClass::Interactive);
        q.push(WorkClass::Interactive, 1, 3, 1e6, t0);
        // After 50 × 500 ms of waiting, batch's head has 50 steps (50 ms).
        let later = t0 + Duration::from_secs(26);
        assert_eq!(q.pop(later).unwrap().class, WorkClass::Batch);
    }

    #[test]
    fn eligibility_remove_and_position() {
        let now = Instant::now();
        let mut q = FairQueue::new(Duration::ZERO);
        q.push(WorkClass::Interactive, 1, 1, 1e6, now);
        q.push(WorkClass::Batch, 1, 2, 1e6, now);
        q.push(WorkClass::Batch, 2, 3, 1e6, now);
        assert_eq!(q.position(|x| *x == 3), Some(2));
        assert_eq!(q.len_of(WorkClass::Batch), 2);
        let p = q.pop_where(now, |c| c == WorkClass::Batch).unwrap();
        assert_eq!(p.class, WorkClass::Batch);
        assert_eq!(q.remove(|x| *x == 1), Some(1));
        assert_eq!(q.remove(|x| *x == 1), None);
        assert_eq!(q.len(), 1);
    }
}
