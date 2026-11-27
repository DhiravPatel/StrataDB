//! Memory accounting hierarchy (spec VIII.5.2; STRATA-SYS-05-005/006/007/009,
//! STRATA-ARCH-01-006, STRATA-SYS-02-029/035).
//!
//! A tree of [`MemoryTracker`]s, each with an optional limit:
//!
//! ```text
//! process            (0.80 x memory limit)
//! ├── control        catalog, sessions, metadata (reserved share)
//! ├── ingest         memtables (+ WAL buffers)
//! ├── cache          decoded index filters (tracked, not evictable yet)
//! ├── background     compaction
//! └── query          query pool
//!     └── tenant:<id>
//!         └── query:<id>   per-query limit
//! ```
//!
//! A reservation is applied from the leaf to the root, and is undone if any
//! node would exceed its limit, so no node is ever over its limit because of
//! a successful reservation. Because the regions are separate subtrees, a
//! runaway query exhausts only the query pool, never ingest memory
//! (ARCH-01-006).
//!
//! Hot paths do not touch shared atomics per allocation: an [`Accountant`]
//! owned by one worker reserves ahead in `batch` units (default 1 MiB), so
//! the accounting error is bounded by `batch x workers` (SYS-05-006).
//! Allocations of 64 KiB or more are reserved before they are made
//! (SYS-05-005) by declaring the new usage first.
//!
//! When a reservation fails, the caller may wait up to a timeout for memory
//! to be released, then fails with `MEMORY_LIMIT_EXCEEDED`, naming the
//! tracker path, the request, the usage and the top 5 consumers (SYS-05-007).
//! Trackers marked private (tenants) are named only to their own subtree, so
//! an error never reveals another tenant.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::{Duration, Instant};

use crate::{Error, Result};

/// Default reservation batch (spec `memory.tracker_batch_bytes`).
pub const DEFAULT_BATCH_BYTES: u64 = 1 << 20;

/// Allocations at least this large must be reserved before they are made.
pub const LARGE_ALLOCATION_BYTES: u64 = 64 << 10;

/// Waiting for memory, shared by a whole tree.
#[derive(Debug, Default)]
struct Waiters {
    count: AtomicU64,
    lock: Mutex<()>,
    cv: Condvar,
}

/// One node of the accounting tree.
#[derive(Debug)]
pub struct MemoryTracker {
    name: String,
    parent: Option<Arc<MemoryTracker>>,
    /// Bytes; 0 = no limit of its own.
    limit: AtomicU64,
    used: AtomicU64,
    peak: AtomicU64,
    /// Named only to its own subtree in errors.
    private: bool,
    children: Mutex<Vec<Weak<MemoryTracker>>>,
    waiters: Arc<Waiters>,
}

/// A tracker and its subtree, for display.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct TrackerNode {
    pub name: String,
    pub used: u64,
    pub peak: u64,
    /// 0 = no limit of its own.
    pub limit: u64,
    pub children: Vec<TrackerNode>,
}

impl MemoryTracker {
    /// A new tree. `limit` 0 means unlimited.
    pub fn root(name: impl Into<String>, limit: u64) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            parent: None,
            limit: AtomicU64::new(limit),
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            private: false,
            children: Mutex::new(Vec::new()),
            waiters: Arc::default(),
        })
    }

    fn new_child(self: &Arc<Self>, name: String, limit: u64, private: bool) -> Arc<Self> {
        let c = Arc::new(Self {
            name,
            parent: Some(Arc::clone(self)),
            limit: AtomicU64::new(limit),
            used: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            private,
            children: Mutex::new(Vec::new()),
            waiters: Arc::clone(&self.waiters),
        });
        let mut ch = self.children.lock().unwrap_or_else(|e| e.into_inner());
        // Prune dropped children so the list stays bounded by live ones.
        if ch.len() >= 64 && ch.len().is_power_of_two() {
            ch.retain(|w| w.strong_count() > 0);
        }
        ch.push(Arc::downgrade(&c));
        c
    }

    /// A child tracker. `limit` 0 means limited only by its ancestors.
    pub fn child(self: &Arc<Self>, name: impl Into<String>, limit: u64) -> Arc<Self> {
        self.new_child(name.into(), limit, false)
    }

    /// A child that other subtrees see only as `<other>` in errors (a
    /// tenant).
    pub fn private_child(self: &Arc<Self>, name: impl Into<String>, limit: u64) -> Arc<Self> {
        self.new_child(name.into(), limit, true)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// `process/query/tenant:a/query:q1`.
    pub fn path(&self) -> String {
        let mut parts = vec![self.name.as_str()];
        let mut n = self.parent.as_deref();
        while let Some(p) = n {
            parts.push(&p.name);
            n = p.parent.as_deref();
        }
        parts.reverse();
        parts.join("/")
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Relaxed)
    }

    pub fn peak(&self) -> u64 {
        self.peak.load(Ordering::Relaxed)
    }

    pub fn limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    /// Change the limit (live settings). Usage above a lowered limit is not
    /// reclaimed; new reservations fail until usage drops below it.
    pub fn set_limit(&self, limit: u64) {
        self.limit.store(limit, Ordering::Relaxed);
    }

    /// Chain from this node to the root.
    fn chain(&self) -> impl Iterator<Item = &MemoryTracker> {
        std::iter::successors(Some(self), |n| n.parent.as_deref())
    }

    /// Reserve `bytes` on this tracker and every ancestor, or nothing. Fails
    /// with `MEMORY_LIMIT_EXCEEDED` when any node would exceed its limit.
    pub fn try_reserve(&self, bytes: u64) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let mut done: Vec<&MemoryTracker> = Vec::with_capacity(6);
        for n in self.chain() {
            let now = n.used.fetch_add(bytes, Ordering::AcqRel) + bytes;
            let limit = n.limit();
            if limit != 0 && now > limit {
                n.used.fetch_sub(bytes, Ordering::AcqRel);
                for d in &done {
                    d.used.fetch_sub(bytes, Ordering::AcqRel);
                }
                return Err(self.limit_error(n, bytes, now - bytes, limit));
            }
            n.peak.fetch_max(now, Ordering::Relaxed);
            done.push(n);
        }
        Ok(())
    }

    /// Account `bytes` on this tracker and every ancestor without checking
    /// limits: for memory already held that must be tracked anyway (rows
    /// replayed from the WAL at startup).
    pub fn force_reserve(&self, bytes: u64) {
        for n in self.chain() {
            let now = n.used.fetch_add(bytes, Ordering::AcqRel) + bytes;
            n.peak.fetch_max(now, Ordering::Relaxed);
        }
    }

    /// Like [`try_reserve`](Self::try_reserve), waiting up to `timeout` for
    /// other reservations to be released.
    pub fn reserve_wait(&self, bytes: u64, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let err = match self.try_reserve(bytes) {
                Ok(()) => return Ok(()),
                Err(e) => e,
            };
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return Err(err);
            }
            let w = &self.waiters;
            w.count.fetch_add(1, Ordering::AcqRel);
            {
                let g = w.lock.lock().unwrap_or_else(|e| e.into_inner());
                // Re-checked on wake-up; the bounded wait also covers a
                // release that raced with going to sleep.
                let _ =
                    w.cv.wait_timeout(g, left.min(Duration::from_millis(50)))
                        .unwrap_or_else(|e| e.into_inner());
            }
            w.count.fetch_sub(1, Ordering::AcqRel);
        }
    }

    /// Return `bytes` to this tracker and every ancestor.
    pub fn release(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        for n in self.chain() {
            let prev = n.used.fetch_sub(bytes, Ordering::AcqRel);
            debug_assert!(prev >= bytes, "release below zero on {}", n.name);
            if prev < bytes {
                // Never wrap: clamp (a bug elsewhere must not poison limits).
                n.used.store(0, Ordering::Release);
            }
        }
        self.notify();
    }

    fn notify(&self) {
        let w = &self.waiters;
        if w.count.load(Ordering::Acquire) > 0 {
            let _g = w.lock.lock().unwrap_or_else(|e| e.into_inner());
            w.cv.notify_all();
        }
    }

    fn live_children(&self) -> Vec<Arc<MemoryTracker>> {
        let mut ch = self.children.lock().unwrap_or_else(|e| e.into_inner());
        ch.retain(|w| w.strong_count() > 0);
        ch.iter().filter_map(Weak::upgrade).collect()
    }

    /// Children by usage, largest first: (name, used).
    pub fn top_consumers(&self, n: usize) -> Vec<(String, u64)> {
        let mut v: Vec<(String, u64)> = self
            .live_children()
            .iter()
            .map(|c| (c.name.clone(), c.used()))
            .collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(n);
        v
    }

    /// The subtree, for display (`/api/v1/admin/memory`).
    pub fn snapshot(&self) -> TrackerNode {
        let mut children: Vec<TrackerNode> =
            self.live_children().iter().map(|c| c.snapshot()).collect();
        children.sort_by(|a, b| b.used.cmp(&a.used).then_with(|| a.name.cmp(&b.name)));
        TrackerNode {
            name: self.name.clone(),
            used: self.used(),
            peak: self.peak(),
            limit: self.limit(),
            children,
        }
    }

    /// `MEMORY_LIMIT_EXCEEDED` for a request by `self` refused at `at`.
    /// Private trackers outside the requester's own path are shown as
    /// `<other>`.
    fn limit_error(&self, at: &MemoryTracker, requested: u64, used: u64, limit: u64) -> Error {
        let own: Vec<*const MemoryTracker> = self.chain().map(|n| n as *const _).collect();
        let mut top: Vec<(String, u64)> = Vec::new();
        for c in at.live_children().iter().filter(|c| c.used() > 0) {
            let name = if c.private && !own.contains(&Arc::as_ptr(c)) {
                "<other>".to_owned()
            } else {
                c.name.clone()
            };
            // Anonymous entries are merged into one.
            match top.iter_mut().find(|(n, _)| *n == name) {
                Some(e) => e.1 += c.used(),
                None => top.push((name, c.used())),
            }
        }
        top.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        let top: Vec<String> = top
            .into_iter()
            .take(5)
            .map(|(n, b)| format!("{n}={}", human(b)))
            .collect();
        let at_path = if at.private && !own.contains(&(at as *const _)) {
            "<other>".to_owned()
        } else {
            at.path()
        };
        Error::resource_exhausted(format!(
            "MEMORY_LIMIT_EXCEEDED: {at_path} would exceed its limit of {} (used {}, requested {} by {}); top consumers: [{}]",
            human(limit),
            human(used),
            human(requested),
            self.path(),
            top.join(", ")
        ))
    }
}

/// `1.5 MiB`.
pub fn human(b: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

/// Bytes reserved on a tracker, released on drop.
#[derive(Debug)]
pub struct Reservation {
    tracker: Arc<MemoryTracker>,
    bytes: u64,
}

impl Reservation {
    /// Reserve `bytes` on `tracker` (waiting up to `wait`).
    pub fn new(tracker: &Arc<MemoryTracker>, bytes: u64, wait: Duration) -> Result<Self> {
        if wait.is_zero() {
            tracker.try_reserve(bytes)?;
        } else {
            tracker.reserve_wait(bytes, wait)?;
        }
        Ok(Self {
            tracker: Arc::clone(tracker),
            bytes,
        })
    }

    /// Track memory already held, without a limit check.
    pub fn forced(tracker: &Arc<MemoryTracker>, bytes: u64) -> Self {
        tracker.force_reserve(bytes);
        Self {
            tracker: Arc::clone(tracker),
            bytes,
        }
    }

    pub fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.tracker.release(self.bytes);
    }
}

/// Batched accounting for one worker (SYS-05-006): the owner declares its
/// current usage; shared atomics are touched only when usage moves past
/// the reserved amount, which grows and shrinks in `batch` steps.
#[derive(Debug)]
pub struct Accountant {
    tracker: Option<Arc<MemoryTracker>>,
    reserved: u64,
    batch: u64,
    wait: Duration,
}

impl Accountant {
    /// `tracker` None: accounting disabled (every call succeeds).
    pub fn new(tracker: Option<Arc<MemoryTracker>>, batch: u64, wait: Duration) -> Self {
        Self {
            tracker,
            reserved: 0,
            batch: batch.max(1),
            wait,
        }
    }

    /// Bytes currently reserved by this accountant.
    pub fn reserved(&self) -> u64 {
        self.reserved
    }

    /// Declare current usage. Call it *before* making an allocation of
    /// [`LARGE_ALLOCATION_BYTES`] or more, with the usage including it.
    /// Fails (and reserves nothing more) when the usage cannot be covered.
    pub fn set_usage(&mut self, usage: u64) -> Result<()> {
        let Some(t) = &self.tracker else {
            return Ok(());
        };
        if usage > self.reserved {
            let need = usage - self.reserved;
            let ahead = need.div_ceil(self.batch) * self.batch;
            // Reserve a whole batch ahead; if that does not fit, exactly
            // what is needed (so a limit is enforced on usage, not batches).
            let got = match t.try_reserve(ahead) {
                Ok(()) => ahead,
                Err(_) if ahead > need => {
                    if self.wait.is_zero() {
                        t.try_reserve(need)?;
                    } else {
                        t.reserve_wait(need, self.wait)?;
                    }
                    need
                }
                Err(e) if self.wait.is_zero() => return Err(e),
                Err(_) => {
                    t.reserve_wait(need, self.wait)?;
                    need
                }
            };
            self.reserved += got;
        } else if self.reserved - usage >= 2 * self.batch {
            // Keep one batch of slack; return the rest.
            let give = self.reserved - usage - self.batch;
            t.release(give);
            self.reserved -= give;
        }
        Ok(())
    }

    /// Take over `other`'s reservation (merging a worker's partial result)
    /// without touching shared counters. Both must use the same tracker.
    pub fn absorb(&mut self, mut other: Accountant) {
        let same = match (&self.tracker, &other.tracker) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };
        if same {
            self.reserved += std::mem::take(&mut other.reserved);
        }
        // Otherwise `other` releases its own reservation on drop.
    }
}

impl Drop for Accountant {
    fn drop(&mut self) {
        if let Some(t) = &self.tracker {
            t.release(self.reserved);
        }
    }
}

/// The memory available to this process and where the number came from:
/// the cgroup limit (v2 `memory.max`, v1 `memory.limit_in_bytes`) when set
/// and below physical memory, else physical memory (SYS-02-029).
pub fn detect_memory_limit() -> Option<(u64, &'static str)> {
    let physical = physical_memory();
    let cgroup = cgroup_memory_limit();
    match (cgroup, physical) {
        (Some(c), Some(p)) if c < p => Some((c, "cgroup")),
        (Some(c), None) => Some((c, "cgroup")),
        (_, Some(p)) => Some((p, "physical")),
        (None, None) => None,
    }
}

/// Physical memory (Linux `/proc/meminfo`, macOS/BSD `sysctl hw.memsize`).
pub fn physical_memory() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/proc/meminfo") {
        return parse_meminfo_total(&s);
    }
    let out = std::process::Command::new("sysctl")
        .args(["-n", "hw.memsize"])
        .output()
        .ok()?;
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

fn parse_meminfo_total(s: &str) -> Option<u64> {
    s.lines().find_map(|l| {
        let kb: u64 = l
            .strip_prefix("MemTotal:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()?;
        kb.checked_mul(1024)
    })
}

/// The cgroup memory limit of this process, if any (Linux).
pub fn cgroup_memory_limit() -> Option<u64> {
    let cg = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup_limit_from(&cg, |p| std::fs::read_to_string(p).ok())
}

fn cgroup_limit_from(cgroup: &str, read: impl Fn(&str) -> Option<String>) -> Option<u64> {
    // v2: "0::/path"
    if let Some(path) = cgroup.lines().find_map(|l| l.strip_prefix("0::")) {
        let v = read(&format!(
            "/sys/fs/cgroup{}/memory.max",
            path.trim_end_matches('/')
        ))
        .or_else(|| read("/sys/fs/cgroup/memory.max"))?;
        let v = v.trim();
        return if v == "max" { None } else { v.parse().ok() };
    }
    // v1: "N:memory:/path"
    let path = cgroup.lines().find_map(|l| {
        let mut it = l.splitn(3, ':');
        let (_, ctrl, p) = (it.next()?, it.next()?, it.next()?);
        ctrl.split(',').any(|c| c == "memory").then_some(p)
    })?;
    let v = read(&format!(
        "/sys/fs/cgroup/memory{}/memory.limit_in_bytes",
        path.trim_end_matches('/')
    ))?;
    let v: u64 = v.trim().parse().ok()?;
    // v1 reports "no limit" as a huge page-aligned number.
    (v < 1 << 60).then_some(v)
}

/// Resident set size of this process (Linux `/proc/self/statm`; elsewhere
/// `ps`, adequate for a 10 s monitor on development machines).
pub fn resident_bytes() -> Option<u64> {
    if let Ok(s) = std::fs::read_to_string("/proc/self/statm") {
        let pages: u64 = s.split_whitespace().nth(1)?.parse().ok()?;
        return pages.checked_mul(page_size());
    }
    let out = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    let kb: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
    kb.checked_mul(1024)
}

fn page_size() -> u64 {
    // /proc/self/statm counts pages; every supported Linux target uses 4 KiB
    // base pages except some arm64/ppc64 kernels, detected from smaps.
    std::fs::read_to_string("/proc/self/smaps_rollup")
        .ok()
        .and_then(|s| {
            s.lines().find_map(|l| {
                l.strip_prefix("KernelPageSize:")
                    .and_then(|r| r.split_whitespace().next()?.parse::<u64>().ok())
            })
        })
        .map_or(4096, |kb| kb * 1024)
}

/// Untracked memory: resident bytes not covered by tracked bytes.
pub fn untracked(resident: u64, tracked: u64) -> u64 {
    resident.saturating_sub(tracked)
}

/// Whether an untracked gap is worth a warning (SYS-05-009: more than
/// `ratio` of resident memory or more than 4 GiB). A floor of `min_bytes`
/// keeps a small process's fixed overhead (binary, stacks, allocator) from
/// warning forever.
pub fn untracked_is_alarming(resident: u64, tracked: u64, ratio: f64, min_bytes: u64) -> bool {
    let gap = untracked(resident, tracked);
    gap > 4 << 30 || (gap > min_bytes && gap as f64 > ratio * resident as f64)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn reservations_are_hierarchical_and_all_or_nothing() {
        let root = MemoryTracker::root("process", 1000);
        let pool = root.child("query", 600);
        let a = pool.private_child("tenant:a", 0);
        let q1 = a.child("query:q1", 300);
        let ingest = root.child("ingest", 400);
        q1.try_reserve(250).unwrap();
        assert_eq!(
            (q1.used(), a.used(), pool.used(), root.used()),
            (250, 250, 250, 250)
        );
        // Leaf limit.
        let e = q1.try_reserve(100).unwrap_err().to_string();
        assert!(
            e.contains("MEMORY_LIMIT_EXCEEDED: process/query/tenant:a/query:q1 would exceed"),
            "{e}"
        );
        assert_eq!(root.used(), 250, "nothing left reserved after a failure");
        // Pool limit, refused at an ancestor: every level is rolled back.
        let q2 = a.child("query:q2", 0);
        q2.try_reserve(300).unwrap();
        let e = q2.try_reserve(100).unwrap_err().to_string();
        assert!(e.contains("process/query would exceed its limit"), "{e}");
        assert!(
            e.contains("requested 100 B by process/query/tenant:a/query:q2"),
            "{e}"
        );
        assert_eq!(
            (q2.used(), a.used(), pool.used(), root.used()),
            (300, 550, 550, 550)
        );
        // Regions are separate: ingest still has its own share.
        ingest.try_reserve(400).unwrap();
        assert!(ingest.try_reserve(1).is_err());
        assert_eq!(root.used(), 950);
        q1.release(250);
        q2.release(300);
        ingest.release(400);
        assert_eq!(root.used(), 0);
        assert_eq!(root.peak(), 950);
    }

    #[test]
    fn errors_name_top_consumers_but_never_other_tenants() {
        let root = MemoryTracker::root("process", 0);
        let pool = root.child("query", 1000);
        let a = pool.private_child("tenant:a", 0);
        let b = pool.private_child("tenant:b", 0);
        let c = pool.private_child("tenant:c", 0);
        let qa = a.child("query:qa", 0);
        let qb = b.child("query:qb", 0);
        let qc = c.child("query:qc", 0);
        qb.try_reserve(500).unwrap();
        qc.try_reserve(300).unwrap();
        qa.try_reserve(100).unwrap();
        let e = qa.try_reserve(200).unwrap_err().to_string();
        assert!(!e.contains("tenant:b") && !e.contains("tenant:c"), "{e}");
        assert!(
            e.contains("<other>=800 B") && e.contains("tenant:a=100 B"),
            "{e}"
        );
        // An operator sees its own subtree by name.
        let e = qa.try_reserve(5000).unwrap_err().to_string();
        assert!(e.contains("process/query would exceed"), "{e}");
        assert_eq!(
            pool.top_consumers(2),
            vec![("tenant:b".to_owned(), 500), ("tenant:c".to_owned(), 300)]
        );
        let snap = root.snapshot();
        assert_eq!(snap.children[0].name, "query");
        assert_eq!(snap.children[0].children[0].name, "tenant:b");
        drop((qa, qb, qc));
        // Dropped trackers disappear from listings (their bytes were
        // released by their owners in real use; here they leak by design of
        // the test).
        assert!(a.top_consumers(5).is_empty());
    }

    #[test]
    fn accountant_batches_and_enforces_usage_not_batches() {
        let root = MemoryTracker::root("q", 3 * DEFAULT_BATCH_BYTES + 10);
        let mut acc = Accountant::new(Some(Arc::clone(&root)), DEFAULT_BATCH_BYTES, Duration::ZERO);
        acc.set_usage(10).unwrap();
        assert_eq!(root.used(), DEFAULT_BATCH_BYTES, "a whole batch ahead");
        acc.set_usage(1000).unwrap();
        assert_eq!(
            root.used(),
            DEFAULT_BATCH_BYTES,
            "no shared update within a batch"
        );
        // Near the limit the exact amount is reserved, not a batch.
        acc.set_usage(3 * DEFAULT_BATCH_BYTES + 5).unwrap();
        assert_eq!(root.used(), 3 * DEFAULT_BATCH_BYTES + 5);
        assert!(acc.set_usage(3 * DEFAULT_BATCH_BYTES + 11).is_err());
        // Shrinking returns memory, keeping one batch of slack.
        acc.set_usage(0).unwrap();
        assert_eq!(root.used(), DEFAULT_BATCH_BYTES);
        let mut other =
            Accountant::new(Some(Arc::clone(&root)), DEFAULT_BATCH_BYTES, Duration::ZERO);
        other.set_usage(5).unwrap();
        acc.absorb(other);
        assert_eq!(acc.reserved(), 2 * DEFAULT_BATCH_BYTES);
        drop(acc);
        assert_eq!(root.used(), 0);
        // Disabled accounting always succeeds.
        Accountant::new(None, 1, Duration::ZERO)
            .set_usage(u64::MAX)
            .unwrap();
    }

    #[test]
    fn waiting_for_memory_succeeds_when_it_is_released() {
        let root = MemoryTracker::root("p", 100);
        let held = Reservation::new(&root, 80, Duration::ZERO).unwrap();
        assert!(Reservation::new(&root, 50, Duration::ZERO).is_err());
        let r2 = Arc::clone(&root);
        let t = std::thread::spawn(move || {
            Reservation::new(&r2, 50, Duration::from_secs(5)).map(|r| r.bytes())
        });
        std::thread::sleep(Duration::from_millis(30));
        drop(held);
        assert_eq!(t.join().unwrap().unwrap(), 50);
        assert_eq!(root.used(), 0);
        let started = Instant::now();
        let _hold = Reservation::new(&root, 100, Duration::ZERO).unwrap();
        assert!(Reservation::new(&root, 1, Duration::from_millis(60)).is_err());
        assert!(started.elapsed() >= Duration::from_millis(60));
    }

    #[test]
    fn concurrent_reservations_never_exceed_a_limit() {
        let root = MemoryTracker::root("p", 10_000);
        let pool = root.child("pool", 7_000);
        let max_seen = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..8)
            .map(|i| {
                let t = pool.child(format!("w{i}"), 0);
                let (root, max_seen) = (Arc::clone(&root), Arc::clone(&max_seen));
                std::thread::spawn(move || {
                    for j in 0..20_000u64 {
                        let b = 1 + (j * 7 + i) % 900;
                        if t.try_reserve(b).is_ok() {
                            max_seen.fetch_max(root.used(), Ordering::Relaxed);
                            t.release(b);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(root.used(), 0);
        assert!(pool.peak() <= 7_000, "{}", pool.peak());
        assert!(max_seen.load(Ordering::Relaxed) <= 7_000);
    }

    #[test]
    fn limits_and_resident_memory_are_detected() {
        assert_eq!(
            parse_meminfo_total("MemTotal:       16000000 kB\nMemFree: 1 kB\n"),
            Some(16_384_000_000)
        );
        let files = |p: &str| match p {
            "/sys/fs/cgroup/system.slice/s.service/memory.max" => Some("1073741824\n".to_owned()),
            "/sys/fs/cgroup/memory/docker/x/memory.limit_in_bytes" => {
                Some("536870912\n".to_owned())
            }
            "/sys/fs/cgroup/memory/free/memory.limit_in_bytes" => {
                Some("9223372036854771712\n".to_owned())
            }
            _ => None,
        };
        assert_eq!(
            cgroup_limit_from("0::/system.slice/s.service\n", files),
            Some(1 << 30)
        );
        assert_eq!(cgroup_limit_from("0::/\n", |_| Some("max".into())), None);
        assert_eq!(
            cgroup_limit_from("4:memory:/docker/x\n3:cpu:/\n", files),
            Some(512 << 20)
        );
        assert_eq!(cgroup_limit_from("4:cpuacct,memory:/free\n", files), None);
        let (limit, _) = detect_memory_limit().unwrap();
        assert!(limit >= 256 << 20);
        let rss = resident_bytes().unwrap();
        assert!(rss > 1 << 20 && rss < limit, "{rss}");
        assert!(!untracked_is_alarming(100 << 20, 90 << 20, 0.10, 64 << 20));
        assert!(untracked_is_alarming(1 << 30, 100 << 20, 0.10, 64 << 20));
        assert!(
            untracked_is_alarming(100 << 30, 95 << 30, 0.10, 64 << 20),
            "> 4 GiB"
        );
        assert_eq!(human(1536 << 10), "1.5 MiB");
    }

    /// Cost of the primitives (run: `cargo test --release -p stratadb-common
    /// --lib memory::tests::primitive_cost -- --ignored --nocapture`).
    #[test]
    #[ignore = "measurement, not a check"]
    fn primitive_cost() {
        let root = MemoryTracker::root("process", 0);
        let leaf = root.child("query", 0).child("tenant", 0).child("exec", 0);
        let n = 2_000_000u32;
        let t = Instant::now();
        for _ in 0..n {
            leaf.try_reserve(std::hint::black_box(4096)).unwrap();
            leaf.release(4096);
        }
        let pair = t.elapsed().as_nanos() / u128::from(n);
        let mut acc = Accountant::new(Some(Arc::clone(&leaf)), DEFAULT_BATCH_BYTES, Duration::ZERO);
        let t = Instant::now();
        for i in 0..n {
            acc.set_usage(std::hint::black_box(u64::from(i % 1000)))
                .unwrap();
        }
        let within = t.elapsed().as_nanos() / u128::from(n);
        println!("try_reserve+release on a 4-level chain: {pair} ns; set_usage within a batch: {within} ns");
    }
}
