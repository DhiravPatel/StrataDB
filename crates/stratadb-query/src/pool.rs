//! The process-wide scan worker pool (spec VIII.3, VIII.11.3).
//!
//! A fixed set of named threads (`sx-work-NN`, STRATA-SYS-03-015) executes
//! scan tasks of every query. No thread is created per query
//! (STRATA-SYS-01-010), and the thread count is fixed when the pool starts.
//! The task queue holds at most `workers` tasks, so tasks queued plus
//! running never exceed 2 × workers across all queries (STRATA-SYS-11-008). When it is full,
//! [`ScanPool::try_submit`] hands the task back and the submitting query
//! runs that share of its scan itself. Queries therefore never block on the
//! pool, and a saturated pool degrades to sequential scans instead of
//! queueing without bound.
//!
//! Tasks own their inputs (`'static`). A task that starts after its query
//! has finished finds no work left and returns immediately.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock};

use stratadb_common::{Error, Result};

type Job = Box<dyn FnOnce() + Send + 'static>;

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
}

pub struct ScanPool {
    tx: SyncSender<Job>,
    workers: usize,
    active: Arc<AtomicUsize>,
    submitted: AtomicUsize,
    rejected: AtomicUsize,
    panicked: Arc<AtomicUsize>,
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

impl ScanPool {
    pub(crate) fn start(workers: usize) -> Self {
        let (tx, rx) = sync_channel::<Job>(workers);
        let rx = Arc::new(Mutex::new(rx));
        let active = Arc::new(AtomicUsize::new(0));
        let panicked = Arc::new(AtomicUsize::new(0));
        let mut started = 0;
        for i in 0..workers {
            let (rx, active, panicked) =
                (Arc::clone(&rx), Arc::clone(&active), Arc::clone(&panicked));
            let spawned = std::thread::Builder::new()
                .name(stratadb_common::threads::Role::Work.thread_name(i))
                .spawn(move || worker(&rx, &active, &panicked));
            if spawned.is_ok() {
                started += 1;
            }
        }
        // A pool whose threads could not start still works: every task is
        // rejected and runs on the submitting thread.
        Self {
            tx,
            workers: started,
            active,
            submitted: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
            panicked,
        }
    }

    pub fn workers(&self) -> usize {
        self.workers
    }

    /// Tasks the pool can still take without exceeding 2 × workers active.
    pub fn free_slots(&self) -> usize {
        (2 * self.workers).saturating_sub(self.active.load(Ordering::Relaxed))
    }

    /// Queue `job`, or hand it back if the pool is saturated (the caller
    /// then runs it itself).
    pub fn try_submit(&self, job: Job) -> std::result::Result<(), Job> {
        if self.workers == 0 {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(job);
        }
        self.active.fetch_add(1, Ordering::Relaxed);
        match self.tx.try_send(job) {
            Ok(()) => {
                self.submitted.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(job) | TrySendError::Disconnected(job)) => {
                self.active.fetch_sub(1, Ordering::Relaxed);
                self.rejected.fetch_add(1, Ordering::Relaxed);
                Err(job)
            }
        }
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            workers: self.workers,
            active_tasks: self.active.load(Ordering::Relaxed),
            submitted: self.submitted.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            panicked: self.panicked.load(Ordering::Relaxed),
        }
    }
}

fn worker(rx: &Mutex<Receiver<Job>>, active: &AtomicUsize, panicked: &AtomicUsize) {
    let me = stratadb_common::threads::register(stratadb_common::threads::Role::Work);
    loop {
        let job = {
            let guard = rx.lock().unwrap_or_else(|e| e.into_inner());
            guard.recv()
        };
        let Ok(job) = job else { return };
        // Keep the thread alive if a task panics (test/debug builds; release
        // builds abort on panic). The task itself reports its failure.
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(job)).is_err() {
            panicked.fetch_add(1, Ordering::Relaxed);
        }
        active.fetch_sub(1, Ordering::Relaxed);
        me.update_cpu();
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

    #[test]
    fn configuration_is_validated() {
        assert!(configure_scan_pool(0).is_err());
        assert!(configure_scan_pool(MAX_WORKERS + 1).is_err());
    }
}
