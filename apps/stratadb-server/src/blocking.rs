//! Fixed pool of `sx-blk-NN` threads for blocking request work (spec VIII.3,
//! STRATA-SYS-03-002/014): commits that wait for the WAL fsync, query
//! execution and admin operations.
//!
//! It replaces tokio's `spawn_blocking`, whose pool creates threads on demand
//! up to a large limit. This pool's maximum is fixed at start and counted
//! against the thread budget (`2 × logical_cpus + 32`). Threads are started
//! on demand up to that maximum (one when a job would otherwise wait) and
//! then stay, so an idle server holds few threads. Its queue is
//! bounded: requests are admitted by the ingest/query semaphores first, so
//! the queue is sized to hold every admitted request plus admin headroom. If
//! it is ever full, the request fails fast with `resource_exhausted` rather
//! than waiting without bound.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};

use stratadb_common::threads::Role;
use stratadb_common::{Error, Result};

type Job = Box<dyn FnOnce() + Send + 'static>;

pub struct BlockingPool {
    tx: SyncSender<Job>,
    rx: Arc<Mutex<Receiver<Job>>>,
    max_workers: usize,
    /// Threads started so far (≤ `max_workers`).
    started: Mutex<usize>,
    queue_capacity: usize,
    busy: Arc<AtomicUsize>,
    queued: Arc<AtomicUsize>,
    rejected: AtomicU64,
}

impl std::fmt::Debug for BlockingPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockingPool")
            .field("max_workers", &self.max_workers)
            .finish_non_exhaustive()
    }
}

/// Counters for metrics and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockingStats {
    /// Threads started (grows on demand up to `max_workers`).
    pub workers: usize,
    pub max_workers: usize,
    pub queue_capacity: usize,
    /// Jobs running now.
    pub busy: usize,
    /// Jobs waiting for a thread.
    pub queued: usize,
    /// Jobs refused because the queue was full.
    pub rejected: u64,
}

impl BlockingPool {
    /// A pool of up to `max_workers` threads with a queue of
    /// `queue_capacity` jobs. One thread starts now, the rest on demand.
    /// Threads exit when the pool is dropped. If the OS refuses a thread the
    /// pool keeps working with those it has (logged); with none, jobs fail
    /// with `unavailable`.
    pub fn start(max_workers: usize, queue_capacity: usize) -> Self {
        let (tx, rx) = sync_channel::<Job>(queue_capacity.max(1));
        let pool = Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
            max_workers: max_workers.max(1),
            started: Mutex::new(0),
            queue_capacity: queue_capacity.max(1),
            busy: Arc::new(AtomicUsize::new(0)),
            queued: Arc::new(AtomicUsize::new(0)),
            rejected: AtomicU64::new(0),
        };
        pool.grow();
        pool
    }

    /// Start one more thread if jobs are waiting with no idle thread and the
    /// maximum is not reached (or if none is running yet).
    fn grow(&self) {
        let mut started = self.started.lock().unwrap_or_else(|e| e.into_inner());
        let busy = self.busy.load(Ordering::Relaxed);
        let queued = self.queued.load(Ordering::Relaxed);
        let idle = started.saturating_sub(busy);
        if *started > 0 && (queued <= idle || *started >= self.max_workers) {
            return;
        }
        let (rx, b, q) = (
            Arc::clone(&self.rx),
            Arc::clone(&self.busy),
            Arc::clone(&self.queued),
        );
        match std::thread::Builder::new()
            .name(Role::Blk.thread_name(*started))
            .spawn(move || worker(&rx, &b, &q))
        {
            Ok(_) => *started += 1,
            Err(e) => {
                tracing::error!(component = "server", error = %e, "could not start a blocking worker thread")
            }
        }
    }

    /// Run `f` on a pool thread and await its result.
    pub async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<T> {
        if *self.started.lock().unwrap_or_else(|e| e.into_inner()) == 0 {
            self.grow();
            if *self.started.lock().unwrap_or_else(|e| e.into_inner()) == 0 {
                return Err(Error::unavailable("no blocking worker threads"));
            }
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let job: Job = Box::new(move || {
            let _ = tx.send(f());
        });
        self.queued.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = self.tx.try_send(job) {
            self.queued.fetch_sub(1, Ordering::Relaxed);
            self.rejected.fetch_add(1, Ordering::Relaxed);
            return Err(match e {
                TrySendError::Full(_) => Error::resource_exhausted(
                    "server busy: blocking work queue is full; retry with backoff",
                ),
                TrySendError::Disconnected(_) => Error::unavailable("blocking pool stopped"),
            });
        }
        self.grow();
        // A job that panicked drops its sender without sending.
        rx.await
            .map_err(|_| Error::internal("blocking worker task failed"))?
    }

    pub fn stats(&self) -> BlockingStats {
        BlockingStats {
            workers: *self.started.lock().unwrap_or_else(|e| e.into_inner()),
            max_workers: self.max_workers,
            queue_capacity: self.queue_capacity,
            busy: self.busy.load(Ordering::Relaxed),
            queued: self.queued.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
        }
    }
}

fn worker(rx: &Mutex<Receiver<Job>>, busy: &AtomicUsize, queued: &AtomicUsize) {
    let me = stratadb_common::threads::register(Role::Blk);
    loop {
        let job = rx.lock().unwrap_or_else(|e| e.into_inner()).recv();
        let Ok(job) = job else { return };
        queued.fetch_sub(1, Ordering::Relaxed);
        busy.fetch_add(1, Ordering::Relaxed);
        // Release builds abort on panic; in tests keep the thread alive.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(job));
        busy.fetch_sub(1, Ordering::Relaxed);
        me.update_cpu();
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread")]
    async fn runs_jobs_on_named_threads_and_reports_failures() {
        let pool = BlockingPool::start(2, 4);
        let name = pool
            .run(|| Ok(std::thread::current().name().unwrap_or("").to_owned()))
            .await
            .unwrap();
        assert!(name.starts_with("sx-blk-"), "{name}");
        let e = pool
            .run(|| -> Result<()> { Err(Error::invalid_input("bad")) })
            .await
            .unwrap_err();
        assert!(e.to_string().contains("bad"));
        let p = pool
            .run(|| -> Result<()> { panic!("bug") })
            .await
            .unwrap_err();
        assert!(p.to_string().contains("failed"));
        // The worker survived the panic.
        assert_eq!(pool.run(|| Ok(7)).await.unwrap(), 7);
        let threads: Vec<_> = stratadb_common::threads::inventory()
            .into_iter()
            .filter(|t| t.role == Role::Blk && t.name.starts_with("sx-blk-"))
            .collect();
        assert!(threads.len() >= 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grows_on_demand_up_to_the_maximum() {
        let pool = Arc::new(BlockingPool::start(3, 16));
        assert_eq!(pool.stats().workers, 1, "one thread at start");
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let mut tasks = Vec::new();
        for _ in 0..6 {
            let (p, g) = (Arc::clone(&pool), Arc::clone(&gate));
            tasks.push(tokio::spawn(async move {
                p.run(move || {
                    let (m, cv) = &*g;
                    let mut open = m.lock().unwrap();
                    while !*open {
                        open = cv.wait(open).unwrap();
                    }
                    Ok(())
                })
                .await
            }));
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while pool.stats().busy < 3 {
            assert!(std::time::Instant::now() < deadline, "{:?}", pool.stats());
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let st = pool.stats();
        assert_eq!((st.workers, st.max_workers, st.busy), (3, 3, 3), "capped");
        {
            let (m, cv) = &*gate;
            *m.lock().unwrap() = true;
            cv.notify_all();
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(pool.stats().workers, 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn full_queue_fails_fast() {
        let pool = Arc::new(BlockingPool::start(1, 1));
        let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
        let block = |g: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>| {
            move || {
                let (m, cv) = &*g;
                let mut open = m.lock().unwrap();
                while !*open {
                    open = cv.wait(open).unwrap();
                }
                Ok(())
            }
        };
        // One running, one queued.
        let p1 = Arc::clone(&pool);
        let a = tokio::spawn({
            let f = block(Arc::clone(&gate));
            async move { p1.run(f).await }
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while pool.stats().busy == 0 {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let p2 = Arc::clone(&pool);
        let b = tokio::spawn({
            let f = block(Arc::clone(&gate));
            async move { p2.run(f).await }
        });
        while pool.stats().queued == 0 {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let e = pool.run(|| Ok(())).await.unwrap_err();
        assert_eq!(e.kind(), stratadb_common::ErrorKind::ResourceExhausted);
        assert_eq!(pool.stats().rejected, 1);
        {
            let (m, cv) = &*gate;
            *m.lock().unwrap() = true;
            cv.notify_all();
        }
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        assert_eq!(pool.stats().busy + pool.stats().queued, 0);
    }
}
