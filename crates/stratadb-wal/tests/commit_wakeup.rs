//! Group-commit wake-ups and fsync concurrency (spec STRATA-SYS-07-005/006).
//!
//! A test I/O engine wraps the real one, replacing `fsync` with a fixed
//! delay (a fast-NVMe stand-in) and recording how many syncs were in flight
//! at once. With a fast device, the cost of waking the writers of a group
//! shows in commit latency rather than hiding behind a slow fsync.

#![allow(clippy::unwrap_used, clippy::expect_used)] // test code: panics are failures

use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use stratadb_common::io::{
    FileRef, IoEngine, IoStats, PreadEngine, ReadReq, Submission, WriteFile,
};
use stratadb_common::Result;
use stratadb_wal::{Durability, RecordType, Wal, WalOptions};

/// Syncs that sleep instead of reaching the device; counts concurrency.
#[derive(Debug)]
struct FastSync {
    inner: PreadEngine,
    delay: Duration,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
    syncs: Arc<AtomicU64>,
}

#[derive(Debug)]
struct FastSyncFile {
    inner: Arc<dyn WriteFile>,
    delay: Duration,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: Arc<AtomicUsize>,
    syncs: Arc<AtomicU64>,
}

impl WriteFile for FastSyncFile {
    fn path(&self) -> &Path {
        self.inner.path()
    }
    fn write_at(&self, data: &[u8], offset: u64) -> Result<usize> {
        self.inner.write_at(data, offset)
    }
    fn sync(&self, _data_only: bool) -> Result<()> {
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        self.syncs.fetch_add(1, Ordering::Relaxed);
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }
    fn set_len(&self, len: u64) -> Result<()> {
        self.inner.set_len(len)
    }
    fn len(&self) -> Result<u64> {
        self.inner.len()
    }
}

impl FastSync {
    fn new(delay: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: PreadEngine::new(64),
            delay,
            in_flight: Arc::default(),
            max_in_flight: Arc::default(),
            syncs: Arc::default(),
        })
    }
    fn wrap(&self, f: Arc<dyn WriteFile>) -> Arc<dyn WriteFile> {
        Arc::new(FastSyncFile {
            inner: f,
            delay: self.delay,
            in_flight: Arc::clone(&self.in_flight),
            max_in_flight: Arc::clone(&self.max_in_flight),
            syncs: Arc::clone(&self.syncs),
        })
    }
}

impl IoEngine for FastSync {
    fn name(&self) -> &'static str {
        "fast-sync"
    }
    fn open(&self, path: &Path) -> Result<FileRef> {
        self.inner.open(path)
    }
    fn submit(&self, reqs: Vec<ReadReq>) -> Submission {
        self.inner.submit(reqs)
    }
    fn complete(&self, s: Submission) -> Vec<Result<Vec<u8>>> {
        self.inner.complete(s)
    }
    fn create_new(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        Ok(self.wrap(self.inner.create_new(path)?))
    }
    fn open_write(&self, path: &Path) -> Result<Arc<dyn WriteFile>> {
        Ok(self.wrap(self.inner.open_write(path)?))
    }
    fn stats(&self) -> IoStats {
        self.inner.stats()
    }
    fn forget(&self, file: &FileRef) {
        self.inner.forget(file)
    }
}

fn opts(io: &Arc<FastSync>, segment_max_bytes: u64) -> WalOptions {
    WalOptions {
        // `WAL_BENCH_GROUP=1`: the production default (adaptive group commit).
        durability: if std::env::var_os("WAL_BENCH_GROUP").is_some() {
            Durability::GroupCommit {
                max_delay: Duration::from_millis(2),
            }
        } else {
            Durability::Sync
        },
        segment_max_bytes,
        io: Some(Arc::clone(io) as Arc<dyn IoEngine>),
        ..WalOptions::default()
    }
}

/// Commit latencies (µs, sorted) of `writers` threads each appending
/// `per_writer` records; returns (latencies, syncs, wall).
fn run(
    dir: &Path,
    io: &Arc<FastSync>,
    writers: usize,
    per_writer: usize,
    seg: u64,
) -> (Vec<f64>, u64, Duration) {
    let (wal, _) = Wal::open(dir, opts(io, seg), 0, |_, _, _| Ok(())).unwrap();
    let wal = Arc::new(wal);
    let start = Arc::new(Barrier::new(writers));
    let t0 = Instant::now();
    let handles: Vec<_> = (0..writers)
        .map(|w| {
            let (wal, start) = (Arc::clone(&wal), Arc::clone(&start));
            std::thread::spawn(move || {
                start.wait();
                let mut lat = Vec::with_capacity(per_writer);
                for i in 0..per_writer {
                    let p = format!("writer {w} record {i}");
                    let t = Instant::now();
                    wal.append(RecordType::EventBatch, p.as_bytes()).unwrap();
                    lat.push(t.elapsed().as_secs_f64() * 1e6);
                }
                lat
            })
        })
        .collect();
    let mut all: Vec<f64> = handles
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    let wall = t0.elapsed();
    let m = wal.metrics().commit_wake_seconds;
    if std::env::var_os("WAKE_STATS").is_some() && m.count > 0 {
        let q = |p: f64| m.quantile_upper_bound(p).map_or(0.0, |s| s * 1e6);
        println!(
            "  wake (publish -> covered writer returns): n {} mean {:.1} us p50 <= {:.0} us p99 <= {:.0} us",
            m.count,
            m.sum / m.count as f64 * 1e6,
            q(0.5),
            q(0.99)
        );
    }
    all.sort_by(f64::total_cmp);
    let syncs = io.syncs.load(Ordering::Relaxed);
    (all, syncs, wall)
}

fn pct(v: &[f64], p: f64) -> f64 {
    v[((v.len() - 1) as f64 * p).round() as usize]
}

/// Every acknowledged record is durable, at most one fsync is ever in flight
/// (also across segment rotations), and many writers share each fsync.
#[test]
fn one_fsync_in_flight_and_groups_are_shared() {
    let dir = tempfile::tempdir().unwrap();
    let io = FastSync::new(Duration::from_micros(300));
    // Small segments: rotations (which sync the old segment) happen while
    // group-commit leaders sync.
    let (lat, syncs, _) = run(dir.path(), &io, 32, 60, 8 << 10);
    assert_eq!(lat.len(), 32 * 60);
    assert_eq!(
        io.max_in_flight.load(Ordering::SeqCst),
        1,
        "concurrent fsyncs on the WAL"
    );
    assert!(
        syncs < (32 * 60) as u64 / 4,
        "{syncs} syncs for {} records",
        32 * 60
    );
    // Replay sees every record.
    let mut n = 0;
    Wal::open(dir.path(), opts(&io, 8 << 10), 0, |_, _, _| {
        n += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(n, 32 * 60);
}

/// Commit latency with many writers on a fast device (measurement; run:
/// `cargo test --release -p stratadb-wal --test commit_wakeup -- --ignored --nocapture`).
/// Environment: `WAL_BENCH_GROUP` (set: adaptive group commit instead of
/// `sync`), `WAKE_STATS` (set: print the wake-latency histogram).
/// Results and the designs compared: benchmarks/reports/2026-11-26-commit-wakeup-and-inflight.md.
#[test]
#[ignore = "measurement, not a check"]
fn wakeup_latency_with_many_writers() {
    for writers in [8usize, 32, 64, 128] {
        let mut rows = Vec::new();
        for _ in 0..3 {
            let dir = tempfile::tempdir().unwrap();
            let io = FastSync::new(Duration::from_micros(200));
            let per = 4000 / writers;
            let (lat, syncs, wall) = run(dir.path(), &io, writers, per.max(20), 64 << 20);
            rows.push((
                pct(&lat, 0.5),
                pct(&lat, 0.99),
                *lat.last().unwrap(),
                lat.len() as f64 / syncs as f64,
                lat.len() as f64 / wall.as_secs_f64(),
            ));
        }
        rows.sort_by(|a, b| a.1.total_cmp(&b.1));
        let (p50, p99, max, group, rate) = rows[1];
        println!(
            "writers {writers:3}: commit p50 {p50:7.0} us  p99 {p99:7.0} us  max {max:7.0} us  records/fsync {group:5.1}  commits/s {rate:8.0}"
        );
    }
}
