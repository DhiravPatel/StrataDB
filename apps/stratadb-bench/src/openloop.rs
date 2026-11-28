//! Open-loop load generation (spec VIII.19, STRATA-SYS-19-002/003).
//!
//! Requests are scheduled on a fixed timeline (`start + i / rate`); latency
//! is measured from the *intended* send time, so time a request spends
//! waiting because the system under test is slow is counted (no coordinated
//! omission). A dispatcher thread follows the timeline and hands requests to
//! a fixed pool of workers through a bounded channel; when every worker is
//! busy the dispatcher blocks, and the requests it releases late are still
//! charged from their intended time.
//!
//! Client capacity (STRATA-SYS-19-003): the run is flagged `CLIENT_BOUND`
//! (and excluded from comparisons) when the dispatcher — the only client
//! work outside the system under test — used more than 70% of one CPU
//! (excluding the deliberate spin before each send time), or
//! fell behind the timeline although workers were free (p99 dispatch lag
//! above `max(2 ms, 10% of the interval)`). Payloads are generated before the
//! timeline starts so their cost is not on the client's critical path.
//!
//! Closed-loop mode (each worker issues its next request when the previous
//! one finishes) measures throughput only and is labelled as such.

use std::sync::mpsc::{sync_channel, TrySendError};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::Serialize;
use stratadb_common::cpu;
use stratadb_common::Result;

use crate::stats::{summarize, Summary};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Open,
    Closed,
}

#[derive(Debug, Clone, Serialize)]
pub struct LoadReport {
    pub mode: Mode,
    /// What the numbers mean (closed-loop runs are throughput-only).
    pub label: &'static str,
    pub target_rate_per_s: Option<f64>,
    pub duration_s: f64,
    pub workers: usize,
    pub requests: usize,
    pub errors: usize,
    pub achieved_rate_per_s: f64,
    /// Milliseconds from intended send time to completion (open loop) or
    /// from actual start (closed loop).
    pub latency_ms: Summary,
    /// Raw latencies (ms) for regression tests: every request up to
    /// [`MAX_RAW_SAMPLES`], else an evenly strided subsample.
    pub latency_ms_samples: Vec<f64>,
    /// Milliseconds from actual start to completion.
    pub service_ms: Summary,
    /// Dispatcher lag behind the timeline for requests a free worker could
    /// take immediately (ms).
    pub dispatch_lag_ms: Option<Summary>,
    /// Requests that found every worker busy (system under test saturated).
    pub queued_behind_busy_workers: usize,
    /// Dispatcher CPU time / wall time (open loop).
    pub client_cpu_fraction: Option<f64>,
    pub flags: Vec<&'static str>,
    pub first_error: Option<String>,
    /// The interference load run at the same time, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alongside: Option<Box<LoadReport>>,
}

/// Bound on raw latency samples kept per load report.
pub const MAX_RAW_SAMPLES: usize = 20_000;

fn subsample(xs: &[f64]) -> Vec<f64> {
    if xs.len() <= MAX_RAW_SAMPLES {
        return xs.to_vec();
    }
    (0..MAX_RAW_SAMPLES)
        .map(|i| xs[i * xs.len() / MAX_RAW_SAMPLES])
        .collect()
}

/// The dispatcher spins (instead of sleeping) for the last part of each
/// interval so requests leave on time; the window starts here and adapts to
/// the OS's observed sleep overshoot.
const SPIN_WINDOW: Duration = Duration::from_millis(1);
/// Upper bound of the adaptive spin window.
const MAX_SPIN_WINDOW: Duration = Duration::from_millis(20);

struct Done {
    latency: Duration,
    service: Duration,
    ok: Result<()>,
}

/// Run `n` requests at `rate_per_s` (open) with `workers` threads; request
/// `i` runs `work(i)`.
pub fn run_open<F>(rate_per_s: f64, n: usize, workers: usize, work: F) -> LoadReport
where
    F: Fn(usize) -> Result<()> + Sync,
{
    let workers = workers.max(1);
    let period = Duration::from_secs_f64(1.0 / rate_per_s.max(1e-9));
    let (tx, rx) = sync_channel::<(usize, Instant)>(workers);
    let rx = Mutex::new(rx);
    let done: Mutex<Vec<Done>> = Mutex::new(Vec::with_capacity(n));
    let mut lags = Vec::with_capacity(n);
    let mut queued = 0usize;
    let start = Instant::now() + Duration::from_millis(5);
    let mut dispatcher_cpu = Duration::ZERO;
    let mut spun = Duration::ZERO;
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| loop {
                let next = rx.lock().unwrap_or_else(|e| e.into_inner()).recv();
                let Ok((i, intended)) = next else { break };
                let began = Instant::now();
                let ok = work(i);
                let end = Instant::now();
                done.lock().unwrap_or_else(|e| e.into_inner()).push(Done {
                    latency: end.saturating_duration_since(intended),
                    service: end - began,
                    ok,
                });
            });
        }
        let cpu0 = cpu::thread_cpu_time();
        let mut unblocked_at = start;
        let mut window = SPIN_WINDOW;
        for i in 0..n {
            let intended = start + period.mul_f64(i as f64);
            // Sleep until shortly before the intended time (OS sleeps
            // overshoot by up to milliseconds under load), then spin to it.
            let now = Instant::now();
            if intended > now + window {
                let wake = intended - window;
                std::thread::sleep(wake - now);
                // Timer coalescing can overshoot by milliseconds: widen the
                // spin window to 1.5× the worst overshoot seen.
                let over = Instant::now().saturating_duration_since(wake);
                window = window.max(over.mul_f64(1.5)).min(MAX_SPIN_WINDOW);
            }
            let spin_from = Instant::now();
            while Instant::now() < intended {
                std::hint::spin_loop();
            }
            spun += spin_from.elapsed();
            match tx.try_send((i, intended)) {
                // Requests that fell due while the dispatcher was blocked on
                // busy workers are late because of the system under test,
                // not the client: they count in latency, not in client lag.
                Ok(()) if intended >= unblocked_at => {
                    lags.push(ms(Instant::now().saturating_duration_since(intended)))
                }
                Ok(()) => {}
                Err(TrySendError::Full(job)) => {
                    queued += 1;
                    if tx.send(job).is_err() {
                        break;
                    }
                    unblocked_at = Instant::now();
                }
                Err(TrySendError::Disconnected(_)) => break,
            }
        }
        // Deliberate spinning is timing precision, not client load.
        dispatcher_cpu = cpu::thread_cpu_time()
            .saturating_sub(cpu0)
            .saturating_sub(spun);
        drop(tx);
    });
    let wall = start.elapsed();
    let done = done.into_inner().unwrap_or_else(|e| e.into_inner());
    let lag = summarize(&lags);
    let client_cpu = dispatcher_cpu.as_secs_f64() / wall.as_secs_f64().max(1e-9);
    let lag_limit = (period.as_secs_f64() * 1000.0 * 0.1).max(2.0);
    let mut flags = Vec::new();
    if client_cpu > 0.7 || (lag.n > 0 && lag.p99 > lag_limit) {
        flags.push("CLIENT_BOUND");
    }
    let achieved = done.len() as f64 / wall.as_secs_f64().max(1e-9);
    if achieved < 0.95 * rate_per_s || queued > n / 100 {
        flags.push("SATURATED");
    }
    finish(
        Mode::Open,
        "open-loop: latency from intended send time (no coordinated omission)",
        Some(rate_per_s),
        wall,
        workers,
        done,
        Some(lag),
        queued,
        Some(client_cpu),
        flags,
    )
}

/// Upper bound on requests of one closed-loop run (bounded memory).
pub const MAX_CLOSED_REQUESTS: usize = 1_000_000;

/// Closed loop for `duration` (or [`MAX_CLOSED_REQUESTS`] requests, whichever
/// comes first): each worker repeats `work` back to back.
pub fn run_closed<F>(duration: Duration, workers: usize, work: F) -> LoadReport
where
    F: Fn(usize) -> Result<()> + Sync,
{
    let workers = workers.max(1);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let done: Mutex<Vec<Done>> = Mutex::new(Vec::new());
    let start = Instant::now();
    std::thread::scope(|s| {
        for _ in 0..workers {
            s.spawn(|| {
                let mut local = Vec::new();
                while start.elapsed() < duration {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= MAX_CLOSED_REQUESTS {
                        break;
                    }
                    let began = Instant::now();
                    let ok = work(i);
                    let d = began.elapsed();
                    local.push(Done {
                        latency: d,
                        service: d,
                        ok,
                    });
                }
                done.lock().unwrap_or_else(|e| e.into_inner()).extend(local);
            });
        }
    });
    let wall = start.elapsed();
    let done = done.into_inner().unwrap_or_else(|e| e.into_inner());
    finish(
        Mode::Closed,
        "closed-loop: throughput only; latencies are service times and understate queueing",
        None,
        wall,
        workers,
        done,
        None,
        0,
        None,
        Vec::new(),
    )
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[allow(clippy::too_many_arguments)]
fn finish(
    mode: Mode,
    label: &'static str,
    rate: Option<f64>,
    wall: Duration,
    workers: usize,
    done: Vec<Done>,
    lag: Option<Summary>,
    queued: usize,
    client_cpu: Option<f64>,
    flags: Vec<&'static str>,
) -> LoadReport {
    let errors = done.iter().filter(|d| d.ok.is_err()).count();
    let first_error = done
        .iter()
        .find_map(|d| d.ok.as_ref().err().map(|e| e.to_string()));
    let lat: Vec<f64> = done.iter().map(|d| ms(d.latency)).collect();
    let svc: Vec<f64> = done.iter().map(|d| ms(d.service)).collect();
    LoadReport {
        mode,
        label,
        target_rate_per_s: rate,
        duration_s: wall.as_secs_f64(),
        workers,
        requests: done.len(),
        errors,
        achieved_rate_per_s: done.len() as f64 / wall.as_secs_f64().max(1e-9),
        latency_ms: summarize(&lat),
        latency_ms_samples: subsample(&lat),
        service_ms: summarize(&svc),
        dispatch_lag_ms: lag,
        queued_behind_busy_workers: queued,
        client_cpu_fraction: client_cpu,
        flags,
        first_error,
        alongside: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Up to 3 attempts until `ok` holds: properties that depend on the OS
    /// scheduling the dispatcher on time (no `CLIENT_BOUND`) can fail on a
    /// heavily loaded machine, where the flag is then correct.
    fn eventually(
        mut run: impl FnMut() -> LoadReport,
        ok: impl Fn(&LoadReport) -> bool,
    ) -> LoadReport {
        let mut r = run();
        for _ in 0..2 {
            if ok(&r) {
                break;
            }
            r = run();
        }
        r
    }

    #[test]
    fn open_loop_counts_queueing_from_intended_time() {
        // 1 worker, 5 ms of service per request, offered 400/s (one every
        // 2.5 ms): the system cannot keep up, so latency from the intended
        // time must grow well beyond the service time. A closed-loop client
        // would report ~5 ms for every request (coordinated omission).
        let run = || {
            run_open(400.0, 40, 1, |_| {
                std::thread::sleep(Duration::from_millis(5));
                Ok(())
            })
        };
        let r = eventually(run, |r| !r.flags.contains(&"CLIENT_BOUND"));
        assert_eq!(r.requests, 40);
        assert!(r.service_ms.median < 15.0, "{:?}", r.service_ms);
        assert!(
            r.latency_ms.max > 50.0,
            "queueing hidden: {:?}",
            r.latency_ms
        );
        assert!(r.flags.contains(&"SATURATED"), "{:?}", r.flags);
        assert!(r.queued_behind_busy_workers > 0);
        // A slow system is not misattributed to the client.
        assert!(!r.flags.contains(&"CLIENT_BOUND"), "{:?}", r.flags);
    }

    #[test]
    fn open_loop_with_capacity_tracks_the_offered_rate() {
        let r = eventually(
            || run_open(200.0, 100, 4, |_| Ok(())),
            |r| r.flags.is_empty() && r.latency_ms.median < 5.0,
        );
        assert_eq!((r.requests, r.errors), (100, 0));
        assert!(r.achieved_rate_per_s > 150.0, "{}", r.achieved_rate_per_s);
        assert!(r.latency_ms.median < 5.0, "{:?}", r.latency_ms);
        assert!(!r.flags.contains(&"SATURATED"), "{:?}", r.flags);
        assert!(r.client_cpu_fraction.unwrap() < 0.7);
    }

    #[test]
    fn raw_samples_are_bounded() {
        let xs: Vec<f64> = (0..50_000).map(f64::from).collect();
        let s = subsample(&xs);
        assert_eq!(s.len(), MAX_RAW_SAMPLES);
        assert_eq!((s[0], s[1]), (0.0, 2.0));
        assert_eq!(subsample(&xs[..10]), &xs[..10]);
    }

    #[test]
    fn errors_are_counted_and_closed_loop_is_labelled() {
        let r = run_open(1000.0, 20, 2, |i| {
            if i % 4 == 0 {
                Err(stratadb_common::Error::invalid_input("boom"))
            } else {
                Ok(())
            }
        });
        assert_eq!(r.errors, 5);
        assert!(r.first_error.unwrap().contains("boom"));
        let c = run_closed(Duration::from_millis(30), 2, |_| Ok(()));
        assert_eq!(c.mode, Mode::Closed);
        assert!(c.label.contains("throughput only"));
        assert!(c.requests > 0 && c.target_rate_per_s.is_none());
    }
}
