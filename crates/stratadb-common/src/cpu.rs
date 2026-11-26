//! CPU-time clocks (spec VIII.1.8, VIII.19): process and per-thread CPU time
//! from `clock_gettime(CLOCK_{PROCESS,THREAD}_CPUTIME_ID)` through `rustix`'s
//! safe wrapper — no `unsafe` in this workspace.
//!
//! CPU time is not a cycle count: the spec's cycles-per-row ledger requires
//! hardware performance counters (`perf_event_open`), which this module does
//! not provide. Benchmarks report `cpu_ns_per_row` and label it as such.

use std::time::Duration;

use rustix::time::{clock_gettime, ClockId};

fn duration(ts: rustix::time::Timespec) -> Duration {
    // The kernel never returns negative CPU time; clamp defensively.
    let secs = u64::try_from(ts.tv_sec).unwrap_or(0);
    let nanos = u32::try_from(ts.tv_nsec).unwrap_or(0).min(999_999_999);
    Duration::new(secs, nanos)
}

/// CPU time consumed by all threads of this process so far (user + system).
pub fn process_cpu_time() -> Duration {
    duration(clock_gettime(ClockId::ProcessCPUTime))
}

/// CPU time consumed by the calling thread so far (user + system).
pub fn thread_cpu_time() -> Duration {
    duration(clock_gettime(ClockId::ThreadCPUTime))
}

/// Measures CPU time and wall time over a region.
#[derive(Debug, Clone, Copy)]
pub struct CpuTimer {
    process: Duration,
    thread: Duration,
    wall: std::time::Instant,
}

/// Result of [`CpuTimer::stop`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CpuSample {
    pub wall: Duration,
    /// All threads of the process (includes worker threads a call spawns).
    pub process_cpu: Duration,
    /// The calling thread only.
    pub thread_cpu: Duration,
}

impl CpuTimer {
    pub fn start() -> Self {
        Self {
            process: process_cpu_time(),
            thread: thread_cpu_time(),
            wall: std::time::Instant::now(),
        }
    }

    pub fn stop(&self) -> CpuSample {
        CpuSample {
            wall: self.wall.elapsed(),
            process_cpu: process_cpu_time().saturating_sub(self.process),
            thread_cpu: thread_cpu_time().saturating_sub(self.thread),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clocks_advance_with_work_and_threads_are_separate() {
        let t = CpuTimer::start();
        // Busy work on this thread.
        let mut x = 0u64;
        while t.stop().thread_cpu < Duration::from_millis(20) {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
        }
        std::hint::black_box(x);
        let s = t.stop();
        assert!(s.thread_cpu >= Duration::from_millis(20));
        assert!(s.process_cpu >= s.thread_cpu);
        // A sleeping thread accrues (almost) no CPU time.
        let before = thread_cpu_time();
        std::thread::sleep(Duration::from_millis(30));
        assert!(thread_cpu_time().saturating_sub(before) < Duration::from_millis(15));
        // Work on another thread shows in process time, not in this thread's.
        let p0 = process_cpu_time();
        let t0 = thread_cpu_time();
        std::thread::spawn(|| {
            let start = thread_cpu_time();
            let mut y = 1u64;
            while thread_cpu_time().saturating_sub(start) < Duration::from_millis(20) {
                y = y.rotate_left(7) ^ 0x9e37_79b9;
            }
            std::hint::black_box(y);
        })
        .join()
        .unwrap();
        assert!(process_cpu_time().saturating_sub(p0) >= Duration::from_millis(20));
        assert!(thread_cpu_time().saturating_sub(t0) < Duration::from_millis(15));
    }
}
