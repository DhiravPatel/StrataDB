# ADR-0032: Group-commit wake-ups, one fsync per WAL, in-flight I/O bound

- **Status:** Accepted
- **Spec references:** STRATA-SYS-07-005, STRATA-SYS-07-006,
  STRATA-SYS-04-008; ADR-0027 (I/O engine)

## Context
- **SYS-07-005:** only one fsync may be in flight per WAL device. A new test,
  whose engine counts syncs in flight, found two. A segment rotation synced
  the old segment under the WAL mutex while a group-commit leader's fsync,
  started earlier without the mutex, was still running.
- **SYS-07-006:** waking the N writers of a group must take O(1) syscalls.
  The WAL used a parking_lot condition variable on the WAL mutex. Its
  `notify_all` costs the leader one wake call, then requeues the other
  waiters onto the mutex, so they resume one after another as each one
  releases it.
- **SYS-04-008:** in-flight I/O must be bounded per worker and per device.
  The engine did not bound either.

## Decision
### One fsync in flight
Before rotating, `append` waits on the commit condition variable until no
sync leader is running. With the WAL mutex held, no new leader can start, so
the rotation's fsync of the old segment is the only one in flight. The
group-commit leader and rotation are the only fsync paths, and `sync()` goes
through the leader.

### Wake-ups: keep parking_lot's hand-off
Two broadcast designs were built and measured against it (report
`2026-11-26-commit-wakeup-and-inflight`). Both had waiters off the WAL mutex
on a commit epoch, and covered writers returning on an atomic `durable`
check without re-taking the WAL lock:
- **One `std` broadcast:** 1 call for the leader.
- **One shard per CPU:** a constant number of calls.

**Results:**
- Both lowered the measured wake latency. At 128 writers the mean was about
  72 µs for the sharded design, against about 340 µs.
- Both cost 8–15% throughput at 8–128 writers on a quiet machine, with
  smaller groups.
- **Why:** the hand-off of the existing design overlaps the next group's
  fsync, while a faster wake-up elects the next leader sooner, with a
  smaller group.
- **Verdict:** the existing mechanism stays. The leader's cost is already
  one wake call per group, and the remaining hand-offs run in user space on
  the woken threads.

**What is added:**
- `commit_wake_seconds` (`strata_wal_commit_wake_seconds`), the wake cost per
  covered writer;
- the measurement harness `crates/stratadb-wal/tests/commit_wakeup.rs`.

A single futex word with leader election decoupled from wake-up remains the
spec's target. It needs a platform wait-on-address primitive and measurement
on HW-REF-1 (Linux), not this laptop.

### In-flight bound
- **Mechanism:** the portable engine (`PreadEngine`) wraps every `pread`,
  `pwrite` and `fsync` in a permit from a per-device gate, keyed by `st_dev`.
  - The limit is `io_max_inflight_per_device`, default 128 (typical NVMe
    saturation depth), set by `[storage] io_max_inflight_per_device` or
    `PreadEngine::with_inflight_limit`.
  - The fast path is one compare-and-swap to take a permit and one atomic
    subtraction to release it. The mutex and condition variable are used
    only while a caller waits.
- **Per worker:** the backend is synchronous, with one call per thread at a
  time, so in-flight I/O per worker is 1 by construction. An asynchronous
  (io_uring) backend must add its own per-worker bound.
- **Stats and metrics:**
  - `IoStats.{max_inflight_per_device, inflight, inflight_peak,
    inflight_waits}`;
  - `strata_io_max_inflight_per_device`, `strata_io_inflight`,
    `strata_io_inflight_peak` and `strata_io_inflight_waits_total`.

## Consequences
- **Single fsync:** the rotation test is stable over 5 runs. Commit
  performance is unchanged within noise.
- **Gate cost:** about 11 ns per I/O call, against about 1 µs for a cached
  4 KiB `pread` (1.2%).
  - The first, mutex-based gate measured 1.029 geometric mean CPU on the
    standard scenario, against a 1.16 A/A spread.
  - A run of the final version was invalidated by external load and is
    reported as inconclusive.
- **Not yet:**
  - **Futex-word wake-ups:** SYS-07-006 is partial.
  - **Default from measurement:** the device default is not derived from a
    measured saturation queue depth. `strata doctor storage` could measure
    it.
  - **Async backend:** a per-worker in-flight bound for an asynchronous
    backend.
