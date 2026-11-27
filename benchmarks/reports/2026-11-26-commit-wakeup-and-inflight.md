# Group-commit wake-ups, single fsync, in-flight I/O bound (spec STRATA-SYS-07-005/006, SYS-04-008) on HW-REF-7

## Summary
1. **One fsync in flight per WAL (SYS-07-005): fixed.**
   - The new test `crates/stratadb-wal/tests/commit_wakeup.rs` found **2
     concurrent fsyncs**: a segment rotation synced the old segment while a
     group-commit leader's fsync was still running.
   - Rotation now waits for the leader's sync, on the same condition
     variable. The test, which records the maximum number of syncs in
     flight, passes in repeated runs.
2. **Wake-up mechanism (SYS-07-006): alternatives measured, the existing
   one kept.**
   - Two broadcast designs were built and measured against the existing
     parking_lot condition variable:
     - waiters off the WAL mutex on a `std` condvar, with covered writers
       returning without the WAL lock;
     - the same, sharded per CPU.
   - Both cut the measured wake-up latency, but neither improved commit
     latency or throughput, and on a quiet machine both were slower.
   - The parking_lot path stays. Its leader makes one wake call per group,
     and the other waiters are handed off in user space.
   - A new histogram, `strata_wal_commit_wake_seconds`, makes the wake cost
     visible.
3. **In-flight I/O per device (SYS-04-008): bounded.**
   - Every `pread`, `pwrite` and `fsync` of the portable engine takes a
     permit from a per-device gate: `[storage] io_max_inflight_per_device`,
     default 128.
   - A permit costs about 11 ns, about 1.2% of a 4 KiB page-cache `pread`.

## 1. Single fsync in flight
**Harness:** a test I/O engine replaces `fsync` with a fixed 300 µs sleep
and counts syncs in flight. 32 writers × 60 records, in `sync` mode, with
8 KiB segments so that rotations interleave with group commits.

| | Max syncs in flight | Records per fsync |
|---|---|---|
| Before | **2** | — |
| After | 1, in 5 of 5 runs | ≥ 4 |

## 2. Wake-ups
**Harness:** the same engine with a 200 µs "fsync" (a fast-NVMe stand-in),
writers doing closed-loop appends, median of 3 runs per row. "Wake" is the
time from the leader publishing durability to a covered writer resuming.

### Sharded `std` broadcast vs existing parking_lot, `sync` mode, quiet machine
These are pairs 4–6 of `wakeup-ab-sync-mode-sharded-std.txt`, commits/s:

| Writers | parking_lot (kept) | Sharded broadcast |
|---|---|---|
| 8 | 15.6k, 15.6k, 15.6k | 14.2k, 14.1k, 14.1k |
| 32 | 56.2k, 55.7k, 56.4k | 49.8k, 49.9k, 50.6k |
| 64 | 106.8k, 101.6k, 102.5k | 86.4k, 85.5k, 87.4k |
| 128 | 110.6k, 106.2k, 104.4k | 94.5k, 97.1k, 95.1k |

- **The broadcast is faster to wake and slower overall.**
  - Mean wake latency at 128 writers: about 72 µs sharded, against about
    340 µs for parking_lot (measured below).
  - Commit p50 was nevertheless higher with the broadcast (e.g. 64 writers:
    676–708 µs vs 545–565 µs), and groups were smaller (4.1 vs 4.3 records
    per fsync at 8 writers).
- **Why:** faster wake-ups let the next leader start its fsync sooner, with
  a smaller group. In parking_lot's hand-off, the remaining waiters are woken
  while the next fsync is already running, so the hand-off overlaps the
  device time instead of adding to it.
- **parking_lot primitives:** sharding with parking_lot primitives was worse
  still.
- **Pairs 1–3** were taken under external CPU load (two unrelated processes
  at about 100% CPU each) and are not comparable.

### Final code vs the previous code, adaptive group commit (the production default)
From `wakeup-final-vs-old-group-commit.txt`, 4 interleaved pairs:

| Writers | Previous: commits/s, p50 | Final: commits/s, p50 | Final: mean wake |
|---|---|---|---|
| 8 | 23.4k–23.7k, 350–353 µs | 23.3k–23.6k, 352–353 µs | 26 µs |
| 32 | 60.6k–63.4k, 495–503 µs | 62.0k–63.4k, 498–503 µs | 103–107 µs |
| 64 | 80.0k–82.6k, 672–680 µs | 80.2k–82.0k, 666–685 µs | 187–191 µs |
| 128 | 99.9k–105.2k, 1,050–1,100 µs | 98.9k–102.9k, 1,093–1,108 µs | 325–363 µs |

The commit path is unchanged, so the results are equal within noise. The
rotation fix only affects segment rotations.

**What remains for SYS-07-006:** a single futex word (Linux `FUTEX_WAKE`;
`__ulock` on macOS) would wake all writers in one syscall without re-taking
a lock. It needs a platform wait-on-address primitive, and the evidence above
is that a faster wake-up does not help unless leader election is separated
from it. It needs a dedicated design and measurement on HW-REF-1 (Linux),
not this laptop.

## 3. In-flight bound per device
- **Mechanism:** a per-`st_dev` gate with an atomic compare-and-swap fast
  path. The mutex and condition variable are used only when a caller must
  wait, and the release notifies only if someone waits.
- **Per worker:** the portable backend issues one call per thread at a time,
  so in-flight I/O per worker is 1 by construction.
- **Primitive cost** (`io::tests::gate_cost`, 3 runs): a permit takes 10.9,
  11.4 and 12.3 ns, against 948–1,032 ns for a 4 KiB cached `pread` through
  the engine (1.1–1.2%).
- **End to end**, the standard scenario, 3 interleaved pairs:
  - **First gate version** (mutex-based; `io-gate-ab1/`): geometric mean CPU
    ratio 1.029, against an A/A spread of 1.16 within the baseline. Plans
    were identical.
  - **Final, lock-free version** (`io-gate-ab2-loaded/`): measured under
    heavy external load. Pair ratios were 2.18, 1.03 and 1.07, swinging with
    the background processes, so it is **inconclusive** and not used as
    evidence.
- **Correctness:**
  - **Gate unit test:** 16 threads under a limit of 3, peak ≤ 3, with waits.
  - **Engine test:** 12 reader threads plus a writer and fsyncs on one
    device under a limit of 1. The peak is exactly 1 and every read is
    correct.

These are developer-laptop measurements (Apple M4, macOS), not capacity
claims.

## Reproduction
```bash
cargo test --release -p stratadb-wal --test commit_wakeup                       # single-fsync check
WAKE_STATS=1 cargo test --release -p stratadb-wal --test commit_wakeup -- --ignored --nocapture
WAL_BENCH_GROUP=1 WAKE_STATS=1 cargo test --release -p stratadb-wal --test commit_wakeup -- --ignored --nocapture
cargo test --release -p stratadb-common --lib io::tests::gate_cost -- --ignored --nocapture
```
