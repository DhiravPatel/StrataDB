# Adaptive group commit (spec STRATA-SYS-07-004) on HW-REF-7

## Summary
The WAL sync leader used to wait the full `max_delay` (2 ms) before every
fsync, even when no other writer was coming. It now waits only when more
records are expected:
- **No wait** when the estimated arrival rate times the fsync latency is
  below 1.
- **Otherwise at most `min(fsync latency, max_delay)`**, stopping early when
  as many records are pending as the previous group had, or when 1 MiB is
  pending.

Same binary, `adaptive_group_delay = true` against `false`, 3 interleaved
foreground pairs of `benchmarks/scenarios/group_commit.toml`, durability
`group_commit`. Adaptive was better on every load:

| Load | Fixed 2 ms delay | Adaptive |
|---|---|---|
| ingest, open loop 20 req/s: p50 | 6.44, 6.25, 5.91 ms | 3.60, 3.87, 3.80 ms |
| ingest, open loop 400 req/s: p50 / p99 | 5.63–5.93 / 9.13–9.58 ms | 4.65–4.98 / 6.63–8.05 ms |
| ingest, 1 closed-loop writer: throughput | 169.5, 188.5, 188.1 /s | 259.2, 284.5, 282.0 /s |
| ingest, 8 closed-loop writers: throughput | 1,469, 1,498, 1,467 /s | 2,244, 2,364, 2,239 /s |
| ingest, 8 closed-loop writers: p99 | 6.51, 6.10, 6.13 ms | 5.09, 4.12, 4.17 ms |
| dataset bulk load (1,000-event batches) | 132k, 148k, 139k events/s | 225k, 208k, 217k events/s |
| `group_commit_wait` stage, mean | 6.33, 5.96, 6.12 ms | 3.57, 3.99, 3.82 ms |

The adaptive commit wait now matches the device's fdatasync latency. `strata
doctor storage` on the same filesystem measured p50 3.86 ms and p99 4.60 ms
over 1,000 iterations (`doctor-storage.json`). One adaptive 20 req/s run had
a single outlier (p99 19 ms, max 49.7 ms); its other two runs had p99 below
5 ms.

These are developer-laptop measurements (Apple M4, macOS, where `fdatasync`
uses `F_FULLFSYNC`). They are not capacity claims.

## A harness bug found on the way
The first run of this A/B showed 1–4 *second* tail latencies with 8
closed-loop writers, in both modes, and fewer than 2 records per fsync. The
WAL alone (8 writers: groups of 7.99, p99 5 ms) and the engine alone (p99
4.9 ms) did not show it. The cause was `strata-bench` itself: in closed-loop
ingest, once the 256 pre-generated batches were used up, the fallback path
called `write_batch` while still holding the batch-pool mutex. That
serialised every writer through the fsync, and the lock's unfairness starved
some writers for seconds.

After the fix, 8 writers reach about 2,200 requests/s with p99 around 5 ms.
Open-loop ingest, which all earlier reports used, pre-generates every batch
and never took that path. Earlier reports are unaffected.

## Correctness evidence
`crates/stratadb-wal/src/tests.rs`:
- `adaptive_group_commit::a_lone_writer_does_not_wait_for_a_group`: 12
  sequential commits with a 40 ms maximum delay. The fixed mode waits at
  least 11 × 40 ms; adaptive takes less than half of that, and the recorded
  leader waits are near zero.
- `...::concurrent_writers_are_batched_and_never_wait_past_max_delay`: 320
  records from 8 threads use fewer than 160 fsyncs. Every record is synced
  once, no leader waits more than `max_delay` plus slack, and every
  acknowledged record is durable.
- `...::a_full_group_is_flushed_before_the_delay_expires`: with a 5 s
  maximum delay and a 1-byte group threshold, 80 commits finish in well
  under 4 s.
- The existing WAL tests (concurrency, recovery, torn tails, encryption),
  the crash matrix and the power-loss simulation are unchanged and pass. The
  delay only decides when an fsync starts; acknowledgement still requires it
  to complete.

## Reproduction
```bash
cargo build --release -p stratadb-bench
sed 's/adaptive_group_delay = true/adaptive_group_delay = false/' \
  benchmarks/scenarios/group_commit.toml > gc-fixed.toml
for r in 1 2 3; do
  target/release/strata-bench scenario gc-fixed.toml --out gcab/fixed-$r
  target/release/strata-bench scenario benchmarks/scenarios/group_commit.toml --out gcab/adaptive-$r
done
strata doctor storage --dir <wal-dir> --iterations 1000
```
