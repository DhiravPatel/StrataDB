# StrataDB benchmark report — flat aggregation groups + aHash

## Summary
Before/after the change below (two release builds of the same tree). Developer
laptop (macOS, aarch64, 10 CPUs); **not a capacity claim**.

## Change
Aggregation groups moved from `HashMap<Vec<u8>, usize>` plus a
`(Vec<Datum>, Vec<AggState>)` per group to a flat `GroupTable`: one
`hashbrown::HashTable` of group indices, an arena of key bytes, and flat key /
state arrays (no per-group allocation; key bytes compared against the arena).
Hashing switched from SipHash to aHash with a random per-table seed. A sampling
profile (`sample`) of query 08 before the change showed allocation
(malloc/free/memmove) and SipHash as the top costs after column decoding.

## Measurement conditions (read this first)
The machine was heavily loaded during this session (load average 5–29: other
applications, file indexing). Wall-clock comparisons — `strata-bench` 5 × 15
and 5 × 30 interleaved pairs, and 10 interleaved `strata shell` pairs on a
persisted dataset — had noise floors of up to ±17 % on unaffected queries and
are **not** used for the conclusions (their JSON is kept in
`2026-11-25-grouping-flat/`). The conclusions use **CPU time** (user + sys from
`/usr/bin/time -p`) of a `strata shell` process running one query 200 times on
the same persisted 200 000-event dataset (`tenant-0`), 5 alternating
before/after pairs per query. CPU time is not inflated by preemption, but load
still affects CPU frequency, so ranges remain wide for query 08.

## Results (CPU seconds per 200 executions, median [range] of 5 runs)
| Query | Before | After | Ratio |
|-------|--------|-------|-------|
| 08 group by `user_id` (≈19 k groups), top 10 | 8.48 [4.32–9.76] | 3.84 [2.43–6.82] | **2.2× less CPU** |
| 12 group by severity (6 groups) | 0.73 [0.63–0.82] | 0.62 [0.61–0.81] | 1.18× |
| 09 group by service, time bucket (48 groups) | 1.37 [1.23–1.46] | 1.37 [1.34–1.55] | 1.00× |
| 06 `LIKE` count (no grouping, control) | 1.25 [1.10–1.32] | 1.26 [1.23–1.33] | 0.99× |

Result rows were identical in every run of every method.

## Correctness evidence
`groups::tests` (first-seen order, lookup, growth, empty key) and the whole
query suite: engine equivalence proptests, `parallel_equals_sequential` (exact
group order across the parallel merge), pagination.

## Raw data
`benchmarks/reports/2026-11-25-grouping-flat/cpu-time/` (`cpu-<query>-<before|after>-<run>.txt`, query scripts `q<query>.sql`);
noisy wall-clock runs: `{before,after}-{1..5}.json`.

## Reproduction
```bash
strata-bench --events 200000 --reps 1 --data-dir /tmp/d --out /dev/null   # dataset
for r in 1 2 3 4 5; do for v in before after; do
  /usr/bin/time -p ./strata-$v shell --data-dir /tmp/d --tenant tenant-0 < q08.sql > /dev/null
done; done
```
