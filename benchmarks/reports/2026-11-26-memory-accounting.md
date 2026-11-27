# Memory accounting hierarchy (spec VIII.5.2) on HW-REF-7

## Summary
Queries, ingest, decoded filters and compaction now reserve memory on a
tracker tree. The tree has a process budget; ingest, cache, background and
query-pool regions; tenant nodes; and query nodes. This report checks that
the production path costs nothing measurable and changes no plan.

**Setup:** the same binary, `[engine] memory_accounting = true` against
`false`, in 3 interleaved foreground pairs of
`benchmarks/scenarios/standard.toml` (200k events, 4 tenants, 12 queries, 10
repetitions each).
- `true` is the server path: a process root with unlimited regions, the
  engine reserving ingest, cache and background memory, and every query
  reserving under a tenant node.
- `false`: no regions; each query is accounted on its own tracker only.

## Results
Process CPU in ms per query. Each figure is the median of 10 repetitions,
best of 3 runs:

| Query | Off | On | Ratio |
|---|---|---|---|
| q01 trace_id lookup | 2.857 | 2.841 | 0.99 |
| q02 request_id lookup | 1.659 | 1.649 | 0.99 |
| q03 recent errors | 1.470 | 1.468 | 1.00 |
| q04 service + severity | 4.692 | 4.671 | 1.00 |
| q05 time range | 1.326 | 1.326 | 1.00 |
| q06 message token search | 10.343 | 10.262 | 0.99 |
| q07 attribute equality | 6.095 | 6.066 | 1.00 |
| q08 high-cardinality GROUP BY | 3.811 | 3.845 | 1.01 |
| q09 service time bucket | 4.301 | 4.287 | 1.00 |
| q10 top error codes | 25.625 | 26.445 | 1.03 |
| q12 historical wide range | 2.462 | 2.485 | 1.01 |

- **CPU:** the geometric mean ratio is 1.001. The run-to-run spread within
  the "off" arm had a median of 1.018 (a quiet laptop). The difference is
  inside the noise.
- **Plans:** identical in all 6 runs, with every plan assertion passing.
  The compared fields are segments scanned, rows scanned, result rows and
  indexes used.
- **Dataset load:** off 50.1k, 51.7k and 51.7k events/s; on 51.5k, 51.6k and
  50.0k events/s.
- **q13:** fully pruned and runs in 3 µs, so it is excluded.

**Primitives** (release build, uncontended; `memory::tests::primitive_cost`):
- a reservation plus release across 4 levels: 23 ns;
- a batched usage update that stays within the reserved batch: 1 ns.

**Not measured here:** the executor's own per-query accountant and batch
estimate run in both arms. The primitive costs above and a few calls per
segment bound them.

These are developer-laptop measurements (Apple M4, macOS), not capacity
claims.

## Correctness evidence
- **`stratadb_common::memory` unit tests:**
  - leaf-to-root, all-or-nothing reservations, with rollback when an
    ancestor refuses;
  - 8 threads × 20,000 reservations never exceed a limit;
  - batching that still enforces exact usage near a limit;
  - waiting for memory;
  - error text with the top consumers, where other tenants appear only as
    `<other>`;
  - cgroup v1/v2 and meminfo parsing.
- **`crates/stratadb-query/tests/memory.rs`:**
  - batches are reserved before the read (a 16 KiB limit refuses a body
    batch);
  - reservations return to zero after successful and failed queries,
    sequential and parallel;
  - pool limits above the query apply and name the pool;
  - a query waits for memory and succeeds when it is released.
- **`crates/stratadb-query/src/exec.rs`:** the existing memory-budget tests
  still pass with the tracker in place of the old counter.
- **`crates/stratadb-storage/tests/memory.rs`:**
  - ingest bytes equal the memtable bytes and are released by flush;
  - a full ingest region refuses a write before the WAL append, the refused
    batch is never committed, and the error is retryable;
  - rows replayed at startup are tracked;
  - regions are independent;
  - decoded filters are tracked on the cache region;
  - compaction is postponed while background memory is full;
  - no reservation remains after the engine drops.
- **`apps/stratadb-server/tests/memory.rs`:**
  - a tenant's `query_memory_bytes` quota fails its query with
    `MEMORY_LIMIT_EXCEEDED`, without naming the other tenant;
  - the admin memory tree (`/api/v1/admin/memory`) and the
    `strata_memory_*` metrics work.

## Reproduction
```bash
cargo build --release -p stratadb-bench
sed 's/^durability = "group_commit"$/durability = "group_commit"\nmemory_accounting = false/' \
  benchmarks/scenarios/standard.toml > off.toml
for r in 1 2 3; do
  target/release/strata-bench scenario off.toml --out ab/off-$r --hw-profile HW-REF-7
  target/release/strata-bench scenario benchmarks/scenarios/standard.toml --out ab/on-$r --hw-profile HW-REF-7
done
cargo test --release -p stratadb-common --lib memory::tests::primitive_cost -- --ignored --nocapture
```
Raw results: `2026-11-26-memory-accounting/{off,on}-{1,2,3}/` and
`summary.txt`.
