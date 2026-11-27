# Columnar memtable (spec STRATA-ARCH-02-012/013/014, STRATA-ARCH-04-002, STRATA-SYS-07-002) on HW-REF-7

## Summary
The memtable used to be a vector of `Arc<Event>` rows per tenant.
- **Snapshots** cloned every row handle (O(rows) per query).
- **Pruning:** memtable rows were never pruned.

It is now **column chunks**, with capacity doubling from 64 to 65,536 rows.
- **Visibility:** a published row count (Release/Acquire) makes a batch
  visible all at once, without locks on the read path.
- **Snapshots** copy chunk pointers, O(chunks).
- **Zone maps:** each chunk has min/max time, min/max sequence and a severity
  set, so queries prune memtable chunks like small segments.
- **Freezing** is an O(1) pointer swap.
- **WAL replay** decodes columnar records straight into columns.

**Setup:** two release binaries, the parent tree against this change, in 3
interleaved foreground pairs:
- `benchmarks/scenarios/memtable.toml`: 240k app-log events in 4 tenants,
  the first 40k flushed to one segment per tenant. The other 200k are left
  unflushed and replayed from the WAL when the harness reopens the engine.
- `benchmarks/scenarios/standard.toml`, as a regression check.

The two new dataset knobs (`max_flushes`, `final_flush`) are in both
binaries.

## Queries over memtable rows
Process CPU in ms. Each figure is the median of 10 repetitions, best of 3
runs:

| Query | Before | After | Speed-up | Rows scanned | Chunk windows pruned |
|---|---|---|---|---|---|
| m01 trace_id lookup | 6.30 | 5.39 | 1.2× | 60,000 → 60,000 | 0 |
| m02 recent errors (time + severity) | 4.93 | 1.56 | **3.2×** | 50,000 → 17,296 | 13 |
| m03 older time range | 2.74 | 0.87 | **3.1×** | 50,000 → 16,384 | 14 |
| m04 GROUP BY service | 2.71 | 2.22 | 1.2× | 60,000 → 60,000 | 0 |
| m05 severity = FATAL | 3.42 | 2.89 | 1.2× | 60,000 → 59,808 | 2 |
| m06 LIMIT 20 | 0.74 | 0.58 | 1.3× | 20 → 20 | 0 |

- **Results:** identical in every run.
- **Plan assertions:** `memory_chunks_pruned >= 1` on m02 and m03 now fail
  the scenario if zone-map pruning regresses.
- **Why queries that prune nothing still gain 1.2–1.3×:** snapshots no
  longer clone 50k row handles, and columns are read straight from chunks.
- **m01:** a trace-id lookup still scans every memtable row, because chunks
  have no Bloom filters.

## Regressions checked
- **Dataset load** (bulk ingest, 1,000-event batches; memtable scenario):

  | | events/s | CPU ns per event |
  |---|---|---|
  | Before | 62.5k, 62.0k, 62.2k | 1,802, 1,777, 1,741 |
  | After | 61.7k, 60.2k, 62.7k | 1,848, 1,874, 2,078 |

  That is about +4–9% CPU per event (column inserts), with throughput the
  same within noise.
- **Standard scenario** (segments; flush through the columnar memtable):
  - query CPU geometric mean ratio 1.002 against an A/A spread of 1.126;
  - every plan identical, and dataset load throughput the same;
  - one *baseline* run flagged its load generator client-bound and
    saturated under external load, so its open-loop numbers are excluded.
    All three candidate runs passed.
- **Power-loss simulation:** the full run (5 variants) took 54 s against
  51 s before, with 0 violations.
- **Measurement note:** while these were taken, unrelated processes put the
  host's 15-minute load average as high as 88 and the machine slept once.
  The runs reported here started after the load fell, with interleaving to
  spread what remained.

These are developer-laptop measurements (Apple M4, macOS), not capacity
claims.

## Correctness evidence
- **`crates/stratadb-storage/src/memtable.rs` tests:**
  - rows round-trip across growing chunks (64, 128, 256, 512, … ≤ 65,536);
  - zone maps;
  - column and row inserts are identical, in bytes and values;
  - 4 readers racing the writer only ever see whole batches, in order, with
    no unwritten cell;
  - mixed-tenant and empty batches are refused.
- **`stratadb_schema` property test:** rows → columns → rows is the
  identity, and the column-side size formula equals `Event::approx_size`, so
  ingest memory reservations stay exact.
- **Unchanged and passing:**
  - every storage and query suite: crash matrix, power-loss simulation,
    fixtures (`storage-v1`, `storage-wal-v3`, `encrypted-v1`), pagination,
    the vectorized and pruning equivalence properties, memory accounting;
  - the server suites.

## Reproduction
```bash
# baseline: the parent tree's strata-bench (same scenarios)
for r in 1 2 3; do
  base/strata-bench scenario benchmarks/scenarios/memtable.toml --out ab/base-$r --hw-profile HW-REF-7
  target/release/strata-bench scenario benchmarks/scenarios/memtable.toml --out ab/col-$r --hw-profile HW-REF-7
done
```
Raw results: `2026-11-27-columnar-memtable/{base,col,std-base,std-col}-{1,2,3}/`
and the two summary files.
