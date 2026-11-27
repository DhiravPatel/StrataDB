# ADR-0033: Columnar memtable with published row counts

- **Status:** Accepted
- **Spec references:** STRATA-ARCH-02-012/013/014, STRATA-ARCH-04-001/002,
  STRATA-SYS-07-002 (replay into columns); ADR-0028 (ScanSource), ADR-0030
  (ingest memory), ADR-0031 (columnar WAL)

## Context
The memtable was a `Mutex`-protected map from tenant to `Vec<MemRow>`, with
one `Arc<Event>` per row. Three consequences:
- **Snapshots** cloned every row handle of the tenant. That is O(memtable
  rows) per query, and it dominated cheap queries once tens of thousands of
  rows were unflushed.
- **Pruning:** memtable rows had no statistics, so every query scanned all
  of them.
- **Replay:** WAL replay rebuilt rows even from columnar records.

## Decision
### Layout and visibility
- **Chunks:** each tenant (the partition) has an append-only list of column
  chunks. A chunk's capacity starts at 64 rows and doubles up to 65,536;
  then new chunks of 65,536 rows follow.
- **Cells:**
  - fixed-width columns (sequence, index, times, severity, schema version)
    are atomics;
  - variable-width columns are write-once cells (`OnceLock`).

  Nothing is allocated per row besides the values' own heap data.
- **Writers:** appends of one tenant are serialized by that tenant's writer
  lock (sharded per tenant, ARCH-04-001 (b)). A batch's cells are written,
  then the zone maps are widened (atomic min/max), then the tenant's
  published row count is stored with `Release`.
- **Readers** load the count with `Acquire` and read only rows below it, so
  a batch is visible entirely or not at all. There is no lock per row or
  per chunk.
  - A reader takes the tenant map's and chunk list's reader locks once per
    query to copy the chunk pointers.
  - The count is per tenant rather than per chunk. One count makes a batch
    that spans two chunks atomic, and each chunk's visible length follows
    from its start row.
- **Freezing:** the engine swaps in a new empty memtable under the write
  gate, which is O(1). The frozen one is immutable (no writer can reach it)
  and is read by flush and by queries concurrently.

### Queries
- **Chunk windows:** `MemChunk` is a window of at most 4,096 rows of one
  chunk, a `ScanSource` like a tiny unsorted segment.
- **Pruning:** it uses the chunk's zone map, which covers every row written
  to the chunk, so pruning is sound even for rows not yet visible. It prunes
  by event-time range and severity set. Attribute keys are not tracked in
  memtable chunks.
- **Stats and EXPLAIN:** `QueryStats.memory_chunks_pruned` and `EXPLAIN`
  report it.

### Replay and flush
- **Replay:** `columnar::decode_columns` produces `EventColumns`, the shared
  column batch type (`stratadb_schema::column`), which is appended to chunks
  directly with no row intermediate. Row records (WAL v1/v2) are converted
  once.
- **Flush:** one tenant at a time, the frozen view is materialized into rows
  for the segment writer. Writing segments straight from columns is future
  work.

### Memory accounting
Bytes per row are `Event::approx_size + 24`. The column-side formula
`EventColumns::row_approx_size` is property-tested equal to it, so ingest
memory reservations (ADR-0030) are released exactly on flush.

## Consequences
- **Measured** (report `2026-11-27-columnar-memtable`), on 200k unflushed
  rows:
  - time- and severity-filtered queries are 3.1–3.2× faster, scanning about
    a third of the memtable rows;
  - the other queries are 1.2–1.3× faster, from cheaper snapshots and direct
    column reads;
  - bulk ingest is about +4–9% CPU per event, with the same throughput;
  - segment queries and the power-loss simulation are unchanged.
- **Behaviour changes:**
  - `Snapshot::memory_rows()` is replaced by `memory_row_count()` and
    `memory_views()`.
  - `Memtable::insert_batch` now returns an error for an empty or
    mixed-tenant batch. The engine already rejects both before the WAL, and
    fails stop if it ever happened after the append.
- **Not yet:**
  - Bloom or key filters per chunk;
  - vectorized zone-map updates;
  - flushing from columns without materializing rows;
  - the per-series latest-value index (ARCH-02-015).
  - A small tenant still pays for a 64-row chunk.
