# Columnar WAL payloads (WAL v3, spec STRATA-SYS-07-002) on HW-REF-7

## Summary
WAL v3 writes each batch column by column:
- the tenant is stored once;
- timestamps are delta-coded;
- `service` and `severity_text` are dictionary-coded;
- the body is LZ4-compressed when it is 16 KiB or larger.

**Setup:** the same binary, `[engine] wal_format_version = 2` (row payloads)
against the default 3, in 3 interleaved foreground pairs of
`benchmarks/scenarios/group_commit.toml` (durability `group_commit`).

**Result: it trades CPU for WAL bytes.**
- WAL bytes per event fall 3.3×.
- Ingest CPU per event rises about 19%.
- Throughput and commit latency on this laptop, which is fsync-bound, are
  unchanged within noise.

| Dataset load (bulk, 1,000-event batches) | v2 rows | v3 columnar |
|---|---|---|
| WAL bytes per event | 232.5, 232.5, 232.5 | **69.8, 69.8, 69.8** |
| CPU ns per event (whole ingest path) | 889, 855, 851 | 1,053, 1,008, 1,055 |
| events/s | 214.8k, 222.2k, 219.7k | 204.1k, 219.6k, 208.3k |
| ack p50 | 4.36, 4.24, 4.34 ms | 4.39, 4.26, 4.36 ms |

| Ingest load | v2 rows | v3 columnar |
|---|---|---|
| open loop 20/s: p50 / p99 | 3.47–3.83 / 4.56–4.89 ms | 3.73–3.82 / 5.52–7.49 ms |
| open loop 400/s: p50 / p99 | 4.61–5.11 / 7.54–7.64 ms | 4.69–5.03 / 6.71–18.35 ms* |
| 1 closed-loop writer: rate | 317, 255, 271 /s | 305, 253, 294 /s |
| 8 closed-loop writers: rate / p99 | 2,229, 2,161, 2,081 /s / 4.19–5.85 ms | 2,338, 2,194, 2,484 /s / 4.12–4.50 ms |

\* One v3 run had a single tail outlier. The other two runs' p99 were 7.6
and 6.7 ms. The open-loop batches are small (below the compression
threshold), so their payloads differ only in layout.

## Where the CPU goes
Operator cost ledger (`perf/cpr_ledger.toml`, CPU ns per row, 20 runs, the
app-logs profile):

| Operation | ns/row |
|---|---|
| row batch encode (v2 write) | 89.7 |
| columnar batch encode, LZ4 (v3 write) | 184.1 |
| columnar batch encode without LZ4 (measured with compression disabled) | ~121 |
| row batch decode (v2 replay) | 807.3 |
| columnar batch decode, LZ4 (v3 replay) | 785.4 |

- **LZ4 costs about 65–70 ns per row**, and the column-major layout about
  20 ns. A last-value check and a linear-scan dictionary saved about 2 ns,
  which shows dictionary hashing was not the cost.
- **Replay is slightly faster** in v3 than in v2, LZ4 included.

## Assessment
- **Where v3 pays:** the spec requires columnar, LZ4-compressed payloads,
  and the win is I/O. 3.3× fewer WAL bytes means less device bandwidth and
  page-cache pressure, and smaller fsyncs, at high ingest rates. On HW-REF-1
  NVMe at 1M events/s that is 70 MB/s instead of 232 MB/s of WAL.
- **Where it does not:** on this laptop the commit path is bound by
  per-fsync latency (`F_FULLFSYNC`), not bytes, so the saving does not
  shorten commits here. The CPU cost is real everywhere.
- **Rollback:** `[storage] wal_format_version = 2` keeps the row payload. It
  also gives a rollback window to the previous release.
- **Not a capacity claim:** these are developer-laptop measurements (Apple
  M4, macOS).

## Correctness evidence
- **`stratadb_schema::columnar` tests:**
  - property round trip, plain and compressed;
  - arbitrary byte damage and truncation never panic and are rejected (or,
    for semantic bytes covered by the frame CRC, keep the row count);
  - dictionaries of 1–1,000 distinct values;
  - mixed tenants, empty batches, unknown versions and flags, trailing
    bytes, impossible row counts and decompression bombs are all rejected
    before allocating.
- **WAL v3 tests:**
  - golden fixture `tests/fixtures/wal-v3` (all four record types);
  - the v2 rollback window (v2 writing refuses columnar records), and the
    upgrade to a new v3 segment;
  - a v2-only writer still reads v3;
  - a columnar record inside a v2 segment is corruption.
- **Engine tests** (`crates/stratadb-storage/tests/compat_wal_v3.rs`):
  - golden data directory `tests/fixtures/storage-wal-v3`: a flushed
    segment, then unflushed plain, LZ4 and idempotent columnar records, all
    replayed exactly;
  - upgrading the v1 fixture and a v2 directory;
  - idempotency keys remembered across restarts in v3.
- **Unchanged and passing:** the crash matrix (21 cases), the power-loss
  simulation, the encrypted-v1 and storage-v1 fixtures, and the write-fault
  tests, now all on v3.
- **Fuzzing:** WAL recovery decodes by record type, and the event-codec
  target checks that the columnar format re-encodes stably.

## Reproduction
```bash
cargo build --release -p stratadb-bench
sed 's/^\[engine\]$/[engine]\nwal_format_version = 2/' benchmarks/scenarios/group_commit.toml > gc-v2.toml
for r in 1 2 3; do
  target/release/strata-bench scenario gc-v2.toml --out ab/v2-$r --hw-profile HW-REF-7
  target/release/strata-bench scenario benchmarks/scenarios/group_commit.toml --out ab/v3-$r --hw-profile HW-REF-7
done
target/release/strata-bench ledger --path ledger.toml --write
```
Raw results: `2026-11-26-columnar-wal/{v2,v3}-{1,2,3}/`, `summary.txt` and
`cpr_ledger.toml`.
