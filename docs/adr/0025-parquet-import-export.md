# ADR-0025: Parquet import/export (CLI)

- **Status:** Accepted
- **Spec references:** §30.3 (import/export Parquet), §36 dependency policy; relates to ADR-0001 (Parquet as a *storage* format — still not adopted)

## Context
Operators need to move log data into analytics tools (DuckDB, Spark, pandas,
warehouses) and to load files produced by them. Parquet is the interchange
format. The workspace avoids heavy dependencies (disk budget, supply chain).

## Measurement (2026-11-25, isolated scratch build)
`parquet` 54 with `default-features = false` (no Arrow, no compression codecs):
36 crates on this platform (61 across all targets in `cargo metadata`), 11 s
release build, 112 MB build output. Licenses: MIT OR Apache-2.0 (45), MIT (7),
Apache-2.0 (2), BSD-2-Clause OR Apache-2.0 OR MIT (2), Unlicense OR MIT,
CC0-1.0 (tiny-keccak), (MIT OR Apache-2.0) AND Unicode-3.0, and one crate
offering MIT OR Apache-2.0 OR LGPL-2.1 — all permissive.

## Decision
* Parquet support lives **only in the `strata` CLI**, behind the default-on
  cargo feature `parquet` (`--no-default-features` builds without it). The
  engine, server and SDKs do not depend on it.
* Export: events only, fixed schema (docs/operations/export-import.md), one
  row group per page (bounded memory), **uncompressed** (every reader supports
  it; no C codecs), written to `<output>.tmp` and renamed when complete. No
  resume (a Parquet footer cannot be appended to) — rerun instead.
* `body` is split into `body` (STRING, string bodies) and `body_json` (JSON,
  structured bodies) so analysts get plain text without losing structure;
  maps are JSON columns (logical type JSON).
* Import: rows are mapped to the JSON ingest shape and pass through the normal
  ingest mapping and validation; idempotent `--import-id` and `--report` work
  as for NDJSON. Timestamp columns (ms/µs/ns, `event_time` or `timestamp`)
  convert exactly.

## Alternatives rejected
* Arrow (`arrow` + `parquet/arrow`): many more crates and much longer builds for
  no functional gain at this scope.
* Compression codecs (zstd/snappy): `zstd-sys` is C; `snap` is possible later
  behind a flag if file size matters.
* Parquet as the segment format: unchanged (ADR-0001; would need benchmarks).

## Evidence and limits
CLI tests: Parquet export → import into a new database → NDJSON exports
byte-identical (250 varied events, 3 row groups); schema and logical types
read back; refusal of overwrite/`--sql`/`--resume`; a foreign file (µs
timestamps, aliases, extra column → attribute) imports correctly (this test
found and fixed an inexact timestamp conversion); idempotent re-import.
**Not yet verified with an independent reader** (pyarrow/DuckDB are not
installed on the development machine): `scripts/parquet_interop_check.py`
performs that check where pyarrow is available.

## Revisit conditions
Query-mode Parquet export (typed columns), compression, server-side export,
or Parquet as a cold-tier segment format.
