# Export, import and the shell

All three commands open the data directory directly (they take its lock), so
the server must be stopped. Filesystem access to the data directory is full
administrative access; `--tenant` is chosen by the operator.

## `strata export` (spec §30.3)
```bash
# All of a tenant's events, re-importable with `strata ingest`:
strata export --data-dir D --tenant acme --output acme.ndjson
# A time slice, as CSV:
strata export --data-dir D --tenant acme --format csv --output nov.csv \
  --where "event_time >= TIMESTAMP '2026-11-01T00:00:00Z' AND event_time < TIMESTAMP '2026-12-01T00:00:00Z'"
# Any row query:
strata export --data-dir D --tenant acme --format csv --output errors.csv \
  --sql "SELECT event_time, service, body FROM logs WHERE severity = 'ERROR' ORDER BY event_time"
```

* **Events mode** (no `--sql`): one JSON object per event in the ingest shape
  (`event_time` as RFC 3339 with nanoseconds, `service`, `severity`, `body`,
  ids, `attributes`, `resource_attributes`), in commit order. Exporting and
  re-importing is lossless for every event that was ingested as JSON
  (property-tested; `ingest_time` is reassigned on import). Lossy only where
  JSON cannot represent the data: timestamp or bytes values nested inside
  body/attributes become strings; an OTLP severity number paired with
  unrelated severity text keeps the normalized severity only.
* **Query mode** (`--sql`): the query's rows (missing values omitted in
  NDJSON). Row queries only — aggregates are bounded already; run them with
  `strata query`.
* **Consistency:** the export is one snapshot. Rows committed after the first
  page never appear, even across `--resume` (keyset pagination, ADR-0023).
* **Memory:** bounded by `--page-size` (default 10 000 rows).
* **Checkpoint / resume:** after every page the output is fsynced and
  `<output>.checkpoint` is atomically rewritten. If the export stops (crash,
  Ctrl-C, `--max-pages N`), rerun the same command with `--resume`: bytes written
  after the last checkpoint are truncated and the export continues where it
  left off. A finished export removes its checkpoint. A resume with a different
  tenant, query or format is refused.
* **Safety:** an existing output file is never overwritten; neither is an
  interrupted export (its checkpoint must be resumed or removed).

### Parquet (ADR-0025)
```bash
strata export --data-dir D --tenant acme --format parquet --output acme.parquet \
  [--where "..."]
strata ingest --data-dir D --tenant acme --file acme.parquet   # *.parquet or --format parquet
```
Events only (`--sql`, `--resume`, `--max-pages` are refused). One row group per
page, uncompressed. Columns: `event_time` INT64 TIMESTAMP(NANOS, UTC);
`service`, `severity` (normalized; absent when UNSPECIFIED), `severity_text`,
`body` (string bodies), `trace_id`, `span_id`, `event_id` as STRING;
`body_json` (non-string bodies), `attributes`, `resource_attributes` as JSON.
Written to `<output>.tmp` and renamed when complete — an interrupted export
leaves no partial file (rerun it). Re-importing an export reproduces the
events exactly (same lossy cases as NDJSON). Importing other Parquet files:
columns map by name like JSON keys (`event_time`/`timestamp` as a timestamp
of any unit, integer nanoseconds or RFC 3339 string; `message` = body; `level`
= severity; every other column becomes an attribute; nested struct/list/map
columns become JSON objects/arrays; binary becomes hex). Readable codecs:
uncompressed, snappy (pyarrow/DuckDB/Spark default), gzip, lz4 — zstd and
brotli files are refused with a clear error (rewrite them with snappy).
Interop is verified against pyarrow and DuckDB (`scripts/parquet_interop.sh`).

### CSV
RFC 4180: comma separator, CRLF line ends, fields with `,` `"` CR or LF are
quoted (quotes doubled), a header row. Missing values and null are empty
(indistinguishable in CSV — use NDJSON when it matters); non-string values are
JSON text (`{"k":1}`, `[1,2]`, `2.5`, `true`). Values are written verbatim:
**cells starting with `=`, `+`, `-` or `@` can be treated as formulas by
spreadsheet programs** (CSV injection) — do not open exports of untrusted log
data in a spreadsheet without disabling formula evaluation.

## `strata ingest` (import)
```bash
strata ingest --data-dir D --tenant acme --file acme.ndjson \
  --import-id acme-2026-11-25 --report import-report.json
```
* Input: NDJSON in the ingest mapping (docs/api/ingestion.md; `-` reads
  stdin) or Parquet (`*.parquet` or `--format parquet`).
* `--report`: JSON with `accepted`, `rejected`, `batches`, `duplicate_batches`
  and per-line `errors` (`line`, `error`, `message`; first 10 000, plus
  `errors_truncated`). Never overwrites an existing file. Exit code 3 when any
  line was rejected.
* `--import-id ID` makes the import **resumable and idempotent**: batch *i*
  is written with idempotency key `import:ID:i` and a SHA-256 of its source
  lines. Re-running an interrupted import of the *same file* skips every batch
  that already committed (`duplicate_batches`) and writes the rest. Reusing an
  ID with different file content is refused (`already_exists`) instead of
  merging. Bounded by the dedup window (default 24 h / 100 000 keys,
  ADR-0019): resume within that window, and keep `--batch-size` unchanged.

## `strata shell` (spec §22.1)
```text
$ strata shell --data-dir D --tenant acme
strata> SELECT service, count(*) AS n FROM logs
   ...>   GROUP BY service ORDER BY n DESC;
```
Statements end with `;`. Meta commands: `\q`, `\help`, `\format table|json|csv`,
`\timing` (statistics after each statement), `\rows N` (display limit for row
queries, default 1 000 — read with pagination, so a huge result is never
loaded; the footer says when more rows exist). Errors are printed and the
shell continues; the exit code is 3 if any statement failed. With piped input
no prompt is printed, so scripts work: `strata shell ... < queries.sql`. There
is no line editing or history (no readline dependency).
