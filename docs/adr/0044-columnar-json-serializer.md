# ADR-0044: Columnar JSON result serializer

- **Status:** Accepted
- **Spec references:** VIII.6 (STRATA-SYS-06-003; groundwork for 06-002)
- **Evidence:** `benchmarks/reports/2026-11-28-json-serializer.md`

## Context
JSON is the API's only result format: StrataStudio, the SDKs and `curl`
all use it. The old path had three costs:
- **A value tree per row:** every row became a `serde_json::Map`, with
  every column name cloned into it. The tree was then serialized a second
  time.
- **Encoding on the reactor:** the encoding ran on an async reactor thread
  after the blocking task returned. A large result therefore stalled the
  other connections on that thread.
- **Allocation-heavy timestamps:** each one was formatted with `format!`.

Measured on events-table rows, the old path wrote **186 MB/s per core**;
the spec requires at least 400.

## Decision
- **Encoding (`json_out::write_rows`):**
  - Rows are taken in blocks of 1,024.
  - Each column of a block is encoded in its own tight loop into a scratch
    buffer, with end offsets. A missing value encodes to nothing.
  - The block is then interleaved into row objects with keys encoded
    once.
- **Formatters:**
  - integers use `itoa`;
  - finite floats use `zmij::format_finite` (shortest round-trip), and
    non-finite floats are `null`;
  - timestamps use `time::write_rfc3339` (no allocation);
  - strings use serde_json's escape table, with runs copied in bulk.
- **Byte-identical output:**
  - keys come in sorted order (serde_json's `Map` is a BTreeMap here);
  - the escaping and number forms are serde_json's own;
  - property tests compare the two outputs, over escaping, key order,
    missing and short rows, nested lists and maps, and block boundaries.
- **Off the reactor:** the rows are encoded inside the blocking closure,
  after the admission permit is released, so a large result never
  occupies an async worker.
- **No copy:**
  - `envelope_parts` serializes the small envelope with serde_json, split
    around `"rows":` at its sorted position.
  - `ChunkedBody` sends prefix, rows and suffix as three `Bytes` with an
    exact size hint. Hyper therefore sends `Content-Length`, not chunked
    encoding, and the encoded rows are never joined into another buffer.
- **Dependencies:** `itoa`, `zmij`, `http-body` and `bytes` become direct
  dependencies. All four were already in the build, through serde_json
  and axum, so no new crate is compiled.

## Consequences
- **Throughput:** 587 MB/s per core on the same rows (3.17×), measured
  in-process with interleaved runs.
- **Clients:** the response bytes are unchanged, so nothing client-side
  changes.
- **The `serialize` stage** (`strata_query_stage_seconds`) now covers the
  row encoding on the blocking pool plus the envelope.
- **Not done:**
  - **Streamed results (06-002):** results are still built whole. The
    executor returns all rows, bounded by `max_result_rows`, before
    encoding starts. Streaming needs the executor to yield result blocks
    and a bounded output queue with backpressure.
  - **A native columnar protocol (06-001).**
