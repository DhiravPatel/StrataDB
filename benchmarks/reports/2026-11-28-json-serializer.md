# Columnar JSON serializer: 2026-11-28 (ADR-0044)

**Command:** `cargo test --release -p stratadb-server --lib json_throughput -- --ignored --nocapture`

**Conditions.**
- **Machine:** the development laptop (Apple Silicon) on AC power.
- **Load:** a load average of about 4 from an external Python process at
  roughly 110% CPU, so the machine was not idle.
- **Method:** one process, both paths interleaved with the order
  alternating over 9 rounds, median reported. Single thread.

**Rows.** 100,000 rows shaped like the standard events table:
- `event_time`, a timestamp;
- `service` and `severity`, short strings;
- `body`, about 60 characters with a quote and a tab to escape;
- `trace_id`, 32 hex characters;
- `user_id`, missing in 1 row of 7;
- `latency_ms` (an integer) and `ratio` (a float);
- `attributes`, a two-entry map.

| path | output | throughput |
|---|---|---|
| old: `serde_json::Value` tree per row, then `serde_json::to_vec` | 32,247,802 bytes | 186 MB/s |
| new: `json_out::write_rows` (columnar blocks) | 32,247,802 bytes (identical) | **587 MB/s (3.17×)** |

The spec target (STRATA-SYS-06-003) is at least 400 MB/s per core, so it
is met.

**Response body.** The server sends the encoded rows without copying them
into another buffer; the test
`query_responses_have_an_exact_length_and_escaped_values` checks that the
response keeps an exact `Content-Length`. The time saved by not copying
was not measured separately.

**To do:** rerun on an idle machine, and measure end to end through the
HTTP server together with the pending A/B.
