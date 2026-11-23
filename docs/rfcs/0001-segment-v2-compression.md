# RFC-0001: Segment format v2 — per-chunk compression

## Status
Accepted (implemented 2026-11-21)

## Authors
StrataDB maintainers

## Context
Baseline report (2026-11-20): segment bytes / encoded event bytes ≈ 1.14 —
lightweight encodings only. Spec §11 requires general-purpose compression
with measured cost, and an uncompressed path where compression does not help.

## Requirements
- Smaller segments for typical logs without unbounded CPU cost on ingest.
- Readers keep accepting v1 segments (golden fixture `tests/fixtures/storage-v1`).
- Decompression of untrusted bytes must be bounded (spec §26.3).
- No C toolchain requirement for builds (ADR-0021 rationale).

## Invariants
- Every chunk checksum covers the **stored (compressed)** bytes; the
  decompressed length must equal the recorded length exactly.
- Compression is chosen per chunk; a chunk is stored uncompressed when
  compression does not save at least 1/8 of its size.

## Options
1. zstd — best ratio, C library (`zstd-sys`).
2. LZ4 via `lz4_flex` — pure Rust, `unsafe`-free default features, very fast.
3. Keep v1 — rejected (no improvement).

## Decision
Segment format **v2**: each column-directory entry gains `u8 compression`
(0 = none, 1 = LZ4 block) and `uvarint raw_len` (decompressed length).
Writer: LZ4 per chunk, fall back to none if it saves < 12.5 %. Readers accept
v1 (no compression fields) and v2. zstd may be added later as `compression = 2`
for cold tiers without another format bump (unknown codes → `Unsupported`).

## Failure behavior
Corrupt compressed bytes are caught by the chunk CRC before decompression;
decompression output is capped at `raw_len` (≤ 1 GiB) and any mismatch is
`Corruption`.

## Security implications
Decompression bombs are impossible beyond `raw_len`, which is itself bounded
and covered by the footer checksum.

## Performance implications
Measured with the A/B harness (`strata-bench --no-compression`); see
`benchmarks/reports/2026-11-21-compression-ab.md`.

## Compatibility
v2 readers read v1 and v2. v1 readers (none released) reject v2 with
`Unsupported`. Manifest `segment_format_version` records 2 for new segments.

## Testing plan
Round-trip and every-byte-flip tests for v2; golden v1 fixture still passes;
fuzz targets cover v2 decoding; property tests unchanged.

## Rollout plan
New segments are written as v2; compaction rewrites v1 segments to v2.
