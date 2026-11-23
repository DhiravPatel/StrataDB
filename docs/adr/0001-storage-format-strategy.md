# ADR-0001: Storage format strategy

- **Status:** Accepted (provisional)
- **Date:** 2026-11-20
- **Spec references:** §10.1, §10.2, §37 (risk: custom format complexity)

## Context
The spec requires evaluating (1) Parquet as the active segment format, (2) a
custom hot format with Parquet export/cold tier, (3) Arrow IPC, and choosing a
custom format only if benchmarks show a meaningful advantage.

## Problem
Phase 1 needs *some* durable, checksummed, versioned segment format to prove
the WAL → flush → manifest → recovery protocol. The benchmark evaluation needs
a working engine and a dataset generator, neither of which existed.

## Options considered
1. **Adopt Parquet now** (`parquet` + `arrow` crates). Mature and interoperable.
   Costs: large dependency tree (≈ hundreds of transitive crates; multi-GB
   `target/` — the development machine had 27 GB free), Arrow schema mapping
   for dynamic attribute maps, and Parquet's footer/row-group checksum model
   would need wrapping to meet our "every byte checksummed" invariant.
2. **Minimal custom columnar format v1** with per-chunk CRC-32C, versioned
   header, TLV extensions; Parquet evaluated later behind the same reader API.
3. **Row-oriented log files only.** Simplest, but forecloses column pruning.

## Decision
Option 2, explicitly **provisional**. The format is versioned (`STRSEG` v1) and
isolated in `stratadb-segment` behind `write_segment` / `SegmentReader`, so a
Parquet-backed (or hybrid) v2 can be introduced as a new format version without
touching the WAL, manifest, or recovery protocol.

## Consequences
- Small, fully understood, fully checksummed format; every byte flip is detected (tested).
- We now carry a maintenance obligation for v1 readers for the compatibility window (ADR-0015).
- No Parquet interoperability until export (spec §30.3) is implemented.

## Alternatives rejected
Option 3: no projection pruning, no per-column statistics.

## Evidence
`crates/stratadb-segment` tests: round-trip property tests, every-byte-flip
detection, truncation detection. **No performance evidence yet.**

## Revisit conditions (mandatory)
Before 0.2: run the Phase 3 benchmark suite comparing v1 vs Parquet (write
throughput, read latency, compression ratio, projection cost, metadata
overhead). If Parquet is within agreed thresholds, adopt it as segment v2.
