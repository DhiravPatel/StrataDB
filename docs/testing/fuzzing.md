# Fuzzing (spec §26.3)

Entry points: `crates/stratadb-fuzz/src/lib.rs` — one function per untrusted
input surface. Contract: no panic and no unbounded allocation for any input.

| Target | Covers |
|--------|--------|
| `wal_frames` | WAL header/frame parsing, valid-frame scanning |
| `wal_recovery` | full WAL recovery over an arbitrary final segment file |
| `segment` | segment open (header/trailer/footer), verify, row decode, Bloom decode |
| `segment_internals` | footer decoder (v1/v2), column decoders and LZ4 decompression **bypassing CRCs** |
| `manifest` | MANIFEST envelope + JSON, BACKUP envelope + path validation |
| `event_codec` | event/batch codec, idempotency record/extension, Bloom extension |
| `query` | lexer, parser, planner and **executor** (tight budgets) on a fixed dataset |
| `json_event` | JSON ingestion mapping and OTLP/JSON mapping |
| `config` | TOML config parsing and resolution |
| `security_files` | token store loading/verification, audit-log verification |
| `cursor` | pagination cursor decoding (raw text and bodies **bypassing base64/CRC**) and cursors wrapped with a valid version/fingerprint/CRC so fuzzed key values reach the executor's cursor comparisons |
| `sealed` | encryption at rest: sealed-container header and length arithmetic, block authentication, random-access reads, KEYRING framing, full recovery of an encrypted WAL segment (keys of the golden fixture `tests/fixtures/encrypted-v1`) |
| `map_projection` | attribute key projection over a map chunk and `skip_value` (validated like `decode_value`) |

## 1. Stable mutation fuzzing (every test run)
`cargo test -p stratadb-fuzz` mutates real artifacts written by the current
code (WAL segments, segments with Bloom + idempotency extensions, MANIFEST,
BACKUP, token store, audit log, queries, JSON/OTLP, config) with seeded bit
flips, overwrites, insertions, deletions, duplications, truncations, splices
and byte repetition. Default 1 500 iterations per target (a few seconds);
deeper: `STRATADB_FUZZ_ITERS=30000 cargo test -p stratadb-fuzz`.

## 2. Coverage-guided fuzzing (nightly, libFuzzer)
```bash
rustup toolchain install nightly --profile minimal
cargo +nightly install cargo-fuzz --locked
cd fuzz && cargo +nightly fuzz run query -- -max_total_time=300 -rss_limit_mb=2048
```
Seed the corpus with real artifacts first — checksums otherwise reject almost
every mutation before it reaches the decoders (measured: `segment` coverage
140 unseeded vs 1 043 seeded):
`cargo test -p stratadb-fuzz -- --ignored export_corpus` then
`cargo +nightly fuzz run segment corpus/segment -- -max_total_time=300`.
Crashes land in `fuzz/artifacts/<target>/`; every crash must become a
regression test before it is fixed.

## Campaign log
| Date | Mode | Targets | Result |
|------|------|---------|--------|
| 2026-11-21 | stable mutation, 1 500 iters/target | all 9 | no panics |
| 2026-11-21 | stable mutation, 30 000 iters/target (10 000 for fs-heavy) | all 9 | no panics (≈ 210 k inputs, 109 s) |
| 2026-11-21 | libFuzzer (nightly, cargo-fuzz 0.13.2), 60 s/target, unseeded, rss ≤ 2 GiB | all 9 | **no crashes**: wal_frames 1 058 223 runs, wal_recovery 53 571, segment 39 692, manifest 598 081, event_codec 574 579, query 1 463 325, json_event 1 396 387, config 650 167, security_files 29 769 |
| 2026-11-21 | libFuzzer 120 s, segment format v2, seeded corpus | segment, segment_internals | no crashes: 231 445 runs (cov 1 043), 2 548 570 runs (cov 1 370) |
| 2026-11-25 | stable mutation, 1 500 iters/target (500 for fs-heavy) | all 14 (new: cursor, sealed, map_projection) | no panics |
| 2026-11-25 | libFuzzer (nightly, cargo-fuzz 0.13.2), 600 s/target in parallel, seeded from real cursors / the encrypted golden fixture / encoded map chunks, rss ≤ 2 GiB, max_len 64 KiB | cursor, sealed, map_projection | **no crashes**: cursor 3 809 397 runs (cov 5 916), sealed 84 416 runs (cov 1 262; each input writes temp files and runs WAL recovery), map_projection 4 307 900 runs (cov 618) |
