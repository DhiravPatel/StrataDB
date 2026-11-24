# Release checklist

Copy spec §34 for each release; every box needs linked evidence (test name,
report, or document). Status for the current tree:

## Data correctness
- [x] WAL framing and checksums validated — `stratadb-wal` tests
- [~] Crash recovery tested at every transition — process crashes ✅, power-loss model (WAL tail) ✅, FS fault injection ☐
- [x] Segment round trips pass
- [ ] Index results match scans — no indexes yet (pruning equivalence ✅)
- [ ] Compaction preserves results — not implemented
- [x] Query optimizer equivalence tests pass — `optimized_equals_unoptimized_equals_reference`
- [x] Concurrent write/read semantics documented — ADR-0017, durability.md
- [ ] Schema evolution tested across versions — single version only
- [x] Timestamp edge cases tested — `stratadb-common::time`
- [x] Corrupt data detected and reported

## Durability
- [x] Acknowledgement semantics documented (ADR-0009)
- [x] Sync behaviour tested · [x] Group commit tested
- [x] WAL reclamation tested under crash
- [ ] Disk-full behaviour tested
- [x] Process crash recovery tested
- [x] Machine-loss behaviour documented (not protected without backups)

## Security / Operations / Performance / Release
- [ ] all items — server, auth, TLS, metrics, backup, Docker, benchmarks,
      license, SBOM not done. See docs/project/KNOWN_LIMITATIONS.md.
