# ADR-0005: Index architecture

- **Status:** Accepted for Bloom filters (F-003); exact-match postings and inverted indexes still Proposed
- **Spec references:** §12

## Proposal
- All indexes are **segment-local, derived, rebuildable** structures stored as
  optional footer extensions or sidecar files referenced by the manifest.
- Planned order: (1) footer statistics + key presence (done); (2) bloom
  filters for selected high-cardinality fields (trace_id, request_id);
  (3) sorted postings / roaring bitmaps for exact-match fields; (4) token
  inverted index for `body` with an explicit tokenizer version.
- No global per-value metadata. Index creation is manual in 0.x; the engine
  records predicate statistics and *recommends* indexes (spec §12.7).
- Every index path has a scan fallback and an equivalence test (index result == scan result).

## Revisit
After the Phase 2 query engine exists and the benchmark dataset generator is in place.

## Implemented (2026-11-21): Bloom filters
Segment footer extension tag 2; string values only; trace_id, span_id,
event_id + configurable attribute keys (default request_id, user_id,
session_id, correlation_id). Lazily decoded per segment handle; undecodable
filters are ignored (no pruning) rather than failing queries. Planner uses
top-level string `=` / `IN` conjuncts only. Evidence: no-false-negative test,
optimizer-equivalence property test with id predicates, A/B report
`benchmarks/reports/2026-11-21-bloom-ab.md` (exact-id lookups 2.6–2.8× faster,
+1 % storage).
