# ADR-0004: Timestamp partition strategy

- **Status:** Proposed
- **Spec references:** §7.5, §20.2

## Current behaviour (v0)
Segments are cut by flush (memtable size/rows/age), not by time partition. Each
segment records min/max `event_time`; the planner will prune by these bounds.
Late or far-past events simply land in the current flush's segment with wide
bounds (no sealed partitions, no rewrite).

## Proposal
Partition by `(tenant, event_time day)` at flush time once the query engine can
measure pruning benefit; compaction merges within a partition. Late data
creates new segments in the old partition (no rewrite). Acceptance window is
already enforced at ingestion (`Limits::max_past_age_ns`, `max_future_skew_ns`).

## Revisit
When Phase 3 benchmarks show time-range pruning loss from wide segments.
