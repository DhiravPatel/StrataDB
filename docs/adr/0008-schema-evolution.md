# ADR-0008: Schema evolution and type conflicts

- **Status:** Accepted (for event schema v1)
- **Spec references:** §2.2, §6.2–§6.5

## Decision
- Fixed canonical columns + typed attribute maps (`Value` = null, bool, int,
  uint, float, string, bytes, timestamp, list, map). Attributes are *never*
  stringified; JSON integers stay integers.
- Type conflicts across events for the same key are **preserved, not coerced**:
  each value keeps its own type (variant semantics). Query comparison rules
  between types are defined in the query-language reference.
- Missing (key absent) ≠ null (`Value::Null`) ≠ empty string; preserved through
  WAL and segments (tested).
- Unknown top-level JSON fields are preserved in `attributes`; reserved names
  (`ingest_time`, `schema_version`) are rejected; a payload `tenant_id` never
  overrides the authenticated tenant.
- Every event and segment records `schema_version`; the event encoding has its
  own version byte; unknown versions are rejected as `Unsupported`.
