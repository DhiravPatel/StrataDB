# StrataQL v0 semantics (normative)

## Scope and tenancy
A query runs against a **snapshot of the authenticated tenant only**. There is
no syntax to address another tenant; `tenant_id` is a constant column equal to
the caller's tenant, so `WHERE tenant_id = 'other'` matches nothing.

## Values
| Column | Type |
|--------|------|
| `event_time`, `ingest_time` | timestamp (ns, UTC) |
| `service`, `severity_text`, `trace_id`, `span_id`, `event_id`, `tenant_id` | string or missing |
| `severity` | string: `UNSPECIFIED TRACE DEBUG INFO WARN ERROR FATAL` |
| `severity_number` | int: 0, 1, 5, 9, 13, 17, 21 |
| `body` | any value or missing |
| `attributes['k']`, `resource_attributes['k']` | the stored typed value, or missing |

## Missing vs null
`IS NULL` is true only for a present null. `IS MISSING` is true only for an
absent field. `count(expr)` counts values that are neither.

## Three-valued logic
Comparisons with missing or null, or between incomparable types, are
**unknown**. `NOT unknown` is unknown; `AND`/`OR` follow Kleene logic.
`WHERE` keeps rows whose predicate is **true**. Consequently
`attributes['region'] != 'x'` does not match rows lacking `region`.

## Comparison
Numbers (int, uint, float) compare numerically with each other (int↔float via
f64). Strings compare by bytes (UTF-8, case-sensitive); booleans, timestamps
and bytes compare only within their own type. Lists and maps are incomparable.
There is **no implicit coercion**: comparing a canonical column with a literal
of the wrong type is a planning error (e.g. `event_time > '2026-01-01'` must be
written `event_time > TIMESTAMP '2026-01-01T00:00:00Z'`).

`LIKE` is case-sensitive, `ILIKE` folds case (Unicode simple lowercase); `%`
matches any run, `_` one character, `\` escapes. Non-string values are unknown.
`x IN (…)` is true if equal to any element, unknown if no element is equal but
some comparison was unknown, false otherwise.

## Aggregation
`count(*)` counts rows; `count(e)` non-null present values; `sum`/`avg` accept
only numbers (other types → error), `sum` of ints is exact (i128) and returns
int when it fits; `min`/`max` over incomparable values → error. Over zero rows,
`count` = 0 and the others are null. Every non-aggregate output must appear in
`GROUP BY`. `time_bucket(INTERVAL 'w', ts)` floors to a multiple of `w` since
the Unix epoch.

## Ordering and limits
`ORDER BY` uses a total order: by type (bool < number < timestamp < string <
bytes < list < map < null), then value; **null and missing sort last in ASC
and first in DESC**. Sorting is stable. Without `ORDER BY`, row order is
unspecified (currently source order) and `LIMIT` may stop scanning early.

## Pagination
`LIKE`/`ILIKE` never depend on indexes for correctness: trigram filters
(ADR-0005) only skip segments that cannot contain a literal run of the pattern.

Row queries can be paginated (`execute_page`; HTTP `page_size` / `cursor`;
ADR-0023). Pages follow the total order `ORDER BY keys…, then commit order
(seq, idx)`; without `ORDER BY`, commit order. Pages never overlap; every row
visible to the first page is returned exactly once (unless retention removes it
meanwhile); rows committed after the first page are excluded; `LIMIT` caps the
total across pages. Aggregates and `EXPLAIN` cannot be paginated. A cursor is
valid only with the same tenant and query text (surrounding whitespace ignored).

## Parallel execution
Segments of sorted and aggregate queries are scanned in parallel
(`QueryOptions::parallelism`, default min(CPUs, 8); 1 = sequential). Results
are identical to a sequential scan — same rows, same order including ties,
same group order — except that float `sum`/`avg` may differ in the last bits
because additions happen in a different order (still deterministic for a given
set of segments). Budgets apply to the whole query.

## Budgets (QueryOptions)
Timeout (default 30 s) → `timeout`; cancellation → `cancelled`; rows scanned
(100 M), result rows (100 k; add `LIMIT`), approximate memory (256 MiB) →
`resource_exhausted`. No partial results are returned in v0.

## EXPLAIN
`EXPLAIN` plans without scanning: columns read, time range, required keys,
severity set, segments pruned per reason. `EXPLAIN ANALYZE` executes and adds
rows scanned/matched, segment bytes read, peak memory estimate, and elapsed
time. Pruning never changes results (property-tested against an unpruned run
and an independent reference evaluator).
