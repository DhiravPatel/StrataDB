# StrataDB benchmark report: undecoded group keys and top-k aggregates

## Summary
On a high-cardinality `GROUP BY` of a shredded attribute (suite query 08,
about 19k groups, top 10), the change below uses **2.95× less CPU** (median of 5
interleaved pairs; the before and after ranges do not overlap). Queries it
does not target stay within noise. Measured on a developer laptop (Apple M4,
10 CPUs, macOS), so this is **not a capacity claim**.

## Change
Measured as two release builds: `ac11fa6` versus the working tree with this change.

1. **Group keys read undecoded.** An attribute key used *only* as a whole
   `GROUP BY` key (`Plan::raw_group_attr_keys`) is read from its shredded
   chunk as `RawValues`: encoded bytes plus row spans, validated by
   `skip_value` exactly as decoding would validate them. Its bytes are copied
   straight into the group key, with no `Value` or `String` allocated per row.
2. **Group tables store key bytes only.** The key values are decoded from the
   key arena once per output group. Before, each per-segment partial
   materialized and cloned them for every new group, and with about 19k
   distinct keys nearly every row starts a group in its segment's partial.
   Partials share the query's hash seed, so the merge reuses stored hashes and
   moves states instead of cloning them.
3. **Key sources resolved once per batch.** Before, every row cloned the key
   name and SipHash-looked it up in the batch's column map.
4. **Top-k for `ORDER BY … LIMIT k`.** The executor selects
   (`select_nth_unstable`, ties broken by position, so the result equals a
   stable sort followed by truncation) instead of sorting every row. When an
   aggregate query orders only by aggregate outputs, groups are ranked on
   their states, and keys are decoded and rows built only for the k selected.

A sampling profile (`sample`) of query 08 before the change showed
malloc/free/memmove (String allocation in shredded-column decoding, key
clones), per-row `eval_expr` for new groups, and SipHash as the top costs.
After the change, the top costs are value validation (`skip_value`, UTF-8
checks), group-table probes and ranking.

## Measurement conditions
The machine was shared (load average 5.0–5.5 during the runs). As in
`2026-11-25-grouping-flat.md`, conclusions use **CPU time** (user + sys from
`/usr/bin/time -p`) of a `strata shell` process running one query 200 times
against the same persisted 200,000-event dataset (`tenant-0`, created by
`strata-bench --events 200000`). That is 5 rounds, each running every query
before then after.

## Results (CPU seconds per 200 executions, median [range] of 5 runs)
| Query | Before | After | Ratio |
|-------|--------|-------|-------|
| 08 group by `attributes['user_id']` (≈19k groups), `ORDER BY n DESC LIMIT 10` | 2.68 [2.18–2.81] | 0.91 [0.81–1.10] | **2.95× less CPU** |
| 09 group by service, time bucket (48 groups) | 1.22 [0.98–1.30] | 1.09 [0.96–1.15] | 1.12× (overlapping ranges) |
| 12 group by severity (6 groups) | 0.50 [0.41–0.63] | 0.53 [0.44–0.70] | 0.94× (noise) |
| 06 `LIKE` count (no grouping, control) | 0.94 [0.86–0.98] | 0.92 [0.91–0.97] | 1.02× |

Median wall time for query 08 was 1.44 s before and 0.58 s after, per 200
executions. For every query, the complete output of all 200 executions was
byte-identical (SHA-1) between the two builds.

## Correctness evidence
- `stratadb-segment`:
  - `raw_values_agree_with_decoding` (proptest): raw indexing accepts exactly
    the chunks decoding accepts, valid or corrupted (byte flips, truncation),
    and returns identical encodings.
  - The v3 shredding test checks raw against decoded reads for every
    shredded key, and that corruption is detected.
- Fuzz target `map_projection`: the same differential check runs as an
  assertion on arbitrary bytes.
- `stratadb-query`:
  - `decode_keys_inverts_encoding` (proptest): a bit-exact round trip,
    including -0.0, NaN payloads and nested values.
  - `merge_keeps_order_and_combines_states`.
  - `top_k_equals_stable_sort_then_truncate` (proptest, heavy ties, mixed
    ASC/DESC, missing values).
  - `raw_group_keys_only_when_used_solely_as_group_keys`.
  - `shredded_attribute_columns_give_identical_results_with_less_io`, now
    with top-k-by-aggregate ties, null-versus-missing groups, first-seen order
    across segments and memtable, and mixed shredded/unshredded keys, all
    checked against an engine without shredding.
  - The existing parallel-versus-sequential and pagination suites.

## Raw data
`benchmarks/reports/2026-11-25-raw-group-keys/cpu-time/`:
`cpu-<query>-<before|after>-<run>.txt`, the query scripts `q<query>.sql`, and
the load averages in `load.txt`.

## Reproduction
```bash
strata-bench --events 200000 --reps 1 --data-dir /tmp/d --out /dev/null   # dataset
for r in 1 2 3 4 5; do for q in 08 12 09 06; do for v in before after; do
  /usr/bin/time -p ./strata-$v shell --data-dir /tmp/d --tenant tenant-0 < q$q.sql > /dev/null
done; done; done
```
