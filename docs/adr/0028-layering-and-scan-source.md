# ADR-0028: Mechanical layer check and the ScanSource seam

- **Status:** Accepted
- **Spec references:** IX.1.1 (STRATA-ARCH-01-001/002/003/004), IX.1.4,
  IX.11 (STRATA-ARCH-11-001/003); ADR-0026 (phase plan, A0)

## Context
The spec puts every crate in a layer, L1 platform to L7 clients:
- Dependencies point downward only. A layer may use the layer directly below
  it and L1, and nothing else.
- CI must enforce this mechanically.
- The execution engine (L3) must not know file formats. It receives column
  batches from a `ScanSource` trait implemented by storage (L2).

Before this change, nothing checked the crate graph. The executor
(`stratadb-query`) read segment footers, column chunks, shredded-key
directories, Bloom and trigram filters directly through `stratadb-segment`.
The planner also computed trigram probes, which are an index detail. The
decoded column types (`ColumnId`, `ColumnData`, `RawValues`, `Scope`) lived
in the segment crate.

## Decision
### Layer map and checker
- **Layer map:** `docs/architecture/layers.toml` gives every workspace crate
  a layer, or a span of adjacent layers for crates not yet split:
  - L1: `stratadb-common`, `stratadb-crypto`, `stratadb-schema` (the types
    crate);
  - L2: `stratadb-wal`, `stratadb-segment`, `stratadb-storage`;
  - L3–L4: `stratadb-query`;
  - L5–L6: `stratadb-server`;
  - L7: `stratadb-client`.
- **Composition roots:** `stratadb-cli`, `stratadb-bench`,
  `stratadb-testkit` and `stratadb-fuzz` are `role = "root"`, like
  strata-bench, -doctor and -sim in spec IX.11. They assemble or exercise the
  whole system, so they may use any crate, and nothing may depend on them.
- **Checker:** `scripts/check_layers.py` reads the graph from `cargo metadata`
  and fails on any of these:
  - an upward edge;
  - a skip-level edge that is not allowed;
  - any edge into a root;
  - a `[[forbid]]` edge;
  - an unlisted crate;
  - a stale rule (an allowed edge that no longer exists).

  Dev-dependencies may skip layers, because a test stands up a whole system.
  They may never point upward.
- **Exceptions:** each one names the spec section that sanctions it. There is
  one: `stratadb-server → stratadb-storage`, the IX.1.4 INSERT flow (L5
  admission hands the batch to the L2 table engine). Queries still go through
  L4.
- **Forbidden edges:** `stratadb-query → stratadb-segment / stratadb-wal`
  (ARCH-01-003). The layer rule alone would allow these, since L2 is directly
  below L3.
- **Where it runs:** `scripts/check.sh` and the CI check job run the
  checker's self-tests (one deliberate violation of each kind), then the
  check itself.

### The seam
- **Column types** (`ColumnId`, `ColumnData`, `Scope`, `RawValues` and its
  validating constructor) moved to `stratadb_schema::column`. That is the
  single column representation above L1 (ARCH-11-001). The segment crate
  re-exports them, and their persisted numbering is unchanged.
- **`stratadb_storage::scan`** defines the contract:
  - `ScanSource`: `owned_by`, `rows`, `event_time_range`, `seq_range`,
    `prune(&Pruning) -> Option<Pruned>`, and
    `read(tenant, &ColumnRequest) -> ColumnBatch`.
  - `Pruning`: the planner states sound predicates. These are time bounds,
    required keys, severities, `add_equals(field, values)` and
    `add_like(field, pattern)`. Storage compiles them to whatever indexes it
    has; trigram probes are computed in storage, once per query.
  - `IndexField`: logical field names.
  - `ColumnRequest`: the columns, plus the attribute keys needed per key, raw
    or decoded.
  - `ColumnBatch`: the columns, the per-key columns and the raw key columns,
    with bytes read.
- **Implementations:**
  - `SegmentHandle`: the former executor `prune` and `load_batch` logic,
    moved unchanged.
  - `MemChunk`: memtable rows, through the same interface (whole columns;
    never pruned).
  - `Snapshot::sources()` and `Snapshot::memory_chunks(n)` hand them to the
    executor.
- **Tenant scoping:** `read` re-checks ownership and fails for another
  tenant. The executor's pre-scan check stays.
- **Dispatch:** trait objects are used once per source, never per row
  (ARCH-11-003).

## Consequences
- **Format independence:** the executor and planner have no dependency on a
  file format, and the checker keeps it that way. A new part format, an
  object-store part or a system table becomes a new `ScanSource`, with no
  executor change. Compact parts (ARCH-02-001) will plug in here.
- **Unchanged behaviour:**
  - query results, pruning statistics, `EXPLAIN` text and index names;
  - the existing query suites (including the pruning-on/off property
    equivalence) and the storage fault tests.
- **Performance:** 3 interleaved foreground pairs of the standard scenario
  showed identical plans and no measurable CPU change (report
  `2026-11-26-scan-source-seam`).
- **Not yet:**
  - The batch is still row-major `Value` vectors. Physical-typed vectors,
    null bitmaps and selection vectors are IX.4/IX.5 work.
  - `stratadb-query` still spans L3 and L4. Splitting it into exec and sql
    crates is deferred until the planner grows a binder and an optimizer.

## Alternatives rejected
- **A Rust `xtask` crate:** the spec names `cargo-deny` or a custom `xtask`.
  A Python checker beside `scripts/spec_inventory.py` adds no build to the
  gate and no dependency. `cargo-deny` cannot express layers.
- **Re-exporting segment types through storage:** the executor would still
  compile against the segment crate's types, and the checker could not tell.
- **Allowing every skip-level edge from the server and CLI:** that would hide
  real violations. Composition roots are named explicitly, and the one
  layered exception cites the spec.
