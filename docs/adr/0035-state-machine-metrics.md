# ADR-0035: System state machines and `strata_state_*` metrics

- **Status:** Accepted
- **Spec references:** spec IX.22 (STRATA-ARCH-22-001..008); VIII.1.2
  (insert stages); ADR-0018 (HTTP API), ADR-0029 (query log)

## Context
Spec IX.22 defines eight state machines: query, insert, WAL segment, merge
task, connection, table, server and device. It requires the time spent in
each state to be exported as `strata_state_seconds{machine,state}`, so that
stuck or slow transitions are observable.

The code already had most of these phases (parse, plan, admission, WAL
append, group commit, rotation, reclamation, compaction), but they were not
modelled as states. Nothing checked the transitions against the spec, and
there was no single export.

## Decision
### One generic tracker (`stratadb_common::state`)
- **`state_machine!`** declares an enum with its spec names and the spec's
  transition table, transcribed exactly. Per machine it provides:
  - **`strata_state_seconds{machine,state}`:** a histogram from 10 µs to
    1 day, recorded when a state is left;
  - **`strata_state_entities{machine,state}`:** a gauge of the entities in
    each state now;
  - **`strata_state_illegal_transitions_total{machine}`:** a counter.
- **`Tracker<S>`** follows one entity.
  - **`to(state, reason)`** checks the edge against the table. An edge the
    spec does not list is refused: it is counted, logged at error level,
    and `debug_assert`s, so tests fail on it.
  - **`to_after(state, spent, reason)`** records a phase that was measured
    elsewhere, such as the engine's WAL and group-commit timings, without
    timing it twice.
  - **`Drop`** removes the entity from its state's gauge.
- **Cost:** a transition is two relaxed atomics plus one histogram
  observation, with no lock and no allocation. A machine's series appear in
  `/metrics` on first use, or at server start through `touch`.
- **Conformance:** `state_tables_match_the_spec` parses the IX.22 section of
  the master spec (state lists and transition tables) and requires the
  code's tables to equal them. Removing a transition was verified to fail
  the test.

### Where each machine is driven
| Machine | Driver | Notes |
|---|---|---|
| `query_lifecycle` | `query_inner` (server) | Parse and plan happen before admission (`exec::parse` → `Parsed::plan` → `execute_prepared`). Paths: `QUEUED` → `REJECTED` on concurrency, duplicate query id or a full pool; `RUNNING` → `STREAMING` → `FINISHED`; a cancel or timeout goes `CANCELLING` → `CANCELLED`; other errors go to `FAILED`. |
| `insert_lifecycle` | JSON and OTLP ingest | `RECEIVED` → `DECODED` → `VALIDATED` (or `REJECTED`), then `THROTTLED` only if the engine really waited (flush backlog or ingest memory), `LOGGED`, `DURABLE`, `VISIBLE`, `ACKED`. Phases are replayed from `WriteReceipt.timings`. |
| `wal_segment` | `Wal` | `PREALLOCATED` while the file is created, then `ACTIVE`. Rotation moves it to `SEALED`; reclamation to `CHECKPOINTED`, then `DELETED`. A poisoned WAL moves its active segment to `FAILED`. Segments found at open start `SEALED` (a reused last segment starts `ACTIVE`). |
| `merge_task` | `Shared::rewrite` (compaction, retention, upgrade and index rebuilds) | `PLANNED` → `RESERVED` (background memory) → `RUNNING` → `WRITTEN` (output durable) → `COMMITTED` (manifest). The disk check or a full memory region gives `SKIPPED`; an error while running gives `FAILED`. |
| `table` | `lifecycle::Lifecycle` | `CREATING` → `ACTIVE` when the engine opens. `ACTIVE` ↔ `READ_ONLY` while the engine is fail-stopped or disk space is below the reserve, re-evaluated every 10 s. |
| `server` | `strata-server serve` | `STARTING` → `RECOVERING` (config applied) → `WARMING` (engine open) → `READY` (listener bound). `READY` ↔ `DEGRADED` follows the device and table state. SIGTERM gives `DRAINING`; `STOPPED` follows the engine close. `/api/v1/ready` reports the state and returns 503 unless the server is `READY` or `DEGRADED`. |
| `device` | `lifecycle::DeviceMonitor` | See below. |
| `connection` | — | Defined and checked against the spec, but not driven (see Consequences). |

### Device health (spec IX.22.8)
The monitor reads the WAL fsync latency histogram every 10 s and works on
the window since the previous tick:
- **Baseline:** the p99 of the first 64 fsyncs gives `UNKNOWN` → `HEALTHY`.
  Normal windows then move the baseline slowly (EWMA with weight 1/8), so a
  gradually busier device is not called degraded.
- **Degraded:** every window with at least 16 fsyncs has p99 > 4 × baseline
  for 30 s.
- **Healthy again:** no slow window for 5 min. An idle window counts as
  normal.
- **Failed:** an I/O fail-stop of the engine while the device is known.
  `FAILED` → `UNKNOWN` is left to the operator: a restart starts `UNKNOWN`.

The p99 values are histogram bucket bounds, so the factor (4) is kept above
the bucket ratio (about 2–2.5×).

## Consequences
- **Spec gaps** (the tables are transcribed, not extended):
  - There is no `DEGRADED → DRAINING` edge, so a server that is degraded at
    SIGTERM stops from `DEGRADED`.
  - There is no `UNKNOWN → FAILED` edge, so an I/O failure before a device
    baseline exists stays `UNKNOWN`.
  - There is no edge into `FAILED` or `REJECTED` from `RECEIVED` for
    inserts, so an undecodable body or a quota or rate rejection is not
    tracked as an insert.
  - `FAILED` after `WRITTEN` is not an edge, so a merge whose manifest
    commit fails stays `WRITTEN`. The engine is fail-stopped in that case.
- **Not driven:**
  - **Insert `FLUSHED`:** requests are acknowledged long before a flush,
    and following each request to its flush would mean keeping per-request
    state.
  - **Merge `PAUSED` and `CANCELLED`:** maintenance has no pause or cancel.
  - **WAL `RECYCLED`:** segments are deleted, not recycled.
  - **Table `ALTERING`, `DETACHED` and `DROPPING`:** there is no DDL.
  - **`connection`:** the HTTP API authenticates per request, and there is
    no `HELLO` handshake.
- **Overhead** (`state::tests::transition_cost`, release build, Apple M4,
  load about 4, 5 runs):
  - **Per lifecycle:** one query lifecycle (create, 6 transitions, drop)
    costs **0.60–0.66 µs**, about 100–110 ns per transition.
  - **Breakdown:** the clock read dominates (`Instant::now` is 31–35 ns on
    macOS, one per transition). A histogram observation costs 7–8 ns, and
    the disabled `debug!` costs nothing measurable.
  - **Relative cost:** under 0.1% of a typical query (milliseconds of CPU).
    For inserts the cost is paid per request, not per event, which is about
    1 µs per HTTP request.
  - **Optimisations made:** a transition reads the clock once, and a state's
    index is its discriminant.
