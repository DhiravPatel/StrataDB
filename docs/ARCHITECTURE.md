# Architecture (as implemented)

```text
 strata CLI ──(local admin)──┐            (HTTP/OTLP server: not yet built)
                             ▼
                 ┌──────── Engine (stratadb-storage) ────────┐
 write_batch ──► │ validate tenant → backpressure → encode   │
                 │ → WAL append + durability wait (group     │
                 │   commit) → memtable insert → receipt     │
                 │                                           │
                 │ flush: freeze (write gate) → segment per  │
                 │ tenant → verify → MANIFEST commit → swap  │
                 │ → WAL reclaim                             │
                 └───┬────────────┬───────────────┬──────────┘
                     │            │               │
               stratadb-wal  stratadb-segment  MANIFEST (JSON+CRC)
                     │            │
               wal/*.log     segments/<tenant>/*.sseg

 query ──► stratadb-query: parse → plan (types, projection, pruning)
           → execute over tenant Snapshot (segments + memtable rows)
```

| Crate | Responsibility | Depends on |
|-------|----------------|------------|
| stratadb-common | errors, CRC-32C, codec, durable fs, failpoints, ids, time | — |
| stratadb-schema | event model, values, validation, JSON mapping, codec | common |
| stratadb-wal | WAL format, append/group commit, recovery, reclamation | common |
| stratadb-segment | segment v1 writer/reader/verify | common, schema |
| stratadb-storage | engine, memtable, manifest, flush, recovery, snapshots | all above |
| stratadb-query | StrataQL parser/planner/executor | storage, segment, schema |
| stratadb-cli | `strata` binary | storage, query |

Dependency rules (spec §5.2) hold: storage has no network code; the query
executor consumes the `Snapshot` interface; tenant checks exist in the storage
service boundary, not only at an API layer.

Key invariants and their enforcement are described in the ADRs (0002, 0003,
0007, 0009, 0017) and the format docs. The proposed full-system architecture
(spec §4) remains the target; components not listed above do not exist yet.
