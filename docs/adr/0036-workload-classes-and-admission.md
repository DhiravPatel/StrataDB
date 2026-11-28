# ADR-0036: Workload classes, fair scan scheduling and admission control

- **Status:** Accepted
- **Spec references:** spec VIII.11 (STRATA-SYS-11-001, 11-003, 11-004,
  11-005, 11-008, 11-010); ADR-0006 (executor), the scan-pool report
  2026-11-26-scan-pool, ADR-0030 (memory accounting)

## Context
- **Admission was a semaphore:** queries were admitted with `try_acquire`,
  so the 17th concurrent query was refused at once. There was no queue, no
  classes, no fairness between tenants and no retry hint.
- **The scan pool was a plain FIFO channel:** a helper task scanned
  segments until its query was done, so a large scan held a worker for its
  whole duration and every class of work was equal.

Spec VIII.11 requires:
- weighted fair queueing over CPU time, by class, then tenant, then FIFO,
  with aging;
- plan-time classification, user/role overrides, and demotion at 2× the
  class budget;
- admission that considers slots, memory and pressure, and refuses with
  `TOO_MANY_QUERIES` / `SERVER_OVERLOADED` plus `retry_after_ms`;
- an AIMD controller on batch slots;
- per-class workload statistics.

## Decision
### Classes (`stratadb_common::workload`)
- **`WorkClass`:** `system`, `interactive` (weight 8), `normal` (4),
  `ingest` (4), `batch` (1) and `background` (1). Queries use interactive,
  normal and batch.
- **`Classifier`:** an estimate below 100 ms of CPU is interactive; at or
  above 10 s it is batch. Each class's time budget equals its upper
  threshold.
- **`ClassCell`:** a query's live class, shared with its queued work so a
  demotion takes effect at the next morsel.

### `FairQueue` (shared by the pool and admission)
- **Stride scheduling per class:** a class's virtual time advances by
  `cpu_ns / weight`, and the smallest goes next. `system` always goes
  first.
- **Within a class:** the tenant with the smallest virtual time goes next,
  then FIFO per tenant.
- **Charging:**
  - `pop` charges the estimate, so many dispatches before any completion
    still alternate;
  - `settle` corrects the charge by the measured CPU.
- **No credit hoarding:** a class or tenant with nothing queued and nothing
  running starts at the current minimum virtual time. Work that ran and
  comes back keeps its virtual time, because it is requeued before it is
  settled.
- **Aging:** a class's head gains one step (1 ms of virtual time) per
  `aging` waited (500 ms).
- **No lock, no I/O.** Callers serialize access and bound the size.

### Scan pool (`stratadb_query::pool`)
- **Stepped tasks:** a task is `FnMut() -> Step` and runs one morsel (one
  segment) per step.
- **Worker loop:** a worker pops by fair order, runs one step, measures its
  thread CPU and settles. If the task has more work, it is requeued under
  its *current* class, so interactive work preempts batch work at morsel
  boundaries.
- **Class caps:** tasks queued plus running stay at most 2 × workers. Of
  that, `normal` may fill 3/4 and `batch`/`background` half, so higher
  classes always find room.
- **Preemption of caller threads:** a lower-class query's own thread, at
  each of its morsel boundaries:
  - runs a waiting higher-class step first (`help_higher`). That CPU is
    reported as `cpu_lent` and is not billed to the query.
  - sleeps up to 2 ms or until its next segment completes, while a
    higher-class query is executing (`enter` guards count executing queries
    per class) and its own helper tasks hold its scan.

  A query with no helpers always scans itself, which guarantees progress.
- **Metrics:** CPU per class is exported as
  `strata_scan_pool_class_cpu_seconds_total`.

### Admission (`apps/stratadb-server/src/admission.rs`)
- **Classification:** after planning, `Prepared::estimate` computes a
  metadata-only estimate: the rows of unpruned segments plus memtable rows,
  times the per-row cost. A tenant class set in `[workload.tenant_classes]`
  overrides it. A request's `workload_class` may lower the class, never
  raise it, because raising would let any tenant jump the queue.
- **Slots:** per class, with defaults derived from `max_concurrent_queries`
  (all of it, 3/4, 1/4), under the global limit.
- **Admit or queue:** every arrival is pushed into the `FairQueue` and the
  dispatcher hands free slots to waiters, so new arrivals never barge.
  - The queue is bounded (`queue_max`, default 64).
  - A wait ends at `queue_timeout` (5 s) or the query timeout, whichever is
    shorter.
  - A queued query's approximate position is shown by
    `GET /api/v1/query/{id}`, and `DELETE` cancels it while it waits.
- **Memory and pressure:**
  - a query is admitted only while the query memory pool has
    `memory_floor_bytes` (1 MiB) free;
  - when Linux PSI `memory full avg10` is above 10%, only interactive
    queries are admitted. The controller reads `/proc/pressure/*` outside
    the lock.
- **Refusals:**
  - `TOO_MANY_QUERIES` (HTTP 429) when the queue is full or the wait timed
    out;
  - `SERVER_OVERLOADED` (HTTP 503) under pressure.

  Both carry `retry_after_ms` and `Retry-After`. The retry hint is the
  class's mean service time × (waiting + 1) / slots, clamped to
  100 ms – 30 s.
- **Demotion:** checked on every admission event and controller tick. A
  query running past 2 × its class budget moves down one class: its slot
  moves, its `ClassCell` changes, and its scan tasks follow at their next
  morsel.
- **AIMD controller,** every `controller_interval` (1 s):
  - if interactive p95 is above `latency_target_ms` (250 ms) and process
    CPU utilization is above 90%, batch slots drop by 1, down to
    `batch_slots_min`;
  - if p95 is below target and utilization is below 70%, they rise by 1,
    up to the configured value.
- **Statistics:** `GET /api/v1/admin/workload` and `strata_workload_*`
  report, per class:
  - slots, running and queued queries, and admission outcomes;
  - arrival rate, mean service time, utilization (`λ·S/slots`, spec
    VIII.11.4) and queue-wait p99;
  - demotions and CPU seconds.
- **Degree of parallelism:** batch queries use `target_ms_per_worker` of
  50 ms, others 5 ms (STRATA-SYS-11-008).

## Consequences
- **Measured effect** (report 2026-11-27-workload-classes):
  - no overhead on the concurrency scenario;
  - no measurable interactive-latency gain in the embedded mixed
    benchmark, where batch concurrency is unbounded and threads outnumber
    cores.

  The guarantees are established by deterministic tests. Load-level
  effects need the server's slot bound and a larger host.
- **Not done:**
  - the `ingest` and `background` classes are not scheduled: ingest has its
    own semaphore, and maintenance runs on its own thread. The background
    minimum share (STRATA-SYS-11-002) is therefore missing;
  - sequential, non-parallel scans neither yield nor lend;
  - `system.workload_stats` is an HTTP endpoint and metrics, not a SQL
    table;
  - PSI is Linux-only, so elsewhere only slots and memory gate admission;
  - an executing query is counted in the class it started with, until its
    scan ends.
- **API changes:**
  - query responses gain `stats.class` and `stats.final_class`;
  - `QueryStats` gains `cpu_lent` and `scan_yields`;
  - `QueryOptions` gains `class`.
