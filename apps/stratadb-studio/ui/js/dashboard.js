// Per-server dashboard: health, readiness and lifecycle state; ingest and
// query rates and latencies from /metrics; workload classes; memory
// regions; snapshots; threads. Refreshed on an interval.

import { h, replace, fmt, errorBox, loading, badge, kv, table, sparkline, storageGet, storageSet } from './dom.js';
import { server } from './api.js';

const HISTORY = 60;

function card(title, big, sub, extra) {
  return h('div', { class: 'card' }, h('h3', {}, title), h('div', { class: 'big' }, big), sub ? h('div', { class: 'sub' }, sub) : null, extra || null);
}

function section(title, ...body) {
  return h('section', { class: 'section' }, h('h2', {}, title), ...body);
}

function stateKind(st) {
  if (!st || !st.reachable) return 'bad';
  if (st.ready === true && st.state !== 'DEGRADED') return 'ok';
  return 'warn';
}

function sectionError(e, what) {
  if (e && e.status === 403) {
    return h('div', { class: 'notice' }, `The connection's token cannot read ${what} (needs cluster.read).`);
  }
  return errorBox(e);
}

function meterRow(used, limit) {
  if (!limit || !isFinite(limit) || limit <= 0) return fmt.bytes(used);
  const m = h('meter', { min: 0, max: limit, value: Math.min(used, limit), high: limit * 0.8, optimum: 0, 'aria-label': `${fmt.bytes(used)} of ${fmt.bytes(limit)}` });
  return h('span', { class: 'nowrap' }, m, ' ', `${fmt.bytes(used)} / ${fmt.bytes(limit)}`);
}

export function createDashboard(conn) {
  const srv = server(conn);
  const prefKey = 'strata-studio:dashboard-interval';
  const series = { ingest: [], queries: [], qp95: [], errors: [] };
  let timer = null;
  let disposed = false;
  let inflight = false;

  const interval = h('select', { 'aria-label': 'Refresh interval' },
    [['2000', '2 s'], ['5000', '5 s'], ['10000', '10 s'], ['30000', '30 s'], ['0', 'Paused']].map(([v, l]) => h('option', { value: v }, l)));
  interval.value = String(storageGet(prefKey, '5000'));
  interval.addEventListener('change', () => { storageSet(prefKey, interval.value); schedule(); });
  const refreshBtn = h('button', { class: 'btn small', type: 'button', onclick: () => refresh() }, 'Refresh now');
  const updated = h('span', { class: 'muted small' });
  const body = h('div', {}, loading('Loading dashboard…'));
  const el = h('div', {},
    h('div', { class: 'page-head' }, h('h1', {}, `Dashboard · ${conn}`), h('span', { class: 'grow' }), updated,
      h('label', { class: 'inline-controls' }, 'Refresh', interval), refreshBtn),
    body);

  function push(arr, v) {
    arr.push(typeof v === 'number' && isFinite(v) ? v : null);
    if (arr.length > HISTORY) arr.shift();
  }

  function schedule() {
    clearTimeout(timer);
    const ms = Number(interval.value);
    if (!disposed && ms > 0) timer = setTimeout(refresh, ms);
  }

  async function settle(p) {
    try { return { ok: true, v: await p }; } catch (e) { return { ok: false, e }; }
  }

  async function refresh() {
    if (disposed || inflight) return;
    if (document.hidden) { schedule(); return; }
    inflight = true;
    refreshBtn.disabled = true;
    const [status, metrics, workload, memory, snapshots, threads] = await Promise.all([
      settle(srv.status()), settle(srv.metrics()), settle(srv.admin('workload')),
      settle(srv.admin('memory')), settle(srv.admin('snapshots')), settle(srv.admin('threads')),
    ]);
    inflight = false;
    refreshBtn.disabled = false;
    if (disposed) return;
    if (metrics.ok) {
      const r = metrics.v.rates || {};
      push(series.ingest, r.ingest_events_per_s);
      push(series.queries, r.queries_per_s);
      push(series.errors, r.query_errors_per_s);
      const lat = (metrics.v.latency && metrics.v.latency.interval && metrics.v.latency.interval.query) || null;
      push(series.qp95, lat ? lat.p95_ms : null);
    }
    replace(body,
      renderHeader(status),
      metrics.ok ? renderRates(metrics.v) : section('Throughput and latency', sectionError(metrics.e, 'metrics')),
      h('div', { class: 'grid-2' },
        workload.ok ? renderWorkload(workload.v) : section('Workload classes', sectionError(workload.e, 'workload')),
        memory.ok ? renderMemory(memory.v, metrics.ok ? metrics.v : null) : section('Memory', sectionError(memory.e, 'memory'))),
      metrics.ok ? renderStorage(metrics.v) : null,
      snapshots.ok ? renderSnapshots(snapshots.v) : section('Snapshots', sectionError(snapshots.e, 'snapshots')),
      threads.ok ? renderThreads(threads.v) : section('Threads', sectionError(threads.e, 'threads')));
    updated.textContent = `Updated ${new Date().toLocaleTimeString()}`;
    schedule();
  }

  function renderHeader(res) {
    if (!res.ok) return h('div', { class: 'cards' }, card('Server', badge('error', 'bad'), res.e.message));
    const s = res.v;
    const kind = stateKind(s);
    const v = s.version || {};
    return h('div', { class: 'cards' },
      card('Health', badge(s.reachable ? 'serving' : 'unreachable', s.reachable ? 'ok' : 'bad'), s.reachable ? `status checked in ${s.elapsed_ms < 1 ? '< 1' : fmt.num(s.elapsed_ms)} ms` : (s.error || '')),
      card('Ready', badge(s.ready ? 'ready' : (s.reachable ? 'not ready' : '–'), kind), s.ready_status ? `GET /ready → ${s.ready_status}` : ''),
      card('Lifecycle state', s.state ? badge(String(s.state), s.state === 'READY' ? 'ok' : (s.state === 'DEGRADED' ? 'warn' : 'warn')) : '–', s.engine ? `engine: ${s.engine}` : ''),
      card('Version', v.server || '–', v.api ? `API ${v.api}${v.formats ? ` · WAL v${v.formats.wal}, segment v${v.formats.segment}` : ''}` : ''));
  }

  function renderRates(m) {
    const r = m.rates || {};
    const g = m.gauges || {};
    const win = m.interval_s ? `over the last ${m.interval_s.toFixed(1)} s` : 'waiting for a second sample';
    const lat = m.latency || {};
    const latRows = ['query', 'ingest', 'otlp_ingest', 'wal_sync', 'flush'].map(k => {
      const iv = lat.interval && lat.interval[k];
      const lt = lat.lifetime && lat.lifetime[k];
      const use = iv && iv.count > 0 ? iv : lt;
      return {
        name: { query: 'Query', ingest: 'Ingest ack', otlp_ingest: 'OTLP ingest ack', wal_sync: 'WAL fsync', flush: 'Memtable flush' }[k],
        win: use === iv && iv && iv.count > 0 ? 'interval' : 'since start',
        count: use ? use.count : null, p50: use && use.p50_ms, p95: use && use.p95_ms, p99: use && use.p99_ms, mean: use && use.mean_ms,
      };
    }).filter(x => x.count !== null);
    const q = m.queries_by_outcome || {};
    return h('div', {},
      section('Throughput', h('p', { class: 'muted small' }, `Rates ${win}.`),
        h('div', { class: 'cards' },
          card('Ingest events/s', fmt.rate(r.ingest_events_per_s), `${fmt.rate(r.ingest_batches_per_s)} batches/s · ${fmt.num(g.events_written_total)} total`, sparkline(series.ingest, 'ingest events per second')),
          card('Queries/s', fmt.rate(r.queries_per_s), `${fmt.num(q.ok)} ok · ${fmt.num(q.invalid)} invalid · ${fmt.num(q.rejected)} rejected · ${fmt.num(q.timeout)} timeout · ${fmt.num(q.cancelled)} cancelled · ${fmt.num(q.error)} error`, sparkline(series.queries, 'queries per second')),
          card('Query p95', fmt.ms(series.qp95[series.qp95.length - 1]), 'interval latency', sparkline(series.qp95, 'query p95 latency')),
          card('Query errors/s', fmt.rate(r.query_errors_per_s), `${fmt.rate(r.query_rejections_per_s)} rejections/s`, sparkline(series.errors, 'query errors per second')),
          card('Rows scanned/s', fmt.rate(r.rows_scanned_per_s), `${fmt.bytes(r.segment_bytes_read_per_s)}/s segment reads`),
          card('WAL write', `${fmt.bytes(r.wal_bytes_per_s)}/s`, `durable seq ${fmt.num(g.wal_durable_seq)}`))),
      section('Latency',
        table([
          { key: 'name', label: 'Operation' },
          { key: 'win', label: 'Window' },
          { key: 'count', label: 'Samples', num: true, render: x => fmt.num(x.count) },
          { key: 'p50', label: 'p50', num: true, render: x => fmt.ms(x.p50) },
          { key: 'p95', label: 'p95', num: true, render: x => fmt.ms(x.p95) },
          { key: 'p99', label: 'p99', num: true, render: x => fmt.ms(x.p99) },
          { key: 'mean', label: 'mean', num: true, render: x => fmt.ms(x.mean) },
        ], latRows, { emptyText: 'No latency histograms yet.' }),
        h('p', { class: 'muted small' }, 'Quantiles are interpolated from histogram buckets, as Prometheus histogram_quantile does.')));
  }

  function renderStorage(m) {
    const g = m.gauges || {};
    return section('Storage and engine',
      g.engine_failed ? h('div', { class: 'notice warn' }, 'The storage engine reports a failure (stratadb_engine_failed = 1). Check the server logs.') : null,
      h('div', { class: 'cards' },
        card('Memtable', fmt.bytes(g.memtable_bytes), `${fmt.num(g.memtable_rows)} rows`),
        card('Segments', fmt.num(g.segments), fmt.bytes(g.segment_bytes)),
        card('Disk free', fmt.bytes(g.disk_free_bytes)),
        card('Write stalls', fmt.num(g.write_stalls_total), `${fmt.num(g.write_rejections_total)} rejections · ${fmt.num(g.quota_rejections_total)} quota`),
        card('Compactions', fmt.num(g.compactions_total), `${fmt.num(g.flush_failures_total)} flush failures`),
        card('Query log drops', fmt.num(g.query_log_dropped_total), 'entries dropped (queue full)')));
  }

  function renderWorkload(w) {
    const rows = (w.classes || []).map(c => c);
    return section('Workload classes',
      h('p', { class: 'muted small' }, `${fmt.num(w.running)} running · ${fmt.num(w.queued)} queued (queue max ${fmt.num(w.queue_max)}) · max concurrent ${fmt.num(w.max_concurrent_queries)} · CPU ${fmt.pct(w.cpu_utilization)}`),
      table([
        { key: 'class', label: 'Class' },
        { key: 'slots', label: 'Slots', num: true, render: c => c.slots === c.configured_slots ? fmt.num(c.slots) : `${fmt.num(c.slots)} (of ${fmt.num(c.configured_slots)})` },
        { key: 'running', label: 'Running', num: true },
        { key: 'queued', label: 'Queued', num: true },
        { key: 'admitted_total', label: 'Admitted', num: true, render: c => fmt.num(c.admitted_total) },
        { key: 'rej', label: 'Refused', num: true, render: c => fmt.num((c.rejected_queue_full_total || 0) + (c.rejected_overloaded_total || 0) + (c.queue_timeouts_total || 0)) },
        { key: 'demoted_total', label: 'Demoted', num: true },
        { key: 'utilization', label: 'Utilization', num: true, render: c => c.utilization >= 0.9 ? badge(fmt.pct(c.utilization), 'bad') : fmt.pct(c.utilization) },
        { key: 'queue_wait_p99_ms', label: 'Wait p99', num: true, render: c => fmt.ms(c.queue_wait_p99_ms) },
      ], rows));
  }

  function renderMemory(mem, m) {
    const plan = mem.plan || {};
    const tree = mem.tree || { children: [] };
    const rows = (tree.children || []).map(c => ({ name: c.name, used: c.used, peak: c.peak, limit: c.limit }));
    const g = (m && m.gauges) || {};
    return section('Memory',
      kv([
        ['Budget', `${fmt.bytes(plan.budget)} (${plan.source || '?'})`],
        ['Tracked', fmt.bytes(mem.tracked_bytes)],
        ['Resident', fmt.bytes(g.memory_resident_bytes)],
        ['Untracked', fmt.bytes(g.memory_untracked_bytes)],
        ['Per-query limit', fmt.bytes(plan.per_query)],
      ]),
      h('div', { class: 'section' }),
      table([
        { key: 'name', label: 'Region' },
        { key: 'used', label: 'Used / limit', render: r => meterRow(r.used, r.limit) },
        { key: 'peak', label: 'Peak', num: true, render: r => fmt.bytes(r.peak) },
      ], rows, { emptyText: 'No memory regions reported.' }));
  }

  function renderSnapshots(s) {
    const engines = Object.entries(s).filter(([, v]) => v && typeof v === 'object');
    return section('Snapshots',
      engines.map(([name, e]) => h('div', { class: 'section' },
        h('h3', {}, name === 'logs' ? 'logs' : name === 'query_log' ? 'system.query_log' : name, ' ',
          e.warn ? badge('retained data above 10% of disk', 'warn') : null),
        h('p', { class: 'muted small' }, `${fmt.num((e.open || []).length)} open · ${fmt.num(e.obsolete_segments)} obsolete segments retained (${fmt.bytes(e.obsolete_bytes)}, ${fmt.pct(e.obsolete_ratio)} of ${fmt.bytes(e.disk_total_bytes)})`),
        table([
          { key: 'id', label: 'Id' },
          { key: 'tenant', label: 'Tenant' },
          { key: 'holder', label: 'Holder' },
          { key: 'age_ms', label: 'Age', num: true, render: x => fmt.age(x.age_ms) },
          { key: 'segments', label: 'Segments', num: true },
          { key: 'obsolete_bytes_pinned', label: 'Retained bytes', num: true, render: x => fmt.bytes(x.obsolete_bytes_pinned) },
        ], e.open || [], { emptyText: 'No open snapshots.' }))));
  }

  function renderThreads(t) {
    const roles = Object.entries(t.by_role || {});
    const pools = t.pools || {};
    return section('Threads',
      h('p', { class: 'muted small' },
        `${fmt.num(t.registered)} registered of a budget of ${fmt.num(t.budget)} (${fmt.num(t.logical_cpus)} logical CPUs)`,
        t.os_threads !== null && t.os_threads !== undefined ? ` · ${fmt.num(t.os_threads)} OS threads, ${fmt.num(t.untracked)} untracked` : '',
        ' ', t.within_budget === false ? badge('over budget', 'bad') : badge('within budget', 'ok')),
      h('div', { class: 'chips' }, roles.map(([r, n]) => badge(`${r}: ${n}`))),
      h('div', { class: 'section' }),
      kv([
        ['Scan pool', pools.scan ? `${fmt.num(pools.scan.workers)} workers · ${fmt.num(pools.scan.active_tasks)} active tasks` : '–'],
        ['Blocking pool', pools.blocking ? `${fmt.num(pools.blocking.workers)}/${fmt.num(pools.blocking.max_workers)} workers · ${fmt.num(pools.blocking.busy)} busy · ${fmt.num(pools.blocking.queued)} queued · ${fmt.num(pools.blocking.rejected)} rejected` : '–'],
      ]),
      h('details', {}, h('summary', {}, `All ${fmt.num((t.threads || []).length)} threads`),
        table([
          { key: 'name', label: 'Name' },
          { key: 'role', label: 'Role' },
          { key: 'cpu_seconds', label: 'CPU', num: true, render: x => fmt.ms(x.cpu_seconds * 1000) },
          { key: 'cpu_age_ms', label: 'Sampled', num: true, render: x => `${fmt.age(x.cpu_age_ms)} ago` },
          { key: 'affinity', label: 'Affinity' },
        ], t.threads || [])));
  }

  const onVisible = () => { if (!document.hidden) refresh(); };
  document.addEventListener('visibilitychange', onVisible);
  refresh();

  return {
    el,
    dispose() {
      disposed = true;
      clearTimeout(timer);
      document.removeEventListener('visibilitychange', onVisible);
    },
  };
}
