// Query log browser: filterable recent queries from system.query_log, and
// the top fingerprints by CPU.

import { h, replace, fmt, errorBox, loading, table, badge, sqlString, likeEscape, kv } from './dom.js';
import { server } from './api.js';
import { infoDialog } from './modal.js';

const WINDOWS = [['900000', 'last 15 min'], ['3600000', 'last hour'], ['21600000', 'last 6 h'], ['86400000', 'last 24 h'], ['604800000', 'last 7 days']];

function attr(k, alias = k) {
  return `attributes['${k}'] AS ${alias}`;
}

/** Build the StrataQL for the current filters. */
export function buildQueryLogSql(f) {
  const since = new Date(Date.now() - Number(f.window)).toISOString();
  const where = [`event_time >= TIMESTAMP ${sqlString(since)}`];
  if (f.status === 'ok') where.push("attributes['status'] = 'ok'");
  if (f.status === 'failed') where.push("attributes['status'] != 'ok'");
  if (f.fingerprint) where.push(`attributes['fingerprint'] = ${sqlString(f.fingerprint)}`);
  if (f.text) where.push(`body ILIKE ${sqlString(`%${likeEscape(f.text)}%`)}`);
  if (f.minMs > 0) where.push(`attributes['elapsed_us'] >= ${Math.round(f.minMs * 1000)}`);
  if (f.mode === 'top') {
    return [
      `SELECT attributes['fingerprint'] AS fingerprint, count(*) AS n,`,
      `       sum(attributes['cpu_us']) AS cpu_us, avg(attributes['elapsed_us']) AS avg_elapsed_us,`,
      `       max(attributes['elapsed_us']) AS max_elapsed_us, sum(attributes['rows_scanned']) AS rows_scanned`,
      'FROM system.query_log',
      `WHERE ${where.join('\n  AND ')}`,
      "GROUP BY attributes['fingerprint']",
      'ORDER BY cpu_us DESC',
      `LIMIT ${Number(f.limit)}`,
    ].join('\n');
  }
  return [
    `SELECT event_time, ${attr('query_id')}, ${attr('fingerprint')}, ${attr('status')},`,
    `       ${attr('elapsed_us')}, ${attr('queue_us')}, ${attr('exec_us')}, ${attr('cpu_us')},`,
    `       ${attr('rows_scanned')}, ${attr('result_rows')}, ${attr('table', 'source_table')}, body`,
    'FROM system.query_log',
    `WHERE ${where.join('\n  AND ')}`,
    'ORDER BY event_time DESC',
    `LIMIT ${Number(f.limit)}`,
  ].join('\n');
}

export function createQueryLog(conn, { openQuery } = {}) {
  const srv = server(conn);
  const mode = h('select', { 'aria-label': 'View' }, h('option', { value: 'recent' }, 'Recent queries'), h('option', { value: 'top' }, 'Top fingerprints by CPU'));
  const win = h('select', { 'aria-label': 'Time window' }, WINDOWS.map(([v, l]) => h('option', { value: v }, l)));
  win.value = '3600000';
  const status = h('select', { 'aria-label': 'Status' }, h('option', { value: '' }, 'any status'), h('option', { value: 'ok' }, 'ok'), h('option', { value: 'failed' }, 'failed'));
  const fp = h('input', { type: 'text', placeholder: 'fingerprint (16 hex)', pattern: '[0-9a-f]{16}', size: 18, 'aria-label': 'Fingerprint', spellcheck: 'false' });
  const text = h('input', { type: 'search', placeholder: 'text in query', 'aria-label': 'Text contained in the normalized query', spellcheck: 'false' });
  const minMs = h('input', { type: 'number', min: 0, step: 1, placeholder: 'min ms', 'aria-label': 'Minimum elapsed in milliseconds' });
  const limit = h('select', { 'aria-label': 'Row limit' }, ['100', '500', '1000'].map(v => h('option', { value: v }, v)));
  const runBtn = h('button', { class: 'btn primary', type: 'submit' }, 'Apply');
  const sqlBtn = h('button', { class: 'btn', type: 'button' }, 'Open in Query Tool');
  const out = h('div', {});
  const filters = h('form', { class: 'inline-controls section', role: 'search' }, mode, win, status, fp, text, minMs, limit, runBtn, sqlBtn);
  const el = h('div', {},
    h('div', { class: 'page-head' }, h('h1', {}, `Query log · ${conn}`)),
    h('p', { class: 'muted small' }, 'From system.query_log: the token\'s tenant only. Entries are written every few seconds (query_log_flush_ms), so the newest queries can take a moment to appear. Query text is normalized: literals are replaced by ?.'),
    filters, out);

  const current = () => ({
    mode: mode.value, window: win.value, status: status.value, fingerprint: fp.value.trim().toLowerCase(),
    text: text.value.trim(), minMs: Number(minMs.value) || 0, limit: limit.value,
  });

  async function load() {
    const f = current();
    if (f.fingerprint && !/^[0-9a-f]{16}$/.test(f.fingerprint)) {
      replace(out, h('p', { class: 'form-error' }, 'A fingerprint is 16 hexadecimal digits.'));
      return;
    }
    const sql = buildQueryLogSql(f);
    runBtn.disabled = true;
    replace(out, loading('Reading the query log…'));
    try {
      const r = await srv.query({ query: sql, timeout_ms: 30000, workload_class: 'normal' });
      replace(out, f.mode === 'top' ? renderTop(r) : renderRecent(r));
    } catch (e) {
      replace(out, e.body && e.body.error === 'unsupported'
        ? h('div', { class: 'notice' }, 'The query log is disabled on this server.')
        : errorBox(e, { onRetry: load }));
    } finally {
      runBtn.disabled = false;
    }
  }

  function statusBadge(s) {
    return s === 'ok' ? badge('ok', 'ok') : badge(s || '?', 'bad');
  }

  function renderRecent(r) {
    const rows = r.rows || [];
    return h('div', {},
      h('p', { class: 'muted small' }, `${fmt.num(rows.length)} entr${rows.length === 1 ? 'y' : 'ies'} · read in ${fmt.ms(r.stats && r.stats.elapsed_ms)}`),
      table([
        { key: 'event_time', label: 'Time', render: x => h('span', { class: 'nowrap' }, fmt.time(x.event_time)) },
        { key: 'status', label: 'Status', render: x => statusBadge(x.status) },
        { key: 'elapsed_us', label: 'Elapsed', num: true, render: x => fmt.us(x.elapsed_us) },
        { key: 'queue_us', label: 'Queue', num: true, render: x => fmt.us(x.queue_us) },
        { key: 'exec_us', label: 'Exec', num: true, render: x => fmt.us(x.exec_us) },
        { key: 'cpu_us', label: 'CPU', num: true, render: x => fmt.us(x.cpu_us) },
        { key: 'rows_scanned', label: 'Scanned', num: true, render: x => fmt.num(x.rows_scanned) },
        { key: 'result_rows', label: 'Rows', num: true, render: x => fmt.num(x.result_rows) },
        { key: 'fingerprint', label: 'Fingerprint', render: x => h('code', {}, x.fingerprint || '') },
        { key: 'body', label: 'Query', render: x => h('code', { class: 'grid-cell', title: typeof x.body === 'string' ? x.body : '' }, typeof x.body === 'string' ? x.body : JSON.stringify(x.body ?? '')) },
      ], rows, { emptyText: 'No queries match these filters in this window.', onRowClick: showEntry }));
  }

  function renderTop(r) {
    const rows = r.rows || [];
    return h('div', {},
      h('p', { class: 'muted small' }, 'Click a fingerprint to list its queries.'),
      table([
        { key: 'fingerprint', label: 'Fingerprint', render: x => h('code', {}, x.fingerprint || '(none)') },
        { key: 'n', label: 'Queries', num: true, render: x => fmt.num(x.n) },
        { key: 'cpu_us', label: 'CPU total', num: true, render: x => fmt.us(x.cpu_us) },
        { key: 'avg_elapsed_us', label: 'Avg elapsed', num: true, render: x => fmt.us(x.avg_elapsed_us) },
        { key: 'max_elapsed_us', label: 'Max elapsed', num: true, render: x => fmt.us(x.max_elapsed_us) },
        { key: 'rows_scanned', label: 'Rows scanned', num: true, render: x => fmt.num(x.rows_scanned) },
      ], rows, {
        emptyText: 'No queries in this window.',
        onRowClick: x => {
          if (!x.fingerprint) return;
          mode.value = 'recent';
          fp.value = x.fingerprint;
          load();
        },
      }));
  }

  function showEntry(x) {
    const body = typeof x.body === 'string' ? x.body : JSON.stringify(x.body);
    const details = h('div', {},
      kv([
        ['Time', fmt.time(x.event_time)], ['Query id', x.query_id], ['Fingerprint', x.fingerprint],
        ['Status', x.status], ['Table', x.source_table], ['Elapsed', fmt.us(x.elapsed_us)],
        ['Queue', fmt.us(x.queue_us)], ['Exec', fmt.us(x.exec_us)], ['CPU', fmt.us(x.cpu_us)],
        ['Rows scanned', fmt.num(x.rows_scanned)], ['Result rows', fmt.num(x.result_rows)],
      ]),
      h('pre', {}, body),
      openQuery ? h('p', {}, h('button', {
        class: 'btn', type: 'button',
        onclick: () => {
          document.getElementById('modal').close();
          openQuery(`-- From system.query_log (literals were replaced by ?)\n${body}`);
        },
      }, 'Open in Query Tool')) : null);
    infoDialog({ title: 'Query log entry', details });
  }

  filters.addEventListener('submit', e => { e.preventDefault(); load(); });
  mode.addEventListener('change', load);
  win.addEventListener('change', load);
  status.addEventListener('change', load);
  sqlBtn.addEventListener('click', () => openQuery && openQuery(buildQueryLogSql(current())));
  load();
  return { el };
}
