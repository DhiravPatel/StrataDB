// Query tool: editor, Run / Explain / Explain Analyze / Cancel, paged
// results grid with timing stats, errors and refusals, export, history.

import { h, replace, clear, fmt, errorBox, empty, toast, randomId, storageGet, storageSet, badge } from './dom.js';
import { server } from './api.js';
import { createEditor } from './editor.js';
import { toCSV, toJSON, download, stamp } from './export.js';

const HISTORY_MAX = 100;
const HISTORY_SQL_MAX = 16 * 1024;
const CELL_PREVIEW = 200;

function historyKey(conn) {
  return `strata-studio:history:${conn}`;
}

export function loadHistory(conn) {
  const v = storageGet(historyKey(conn), []);
  return Array.isArray(v) ? v.slice(0, HISTORY_MAX) : [];
}

function pushHistory(conn, entry) {
  const sql = entry.sql.slice(0, HISTORY_SQL_MAX);
  const list = loadHistory(conn).filter(e => e.sql !== sql);
  list.unshift({ ...entry, sql });
  storageSet(historyKey(conn), list.slice(0, HISTORY_MAX));
}

function cellView(v) {
  if (v === undefined) return { text: '', cls: 'missing', title: 'missing' };
  if (v === null) return { text: 'NULL', cls: 'null', title: 'null' };
  if (typeof v === 'object') {
    const s = JSON.stringify(v);
    return { text: s.length > CELL_PREVIEW ? `${s.slice(0, CELL_PREVIEW)}…` : s, cls: 'obj' };
  }
  const s = String(v);
  return { text: s.length > CELL_PREVIEW ? `${s.slice(0, CELL_PREVIEW)}…` : s, cls: '' };
}

export function createQueryTool(conn, { sql = '' } = {}) {
  const srv = server(conn);
  const st = {
    running: false, queryId: null, startedAt: 0, timer: null, poll: null,
    sql: '', paginate: true, explain: false, pages: [], pageIndex: 0, columns: [],
    last: null, disposed: false,
  };

  // Toolbar
  const runBtn = h('button', { class: 'btn primary', type: 'button', title: 'Run (Ctrl+Enter / Cmd+Enter); runs the selection if any', onclick: () => run('run') }, '▶ Run');
  const explainBtn = h('button', { class: 'btn', type: 'button', title: 'Show the plan', onclick: () => run('explain') }, 'Explain');
  const analyzeBtn = h('button', { class: 'btn', type: 'button', title: 'Run and show the plan with actual counts', onclick: () => run('analyze') }, 'Explain Analyze');
  const cancelBtn = h('button', { class: 'btn danger', type: 'button', disabled: true, onclick: cancel }, '■ Cancel');
  const pageSize = h('select', { 'aria-label': 'Rows per page' },
    [50, 100, 500, 1000, 5000].map(n => h('option', { value: n }, String(n))));
  pageSize.value = '100';
  const klass = h('select', { 'aria-label': 'Work class' },
    h('option', { value: '' }, 'auto'), h('option', { value: 'normal' }, 'normal'), h('option', { value: 'batch' }, 'batch'));
  const timeout = h('input', { type: 'number', min: 1, max: 300, step: 1, value: 60, 'aria-label': 'Timeout in seconds' });
  const csvBtn = h('button', { class: 'btn', type: 'button', disabled: true, onclick: () => exportRows('csv') }, 'CSV');
  const jsonBtn = h('button', { class: 'btn', type: 'button', disabled: true, onclick: () => exportRows('json') }, 'JSON');
  const histBtn = h('button', { class: 'btn', type: 'button', 'aria-expanded': 'false', onclick: toggleHistory }, 'History');
  const toolbar = h('div', { class: 'qt-toolbar', role: 'toolbar', 'aria-label': 'Query tool' },
    runBtn, explainBtn, analyzeBtn, cancelBtn,
    h('span', { class: 'sep' }),
    h('label', {}, 'Page', pageSize),
    h('label', { title: 'A request can lower its work class, never raise it' }, 'Class', klass),
    h('label', {}, 'Timeout (s)', timeout),
    h('span', { class: 'sep' }),
    h('span', { class: 'muted small' }, 'Export'), csvBtn, jsonBtn,
    h('span', { class: 'sep' }),
    histBtn);

  const editor = createEditor({ value: sql, onRun: () => run('run') });
  const editorBox = h('div', { class: 'qt-editor' }, editor.el);
  const status = h('div', { class: 'qt-status', role: 'status', 'aria-live': 'polite' }, h('span', { class: 'muted' }, 'Ready. Ctrl+Enter runs the query (or the selection).'));
  const results = h('div', { class: 'qt-results' }, empty('Run a query to see results here.'));
  const prevBtn = h('button', { class: 'btn small', type: 'button', disabled: true, onclick: () => gotoPage(st.pageIndex - 1) }, '◀ Prev');
  const nextBtn = h('button', { class: 'btn small', type: 'button', disabled: true, onclick: nextPage }, 'Next ▶');
  const pageInfo = h('span', { class: 'muted' });
  const pager = h('div', { class: 'qt-pager', hidden: true }, prevBtn, pageInfo, nextBtn, h('span', { class: 'grow' }), h('span', { class: 'muted', text: '' }));
  const detail = h('div', { class: 'qt-detail', hidden: true });
  const history = h('div', { class: 'history', hidden: true, role: 'dialog', 'aria-label': 'Query history' });
  const el = h('div', { class: 'qt' }, toolbar, editorBox, status, results, pager, detail, history);

  // ---------------------------------------------------------------------------

  function setRunning(on, id) {
    st.running = on;
    st.queryId = on ? id : null;
    runBtn.disabled = explainBtn.disabled = analyzeBtn.disabled = on;
    cancelBtn.disabled = !on;
    clearInterval(st.timer);
    clearInterval(st.poll);
    if (on) {
      st.startedAt = performance.now();
      const label = h('span', { class: 'stat' });
      const phase = h('span', { class: 'stat muted' });
      replace(status, h('span', { class: 'spinner', 'aria-hidden': 'true' }), label, phase);
      const tick = () => { label.textContent = `Running… ${fmt.ms(performance.now() - st.startedAt)}`; };
      tick();
      st.timer = setInterval(tick, 100);
      // Show queue position / state from GET /query/{id} while it runs.
      st.poll = setInterval(async () => {
        if (!st.running || st.queryId !== id) return;
        try {
          const s = await srv.queryStatus(id);
          if (st.queryId !== id) return;
          phase.textContent = s.state === 'queued'
            ? `queued (${s.class}${s.queue_position !== null && s.queue_position !== undefined ? `, position ${s.queue_position}` : ''})`
            : `${s.state} (${s.class})${s.cancel_requested ? ', cancelling' : ''}`;
        } catch {
          // 404 once it has finished: nothing to show.
        }
      }, 1000);
    }
  }

  async function cancel() {
    const id = st.queryId;
    if (!id) return;
    cancelBtn.disabled = true;
    try {
      await srv.cancel(id);
      toast('Cancellation requested.');
    } catch (e) {
      if (e.status === 404) toast('The query had already finished (or not started yet).');
      else toast(`Cancel failed: ${e.message}`, 'bad');
      if (st.running && st.queryId === id) cancelBtn.disabled = false;
    }
  }

  function renderStats(resp, ms) {
    const s = resp.stats || {};
    const page = st.pages.length;
    const rows = (resp.rows || []).length;
    const parts = [
      h('span', { class: 'stat' }, badge(resp.explain ? 'plan' : 'ok', 'ok')),
      resp.explain ? null : h('span', { class: 'stat' }, h('b', {}, fmt.num(rows)), ` row${rows === 1 ? '' : 's'}`, st.paginate && !resp.explain ? ` (page ${page})` : ''),
      h('span', { class: 'stat', title: 'Server-side elapsed (from arrival)' }, 'elapsed ', h('b', {}, fmt.ms(s.elapsed_ms))),
      h('span', { class: 'stat', title: 'Round trip through the studio' }, 'round trip ', h('b', {}, fmt.ms(ms))),
      h('span', { class: 'stat' }, 'queue ', h('b', {}, fmt.us(s.queue_us))),
      h('span', { class: 'stat' }, 'parse ', h('b', {}, fmt.us(s.parse_us))),
      h('span', { class: 'stat' }, 'plan ', h('b', {}, fmt.us(s.plan_us))),
      h('span', { class: 'stat' }, 'exec ', h('b', {}, fmt.us(s.exec_us))),
      s.cpu_us !== undefined ? h('span', { class: 'stat' }, 'cpu ', h('b', {}, fmt.us(s.cpu_us))) : null,
      s.class ? h('span', { class: 'stat', title: 'Work class at admission → at the end' }, 'class ', h('b', {}, s.final_class && s.final_class !== s.class ? `${s.class} → ${s.final_class}` : s.class)) : null,
      h('span', { class: 'stat' }, 'scanned ', h('b', {}, fmt.num(s.rows_scanned)), ' / matched ', h('b', {}, fmt.num(s.rows_matched))),
      s.segments_total !== undefined ? h('span', { class: 'stat' }, 'segments ', h('b', {}, `${fmt.num(s.segments_scanned)}/${fmt.num(s.segments_total)}`), ` (pruned ${fmt.num(s.segments_pruned)})`) : null,
      s.segment_bytes_read !== undefined ? h('span', { class: 'stat' }, 'read ', h('b', {}, fmt.bytes(s.segment_bytes_read))) : null,
      resp.fingerprint ? h('span', { class: 'stat muted mono', title: 'Normalized query fingerprint' }, `fp ${resp.fingerprint}`) : null,
      resp.query_id ? h('span', { class: 'stat muted mono' }, `id ${resp.query_id}`) : null,
    ];
    replace(status, parts);
  }

  function renderGrid() {
    clear(detail);
    detail.hidden = true;
    const page = st.pages[st.pageIndex];
    if (!page) return;
    if (page.rows.length === 0) {
      replace(results, empty('The query returned no rows.'));
    } else {
      const base = st.pages.slice(0, st.pageIndex).reduce((n, p) => n + p.rows.length, 0);
      const thead = h('thead', {}, h('tr', {}, h('th', { class: 'grid-rownum', scope: 'col' }, '#'), st.columns.map(c => h('th', { scope: 'col' }, c))));
      const tbody = h('tbody');
      page.rows.forEach((r, i) => {
        const tr = h('tr', {}, h('td', { class: 'grid-rownum' }, String(base + i + 1)));
        for (const c of st.columns) {
          const v = cellView(r[c]);
          const td = h('td', { class: ['grid-cell', v.cls], title: v.title || null, tabindex: 0 }, v.text);
          td.addEventListener('click', () => showDetail(c, r[c]));
          td.addEventListener('keydown', e => { if (e.key === 'Enter') showDetail(c, r[c]); });
          tr.append(td);
        }
        tbody.append(tr);
      });
      replace(results, h('div', { class: 'table-wrap' }, h('table', { class: 'data' }, thead, tbody)));
    }
    renderPager();
  }

  function renderPager() {
    const page = st.pages[st.pageIndex];
    pager.hidden = !st.paginate || !page;
    if (pager.hidden) return;
    const base = st.pages.slice(0, st.pageIndex).reduce((n, p) => n + p.rows.length, 0);
    const fetched = st.pages.reduce((n, p) => n + p.rows.length, 0);
    pageInfo.textContent = page.rows.length
      ? `Page ${st.pageIndex + 1} · rows ${fmt.num(base + 1)}–${fmt.num(base + page.rows.length)} · ${fmt.num(fetched)} fetched${st.pages[st.pages.length - 1].next ? '' : ' (all)'}`
      : `Page ${st.pageIndex + 1}`;
    prevBtn.disabled = st.running || st.pageIndex === 0;
    nextBtn.disabled = st.running || !(st.pageIndex < st.pages.length - 1 || st.pages[st.pages.length - 1].next);
  }

  function showDetail(col, v) {
    const text = v === undefined ? '(missing)' : JSON.stringify(v, null, 2);
    replace(detail,
      h('div', { class: 'inline-controls' },
        h('strong', {}, col),
        h('button', { class: 'btn small', type: 'button', onclick: () => navigator.clipboard && navigator.clipboard.writeText(typeof v === 'string' ? v : text).then(() => toast('Copied.'), () => toast('Copy failed', 'bad')) }, 'Copy'),
        h('button', { class: 'btn small ghost', type: 'button', onclick: () => { detail.hidden = true; } }, 'Close')),
      h('pre', {}, text));
    detail.hidden = false;
  }

  function showExplain(resp) {
    replace(results, h('pre', { class: 'explain', tabindex: 0, 'aria-label': 'Query plan' }, resp.explain));
    pager.hidden = true;
  }

  function showError(e) {
    const b = e.body || {};
    const refused = e.status === 429 || b.code === 'TOO_MANY_QUERIES' || b.code === 'SERVER_OVERLOADED';
    replace(status, badge(refused ? 'refused' : 'error', 'bad'), h('span', { class: 'stat' }, b.code || b.error || e.message));
    const box = errorBox(e, { onRetry: () => run(st.lastMode || 'run') });
    if (refused) {
      box.append(h('div', { class: 'emeta' }, b.code === 'SERVER_OVERLOADED'
        ? 'The server is under memory pressure and refuses non-interactive queries. Retry later or make the query cheaper.'
        : 'The server has no free query slot for this work class and its queue is full or the wait timed out.'));
    }
    replace(results, box);
  }

  function updateExportButtons() {
    const has = st.pages.some(p => p.rows.length);
    csvBtn.disabled = jsonBtn.disabled = !has;
  }

  async function send(cursor) {
    const id = randomId();
    const body = { query: st.sql, query_id: id };
    const t = Math.round(Number(timeout.value));
    if (t >= 1 && t <= 300) body.timeout_ms = t * 1000;
    if (klass.value) body.workload_class = klass.value;
    if (st.paginate && !st.explain) body.page_size = Number(pageSize.value);
    if (cursor) body.cursor = cursor;
    setRunning(true, id);
    const t0 = performance.now();
    try {
      const resp = await srv.query(body);
      return { resp, ms: performance.now() - t0 };
    } finally {
      if (!st.disposed) setRunning(false);
    }
  }

  async function run(mode) {
    if (st.running) return;
    let text = editor.runnable.trim().replace(/;\s*$/, '');
    if (!text) {
      toast('Nothing to run.');
      return;
    }
    if (mode !== 'run' && !/^explain\b/i.test(text)) text = `${mode === 'analyze' ? 'EXPLAIN ANALYZE' : 'EXPLAIN'} ${text}`;
    Object.assign(st, { sql: text, explain: /^explain\b/i.test(text), paginate: true, pages: [], pageIndex: 0, columns: [], lastMode: mode });
    pager.hidden = true;
    detail.hidden = true;
    updateExportButtons();
    replace(results, h('div', { class: 'state' }, h('span', { class: 'spinner', 'aria-hidden': 'true' }), 'Waiting for the server…'));
    let out;
    try {
      try {
        out = await send(null);
      } catch (e) {
        // Aggregates cannot be paginated: run once more without page_size.
        if (e.status === 400 && /cannot be paginated/i.test((e.body && e.body.message) || '')) {
          st.paginate = false;
          out = await send(null);
        } else {
          throw e;
        }
      }
    } catch (e) {
      if (st.disposed) return;
      pushHistory(conn, { sql: st.sql, at: Date.now(), ok: false, error: (e.body && (e.body.code || e.body.error)) || e.message });
      showError(e);
      return;
    }
    if (st.disposed) return;
    const { resp, ms } = out;
    pushHistory(conn, { sql: st.sql, at: Date.now(), ok: true, rows: (resp.rows || []).length, ms: resp.stats && resp.stats.elapsed_ms });
    st.columns = resp.columns || [];
    st.last = resp;
    if (resp.explain) {
      st.paginate = false;
      st.pages = [];
      showExplain(resp);
    } else {
      st.pages = [{ rows: resp.rows || [], next: resp.next_cursor || null }];
      st.pageIndex = 0;
      renderGrid();
    }
    renderStats(resp, ms);
    updateExportButtons();
  }

  async function nextPage() {
    if (st.running) return;
    if (st.pageIndex < st.pages.length - 1) {
      gotoPage(st.pageIndex + 1);
      return;
    }
    const last = st.pages[st.pages.length - 1];
    if (!last || !last.next) return;
    nextBtn.disabled = prevBtn.disabled = true;
    try {
      const { resp, ms } = await send(last.next);
      if (st.disposed) return;
      st.pages.push({ rows: resp.rows || [], next: resp.next_cursor || null });
      st.pageIndex = st.pages.length - 1;
      renderGrid();
      renderStats(resp, ms);
      updateExportButtons();
    } catch (e) {
      if (st.disposed) return;
      showError(e);
    }
  }

  function gotoPage(i) {
    if (i < 0 || i >= st.pages.length) return;
    st.pageIndex = i;
    renderGrid();
  }

  function exportRows(kind) {
    const rows = st.pages.flatMap(p => p.rows);
    if (!rows.length) return;
    const name = `strata-${conn}-${stamp()}`;
    if (kind === 'csv') download(`${name}.csv`, toCSV(st.columns, rows), 'text/csv;charset=utf-8');
    else download(`${name}.json`, toJSON(st.columns, rows), 'application/json');
    toast(`Exported ${fmt.num(rows.length)} fetched row${rows.length === 1 ? '' : 's'}.`);
  }

  function toggleHistory() {
    if (!history.hidden) {
      history.hidden = true;
      histBtn.setAttribute('aria-expanded', 'false');
      return;
    }
    const list = loadHistory(conn);
    const head = h('div', { class: 'history-head' },
      h('strong', {}, `History · ${conn}`),
      h('span', { class: 'inline-controls' },
        h('button', { class: 'btn small', type: 'button', disabled: !list.length, onclick: () => { storageSet(historyKey(conn), []); toggleHistory(); toast('History cleared.'); } }, 'Clear'),
        h('button', { class: 'btn small ghost', type: 'button', onclick: toggleHistory }, 'Close')));
    const ul = h('ul');
    for (const e of list) {
      ul.append(h('li', {}, h('button', {
        type: 'button',
        onclick: () => { editor.value = e.sql; toggleHistory(); editor.focus(); },
      },
      h('span', { class: 'hsql' }, e.sql),
      h('span', { class: 'hmeta' }, `${fmt.time(e.at)} · ${e.ok ? `${fmt.num(e.rows)} rows · ${fmt.ms(e.ms)}` : `failed: ${e.error}`}`))));
    }
    replace(history, head, list.length ? ul : h('div', { class: 'state' }, 'No queries yet for this server.'));
    history.hidden = false;
    histBtn.setAttribute('aria-expanded', 'true');
  }

  el.addEventListener('keydown', e => {
    if (e.key === 'Escape' && !history.hidden) toggleHistory();
  });

  return {
    el,
    focus: () => editor.focus(),
    setSql(text) { editor.value = text; },
    run: () => run('run'),
    dispose() {
      st.disposed = true;
      clearInterval(st.timer);
      clearInterval(st.poll);
      // Leaving a running query behind would hold a server slot: cancel it.
      if (st.queryId) srv.cancel(st.queryId).catch(() => {});
    },
  };
}
