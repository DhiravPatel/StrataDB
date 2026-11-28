// DOM helpers. Everything is built with createElement/textContent (never
// by parsing HTML strings), so server values can never inject markup, and
// no inline style attributes are used (the CSP forbids them).

const SVG_NS = 'http://www.w3.org/2000/svg';

/** h('div', {class: 'x', onclick: fn, 'aria-label': 'y'}, child, ...) */
export function h(tag, attrs, ...children) {
  const el = document.createElement(tag);
  applyAttrs(el, attrs);
  append(el, children);
  return el;
}

function applyAttrs(el, attrs) {
  if (!attrs) return;
  for (const [k, v] of Object.entries(attrs)) {
    if (v === undefined || v === null || v === false) continue;
    if (k === 'class') el.className = Array.isArray(v) ? v.filter(Boolean).join(' ') : v;
    else if (k === 'text') el.textContent = String(v);
    else if (k === 'dataset') Object.assign(el.dataset, v);
    else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
    else if (k === 'value') el.value = v;
    else if (k === 'checked') el.checked = Boolean(v);
    else if (k === 'style') throw new Error('inline styles are not allowed (CSP)');
    else if (v === true) el.setAttribute(k, '');
    else el.setAttribute(k, String(v));
  }
}

export function append(el, children) {
  for (const c of children.flat(Infinity)) {
    if (c === null || c === undefined || c === false) continue;
    el.append(c instanceof Node ? c : document.createTextNode(String(c)));
  }
  return el;
}

export function clear(el) {
  while (el.firstChild) el.removeChild(el.firstChild);
  return el;
}

export function replace(el, ...children) {
  clear(el);
  return append(el, children);
}

export function svg(tag, attrs, ...children) {
  const el = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v !== undefined && v !== null) el.setAttribute(k, String(v));
  }
  for (const c of children) if (c) el.append(c);
  return el;
}

/** A small sparkline of the last values (null gaps are skipped). */
export function sparkline(values, label) {
  const w = 120, ht = 30;
  const pts = values.map((v, i) => [i, v]).filter(([, v]) => typeof v === 'number' && isFinite(v));
  const el = svg('svg', { class: 'spark', viewBox: `0 0 ${w} ${ht}`, preserveAspectRatio: 'none', role: 'img', 'aria-label': label || 'trend' });
  if (pts.length < 2) return el;
  const max = Math.max(...pts.map(p => p[1]), 1e-9);
  const n = Math.max(values.length - 1, 1);
  const xy = pts.map(([i, v]) => [(i / n) * w, ht - 2 - (v / max) * (ht - 4)]);
  const line = xy.map(([x, y]) => `${x.toFixed(1)},${y.toFixed(1)}`).join(' ');
  const area = `${xy[0][0].toFixed(1)},${ht} ${line} ${xy[xy.length - 1][0].toFixed(1)},${ht}`;
  el.append(svg('polygon', { class: 'area', points: area }));
  el.append(svg('polyline', { points: line }));
  return el;
}

// ---------------------------------------------------------------------------
// Formatting

export const fmt = {
  num(n, digits = 0) {
    if (n === null || n === undefined || !isFinite(n)) return '–';
    return Number(n).toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: 0 });
  },
  rate(n) {
    if (n === null || n === undefined || !isFinite(n)) return '–';
    if (n === 0) return '0';
    if (n < 1) return Number(n).toFixed(2);
    if (n < 100) return Number(n).toFixed(1);
    return fmt.num(n);
  },
  bytes(n) {
    if (n === null || n === undefined || !isFinite(n)) return '–';
    const u = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
    let i = 0, v = Number(n);
    while (Math.abs(v) >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return `${i === 0 ? v.toFixed(0) : v.toFixed(v < 10 ? 2 : 1)} ${u[i]}`;
  },
  ms(n) {
    if (n === null || n === undefined || !isFinite(n)) return '–';
    if (n < 1) return `${(n * 1000).toFixed(0)} µs`;
    if (n < 1000) return `${n.toFixed(n < 10 ? 2 : 1)} ms`;
    return `${(n / 1000).toFixed(2)} s`;
  },
  us(n) {
    if (n === null || n === undefined || !isFinite(n)) return '–';
    return fmt.ms(n / 1000);
  },
  pct(x) {
    if (x === null || x === undefined || !isFinite(x)) return '–';
    return `${(x * 100).toFixed(1)}%`;
  },
  age(ms) {
    if (ms === null || ms === undefined || !isFinite(ms)) return '–';
    const s = ms / 1000;
    if (s < 60) return `${s.toFixed(1)} s`;
    if (s < 3600) return `${(s / 60).toFixed(1)} min`;
    if (s < 86400) return `${(s / 3600).toFixed(1)} h`;
    return `${(s / 86400).toFixed(1)} d`;
  },
  time(v) {
    if (!v) return '–';
    const d = new Date(v);
    return isNaN(d) ? String(v) : d.toLocaleString();
  },
};

// ---------------------------------------------------------------------------
// States

export function loading(text = 'Loading…') {
  return h('div', { class: 'state', role: 'status' }, h('span', { class: 'spinner', 'aria-hidden': 'true' }), text);
}

export function empty(text, ...extra) {
  return h('div', { class: 'state' }, h('div', { text }), ...extra);
}

/** An error box for an ApiError (or any Error). */
export function errorBox(err, { onRetry } = {}) {
  const b = (err && err.body) || {};
  const kind = b.code || b.error || (err && err.name) || 'error';
  const meta = [];
  if (b.upstream_status) meta.push(`HTTP ${b.upstream_status} from server`);
  else if (err && err.status) meta.push(`HTTP ${err.status}`);
  if (b.source) meta.push(b.source === 'studio' ? 'raised by the studio' : 'raised by StrataDB');
  if (b.query_id) meta.push(`query ${b.query_id}`);
  if (b.request_id) meta.push(`request ${b.request_id}`);
  const box = h('div', { class: 'error-box', role: 'alert' },
    h('div', { class: 'ekind' }, kind),
    h('div', { class: 'emsg' }, (b.message || (err && err.message) || 'Unknown error')),
    meta.length ? h('div', { class: 'emeta' }, meta.join(' · ')) : null);
  if (typeof b.retry_after_ms === 'number') {
    const hint = h('div', { class: 'emeta' });
    const retryBtn = onRetry ? h('button', { class: 'btn small', type: 'button', disabled: true, onclick: onRetry }, 'Retry') : null;
    box.append(hint);
    if (retryBtn) box.append(h('div', {}, retryBtn));
    const until = Date.now() + b.retry_after_ms;
    const started = Date.now();
    const tick = () => {
      const left = until - Date.now();
      // Stop once the box has been removed from the page.
      if (!hint.isConnected && Date.now() - started > 200) return;
      if (left <= 0) {
        hint.textContent = 'The server suggests retrying now.';
        if (retryBtn) retryBtn.disabled = false;
        return;
      }
      hint.textContent = `The server asks to retry after ${(left / 1000).toFixed(1)} s.`;
      setTimeout(tick, 100);
    };
    tick();
  } else if (onRetry && b.retryable) {
    box.append(h('div', {}, h('button', { class: 'btn small', type: 'button', onclick: onRetry }, 'Retry')));
  }
  return box;
}

export function toast(text, kind = '') {
  const host = document.getElementById('toasts');
  if (!host) return;
  const t = h('div', { class: ['toast', kind] }, text);
  host.append(t);
  setTimeout(() => t.remove(), kind === 'bad' ? 7000 : 3500);
}

export function badge(text, kind = '') {
  return h('span', { class: ['badge', kind] }, text);
}

export function kv(pairs) {
  const dl = h('dl', { class: 'kv' });
  for (const [k, v] of pairs) {
    if (v === undefined) continue;
    dl.append(h('dt', {}, k), h('dd', {}, v === null ? '–' : v));
  }
  return dl;
}

/** A data table: cols = [{key, label, num, render(row)}]. */
export function table(cols, rows, { onRowClick, emptyText = 'No rows.' } = {}) {
  if (!rows || rows.length === 0) return empty(emptyText);
  const thead = h('thead', {}, h('tr', {}, cols.map(c => h('th', { class: c.num ? 'num' : '', scope: 'col' }, c.label))));
  const tbody = h('tbody');
  for (const r of rows) {
    const tr = h('tr', { class: onRowClick ? 'clickable' : '' });
    if (onRowClick) {
      tr.tabIndex = 0;
      tr.addEventListener('click', () => onRowClick(r));
      tr.addEventListener('keydown', e => { if (e.key === 'Enter') onRowClick(r); });
    }
    for (const c of cols) {
      const v = c.render ? c.render(r) : r[c.key];
      tr.append(h('td', { class: c.num ? 'num' : '' }, v === undefined || v === null ? '–' : v));
    }
    tbody.append(tr);
  }
  return h('div', { class: 'table-wrap' }, h('table', { class: 'data' }, thead, tbody));
}

/** Escape a string for a StrataQL single-quoted literal. */
export function sqlString(s) {
  return `'${String(s).replace(/'/g, "''")}'`;
}

/** Escape LIKE metacharacters (backslash is the escape character). */
export function likeEscape(s) {
  return String(s).replace(/[\\%_]/g, m => `\\${m}`);
}

export function randomId(prefix = 'ss') {
  const b = new Uint8Array(8);
  crypto.getRandomValues(b);
  return prefix + Array.from(b, x => x.toString(16).padStart(2, '0')).join('');
}

export function storageGet(key, fallback) {
  try {
    const v = localStorage.getItem(key);
    return v === null ? fallback : JSON.parse(v);
  } catch {
    return fallback;
  }
}

export function storageSet(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // Storage full or disabled: history is a convenience only.
  }
}
