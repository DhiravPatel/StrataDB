// StrataStudio bootstrap: session/login, theme, object browser and tabs.

import { h, replace, toast, storageGet, storageSet } from './dom.js';
import { studio, server } from './api.js';
import { createTree } from './tree.js';
import { createServersPanel, connectionForm } from './connections.js';
import { createQueryTool } from './query.js';
import { createDashboard } from './dashboard.js';
import { createAdmin } from './admin.js';
import { createQueryLog } from './querylog.js';

const $ = id => document.getElementById(id);
const STATUS_REFRESH_MS = 30000;
const MAX_TABS = 30;

// ---------------------------------------------------------------------------
// Theme

function applyTheme(t) {
  if (t === 'light' || t === 'dark') document.documentElement.dataset.theme = t;
  else delete document.documentElement.dataset.theme;
  const dark = t === 'dark' || (t !== 'light' && window.matchMedia('(prefers-color-scheme: dark)').matches);
  $('theme-toggle').textContent = dark ? 'Light' : 'Dark';
  $('theme-toggle').setAttribute('aria-label', dark ? 'Switch to the light theme' : 'Switch to the dark theme');
}

function initTheme() {
  applyTheme(storageGet('strata-studio:theme', 'auto'));
  $('theme-toggle').addEventListener('click', () => {
    const cur = document.documentElement.dataset.theme
      || (window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
    const next = cur === 'dark' ? 'light' : 'dark';
    storageSet('strata-studio:theme', next);
    applyTheme(next);
  });
}

// ---------------------------------------------------------------------------
// Tabs

const tabs = new Map(); // id → {id, title, tab, panel, view}
let activeId = null;
let queryCounter = 0;

function activate(id) {
  const t = tabs.get(id);
  if (!t) return;
  activeId = id;
  for (const x of tabs.values()) {
    const on = x.id === id;
    x.tab.setAttribute('aria-selected', String(on));
    x.tab.tabIndex = on ? 0 : -1;
    x.panel.hidden = !on;
  }
  if (t.view && t.view.focus) t.view.focus();
}

function closeTab(id) {
  const t = tabs.get(id);
  if (!t) return;
  if (t.view && t.view.dispose) t.view.dispose();
  t.tab.remove();
  t.panel.remove();
  tabs.delete(id);
  if (activeId === id) {
    const last = [...tabs.keys()].pop();
    activeId = null;
    if (last) activate(last);
  }
}

function openTab(id, title, factory, { flush = false } = {}) {
  if (tabs.has(id)) {
    activate(id);
    return tabs.get(id).view;
  }
  if (tabs.size >= MAX_TABS) {
    // Bounded: close the oldest tab that is not the active one.
    const oldest = [...tabs.keys()].find(k => k !== activeId);
    if (oldest) closeTab(oldest);
  }
  const view = factory();
  const tabId = `tab-${id.replace(/[^A-Za-z0-9_-]/g, '_')}`;
  const panelId = `panel-${id.replace(/[^A-Za-z0-9_-]/g, '_')}`;
  const tab = h('div', { class: 'tab', role: 'tab', id: tabId, 'aria-controls': panelId, 'aria-selected': 'false', tabindex: -1, title },
    h('span', { class: 'tab-title' }, title),
    h('button', { class: 'tab-close', type: 'button', 'aria-label': `Close ${title}`, onclick: e => { e.stopPropagation(); closeTab(id); } }, '×'));
  tab.addEventListener('click', () => activate(id));
  tab.addEventListener('keydown', e => {
    const ids = [...tabs.keys()];
    const i = ids.indexOf(id);
    if (e.key === 'ArrowRight' && ids[i + 1]) { activate(ids[i + 1]); tabs.get(ids[i + 1]).tab.focus(); }
    if (e.key === 'ArrowLeft' && ids[i - 1]) { activate(ids[i - 1]); tabs.get(ids[i - 1]).tab.focus(); }
    if (e.key === 'Delete') closeTab(id);
  });
  const panel = h('div', { class: ['panel', flush ? 'flush' : ''], role: 'tabpanel', id: panelId, 'aria-labelledby': tabId, hidden: true }, view.el);
  $('tabs').append(tab);
  $('panels').append(panel);
  tabs.set(id, { id, title, tab, panel, view });
  activate(id);
  return view;
}

function closeTabsOf(conn) {
  for (const id of [...tabs.keys()]) if (id.split(':')[1] === conn) closeTab(id);
}

// ---------------------------------------------------------------------------
// App

let tree;
let serversPanel = null;
let statusTimer = null;
let connections = [];

const actions = {
  openServers() {
    const v = openTab('servers', 'Servers', () => {
      serversPanel = createServersPanel({ onChange: reloadConnections, openDashboard: actions.openDashboard });
      return { ...serversPanel, dispose() { serversPanel = null; } };
    });
    return v;
  },
  openDashboard(conn) {
    openTab(`dash:${conn}`, `${conn} · Dashboard`, () => createDashboard(conn));
  },
  openAdmin(conn) {
    openTab(`admin:${conn}`, `${conn} · Admin`, () => createAdmin(conn));
  },
  openQueryLog(conn) {
    openTab(`qlog:${conn}`, `${conn} · Query Log`, () => createQueryLog(conn, { openQuery: sql => actions.openQuery(conn, sql) }));
  },
  openQuery(conn, sql, key) {
    const id = key ? `query:${conn}:${key}` : `query:${conn}:${++queryCounter}`;
    const title = key ? `${conn} · ${key}` : `${conn} · Query ${queryCounter}`;
    const fresh = !tabs.has(id);
    const v = openTab(id, title, () => createQueryTool(conn, { sql: sql || '' }), { flush: true });
    if (!fresh && sql && key === undefined) v.setSql(sql);
  },
};

async function refreshStatuses() {
  clearTimeout(statusTimer);
  await Promise.all(connections.map(async c => {
    try {
      tree.setStatus(c.name, await server(c.name).status());
    } catch {
      tree.setStatus(c.name, { reachable: false });
    }
  }));
  statusTimer = setTimeout(refreshStatuses, STATUS_REFRESH_MS);
}

async function reloadConnections() {
  try {
    const r = await studio.connections();
    const before = new Set(connections.map(c => c.name));
    connections = r.connections;
    for (const name of before) if (!connections.some(c => c.name === name)) closeTabsOf(name);
    tree.setConnections(connections);
    refreshStatuses();
  } catch (e) {
    toast(`Could not load servers: ${e.message}`, 'bad');
  }
}

async function addServer() {
  const r = await connectionForm(null);
  if (r) {
    toast(`Added ${r.name}.`, 'ok');
    await reloadConnections();
    if (serversPanel) serversPanel.reload();
    tree.expandServer(r.name);
    actions.openDashboard(r.name);
  }
}

function initLayout() {
  const layout = $('app');
  const toggle = $('nav-toggle');
  const narrow = window.matchMedia('(max-width: 820px)');
  const set = open => {
    layout.classList.toggle('nav-collapsed', !open);
    toggle.setAttribute('aria-expanded', String(open));
  };
  set(!narrow.matches);
  toggle.addEventListener('click', () => set(layout.classList.contains('nav-collapsed')));
  narrow.addEventListener('change', e => set(!e.matches));
  $('add-server').addEventListener('click', addServer);
}

async function startApp(session) {
  $('login').hidden = true;
  $('boot').hidden = true;
  $('app').hidden = false;
  $('logout').hidden = !session.auth_required;
  tree = createTree($('tree'), actions);
  initLayout();
  await reloadConnections();
  actions.openServers();
  if (connections.length === 1) {
    tree.expandServer(connections[0].name);
    actions.openDashboard(connections[0].name);
  }
}

function showLogin() {
  $('boot').hidden = true;
  $('app').hidden = true;
  $('login').hidden = false;
  const pw = $('login-password');
  pw.value = '';
  pw.focus();
}

function initLogin() {
  $('login-form').addEventListener('submit', async e => {
    e.preventDefault();
    const err = $('login-error');
    err.hidden = true;
    try {
      await studio.login($('login-password').value);
      window.location.reload();
    } catch (ex) {
      err.textContent = ex.status === 429 ? 'Too many failed attempts. Wait a minute and retry.' : (ex.message || 'Sign-in failed.');
      err.hidden = false;
    }
  });
  $('logout').addEventListener('click', async () => {
    try { await studio.logout(); } catch { /* the session may already be gone */ }
    window.location.reload();
  });
  window.addEventListener('studio:unauthenticated', () => showLogin());
}

async function boot() {
  initTheme();
  initLogin();
  let session;
  try {
    session = await studio.session();
  } catch (e) {
    replace($('boot'), h('div', { class: 'error-box' }, `Cannot reach the studio backend: ${e.message}`));
    return;
  }
  $('studio-version').textContent = `v${session.version}`;
  if (session.auth_required && !session.authenticated) showLogin();
  else await startApp(session);
}

boot();
