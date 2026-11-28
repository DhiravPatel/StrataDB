// Server (connection) management: add, edit, test, remove. Tokens are
// write-only: the studio never sends a saved token back to the browser.

import { h, replace, fmt, errorBox, loading, table, badge, kv, toast } from './dom.js';
import { studio } from './api.js';
import { confirmDialog, formDialog, infoDialog } from './modal.js';

function testReport(r) {
  const perms = r.permissions || {};
  const yn = v => (v === true ? badge('yes', 'ok') : v === false ? badge('no', 'warn') : badge('unknown'));
  const v = r.version || {};
  return h('div', {},
    h('p', {}, r.ok ? badge('connection OK', 'ok') : badge('connection failed', 'bad'), ' ', h('span', { class: 'muted' }, `${r.url} · ${fmt.ms(r.elapsed_ms)}`)),
    r.error ? h('p', { class: 'form-error' }, r.error) : null,
    kv([
      ['Reachable', r.reachable ? 'yes' : 'no'],
      ['Token accepted', r.token_valid === true ? 'yes' : r.token_valid === false ? 'no: the server rejected it (401)' : 'unknown'],
      ['Ready', r.ready_status === 200 ? `yes${r.ready && r.ready.state ? ` (${r.ready.state})` : ''}` : (r.ready_status ? `no (HTTP ${r.ready_status}${r.ready && r.ready.state ? `, ${r.ready.state}` : ''})` : '–')],
      ['Server version', v.server ? `${v.server} (API ${v.api})` : '–'],
    ]),
    h('h4', {}, 'Token permissions (probed without side effects)'),
    kv([
      ['query', yn(perms.query)],
      ['cluster.read (dashboard, metrics)', yn(perms['cluster.read'])],
      ['retention.manage', yn(perms['retention.manage'])],
    ]),
    h('p', { class: 'muted small' }, 'cluster.admin (compaction) and backup.create cannot be probed without acting; the Admin panel reports them when used.'));
}

/** Open the add/edit form. existing: a connection view (edit) or null (add). Resolves to the saved view or null. */
export function connectionForm(existing) {
  const editing = Boolean(existing);
  const toBody = v => {
    const b = { url: v.url.trim(), tls_verify: v.tls_verify, ca_file: v.ca_file.trim() };
    if (!editing) b.name = v.name.trim();
    if (v.token) b.token = v.token;
    return b;
  };
  const fields = [
    editing ? null : { name: 'name', label: 'Name', required: true, placeholder: 'local', help: 'Letters, digits, _ and -; up to 64 characters.' },
    { name: 'url', label: 'Server URL', type: 'url', required: true, value: existing ? existing.url : 'http://127.0.0.1:7070', placeholder: 'https://db.example:7070', help: 'scheme://host:port of strata-server (the studio adds /api/v1).' },
    {
      name: 'token', label: editing ? 'API token (leave empty to keep the saved one)' : 'API token', type: 'password', required: !editing,
      autocomplete: 'new-password', placeholder: editing ? '••••••• (saved)' : 'sdb_…',
      help: 'Stored only in the studio\'s registry (mode 0600) and never shown again. Create one with strata-server token create.',
    },
    { name: 'tls_verify', label: 'Verify the server\'s TLS certificate', type: 'checkbox', value: existing ? existing.tls_verify : true, help: 'Turn off only for a test server with a self-signed certificate.' },
    { name: 'ca_file', label: 'CA certificate file (optional)', value: existing && existing.ca_file ? existing.ca_file : '', placeholder: '/etc/strata/ca.pem', help: 'Absolute path, on the studio host, of a PEM file to trust instead of the public roots.' },
  ].filter(Boolean);
  return formDialog({
    title: editing ? `Edit server “${existing.name}”` : 'Add a StrataDB server',
    fields,
    submitText: editing ? 'Save' : 'Add server',
    extra: [{
      text: 'Test',
      onClick: async (v, setStatus) => {
        setStatus(loading('Testing…'));
        let r;
        if (editing && !v.token) {
          // The saved token is used; unsaved URL/TLS edits are not tested.
          r = await studio.testConnection(existing.name);
          setStatus(h('div', {}, h('p', { class: 'muted small' }, 'Tested the saved settings (enter a token to test new ones).'), testReport(r)));
        } else {
          if (!v.token) throw new Error('Enter the API token to test.');
          const b = toBody(v);
          b.name = editing ? existing.name : (b.name || 'unsaved');
          r = await studio.testUnsaved(b);
          setStatus(testReport(r));
        }
      },
    }],
    onSubmit: async v => {
      const b = toBody(v);
      return editing ? studio.updateConnection(existing.name, b) : studio.addConnection(b);
    },
  });
}

export async function testAndReport(name) {
  try {
    const r = await studio.testConnection(name);
    await infoDialog({ title: `Test “${name}”`, details: testReport(r) });
  } catch (e) {
    await infoDialog({ title: `Test “${name}”`, details: errorBox(e) });
  }
}

export async function removeConnection(name) {
  const ok = await confirmDialog({
    title: `Remove server “${name}”?`,
    message: 'The connection and its saved token are deleted from the studio\'s registry. Nothing changes on the server. Query history in this browser is kept.',
    confirmText: 'Remove',
    danger: true,
  });
  if (!ok) return false;
  try {
    await studio.removeConnection(name);
    toast(`Removed ${name}.`, 'ok');
    return true;
  } catch (e) {
    toast(`Remove failed: ${e.message}`, 'bad');
    return false;
  }
}

/** The Servers manager panel. onChange() is called after any change. */
export function createServersPanel({ onChange, openDashboard }) {
  const list = h('div', {}, loading());
  const addBtn = h('button', { class: 'btn primary', type: 'button' }, '+ Add server');
  const el = h('div', {},
    h('div', { class: 'page-head' }, h('h1', {}, 'Servers'), h('span', { class: 'grow' }), addBtn),
    h('p', { class: 'muted' }, 'StrataDB servers this studio connects to. The browser talks only to the studio; the studio calls each server\'s HTTP API with the saved token.'),
    list);

  async function load() {
    replace(list, loading());
    try {
      const { connections } = await studio.connections();
      if (!connections.length) {
        replace(list, h('div', { class: 'state' },
          h('p', {}, 'No servers yet.'),
          h('p', { class: 'muted' }, 'Add a strata-server with an API token (for example one with query and cluster.read).'),
          h('button', { class: 'btn primary', type: 'button', onclick: add }, '+ Add server')));
        return;
      }
      replace(list, table([
        { key: 'name', label: 'Name', render: c => h('strong', {}, c.name) },
        { key: 'url', label: 'URL', render: c => h('code', {}, c.url) },
        { key: 'tls', label: 'TLS', render: c => c.url.startsWith('https:') ? (c.tls_verify ? badge('verified', 'ok') : badge('not verified', 'warn')) : badge('plain HTTP') },
        { key: 'ca_file', label: 'CA file', render: c => c.ca_file || '–' },
        { key: 'token_set', label: 'Token', render: c => (c.token_set ? badge('saved', 'ok') : badge('missing', 'bad')) },
        {
          key: 'actions', label: 'Actions', render: c => h('span', { class: 'inline-controls' },
            h('button', { class: 'btn small', type: 'button', onclick: () => openDashboard(c.name) }, 'Dashboard'),
            h('button', { class: 'btn small', type: 'button', onclick: () => testAndReport(c.name) }, 'Test'),
            h('button', { class: 'btn small', type: 'button', onclick: () => edit(c) }, 'Edit'),
            h('button', { class: 'btn small danger', type: 'button', onclick: async () => { if (await removeConnection(c.name)) { await load(); onChange(); } } }, 'Remove')),
        },
      ], connections));
    } catch (e) {
      replace(list, errorBox(e, { onRetry: load }));
    }
  }

  async function add() {
    const r = await connectionForm(null);
    if (r) { toast(`Added ${r.name}.`, 'ok'); await load(); onChange(); }
  }

  async function edit(c) {
    const r = await connectionForm(c);
    if (r) { toast(`Saved ${c.name}.`, 'ok'); await load(); onChange(); }
  }

  addBtn.addEventListener('click', add);
  load();
  return { el, reload: load, add };
}
