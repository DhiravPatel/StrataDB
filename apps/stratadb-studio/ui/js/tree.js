// The object browser: servers → Dashboard, Query Tool, Tables (with
// columns), Query Log, Admin. Disclosure buttons with aria-expanded.

import { h, replace, storageGet, storageSet } from './dom.js';
import { TABLES } from './schema.js';

const OPEN_KEY = 'strata-studio:tree-open';

export function createTree(host, actions) {
  let openSet = new Set(storageGet(OPEN_KEY, ['servers']));
  let connections = [];
  const statuses = new Map();

  const save = () => storageSet(OPEN_KEY, [...openSet].slice(0, 500));

  function row({ label, icon, onClick, toggleKey, title, extra, cls, ariaLabel }) {
    const expandable = Boolean(toggleKey);
    const expanded = expandable && openSet.has(toggleKey);
    const btn = h('button', {
      class: ['tree-row', cls, !onClick && !expandable ? 'static' : ''],
      type: 'button',
      title: title || null,
      'aria-label': ariaLabel || null,
      'aria-expanded': expandable ? String(expanded) : null,
      tabindex: !onClick && !expandable ? -1 : null,
    },
    h('span', { class: 'twisty', 'aria-hidden': 'true' }, expandable ? (expanded ? '▾' : '▸') : ''),
    icon ? h('span', { class: 'ico', 'aria-hidden': 'true' }, icon) : null,
    h('span', { class: 'label' }, label),
    extra || null);
    btn.addEventListener('click', () => {
      if (expandable) {
        if (openSet.has(toggleKey)) openSet.delete(toggleKey); else openSet.add(toggleKey);
        save();
        render();
      }
      if (onClick) onClick();
    });
    return btn;
  }

  function node(opts, childrenFn) {
    const li = h('li', {}, row(opts));
    if (opts.toggleKey && openSet.has(opts.toggleKey) && childrenFn) {
      li.append(h('ul', { role: 'group' }, childrenFn()));
    }
    return li;
  }

  function dotFor(name) {
    const s = statuses.get(name);
    let kind = '', label = 'status unknown';
    if (s) {
      if (!s.reachable) { kind = 'bad'; label = 'unreachable'; }
      else if (s.ready && s.state !== 'DEGRADED') { kind = 'ok'; label = `ready${s.state ? ` (${s.state})` : ''}`; }
      else { kind = 'warn'; label = s.state ? `not ready (${s.state})` : 'not ready'; }
    }
    return h('span', { class: ['dot', kind], role: 'img', 'aria-label': label, title: label });
  }

  function tableNode(conn, t) {
    const key = `t:${conn}:${t.name}`;
    return node({ label: t.name, icon: '▦', toggleKey: key, title: t.doc, onClick: () => actions.openQuery(conn, t.sample, t.name) }, () => [
      node({ label: `Columns (${t.columns.length})`, icon: '≡', toggleKey: `${key}:cols` }, () => t.columns.map(c =>
        h('li', {}, row({ label: c.name, icon: '·', title: c.doc, extra: h('span', { class: 'type' }, c.type) })))),
      t.attributes ? node({ label: `Attributes (${t.attributes.length})`, icon: '≡', toggleKey: `${key}:attrs` }, () => t.attributes.map(([n, ty]) =>
        h('li', {}, row({ label: `attributes['${n}']`, icon: '·', extra: h('span', { class: 'type' }, ty) })))) : null,
    ]);
  }

  function serverNode(c) {
    const key = `s:${c.name}`;
    return node({ label: c.name, icon: null, toggleKey: key, title: c.url, ariaLabel: `Server ${c.name}`, extra: dotFor(c.name), cls: 'server' }, () => [
      h('li', {}, row({ label: 'Dashboard', icon: '◔', onClick: () => actions.openDashboard(c.name) })),
      h('li', {}, row({ label: 'Query Tool', icon: '⌨', onClick: () => actions.openQuery(c.name) })),
      node({ label: 'Tables', icon: '▤', toggleKey: `${key}:tables` }, () => TABLES.map(t => tableNode(c.name, t))),
      h('li', {}, row({ label: 'Query Log', icon: '☰', onClick: () => actions.openQueryLog(c.name) })),
      h('li', {}, row({ label: 'Admin', icon: '⚙', onClick: () => actions.openAdmin(c.name) })),
    ]);
  }

  function render() {
    const focusedLabel = document.activeElement && host.contains(document.activeElement)
      ? document.activeElement.textContent : null;
    const root = h('ul', {},
      node({ label: `Servers (${connections.length})`, icon: '🗄', toggleKey: 'servers', onClick: actions.openServers }, () =>
        connections.length
          ? connections.map(serverNode)
          : [h('li', { class: 'tree-empty' }, 'No servers. Use “+ Server”.')]));
    replace(host, root);
    if (focusedLabel) {
      const again = [...host.querySelectorAll('.tree-row')].find(b => b.textContent === focusedLabel);
      if (again) again.focus();
    }
  }

  return {
    setConnections(list) {
      connections = list;
      for (const name of [...statuses.keys()]) if (!list.some(c => c.name === name)) statuses.delete(name);
      render();
    },
    setStatus(name, s) {
      statuses.set(name, s);
      render();
    },
    expandServer(name) {
      openSet.add('servers');
      openSet.add(`s:${name}`);
      save();
      render();
    },
  };
}
