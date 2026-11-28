// Client for the studio's JSON API. State-changing requests carry the CSRF
// header; a studio 401 (session expired) raises an app-wide event.

export class ApiError extends Error {
  constructor(status, body) {
    super((body && body.message) || `HTTP ${status}`);
    this.name = (body && (body.code || body.error)) || 'error';
    this.status = status;
    this.body = body || {};
  }
}

const CSRF = { 'X-Strata-Studio': '1' };

export async function request(method, path, body, { signal } = {}) {
  const headers = { Accept: 'application/json' };
  const init = { method, headers, credentials: 'same-origin', signal, cache: 'no-store' };
  if (method !== 'GET' && method !== 'HEAD') {
    Object.assign(headers, CSRF);
    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
      init.body = JSON.stringify(body);
    }
  }
  let resp;
  try {
    resp = await fetch(path, init);
  } catch (e) {
    if (e && e.name === 'AbortError') throw e;
    throw new ApiError(0, { error: 'unavailable', message: 'Cannot reach the studio backend. Is strata-studio still running?', source: 'studio' });
  }
  let data = null;
  const text = await resp.text();
  if (text) {
    try { data = JSON.parse(text); } catch { data = { error: 'internal', message: text.slice(0, 500) }; }
  }
  if (!resp.ok) {
    if (resp.status === 401 && data && data.source === 'studio') {
      window.dispatchEvent(new CustomEvent('studio:unauthenticated'));
    }
    throw new ApiError(resp.status, data);
  }
  return data;
}

const enc = encodeURIComponent;

export const studio = {
  session: () => request('GET', '/api/session'),
  login: password => request('POST', '/api/auth/login', { password }),
  logout: () => request('POST', '/api/auth/logout'),
  connections: () => request('GET', '/api/connections'),
  addConnection: c => request('POST', '/api/connections', c),
  updateConnection: (name, patch) => request('PUT', `/api/connections/${enc(name)}`, patch),
  removeConnection: name => request('DELETE', `/api/connections/${enc(name)}`),
  testConnection: name => request('POST', `/api/connections/${enc(name)}/test`),
  testUnsaved: c => request('POST', '/api/connection-test', c),
};

export function server(name) {
  const base = `/api/servers/${enc(name)}`;
  return {
    status: () => request('GET', `${base}/status`),
    ready: () => request('GET', `${base}/ready`),
    version: () => request('GET', `${base}/version`),
    query: (body, opts) => request('POST', `${base}/query`, body, opts),
    queryStatus: id => request('GET', `${base}/query/${enc(id)}`),
    cancel: id => request('DELETE', `${base}/query/${enc(id)}`),
    metrics: () => request('GET', `${base}/metrics`),
    families: () => request('GET', `${base}/metrics/families`),
    admin: which => request('GET', `${base}/admin/${enc(which)}`),
    compaction: () => request('POST', `${base}/admin/compaction`),
    backup: backupName => request('POST', `${base}/admin/backup`, { name: backupName }),
    retention: () => request('GET', `${base}/retention`),
    setRetention: (days, hold) => request('PUT', `${base}/retention`, { max_age_days: days, hold }),
    clearRetention: () => request('DELETE', `${base}/retention`),
  };
}
