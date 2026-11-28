// Admin panel: compaction, backup and retention. Every state-changing
// action asks for confirmation in an in-page modal first.

import { h, replace, fmt, errorBox, loading, kv, toast } from './dom.js';
import { server } from './api.js';
import { confirmDialog } from './modal.js';
import { stamp } from './export.js';

const DAY_MS = 86_400_000;

function card(title, ...body) {
  return h('section', { class: 'card section' }, h('h2', {}, title), ...body);
}

export function createAdmin(conn) {
  const srv = server(conn);
  const el = h('div', {}, h('div', { class: 'page-head' }, h('h1', {}, `Admin · ${conn}`)),
    h('p', { class: 'muted' }, 'These actions run on the server with the connection\'s token and are recorded in the server\'s audit log and in the studio\'s log.'));

  // Compaction
  const compOut = h('div');
  const compBtn = h('button', { class: 'btn primary', type: 'button' }, 'Run compaction…');
  compBtn.addEventListener('click', async () => {
    const ok = await confirmDialog({
      title: 'Run compaction?',
      message: `Compaction merges small segments on "${conn}" until nothing is left to merge. It uses disk I/O and CPU while it runs and may take a while on a large data set.`,
      confirmText: 'Run compaction',
    });
    if (!ok) return;
    compBtn.disabled = true;
    replace(compOut, loading('Compacting…'));
    try {
      const r = await srv.compaction();
      replace(compOut, kv([
        ['Groups merged', fmt.num(r.groups)],
        ['Input segments', fmt.num(r.inputs_merged)],
        ['Obsolete deleted', fmt.num(r.obsolete_deleted)],
        ['Segments now', fmt.num(r.segments_now)],
      ]));
      toast('Compaction finished.', 'ok');
    } catch (e) {
      replace(compOut, e.status === 403 ? h('div', { class: 'notice' }, 'The token lacks cluster.admin, which compaction needs.') : errorBox(e));
    } finally {
      compBtn.disabled = false;
    }
  });

  // Backup
  const backupName = h('input', { type: 'text', value: `studio-${stamp()}`, pattern: '[A-Za-z0-9_][A-Za-z0-9_\\-]{0,63}', maxlength: 64, 'aria-label': 'Backup name', spellcheck: 'false' });
  const backupOut = h('div');
  const backupBtn = h('button', { class: 'btn primary', type: 'button' }, 'Create backup…');
  backupBtn.addEventListener('click', async () => {
    const name = backupName.value.trim();
    if (!/^[A-Za-z0-9_][A-Za-z0-9_-]{0,63}$/.test(name)) {
      replace(backupOut, h('p', { class: 'form-error' }, 'Use 1–64 characters of A–Z, a–z, 0–9, _ and -, not starting with -.'));
      return;
    }
    const ok = await confirmDialog({
      title: 'Create a backup?',
      message: `The server flushes the memtable and copies a consistent snapshot to its backup directory as "${name}". Restore is offline only (strata backup restore).`,
      confirmText: 'Create backup',
    });
    if (!ok) return;
    backupBtn.disabled = true;
    replace(backupOut, loading('Backing up…'));
    try {
      const r = await srv.backup(name);
      replace(backupOut, kv([
        ['Name', r.name],
        ['Rows', fmt.num(r.rows)],
        ['Segments', fmt.num(r.segments)],
        ['Bytes', fmt.bytes(r.bytes)],
        ['Manifest version', fmt.num(r.manifest_version)],
        ['Flushed through seq', fmt.num(r.flushed_through_seq)],
        ['Elapsed', fmt.ms(r.elapsed_ms)],
      ]));
      backupName.value = `studio-${stamp()}`;
      toast('Backup created.', 'ok');
    } catch (e) {
      const b = e.body || {};
      if (e.status === 400 && /backup-dir|disabled/i.test(b.message || '')) {
        replace(backupOut, h('div', { class: 'notice' }, 'Backups are not configured on this server: start strata-server with --backup-dir to enable POST /admin/backup.'));
      } else if (e.status === 409) {
        replace(backupOut, h('div', { class: 'notice warn' }, `A backup named "${name}" already exists. Choose another name.`));
      } else if (e.status === 403) {
        replace(backupOut, h('div', { class: 'notice' }, 'The token lacks backup.create.'));
      } else {
        replace(backupOut, errorBox(e));
      }
    } finally {
      backupBtn.disabled = false;
    }
  });

  // Retention
  const retCurrent = h('div', {}, loading('Loading retention policy…'));
  const days = h('input', { type: 'number', min: 1, step: 1, value: 30, 'aria-label': 'Maximum age in days' });
  const hold = h('input', { type: 'checkbox', 'aria-label': 'Legal hold' });
  const retOut = h('div');
  const saveBtn = h('button', { class: 'btn primary', type: 'button' }, 'Set policy…');
  const clearBtn = h('button', { class: 'btn danger', type: 'button' }, 'Remove policy…');

  async function loadRetention() {
    try {
      const r = await srv.retention();
      const p = r.policy;
      replace(retCurrent, kv([
        ['Tenant', r.tenant],
        ['Policy', p ? `delete data older than ${fmt.num(p.max_age_ms / DAY_MS, 2)} day(s)` : 'none (data is kept until a policy is set)'],
        ['Hold', p ? (p.hold ? 'on: nothing is deleted while the hold is set' : 'off') : '–'],
      ]));
      if (p) {
        days.value = String(Math.max(1, Math.round(p.max_age_ms / DAY_MS)));
        hold.checked = Boolean(p.hold);
      }
      clearBtn.disabled = !p;
      saveBtn.disabled = false;
    } catch (e) {
      saveBtn.disabled = clearBtn.disabled = true;
      replace(retCurrent, e.status === 403 ? h('div', { class: 'notice' }, 'The token lacks retention.manage, so the policy cannot be read or changed.') : errorBox(e, { onRetry: loadRetention }));
    }
  }

  saveBtn.addEventListener('click', async () => {
    const n = Number(days.value);
    if (!Number.isInteger(n) || n < 1) {
      replace(retOut, h('p', { class: 'form-error' }, 'The maximum age must be a whole number of days (at least 1).'));
      return;
    }
    const ok = await confirmDialog({
      title: 'Set the retention policy?',
      message: hold.checked
        ? `The policy will keep ${n} day(s) of data, but with the hold on nothing is deleted until the hold is lifted.`
        : `The background retention task will permanently DELETE this tenant's events older than ${n} day(s) on "${conn}". This cannot be undone.`,
      confirmText: 'Set policy',
      danger: !hold.checked,
      typeToConfirm: hold.checked ? undefined : String(n),
    });
    if (!ok) return;
    saveBtn.disabled = true;
    try {
      await srv.setRetention(n, hold.checked);
      replace(retOut, h('p', { class: 'muted' }, 'Policy saved; the background retention task applies it.'));
      toast('Retention policy saved.', 'ok');
      await loadRetention();
    } catch (e) {
      replace(retOut, errorBox(e));
    } finally {
      saveBtn.disabled = false;
    }
  });

  clearBtn.addEventListener('click', async () => {
    const ok = await confirmDialog({
      title: 'Remove the retention policy?',
      message: `The tenant's data on "${conn}" will be kept indefinitely (or per the server's default retention).`,
      confirmText: 'Remove policy',
      danger: true,
    });
    if (!ok) return;
    clearBtn.disabled = true;
    try {
      await srv.clearRetention();
      toast('Retention policy removed.', 'ok');
      replace(retOut, '');
      await loadRetention();
    } catch (e) {
      replace(retOut, errorBox(e));
      clearBtn.disabled = false;
    }
  });

  el.append(h('div', { class: 'grid-2' },
    card('Compaction',
      h('p', { class: 'muted' }, 'Merge segments now instead of waiting for the background task (needs cluster.admin).'),
      compBtn, compOut),
    card('Backup',
      h('p', { class: 'muted' }, 'Write a named, consistent backup under the server\'s --backup-dir (needs backup.create).'),
      h('div', { class: 'inline-controls' }, h('label', {}, 'Name ', backupName), backupBtn), backupOut),
    card('Retention',
      h('p', { class: 'muted' }, 'The policy of the token\'s tenant (needs retention.manage).'),
      retCurrent,
      h('div', { class: 'section' }),
      h('div', { class: 'inline-controls' },
        h('label', {}, 'Max age (days) ', days),
        h('label', {}, hold, ' Hold'),
        saveBtn, clearBtn),
      retOut)));

  loadRetention();
  return { el };
}
