// In-page modal dialogs on the single <dialog id="modal"> element
// (never window.confirm). One dialog is open at a time.

import { h, replace } from './dom.js';

function dialogEl() {
  return document.getElementById('modal');
}

let busy = Promise.resolve();

/** Serialize dialogs: a second request waits for the first to close. */
function exclusive(fn) {
  const run = busy.then(fn, fn);
  busy = run.catch(() => {});
  return run;
}

function open(content, onCancel) {
  const d = dialogEl();
  const returnFocus = document.activeElement;
  replace(d, content);
  const cancel = e => {
    e.preventDefault();
    onCancel();
  };
  // Closed by other code (e.g. a button that navigates away): treat as cancel.
  const closed = () => onCancel();
  d.addEventListener('cancel', cancel);
  d.addEventListener('close', closed);
  if (!d.open) d.showModal();
  const first = d.querySelector('[autofocus], input, select, textarea, button.primary, button.danger, button');
  if (first) first.focus();
  return () => {
    d.removeEventListener('cancel', cancel);
    d.removeEventListener('close', closed);
    if (d.open) d.close();
    if (returnFocus && returnFocus.isConnected && typeof returnFocus.focus === 'function') returnFocus.focus();
  };
}

/**
 * Ask for confirmation. Resolves true (confirmed) or false.
 * danger: style the confirm button as destructive.
 * typeToConfirm: the user must type this text to enable the button.
 */
export function confirmDialog({ title, message, details, confirmText = 'Confirm', danger = false, typeToConfirm }) {
  return exclusive(() => new Promise(resolve => {
    let close;
    const done = v => { close(); resolve(v); };
    const okBtn = h('button', { class: ['btn', danger ? 'danger' : 'primary'], type: 'submit' }, confirmText);
    let typed = null;
    if (typeToConfirm) {
      okBtn.disabled = true;
      typed = h('input', {
        type: 'text', autocomplete: 'off', 'aria-label': `Type ${typeToConfirm} to confirm`,
        oninput: () => { okBtn.disabled = typed.value !== typeToConfirm; },
      });
    }
    const form = h('form', { method: 'dialog' },
      h('h2', {}, title),
      h('p', { class: 'msg' }, message),
      details ? h('pre', {}, details) : null,
      typed ? h('div', { class: 'field' }, h('label', {}, `Type `, h('code', {}, typeToConfirm), ` to confirm`), typed) : null,
      h('div', { class: 'actions' },
        h('button', { class: 'btn', type: 'button', onclick: () => done(false) }, 'Cancel'),
        okBtn));
    form.addEventListener('submit', e => { e.preventDefault(); if (!okBtn.disabled) done(true); });
    close = open(form, () => done(false));
  }));
}

/** Show a message with a single OK button. */
export function infoDialog({ title, message, details }) {
  return exclusive(() => new Promise(resolve => {
    let close;
    const done = () => { close(); resolve(); };
    const form = h('form', { method: 'dialog' },
      h('h2', {}, title),
      message ? h('p', { class: 'msg' }, message) : null,
      details ? (details instanceof Node ? details : h('pre', {}, details)) : null,
      h('div', { class: 'actions' }, h('button', { class: 'btn primary', type: 'submit' }, 'OK')));
    form.addEventListener('submit', e => { e.preventDefault(); done(); });
    close = open(form, done);
  }));
}

/**
 * A form dialog. fields: [{name, label, type: text|password|url|number|checkbox|select,
 * value, required, placeholder, help, options: [[value, label]], autocomplete}].
 * onSubmit(values) may throw (its message is shown) or return a result to resolve with.
 * extra: [{text, onClick(values, setStatus)}] additional buttons (e.g. Test).
 */
export function formDialog({ title, intro, fields, submitText = 'Save', onSubmit, extra = [] }) {
  return exclusive(() => new Promise(resolve => {
    let close;
    const done = v => { close(); resolve(v); };
    const inputs = {};
    const status = h('div', { class: 'form-status', role: 'status' });
    const setStatus = node => replace(status, node || '');
    const rows = fields.map(f => {
      const id = `f-${f.name}-${Math.random().toString(36).slice(2, 8)}`;
      let input;
      if (f.type === 'select') {
        input = h('select', { id, name: f.name }, (f.options || []).map(([v, l]) => h('option', { value: v }, l)));
        if (f.value !== undefined) input.value = f.value;
      } else if (f.type === 'checkbox') {
        input = h('input', { id, name: f.name, type: 'checkbox', checked: Boolean(f.value) });
      } else {
        input = h('input', {
          id, name: f.name, type: f.type || 'text', value: f.value ?? '', placeholder: f.placeholder,
          required: f.required, autocomplete: f.autocomplete || 'off', spellcheck: 'false',
          min: f.min, max: f.max, step: f.step,
        });
      }
      inputs[f.name] = { f, input };
      const helpId = f.help ? `${id}-help` : null;
      if (helpId) input.setAttribute('aria-describedby', helpId);
      if (f.type === 'checkbox') {
        return h('div', { class: 'field inline' }, input, h('label', { for: id }, f.label),
          f.help ? h('span', { class: 'help', id: helpId }, f.help) : null);
      }
      return h('div', { class: 'field' }, h('label', { for: id }, f.label), input,
        f.help ? h('span', { class: 'help', id: helpId }, f.help) : null);
    });
    const values = () => {
      const out = {};
      for (const [k, { f, input }] of Object.entries(inputs)) {
        if (f.type === 'checkbox') out[k] = input.checked;
        else if (f.type === 'number') out[k] = input.value === '' ? null : Number(input.value);
        else out[k] = input.value;
      }
      return out;
    };
    const err = h('p', { class: 'form-error', role: 'alert', hidden: true });
    const submit = h('button', { class: 'btn primary', type: 'submit' }, submitText);
    const extraBtns = extra.map(x => h('button', {
      class: 'btn', type: 'button',
      onclick: async ev => {
        const b = ev.currentTarget;
        b.disabled = true;
        err.hidden = true;
        try { await x.onClick(values(), setStatus); } catch (e) { err.textContent = e.message; err.hidden = false; }
        b.disabled = false;
      },
    }, x.text));
    const form = h('form', { method: 'dialog', novalidate: true },
      h('h2', {}, title),
      intro ? h('p', { class: 'muted' }, intro) : null,
      rows, status, err,
      h('div', { class: 'actions' }, extraBtns, h('span', { class: 'grow' }),
        h('button', { class: 'btn', type: 'button', onclick: () => done(null) }, 'Cancel'),
        submit));
    form.addEventListener('submit', async e => {
      e.preventDefault();
      if (!form.reportValidity()) return;
      submit.disabled = true;
      err.hidden = true;
      try {
        const r = await onSubmit(values(), setStatus);
        done(r === undefined ? true : r);
      } catch (ex) {
        err.textContent = ex.message || String(ex);
        err.hidden = false;
        submit.disabled = false;
      }
    });
    close = open(form, () => done(null));
  }));
}
