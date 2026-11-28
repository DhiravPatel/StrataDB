// A small SQL editor: a transparent <textarea> over a highlighted <pre>.
// Tab / Shift+Tab indent and outdent, Ctrl/Cmd+Enter runs.

import { h, clear } from './dom.js';
import { KEYWORDS, FUNCTIONS } from './schema.js';

const KW = new Set(KEYWORDS);
const FN = new Set(FUNCTIONS);
const INDENT = '  ';

/** Split SQL into [class, text] tokens (class '' = plain). */
export function tokenize(src) {
  const out = [];
  let i = 0;
  const n = src.length;
  const push = (cls, s) => { if (s) out.push([cls, s]); };
  while (i < n) {
    const c = src[i];
    if (c === '-' && src[i + 1] === '-') {
      let j = src.indexOf('\n', i);
      if (j < 0) j = n;
      push('tok-com', src.slice(i, j));
      i = j;
    } else if (c === "'") {
      let j = i + 1;
      while (j < n) {
        if (src[j] === "'" && src[j + 1] === "'") j += 2;
        else if (src[j] === "'") { j++; break; }
        else j++;
      }
      push('tok-str', src.slice(i, j));
      i = j;
    } else if (/[0-9]/.test(c)) {
      let j = i + 1;
      while (j < n && /[0-9._eE]/.test(src[j])) j++;
      push('tok-num', src.slice(i, j));
      i = j;
    } else if (/[A-Za-z_]/.test(c)) {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_]/.test(src[j])) j++;
      const w = src.slice(i, j);
      const up = w.toUpperCase();
      push(KW.has(up) ? 'tok-kw' : FN.has(up) ? 'tok-fn' : '', w);
      i = j;
    } else if (/[=<>!*,;()[\].]/.test(c)) {
      push('tok-op', c);
      i++;
    } else {
      let j = i + 1;
      while (j < n && !/[-'0-9A-Za-z_=<>!*,;()[\].]/.test(src[j])) j++;
      push('', src.slice(i, j));
      i = j;
    }
  }
  return out;
}

export function createEditor({ value = '', onRun, label = 'SQL editor' } = {}) {
  const code = h('code');
  const pre = h('pre', { 'aria-hidden': 'true' }, code);
  const ta = h('textarea', {
    spellcheck: 'false', autocapitalize: 'off', autocomplete: 'off', autocorrect: 'off',
    'aria-label': `${label} (Ctrl+Enter or Cmd+Enter runs)`, wrap: 'soft',
  });
  ta.value = value;
  const el = h('div', { class: 'editor' }, pre, ta);

  const render = () => {
    clear(code);
    for (const [cls, s] of tokenize(ta.value)) {
      code.append(cls ? h('span', { class: cls }, s) : document.createTextNode(s));
    }
    // A trailing newline needs content to keep the layers' heights equal.
    code.append(document.createTextNode('\n'));
    pre.scrollTop = ta.scrollTop;
    pre.scrollLeft = ta.scrollLeft;
  };

  const lineStart = pos => ta.value.lastIndexOf('\n', pos - 1) + 1;

  ta.addEventListener('input', render);
  ta.addEventListener('scroll', () => {
    pre.scrollTop = ta.scrollTop;
    pre.scrollLeft = ta.scrollLeft;
  });
  ta.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      if (onRun) onRun();
      return;
    }
    if (e.key === 'Tab' && !e.altKey && !e.ctrlKey && !e.metaKey) {
      // Escape then Tab leaves the editor (keyboard users are not trapped).
      if (ta.dataset.escaped === '1') { ta.dataset.escaped = ''; return; }
      e.preventDefault();
      const { selectionStart: s, selectionEnd: t } = ta;
      if (e.shiftKey) {
        const ls = lineStart(s);
        const block = ta.value.slice(ls, t);
        const out = block.replace(/^ {1,2}/gm, '');
        if (s !== t) {
          ta.setRangeText(out, ls, t, 'select');
        } else {
          ta.setRangeText(out, ls, t, 'preserve');
          const caret = Math.max(ls, s - (block.length - out.length));
          ta.setSelectionRange(caret, caret);
        }
      } else if (s !== t && ta.value.slice(s, t).includes('\n')) {
        const ls = lineStart(s);
        ta.setRangeText(ta.value.slice(ls, t).replace(/^/gm, INDENT), ls, t, 'select');
      } else {
        ta.setRangeText(INDENT, s, t, 'end');
      }
      render();
      return;
    }
    ta.dataset.escaped = e.key === 'Escape' ? '1' : '';
    if (e.key === 'Enter' && !e.shiftKey) {
      // Keep the current line's indentation.
      const s = ta.selectionStart;
      const ls = lineStart(s);
      const indent = (ta.value.slice(ls, s).match(/^\s*/) || [''])[0];
      if (indent) {
        e.preventDefault();
        ta.setRangeText(`\n${indent}`, s, ta.selectionEnd, 'end');
        render();
      }
    }
  });
  render();

  return {
    el,
    focus: () => ta.focus(),
    get value() { return ta.value; },
    set value(v) { ta.value = v; render(); },
    /** The selected text, or the whole text when nothing is selected. */
    get runnable() {
      const { selectionStart: s, selectionEnd: t } = ta;
      const sel = s !== t ? ta.value.slice(s, t) : '';
      return sel.trim() ? sel : ta.value;
    },
    insert(text) {
      ta.setRangeText(text, ta.selectionStart, ta.selectionEnd, 'end');
      render();
      ta.focus();
    },
  };
}
