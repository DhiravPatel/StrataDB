// Client-side export of query results (nothing is sent anywhere).

function cellText(v) {
  if (v === undefined) return '';
  if (v === null) return '';
  if (typeof v === 'object') return JSON.stringify(v);
  return String(v);
}

/** RFC 4180 CSV. String cells that a spreadsheet would treat as a formula
 * are prefixed with a quote (CSV injection). */
export function toCSV(columns, rows) {
  const esc = (v, raw) => {
    let s = cellText(v);
    if (typeof raw === 'string' && /^[=+\-@\t\r]/.test(s)) s = `'${s}`;
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  const lines = [columns.map(c => esc(c, c)).join(',')];
  for (const r of rows) lines.push(columns.map(c => esc(r[c], r[c])).join(','));
  return `${lines.join('\r\n')}\r\n`;
}

export function toJSON(columns, rows) {
  return JSON.stringify({ columns, rows }, null, 2);
}

export function download(filename, text, type) {
  const blob = new Blob([text], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.append(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function stamp() {
  const d = new Date();
  const p = n => String(n).padStart(2, '0');
  return `${d.getFullYear()}${p(d.getMonth() + 1)}${p(d.getDate())}-${p(d.getHours())}${p(d.getMinutes())}${p(d.getSeconds())}`;
}
