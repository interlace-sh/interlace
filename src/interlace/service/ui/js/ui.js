// DOM + formatting helpers shared by every view. Glyphs and table style mirror
// the CLI (✓ ✗ ⊘, rule-under-header tables, +green ~amber -red row deltas).

export function h(tag, attrs = {}, ...children) {
  const el = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === null || value === undefined || value === false) continue;
    if (key === "class") el.className = value;
    else if (key === "dataset") Object.assign(el.dataset, value);
    else if (key.startsWith("on") && typeof value === "function") el.addEventListener(key.slice(2), value);
    else if (key === "html") el.innerHTML = value;
    else el.setAttribute(key, value === true ? "" : value);
  }
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    el.append(child.nodeType ? child : document.createTextNode(String(child)));
  }
  return el;
}

export const glyph = { ok: "✓", fail: "✗", skip: "⊘", run: "◌" };

export function relTime(iso) {
  if (!iso) return "—";
  const seconds = (Date.now() - new Date(iso.endsWith("Z") || iso.includes("+") ? iso : iso + "Z").getTime()) / 1000;
  if (!Number.isFinite(seconds)) return iso;
  if (seconds < 0) return "in " + relSpan(-seconds);
  if (seconds < 5) return "just now";
  return relSpan(seconds) + " ago";
}

function relSpan(seconds) {
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
}

export function clock(iso) {
  if (!iso) return "—";
  const when = new Date(iso.endsWith("Z") || iso.includes("+") ? iso : iso + "Z");
  return when.toLocaleTimeString([], { hour12: false });
}

export function seconds(value) {
  if (value === null || value === undefined) return "—";
  return value >= 10 ? `${value.toFixed(1)}s` : `${value.toFixed(2)}s`;
}

export function count(value) {
  return Number(value ?? 0).toLocaleString();
}

/** +12,340 ~5 -2 in the CLI's colours; em-dash when nothing moved. */
export function rowsDelta(rows) {
  if (!rows) return h("span", { class: "faint" }, "—");
  const parts = [];
  if (rows.inserted) parts.push(h("span", { class: "ins" }, `+${count(rows.inserted)}`));
  if (rows.updated) parts.push(h("span", { class: "upd" }, `~${count(rows.updated)}`));
  if (rows.deleted) parts.push(h("span", { class: "del" }, `-${count(rows.deleted)}`));
  if (!parts.length) return h("span", { class: "faint" }, "—");
  const wrap = h("span", { class: "rows-delta" });
  parts.forEach((part, index) => {
    if (index) wrap.append(" ");
    wrap.append(part);
  });
  return wrap;
}

export function pill(text, tone = "") {
  return h("span", { class: `pill ${tone}` }, text);
}

export function statusPill(status) {
  const tone =
    { succeeded: "green", passed: "green", running: "amber", queued: "", failed: "red", error: "red", cancelled: "" }[
      status
    ] ?? "";
  return pill(status, tone);
}

/** The house table: columns = [{k, label, num?, render?}], rows = objects.
 * `expandRow(row)` may return a node rendered full-width directly under that row. */
export function table(columns, rows, { onRow, empty = "nothing here yet", hint, expandRow } = {}) {
  if (!rows.length) {
    return h("div", { class: "empty" }, empty, hint ? h("div", { class: "hint" }, hint) : null);
  }
  const head = h("tr", {}, columns.map((col) => h("th", { class: col.num ? "num" : "" }, col.label ?? col.k)));
  const body = [];
  for (const row of rows) {
    const tr = h(
      "tr",
      { class: onRow ? "click" : "" },
      columns.map((col) => {
        const cell = col.render ? col.render(row) : row[col.k];
        return h("td", { class: col.num ? "num" : "" }, cell ?? "—");
      }),
    );
    if (onRow) tr.addEventListener("click", () => onRow(row));
    body.push(tr);
    const detail = expandRow?.(row);
    if (detail) body.push(h("tr", { class: "expand-row" }, h("td", { colspan: columns.length }, detail)));
  }
  return h("table", { class: "t" }, h("thead", {}, head), h("tbody", {}, body));
}

// ---- SQL highlighting (display only — the server owns parsing) ---------------

const KEYWORDS = new RegExp(
  "\\b(select|from|where|group by|order by|having|qualify|join|left|right|full|inner|outer|cross|natural|on|using|" +
    "with|as|case|when|then|else|end|union|all|distinct|limit|offset|and|or|not|in|exists|between|like|ilike|is|" +
    "null|true|false|over|partition by|rows|range|interval|create|table|view|insert|into|values|update|delete|cast)\\b",
  "gi",
);

export function highlightSql(sql) {
  if (!sql) return "";
  let out = sql.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
  out = out.replace(/('(?:[^']|'')*')/g, '<span class="str">$1</span>');
  out = out.replace(/(--[^\n]*)/g, '<span class="cmt">$1</span>');
  out = out.replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="num">$1</span>');
  out = out.replace(/\b([a-z_][a-z0-9_]*)\s*\(/gi, '<span class="fn">$1</span>(');
  out = out.replace(KEYWORDS, (match) => `<span class="kw">${match}</span>`);
  return out;
}

export function sqlBlock(sql) {
  return h("pre", { class: "sql", html: highlightSql(sql) });
}

const PY_KEYWORDS = new RegExp(
  "\\b(def|return|import|from|as|if|elif|else|for|while|in|not|and|or|is|None|True|False|class|with|async|await|" +
    "yield|lambda|try|except|finally|raise|pass|break|continue|global|nonlocal|assert|del|match|case)\\b",
  "g",
);
const PY_STRINGS = /("(?:""[^]*?""|(?:[^"\\\n]|\\.)*)"|'(?:''[^]*?''|(?:[^'\\\n]|\\.)*)')/g;

/** Python source in the same block chrome as SQL (display only). */
export function pythonBlock(source) {
  let out = (source || "").replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" })[c]);
  out = out.replace(PY_STRINGS, '<span class="str">$1</span>');
  out = out.replace(/(#[^\n]*)/g, '<span class="cmt">$1</span>');
  out = out.replace(/^(\s*@[\w.]+)/gm, '<span class="dec">$1</span>');
  out = out.replace(/\b(\d+(?:\.\d+)?)\b/g, '<span class="num">$1</span>');
  out = out.replace(PY_KEYWORDS, '<span class="kw">$1</span>');
  return h("pre", { class: "sql", html: out });
}

// ---- line diff (LCS) -----------------------------------------------------------

export function diffLines(before, after) {
  const a = (before || "").split("\n");
  const b = (after || "").split("\n");
  const lcs = Array.from({ length: a.length + 1 }, () => new Array(b.length + 1).fill(0));
  for (let i = a.length - 1; i >= 0; i--)
    for (let j = b.length - 1; j >= 0; j--)
      lcs[i][j] = a[i] === b[j] ? lcs[i + 1][j + 1] + 1 : Math.max(lcs[i + 1][j], lcs[i][j + 1]);
  const out = [];
  let i = 0;
  let j = 0;
  while (i < a.length && j < b.length) {
    if (a[i] === b[j]) {
      out.push(["=", a[i]]);
      i++;
      j++;
    } else if (lcs[i + 1][j] >= lcs[i][j + 1]) out.push(["-", a[i++]]);
    else out.push(["+", b[j++]]);
  }
  while (i < a.length) out.push(["-", a[i++]]);
  while (j < b.length) out.push(["+", b[j++]]);
  return out;
}

export function diffBlock(before, after) {
  const wrap = h("div", { class: "diff" });
  for (const [kind, line] of diffLines(before, after)) {
    const cls = kind === "+" ? "l add" : kind === "-" ? "l del" : "l";
    wrap.append(h("div", { class: cls }, `${kind === "=" ? " " : kind} ${line}`));
  }
  return wrap;
}

// ---- misc ----------------------------------------------------------------------

export function debounce(fn, wait = 150) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), wait);
  };
}

export function copy(text, toast) {
  navigator.clipboard?.writeText(text).then(
    () => toast?.("copied"),
    () => toast?.("copy failed", "err"),
  );
}
