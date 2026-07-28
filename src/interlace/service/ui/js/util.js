// Shared helpers: escaping, formatting, toasts, glyph/colour maps.
export const $ = (s, r = document) => r.querySelector(s);
export const $$ = (s, r = document) => [...r.querySelectorAll(s)];
export const css = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();
export const esc = (s) => String(s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;");
export const reduce = matchMedia("(prefers-reduced-motion:reduce)").matches;

export const MAT_GLYPH = { table: "▦", view: "◇", ephemeral: "◌", sink: "⇥" };
export const STATE_COLOR = { added: "#34D399", breaking: "#EF4444", nonbreaking: "#8B5CF6", unchanged: "#71717A", running: "#3B82F6" };
export const STATE_LABEL = { added: "added", breaking: "breaking", nonbreaking: "non-breaking", unchanged: "unchanged" };

export const runColor = (s) =>
  ({ running: css("--cyan"), succeeded: css("--green"), failed: css("--coral"), queued: css("--warn"), cancelling: css("--warn"), cancelled: css("--faint") })[s] || css("--faint");

export function toast(title, msg) {
  const t = document.createElement("div");
  t.className = "toast";
  t.innerHTML = `<div class="tt">${esc(title)}</div>${esc(msg)}`;
  $("#toasts").appendChild(t);
  setTimeout(() => t.remove(), 3600);
}

export function highlightSql(s) {
  return esc(s)
    .replace(/(--[^\n]*)/g, '<span class="cm">$1</span>')
    .replace(/\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|GROUP BY|ORDER BY|LIMIT|UNION|EXCEPT|USING|ON|AS|AND|OR|NOT|CASE|WHEN|THEN|ELSE|END|DESC|ASC|WITH)\b/gi, '<span class="kw">$1</span>')
    .replace(/\b(count|sum|avg|min|max|lower|upper|coalesce|round|hash|date_trunc|now|cast|read_json)\b/gi, '<span class="fn">$1</span>');
}

export const shortFp = (fp) => (fp || "").slice(0, 8) || "—";

export function fmtSchedule(schedule) {
  if (!schedule) return "—";
  if (schedule.cron) return schedule.cron;
  if (schedule.every) return `every ${schedule.every}`;
  return "—";
}

export function fmtTime(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  return isNaN(d) ? String(iso).slice(11, 19) || String(iso) : d.toLocaleTimeString([], { hour12: false });
}

export function fmtAgo(iso) {
  if (!iso) return "—";
  const d = new Date(iso);
  if (isNaN(d)) return String(iso);
  const s = Math.max(0, (Date.now() - d.getTime()) / 1000);
  if (s < 60) return "just now";
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

// Map an API change (change_type + category + reused) onto the prototype's state names.
export function changeState(change) {
  if (!change) return "unchanged";
  if (change.change_type === "added") return "added";
  if (change.change_type === "removed") return "unchanged";
  if (change.category === "breaking") return "breaking";
  return "nonbreaking";
}

// The trigger source is the idempotency key's prefix — same rule as the CLI.
export const runTrigger = (key) => (key && key.includes(":") ? key.split(":", 1)[0] : "manual");
