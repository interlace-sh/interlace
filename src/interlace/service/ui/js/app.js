// Boot + router + command palette + environment switcher.
import { api } from "./api.js";
import { currentEnv, invalidate, loadEnvironments, loadModels, loadPlan, setEnv } from "./data.js";
import { onEvent, startLive } from "./live.js";
import { $, $$, MAT_GLYPH, esc, toast } from "./util.js";
import { ui, views } from "./views.js";

let teardown = null;
let routeSeq = 0;

async function setRoute(name) {
  if (!views[name]) name = "lineage";
  const seq = ++routeSeq;
  if (teardown) { teardown(); teardown = null; }
  $$(".nav-item").forEach((it) => it.classList.toggle("active", it.dataset.route === name));
  $("#view").innerHTML = `<div class="loading">loading…</div>`;
  let view;
  try { view = await views[name](); }
  catch (err) { view = { html: `<div class="vhead"><h1>${esc(name)}</h1></div><div class="error-box">✕ ${esc(err.detail || err.message || err)}</div>` }; }
  if (seq !== routeSeq) return; // navigated away while loading
  $("#view").innerHTML = view.html;
  if (view.mount) teardown = view.mount() || null;
  if (location.hash.slice(1) !== name) location.hash = name;
  refreshCounts();
}
ui.navigate = setRoute;
ui.setEnv = (env) => { setEnv(env); invalidate("plan"); paintEnvPill(); };

$("#nav").addEventListener("click", (e) => { const it = e.target.closest(".nav-item"); if (it) setRoute(it.dataset.route); });
document.addEventListener("click", (e) => { const g = e.target.closest("[data-go]"); if (g) setRoute(g.dataset.go); });
addEventListener("hashchange", () => { const r = location.hash.slice(1); if (r && !$(`.nav-item[data-route="${r}"].active`)) setRoute(r); });

/* ---------------- confirm/modal ---------------- */
const modalScrim = $("#modalScrim"), modalCard = $("#modalCard");
const closeModal = () => modalScrim.classList.remove("show");
modalScrim.addEventListener("click", (e) => { if (e.target === modalScrim) closeModal(); });
ui.confirm = ({ title, body, action, onConfirm, danger = false }) => {
  modalCard.innerHTML = `<header>${title}</header>
    <div class="mc-body">${body}</div>
    <footer><button class="btn" id="mCancel">Cancel</button>
      <button class="btn primary" id="mGo" ${danger ? 'style="background:var(--coral);border-color:var(--coral)"' : ""}>${esc(action)}</button></footer>`;
  $("#mCancel").onclick = closeModal;
  $("#mGo").onclick = () => { closeModal(); onConfirm(); };
  modalScrim.classList.add("show");
  modalCard.querySelector("input,textarea")?.focus();
};

/* ---------------- command palette ---------------- */
const VIEW_ITEMS = [
  ["lineage", "⧉", "Lineage"], ["models", "▤", "Models"], ["plan", "±", "Plan"], ["runs", "▸", "Runs"],
  ["streams", "≈", "Streams"], ["environments", "⊞", "Environments"], ["checks", "✓", "Checks"], ["settings", "⌥", "Settings"],
];
const scrim = $("#scrim"), pinput = $("#pinput"), presults = $("#presults");
let pidx = 0, pitems = [], paletteModels = { byName: {}, names: [] };
function openPalette() {
  loadModels().then((m) => { paletteModels = m; renderPalette(pinput.value); }).catch(() => {});
  scrim.classList.add("show"); pinput.value = ""; renderPalette(""); pinput.focus();
}
const closePalette = () => scrim.classList.remove("show");
function renderPalette(q) {
  q = q.toLowerCase();
  const vs = VIEW_ITEMS.filter((v) => v[2].toLowerCase().includes(q)).map((v) => ({ type: "view", route: v[0], g: v[1], nm: v[2], k: "view" }));
  const ms = paletteModels.names.filter((n) => n.toLowerCase().includes(q)).map((n) => ({ type: "model", route: n, g: MAT_GLYPH[paletteModels.byName[n].mat] || "·", nm: n, k: "model" }));
  pitems = [...vs, ...ms].slice(0, 9); pidx = 0;
  presults.innerHTML = pitems.map((it, i) => `<div class="pitem ${i === 0 ? "cur" : ""}" data-i="${i}"><span class="pg">${it.g}</span><span class="nm">${esc(it.nm)}</span><span class="pk">${it.k}</span></div>`).join("")
    || '<div class="empty">no matches</div>';
  $$(".pitem", presults).forEach((el) => (el.onclick = () => choose(+el.dataset.i)));
}
function choose(i) {
  const it = pitems[i]; if (!it) return;
  closePalette();
  if (it.type === "view") setRoute(it.route);
  else { ui.selected = it.route; setRoute("lineage"); }
}
pinput.addEventListener("input", (e) => renderPalette(e.target.value));
pinput.addEventListener("keydown", (e) => {
  if (e.key === "Escape") closePalette();
  else if (e.key === "ArrowDown") { pidx = Math.min(pidx + 1, pitems.length - 1); highlightP(); }
  else if (e.key === "ArrowUp") { pidx = Math.max(pidx - 1, 0); highlightP(); }
  else if (e.key === "Enter") choose(pidx);
});
const highlightP = () => $$(".pitem", presults).forEach((el, i) => el.classList.toggle("cur", i === pidx));
scrim.addEventListener("click", (e) => { if (e.target === scrim) closePalette(); });
$("#searchBtn").onclick = openPalette;
addEventListener("keydown", (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); openPalette(); }
  else if (e.key === "Escape" && modalScrim.classList.contains("show")) closeModal();
});

/* ---------------- environment switch ---------------- */
function paintEnvPill() { $("#envTo").textContent = currentEnv(); }
$("#envSwitch").onclick = async () => {
  let names = ["prod", "dev"];
  try {
    const envs = await loadEnvironments();
    if (envs.length) names = [...new Set([...envs.map((e) => e.name), "prod", "dev"])];
  } catch { /* offline: cycle defaults */ }
  const next = names[(names.indexOf(currentEnv()) + 1) % names.length];
  ui.setEnv(next);
  toast("Target changed", `plan/apply target → ${next}`);
  const route = location.hash.slice(1);
  if (route === "plan" || route === "lineage" || route === "environments") setRoute(route);
};

/* ---------------- nav counts + foot ---------------- */
async function refreshCounts() {
  const set = (key, value) => $$(`[data-count="${key}"]`).forEach((el) => (el.textContent = value ?? ""));
  loadModels().then(({ names }) => set("models", names.length)).catch(() => set("models", ""));
  loadPlan().then((p) => set("plan", p.changes.filter((c) => !c.reused).length || "")).catch(() => set("plan", ""));
  api.runs().then((rs) => set("runs", rs.filter((r) => r.state === "queued" || r.state === "running").length || "")).catch(() => set("runs", ""));
  loadModels().catch(() => {}); // keep cache warm for the palette
  api.streams().then((ss) => set("streams", ss.length || "")).catch(() => set("streams", ""));
  loadEnvironments().then((es) => set("envs", es.length || "")).catch(() => set("envs", ""));
  api.checks().then((cs) => set("checks", cs.length || "")).catch(() => set("checks", ""));
}

async function boot() {
  paintEnvPill();
  try {
    const health = await api.health();
    $("#navFoot").innerHTML = `state <span class="ok">healthy</span><br>v${esc(health.version || "?")}`;
  } catch {
    $("#navFoot").innerHTML = `state <span style="color:var(--coral)">unreachable</span>`;
  }
  startLive();
  // live events nudge the caches so the next render is fresh
  onEvent(() => invalidate());
  setRoute(location.hash.slice(1) || "lineage");
}
boot();
