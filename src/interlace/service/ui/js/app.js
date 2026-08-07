// Shell: hash router, command palette, live pill, rail badges, build dock.
// Views are ES modules under views/ exporting render(el, ctx) -> cleanup?.

import { api, feed, token } from "./api.js";
import { glyph, h, seconds, toUtc } from "./ui.js";

// Views load on demand: only the module for the route you visit is fetched (and then
// cached by the browser), so the initial payload is the shell + the first view, not
// all ten. Each entry is a dynamic import; the router awaits it before rendering.
const routes = {
  overview: () => import("./views/overview.js"),
  lineage: () => import("./views/lineage.js"),
  models: () => import("./views/models.js"),
  plan: () => import("./views/plan.js"),
  runs: () => import("./views/runs.js"),
  query: () => import("./views/query.js"),
  streams: () => import("./views/streams.js"),
  checks: () => import("./views/checks.js"),
  environments: () => import("./views/environments.js"),
  system: () => import("./views/system.js"),
};

// ---- toasts / modal ------------------------------------------------------------

export function toast(message, tone = "") {
  // errors are announced assertively (role=alert); the container is aria-live polite
  const el = h("div", { class: `toast ${tone}`, role: tone === "err" ? "alert" : null }, message);
  document.getElementById("toasts").append(el);
  setTimeout(() => el.remove(), tone === "err" ? 6000 : 2800);
}

const FOCUSABLE =
  'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])';

export function modal(build) {
  const scrim = document.getElementById("modalScrim");
  const body = document.getElementById("modalBody");
  const restoreFocus = document.activeElement; // return focus here when the dialog closes
  body.replaceChildren();

  const close = () => {
    scrim.hidden = true;
    document.removeEventListener("keydown", onKeydown, true);
    scrim.onclick = null;
    body.replaceChildren();
    body.removeAttribute("aria-label");
    if (restoreFocus && typeof restoreFocus.focus === "function") restoreFocus.focus();
  };

  // Escape closes; Tab is trapped inside the dialog so focus can't wander to the
  // page behind the scrim (capture phase, so children can't stop it first).
  const onKeydown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      close();
      return;
    }
    if (event.key !== "Tab") return;
    const items = [...body.querySelectorAll(FOCUSABLE)];
    if (!items.length) return;
    const first = items[0];
    const last = items[items.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };

  build(body, close);
  const heading = body.querySelector("h2"); // name the dialog for assistive tech
  if (heading) body.setAttribute("aria-label", heading.textContent);
  scrim.hidden = false;
  document.addEventListener("keydown", onKeydown, true);
  // statement body, never an expression: a DOM0 handler RETURNING false means
  // preventDefault — which silently cancels checkbox toggles inside the modal
  scrim.onclick = (event) => {
    if (event.target === scrim) close();
  };
  (body.querySelector(FOCUSABLE) ?? body).focus(); // initial focus inside the dialog
  return close;
}

// ---- build dock: live per-model rows, CLI-style ---------------------------------

const dock = {
  el: null,
  rows: new Map(), // model -> {row, started, running}
  foldTimer: null,
  start(model) {
    clearTimeout(this.foldTimer); // building again: stay open
    const previous = this.rows.get(model);
    if (previous && !previous.running) {
      previous.row.remove(); // same model, new run: fresh row
      this.rows.delete(model);
    }
    if (!this.rows.has(model)) {
      const row = h(
        "div",
        { class: "dock-row running" },
        h("span", { class: "st" }),
        h("span", { class: "nm" }, model),
        h("span", { class: "tm" }),
      );
      document.getElementById("dockRows").append(row);
      this.rows.set(model, { row, started: Date.now(), running: true });
    }
    this.el.hidden = false;
  },
  finish(model, outcome) {
    const entry = this.rows.get(model);
    if (!entry) return;
    entry.running = false;
    entry.row.classList.remove("running");
    const mark = { done: glyph.ok, failed: glyph.fail, cancelled: glyph.skip }[outcome] ?? glyph.ok;
    const cls = { done: "glyph-ok", failed: "glyph-fail", cancelled: "glyph-skip" }[outcome] ?? "glyph-ok";
    entry.row.querySelector(".st").replaceChildren(h("span", { class: cls }, mark));
    entry.row.querySelector(".tm").textContent = seconds((Date.now() - entry.started) / 1000);
    // fold away 8s after the LAST model resolves; any new start cancels the fold
    clearTimeout(this.foldTimer);
    this.foldTimer = setTimeout(() => this.clear(), 8000);
  },
  clear() {
    clearTimeout(this.foldTimer);
    this.rows.clear();
    document.getElementById("dockRows").replaceChildren();
    this.el.hidden = true;
  },
};

// ---- router ---------------------------------------------------------------------

let cleanup = null;
let generation = 0; // stale async renders must not touch the live view

export function go(route, params = {}) {
  const query = new URLSearchParams(params).toString();
  location.hash = `#/${route}${query ? "?" + query : ""}`;
}

function currentRoute() {
  const [path, queryString] = (location.hash.replace(/^#\//, "") || "overview").split("?");
  return { name: routes[path] ? path : "overview", params: Object.fromEntries(new URLSearchParams(queryString || "")) };
}

async function renderRoute() {
  const { name, params } = currentRoute();
  const mine = ++generation;
  document.querySelectorAll("#rail a").forEach((a) => a.classList.toggle("on", a.dataset.route === name));
  if (typeof cleanup === "function") cleanup();
  cleanup = null;
  const view = document.getElementById("view");
  view.dataset.route = name; // route-scoped chrome (lineage disables page scroll)
  view.replaceChildren();
  view.scrollTop = 0;
  // render into a detached container: a view that resolves AFTER the user has
  // already navigated away must neither touch the live view nor win the
  // cleanup slot — its listeners are torn down immediately instead
  const stage = h("div", { style: "display:contents" });
  // an unobtrusive top progress strip while the view's first fetch is in flight —
  // most views await their data before appending anything, so without this the
  // panel is a blank flash until it resolves
  const loading = h("div", { class: "route-loading" });
  view.append(loading, stage);
  try {
    const module = await routes[name](); // dynamic import (cached after first visit)
    const done = await module.render(stage, { api, feed, go, toast, modal, params, token });
    loading.remove();
    if (mine !== generation) {
      if (typeof done === "function") done(); // stale: release its listeners now
      stage.remove();
      return;
    }
    cleanup = done;
  } catch (error) {
    loading.remove();
    if (mine !== generation) return;
    view.replaceChildren(
      h("div", { class: "empty" }, `this view failed to load: ${error.message}`, h("div", { class: "hint" }, "the daemon may be unreachable — check that `interlace serve` is running")),
    );
  }
}

// ---- rail badges + drift chip -----------------------------------------------------

async function refreshBadges() {
  try {
    const [planBody, runsBody] = await Promise.all([api.get("/plan"), api.get("/runs")]);
    const changes = planBody.changes.length;
    const planBadge = document.querySelector('[data-badge="plan"]');
    planBadge.textContent = changes || "";
    const driftChip = document.getElementById("driftChip");
    driftChip.hidden = !changes;
    driftChip.textContent = `${changes} pending`;
    const active = runsBody.filter((run) => run.state === "running" || run.state === "queued").length;
    const runsBadge = document.querySelector('[data-badge="runs"]');
    runsBadge.textContent = active || "";
    runsBadge.classList.toggle("hot", runsBody.some((run) => run.state === "running"));
  } catch {
    /* unauthenticated or daemon away; badges stay quiet */
  }
}

// ---- command palette ---------------------------------------------------------------

function paletteSetup() {
  const scrim = document.getElementById("scrim");
  const input = document.getElementById("paletteInput");
  const results = document.getElementById("paletteResults");
  let hits = [];
  let selected = 0;
  let modelNames = [];

  const open = async () => {
    scrim.hidden = false;
    input.value = "";
    input.focus();
    try {
      modelNames = (await api.get("/models")).map((m) => m.name);
    } catch {
      modelNames = [];
    }
    update("");
  };
  const close = () => (scrim.hidden = true);

  const update = (needle) => {
    const lower = needle.toLowerCase();
    const views = Object.keys(routes)
      .filter((route) => route.includes(lower))
      .map((route) => ({ kind: "view", label: route, act: () => go(route) }));
    const models = modelNames
      .filter((name) => name.toLowerCase().includes(lower))
      .slice(0, 12)
      .map((name) => ({ kind: "model", label: name, act: () => go("models", { m: name }) }));
    const runMatch = /^\d+$/.test(needle) ? [{ kind: "run", label: `run #${needle}`, act: () => go("runs", { r: needle }) }] : [];
    hits = [...runMatch, ...models, ...views].slice(0, 16);
    selected = 0;
    results.replaceChildren(
      ...hits.map((hit, index) =>
        h(
          "div",
          { class: `hit ${index === selected ? "on" : ""}`, onclick: () => pick(index) },
          h("span", { class: "kind" }, hit.kind),
          h("span", {}, hit.label),
        ),
      ),
    );
  };

  const pick = (index) => {
    if (hits[index]) {
      close();
      hits[index].act();
    }
  };

  input.addEventListener("input", () => update(input.value));
  input.addEventListener("keydown", (event) => {
    if (event.key === "Escape") close();
    if (event.key === "Enter") pick(selected);
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      selected = (selected + (event.key === "ArrowDown" ? 1 : hits.length - 1)) % Math.max(hits.length, 1);
      results.querySelectorAll(".hit").forEach((el, index) => el.classList.toggle("on", index === selected));
    }
  });
  scrim.addEventListener("click", (event) => event.target === scrim && close());
  document.getElementById("cmdkBtn").addEventListener("click", open);
  document.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
      event.preventDefault();
      scrim.hidden ? open() : close();
    }
  });
}

// ---- boot -----------------------------------------------------------------------

async function boot() {
  dock.el = document.getElementById("dock");
  document.getElementById("dockClose").addEventListener("click", () => dock.clear());
  document.querySelectorAll("#rail a").forEach((a) => {
    a.href = `#/${a.dataset.route}`;
  });
  document.getElementById("envChip").addEventListener("click", () => go("environments"));
  paletteSetup();

  feed.onState((state) => {
    const pillEl = document.getElementById("livePill");
    pillEl.dataset.state = state;
    pillEl.querySelector(".live-label").textContent = { live: "live", poll: "poll", connecting: "sync" }[state];
  });
  feed.on((event) => {
    // the dock narrates the build happening NOW — replayed history stays out of it
    const fresh = event.ts && Date.now() - toUtc(event.ts).getTime() < 15000;
    if (event.type === "model.start" && fresh) dock.start(event.entity);
    else if (event.type?.startsWith("model.") && fresh) dock.finish(event.entity, event.type.slice(6));
    if (["run.enqueued", "run.started", "run.succeeded", "run.failed", "apply.finished"].includes(event.type)) {
      refreshBadges();
    }
  });
  feed.start();

  try {
    const health = await api.get("/health");
    document.getElementById("railFoot").textContent = `v${health.version}`;
    if (health.environment) document.getElementById("envName").textContent = health.environment;
  } catch {
    document.getElementById("railFoot").textContent = "offline";
  }
  refreshBadges();
  setInterval(refreshBadges, 30000);

  window.addEventListener("hashchange", renderRoute);
  // let the page enter the back/forward cache: close the live feed as it's hidden,
  // reopen only when it's actually restored from cache (persisted)
  window.addEventListener("pagehide", () => feed.stop());
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) feed.start();
  });
  renderRoute();
}

boot();
