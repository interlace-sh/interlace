// Turns the flat event stream into build *episodes* — one apply or one run — and
// renders them as timelines. Shared by the overview feed (grouped live) and the runs
// view (a single run's detail). Pure/DOM only; no fetching.
//
// Correlation, from how the daemon emits events:
//   apply  — apply.started(entity=env) … model.*(payload.environment=env) … apply.finished
//   run    — model.*(payload.run=<id>) … run.succeeded|failed|cancelled(entity=<id>)
// Everything else (run.enqueued, stream.flushed, publishes) stays a loose row.

import { clickableAttrs, clock, glyph, h, seconds, toUtc } from "./ui.js";

const _MARK = { done: glyph.ok, failed: glyph.fail, cancelled: glyph.skip };
const _TONE = { done: "glyph-ok", failed: "glyph-fail", cancelled: "glyph-skip" };
const _STATUS_MARK = { ok: glyph.ok, failed: glyph.fail, skip: glyph.skip };
const _STATUS_TONE = { ok: "glyph-ok", failed: "glyph-fail", skip: "glyph-skip" };

function _secs(from, to) {
  return (toUtc(to.ts).getTime() - toUtc(from.ts).getTime()) / 1000;
}

// ---- grouping -----------------------------------------------------------------

function _episode(kind, target, ev) {
  return { kind, target, start: ev, end: null, status: "running", steps: [] };
}

/** Group chronological events into items (episodes + loose events), newest first. */
export function groupEpisodes(events) {
  const openApply = new Map(); // env -> open apply episode
  const runs = new Map(); // run id -> run episode
  const items = [];
  for (const ev of events) {
    const group = ev.type.split(".")[0];
    const phase = ev.type.slice(group.length + 1);
    if (group === "apply" && phase === "started") {
      const ep = _episode("apply", ev.entity, ev);
      openApply.set(ev.entity, ep);
      items.push(ep);
    } else if (group === "apply") {
      const ep = openApply.get(ev.entity); // finished / blocked
      if (ep) {
        ep.end = ev;
        ep.status = phase === "blocked" ? "failed" : "ok";
        openApply.delete(ev.entity);
      } else items.push({ kind: "event", ev });
    } else if (group === "run" && ["succeeded", "failed", "cancelled"].includes(phase)) {
      const ep = runs.get(ev.entity) ?? _episode("run", ev.entity, ev);
      if (!runs.has(ev.entity)) {
        runs.set(ev.entity, ep);
        items.push(ep);
      }
      ep.end = ev;
      ep.status = phase === "succeeded" ? "ok" : phase === "failed" ? "failed" : "skip";
    } else if (group === "model") {
      const id = ev.payload?.run;
      const ep = id != null ? runs.get(String(id)) : openApply.get(ev.payload?.environment);
      if (ep) ep.steps.push(ev);
      else if (id != null) {
        const created = _episode("run", String(id), ev); // first model.* before the terminal
        runs.set(String(id), created);
        created.steps.push(ev);
        items.push(created);
      } else items.push({ kind: "event", ev });
    } else {
      items.push({ kind: "event", ev }); // run.enqueued, stream.flushed, publishes, …
    }
  }
  return items.reverse();
}

/** A stable key so an expanded episode stays expanded across live re-renders. */
export function episodeKey(item) {
  if (item.kind === "event") return `e:${item.ev.seq}`;
  return item.kind === "apply" ? `a:${item.start.seq}` : `r:${item.target}`;
}

// ---- rendering ----------------------------------------------------------------

/** Per-model steps from model.* events: pair each start with its terminal by name. */
function modelSteps(events) {
  const steps = [];
  for (const ev of events) {
    if (!ev.type.startsWith("model.")) continue;
    const phase = ev.type.slice(6);
    if (phase === "start") steps.push({ name: ev.entity, start: ev, end: null, phase: "running" });
    else {
      const open = [...steps].reverse().find((s) => s.name === ev.entity && s.end === null);
      if (open) {
        open.end = ev;
        open.phase = phase;
      } else steps.push({ name: ev.entity, start: null, end: ev, phase });
    }
  }
  return steps;
}

/** A vertical model timeline for one build. `events` is the build's model.* events. */
export function modelTimeline(events) {
  const steps = modelSteps(events);
  if (!steps.length) return h("div", { class: "tl-empty" }, "no models recorded");
  const rail = h("div", { class: "timeline" });
  for (const step of steps) {
    const running = step.phase === "running" || step.end === null;
    rail.append(
      h(
        "div",
        { class: "tl-step" },
        h("span", { class: `tl-mark ${running ? "running" : _TONE[step.phase] ?? "glyph-ok"}` }, running ? "" : _MARK[step.phase] ?? glyph.ok),
        h("span", { class: "tl-name" }, step.name),
        h("span", { class: "tl-dur" }, step.start && step.end ? seconds(_secs(step.start, step.end)) : ""),
      ),
    );
  }
  return rail;
}

function _looseRow(ev) {
  const isModel = ev.type.startsWith("model.");
  return h(
    "div",
    { class: "feed-row" },
    h("span", { class: "ts" }, clock(ev.ts)),
    h("span", { class: "ty" }, ev.type),
    h("span", { class: "en" }, isModel ? ev.entity : ev.entity || JSON.stringify(ev.payload ?? {}).slice(0, 120)),
  );
}

/** One feed item: a loose event row, or an expandable episode card. */
export function feedItem(item, isExpanded, toggle) {
  if (item.kind === "event") return _looseRow(item.ev);

  const count = new Set(item.steps.map((s) => s.entity)).size;
  const dur = item.end ? seconds(_secs(item.start, item.end)) : null;
  const label = item.kind === "apply" ? "apply" : `run #${item.target}`;
  const mark = item.status === "running" ? "" : _STATUS_MARK[item.status] ?? glyph.ok;
  const tone = item.status === "running" ? "running" : _STATUS_TONE[item.status] ?? "glyph-ok";

  const head = h(
    "div",
    { class: "episode-head", ...clickableAttrs(toggle), "aria-expanded": String(isExpanded) },
    h("span", { class: `tl-mark ${tone}` }, mark),
    h("span", { class: "ep-kind" }, label),
    item.kind === "apply" ? h("span", { class: "ep-target" }, item.target) : null,
    h("span", { class: "spread" }),
    h("span", { class: "ep-meta" }, `${count} model${count === 1 ? "" : "s"}${dur ? ` · ${dur}` : ""}`),
    h("span", { class: "ts" }, clock(item.start.ts)),
  );
  const card = h("div", { class: "episode" }, head);
  if (isExpanded) card.append(modelTimeline(item.steps));
  return card;
}
