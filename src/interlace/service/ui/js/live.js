// The live event feed. EventSource on /events/stream when the API is open;
// with a token set, EventSource can't send Authorization, so we poll /events.
import { api, getToken } from "./api.js";
import { $, esc, fmtTime } from "./util.js";

const ICON = { succeeded: "✓", started: "▸", enqueued: "+", failed: "✕", neutral: "·" };
const KIND = {
  "apply.started": "started",
  "apply.finished": "succeeded",
  "apply.blocked": "failed",
  "run.enqueued": "enqueued",
  "run.succeeded": "succeeded",
  "run.failed": "failed",
  "run.cancel_requested": "neutral",
  "run.cancelled": "neutral",
  "stream.flushed": "succeeded",
  "environment.dropped": "neutral",
  "gc.finished": "neutral",
};

function describe(event) {
  const p = event.payload || {};
  switch (event.type) {
    case "apply.started": return `apply → ${event.entity} · ${(p.models || []).length} models`;
    case "apply.finished": return `applied → ${event.entity} · built ${(p.built || []).length} · promoted ${p.promoted ?? "?"}`;
    case "apply.blocked": return `apply blocked · ${p.reason || ""}`;
    case "run.enqueued": return `enqueued · ${(p.models || []).join(", ")}`;
    case "run.succeeded": return `run ${event.entity} built ${(p.built || []).join(", ")}`;
    case "run.failed": return `run ${event.entity} failed · ${p.error || ""}`;
    case "stream.flushed": return `flushed ${p.rows ?? "?"} rows → streams.${event.entity}`;
    case "environment.dropped": return `environment ${event.entity} dropped`;
    case "gc.finished": return `gc reclaimed ${p.snapshots ?? "?"} snapshots`;
    default: return event.type;
  }
}

const listeners = new Set(); // fn(event) — views subscribe for refresh nudges
export const onEvent = (fn) => (listeners.add(fn), () => listeners.delete(fn));

let feedEl = null;
let lastSeq = 0;
let source = null;
let pollTimer = 0;

function setPill(state) {
  const dot = $("#livePill .dot"), label = $("#liveLabel");
  if (!dot) return;
  dot.className = "dot" + (state === "live" ? "" : state === "poll" ? "" : state === "err" ? " err" : " off");
  label.textContent = state === "live" ? "STREAMING" : state === "poll" ? "POLLING" : state === "err" ? "OFFLINE" : "CONNECTING";
}

function render(event, animate) {
  if (!feedEl) return;
  const kind = KIND[event.type] || "neutral";
  const el = document.createElement("div");
  el.className = `ev k-${kind}` + (animate ? " enter" : "");
  const name = event.entity && !String(event.entity).includes(":") ? `<b>${esc(event.entity)}</b> ` : "";
  el.innerHTML = `<span class="t">${fmtTime(event.ts)}</span><span class="ico">${ICON[kind]}</span><span class="msg">${name}${esc(describe(event))}</span>`;
  feedEl.prepend(el);
  while (feedEl.children.length > 16) feedEl.lastChild.remove();
}

function dispatch(event, animate) {
  if (event.seq <= lastSeq) return;
  lastSeq = event.seq;
  render(event, animate);
  for (const fn of listeners) fn(event);
}

async function poll() {
  try {
    const events = await api.events(lastSeq);
    for (const e of events) dispatch(e, true);
    setPill("poll");
  } catch {
    setPill("err");
  }
  pollTimer = setTimeout(poll, 2500);
}

export function startLive() {
  if (getToken()) { poll(); return; } // keyed deployment: EventSource can't authenticate
  source = new EventSource("/events/stream");
  source.onopen = () => setPill("live");
  source.onerror = () => setPill("err"); // EventSource reconnects itself (Last-Event-ID replays)
  source.onmessage = (msg) => { try { dispatch(JSON.parse(msg.data), true); } catch { /* ignore */ } };
}

export function stopLive() {
  source?.close(); source = null;
  clearTimeout(pollTimer);
}

export const feedComponent = () =>
  `<div class="feed-head"><span class="pulse"></span> Event stream · /events</div><div class="feed-list" id="feed"></div>`;

// (Re)attach the shared feed element inside the current view and seed it with history.
export async function wireFeed() {
  feedEl = $("#feed");
  if (!feedEl) return;
  feedEl.innerHTML = "";
  try {
    const events = await api.events(Math.max(0, lastSeq - 200));
    for (const e of events.slice(-16)) { if (feedEl) render(e, false); lastSeq = Math.max(lastSeq, e.seq); }
  } catch { /* feed stays empty; pill shows the state */ }
}
export const unwireFeed = () => { feedEl = null; };
