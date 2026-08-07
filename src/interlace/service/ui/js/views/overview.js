// Overview: the room at a glance — drift, queue, streams, checks, and the live
// event feed. Every number links to the view that explains it.

import { clock, count, h, latestPerCheck, statusPill, table } from "../ui.js";
import { episodeKey, feedItem, groupEpisodes } from "../timeline.js";

export async function render(el, { api, feed, go }) {
  // /health is the liveness probe: if it rejects the daemon is down and the router
  // shows the error. The other five are wrapped so one flaky endpoint degrades to a
  // fallback instead of blanking the whole landing page.
  const safe = (promise, fallback) => promise.catch(() => fallback);
  const [planBody, runsBody, streamsBody, envsBody, checksBody, health] = await Promise.all([
    safe(api.get("/plan"), { changes: [] }),
    safe(api.get("/runs"), []),
    safe(api.get("/streams"), []),
    safe(api.get("/environments"), []),
    safe(api.get("/checks"), []),
    api.get("/health"),
  ]);

  const active = runsBody.filter((run) => run.state === "running" || run.state === "queued");
  const failed = runsBody.filter((run) => run.state === "failed").length;
  const lag = streamsBody.reduce((sum, stream) => sum + Math.max(0, stream.head - stream.watermark), 0);
  // one row per (model, check) first — /checks returns history, so raw filtering
  // would count a check that failed earlier and passes now (matches the checks view)
  const failingChecks = latestPerCheck(checksBody).filter((check) => check.status !== "passed").length;

  const stat = (label, value, { alert = false, route, small } = {}) => {
    const children = [
      h("div", { class: "k" }, label),
      h("div", { class: "v" }, String(value), small ? h("small", {}, ` ${small}`) : null),
    ];
    // a routed stat is a real link — keyboard-focusable and activable for free
    return route
      ? h("a", { class: `stat ${alert ? "alert" : ""}`, href: `#/${route}` }, ...children)
      : h("div", { class: `stat ${alert ? "alert" : ""}` }, ...children);
  };

  el.append(
    h("div", { class: "view-head" }, h("h1", {}, "Overview"), h("span", { class: "sub" }, `daemon v${health.version}`)),
    h(
      "div",
      { class: "stat-row" },
      stat("pending changes", planBody.changes.length, { alert: planBody.changes.length > 0, route: "plan" }),
      stat("active runs", active.length, { route: "runs" }),
      stat("failed runs", failed, { alert: failed > 0, route: "runs" }),
      stat("stream lag", count(lag), { alert: lag > 0, route: "streams", small: "events" }),
      stat("failing checks", failingChecks, { alert: failingChecks > 0, route: "checks" }),
      stat("environments", envsBody.length, { route: "environments" }),
    ),
  );

  const recentRuns = h(
    "div",
    { class: "card" },
    h("div", { class: "card-head" }, "recent runs", h("span", { class: "spread" }), h("a", { href: "#/runs", style: "color:var(--violet)" }, "all")),
    table(
      [
        { k: "id", label: "#", num: true },
        { k: "flow_selector", label: "models", render: (run) => run.flow_selector.join(", ") || "all" },
        { k: "state", label: "state", render: (run) => statusPill(run.state) },
        { k: "enqueued_at", label: "when", render: (run) => h("span", { class: "dim" }, clock(run.enqueued_at)) },
      ],
      runsBody.slice(0, 8),
      { onRow: (run) => go("runs", { r: run.id }), empty: "no runs yet", hint: "enqueue one from the runs view, or POST /runs" },
    ),
  );

  const feedRows = h("div", { class: "feed" });
  const liveCard = h(
    "div",
    { class: "card" },
    h("div", { class: "card-head" }, "activity", h("span", { class: "spread" }), h("span", { class: "faint" }, "live")),
    feedRows,
  );

  el.append(h("div", { class: "grid2" }, recentRuns, liveCard));

  // group the event stream into apply/run episodes (expandable), re-derived on every
  // event so it stays correct without incremental bookkeeping; `expanded` persists the
  // open episodes across re-renders
  const EVENT_CAP = 200;
  let events = [];
  const expanded = new Set();

  function renderFeed() {
    const items = groupEpisodes(events);
    if (!items.length) {
      feedRows.replaceChildren(h("div", { class: "empty" }, "no activity yet"));
      return;
    }
    feedRows.replaceChildren(
      ...items.map((item) => {
        const key = episodeKey(item);
        return feedItem(item, expanded.has(key), () => {
          if (expanded.has(key)) expanded.delete(key);
          else expanded.add(key);
          renderFeed();
        });
      }),
    );
  }

  const ingest = (event) => {
    events.push(event);
    if (events.length > EVENT_CAP) events = events.slice(-EVENT_CAP);
    renderFeed();
  };

  try {
    (await api.get("/events")).slice(-EVENT_CAP).forEach((event) => events.push(event));
  } catch {
    /* fine — fills live */
  }
  renderFeed();
  const offFeed = feed.on(ingest);
  return () => offFeed();
}
