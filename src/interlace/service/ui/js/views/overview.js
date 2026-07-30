// Overview: the room at a glance — drift, queue, streams, checks, and the live
// event feed. Every number links to the view that explains it.

import { clock, count, h, statusPill, table } from "../ui.js";

export async function render(el, { api, feed, go }) {
  const [planBody, runsBody, streamsBody, envsBody, checksBody, health] = await Promise.all([
    api.get("/plan"),
    api.get("/runs"),
    api.get("/streams"),
    api.get("/environments"),
    api.get("/checks"),
    api.get("/health"),
  ]);

  const active = runsBody.filter((run) => run.state === "running" || run.state === "queued");
  const failed = runsBody.filter((run) => run.state === "failed").length;
  const lag = streamsBody.reduce((sum, stream) => sum + Math.max(0, stream.head - stream.watermark), 0);
  const failingChecks = checksBody.filter((check) => check.status !== "passed").length;

  const stat = (label, value, { alert = false, route, small } = {}) => {
    const card = h(
      "div",
      { class: `stat ${alert ? "alert" : ""}`, style: route ? "cursor:pointer" : "" },
      h("div", { class: "k" }, label),
      h("div", { class: "v" }, String(value), small ? h("small", {}, ` ${small}`) : null),
    );
    if (route) card.addEventListener("click", () => go(route));
    return card;
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
    h("div", { class: "card-head" }, "event feed", h("span", { class: "spread" }), h("span", { class: "faint" }, "live")),
    feedRows,
  );

  el.append(h("div", { class: "grid2" }, recentRuns, liveCard));

  const pushEvent = (event) => {
    feedRows.prepend(
      h(
        "div",
        { class: "feed-row" },
        h("span", { class: "ts" }, clock(event.ts)),
        h("span", { class: "ty" }, event.type),
        h("span", { class: "en" }, event.entity ?? ""),
      ),
    );
    while (feedRows.children.length > 40) feedRows.lastChild.remove();
  };

  try {
    const recent = await api.get("/events");
    recent.slice(-25).forEach(pushEvent);
  } catch {
    /* fine — feed fills live */
  }
  const offFeed = feed.on(pushEvent);
  return () => offFeed();
}
