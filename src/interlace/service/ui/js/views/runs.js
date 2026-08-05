// Runs: the durable queue, live. Clicking a run expands its detail directly
// under the row — the CLI's build-results table (model / output / strategy /
// engine / depends on / rows / time), the checks line, and the summary line,
// then the raw event timeline.

import { clock, glyph, h, pill, relTime, rowsDelta, seconds, statusPill, table } from "../ui.js";

export async function render(el, { api, feed, go, toast, modal, params }) {
  const listBody = h("div", {});
  const enqueueBtn = h("button", { class: "btn primary" }, "run…");

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Runs"),
      h("span", { class: "sub" }, "the durable queue — leases, retries, cancellation"),
      h("span", { class: "spread" }),
      enqueueBtn,
    ),
    listBody,
  );

  let runs = [];
  let catalog = new Map(); // model name -> ModelInfo (output/strategy/engine/depends_on)
  let openRun = params.r ? Number(params.r) : null;
  let openDetail = null; // fetched RunDetail for openRun

  async function refresh() {
    const [runList, models] = await Promise.all([api.get("/runs"), catalog.size ? null : api.get("/models")]);
    runs = runList;
    if (models) catalog = new Map(models.map((m) => [m.name, m]));
    if (openRun !== null) {
      try {
        openDetail = await api.get(`/runs/${openRun}`);
      } catch {
        openDetail = null;
      }
    }
    renderList();
  }

  async function toggleRun(id) {
    if (openRun === id) {
      openRun = null;
      openDetail = null;
      history.replaceState(null, "", "#/runs");
      renderList();
      return;
    }
    openRun = id;
    history.replaceState(null, "", `#/runs?r=${id}`); // deep-linkable without a re-render
    await refresh(); // list AND detail together — an expanded run must not show a stale state pill
  }

  async function cancel(id) {
    try {
      const outcome = await api.post(`/runs/${id}/cancel`);
      toast(`run #${id}: ${outcome.state}`);
      refresh();
    } catch (error) {
      toast(error.message, "err");
    }
  }

  // ---- the expanded detail: CLI build results, checks line, summary, timeline ----

  function buildResults(payload) {
    const names = [...(payload.built ?? [])];
    if (!names.length) return null;
    const rows = names.map((name) => {
      const info = catalog.get(name) ?? {};
      return {
        name,
        output: info.output ?? "—",
        strategy: info.strategy ?? "—",
        engine: info.engine ?? "default",
        deps: (info.depends_on ?? []).join(", "),
        rows: payload.rows?.[name],
        time: payload.timings?.[name],
      };
    });
    return table(
      [
        { k: "name", label: "model" },
        { k: "output", label: "output", render: (row) => h("span", { class: "dim" }, row.output) },
        { k: "strategy", label: "strategy", render: (row) => h("span", { class: "dim" }, row.strategy) },
        {
          k: "engine",
          label: "engine",
          render: (row) => h("span", { class: "dim" }, row.engine === "default" ? "default" : row.engine),
        },
        { k: "deps", label: "depends on", render: (row) => h("span", { class: "dim" }, row.deps || "—") },
        { k: "rows", label: "rows", num: true, render: (row) => rowsDelta(row.rows) },
        { k: "time", label: "time", num: true, render: (row) => h("span", { class: "dim" }, seconds(row.time)) },
      ],
      rows,
    );
  }

  function summaryLines(detail, payload) {
    const lines = [];
    if (payload.checks?.total) {
      const warned = payload.checks.failing ?? [];
      lines.push(
        h(
          "div",
          { style: warned.length ? "color:var(--amber)" : "" },
          `Checks: ${payload.checks.passed}/${payload.checks.total} passed`,
          warned.length ? ` — ${warned.join(", ")}` : "",
        ),
      );
    }
    const built = (payload.built ?? []).length;
    const reused = (payload.reused ?? []).length;
    const gated = (payload.gated ?? []).length;
    if (built || payload.promoted) {
      const parts = [`Ran ${built} model(s)`];
      if (reused) parts.push(`${reused} reused`);
      if (gated) parts.push(`${gated} gated`);
      let line = parts.join(", ");
      if (payload.promoted) line += `; promoted ${payload.promoted} to '${payload.environment ?? ""}'`;
      lines.push(h("div", { class: "dim" }, line + "."));
    }
    return lines;
  }

  function timeline(detail) {
    const feedEl = h("div", { class: "feed", style: "border-top:1px solid var(--line-soft); margin-top:10px; padding-top:6px" });
    for (const event of detail.events) {
      const isModel = event.type.startsWith("model.");
      const mark = { "model.done": glyph.ok, "model.failed": glyph.fail, "model.cancelled": glyph.skip, "model.start": "▸" }[event.type];
      feedEl.append(
        h(
          "div",
          { class: "feed-row" },
          h("span", { class: "ts" }, clock(event.ts)),
          h("span", { class: "ty" }, isModel && mark ? `${mark} ${event.type}` : event.type),
          h("span", { class: "en" }, isModel ? event.entity : JSON.stringify(event.payload ?? {}).slice(0, 120)),
        ),
      );
    }
    return feedEl;
  }

  function detailNode(run) {
    if (run.id !== openRun) return null;
    if (!openDetail) return h("div", { class: "empty" }, "loading…");
    const terminal = openDetail.events.find((event) =>
      ["run.succeeded", "run.failed", "run.cancelled"].includes(event.type),
    );
    const payload = terminal?.payload ?? {};
    const parts = [];
    if (openDetail.error) parts.push(h("div", { style: "color:var(--red); margin-bottom:8px" }, openDetail.error));
    const results = buildResults(payload);
    if (results) parts.push(results);
    const summary = summaryLines(openDetail, payload);
    if (summary.length) parts.push(h("div", { style: "margin-top:10px; display:flex; flex-direction:column; gap:2px" }, ...summary));
    if (!results && !summary.length && !openDetail.error) {
      parts.push(h("div", { class: "dim" }, run.state === "queued" ? "waiting for a worker…" : "no build output recorded"));
    }
    parts.push(timeline(openDetail));
    return h("div", {}, ...parts);
  }

  function renderList() {
    listBody.replaceChildren(
      h(
        "div",
        { class: "card" },
        table(
          [
            { k: "id", label: "#", num: true },
            { k: "flow_selector", label: "models", render: (run) => run.flow_selector.join(", ") || "all" },
            { k: "state", label: "state", render: (run) => statusPill(run.state) },
            { k: "attempts", label: "attempt", num: true, render: (run) => h("span", { class: "dim" }, String(run.attempts)) },
            {
              k: "idempotency_key",
              label: "trigger",
              render: (run) => h("span", { class: "dim" }, run.idempotency_key?.split(":")[0] ?? "—"),
            },
            {
              k: "partition",
              label: "window",
              render: (run) =>
                run.partition ? h("span", { class: "dim" }, `${run.partition[0] ?? ""} → ${run.partition[1] ?? ""}`) : h("span", { class: "faint" }, "—"),
            },
            { k: "enqueued_at", label: "enqueued", render: (run) => h("span", { class: "dim" }, relTime(run.enqueued_at)) },
            {
              k: "_actions",
              label: "",
              render: (run) =>
                ["queued", "running"].includes(run.state)
                  ? h("button", { class: "btn small danger", onclick: (event) => { event.stopPropagation(); cancel(run.id); } }, "cancel")
                  : h("span", {}, ""),
            },
          ],
          runs,
          {
            onRow: (run) => toggleRun(run.id),
            expandRow: detailNode,
            empty: "no runs yet",
            hint: "run… enqueues onto the durable queue; a running scheduler (interlace serve / scheduler) drains it",
          },
        ),
      ),
    );
  }

  function enqueueModal() {
    modal((box, close) => {
      const selectors = h("input", { class: "in", placeholder: "event_totals, +daily_revenue, tag:core" });
      const start = h("input", { class: "in", placeholder: "2026-07-01T00:00:00" });
      const end = h("input", { class: "in", placeholder: "2026-07-02T00:00:00" });
      const restate = h("input", { type: "checkbox" });
      box.append(
        h("h2", {}, "Enqueue a run"),
        h(
          "div",
          { class: "form-grid" },
          h("label", { class: "field wide" }, h("span", {}, "selectors (empty = everything)"), selectors),
          h("label", { class: "field" }, h("span", {}, "window start"), start),
          h("label", { class: "field" }, h("span", {}, "window end"), end),
          h("label", { class: "check wide" }, restate, "restate — reprocess the window even where intervals are already filled"),
        ),
        h(
          "div",
          { class: "actions" },
          h("button", { class: "btn", onclick: close }, "cancel"),
          h(
            "button",
            {
              class: "btn primary",
              onclick: async () => {
                const payload = { restate: restate.checked };
                if (selectors.value.trim()) payload.selectors = selectors.value.split(",").map((s) => s.trim()).filter(Boolean);
                if (start.value.trim()) payload.start = start.value.trim();
                if (end.value.trim()) payload.end = end.value.trim();
                try {
                  const created = await api.post("/runs", payload);
                  toast(created.enqueued ? `enqueued ${created.models.length ? created.models.join(", ") : "all models"}` : "already queued (deduplicated)");
                  close();
                  refresh();
                } catch (error) {
                  toast(error.message, "err");
                }
              },
            },
            "enqueue",
          ),
        ),
      );
    });
  }

  enqueueBtn.addEventListener("click", enqueueModal);
  await refresh();

  const offFeed = feed.on((event) => {
    if (event.type.startsWith("run.")) refresh();
    else if (event.type.startsWith("model.") && openRun !== null && event.payload?.run === openRun) refresh();
  });
  return () => offFeed();
}
