// Runs: the durable queue, live. List updates as events land; a run's detail
// shows its lifecycle events with per-model build rows (rows moved, timings)
// exactly as the CLI would print them.

import { clock, count, glyph, h, pill, relTime, rowsDelta, seconds, statusPill, table } from "../ui.js";

export async function render(el, { api, feed, go, toast, modal, params }) {
  const listBody = h("div", {});
  const detailBody = h("div", {});
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
    detailBody,
  );

  let runs = [];
  let openRun = params.r ? Number(params.r) : null;

  async function refresh() {
    runs = await api.get("/runs");
    renderList();
    if (openRun !== null) renderDetail();
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
            onRow: (run) => {
              openRun = run.id;
              history.replaceState(null, "", `#/runs?r=${run.id}`); // deep-linkable without a re-render
              renderDetail();
            },
            empty: "no runs yet",
            hint: "run… enqueues onto the durable queue; a running scheduler (interlace serve / scheduler) drains it",
          },
        ),
      ),
    );
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

  async function renderDetail() {
    let detail;
    try {
      detail = await api.get(`/runs/${openRun}`);
    } catch {
      detailBody.replaceChildren();
      return;
    }
    const terminal = detail.events.find((event) => ["run.succeeded", "run.failed", "run.cancelled"].includes(event.type));
    const payload = terminal?.payload ?? {};
    const buildRows = Object.keys(payload.timings ?? {}).map((name) => ({
      name,
      rows: payload.rows?.[name],
      time: payload.timings?.[name],
    }));

    const card = h(
      "div",
      { class: "card" },
      h(
        "div",
        { class: "card-head", style: "text-transform:none; letter-spacing:0" },
        h("strong", {}, `run #${detail.id}`),
        statusPill(detail.state),
        detail.restate ? pill("restate", "amber") : null,
        h("span", { class: "spread" }),
        detail.error ? h("span", { style: "color:var(--red)" }, detail.error) : null,
      ),
    );

    if (buildRows.length) {
      card.append(
        table(
          [
            { k: "name", label: "model" },
            { k: "rows", label: "rows", num: true, render: (row) => rowsDelta(row.rows) },
            { k: "time", label: "time", num: true, render: (row) => h("span", { class: "dim" }, seconds(row.time)) },
          ],
          buildRows,
        ),
      );
    }

    const timeline = h("div", { class: "feed" });
    for (const event of detail.events) {
      const isModel = event.type.startsWith("model.");
      const mark = { "model.done": glyph.ok, "model.failed": glyph.fail, "model.cancelled": glyph.skip, "model.start": "▸" }[event.type];
      timeline.append(
        h(
          "div",
          { class: "feed-row" },
          h("span", { class: "ts" }, clock(event.ts)),
          h("span", { class: "ty" }, isModel && mark ? `${mark} ${event.type}` : event.type),
          h("span", { class: "en" }, isModel ? event.entity : JSON.stringify(event.payload ?? {}).slice(0, 120)),
        ),
      );
    }
    card.append(h("div", { class: "card-head" }, "timeline"), timeline);
    detailBody.replaceChildren(card);
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
    else if (event.type.startsWith("model.") && openRun !== null && event.payload?.run === openRun) renderDetail();
  });
  return () => offFeed();
}
