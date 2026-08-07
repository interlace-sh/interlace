// Runs: the durable queue, live. Clicking a run expands its detail directly under
// the row — a single compact build table (status tick / model / output / strategy /
// engine / depends on / checks / rows / time), a header line (env · duration · models,
// plus the backfill window and attempt when they apply), and the summary line.

import { debounce, glyph, h, relTime, rowsDelta, seconds, statusPill, table } from "../ui.js";

const _MARK = { done: glyph.ok, failed: glyph.fail, cancelled: glyph.skip, skip: glyph.skip };
const _TONE = { done: "glyph-ok", failed: "glyph-fail", cancelled: "glyph-skip", skip: "glyph-skip" };

/** Join nodes with a dim "·" separator. */
function dotted(...bits) {
  const kept = bits.filter(Boolean);
  const wrap = h("span", { class: "run-meta" });
  kept.forEach((bit, index) => {
    if (index) wrap.append(h("span", { class: "faint" }, " · "));
    wrap.append(bit);
  });
  return wrap;
}

export async function render(el, { api, feed, toast, modal, params }) {
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
    h(
      "div",
      { class: "sub", style: "margin:-8px 0 12px; color:var(--tx-faint)" },
      "ad-hoc applies (interlace apply) aren't queued — they appear in the overview activity feed",
    ),
    listBody,
  );

  let runs = [];
  let catalog = new Map(); // model name -> ModelInfo (output/strategy/engine/depends_on/has_checks)
  let daemonEnv = null; // fallback env for runs not yet succeeded (all runs build into it)
  let openRun = params.r ? Number(params.r) : null;
  let openDetail = null; // fetched RunDetail for openRun
  let refreshSeq = 0; // only the newest refresh may repaint — feed storms race otherwise

  async function refresh() {
    const mine = ++refreshSeq;
    const targetRun = openRun;
    let runList;
    let models;
    let health;
    let detail = null;
    try {
      [runList, models, health] = await Promise.all([
        api.get("/runs"),
        catalog.size ? null : api.get("/models"),
        daemonEnv ? null : api.get("/health").catch(() => null),
      ]);
      if (targetRun !== null) detail = await api.get(`/runs/${targetRun}`).catch(() => null);
    } catch {
      return; // daemon hiccup: keep the last good list rather than blanking or throwing
    }
    if (mine !== refreshSeq) return; // a newer refresh already landed — don't repaint stale data
    runs = runList;
    if (models) catalog = new Map(models.map((m) => [m.name, m]));
    if (health) daemonEnv = health.environment;
    openDetail = targetRun !== null ? detail : null;
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

  // ---- the expanded detail: one compact build table, header, summary -------------

  /** name -> terminal model.* phase (done/failed/cancelled), for the status tick. */
  function phaseMap(events) {
    const phases = new Map();
    for (const event of events) {
      if (!event.type.startsWith("model.")) continue;
      const phase = event.type.slice(6);
      if (phase !== "start") phases.set(event.entity, phase);
    }
    return phases;
  }

  /** The per-model checks cell: — none declared, ✓ passed, ✗N some failed. */
  function checkCell(name, payload) {
    if (!catalog.get(name)?.has_checks) return h("span", { class: "faint" }, "—");
    const failed = (payload.checks?.failing ?? []).filter((c) => c.startsWith(`${name}.`)).length;
    return failed
      ? h("span", { class: "glyph-fail" }, `${glyph.fail} ${failed}`)
      : h("span", { class: "glyph-ok" }, glyph.ok);
  }

  function buildTable(payload, events) {
    const phases = phaseMap(events);
    const built = payload.built ?? [];
    const names = built.length ? built : [...phases.keys()];
    const gated = (payload.gated ?? []).filter((name) => !names.includes(name));
    if (!names.length && !gated.length) return null;
    const rows = [
      ...names.map((name) => ({ name, phase: phases.get(name) ?? "done" })),
      ...gated.map((name) => ({ name, phase: "skip", gated: true })),
    ].map((row) => {
      const info = catalog.get(row.name) ?? {};
      return {
        ...row,
        output: info.output ?? "—",
        strategy: info.strategy ?? "—",
        engine: info.engine ?? "default",
        deps: (info.depends_on ?? []).join(", "),
        rows: payload.rows?.[row.name],
        time: payload.timings?.[row.name],
      };
    });
    const dim = (text) => h("span", { class: "dim" }, text);
    return table(
      [
        {
          k: "phase",
          label: "",
          render: (row) => h("span", { class: `tl-mark ${_TONE[row.phase] ?? "glyph-ok"}` }, _MARK[row.phase] ?? glyph.ok),
        },
        { k: "name", label: "model" },
        { k: "output", label: "output", render: (row) => dim(row.output) },
        { k: "strategy", label: "strategy", render: (row) => dim(row.strategy) },
        { k: "engine", label: "engine", render: (row) => dim(row.engine) },
        { k: "deps", label: "depends on", render: (row) => dim(row.deps || "—") },
        { k: "checks", label: "checks", render: (row) => (row.gated ? h("span", { class: "faint" }, "gated") : checkCell(row.name, payload)) },
        { k: "rows", label: "rows", num: true, render: (row) => rowsDelta(row.rows) },
        { k: "time", label: "time", num: true, render: (row) => dim(seconds(row.time)) },
      ],
      rows,
      { class: "compact" },
    );
  }

  function detailHeader(run, payload) {
    const built = (payload.built ?? []).length;
    const env = run.environment ?? daemonEnv;
    return dotted(
      env ? h("span", { class: "dim" }, env) : null,
      run.duration != null ? h("span", { class: "dim" }, seconds(run.duration)) : null,
      built ? h("span", { class: "dim" }, `${built} model${built === 1 ? "" : "s"}`) : null,
      // the backfill window: the partition an incremental model (re)processes — empty
      // for an ordinary full run, so it only shows when one was actually requested
      run.partition ? h("span", { class: "dim" }, `window ${run.partition[0] ?? ""} → ${run.partition[1] ?? ""}`) : null,
      run.attempts > 1 ? h("span", { class: "dim" }, `attempt ${run.attempts}`) : null,
    );
  }

  function summaryLine(payload) {
    if (!payload.checks?.total) return null;
    const warned = payload.checks.failing ?? [];
    return h(
      "div",
      { class: "sub", style: warned.length ? "color:var(--amber)" : "" },
      `Checks: ${payload.checks.passed}/${payload.checks.total} passed`,
      warned.length ? ` — ${warned.join(", ")}` : "",
    );
  }

  function detailNode(run) {
    if (run.id !== openRun) return null;
    if (!openDetail) return h("div", { class: "empty" }, "loading…");
    const terminal = openDetail.events.find((event) =>
      ["run.succeeded", "run.failed", "run.cancelled"].includes(event.type),
    );
    const payload = terminal?.payload ?? {};
    const parts = [detailHeader(run, payload)];
    if (openDetail.error) parts.push(h("div", { style: "color:var(--red); margin:6px 0" }, openDetail.error));
    const built = buildTable(payload, openDetail.events);
    if (built) parts.push(built);
    const summary = summaryLine(payload);
    if (summary) parts.push(summary);
    if (!built && !summary && !openDetail.error) {
      parts.push(h("div", { class: "dim" }, run.state === "queued" ? "waiting for a worker…" : "no build output recorded"));
    }
    return h("div", { class: "run-detail" }, ...parts);
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
            {
              k: "environment",
              label: "env",
              render: (run) => h("span", { class: "dim" }, run.environment ?? daemonEnv ?? "—"),
            },
            {
              k: "idempotency_key",
              label: "trigger",
              render: (run) => h("span", { class: "dim" }, run.idempotency_key?.split(":")[0] ?? "—"),
            },
            { k: "duration", label: "duration", num: true, render: (run) => h("span", { class: "dim" }, seconds(run.duration)) },
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

  // collapse feed storms (a busy build emits many model.* events) into one refetch
  const scheduleRefresh = debounce(refresh, 150);
  const offFeed = feed.on((event) => {
    if (event.type.startsWith("run.")) scheduleRefresh();
    else if (event.type.startsWith("model.") && openRun !== null && event.payload?.run === openRun) scheduleRefresh();
  });
  return () => offFeed();
}
