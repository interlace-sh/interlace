// Checks: latest verdict per (model, check) — failures surfaced first, the
// green wall below. Run the whole suite ad hoc from here.

import { dataGrid, debounce, h, latestPerCheck, relTime, statusPill, table } from "../ui.js";

export async function render(el, { api, feed, go, toast }) {
  const runBtn = h("button", { class: "btn primary" }, "run checks");
  const body = h("div", {});

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Checks"),
      h("span", { class: "sub" }, "latest result per model check"),
      h("span", { class: "spread" }),
      runBtn,
    ),
    body,
  );

  const columns = [
    {
      k: "model",
      label: "model",
      render: (row) =>
        h("a", { href: `#/models?m=${encodeURIComponent(row.model)}`, style: "color:var(--violet)", onclick: (event) => { event.preventDefault(); go("models", { m: row.model }); } }, row.model),
    },
    { k: "check_name", label: "check" },
    { k: "check_type", label: "type", render: (row) => h("span", { class: "dim" }, row.check_type) },
    { k: "severity", label: "severity", render: (row) => h("span", { class: "dim" }, row.severity) },
    { k: "status", label: "status", render: (row) => statusPill(row.status) },
    { k: "failures", label: "failures", num: true, render: (row) => (row.failures ? h("span", { style: "color:var(--red)" }, String(row.failures)) : h("span", { class: "faint" }, "0")) },
    { k: "executed_at", label: "when", render: (row) => h("span", { class: "dim" }, relTime(row.executed_at)) },
  ];

  function failingDetail(row) {
    if (row.status === "passed" && !row.message) return null;
    const box = h("div", { style: "display:flex; flex-direction:column; gap:8px" });
    if (row.message) box.append(h("div", { class: "sub", style: "white-space:normal" }, row.message));
    if (row.status === "failed" && row.failures) {
      const sample = h("div", {});
      box.append(
        h(
          "button",
          {
            class: "btn small",
            onclick: async () => {
              sample.replaceChildren(h("div", { class: "dim" }, "loading…"));
              try {
                const body = await api.get(
                  `/models/${encodeURIComponent(row.model)}/checks/${encodeURIComponent(row.check_name)}/rows`,
                );
                sample.replaceChildren(
                  body.available
                    ? dataGrid(body)
                    : h("div", { class: "dim" }, body.message || "no rows"),
                );
              } catch (error) {
                sample.replaceChildren(h("div", { style: "color:var(--red)" }, error.message));
              }
            },
          },
          "show failing rows",
        ),
        sample,
      );
    }
    return box.childNodes.length ? box : null;
  }

  async function refresh() {
    let rows;
    try {
      rows = await api.get("/checks");
    } catch (error) {
      body.replaceChildren(h("div", { class: "empty" }, error.message));
      return;
    }
    const latest = latestPerCheck(rows);
    const failing = latest.filter((row) => row.status !== "passed");
    const passing = latest.filter((row) => row.status === "passed");
    body.replaceChildren();

    if (!latest.length) {
      body.append(
        h("div", { class: "card" }, h("div", { class: "empty" }, "no check results yet — checks run with every apply")),
      );
      return;
    }
    if (failing.length) {
      body.append(
        h(
          "div",
          { class: "card", style: "border-color: color-mix(in srgb, var(--red) 35%, var(--line-soft))" },
          h("div", { class: "card-head", style: "color:var(--red)" }, `failing · ${failing.length}`),
          // surface WHY each check failed, full-width under its row (server `message`)
          table(columns, failing, {
            expandRow: (row) => failingDetail(row),
          }),
        ),
      );
    }
    body.append(
      h(
        "div",
        { class: "card" },
        h("div", { class: "card-head" }, `passing · ${passing.length}`),
        table(columns, passing, { empty: "nothing passing — every check above needs attention" }),
      ),
    );
  }

  async function runChecks() {
    runBtn.disabled = true;
    runBtn.textContent = "running…";
    try {
      const result = await api.post("/checks/run", {});
      let summary = `${result.passed}/${result.outcomes.length} passed`;
      if (result.skipped.length) summary += ` · skipped (not promoted): ${result.skipped.join(", ")}`;
      toast(summary, result.blocking_failures ? "err" : "ok");
      refresh();
    } catch (error) {
      toast(error.message, "err");
    } finally {
      runBtn.disabled = false;
      runBtn.textContent = "run checks";
    }
  }

  runBtn.addEventListener("click", runChecks);
  await refresh();
  const scheduleRefresh = debounce(refresh, 150);
  const offFeed = feed.on((event) => {
    if (["apply.finished", "apply.blocked", "run.succeeded", "run.failed", "reset.finished"].includes(event.type)) {
      scheduleRefresh();
    }
  });
  return () => offFeed();
}
