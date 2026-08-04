// Plan & apply: preview exactly what apply will do — per-change SQL diffs,
// breaking gates, scope/forward-only knobs — then watch the build live
// (the dock mirrors the CLI's ✓/✗ rows; this view also inlines them).

import { diffBlock, h, pill, rowsDelta, seconds, sqlBlock, table } from "../ui.js";

const CATEGORY_TONE = { breaking: "red", non_breaking: "green", forward_only: "amber" };

export async function render(el, { api, toast, modal, go }) {
  const envInput = h("input", {
    class: "in",
    placeholder: "environment",
    title: "target environment (blank = the daemon's default; prod = the unprefixed namespace)",
    style: "width:110px",
  });
  const selectInput = h("input", {
    class: "in",
    placeholder: "selectors: name, +name, name+, tag:x, state:modified",
    style: "width:300px",
  });
  const modifiedBtn = h(
    "button",
    {
      class: "btn small",
      title: "scope to models whose fingerprint drifted from this environment, plus everything downstream",
      onclick: () => {
        selectInput.value = "state:modified+";
        preview();
      },
    },
    "changed only",
  );
  const forwardOnly = h("input", { type: "checkbox" });
  const previewBtn = h("button", { class: "btn" }, "preview");
  const applyBtn = h("button", { class: "btn primary" }, "apply");
  const body = h("div", {});

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Plan"),
      h("span", { class: "sub" }, "what would change, and why"),
      h("span", { class: "spread" }),
      envInput,
      selectInput,
      modifiedBtn,
      h("label", { class: "check" }, forwardOnly, "forward-only"),
      previewBtn,
      applyBtn,
    ),
    body,
  );

  let current = null;

  async function preview() {
    body.replaceChildren(h("div", { class: "empty" }, "planning…"));
    const query = new URLSearchParams();
    if (envInput.value.trim()) query.set("environment", envInput.value.trim());
    if (selectInput.value.trim()) query.set("select", selectInput.value.trim());
    if (forwardOnly.checked) query.set("forward_only", "true");
    try {
      current = await api.get(`/plan${query.toString() ? "?" + query : ""}`);
    } catch (error) {
      body.replaceChildren(h("div", { class: "empty" }, error.message));
      return;
    }
    renderPlan();
  }

  function renderPlan() {
    body.replaceChildren();
    if (!current.changes.length) {
      body.append(
        h("div", { class: "card" }, h("div", { class: "empty" }, `nothing to do — ${current.environment} is up to date`)),
      );
      return;
    }
    const breaking = current.changes.filter((c) => c.category === "breaking").length;
    if (current.transfers.length) {
      body.append(
        h("div", { class: "card" }, h("div", { class: "card-body" }, "cross-engine transfers: ", current.transfers.join(", "))),
      );
    }
    for (const change of current.changes) {
      const headBits = [
        h("strong", {}, change.name),
        pill(change.change_type, change.change_type === "removed" ? "red" : ""),
      ];
      if (change.category) headBits.push(pill(change.category.replace("_", "-"), CATEGORY_TONE[change.category] ?? ""));
      if (change.reused) headBits.push(pill("reused — no rebuild", "violet"));
      if (change.impacted_columns.length) headBits.push(h("span", { class: "sub" }, `+ ${change.impacted_columns.join(", ")}`));

      const card = h("div", { class: "card" });
      const head = h("div", { class: "card-head", style: "text-transform:none; letter-spacing:0; cursor:pointer" }, ...headBits, h("span", { class: "spread" }), h("span", { class: "faint" }, change.new_fingerprint?.slice(0, 8) ?? ""));
      const detail = h("div", { class: "card-body" });
      detail.hidden = true;
      head.addEventListener("click", () => {
        detail.hidden = !detail.hidden;
        if (!detail.hidden && !detail.childNodes.length) {
          if (change.previous_sql && change.new_sql && change.previous_sql !== change.new_sql) {
            detail.append(diffBlock(change.previous_sql, change.new_sql));
          } else if (change.new_sql) {
            detail.append(sqlBlock(change.new_sql));
          } else {
            detail.append(h("div", { class: "sub" }, "python model — source-level change"));
          }
        }
      });
      card.append(head, detail);
      body.append(card);
    }
    if (breaking) {
      body.append(h("div", { class: "card" }, h("div", { class: "card-body", style: "color: var(--amber)" }, `${breaking} breaking change(s) — apply will ask before proceeding`)));
    }
  }

  async function runApply(force = false) {
    applyBtn.disabled = true;
    applyBtn.textContent = "applying…";
    const payload = { force, forward_only: forwardOnly.checked };
    if (envInput.value.trim()) payload.environment = envInput.value.trim();
    if (selectInput.value.trim()) payload.selectors = selectInput.value.split(",").map((s) => s.trim()).filter(Boolean);
    try {
      const result = await api.post("/apply", payload);
      renderResult(result);
      toast(`applied — ${result.built.length} built, ${result.promoted} promoted`, "ok");
    } catch (error) {
      if (error.message.includes("resubmit with force=true") && !force) {
        modal((box, close) => {
          box.append(
            h("h2", {}, "Breaking changes"),
            h("p", { class: "sub" }, error.message),
            h("div", { class: "actions" },
              h("button", { class: "btn", onclick: close }, "cancel"),
              h("button", { class: "btn danger", onclick: () => { close(); runApply(true); } }, "apply anyway"),
            ),
          );
        });
      } else {
        toast(error.message, "err");
      }
    } finally {
      applyBtn.disabled = false;
      applyBtn.textContent = "apply";
    }
  }

  function renderResult(result) {
    const rows = result.built.map((name) => ({
      name,
      rows: result.rows[name],
      time: result.timings[name],
    }));
    body.replaceChildren(
      h(
        "div",
        { class: "card" },
        h("div", { class: "card-head" }, "build results", h("span", { class: "spread" }), `${result.built.length} built · ${result.reused.length} reused · ${result.promoted} promoted`),
        table(
          [
            { k: "name", label: "model" },
            { k: "rows", label: "rows", num: true, render: (row) => rowsDelta(row.rows) },
            { k: "time", label: "time", num: true, render: (row) => h("span", { class: "dim" }, seconds(row.time)) },
          ],
          rows,
          { empty: "nothing was built — everything reused or already current" },
        ),
      ),
    );
    if (result.reused.length) {
      body.append(h("div", { class: "card" }, h("div", { class: "card-body sub" }, "reused without rebuild: ", result.reused.join(", "))));
    }
    body.append(h("div", { class: "card" }, h("div", { class: "card-body sub" }, "preview again to confirm the plan is clean")));
  }

  previewBtn.addEventListener("click", preview);
  applyBtn.addEventListener("click", () => runApply(false));
  await preview();
}
