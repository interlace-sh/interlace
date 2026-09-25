// Plan & apply: preview exactly what apply will do — per-change SQL diffs,
// breaking gates, scope/forward-only knobs — then apply. Live build progress is
// narrated by the global dock (the CLI's ✓/✗ rows); this view renders the final
// build results when apply returns.

import { diffBlock, h, pill, rowsDelta, seconds, sqlBlock, table } from "../ui.js";

const CATEGORY_TONE = { breaking: "red", non_breaking: "green", forward_only: "amber" };

export async function render(el, { api, toast, modal }) {
  const envSelect = h("select", {
    class: "in",
    "aria-label": "environment",
    title: "target environment. prod is the unprefixed namespace",
    style: "width:140px",
  });
  const picked = new Set();
  let upstream = false;
  let downstream = false;
  let changedOnly = false;
  let models = [];

  function token(name) {
    return `${upstream ? "+" : ""}${name}${downstream ? "+" : ""}`;
  }

  function currentSelectors() {
    if (changedOnly) return ["state:modified+"];
    return models.filter((model) => picked.has(model.name)).map((model) => token(model.name));
  }

  const pickBtn = h("button", {
    class: "btn",
    type: "button",
    "aria-haspopup": "listbox",
    "aria-expanded": "false",
    title: "models to plan. empty means every model",
  }, "all models");
  const filter = h("input", { class: "in", placeholder: "filter models", "aria-label": "filter models" });
  const modelList = h("div", { class: "plan-models", role: "listbox", "aria-label": "models", "aria-multiselectable": "true" });
  const menu = h("div", { class: "plan-menu", hidden: true }, filter, modelList);
  const pick = h("div", { class: "plan-pick" }, pickBtn, menu);

  const upstreamBtn = h("button", {
    class: "btn small",
    type: "button",
    "aria-pressed": "false",
    title: "include ancestors of each selected model (+model)",
  }, "upstream");
  const downstreamBtn = h("button", {
    class: "btn small",
    type: "button",
    "aria-pressed": "false",
    title: "include descendants of each selected model (model+)",
  }, "downstream");
  const modifiedBtn = h("button", {
    class: "btn small",
    type: "button",
    "aria-pressed": "false",
    title: "models whose fingerprint drifted from this environment, plus everything downstream (state:modified+)",
  }, "changed only");
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
      envSelect,
      pick,
      upstreamBtn,
      downstreamBtn,
      modifiedBtn,
      h("label", { class: "check" }, forwardOnly, "forward-only"),
      previewBtn,
      applyBtn,
    ),
    body,
  );

  let current = null;
  let previewSeq = 0;

  async function preview() {
    const mine = ++previewSeq;
    body.replaceChildren(h("div", { class: "empty" }, "planning…"));
    const query = new URLSearchParams();
    if (envSelect.value) query.set("environment", envSelect.value);
    const selectors = currentSelectors();
    if (selectors.length) query.set("select", selectors.join(","));
    if (forwardOnly.checked) query.set("forward_only", "true");
    try {
      current = await api.get(`/plan${query.toString() ? "?" + query : ""}`);
    } catch (error) {
      if (mine === previewSeq) body.replaceChildren(h("div", { class: "empty" }, error.message));
      return;
    }
    if (mine !== previewSeq) return;
    renderPlan();
  }

  function renderPlan() {
    body.replaceChildren();
    if (!current.changes.length && !(current.physical || []).length && !(current.drift || []).length) {
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
    const physical = current.physical || [];
    const drift = current.drift || [];
    if (physical.length || drift.length) {
      const parts = [];
      if (physical.length) {
        parts.push(
          h("div", { class: "card-head" }, "indexes and constraints"),
          h("div", { class: "card-body" }, ...physical.map((line) => h("div", {}, line))),
        );
      }
      if (drift.length) {
        parts.push(
          h("div", { class: "card-head" }, "drift"),
          h("div", { class: "card-body" }, ...drift.map((line) => h("div", { class: "sub" }, line))),
        );
      }
      body.append(h("div", { class: "card" }, ...parts));
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
      const detail = h("div", { class: "card-body" });
      detail.hidden = true;
      const head = h(
        "button",
        { class: "card-head", style: "text-transform:none; letter-spacing:0; width:100%; text-align:left", "aria-expanded": "false" },
        ...headBits,
        h("span", { class: "spread" }),
        h("span", { class: "faint" }, change.new_fingerprint?.slice(0, 8) ?? ""),
      );
      head.addEventListener("click", () => {
        detail.hidden = !detail.hidden;
        head.setAttribute("aria-expanded", String(!detail.hidden));
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
    if (envSelect.value) payload.environment = envSelect.value;
    const selectors = currentSelectors();
    if (selectors.length) payload.selectors = selectors;
    try {
      const result = await api.post("/apply", payload);
      renderResult(result);
      toast(`applied — ${result.built.length} built, ${result.promoted} promoted`, "ok");
    } catch (error) {
      if (error.status === 409 && !force) {
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
        if (error.statement) {
          body.prepend(
            h(
              "div",
              { class: "card", style: "margin-bottom:12px" },
              h("div", { class: "card-head", style: "color:var(--red)" }, "failed statement"),
              h("div", { class: "card-body" }, sqlBlock(error.statement)),
            ),
          );
        }
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

  function syncScope() {
    const selectors = currentSelectors();
    pickBtn.textContent = !selectors.length ? "all models" : selectors.length === 1 ? selectors[0] : `${selectors.length} models`;
    pickBtn.title = selectors.join(", ") || "every model";
    for (const [button, on] of [[upstreamBtn, upstream], [downstreamBtn, downstream], [modifiedBtn, changedOnly]]) {
      button.classList.toggle("on", on);
      button.setAttribute("aria-pressed", String(on));
    }
    upstreamBtn.disabled = changedOnly;
    downstreamBtn.disabled = changedOnly;
  }

  function paintModels() {
    const needle = filter.value.trim().toLowerCase();
    const rows = models
      .filter((model) => !needle || model.name.toLowerCase().includes(needle))
      .map((model) => {
        const box = h("input", { type: "checkbox", checked: picked.has(model.name) });
        box.addEventListener("change", () => {
          if (box.checked) picked.add(model.name);
          else picked.delete(model.name);
          changedOnly = false;
          syncScope();
          preview();
        });
        return h("label", {}, box, model.name);
      });
    modelList.replaceChildren(...(rows.length ? rows : [h("div", { class: "empty" }, "no models")]));
  }

  function closeMenu() {
    menu.hidden = true;
    pickBtn.setAttribute("aria-expanded", "false");
  }

  function onDocClick(event) {
    if (!pick.contains(event.target)) closeMenu();
  }

  function onKey(event) {
    if (event.key === "Escape") closeMenu();
  }

  pickBtn.addEventListener("click", () => {
    menu.hidden = !menu.hidden;
    pickBtn.setAttribute("aria-expanded", String(!menu.hidden));
    if (!menu.hidden) filter.focus();
  });
  filter.addEventListener("input", paintModels);
  upstreamBtn.addEventListener("click", () => {
    if (changedOnly) return;
    upstream = !upstream;
    syncScope();
    if (picked.size) preview();
  });
  downstreamBtn.addEventListener("click", () => {
    if (changedOnly) return;
    downstream = !downstream;
    syncScope();
    if (picked.size) preview();
  });
  modifiedBtn.addEventListener("click", () => {
    changedOnly = !changedOnly;
    if (changedOnly) {
      picked.clear();
      upstream = false;
      downstream = false;
      paintModels();
    }
    syncScope();
    closeMenu();
    preview();
  });
  envSelect.addEventListener("change", preview);
  document.addEventListener("click", onDocClick);
  document.addEventListener("keydown", onKey);

  previewBtn.addEventListener("click", preview);
  applyBtn.addEventListener("click", () => runApply(false));

  try {
    const [health, envs, modelList] = await Promise.all([
      api.get("/health"),
      api.get("/environments").catch(() => []),
      api.get("/models"),
    ]);
    models = modelList;
    const names = [...new Set([health.environment, ...envs.map((env) => env.name)])].filter(Boolean);
    envSelect.replaceChildren(
      ...names.map((name) => h("option", { value: name, selected: name === health.environment }, name)),
    );
  } catch (error) {
    body.replaceChildren(h("div", { class: "empty" }, error.message));
    return () => {
      document.removeEventListener("click", onDocClick);
      document.removeEventListener("keydown", onKey);
    };
  }
  paintModels();
  syncScope();
  await preview();
  return () => {
    document.removeEventListener("click", onDocClick);
    document.removeEventListener("keydown", onKey);
  };
}
