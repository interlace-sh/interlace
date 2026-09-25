// Models: the compiled catalog at #/models. Selecting a row opens that model
// on its own page (#/models?m=) — column lineage, graph neighbours, source,
// latest checks — and act from there: trace it, run it, query it.

import {
  copy,
  debounce,
  h,
  latestPerCheck,
  pill,
  previewPanel,
  pythonBlock,
  relTime,
  sqlBlock,
  statusPill,
  table,
} from "../ui.js";

const OUTPUT_TONE = { sink: "cyan", view: "violet" };

export async function render(el, ctx) {
  if (ctx.params.m) return renderModel(el, ctx);
  return renderCatalog(el, ctx);
}

async function renderCatalog(el, { api, go }) {
  const filter = h("input", { class: "in", placeholder: "filter by name or tag", style: "width:240px" });
  const countLabel = h("span", { class: "sub" });
  const tableWrap = h("div", {});

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Models"),
      h("span", { class: "sub" }, "the compiled catalog"),
      countLabel,
      h("span", { class: "spread" }),
      filter,
    ),
    h("div", { class: "card" }, tableWrap),
  );

  const models = await api.get("/models");

  function matches(model, needle) {
    if (!needle) return true;
    if (model.name.toLowerCase().includes(needle)) return true;
    return model.tags.some((tag) => tag.toLowerCase().includes(needle));
  }

  function renderList() {
    const needle = filter.value.trim().toLowerCase();
    const rows = models.filter((model) => matches(model, needle));
    countLabel.textContent = needle ? `${rows.length} of ${models.length}` : `${models.length} models`;
    tableWrap.replaceChildren(
      table(
        [
          { k: "name", label: "model" },
          {
            k: "language",
            label: "lang",
            render: (m) => pill(m.language === "python" ? "py" : "sql", m.language === "python" ? "amber" : ""),
          },
          { k: "output", label: "output", render: (m) => pill(m.output, OUTPUT_TONE[m.output] ?? "") },
          { k: "strategy", label: "strategy", render: (m) => h("span", { class: "dim" }, m.strategy) },
          {
            k: "engine",
            label: "engine",
            // the payload may omit engine; the default engine stays quiet either way
            render: (m) => (m.engine && m.engine !== "default" ? h("span", { class: "dim" }, m.engine) : h("span", {}, "")),
          },
          {
            k: "tags",
            label: "tags",
            render: (m) => (m.tags.length ? h("span", { class: "dim" }, m.tags.join(", ")) : h("span", { class: "faint" }, "—")),
          },
          {
            k: "schedule",
            label: "",
            render: (m) =>
              m.schedule
                ? h("span", { title: Object.entries(m.schedule).map(([k, v]) => `${k}: ${v}`).join(", ") }, "⏱")
                : h("span", {}, ""),
          },
        ],
        rows,
        {
          onRow: (m) => go("models", { m: m.name }),
          empty: "no models match",
          hint: "the filter checks names and tags",
        },
      ),
    );
  }

  filter.addEventListener("input", debounce(renderList, 120));
  renderList();
}

async function renderModel(el, { api, go, toast, modal, params }) {
  const name = params.m;
  el.append(
    h(
      "div",
      { class: "view-head" },
      catalogLink(go),
      h("span", { class: "faint" }, "/"),
      h("h1", {}, name),
    ),
    h("div", { class: "empty" }, "loading…"),
  );
  let detail;
  let checkRows = [];
  try {
    [detail, checkRows] = await Promise.all([
      api.get(`/models/${encodeURIComponent(name)}`),
      api.get(`/checks?model=${encodeURIComponent(name)}`).catch(() => []),
    ]);
  } catch (error) {
    el.replaceChildren(
      h(
        "div",
        { class: "view-head" },
        catalogLink(go),
        h("span", { class: "faint" }, "/"),
        h("h1", {}, name),
      ),
      h("div", { class: "empty" }, error.message),
    );
    return;
  }
  const modelNames = new Set([...detail.depends_on, ...detail.upstream, ...detail.downstream]);
  const previewCard = h("div", { class: "card" }, h("div", { class: "card-head" }, "preview"), h("div", { class: "empty" }, "loading…"));
  el.replaceChildren(pageHead(detail, { go, toast, api }), ...detailCards(detail, checkRows, { api, go, toast, modal, modelNames }), previewCard);
  try {
    const preview = await api.get(`/models/${encodeURIComponent(name)}/preview`);
    previewCard.replaceWith(previewPanel(preview, { name }));
  } catch (error) {
    previewCard.replaceChildren(h("div", { class: "empty" }, error.message));
  }
}

function catalogLink(go) {
  return h(
    "a",
    {
      href: "#/models",
      class: "sub",
      onclick: (event) => {
        event.preventDefault();
        go("models");
      },
    },
    "models",
  );
}

function pageHead(detail, { go, toast, api }) {
  const schedule = detail.schedule
    ? Object.entries(detail.schedule).map(([key, value]) => `${key}: ${value}`).join(", ")
    : "";
  return h(
    "div",
    { class: "view-head" },
    catalogLink(go),
    h("span", { class: "faint" }, "/"),
    h("h1", {}, detail.name),
    pill(detail.language === "python" ? "py" : "sql", detail.language === "python" ? "amber" : ""),
    pill(detail.output, OUTPUT_TONE[detail.output] ?? ""),
    detail.is_terminal ? pill("terminal", "cyan") : null,
    h("span", { class: "dim" }, detail.strategy),
    detail.engine && detail.engine !== "default" ? h("span", { class: "dim" }, detail.engine) : null,
    schedule ? h("span", { class: "sub", title: schedule }, "⏱") : null,
    detail.owner ? h("span", { class: "sub" }, detail.owner) : null,
    h(
      "span",
      {
        class: "faint",
        style: "cursor:pointer",
        title: "copy full fingerprint",
        onclick: () => copy(detail.fingerprint, toast),
      },
      detail.fingerprint.slice(0, 12),
    ),
    h("span", { class: "spread" }),
    h("button", { class: "btn small", onclick: () => go("lineage", { m: detail.name }) }, "trace in lineage"),
    h("button", { class: "btn small", onclick: () => enqueue(api, toast, detail.name) }, "run"),
    h(
      "button",
      { class: "btn small", onclick: () => go("query", { sql: `SELECT * FROM ${detail.name} LIMIT 100` }) },
      "query",
    ),
  );
}

function modelLink(go, name) {
  return h(
    "a",
    {
      href: `#/models?m=${encodeURIComponent(name)}`,
      style: "color:var(--violet)",
      onclick: (event) => {
        event.preventDefault();
        go("models", { m: name });
      },
    },
    name,
  );
}

function joinLinks(go, names) {
  const wrap = h("span", {});
  names.forEach((name, index) => {
    if (index) wrap.append(h("span", { style: "color:var(--tx-faint)" }, ", "));
    wrap.append(modelLink(go, name));
  });
  return wrap;
}

function impactModal(api, go, modal, model, column) {
  modal(async (box, close) => {
    box.append(h("h2", {}, `Impact of ${model}.${column}`), h("div", { class: "empty" }, "tracing…"));
    let result;
    try {
      result = await api.get(`/models/${encodeURIComponent(model)}/impact?column=${encodeURIComponent(column)}`);
    } catch (error) {
      box.lastChild.replaceWith(h("div", { class: "empty" }, error.message));
      return;
    }
    const nodes = [];
    nodes.push(
      h("p", { class: "sub", style: "margin-bottom:10px" }, "every downstream column derived from this one, transitively"),
      result.impacted.length
        ? table(
            [
              { k: "model", label: "model", render: (row) => modelLink(go, row.model) },
              { k: "column", label: "column" },
              { k: "via", label: "via", render: (row) => h("span", { class: "dim" }, row.via) },
            ],
            result.impacted,
          )
        : h("div", { class: "empty" }, "nothing downstream reads this column"),
    );
    if (result.opaque_consumers.length) {
      nodes.push(
        h(
          "p",
          { class: "sub", style: "margin-top:10px; color:var(--amber)" },
          "opaque consumers (see every column): ",
          result.opaque_consumers.join(", "),
        ),
      );
    }
    nodes.push(h("div", { class: "actions" }, h("button", { class: "btn", onclick: close }, "close")));
    box.replaceChildren(h("h2", {}, `Impact of ${model}.${column}`), ...nodes);
  });
}

async function enqueue(api, toast, name) {
  try {
    const created = await api.post("/runs", { selectors: [name] });
    toast(created.enqueued ? `enqueued ${created.models.join(", ")}` : "already queued (deduplicated)", "ok");
  } catch (error) {
    toast(error.message, "err");
  }
}

function detailCards(detail, checkRows, { api, go, modal, modelNames }) {
  const cards = [];

  // columns + their upstream sources + a per-column impact (blast-radius) action
    const columnRows = Object.entries(detail.columns).map(([column, sources]) => ({ column, sources }));
    const columnsTable = table(
      [
        { k: "column", label: "column" },
        {
          k: "sources",
          label: "sources",
          render: (row) => {
            if (!row.sources.length) return h("span", { class: "faint" }, "—");
            const cell = h("span", {});
            row.sources.forEach((source, index) => {
              if (index) cell.append(" ");
              const dot = source.lastIndexOf(".");
              const upmodel = dot > 0 ? source.slice(0, dot) : source;
              // only a real model is navigable; an external table or a stream source
              // (streams.<name>.<col>) has no model page — render it as plain text
              cell.append(
                modelNames.has(upmodel)
                  ? h(
                      "a",
                      {
                        class: "dim",
                        href: `#/models?m=${encodeURIComponent(upmodel)}`,
                        onclick: (event) => {
                          event.preventDefault();
                          go("models", { m: upmodel });
                        },
                      },
                      source,
                    )
                  : h("span", { class: "faint" }, source),
              );
            });
            return cell;
          },
        },
        {
          k: "impact",
          label: "",
          render: (row) =>
            h(
              "button",
              { class: "btn small", onclick: () => impactModal(api, go, modal, detail.name, row.column) },
              "impact",
            ),
        },
      ],
      columnRows,
      { empty: "no column lineage", hint: "column-level lineage comes from parsed SQL" },
    );

    cards.push(h("div", { class: "card" }, h("div", { class: "card-head" }, "columns"), columnsTable));

    // graph neighbours
    const neighbourRow = (label, names, none) =>
      h(
        "div",
        { style: "display:flex; gap:12px; align-items:baseline; padding:2px 0" },
        h("span", { style: "color:var(--tx-faint); width:90px; flex-shrink:0" }, label),
        names.length ? joinLinks(go, names) : h("span", { style: "color:var(--tx-faint)" }, none),
      );
    const indexes = detail.indexes || [];
    const constraints = detail.constraints || [];
    if (indexes.length || constraints.length || detail.is_terminal) {
      const lines = [
        ...indexes.map((index) =>
          h(
            "div",
            {},
            h("span", { class: "dim" }, index.unique ? "unique index" : "index"),
            " ",
            index.name,
            h("span", { class: "faint" }, `  ${index.columns.join(", ")}`),
          ),
        ),
        ...constraints.map((constraint) => {
          const extra = constraint.expression
            ? constraint.expression
            : constraint.reference
              ? `${constraint.columns.join(", ")} → ${constraint.reference}`
              : constraint.columns.join(", ");
          return h(
            "div",
            {},
            h("span", { class: "dim" }, constraint.type.replace("_", " ")),
            " ",
            constraint.name,
            extra ? h("span", { class: "faint" }, `  ${extra}`) : null,
          );
        }),
      ];
      if (detail.is_terminal && detail.schema) {
        const policy = detail.schema;
        lines.push(
          h(
            "div",
            { class: "sub", style: "margin-top:6px" },
            `schema: columns ${policy.columns} · indexes ${policy.indexes} · constraints ${policy.constraints}`,
          ),
        );
      }
      cards.push(
        h(
          "div",
          { class: "card" },
          h("div", { class: "card-head" }, "indexes and constraints"),
          h("div", { class: "card-body" }, ...lines),
        ),
      );
    }

    cards.push(
      h(
        "div",
        { class: "card" },
        h("div", { class: "card-head" }, "graph"),
        h(
          "div",
          { class: "card-body" },
          neighbourRow("upstream", detail.upstream, "none — reads sources directly"),
          neighbourRow("downstream", detail.downstream, "none — nothing depends on it"),
        ),
      ),
    );

    // definition: canonical SQL, or the Python function's source
    cards.push(
      h(
        "div",
        { class: "card" },
        h("div", { class: "card-head" }, detail.language === "python" ? "python source" : "sql"),
        detail.sql
          ? h("div", { class: "card-body" }, sqlBlock(detail.sql))
          : detail.source
            ? h("div", { class: "card-body" }, pythonBlock(detail.source))
            : h("div", { class: "empty" }, "python model", h("div", { class: "hint" }, "source unavailable in this session")),
      ),
    );

    // latest check results — one row per check (dedup by max id), newest first
    const recent = latestPerCheck(checkRows)
      .sort((a, b) => (b.executed_at || "").localeCompare(a.executed_at || ""))
      .slice(0, 10);
    cards.push(
      h(
        "div",
        { class: "card" },
        h("div", { class: "card-head" }, "latest checks"),
        table(
          [
            { k: "check_name", label: "check" },
            { k: "status", label: "status", render: (row) => statusPill(row.status) },
            {
              k: "failures",
              label: "failures",
              num: true,
              render: (row) =>
                row.failures ? h("span", { style: "color:var(--red)" }, String(row.failures)) : h("span", { class: "dim" }, "0"),
            },
            { k: "executed_at", label: "when", render: (row) => h("span", { class: "dim" }, relTime(row.executed_at)) },
          ],
          recent,
          {
            empty: "no check results yet",
            hint: "run checks from the checks view",
            expandRow: (row) => (row.message ? h("div", { class: "sub", style: "white-space:normal" }, row.message) : null),
          },
        ),
      ),
    );

  return cards;
}
