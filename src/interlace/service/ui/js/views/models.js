// Models: the compiled catalog. Filter the table, open a model's detail below —
// column lineage, graph neighbours, canonical SQL, latest check results — and
// act from there: trace it, run it, query it.

import { copy, debounce, h, pill, relTime, sqlBlock, statusPill, table } from "../ui.js";

const OUTPUT_TONE = { sink: "cyan", view: "violet" };

export async function render(el, { api, go, toast, params }) {
  const filter = h("input", { class: "in", placeholder: "filter by name or tag", style: "width:240px" });
  const countLabel = h("span", { class: "sub" });
  const tableWrap = h("div", {});
  const detailBody = h("div", {});

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
    detailBody,
  );

  const models = await api.get("/models");
  let detailSeq = 0;

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
          onRow: (m) => openDetail(m.name),
          empty: "no models match",
          hint: "the filter checks names and tags",
        },
      ),
    );
  }

  // Opens detail below the table. The hash is updated via replaceState so the
  // router does not re-render — the filter and scroll position stay put.
  async function openDetail(name, { scroll = false } = {}) {
    const seq = ++detailSeq;
    history.replaceState(null, "", `#/models?m=${encodeURIComponent(name)}`);
    detailBody.replaceChildren(h("div", { class: "empty" }, "loading…"));
    let detail;
    let checkRows = [];
    try {
      [detail, checkRows] = await Promise.all([
        api.get(`/models/${encodeURIComponent(name)}`),
        api.get(`/checks?model=${encodeURIComponent(name)}`).catch(() => []),
      ]);
    } catch (error) {
      if (seq === detailSeq) detailBody.replaceChildren(h("div", { class: "empty" }, error.message));
      return;
    }
    if (seq !== detailSeq) return;
    detailBody.replaceChildren(...detailCards(detail, checkRows));
    if (scroll) detailBody.scrollIntoView({ block: "start" });
  }

  function modelLink(name) {
    return h(
      "span",
      { style: "color:var(--violet); cursor:pointer", onclick: () => go("models", { m: name }) },
      name,
    );
  }

  function joinLinks(names) {
    const wrap = h("span", {});
    names.forEach((name, index) => {
      if (index) wrap.append(h("span", { style: "color:var(--tx-faint)" }, ", "));
      wrap.append(modelLink(name));
    });
    return wrap;
  }

  async function enqueue(name) {
    try {
      const created = await api.post("/runs", { selectors: [name] });
      toast(created.enqueued ? `enqueued ${created.models.join(", ")}` : "already queued (deduplicated)", "ok");
    } catch (error) {
      toast(error.message, "err");
    }
  }

  function detailCards(detail, checkRows) {
    const cards = [];

    // head: identity + actions
    const head = h(
      "div",
      { class: "card-head", style: "text-transform:none; letter-spacing:0" },
      h("strong", {}, detail.name),
      pill(detail.output, OUTPUT_TONE[detail.output] ?? ""),
      h(
        "span",
        {
          class: "faint",
          style: "color:var(--tx-faint); cursor:pointer",
          title: "copy full fingerprint",
          onclick: () => copy(detail.fingerprint, toast),
        },
        detail.fingerprint.slice(0, 12),
      ),
      h("span", { class: "spread" }),
      h("button", { class: "btn small", onclick: () => go("lineage", { m: detail.name }) }, "trace in lineage"),
      h("button", { class: "btn small", onclick: () => enqueue(detail.name) }, "run"),
      h(
        "button",
        { class: "btn small", onclick: () => go("query", { sql: `SELECT * FROM ${detail.name} LIMIT 100` }) },
        "query",
      ),
    );

    // columns + their upstream sources
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
              cell.append(
                h(
                  "span",
                  { class: "dim", style: "cursor:pointer", onclick: () => go("models", { m: upmodel }) },
                  source,
                ),
              );
            });
            return cell;
          },
        },
      ],
      columnRows,
      { empty: "no column lineage", hint: "column-level lineage comes from parsed SQL" },
    );

    cards.push(h("div", { class: "card" }, head, columnsTable));

    // graph neighbours
    const neighbourRow = (label, names, none) =>
      h(
        "div",
        { style: "display:flex; gap:12px; align-items:baseline; padding:2px 0" },
        h("span", { style: "color:var(--tx-faint); width:90px; flex-shrink:0" }, label),
        names.length ? joinLinks(names) : h("span", { style: "color:var(--tx-faint)" }, none),
      );
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

    // canonical SQL
    cards.push(
      h(
        "div",
        { class: "card" },
        h("div", { class: "card-head" }, "sql"),
        detail.sql
          ? h("div", { class: "card-body" }, sqlBlock(detail.sql))
          : h("div", { class: "empty" }, "python model", h("div", { class: "hint" }, "defined in code — no canonical SQL to show")),
      ),
    );

    // latest check results
    const recent = [...checkRows]
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
          { empty: "no check results yet", hint: "run checks from the checks view" },
        ),
      ),
    );

    return cards;
  }

  filter.addEventListener("input", debounce(renderList, 120));
  renderList();
  if (params.m) await openDetail(params.m, { scroll: true });
}
