// Query: the read-only SQL console. SELECT only, always row-capped — the
// server enforces both; this view just makes the cap visible. ⌘⏎ runs.

import { count, h } from "../ui.js";

const HISTORY_KEY = "interlace.qhistory";
const HISTORY_MAX = 8;

function loadHistory() {
  try {
    const stored = JSON.parse(localStorage.getItem(HISTORY_KEY) || "[]");
    return Array.isArray(stored) ? stored : [];
  } catch {
    return [];
  }
}

function saveHistory(sql) {
  const entries = [sql, ...loadHistory().filter((entry) => entry !== sql)].slice(0, HISTORY_MAX);
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(entries));
  } catch {
    /* storage full or blocked — history is a convenience, not state */
  }
}

export async function render(el, { api, modal, params }) {
  const editor = h("textarea", { class: "q-editor", placeholder: "SELECT … — read-only, always row-capped" });
  if (params.sql) editor.value = params.sql;

  // ---- table browser: what you can FROM, per environment --------------------
  const envSelect = h("select", { class: "in", style: "width:100%" });
  const tableFilter = h("input", { class: "in", placeholder: "filter…", style: "width:100%" });
  const tableList = h("div", { class: "q-tables" });
  let models = [];
  let streams = [];

  function qualify(name, env) {
    const qualified = name.includes(".") ? name : `main.${name}`;
    return env === "prod" ? qualified : `${env}__${qualified}`;
  }

  function insertRef(ref) {
    editor.setRangeText(ref, editor.selectionStart, editor.selectionEnd, "end");
    editor.focus();
  }

  function renderTables() {
    const env = envSelect.value || "prod";
    const needle = tableFilter.value.trim().toLowerCase();
    const rows = [];
    for (const model of models) {
      if (model.is_terminal) continue; // terminal table/file has no readable table
      if (needle && !model.name.toLowerCase().includes(needle)) continue;
      const ref = qualify(model.name, env);
      rows.push(
        h(
          "div",
          { class: "q-table", title: `insert ${ref}`, onclick: () => insertRef(ref) },
          h("span", { class: "nm" }, model.name),
          h("span", { class: "ty" }, model.output),
        ),
      );
    }
    for (const stream of streams) {
      const ref = `streams.${stream.name}`;
      if (needle && !ref.includes(needle)) continue;
      rows.push(
        h(
          "div",
          { class: "q-table", title: `insert ${ref}`, onclick: () => insertRef(ref) },
          h("span", { class: "nm" }, stream.name),
          h("span", { class: "ty", style: "color:var(--cyan)" }, "stream"),
        ),
      );
    }
    tableList.replaceChildren(...(rows.length ? rows : [h("div", { class: "empty" }, "nothing matches")]));
  }

  async function loadBrowser() {
    try {
      const [health, envs, modelList, streamList] = await Promise.all([
        api.get("/health"),
        api.get("/environments").catch(() => []),
        api.get("/models"),
        api.get("/streams").catch(() => []),
      ]);
      models = modelList;
      streams = streamList;
      const names = [...new Set([health.environment, ...envs.map((e) => e.name)])].filter(Boolean);
      envSelect.replaceChildren(...names.map((name) => h("option", { value: name, selected: name === health.environment }, name)));
      renderTables();
    } catch {
      tableList.replaceChildren(h("div", { class: "empty" }, "daemon unreachable"));
    }
  }
  envSelect.addEventListener("change", renderTables);
  tableFilter.addEventListener("input", renderTables);

  const limitSelect = h(
    "select",
    { class: "in" },
    [100, 500, 2000, 10000].map((value) => h("option", { value, selected: value === 500 }, String(value))),
  );
  const runBtn = h("button", { class: "btn primary" }, "run (⌘⏎)");
  const historyBtn = h("button", { class: "btn" }, "history");
  const meta = h("span", { class: "q-meta" });
  const results = h("div", {});

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Query"),
      h("span", { class: "sub" }, "inspect the warehouse — the console never writes"),
    ),
    h(
      "div",
      { class: "q-layout" },
      h(
        "div",
        {},
        h(
          "div",
          { class: "card" },
          h(
            "div",
            { class: "card-body" },
            editor,
            h(
              "div",
              { style: "display:flex; gap:8px; align-items:center; margin-top:10px" },
              h("span", { style: "color:var(--tx-faint); font-size:10.5px; text-transform:uppercase; letter-spacing:0.06em" }, "limit"),
              limitSelect,
              runBtn,
              historyBtn,
              h("span", { style: "flex:1" }),
              meta,
            ),
          ),
        ),
        results,
      ),
      h(
        "div",
        { class: "card" },
        h("div", { class: "card-head" }, "tables"),
        h(
          "div",
          { class: "card-body", style: "display:flex; flex-direction:column; gap:8px" },
          h("label", { class: "field" }, h("span", {}, "environment"), envSelect),
          tableFilter,
          tableList,
        ),
      ),
    ),
  );
  loadBrowser();

  function cellFor(value) {
    if (value === null || value === undefined) return h("td", { class: "null" }, "∅");
    if (typeof value === "number") return h("td", { class: "num" }, String(value));
    return h("td", {}, String(value));
  }

  function grid(response) {
    const head = h(
      "tr",
      {},
      response.columns.map((column, index) => h("th", {}, column, h("small", {}, response.types[index] ?? ""))),
    );
    const body = response.rows.map((row) => h("tr", {}, row.map(cellFor)));
    return h("div", { class: "q-grid" }, h("table", {}, h("thead", {}, head), h("tbody", {}, body)));
  }

  async function run() {
    const sql = editor.value.trim();
    if (!sql) return;
    runBtn.disabled = true;
    runBtn.textContent = "running…";
    meta.textContent = "";
    try {
      const response = await api.post("/query", { sql, limit: Number(limitSelect.value) });
      saveHistory(sql);
      meta.textContent =
        `${count(response.row_count)} rows · ${Math.round(response.elapsed_ms)} ms` +
        (response.truncated ? " · truncated" : "");
      results.replaceChildren(
        response.rows.length
          ? grid(response)
          : h("div", { class: "empty" }, "no rows", h("div", { class: "hint" }, "the query ran — it just returned nothing")),
      );
    } catch (error) {
      results.replaceChildren(h("div", { class: "empty" }, error.message));
    } finally {
      runBtn.disabled = false;
      runBtn.textContent = "run (⌘⏎)";
    }
  }

  function openHistory() {
    modal((box, close) => {
      box.append(h("h2", {}, "Query history"));
      const entries = loadHistory();
      if (!entries.length) {
        box.append(h("div", { class: "empty" }, "no queries yet", h("div", { class: "hint" }, "run one and it lands here")));
        return;
      }
      box.append(
        h(
          "div",
          { class: "feed" },
          entries.map((sql) =>
            h(
              "div",
              {
                class: "feed-row",
                style: "cursor:pointer",
                onclick: () => {
                  editor.value = sql;
                  close();
                  editor.focus();
                },
              },
              h("span", { class: "en", title: sql }, sql),
            ),
          ),
        ),
      );
    });
  }

  editor.addEventListener("keydown", (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
      event.preventDefault();
      run();
    }
  });
  runBtn.addEventListener("click", run);
  historyBtn.addEventListener("click", openHistory);
  editor.focus();
}
