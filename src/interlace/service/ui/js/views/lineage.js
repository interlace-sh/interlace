// Lineage: the whole graph on one canvas. Click a model to trace its blast
// radius, expand pins to trace a single column, watch edges flow while builds
// run. One /lineage payload — no request per node.

import { createDag } from "../dag.js";
import { debounce, h } from "../ui.js";

export async function render(el, { api, feed, go, params }) {
  const data = await api.get("/lineage");
  // streams join the canvas as source nodes, keyed exactly as SQL references
  // them (streams.<name>) so column lineage lines up without translation
  for (const stream of data.streams ?? []) {
    data.models.unshift({
      name: stream.name,
      display: stream.stream,
      output: "stream",
      strategy: "",
      engine: "",
      tags: [],
      columns: stream.columns,
      types: stream.types,
      is_stream: true,
    });
  }

  // custom autocomplete — the native datalist fights the dark theme and
  // can't show node kinds
  const search = h("input", { class: "in", placeholder: "focus a model…", autocomplete: "off", spellcheck: "false" });
  const hits = h("div", { class: "hits" });
  const searchWrap = h("div", { class: "canvas-search" }, search, hits);
  const fitBtn = h("button", { class: "btn small" }, "fit");
  let matches = [];
  let hitIndex = 0;

  function updateHits() {
    const needle = search.value.trim().toLowerCase();
    matches = needle
      ? data.models.filter((m) => (m.display ?? m.name).toLowerCase().includes(needle)).slice(0, 12)
      : [];
    hitIndex = 0;
    hits.replaceChildren(
      ...matches.map((m, index) =>
        h(
          "div",
          { class: `hit ${index === hitIndex ? "on" : ""}`, onclick: () => pickHit(index) },
          h("span", {}, m.display ?? m.name),
          h("span", { class: "ty" }, m.output),
        ),
      ),
    );
  }

  function pickHit(index) {
    const model = matches[index];
    if (!model) return;
    search.value = model.display ?? model.name;
    hits.replaceChildren();
    dag.focus(model.name);
  }

  search.addEventListener("input", debounce(updateHits, 80));
  search.addEventListener("keydown", (event) => {
    if (event.key === "Enter") pickHit(hitIndex);
    else if (event.key === "Escape") hits.replaceChildren();
    else if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (!matches.length) return;
      hitIndex = (hitIndex + (event.key === "ArrowDown" ? 1 : matches.length - 1)) % matches.length;
      hits.querySelectorAll(".hit").forEach((el, index) => el.classList.toggle("on", index === hitIndex));
    }
  });
  search.addEventListener("blur", () => setTimeout(() => hits.replaceChildren(), 150));
  const hintEl = h("span", { class: "canvas-legend" }, `${data.models.length} nodes · ${data.edges.length} edges — click a model to trace, ▸ to expand columns, click a column to trace it through the graph`);

  const wrap = h("div", { class: "canvas-wrap" });
  const tools = h("div", { class: "canvas-tools" }, searchWrap, fitBtn);
  const detail = h("span", { class: "sub" });

  el.append(
    h("div", { class: "view-head" }, h("h1", {}, "Lineage"), detail, h("span", { class: "spread" })),
    wrap,
  );
  wrap.append(tools, hintEl);

  const dag = createDag(wrap, data, {
    onSelect(name) {
      detail.replaceChildren();
      if (name) {
        detail.append(
          name,
          " — ",
          h("a", { href: `#/models?m=${encodeURIComponent(name)}`, style: "color: var(--violet)" }, "open model"),
        );
      }
    },
  });

  fitBtn.addEventListener("click", () => dag.fit());
  if (params.m) dag.focus(params.m);

  const offFeed = feed.on((event) => {
    if (event.type === "model.start") dag.setFlow(event.entity, true);
    else if (event.type?.startsWith("model.")) dag.setFlow(event.entity, false);
  });

  return () => {
    offFeed();
    dag.destroy();
  };
}
