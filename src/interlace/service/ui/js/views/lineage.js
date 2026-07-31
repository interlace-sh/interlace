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

  const search = h("input", { class: "in", placeholder: "focus a model…", list: "lineage-models" });
  const datalist = h("datalist", { id: "lineage-models" }, data.models.map((m) => h("option", { value: m.name })));
  const fitBtn = h("button", { class: "btn small" }, "fit");
  const hintEl = h("span", { class: "canvas-legend" }, `${data.models.length} nodes · ${data.edges.length} edges — click a model to trace, ▸ to expand columns, click a column to trace it through the graph`);

  const wrap = h("div", { class: "canvas-wrap" });
  const tools = h("div", { class: "canvas-tools" }, search, datalist, fitBtn);
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

  search.addEventListener("input", debounce(() => {
    const name = search.value.trim();
    if (data.models.some((m) => m.name === name)) dag.focus(name);
  }, 120));
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
