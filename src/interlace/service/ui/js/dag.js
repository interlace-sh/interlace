// Lineage canvas: a circuit-schematic DAG. Layered left→right by longest path,
// barycenter-ordered rows, orthogonally routed edges, column pins that expand
// per node and trace column-level lineage both directions. Pure SVG — no
// dependencies, thousands of edges stay cheap because everything is one <g>
// transform and hit-testing rides the DOM.

const NODE_W = 176;
const NODE_H = 46;
const PIN_H = 16;
const GAP_X = 90;
const GAP_Y = 26;

const SVG = "http://www.w3.org/2000/svg";

const TYPE_SHORT = [
  [/^(var)?char|^text|^string/i, "str"],
  [/int/i, "int"],
  [/^(double|float|real)/i, "float"],
  [/^bool/i, "bool"],
  [/^timestamp/i, "ts"],
  [/^date$/i, "date"],
  [/^time/i, "time"],
  [/^(decimal|numeric)/i, "dec"],
  [/^json/i, "json"],
  [/^blob|^bytea/i, "bytes"],
];

function shortType(raw) {
  if (!raw) return "";
  for (const [pattern, short] of TYPE_SHORT) if (pattern.test(raw)) return short;
  return raw.toLowerCase().split("(")[0].slice(0, 6);
}

function svg(tag, attrs = {}) {
  const el = document.createElementNS(SVG, tag);
  for (const [key, value] of Object.entries(attrs)) el.setAttribute(key, value);
  return el;
}

export function createDag(container, data, { onSelect } = {}) {
  // data: {models:[{name,output,strategy,engine,columns,...}], edges:[[up,down]], columns:{m:{c:[[um,uc]]}}}
  const byName = new Map(data.models.map((model) => [model.name, model]));
  const downstream = new Map();
  const upstream = new Map();
  for (const [up, down] of data.edges) {
    if (!downstream.has(up)) downstream.set(up, []);
    if (!upstream.has(down)) upstream.set(down, []);
    downstream.get(up).push(down);
    upstream.get(down).push(up);
  }
  // column reverse index: "model.col" -> [[downModel, downCol]]
  const columnDown = new Map();
  for (const [model, cols] of Object.entries(data.columns || {})) {
    for (const [col, sources] of Object.entries(cols)) {
      for (const [upModel, upCol] of sources) {
        const key = `${upModel}.${upCol}`;
        if (!columnDown.has(key)) columnDown.set(key, []);
        columnDown.get(key).push([model, col]);
      }
    }
  }

  // ---- layout: layer = longest path from any root --------------------------
  const layer = new Map();
  const depth = (name) => {
    if (layer.has(name)) return layer.get(name);
    const ups = upstream.get(name) || [];
    const value = ups.length ? 1 + Math.max(...ups.map(depth)) : 0;
    layer.set(name, value);
    return value;
  };
  data.models.forEach((model) => depth(model.name));
  const layers = [];
  for (const model of data.models) {
    const l = layer.get(model.name);
    (layers[l] ??= []).push(model.name);
  }
  // two barycenter passes settle most graphs
  const rowIndex = new Map();
  layers.forEach((names, l) => names.forEach((name, index) => rowIndex.set(name, index)));
  for (let pass = 0; pass < 2; pass++) {
    for (const names of layers) {
      names.sort((a, b) => {
        const center = (name) => {
          const ups = upstream.get(name) || [];
          return ups.length ? ups.reduce((sum, up) => sum + (rowIndex.get(up) ?? 0), 0) / ups.length : rowIndex.get(name);
        };
        return center(a) - center(b) || a.localeCompare(b);
      });
      names.forEach((name, index) => rowIndex.set(name, index));
    }
  }

  const expanded = new Set(); // model names showing pins
  const positions = new Map(); // name -> {x, y, h}

  function nodeHeight(name) {
    const cols = byName.get(name)?.columns || [];
    return expanded.has(name) && cols.length ? NODE_H + 6 + cols.length * PIN_H : NODE_H;
  }

  function layout() {
    layers.forEach((names, l) => {
      let y = 0;
      for (const name of names) {
        const height = nodeHeight(name);
        positions.set(name, { x: l * (NODE_W + GAP_X), y, h: height });
        y += height + GAP_Y;
      }
    });
    // vertically centre shorter layers against the tallest
    const total = Math.max(...layers.map((names) => names.reduce((sum, n) => sum + nodeHeight(n) + GAP_Y, 0)), 1);
    layers.forEach((names) => {
      const mine = names.reduce((sum, n) => sum + nodeHeight(n) + GAP_Y, 0);
      const offset = (total - mine) / 2;
      for (const name of names) positions.get(name).y += offset;
    });
  }

  // ---- svg scaffolding -------------------------------------------------------
  const root = svg("svg");
  const world = svg("g");
  const edgeLayer = svg("g");
  const nodeLayer = svg("g");
  world.append(edgeLayer, nodeLayer);
  root.append(world);
  container.append(root);

  let scale = 1;
  let panX = 40;
  let panY = 30;
  const applyTransform = () => world.setAttribute("transform", `translate(${panX},${panY}) scale(${scale})`);

  root.addEventListener("wheel", (event) => {
    event.preventDefault();
    const factor = event.deltaY < 0 ? 1.12 : 0.89;
    const rect = root.getBoundingClientRect();
    const mx = event.clientX - rect.left;
    const my = event.clientY - rect.top;
    panX = mx - (mx - panX) * factor;
    panY = my - (my - panY) * factor;
    scale = Math.min(2.5, Math.max(0.15, scale * factor));
    applyTransform();
  }, { passive: false });

  // Panning WITHOUT pointer capture: capture would retarget pointerup to the
  // svg root and node/pin click handlers would never fire. A 4px threshold
  // separates a pan from a click; `moved` lets click handlers ignore drag-ends.
  let dragging = null;
  let moved = false;
  root.addEventListener("pointerdown", (event) => {
    dragging = { x: event.clientX - panX, y: event.clientY - panY, sx: event.clientX, sy: event.clientY };
    moved = false;
  });
  window.addEventListener("pointermove", (event) => {
    if (!dragging) return;
    if (!moved && Math.hypot(event.clientX - dragging.sx, event.clientY - dragging.sy) < 4) return;
    moved = true;
    root.classList.add("panning");
    panX = event.clientX - dragging.x;
    panY = event.clientY - dragging.y;
    applyTransform();
  });
  window.addEventListener("pointerup", () => {
    dragging = null;
    root.classList.remove("panning");
  });
  const wasDrag = () => moved;

  // ---- rendering ---------------------------------------------------------------
  const edgeEls = new Map(); // "up->down" -> path
  const nodeEls = new Map();
  let selected = null;
  let selectedPin = null;

  function edgePath(up, down) {
    const a = positions.get(up);
    const b = positions.get(down);
    const sx = a.x + NODE_W;
    const sy = a.y + NODE_H / 2;
    const tx = b.x;
    const ty = b.y + NODE_H / 2;
    const mid = sx + (tx - sx) / 2;
    return `M ${sx} ${sy} H ${mid} V ${ty} H ${tx}`;
  }

  function draw() {
    layout();
    edgeLayer.replaceChildren();
    nodeLayer.replaceChildren();
    edgeEls.clear();
    nodeEls.clear();

    for (const [up, down] of data.edges) {
      if (!positions.has(up) || !positions.has(down)) continue;
      const path = svg("path", { class: "edge", d: edgePath(up, down) });
      edgeLayer.append(path);
      edgeEls.set(`${up}->${down}`, path);
    }

    for (const model of data.models) {
      const { x, y } = positions.get(model.name);
      const group = svg("g", { class: "node", transform: `translate(${x},${y})` });
      const height = nodeHeight(model.name);
      group.append(svg("rect", { class: "body", width: NODE_W, height, rx: 6 }));

      if (model.is_stream) group.classList.add("stream");
      const label = model.display ?? model.name;
      const name = svg("text", { class: "name", x: 10, y: 19 });
      name.textContent = label.length > 21 ? label.slice(0, 20) + "…" : label;
      const meta = svg("text", { class: "meta", x: 10, y: 34 });
      meta.textContent = `${model.output}${model.strategy && model.strategy !== "full" ? " · " + model.strategy : ""}${model.engine && model.engine !== "default" ? " · " + model.engine : ""}`;
      group.append(name, meta);

      const marks = [];
      if (model.has_schedule) marks.push("⏱");
      if (model.has_checks) marks.push("✓");
      if (marks.length) {
        const marksEl = svg("text", { class: "marks", x: NODE_W - 10 - marks.length * 12, y: 19 });
        marksEl.textContent = marks.join(" ");
        group.append(marksEl);
      }

      if ((model.columns || []).length) {
        // a real control: vertical chevron with a generous invisible hit target
        const expander = svg("g", { class: "expander" });
        const hit = svg("rect", { x: NODE_W - 26, y: NODE_H - 26, width: 24, height: 24, fill: "transparent" });
        const open = expanded.has(model.name);
        const chevron = svg("path", {
          class: "chev",
          d: open ? `M ${NODE_W - 20} ${NODE_H - 10} l 6 -6 l 6 6` : `M ${NODE_W - 20} ${NODE_H - 16} l 6 6 l 6 -6`,
        });
        expander.append(hit, chevron);
        expander.addEventListener("click", (event) => {
          event.stopPropagation();
          if (wasDrag()) return;
          expanded.has(model.name) ? expanded.delete(model.name) : expanded.add(model.name);
          draw();
          applySelection();
        });
        group.append(expander);
      }

      if (expanded.has(model.name)) {
        (model.columns || []).forEach((column, index) => {
          const pin = svg("g", { class: "pin", "data-pin": `${model.name}.${column}` });
          const pinLabel = svg("text", { x: 18, y: NODE_H + 8 + index * PIN_H + 4 });
          pinLabel.textContent = column.length > 16 ? column.slice(0, 15) + "…" : column;
          const kind = shortType((model.types || {})[column]);
          if (kind) {
            const typeEl = svg("text", { class: "pin-type", x: NODE_W - 10, y: NODE_H + 8 + index * PIN_H + 4, "text-anchor": "end" });
            typeEl.textContent = kind;
            pin.append(typeEl);
          }
          pin.append(pinLabel);
          pin.addEventListener("click", (event) => {
            event.stopPropagation();
            if (wasDrag()) return;
            selectPin(`${model.name}.${column}`);
          });
          group.append(pin);
        });
      }

      group.addEventListener("click", () => {
        if (!wasDrag()) select(model.name);
      });
      nodeLayer.append(group);
      nodeEls.set(model.name, group);
    }
    applySelection();
  }

  // ---- selection / tracing --------------------------------------------------------
  function reach(start, adjacency) {
    const seen = new Set([start]);
    const stack = [start];
    while (stack.length) {
      for (const next of adjacency.get(stack.pop()) || []) {
        if (!seen.has(next)) {
          seen.add(next);
          stack.push(next);
        }
      }
    }
    return seen;
  }

  function applySelection() {
    const litNodes = selected ? new Set([...reach(selected, upstream), ...reach(selected, downstream)]) : null;
    for (const [name, el] of nodeEls) {
      el.classList.toggle("sel", name === selected);
      el.classList.toggle("dimmed", !!litNodes && !litNodes.has(name));
    }
    for (const [key, el] of edgeEls) {
      const [up, down] = key.split("->");
      const lit = !!litNodes && litNodes.has(up) && litNodes.has(down);
      el.classList.toggle("lit", lit);
      el.classList.toggle("dimmed", !!litNodes && !lit);
    }
    // pin highlight: trace the selected column both directions, light the pins,
    // the models that carry them, and the edges between those models
    nodeLayer.querySelectorAll(".pin").forEach((pin) => pin.classList.remove("sel", "lit"));
    if (selectedPin) {
      const lit = traceColumns(selectedPin);
      const litModels = new Set([...lit].map((key) => splitPin(key)[0]));
      nodeLayer.querySelectorAll(".pin").forEach((pin) => {
        const key = pin.dataset.pin;
        if (key === selectedPin) pin.classList.add("sel");
        else if (lit.has(key)) pin.classList.add("lit");
      });
      for (const [nodeName, el] of nodeEls) el.classList.toggle("dimmed", !litModels.has(nodeName));
      for (const [key, el] of edgeEls) {
        const [up, down] = key.split("->");
        const onPath = litModels.has(up) && litModels.has(down);
        el.classList.toggle("lit", onPath);
        el.classList.toggle("dimmed", !onPath);
      }
    }
  }

  function splitPin(key) {
    const dot = key.lastIndexOf(".");
    return [key.slice(0, dot), key.slice(dot + 1)];
  }

  function select(name) {
    selected = selected === name ? null : name;
    selectedPin = null;
    applySelection();
    onSelect?.(selected);
  }

  function traceColumns(start) {
    // the full column blast-radius, both directions, data-level (no DOM needed)
    const lit = new Set([start]);
    const up = (key) => {
      const [model, column] = splitPin(key);
      for (const [upModel, upCol] of data.columns?.[model]?.[column] || []) {
        const next = `${upModel}.${upCol}`;
        if (!lit.has(next)) {
          lit.add(next);
          up(next);
        }
      }
    };
    const down = (key) => {
      for (const [downModel, downCol] of columnDown.get(key) || []) {
        const next = `${downModel}.${downCol}`;
        if (!lit.has(next)) {
          lit.add(next);
          down(next);
        }
      }
    };
    up(start);
    down(start);
    return lit;
  }

  function selectPin(key) {
    selectedPin = selectedPin === key ? null : key;
    if (selectedPin) {
      // expand EVERY node the trace touches so the whole path is visible
      let grew = false;
      for (const litKey of traceColumns(selectedPin)) {
        const [model] = splitPin(litKey);
        if (byName.has(model) && !expanded.has(model)) {
          expanded.add(model);
          grew = true;
        }
      }
      if (grew) draw();
    }
    applySelection();
  }

  function focus(name) {
    if (!positions.has(name)) return;
    const { x, y } = positions.get(name);
    const rect = root.getBoundingClientRect();
    scale = Math.max(scale, 0.8);
    panX = rect.width / 2 - (x + NODE_W / 2) * scale;
    panY = rect.height / 2 - (y + NODE_H / 2) * scale;
    applyTransform();
    selected = name;
    selectedPin = null;
    applySelection();
    onSelect?.(name);
  }

  function fit() {
    let maxX = 0;
    let maxY = 0;
    for (const { x, y, h } of positions.values()) {
      maxX = Math.max(maxX, x + NODE_W);
      maxY = Math.max(maxY, y + h);
    }
    const rect = root.getBoundingClientRect();
    scale = Math.min(1, (rect.width - 80) / Math.max(maxX, 1), (rect.height - 110) / Math.max(maxY, 1));
    panX = Math.max(40, (rect.width - maxX * scale) / 2);
    panY = Math.max(64, (rect.height - maxY * scale) / 2); // clear of the toolbar
    applyTransform();
  }

  function setFlow(name, on) {
    // light the model's incoming edges while it builds
    for (const up of upstream.get(name) || []) {
      edgeEls.get(`${up}->${name}`)?.classList.toggle("flow", on);
    }
  }

  draw();
  fit();
  applyTransform();

  return { focus, fit, setFlow, destroy: () => root.remove() };
}
