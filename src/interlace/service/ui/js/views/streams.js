// Streams: the durable append log, per stream — head vs watermark (lag is the
// story), drift policy, schema, publish and peek without leaving the page.

import { count, h, pill } from "../ui.js";

const DRIFT_TONE = { reject: "", evolve: "green", quarantine: "amber" };

export async function render(el, { api, feed, toast, modal }) {
  const body = h("div", { class: "grid2" });

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Streams"),
      h("span", { class: "sub" }, "durable append logs — publish lands in the log first, the flusher materialises"),
    ),
    body,
  );

  let streams = [];
  const cards = new Map(); // name -> {lagEl, headEl, wmEl, peekEl, peekOpen}

  function lagPill(stream) {
    const lag = stream.head - stream.watermark;
    return lag > 0 ? pill(`lag ${count(lag)}`, "amber") : pill("drained", "green");
  }

  function renderCards() {
    body.replaceChildren();
    cards.clear();
    if (!streams.length) {
      body.replaceChildren(
        h(
          "div",
          { class: "card", style: "grid-column: 1 / -1" },
          h("div", { class: "empty" }, "no streams defined", h("div", { class: "hint" }, "declare one with @stream() and it appears here")),
        ),
      );
      return;
    }
    for (const stream of streams) {
      const lagEl = h("span", {}, lagPill(stream));
      const headEl = h("span", {}, count(stream.head));
      const wmEl = h("span", {}, count(stream.watermark));
      const peekEl = h("div", {});

      const schema = h("div", { class: "sub", style: "margin-top:6px" });
      Object.entries(stream.schema).forEach(([field, type], index) => {
        if (index) schema.append("  ·  ");
        schema.append(field + " ", h("span", { class: "faint" }, type));
      });

      const card = h(
        "div",
        { class: "card", style: "margin-top:0" },
        h(
          "div",
          { class: "card-head", style: "text-transform:none; letter-spacing:0" },
          h("strong", {}, stream.name),
          pill(stream.on_schema_drift, DRIFT_TONE[stream.on_schema_drift] ?? ""),
          h("span", { class: "spread" }),
          lagEl,
        ),
        h(
          "div",
          { class: "card-body" },
          h(
            "div",
            { style: "display:flex; gap:18px; align-items:baseline" },
            h("span", {}, h("span", { class: "sub" }, "head "), headEl),
            h("span", {}, h("span", { class: "sub" }, "watermark "), wmEl),
            h("span", {}, h("span", { class: "sub" }, "pending "), h("span", {}, count(stream.pending))),
            stream.retention
              ? h("span", {}, h("span", { class: "sub" }, "retention "), h("span", {}, stream.retention))
              : null,
            h("span", { class: "spread" }),
            h("button", { class: "btn small", onclick: () => peek(stream.name) }, "peek"),
            h("button", { class: "btn small primary", onclick: () => publishModal(stream.name) }, "publish…"),
          ),
          schema,
          h("div", { class: "sub", style: "margin-top:4px; color:var(--tx-faint)" }, "→ ", stream.table),
          peekEl,
        ),
      );
      cards.set(stream.name, { lagEl, headEl, wmEl, peekEl, peekOpen: false, stream });
      body.append(card);
    }
  }

  async function refreshNumbers() {
    let fresh;
    try {
      fresh = await api.get("/streams");
    } catch {
      return; // daemon away; keep what we have
    }
    streams = fresh;
    const names = new Set(fresh.map((s) => s.name));
    if (fresh.length !== cards.size || ![...cards.keys()].every((name) => names.has(name))) {
      renderCards();
      return;
    }
    for (const stream of fresh) {
      const card = cards.get(stream.name);
      card.stream = stream;
      card.lagEl.replaceChildren(lagPill(stream));
      card.headEl.textContent = count(stream.head);
      card.wmEl.textContent = count(stream.watermark);
      if (card.peekOpen) peek(stream.name, true);
    }
  }

  async function peek(name, keepOpen = false) {
    const card = cards.get(name);
    if (!card) return;
    if (card.peekOpen && !keepOpen) {
      card.peekOpen = false;
      card.peekEl.replaceChildren();
      return;
    }
    let detail;
    try {
      detail = await api.get(`/streams/${encodeURIComponent(name)}`);
    } catch (error) {
      toast(error.message, "err");
      return;
    }
    card.peekOpen = true;
    const rows = h("div", { class: "feed", style: "margin-top:8px; border:1px solid var(--line-soft); border-radius:var(--r)" });
    const recent = [...detail.recent].reverse(); // newest first
    if (!recent.length) {
      rows.append(h("div", { class: "feed-row" }, h("span", { class: "en faint" }, "nothing in the log yet")));
    }
    for (const payload of recent) {
      const { _offset, ...fields } = payload;
      rows.append(
        h(
          "div",
          { class: "feed-row" },
          h("span", { class: "ts" }, `#${_offset}`),
          h("span", { class: "en" }, JSON.stringify(fields).slice(0, 160)),
        ),
      );
    }
    card.peekEl.replaceChildren(rows);
  }

  function publishModal(name) {
    modal((box, close) => {
      const editor = h("textarea", { class: "in", rows: "8", style: "width:100%; resize:vertical", placeholder: '{"field": "value"}  — or an array for a batch' });
      box.append(
        h("h2", {}, `Publish to ${name}`),
        h("p", { class: "sub", style: "margin-bottom:8px" }, "one event (object) or a batch (array) — durable before the ack"),
        editor,
        h(
          "div",
          { class: "actions" },
          h("button", { class: "btn", onclick: close }, "cancel"),
          h(
            "button",
            {
              class: "btn primary",
              onclick: async () => {
                let parsed;
                try {
                  parsed = JSON.parse(editor.value);
                } catch {
                  toast("invalid JSON — expected an object or an array", "err");
                  return;
                }
                try {
                  const result = await api.post(`/streams/${encodeURIComponent(name)}`, parsed);
                  toast(`accepted ${result.accepted} · deduplicated ${result.deduplicated} · quarantined ${result.quarantined}`, "ok");
                  close();
                  refreshNumbers();
                } catch (error) {
                  toast(error.message, "err");
                }
              },
            },
            "publish",
          ),
        ),
      );
      editor.focus();
    });
  }

  try {
    streams = await api.get("/streams");
  } catch (error) {
    body.replaceChildren(h("div", { class: "empty", style: "grid-column: 1 / -1" }, error.message));
    return;
  }
  renderCards();

  const offFeed = feed.on((event) => {
    if (event.type === "stream.flushed" || streams.some((stream) => stream.name === event.entity)) refreshNumbers();
  });
  return () => offFeed();
}
