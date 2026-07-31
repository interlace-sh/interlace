// System: the daemon's plumbing on one page — engines, schedules, API keys,
// and maintenance (this browser's token, snapshot gc, daemon version).

import { copy, h, pill, relTime, table } from "../ui.js";

export async function render(el, { api, go, toast, modal, token }) {
  // engines/schedules carry long DSNs and cron lines: full width. Keys and
  // maintenance are compact and pair up.
  const pair = h("div", { class: "grid2", style: "margin-top:12px" });
  const enginesCard = h("div", { class: "card" });
  const schedulesCard = h("div", { class: "card" });
  const keysCard = h("div", { class: "card", style: "margin-top:0" });
  const maintCard = h("div", { class: "card", style: "margin-top:0" });

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "System"),
      h("span", { class: "sub" }, "engines, schedules, keys, maintenance"),
    ),
    enginesCard,
    schedulesCard,
    pair,
  );
  pair.append(keysCard, maintCard);

  // ---- engines ----------------------------------------------------------------

  async function renderEngines() {
    enginesCard.replaceChildren(h("div", { class: "card-head" }, "engines"));
    try {
      const engines = await api.get("/engines");
      enginesCard.append(
        table(
          [
            {
              k: "name",
              label: "engine",
              render: (engine) => (engine.default ? h("span", {}, engine.name, " ", pill("default", "violet")) : engine.name),
            },
            { k: "type", label: "type" },
            { k: "dialect", label: "dialect", render: (engine) => h("span", { class: "dim" }, engine.dialect) },
            {
              k: "database",
              label: "connection",
              render: (engine) =>
                h("span", { class: "dim", style: "word-break:break-all" }, engine.database || "—"),
            },
          ],
          engines,
          { empty: "no engines configured" },
        ),
      );
    } catch (error) {
      enginesCard.append(h("div", { class: "empty" }, error.message));
    }
  }

  // ---- schedules --------------------------------------------------------------

  async function renderSchedules() {
    schedulesCard.replaceChildren(h("div", { class: "card-head" }, "schedules"));
    try {
      const schedules = await api.get("/schedules");
      schedulesCard.append(
        table(
          [
            {
              k: "model",
              label: "model",
              render: (row) =>
                h("a", { href: `#/models?m=${encodeURIComponent(row.model)}`, style: "color:var(--violet)", onclick: (event) => { event.preventDefault(); go("models", { m: row.model }); } }, row.model),
            },
            {
              k: "expression",
              label: "schedule",
              render: (row) => h("span", {}, h("span", { class: "dim" }, row.kind + " "), row.expression),
            },
            { k: "next_fire", label: "next", render: (row) => h("span", { class: "dim" }, relTime(row.next_fire)) },
            { k: "last_fired", label: "last", render: (row) => h("span", { class: "dim" }, relTime(row.last_fired)) },
          ],
          schedules,
          { empty: "no scheduled models", hint: "add schedule: {cron: …} to a model" },
        ),
      );
    } catch (error) {
      schedulesCard.append(h("div", { class: "empty" }, error.message));
    }
  }

  // ---- api keys -----------------------------------------------------------------

  async function renderKeys() {
    const newKeyBtn = h("button", { class: "btn small", onclick: newKeyModal }, "new key…");
    keysCard.replaceChildren(h("div", { class: "card-head" }, "api keys", h("span", { class: "spread" }), newKeyBtn));
    try {
      const keys = await api.get("/apikeys");
      keysCard.append(
        table(
          [
            { k: "name", label: "key" },
            { k: "scopes", label: "scopes", render: (key) => h("span", { class: "dim" }, key.scopes.join(", ")) },
            { k: "created_at", label: "created", render: (key) => h("span", { class: "dim" }, relTime(key.created_at)) },
            {
              k: "_actions",
              label: "",
              render: (key) => h("button", { class: "btn small danger", onclick: () => revokeModal(key.name) }, "revoke"),
            },
          ],
          keys,
          { empty: "no api keys", hint: "keyless mode: every request is admin until the first key exists" },
        ),
      );
    } catch (error) {
      keysCard.append(
        h(
          "div",
          { class: "empty" },
          error.message,
          [401, 403].includes(error.status) ? h("div", { class: "hint" }, "admin scope required") : null,
        ),
      );
    }
  }

  function revokeModal(name) {
    modal((box, close) => {
      box.append(
        h("h2", {}, `Revoke ${name}`),
        h("p", { class: "sub" }, "requests using this key stop working immediately."),
        h(
          "div",
          { class: "actions" },
          h("button", { class: "btn", onclick: close }, "cancel"),
          h(
            "button",
            {
              class: "btn danger",
              onclick: async () => {
                try {
                  await api.del(`/apikeys/${encodeURIComponent(name)}`);
                  toast(`revoked ${name}`, "ok");
                  close();
                  renderKeys();
                } catch (error) {
                  toast(error.message, "err");
                }
              },
            },
            "revoke",
          ),
        ),
      );
    });
  }

  function newKeyModal() {
    modal((box, close) => {
      const name = h("input", { class: "in", placeholder: "ci-deploy" });
      const boxes = { read: h("input", { type: "checkbox", checked: true }), write: h("input", { type: "checkbox" }), admin: h("input", { type: "checkbox" }) };
      box.append(
        h("h2", {}, "New api key"),
        h(
          "div",
          { class: "form-grid" },
          h("label", { class: "field wide" }, h("span", {}, "name"), name),
          h(
            "div",
            { class: "wide", style: "display:flex; gap:16px" },
            h("label", { class: "check" }, boxes.read, "read"),
            h("label", { class: "check" }, boxes.write, "write"),
            h("label", { class: "check" }, boxes.admin, "admin"),
          ),
        ),
        h(
          "div",
          { class: "actions" },
          h("button", { class: "btn", onclick: close }, "cancel"),
          h(
            "button",
            {
              class: "btn primary",
              onclick: async () => {
                const scopes = Object.entries(boxes).filter(([, el]) => el.checked).map(([scope]) => scope);
                if (!name.value.trim()) {
                  toast("name the key", "err");
                  return;
                }
                if (!scopes.length) {
                  toast("pick at least one scope", "err");
                  return;
                }
                try {
                  const created = await api.post("/apikeys", { name: name.value.trim(), scopes });
                  showToken(created);
                  renderKeys();
                } catch (error) {
                  toast(error.message, "err");
                }
              },
            },
            "create",
          ),
        ),
      );
      name.focus();
    });
  }

  function showToken(created) {
    modal((box, close) => {
      box.append(
        h("h2", {}, `Key ${created.name} created`),
        h("p", { class: "sub", style: "margin-bottom:10px" }, "store it now — it will not be shown again."),
        h(
          "div",
          { style: "display:flex; gap:8px; align-items:center" },
          h("input", { class: "in", style: "flex:1", readonly: true, value: created.token }),
          h("button", { class: "btn", onclick: () => copy(created.token, toast) }, "copy"),
        ),
        h("div", { class: "actions" }, h("button", { class: "btn primary", onclick: close }, "done")),
      );
    });
  }

  // ---- maintenance ----------------------------------------------------------------

  async function renderMaintenance() {
    const tokenInput = h("input", { class: "in", style: "flex:1", type: "password", placeholder: "bearer token for this browser", value: token.get() });
    const saveBtn = h(
      "button",
      {
        class: "btn small",
        onclick: () => {
          token.set(tokenInput.value.trim());
          location.reload();
        },
      },
      "save",
    );
    const gcBtn = (label, dryRun) =>
      h(
        "button",
        {
          class: "btn small",
          onclick: async (event) => {
            const btn = event.currentTarget;
            btn.disabled = true;
            try {
              const result = await api.post("/gc", { grace: "7d", dry_run: dryRun });
              const verb = result.dry_run ? "would remove" : "removed";
              toast(`gc: ${verb} ${result.removed_snapshots} snapshot(s), ${result.dropped_tables.length} table(s) — ${result.kept_snapshots} kept`, "ok");
            } catch (error) {
              toast(error.message, "err");
            } finally {
              btn.disabled = false;
            }
          },
        },
        label,
      );

    const versionLine = h("div", { class: "sub" }, "daemon: …");
    maintCard.replaceChildren(
      h("div", { class: "card-head" }, "maintenance"),
      h(
        "div",
        { class: "card-body", style: "display:flex; flex-direction:column; gap:12px" },
        h(
          "div",
          { class: "field" },
          h("span", {}, "api token (this browser)"),
          h("div", { style: "display:flex; gap:8px" }, tokenInput, saveBtn),
        ),
        h(
          "div",
          { class: "field" },
          h("span", {}, "snapshot gc — 7d grace"),
          h("div", { style: "display:flex; gap:8px" }, gcBtn("gc (dry run)", true), gcBtn("gc now", false)),
        ),
        versionLine,
      ),
    );
    try {
      const health = await api.get("/health");
      versionLine.textContent = `daemon v${health.version} · environment ${health.environment}`;
    } catch {
      versionLine.textContent = "daemon unreachable";
    }
  }

  await Promise.all([renderEngines(), renderSchedules(), renderKeys(), renderMaintenance()]);
}
