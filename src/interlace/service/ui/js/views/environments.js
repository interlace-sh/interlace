// Environments: prod plus the prefixed sandboxes — how many models each holds,
// how far each has drifted from the compiled project, and a guarded drop.

import { count, h, pill, relTime, table } from "../ui.js";

const PRODUCTION = "prod"; // the unprefixed namespace (PRODUCTION_ENV server-side)

export async function render(el, { api, go, toast, modal }) {
  const body = h("div", {});

  const newBtn = h("button", { class: "btn primary", onclick: () => createModal() }, "new environment…");

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Environments"),
      h("span", { class: "sub" }, "prod is the unprefixed namespace; other environments are prefixed sandboxes."),
      h("span", { class: "spread" }),
      newBtn,
    ),
    body,
  );

  function createModal() {
    modal((box, close) => {
      const name = h("input", { class: "in", placeholder: "dev, staging, feature-x…" });
      const buildBtn = h(
        "button",
        {
          class: "btn primary",
          onclick: async () => {
            const env = name.value.trim();
            if (!/^[a-z][a-z0-9_-]*$/i.test(env)) {
              toast("environment names are letters, digits, _ and -", "err");
              return;
            }
            buildBtn.disabled = true;
            buildBtn.textContent = "building…";
            try {
              const result = await api.post("/apply", { environment: env });
              toast(`built ${result.built.length} model(s) into '${env}'`, "ok");
              close();
              refresh();
            } catch (error) {
              toast(error.message, "err");
              buildBtn.disabled = false;
              buildBtn.textContent = "build";
            }
          },
        },
        "build",
      );
      box.append(
        h("h2", {}, "New environment"),
        h(
          "p",
          { class: "sub", style: "margin-bottom:10px" },
          "an environment is created by building into it: every model materialises under the ",
          h("code", {}, "<name>__"),
          " schema prefix and gets its own views. prod stays untouched.",
        ),
        h("label", { class: "field" }, h("span", {}, "name"), name),
        h("div", { class: "actions" }, h("button", { class: "btn", onclick: close }, "cancel"), buildBtn),
      );
      name.focus();
    });
  }

  async function refresh() {
    let environments;
    try {
      environments = await api.get("/environments");
    } catch (error) {
      body.replaceChildren(h("div", { class: "empty" }, error.message));
      return;
    }
    body.replaceChildren(
      h(
        "div",
        { class: "card" },
        table(
          [
            {
              k: "name",
              label: "environment",
              render: (env) =>
                env.name === PRODUCTION
                  ? h("span", {}, h("strong", {}, env.name), " ", pill("production", "violet"))
                  : env.name,
            },
            { k: "models", label: "models", num: true, render: (env) => count(env.models) },
            {
              k: "changed",
              label: "changed",
              num: true,
              render: (env) =>
                env.changed > 0 ? h("span", { style: "color:var(--amber)" }, count(env.changed)) : h("span", { class: "faint" }, "0"),
            },
            { k: "promoted_at", label: "promoted", render: (env) => h("span", { class: "dim" }, relTime(env.promoted_at)) },
            {
              k: "_actions",
              label: "",
              render: (env) =>
                h(
                  "span",
                  { style: "display:inline-flex; gap:6px" },
                  h("button", { class: "btn small", onclick: () => go("plan") }, "plan"),
                  h("button", { class: "btn small", onclick: () => historyModal(env.name) }, "history…"),
                  h("button", { class: "btn small danger", onclick: () => dropModal(env.name) }, "drop"),
                ),
            },
          ],
          environments,
          { empty: "no environments yet", hint: "apply promotes into an environment; it appears here" },
        ),
      ),
    );
  }

  function historyModal(name) {
    modal(async (box, close) => {
      box.append(h("h2", {}, `Promotions — ${name}`), h("div", { class: "empty" }, "loading…"));
      let generations;
      try {
        generations = await api.get(`/environments/${encodeURIComponent(name)}/history`);
      } catch (error) {
        box.lastChild.replaceWith(h("div", { class: "empty" }, error.message));
        return;
      }
      const latest = generations[0]?.generation;
      const rows = table(
        [
          {
            k: "generation",
            label: "gen",
            num: true,
            render: (g) =>
              g.generation === latest
                ? h("span", {}, String(g.generation), " ", pill("current", "violet"))
                : String(g.generation),
          },
          { k: "promoted_at", label: "promoted", render: (g) => h("span", { class: "dim" }, relTime(g.promoted_at)) },
          { k: "models", label: "models", num: true },
          {
            k: "_act",
            label: "",
            render: (g) =>
              g.generation === latest
                ? h("span", {}, "")
                : h(
                    "button",
                    {
                      class: "btn small",
                      onclick: async (event) => {
                        event.target.disabled = true;
                        try {
                          const result = await api.post(`/environments/${encodeURIComponent(name)}/rollback`, {
                            generation: g.generation,
                          });
                          toast(
                            `rolled ${name} back to generation ${result.generation} — ` +
                              `${result.repointed.length} view(s) repointed, nothing rebuilt`,
                            "ok",
                          );
                          close();
                          refresh();
                        } catch (error) {
                          toast(error.message, "err");
                          event.target.disabled = false;
                        }
                      },
                    },
                    "roll back",
                  ),
          },
        ],
        generations,
        { empty: "no promotion history yet", hint: "every apply that promotes records a generation" },
      );
      box.lastChild.replaceWith(
        h(
          "p",
          { class: "sub", style: "margin-bottom:10px" },
          "rolling back repoints the views at that generation's snapshots — nothing rebuilds, and applying again returns to the latest state.",
        ),
        rows,
        h("div", { class: "actions" }, h("button", { class: "btn", onclick: close }, "close")),
      );
    });
  }

  function dropModal(name) {
    modal((box, close) => {
      const confirm = h("input", { class: "in", placeholder: name, autocomplete: "off" });
      const dropBtn = h("button", { class: "btn danger", disabled: true }, "drop");
      confirm.addEventListener("input", () => {
        dropBtn.disabled = confirm.value !== name;
      });
      dropBtn.addEventListener("click", async () => {
        dropBtn.disabled = true;
        try {
          const result = await api.del(
            `/environments/${encodeURIComponent(name)}${name === PRODUCTION ? "?force=true" : ""}`,
          );
          toast(`dropped ${name} — ${result.dropped_views.length} view(s) removed, snapshots released to gc`, "ok");
          close();
          refresh();
        } catch (error) {
          toast(error.message, "err");
          dropBtn.disabled = false;
        }
      });
      box.append(
        h("h2", {}, `Drop ${name}`),
        h(
          "p",
          { class: "sub", style: "margin-bottom:10px" },
          name === PRODUCTION
            ? "this is the production environment — its views are removed and its snapshots released to gc."
            : "views are removed and the environment's snapshots are released to gc.",
        ),
        h("label", { class: "field" }, h("span", {}, `type ${name} to confirm`), confirm),
        h("div", { class: "actions" }, h("button", { class: "btn", onclick: close }, "cancel"), dropBtn),
      );
      confirm.focus();
    });
  }

  await refresh();
}
