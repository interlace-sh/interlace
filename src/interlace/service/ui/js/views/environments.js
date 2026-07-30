// Environments: prod plus the prefixed sandboxes — how many models each holds,
// how far each has drifted from the compiled project, and a guarded drop.

import { count, h, pill, relTime, table } from "../ui.js";

const PRODUCTION = "prod"; // the unprefixed namespace (PRODUCTION_ENV server-side)

export async function render(el, { api, go, toast, modal }) {
  const body = h("div", {});

  el.append(
    h(
      "div",
      { class: "view-head" },
      h("h1", {}, "Environments"),
      h("span", { class: "sub" }, "prod is the unprefixed namespace; other environments are prefixed sandboxes."),
    ),
    body,
  );

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
