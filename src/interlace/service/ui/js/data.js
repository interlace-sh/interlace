// The view-facing data layer: caches API responses and derives the prototype's
// model map (layer, state, running, downstream) from them. Views call load*();
// invalidate() drops caches so the next view render refetches.
import { api } from "./api.js";
import { changeState } from "./util.js";

const TTL = 5000; // ms — cheap endpoints; views refetch on navigation anyway
const cache = new Map(); // key -> {at, value}

async function cached(key, loader) {
  const hit = cache.get(key);
  if (hit && Date.now() - hit.at < TTL) return hit.value;
  const value = await loader();
  cache.set(key, { at: Date.now(), value });
  return value;
}

export const invalidate = (prefix = "") => {
  for (const key of [...cache.keys()]) if (key.startsWith(prefix)) cache.delete(key);
};

export const currentEnv = () => localStorage.getItem("interlace.env") || "prod";
export const setEnv = (env) => localStorage.setItem("interlace.env", env);

export const loadPlan = () => cached(`plan:${currentEnv()}`, () => api.plan(currentEnv()));
export const loadRuns = () => cached("runs", () => api.runs());
export const loadStreams = () => cached("streams", () => api.streams());
export const loadChecks = () => cached("checks", () => api.checks());
export const loadEnvironments = () => cached("envs", () => api.environments());
export const loadDetail = (name) => cached(`model:${name}`, () => api.model(name));
export const loadHealth = () => cached("health", () => api.health());

// The model map every view leans on: ModelInfo + derived layer/state/running/down,
// in the prototype's shape. Detail fields (cols, sql, upstream/downstream closure)
// hydrate lazily per model via loadDetail().
export async function loadModels() {
  return cached("models", async () => {
    const [infos, plan, runs] = await Promise.all([api.models(), loadPlan().catch(() => null), loadRuns().catch(() => [])]);
    const byName = {};
    const names = [];
    for (const info of infos) {
      names.push(info.name);
      byName[info.name] = {
        ...info,
        mat: info.output, // "sink" or the materialise value — the prototype's `mat`
        layer: 0,
        state: "unchanged",
        running: false,
        down: [],
      };
    }
    for (const model of Object.values(byName)) {
      for (const dep of model.depends_on) byName[dep]?.down.push(model.name);
    }
    for (const name of names) {
      // /models is topologically sorted, so dependency layers are already final
      const m = byName[name];
      m.layer = m.depends_on.length ? Math.max(...m.depends_on.map((d) => (byName[d]?.layer ?? 0) + 1)) : 0;
    }
    if (plan) for (const change of plan.changes) {
      if (byName[change.name]) byName[change.name].state = change.reused ? "unchanged" : changeState(change);
    }
    for (const run of runs) {
      if (run.state !== "running") continue;
      for (const sel of run.flow_selector) {
        const bare = sel.replace(/^\+|\+$/g, "");
        if (byName[bare]) byName[bare].running = true;
      }
    }
    return { byName, names };
  });
}

// Prototype column shape: [[out, srcTable, srcCol]] — first source ref wins for the DAG.
export function toCols(detail) {
  const entries = Object.entries(detail.columns || {});
  return entries.map(([out, refs]) => {
    if (!refs || !refs.length) return [out, "", "—"];
    const [table, col] = String(refs[0]).split(/\.(?=[^.]+$)/); // last dot: table may be schema-qualified
    return [out, table, col ?? "—"];
  });
}
