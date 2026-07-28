// Typed-ish fetch wrapper over the interlace API (same origin — the daemon serves us).
// A key is only needed once one exists (open-until-keyed); stored locally, never sent elsewhere.
const TOKEN_KEY = "interlace.token";

export class APIError extends Error {
  constructor(status, detail) {
    super(detail || `HTTP ${status}`);
    this.status = status;
    this.detail = detail;
  }
}

export const getToken = () => localStorage.getItem(TOKEN_KEY) || "";
export const setToken = (t) => (t ? localStorage.setItem(TOKEN_KEY, t) : localStorage.removeItem(TOKEN_KEY));

async function request(method, path, body) {
  const headers = { Accept: "application/json" };
  const token = getToken();
  if (token) headers.Authorization = `Bearer ${token}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const res = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (!res.ok) throw new APIError(res.status, (data && data.detail) || String(data || res.statusText));
  return data;
}

export const api = {
  get: (path) => request("GET", path),
  post: (path, body) => request("POST", path, body),
  del: (path) => request("DELETE", path),

  health: () => request("GET", "/health"),
  models: () => request("GET", "/models"),
  model: (name) => request("GET", `/models/${encodeURIComponent(name)}`),
  plan: (env) => request("GET", `/plan?environment=${encodeURIComponent(env)}`),
  apply: (body) => request("POST", "/apply", body),
  environments: () => request("GET", "/environments"),
  dropEnvironment: (name, force) => request("DELETE", `/environments/${encodeURIComponent(name)}?force=${!!force}`),
  runs: () => request("GET", "/runs"),
  run: (id) => request("GET", `/runs/${id}`),
  createRun: (body) => request("POST", "/runs", body),
  cancelRun: (id) => request("POST", `/runs/${id}/cancel`),
  checks: (model) => request("GET", model ? `/checks?model=${encodeURIComponent(model)}` : "/checks"),
  streams: () => request("GET", "/streams"),
  stream: (name) => request("GET", `/streams/${encodeURIComponent(name)}`),
  publish: (name, payload) => request("POST", `/streams/${encodeURIComponent(name)}`, payload),
  events: (after = 0) => request("GET", `/events?after=${after}`),
};
