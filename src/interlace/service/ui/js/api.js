// API client: bearer-token fetch + one shared event feed (SSE with replay,
// polling fallback). Every consumer subscribes to the same feed — one upstream
// connection no matter how many views are listening.

const TOKEN_KEY = "interlace.token";

export const token = {
  get: () => localStorage.getItem(TOKEN_KEY) || "",
  set: (value) => (value ? localStorage.setItem(TOKEN_KEY, value) : localStorage.removeItem(TOKEN_KEY)),
};

class ApiError extends Error {
  constructor(status, detail) {
    super(detail || `HTTP ${status}`);
    this.status = status;
  }
}

async function call(method, path, body) {
  const headers = {};
  if (token.get()) headers["Authorization"] = `Bearer ${token.get()}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  if (!response.ok) {
    let detail = response.statusText;
    try {
      detail = (await response.json()).detail || detail;
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(response.status, detail);
  }
  if (response.status === 204) return null;
  return response.json();
}

export const api = {
  get: (path) => call("GET", path),
  post: (path, body) => call("POST", path, body ?? {}),
  del: (path) => call("DELETE", path),
};

// ---- live event feed ---------------------------------------------------------

const listeners = new Set();
let lastSeq = 0;
let feedState = "connecting"; // connecting | live | poll
let stateListeners = new Set();
let source = null;
let pollTimer = null;

function emit(event) {
  if (event.seq) lastSeq = Math.max(lastSeq, event.seq);
  for (const listener of listeners) listener(event);
}

function setFeedState(next) {
  feedState = next;
  for (const listener of stateListeners) listener(next);
}

function startPolling() {
  if (pollTimer) return;
  pollTimer = -1; // claimed synchronously: a second connect() in the first tick's await window must not double-start
  setFeedState("poll");
  const tick = async () => {
    try {
      const events = await api.get(`/events?after=${lastSeq}`);
      for (const event of events) emit(event);
    } catch {
      /* daemon away; keep trying */
    }
    pollTimer = setTimeout(tick, 1500);
  };
  tick();
}

function connect() {
  // EventSource cannot send Authorization headers; with a token configured we
  // poll instead (same events, ~1.5s cadence). Keyless local use gets SSE.
  if (token.get()) return startPolling();
  source = new EventSource(`/events/stream?after=${lastSeq}`);
  source.onopen = () => setFeedState("live");
  source.onmessage = (message) => {
    try {
      emit(JSON.parse(message.data));
    } catch {
      /* keepalive */
    }
  };
  source.onerror = () => {
    source.close();
    source = null;
    setFeedState("connecting");
    setTimeout(connect, 2000);
  };
}

export const feed = {
  start: connect,
  on(listener) {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  onState(listener) {
    stateListeners.add(listener);
    listener(feedState);
    return () => stateListeners.delete(listener);
  },
  get state() {
    return feedState;
  },
};
