// API client: bearer-token fetch + one shared SSE event feed. Every consumer
// subscribes to the same EventSource — one upstream connection no matter how
// many views are listening. Reconnects resume from lastSeq (?after= and the
// browser's Last-Event-ID); a daemon that's down is retried with backoff.

const TOKEN_KEY = "interlace.token";

export const token = {
  get: () => localStorage.getItem(TOKEN_KEY) || "",
  set: (value) => (value ? localStorage.setItem(TOKEN_KEY, value) : localStorage.removeItem(TOKEN_KEY)),
};

class ApiError extends Error {
  constructor(status, detail, statement) {
    super(detail || `HTTP ${status}`);
    this.status = status;
    this.statement = statement || "";
  }
}

async function call(method, path, body) {
  const headers = {};
  if (token.get()) headers["Authorization"] = `Bearer ${token.get()}`;
  if (body !== undefined) headers["Content-Type"] = "application/json";
  const response = await fetch(path, { method, headers, body: body === undefined ? undefined : JSON.stringify(body) });
  if (!response.ok) {
    let detail = response.statusText;
    let statement = "";
    try {
      const body = await response.json();
      detail = body.detail || detail;
      statement = body.statement || "";
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(response.status, detail, statement);
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
let feedState = "connecting"; // connecting | live
let stateListeners = new Set();
let source = null;
let reconnectTimer = null;
let sseBackoff = 1000; // reconnect delay, grows to a cap and resets on a clean open
let wanted = false; // false while the page is hidden — close() must not schedule a reconnect
let generation = 0; // invalidate an in-flight EventSource when start/stop races

function emit(event) {
  if (event.seq) lastSeq = Math.max(lastSeq, event.seq);
  for (const listener of listeners) listener(event);
}

function setFeedState(next) {
  if (feedState === next) return;
  feedState = next;
  for (const listener of stateListeners) listener(next);
}

function connect() {
  wanted = true;
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  const mine = ++generation;
  if (source) {
    source.onerror = null; // close() fires error; this generation must not reconnect itself
    source.close();
    source = null;
  }
  // EventSource cannot send Authorization headers. Pass ?token= (auth.py accepts
  // it on /events/stream only) so keyed clients still get a live SSE feed.
  const auth = token.get();
  const qs = new URLSearchParams({ after: String(lastSeq) });
  if (auth) qs.set("token", auth);
  source = new EventSource(`/events/stream?${qs}`);
  source.onopen = () => {
    if (mine !== generation) return;
    sseBackoff = 1000; // a clean connection resets the backoff
    setFeedState("live");
  };
  source.onmessage = (message) => {
    if (mine !== generation) return;
    try {
      emit(JSON.parse(message.data));
    } catch {
      /* keepalive */
    }
  };
  source.onerror = () => {
    if (mine !== generation) return;
    source.close();
    source = null;
    if (!wanted) return;
    setFeedState("connecting");
    // exponential backoff with a ceiling — a daemon that's down (or a proxy dropping
    // the stream) must not be hammered every 2s forever
    reconnectTimer = setTimeout(connect, sseBackoff);
    sseBackoff = Math.min(sseBackoff * 2, 30000);
  };
}

// Tear the upstream down without dropping subscribers — used on pagehide so an open
// EventSource can't keep the page out of the back/forward cache; start() restores it
// (lastSeq is preserved, so the server replays anything missed in between).
function stop() {
  wanted = false;
  generation += 1;
  clearTimeout(reconnectTimer);
  reconnectTimer = null;
  if (source) {
    source.onerror = null;
    source.close();
    source = null;
  }
  setFeedState("connecting");
}

export const feed = {
  start: connect,
  stop,
  on(listener) {
    listeners.add(listener);
    return () => listeners.delete(listener);
  },
  onState(listener) {
    stateListeners.add(listener);
    listener(feedState);
    return () => stateListeners.delete(listener);
  },
};
