# HTTP API reference

A Litestar app served by `interlace serve` (needs the `service` extra). msgspec structs are
the wire types; OpenAPI + Scalar docs at `/schema` (`/schema/scalar`). All responses are JSON.

## Authentication

Scoped API keys (`ilk_...` tokens; `Authorization: Bearer <token>`). Three scopes: **read**,
**write**, **admin** (`admin` satisfies any requirement). A route with no declared scope
requires `read`.

- **Open until the first key exists.** With zero API keys, every request is allowed (local
  dev). Create one (`interlace apikey create` or `POST /apikeys`) to enforce auth; the last
  key can't be revoked while it's the only one. `interlace serve` warns when bound to a
  non-loopback host with no keys.
- Always-open paths regardless of keys: `/health`, `/`, `/ui/*`, `/schema/*`.

## Errors

`InterlaceError` → 404 if the message begins with "unknown", else 400. `ClientException` =
400, `NotFoundException` = 404, missing/invalid token (once keyed) = 401, wrong scope = 403.

## Endpoints

### Health & UI
- **`GET /health`** (open) → `{status, version, environment}`.
- **`GET /`** (open) → 302 redirect to `/ui/`.

### Models & lineage (read)
- **`GET /models`** → `[ModelInfo]` (topo-sorted). `ModelInfo`: `name, output, materialise,
  strategy, is_sink, fingerprint, depends_on[], tags[], owner, schedule, engine, language`.
- **`GET /models/{name}`** → `ModelDetail` (404 if unknown): adds `upstream[], downstream[],
  columns{col: [sources]}, sql, source` (Python source).
- **`GET /models/{name}/impact?column=COL`** → `ImpactResponse` `{source, impacted:
  [{model, column, via}], opaque_consumers[]}` — the column blast radius (mirrors
  `interlace impact`).
- **`GET /lineage`** → `LineageResponse` `{models[], edges[[up,down]], columns{...}, streams[]}` —
  the whole graph in one payload (nodes carry warehouse-described column types, schedule/check
  flags; streams appear as source nodes).

### Plan & apply
- **`GET /plan?environment=&select=&forward_only=`** (read) → `PlanResponse` `{environment,
  changes: [Change], transfers[]}`. `Change`: `name, change_type, category,
  previous_fingerprint, new_fingerprint, impacted_columns[], new_sql, previous_sql, reused`.
  Selector errors → 400.
- **`POST /apply`** (write) → `ApplyResponse`. Body `ApplyRequest` `{selectors[], environment,
  force, forward_only}`. Runs diff → build → promote under a warehouse lock (flushing streams
  first). A breaking plan without `force` → 400 listing the breaking models. A blocking check
  failure → 400 (`apply.blocked` event). Response: `{environment, built[], promoted, breaking,
  reused[], transfers[], rows{model:{inserted,updated,deleted}}, timings{model:sec}}`.

### Environments
- **`GET /environments`** (read) → `[EnvironmentInfo]` `{name, models, changed, promoted_at}`.
- **`DELETE /environments/{name}?force=`** (admin) → `{environment, dropped_views}`. Prod needs
  `force=true`; unknown → 404. Emits `environment.dropped`.
- **`GET /environments/{name}/history`** (read) → `[{generation, promoted_at, models}]`
  (rollback targets, newest first).
- **`POST /environments/{name}/rollback`** (admin) → repoint views at a generation. Body
  `{generation?}` (default: the one before latest). Emits `environment.rolled_back`.

### Runs
- **`GET /runs`** (read) → `[RunInfo]` `{id, flow_selector[], state, attempts, error,
  enqueued_at, priority, partition, restate, idempotency_key}`.
- **`GET /runs/{id}`** (read) → `RunDetail` (adds `events: [EventInfo]`); 404 if unknown.
- **`POST /runs`** (write) → `CreateRunResult {enqueued, models[]}`. Body `CreateRun
  {selectors[], environment, start, end, restate}` — enqueues onto the durable queue (a
  running scheduler drains it). Empty selectors = all models. Emits `run.enqueued`.
- **`POST /runs/{id}/cancel`** (write) → `{id, state}`; 404 if unknown/finished. Emits
  `run.cancel_requested`.

### Checks
- **`GET /checks?model=`** (read) → `[CheckResultInfo]` `{id, environment, model, fingerprint,
  check_name, check_type, severity, status, failures, message, executed_at}`.
- **`POST /checks/run`** (write) → `RunChecksResponse {environment, outcomes[], skipped[],
  passed, blocking_failures}`. Body `{environment?, selectors[]}`. Runs checks against promoted
  tables without rebuilding; 404 if the env isn't promoted.

### Streams
- **`GET /streams`** (read) → `[StreamInfo]` `{name, schema, table, head, watermark, pending,
  on_schema_drift, retention}`.
- **`GET /streams/{name}`** (read) → `StreamDetail` (adds `idempotency_key, recent[]`); 404 if
  unknown.
- **`POST /streams/{name}`** (write) → `PublishResult {accepted, deduplicated, last_offset,
  quarantined}`. Body: a JSON object or an array of objects (raw, not a struct). Durable before
  it returns. **429** when unmaterialised pending > 100 000. Drift handled per the stream's
  `on_schema_drift` (`reject` → 400 on bad data; `evolve`; `quarantine`).

### Query console
- **`POST /query`** (read) → `QueryResponse {columns, types, rows, row_count, truncated,
  elapsed_ms}`. Body `{sql, limit=500}` (limit capped at 10 000). **SELECT only**: exactly one
  statement, `Select`/`Union` at top level, and external/file/HTTP reader functions
  (`read_csv`, `query`, `glob`, …) rejected — the query runs on a **sandboxed cursor with
  external access disabled**, so it can only read the warehouse. 30s timeout; ~8 MB cell cap.

### System (admin)
- **`GET /engines`** (read) → `[EngineInfo]` `{name, type, dialect, database (redacted),
  default}`.
- **`GET /schedules`** (read) → `[ScheduleInfo]` `{model, kind, expression, next_fire,
  last_fired}`.
- **`GET /apikeys`** (admin) → `[ApiKeyInfo]` `{name, scopes, created_at}`.
- **`POST /apikeys`** (admin) → `{name, scopes, token}` (token shown once). Body `{name,
  scopes=["read"]}`.
- **`DELETE /apikeys/{name}`** (admin) → `{name, removed}`; 404 if none; 400 if it would remove
  the last key.
- **`POST /gc`** (admin) → `GcResponse {removed_snapshots, dropped_tables[], kept_snapshots,
  dry_run}`. Body `{grace="7d", dry_run=false}`. Emits `gc.finished`.

### Events (read)
- **`GET /events?after=`** → `[EventInfo]` `{seq, ts, type, entity, payload}` after a cursor.
- **`GET /events/stream?after=`** (SSE) → the live event tail. Reconnects resume from
  `Last-Event-ID`; a slow client is dropped and reconnects. Event types: `run.enqueued`,
  `run.cancel_requested`, `apply.started/blocked/finished`, `model.*` (per-model build
  progress), `stream.flushed`, `environment.dropped/rolled_back`, `gc.finished`.
  (EventSource can't send an `Authorization` header, so keyed clients poll `GET /events`
  instead.)
