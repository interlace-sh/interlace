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

`InterlaceError` → 404 if "unknown" appears in the first 40 characters of the message, else 400.
`ClientException` = 400, `NotFoundException` = 404, missing/invalid token (once keyed) = 401,
wrong scope = 403. When an engine statement fails, the error body also includes `statement`
(the SQL that failed, capped at 12 000 characters). The same text is stored on the
`model.failed` event (`payload.message`, `payload.statement`). `model.done` carries
`payload.seconds` and `payload.rows` `{inserted, updated, deleted}`.

## Endpoints

### Health & UI
- **`GET /health`** (open) → `{status, version, environment}`.
- **`GET /`** (open) → 302 redirect to `/ui/`.

### Models & lineage (read)
- **`GET /models`** → `[ModelInfo]` (topo-sorted). `ModelInfo`: `name, output, materialise,
  strategy, is_terminal, fingerprint, depends_on[], tags[], owner, schedule, engine, language`.
- **`GET /models/{name}`** → `ModelDetail` (404 if unknown): adds `upstream[], downstream[],
  columns{col: [sources]}, sql, source` (Python source), `indexes[]` (`columns, unique, name`),
  `constraints[]` (`type, name, columns, expression, reference, fields`), and `schema`
  (`columns` / `indexes` / `constraints` drift policy). `name` on an index or constraint is
  the object interlace will create (`il__<model>__…` unless the model set one).
- **`GET /models/{name}/impact?column=COL`** → `ImpactResponse` `{source, impacted:
  [{model, column, via}], opaque_consumers[]}` — the column blast radius (mirrors
  `interlace impact`).
- **`GET /models/{name}/preview?environment=&limit=`** → `SampleResponse`. Reads the
  environment view when it exists, otherwise the snapshot table for the current fingerprint
  (a blocked promotion can still be inspected). `limit` defaults to 25 and is capped at 100.
  `{available, message, relation, columns, types, rows, row_count, truncated, profile[],
  last_build}`. `profile[]` is `{column, type, nulls, distinct, min, max}` from one aggregate;
  min/max are omitted when the engine cannot cast them. `last_build` is the newest
  `model.done` / `model.failed` / `model.cancelled` (`status, at, seconds, rows, message,
  statement`). Ephemeral and file outputs set `available` false. Unknown model → 404.
- **`GET /models/{name}/checks/{check}/rows?environment=&limit=`** → the same
  `SampleResponse`, with an empty profile. The rows that check rejected, from the snapshot
  the check ran against. `row_count` and `freshness` (they judge the table) and Python
  checks set `available` false. `unique` returns the duplicate rows; the recorded failure
  count stays the number of duplicate groups. Unknown check → 404. Does not change the
  promotion gate.
- **`GET /lineage`** → `LineageResponse` `{models[], edges[[up,down]], columns{...}, streams[]}` —
  the whole graph in one payload (nodes carry warehouse-described column types, schedule/check
  flags; streams appear as source nodes).

### Plan & apply
- **`GET /plan?environment=&select=&forward_only=`** (read) → `PlanResponse` `{environment,
  changes: [Change], transfers[], physical[], drift[]}`. `physical` is `"+ index <name>"` /
  `"- constraint <name>"` lines that do not rebuild data. `drift` reports external-table
  differences (extra columns, unmanaged indexes); a `schema.columns: reject` mismatch fails
  the plan before any write. `Change`: `name, change_type, category,
  previous_fingerprint, new_fingerprint, impacted_columns[], new_sql, previous_sql, reused`.
  Selector errors → 400.
- **`POST /apply`** (write) → `ApplyResponse`. Body `ApplyRequest` `{selectors[], environment,
  force, forward_only}`. Runs diff → build → promote under a cross-process warehouse lock
  (flushing streams first). A breaking plan without `force` → 409 listing the breaking models.
  Blocking schema drift (`schema.columns: reject`) → 400 before any write; `force` does not
  bypass it. A blocking check failure → 400 (`apply.blocked` event). Lock contention → 409. Response:
  `{environment, built[], promoted, breaking, reused[], transfers[],
  rows{model:{inserted,updated,deleted}}, timings{model:sec}, gated[], checks[]}`.
- **`POST /run`** (write) → `ApplyResponse`. Body `CreateRun` `{selectors[], environment,
  start, end, restate}` — force-builds immediately (CLI `interlace run` / `restate` parity).
  Prefer `POST /runs` to enqueue onto the durable queue.

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
  running scheduler drains it). Empty selectors = all models. Emits `run.enqueued`. For a
  synchronous force-build use `POST /run` instead.
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
- **`GET /streams/{name}/events?after=&group=&token=`** (read, SSE) → a tail of the durable
  log for external consumers. Each data frame is `{offset, ts, payload, idempotency_key,
  headers}` with `id` set to the offset. A comment frame is sent on connect so EventSource
  opens before any event. The tail blocks until an append wakes it, and sends a keepalive
  comment after 15s of quiet. No cursor (`after` / `Last-Event-ID`)
  starts at the current head (live only); `after=0` replays from the first offset. Reconnects
  resume from `Last-Event-ID`. `<name>__quarantine` is the shadow log when the stream's drift
  mode is `quarantine`. Sending a frame does **not** ack it.
  With `group`, the tail takes that consumer group's lease (409 if another owner holds it)
  and, unless a cursor was given, resumes from the group's committed offset. The first frame
  is `event: lease` with `{group, token, committed_offset}` and no `id`. The lease lasts 30s
  and is renewed while the connection is open; disconnect releases it without moving the
  committed offset. Pass the API key as `?token=` on this path (EventSource cannot send
  `Authorization`).
- **`POST /streams/{name}/commit`** (write) → **201** `StreamCommitResult {group,
  committed_offset}`. Body `{group, offset, token}`. The token is the one from the lease
  frame. A stale token (the lease was released, expired, or taken by someone else) is **400**.

### Query console
- **`POST /query`** (read) → `QueryResponse {columns, types, rows, row_count, truncated,
  elapsed_ms}`. Body `{sql, limit=500}` (limit capped at 10 000). **SELECT only**: the SQL is
  parsed and fenced *before* execution — exactly one `Select`/`Union` at the top level, and every
  table source must be a real table or view (table functions and file/HTTP readers like
  `read_csv`, `query`, `glob` are rejected, named or not), so it can only read the warehouse. The
  same fence backs `interlace query` on the CLI. 30s timeout; ~8 MB cell cap.

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
- **`POST /reset`** (admin) → `ResetResponse {dropped_views[], dropped_schemas[],
  cleared_snapshots, kept_terminals[], environments[], stream_log_cleared, dry_run}`.
  Body `{confirm=false, dry_run=false}`. Requires `confirm=true` unless `dry_run`. Wipes
  Interlace-owned views/snapshots/runs/events/streams; does **not** drop table/file
  destinations and keeps those models recorded so the next apply will not re-deliver.
  Emits `reset.finished`.

### Events (read)
- **`GET /events?after=`** → `[EventInfo]` `{seq, ts, type, entity, payload}` after a cursor.
- **`GET /events/stream?after=&token=`** (SSE) → the live event tail. Reconnects resume from
  `Last-Event-ID`; a slow client is dropped and reconnects. A comment frame is sent on connect
  so EventSource opens before any event, then every 15s as a keepalive. Event types: `run.enqueued`,
  `run.cancel_requested`, `apply.started/blocked/finished`, `run.started/finished`,
  `model.*` (per-model build progress), `stream.flushed`, `environment.dropped/rolled_back`,
  `gc.finished`, `reset.finished`. EventSource cannot send an `Authorization` header — pass the API key as
  `?token=` on this path and on `GET /streams/{name}/events` (the in-package UI does, for the
  operator feed). Prefer the Bearer header everywhere else.
