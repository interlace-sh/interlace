# Web UI

A zero-build single-page app served at **`/ui`** by `interlace serve`. Vanilla ES modules, no
framework, no external fonts (it renders air-gapped). It is driven entirely by the [HTTP
API](api.md) — every view is a thin client over the endpoints. Ten hash-routed views, plus a
command palette (⌘K), a live event feed, and a build dock that mirrors the CLI's ✓/✗ rows.

Live updates come over SSE (`GET /events/stream`). When a bearer token is configured the UI
passes it as `?token=` (EventSource cannot set Authorization). Rail badges, the build dock,
and the views that change with apply/run/stream activity follow that feed — there is no
polling fallback and no websocket. Reconnects resume from `Last-Event-ID`. `GET /events`
is the snapshot/replay API (the overview activity list loads its history from it once).

## Views

| View | Shows | Actions (→ endpoint) |
|---|---|---|
| **overview** | drift / active runs / failed runs / stream lag / failing checks / env count stat cards, recent runs, live event feed | navigate to any view |
| **lineage** | the whole DAG (one `/lineage` payload) with per-node output/strategy/engine, schedule ⏱ / checks ✓ marks, expandable column pins with types; streams as source nodes. The selected model's bar shows its name, relation, last status, row delta, and duration; Preview (the row sample, including a failed statement) is the default tab and Schema is the column profile | search/focus a model, expand columns, click a column to trace it through the graph, edges flow live while models build |
| **models** | filterable catalog (`#/models`: name, language, output, strategy, engine, tags, schedule). A selected model is its own page (`#/models?m=`): fingerprint (click to copy), column lineage, declared indexes and constraints (and the external `schema` policy on a terminal table), upstream/downstream, SQL or Python source, latest checks, and the same preview bar (row sample by default, column profile on Schema) | **models** back to the catalog, **trace in lineage**, **run** (`POST /runs`), **query**, per-column **impact** (`GET /models/{name}/impact`), preview (`GET /models/{name}/preview`) |
| **plan** | per-change cards with category pills, impacted columns, SQL diffs, an indexes-and-constraints card (`+ index` / `- constraint`), drift notes, cross-engine transfers, breaking warning; apply results (built/reused/promoted, row deltas, timings). A physical-only plan is not "nothing to do". A failed apply shows the engine statement | environment menu, model menu (empty = every model) with **upstream** (`+model`) and **downstream** (`model+`) switches, **changed only** (`state:modified+`), forward-only, **preview** (`GET /plan`), **apply** (`POST /apply`) with a breaking-change confirm modal |
| **runs** | the durable queue (id, models, state, attempts, trigger, window); detail: build-results table, checks summary, event timeline, and the SQL of any `model.failed` event on that run | **run…** modal (selectors / window / restate → `POST /runs`), **cancel** (`POST /runs/{id}/cancel`) |
| **query** | SQL editor (⌘⏎ to run), table browser (models + streams), typed result grid, `rows · ms · truncated` | **run** (`POST /query`), local query history |
| **streams** | per-stream card: drift policy, lag, head, watermark, pending, retention, schema, target table | **peek** (`GET /streams/{name}`), **publish…** modal (`POST /streams/{name}`) |
| **checks** | latest result per (model, check), split failing / passing. A failing row check can load the rows it rejected | model links, **show failing rows** (`GET /models/{name}/checks/{check}/rows`), **run checks** (`POST /checks/run`) |
| **environments** | table (name, models, drift, promoted-at); prod marked | **new environment…** (`POST /apply` into a sandbox), **plan**, **history…** (generations → **roll back**, `POST /environments/{name}/rollback`), **drop** (type-to-confirm → `DELETE /environments/{name}`) |
| **system** | engines (redacted DSNs), connections (redacted HTTP headers and Postgres DSNs), schedules (cron, interval, file watch, webhook; next/last fire), API keys | **new key…** (`POST /apikeys`, token shown once), **revoke** (`DELETE /apikeys/{name}`), **gc dry-run / now** (`POST /gc`), **reset…** (type-to-confirm → `POST /reset`), this-browser token field |

## Command palette (⌘K)

Jumps to a view, a model (`models?m=`), or a run by id. Backed by `GET /models`.
