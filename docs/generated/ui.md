# Web UI

A zero-build single-page app served at **`/ui`** by `interlace serve`. Vanilla ES modules, no
framework, no external fonts (it renders air-gapped). It is driven entirely by the [HTTP
API](api.md) — every view is a thin client over the endpoints. Ten hash-routed views, plus a
command palette (⌘K), a live event feed, and a build dock that mirrors the CLI's ✓/✗ rows.

Live updates come over SSE (`GET /events/stream`); when a bearer token is configured the UI
falls back to polling `GET /events` (EventSource can't send auth headers). Rail badges show
pending plan changes and active runs; the build dock narrates per-model `model.*` events as
apply runs.

## Views

| View | Shows | Actions (→ endpoint) |
|---|---|---|
| **overview** | drift / active runs / failed runs / stream lag / failing checks / env count stat cards, recent runs, live event feed | navigate to any view |
| **lineage** | the whole DAG (one `/lineage` payload) with per-node output/strategy/engine, schedule ⏱ / checks ✓ marks, expandable column pins with types; streams as source nodes | search/focus a model, expand columns, click a column to trace it through the graph, edges flow live while models build |
| **models** | filterable catalog (name, language, output, strategy, engine, tags, schedule); detail: fingerprint (click to copy), column lineage, upstream/downstream, SQL or Python source, latest checks | **trace in lineage**, **run** (`POST /runs`), **query**, per-column **impact** (`GET /models/{name}/impact`) |
| **plan** | per-change cards with category pills, impacted columns, SQL diffs, cross-engine transfers, breaking warning; apply results (built/reused/promoted, row deltas, timings) | environment field, selector input, **changed only** (`state:modified+`), forward-only, **preview** (`GET /plan`), **apply** (`POST /apply`) with a breaking-change confirm modal |
| **runs** | the durable queue (id, models, state, attempts, trigger, window); detail: build-results table, checks summary, event timeline | **run…** modal (selectors / window / restate → `POST /runs`), **cancel** (`POST /runs/{id}/cancel`) |
| **query** | SQL editor (⌘⏎ to run), table browser (models + streams), typed result grid, `rows · ms · truncated` | **run** (`POST /query`), local query history |
| **streams** | per-stream card: drift policy, lag, head, watermark, pending, retention, schema, target table | **peek** (`GET /streams/{name}`), **publish…** modal (`POST /streams/{name}`) |
| **checks** | latest result per (model, check), split failing / passing | model links, **run checks** (`POST /checks/run`) |
| **environments** | table (name, models, drift, promoted-at); prod marked | **new environment…** (`POST /apply` into a sandbox), **plan**, **history…** (generations → **roll back**, `POST /environments/{name}/rollback`), **drop** (type-to-confirm → `DELETE /environments/{name}`) |
| **system** | engines (redacted DSNs), schedules (next/last fire), API keys | **new key…** (`POST /apikeys`, token shown once), **revoke** (`DELETE /apikeys/{name}`), **gc dry-run / now** (`POST /gc`), this-browser token field |

## Command palette (⌘K)

Jumps to a view, a model (`models?m=`), or a run by id. Backed by `GET /models`.
