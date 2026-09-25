# Surface parity — CLI ↔ API ↔ UI

The same functionality is reachable from the [CLI](cli.md), the [HTTP API](api.md), and the
[web UI](ui.md). This table is the authoritative map; where a capability is intentionally
limited to one surface, the reason is given.

| Capability | CLI | API | UI |
|---|---|---|---|
| List / inspect models | `models`, `lineage` | `GET /models`, `/models/{name}`, `/lineage` | models, lineage |
| Row sample and column profile | `mcp` `preview_model` | `GET /models/{name}/preview` | models, lineage (selected node) |
| Column impact / blast radius | `impact` | `GET /models/{name}/impact` | models (per-column) |
| Preview a plan | `plan` | `GET /plan` | plan |
| Apply (build + promote) | `apply` | `POST /apply` | plan |
| Force-run / restate (sync) | `run`, `restate` | `POST /run` | — |
| Force-run / restate (queued) | — | `POST /runs` | runs (queued) |
| Environments: list / drift | `env list` | `GET /environments` | environments |
| Environments: drop | `env drop` | `DELETE /environments/{name}` | environments |
| Environments: rollback + history | `env rollback [--list]` | `POST .../rollback`, `GET .../history` | environments |
| Checks: history | `checks list` | `GET /checks` | checks |
| Checks: failing rows | `mcp` `failing_rows` | `GET /models/{name}/checks/{check}/rows` | checks |
| Checks: run ad hoc | `checks run` | `POST /checks/run` | checks |
| Streams: inspect | `streams` | `GET /streams`, `/streams/{name}` | streams |
| Streams: publish | — | `POST /streams/{name}` | streams |
| Streams: consume | — | `GET /streams/{name}/events` (SSE), `POST /streams/{name}/commit` | streams |
| Query console | — | `POST /query` | query |
| Engines | `engines` | `GET /engines` | system |
| Schedules | (via `models`) | `GET /schedules` | system |
| API keys | `apikey create/revoke/list` | `/apikeys` (GET/POST/DELETE) | system |
| Garbage collection | `gc` | `POST /gc` | system |
| Reset (fresh start) | `reset` | `POST /reset` | system |
| Runs: list / cancel | `runs`, `cancel` | `GET /runs`, `POST .../cancel` | runs |
| Events | — | `GET /events`, `/events/stream` | live feed |
| Scaffold a project | `init` | — | — |
| Run the daemon | `serve`, `scheduler` | — | — |
| MCP (stdio) | `mcp` | — | — |

## Intentionally surface-specific

- **CLI-only** — `init` (scaffolds files on disk), `serve`/`scheduler` (they *are* the process
  that hosts the API), `mcp` (a stdio server over the same project; `apply` refuses unless
  `confirm` is true), and `lineage --format dot` (a Graphviz export; the API returns lineage
  as JSON via `GET /lineage`, which the UI renders as an interactive canvas).
- **API/UI-only** — stream **publish** (`POST /streams/{name}`) and the external **consumer
  tail** (`GET /streams/{name}/events`, acked with `POST /streams/{name}/commit`) are HTTP
  operations against a running daemon; there's no `interlace publish`. Live **operator events**
  (`GET /events/stream`; snapshot `GET /events`) are an API/UI concern. (Ad-hoc read-only SQL is on **both** surfaces —
  `interlace query "SELECT …"` and the `POST /query` console share one parse-and-fence path.)
- **Enqueue vs immediate** — `interlace run`/`restate` and `POST /run` build **immediately**;
  `POST /runs` (and the UI "run…") **enqueue** onto the durable queue for a running
  scheduler to drain. `POST /apply` (and the UI apply) build immediately in the daemon.

Every HTTP endpoint is exercised by at least one UI view, except the external consumer tail
(`GET /streams/{name}/events` and `POST /streams/{name}/commit`), which is for subscribers
outside the operator UI. Some response fields (e.g. a check's `message`, a run's `priority`)
are carried on the wire but not yet rendered; those are display gaps, not capability gaps.
