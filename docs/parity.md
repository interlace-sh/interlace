# Surface parity — CLI ↔ API ↔ UI

The same functionality is reachable from the [CLI](cli.md), the [HTTP API](api.md), and the
[web UI](ui.md). This table is the authoritative map; where a capability is intentionally
limited to one surface, the reason is given.

| Capability | CLI | API | UI |
|---|---|---|---|
| List / inspect models | `models`, `lineage` | `GET /models`, `/models/{name}`, `/lineage` | models, lineage |
| Column impact / blast radius | `impact` | `GET /models/{name}/impact` | models (per-column) |
| Preview a plan | `plan` | `GET /plan` | plan |
| Apply (build + promote) | `apply` | `POST /apply` | plan |
| Force-run / restate | `run`, `restate` | `POST /runs` (queued) | runs (queued) |
| Environments: list / drift | `env list` | `GET /environments` | environments |
| Environments: drop | `env drop` | `DELETE /environments/{name}` | environments |
| Environments: rollback + history | `env rollback [--list]` | `POST .../rollback`, `GET .../history` | environments |
| Checks: history | `checks list` | `GET /checks` | checks |
| Checks: run ad hoc | `checks run` | `POST /checks/run` | checks |
| Streams: inspect | `streams` | `GET /streams`, `/streams/{name}` | streams |
| Streams: publish | — | `POST /streams/{name}` | streams |
| Query console | — | `POST /query` | query |
| Engines | `engines` | `GET /engines` | system |
| Schedules | (via `models`) | `GET /schedules` | system |
| API keys | `apikey create/revoke/list` | `/apikeys` (GET/POST/DELETE) | system |
| Garbage collection | `gc` | `POST /gc` | system |
| Runs: list / cancel | `runs`, `cancel` | `GET /runs`, `POST .../cancel` | runs |
| Events | — | `GET /events`, `/events/stream` | live feed |
| Scaffold a project | `init` | — | — |
| Run the daemon | `serve`, `scheduler` | — | — |

## Intentionally surface-specific

- **CLI-only** — `init` (scaffolds files on disk), `serve`/`scheduler` (they *are* the process
  that hosts the API), and `lineage --format dot` (a Graphviz export; the API returns lineage
  as JSON via `GET /lineage`, which the UI renders as an interactive canvas).
- **API/UI-only** — stream **publish** (`POST /streams/{name}`) and the **query console**
  (`POST /query`) are HTTP operations against a running daemon; there's no `interlace publish`
  or `interlace query`. Live **events** (`GET /events`) are an API/UI concern.
- **Enqueue vs immediate** — `interlace run`/`restate` build **immediately** in the CLI
  process; `POST /runs` (and the UI "run…") **enqueue** onto the durable queue for a running
  scheduler to drain. `POST /apply` (and the UI apply) build immediately in the daemon.

Every HTTP endpoint is exercised by at least one UI view — there are no API features hidden
from the UI. Some response fields (e.g. a check's `message`, a run's `priority`) are carried
on the wire but not yet rendered; those are display gaps, not capability gaps.
