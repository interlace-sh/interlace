# CLI reference

`interlace <command>`. Global: `--version` / `-v`. Shared options across commands:

- `--env` / `-e` (default `prod`, env `INTERLACE_ENV`) — target environment.
- `--path` / `-p` (default `.`) — project root.
- `--select` / `-s` (repeatable) — model selectors (see [selectors](#selectors)).
- `--json` — emit JSON instead of a table.
- `--parallelism` (default 0 = the project's `parallelism`) — models built at once.

Exit codes: `0` ok; `1` selection error / breaking-plan-without-force / check failure /
unknown target / guard tripped; `2` malformed input (bad ISO window, bad grace, bad format).

## Transformation

### `interlace init [PATH] [--name NAME] [--template NAME] [--list]`
Scaffold a new project from a template (writes `interlace.yaml`, `models/`, a README).
`--template/-t` picks the starter (default `quickstart`); `--list` shows every template and any
credentials it needs. Filesystem only. Bundled templates:

| Template | What it scaffolds | Needs |
|---|---|---|
| `quickstart` (default) | A no-source SQL → Python → SQL chain with checks | — |
| `events` | Durable `@stream` HTTP ingestion + exactly-once live rollups | `[service]` extra to run |
| `github` | Incremental pull of GitHub issues via the REST source client | `[sources]` extra |
| `postgres` | Incremental pull from a Postgres source (bundled seeded docker-compose) | Docker + `[postgres]` extra |

### `interlace plan [--env] [--select] [--forward-only] [--json]`
Preview what `apply` would change in an environment, without building. Opens the state store
only (no warehouse). `--json` mirrors the HTTP `PlanResponse` shape. `--forward-only` previews
history-inheriting plans.

### `interlace apply [--env] [--select] [--forward-only] [--force] [--parallelism]`
Build changed models, run their checks, and promote the environment. Refuses to proceed on a
**breaking** plan unless `--force`. Shows live per-model build rows (✓/✗/⊘). A blocking check
failure aborts before promotion (exit 1). Needs a live warehouse.

### `interlace run [--env] [--select] [--start] [--end] [--parallelism]`
Force-build models regardless of change detection, then promote. `--start`/`--end` set the
catch-up window for `incremental` models (default: the latest grain); it fills only
windows the interval ledger doesn't yet cover.

### `interlace restate [--env] [--select] [--start] [--end] [--parallelism]`
Like `run`, but **reprocesses** the window even where the ledger already covers it — for
correcting historical data.

### `interlace gc [--grace 7d] [--dry-run]`
Garbage-collect snapshots no environment references and drop their physical tables
(reference-aware). On a real run also trims the event log / check results / finished runs
older than 30 days and sweeps expired stream events.

## Inspection (no warehouse)

### `interlace models [--select] [--json]`
List models with materialisation, strategy, engine (shown when multi-engine), and
dependencies. Compile only.

### `interlace lineage MODEL [--columns] [--format text|json|dot]`
A model's upstream/downstream lineage; `--columns` adds column-level lineage; `--format dot`
emits a Graphviz digraph (pipe to `dot -Tsvg`). Compile only.

### `interlace impact MODEL.COLUMN [--json]`
Column-level blast radius: every downstream column transitively derived from `MODEL.COLUMN`,
plus opaque consumers (Python models / `*` projections that read the source whole). Compile
only. (Same data as the HTTP `GET /models/{name}/impact`.)

### `interlace engines [--json]`
Configured engines (name, type, dialect, DSN with credentials redacted). Config read only.

### `interlace streams [--json]`
Declared streams with drift policy, retention, log head, warehouse watermark, and pending
backlog. Opens the warehouse + stream log.

### `interlace query "SELECT ..." [--limit/-n 100]`
Run a read-only `SELECT` against the warehouse and print the result. `SELECT` only — the same
parse-time fence as the web console (real tables and views, never table functions or file
readers). Unqualified names resolve to the promoted (prod) views; capped at `--limit` rows (max
10,000). Opens the warehouse. (Same fence as the HTTP `POST /query`.)

## Environments — `interlace env ...`

### `interlace env list [--json]`
Environments with promoted-model counts and drift vs the compiled project.

### `interlace env drop NAME [--force]`
Remove an environment's views; its snapshots become gc-reclaimable. Dropping `prod` needs
`--force`.

### `interlace env rollback [NAME] [--to N] [--list] [--json]`
Repoint an environment's views at an earlier promotion generation — **nothing rebuilds**.
`--to N` picks a generation (default: the one before the latest); `--list` shows the promotion
history instead (state only). The rollback itself needs the warehouse.

## Checks — `interlace checks ...`

### `interlace checks list [--model M] [--limit N] [--json]`
Recent recorded check results (newest first). State only.

### `interlace checks run [--env] [--select] [--json]`
Run checks ad hoc against an environment's promoted tables (no rebuild), recording results.
Skips sinks and declared-but-not-promoted models; exits 1 if any error-severity check fails.

## Runs & the daemon

### `interlace runs [--limit 20] [--json]`
Recent runs from the durable queue (newest first). State only.

### `interlace cancel RUN_ID`
Cancel a run — queued cancels immediately, running cancels at the worker's next heartbeat.

### `interlace scheduler [--env] [--interval 60] [--once]`
Run the scheduler loop: tick triggers, flush streams, drain due runs, sweep stream retention.
`--once` does a single tick and exits. Needs a live warehouse.

### `interlace serve [--env] [--host 127.0.0.1] [--port 8000] [--scheduler/--no-scheduler] [--interval 60] [--quack] [--quack-token]`
Run the daemon: HTTP API + web UI (`/ui`) + scheduler + streams in one process. Requires the
`service` extra. `--no-scheduler` runs API-only (pair with a separate `interlace scheduler`).
`--quack` also serves the warehouse over the quack protocol. Warns loudly if bound to a
non-loopback host with no API keys configured (the API is then open).

## API keys — `interlace apikey ...`

### `interlace apikey create NAME [--scope read|write|admin ...]`
Create a key and print the token **once**. State only.

### `interlace apikey revoke NAME`
Revoke every key with that name (refuses to remove the last remaining key — that would
disable auth).

### `interlace apikey list`
Names and scopes (never the secrets).

## Selectors

`--select` accepts (comma- or space-separated, or repeated; unioned):

- `model` — exact.
- `+model` — model and its ancestors; `model+` — model and its descendants; `+model+` — both.
- `tag:x` — models carrying tag `x` (a tag matching nothing raises, so a CI gate can't
  silently no-op); affixes compose (`tag:x+`).
- `state:modified` — models whose fingerprint differs from the target environment's promoted
  mapping (the CI diff); affixes compose (`state:modified+`); an empty match is legitimate.

Accepted by `plan`, `apply`, `run`, `restate`, `models`, `checks run`.
