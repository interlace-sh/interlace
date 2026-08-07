# Changelog

## Unreleased

**New: `hash_merge` strategy — a change-detected keyed upsert.** Like `merge` (a keyed
upsert that keeps rows absent from the source) but it stores an `_hash` (md5 of the non-key
columns) and writes only the delta — new key inserts, changed hash updates, unchanged rows
skipped. Idempotent (identical data writes nothing) with counts that split cleanly into
`+inserted` / `~updated`, where `merge`'s native `MERGE` rewrites every matched row each run
and reports one lumped count. See `docs/strategies.md`.

**Fix: ephemeral models aren't counted in "promoted N".** An ephemeral model is inlined and
has no promotable table, so a project with one (e.g. the benchmark) no longer reads
"Ran 12 model(s); promoted 13" — the count matches the build rows.

**Improved: promoting existing logic to a second environment is a view-swap, not a rebuild.**
Snapshots are content-addressed and shared across environments, but a fresh environment
still rebuilt every model. `apply` now recognises a fingerprint already materialised by a
prior apply (typically in another environment) and reuses the shared table — recording the
snapshot, running its checks, and swapping the environment's view — instead of recomputing.
Scoped to virtual/view models (terminals always deliver); checks still gate promotion; falls
back to a real build if the table is gone (in-memory warehouse, gc).

**Fix: `interlace init --list` shows each template's extra and full description.** The
templates now declare `requires_env`, so the Needs column lists the extra
(service/sources/postgres) instead of "—"; and the descriptions no longer lose their
`[sources]`-style bracketed text, which Rich had been eating as markup.

**Fix: `interlace serve` picks up model edits without a restart.** The daemon compiled the
project once at startup, so editing a model and pressing Plan/Apply in the UI showed nothing
new — only a restart (or `interlace plan` in a fresh process) did. It now recompiles on demand
when a model file changes on disk (a cheap mtime probe; the graph, lineage and stream→consumer
map re-derive only when something changed). Changing engine/stream/path topology in
`interlace.yaml` still needs a restart.

**Fix: enqueued runs start immediately, not on the next tick.** A run enqueued from the UI/API
waited out the scheduler interval (up to `--interval`, 60s) before a worker picked it up. An
enqueue now wakes the drain at once; the interval remains the periodic fallback for schedules.

**UI: run detail redesign.** The expanded run is one compact table — a status tick (✓/✗/⊘) plus
model / output / strategy / engine / depends on / **checks** / rows / time — instead of a
build-results table *and* a separate tick timeline. The runs list gains **env** and overall
**duration** columns and drops the usually-empty *window* column (the backfill window now shows in
the detail header only when a run had one).

**UI: query console — a runnable starter.** Clicking a table into an empty editor inserts
`select * from <ref>`; mid-query it still drops just the ref at the cursor.

**API: CLI↔API↔UI parity.** `POST /apply` returns `checks` + `gated`; `GET /runs` and `/checks`
take `?limit=`; `GET /lineage` takes `?environment=`.

## 2.1.1 (2026-08-07)

**Fix (2.1.0 regression): `gc` reclaims `materialise: view` snapshots.** A view model's
physical snapshot is a view (`CREATE OR REPLACE VIEW`), but `gc` dropped every superseded
snapshot by trying `DROP TABLE` first — and `DROP TABLE` on a view *raises* rather than
no-opping, so `gc` aborted before the `DROP VIEW` could run and reclaimed nothing. Any
project with view models could never `gc`. It now drops each object by its actual catalog
kind (`DROP VIEW` / `DROP TABLE`).

**UI: brand refresh.** The `interlace serve` web UI adopts the interlace.sh woven mark
(favicon + topbar) and the `interlace.sh` wordmark with a muted `.sh`.

**UI: activity timeline.** The overview feed now groups the event stream into expandable
build *episodes* — one per apply or run — each a per-model timeline (start → done/failed with
durations) instead of a flat list; the run detail renders the same timeline. Ad-hoc applies
(which aren't queued runs) surface in this feed.

## 2.1.0 (2026-08-07)

**New: source models — ingestion by pull (`interlace.sources`).** A small synchronous REST
client behind the `interlaced[sources]` extra — auth (bearer / API-key / basic), pagination
(page / offset / cursor / RFC-5988 link header), retry with jittered backoff that honours
`Retry-After`, and rate limiting — that turns an API into Arrow. A *source* is an ordinary
`@model` that pulls and yields batches; incremental via the reserved `cursor` param, idempotent
via `merge`. See `docs/sources.md`.

**New: init templates (`interlace init --template NAME` / `--list`).** `interlace init` is now a
registry over runnable starter projects, each shipped in the wheel with a README that doubles as
its landing page: `quickstart` (default, no-source SQL → Python → SQL), `github` (incremental
REST pull), `postgres` (incremental DB pull via psycopg, with a seeded docker-compose), and
`events` (durable `@stream` ingestion + live rollups — formerly the `event_stream` example).

**New: `interlace query "SELECT …"`.** A read-only query command — the CLI counterpart of the web
console, sharing one parse-and-fence path (SELECT-only; table functions and file/HTTP readers
rejected).

**Improved: the web UI.** A correctness / robustness / accessibility / security / performance pass
over the in-package UI — fixed the "failing checks" over-count and dead stream-column links,
keyboard-operable modals and navigation, a same-origin Content-Security-Policy + `nosniff` /
frame headers scoped to `/ui`, gzip compression, and lazy-loaded views.

**Fix: column lineage traces through Python models.** A Python model (no SQL to qualify) no
longer dead-ends the whole downstream subtree — the differ, UI and `interlace lineage`/`impact`
resolve columns through it — and a `count(*)` is no longer misread as a row-expanding `SELECT *`.

**Fix: adding an aggregate column to a rollup is non-breaking.** The additive-change class now
fires for aggregate models (same `count(*)`-as-star cause), so `apply` no longer gates a plain
column addition behind `--force`. A dimension add that changes the row set stays breaking.

**Fix: errors read like errors.** A model that fails to build — or a typo in a model file — now
surfaces as one `error:` line naming the model/file, not a raw traceback. `CheckSpec` is exported
from the package root, and a Python `@model(checks=…)` accepts the same dict shorthand as a SQL
config block.

## 2.0.3 (2026-08-06)

**Fix (correctness + security): the query console no longer disables warehouse writes.**
The read-only SQL console sandboxed untrusted queries by setting DuckDB's
`enable_external_access = false` on the shared warehouse connection. That setting is
**instance-wide and one-way**, so the first console query permanently disabled the
warehouse's own file writes — the stream flusher, `apply` and exports all started failing
("file system operations are disabled"), and publishes then returned 429 forever. The fence
now sits at parse time: a console `SELECT` may read tables and vetted row generators
(`range` / `generate_series`) only — every table function (`read_csv`, `read_parquet`,
`query`, `query_table`, `glob`, and unnamed or future ones) is rejected structurally, with a
file/network function backstop. No engine latch, so writes are never affected. (An
engine-level lockdown isn't possible here: a DuckLake catalog is held by one connection per
process, so the console necessarily shares the writer's.)

**Fix: `interlace serve` shuts down cleanly on Ctrl+C.** An open SSE stream (`/events/stream`,
the UI's live feed) blocked uvicorn's graceful shutdown until it timed out and force-cancelled
the held-open connection, dumping a `CancelledError` traceback ("Cancel 1 running task(s),
timeout graceful shutdown exceeded"). The daemon now ends open SSE streams the instant
shutdown begins, so the drain finds the connections already closed — Ctrl+C is immediate and
quiet.

**New example: `event_stream`** — durable ingestion, end to end. A `@stream` endpoint, the
exactly-once micro-batch materializer, backpressure, and live rollups over a moving stream,
with a standard-library load generator that fires events in parallel batches (a million per
burst, `--loop` for a million a minute).

## 2.0.2 (2026-08-06)

Docs and examples — no code changes.

- The `benchmark` example now exercises **every strategy** in one DAG: added `scd` (Type 2
  history), `full_merge` (composite key) and `append` (reverse ETL into an attached DuckDB),
  alongside the existing `replace` / `incremental_by_time` / `merge` / `view` / `file`.
- Install docs lead with `pip install interlaced` (interlace is CLI-first — `interlace init`
  scaffolds a project before one exists), with `uv tool install` for an isolated CLI and
  `uv add` for adding interlace as a project library; `interlace serve` still needs the
  `service` extra. `docs/engines.md` and the site carry the engine × strategy support matrix.

## 2.0.1 (2026-08-06)

**Breaking, despite the patch number.** Strategies carry short, plain names: `replace` (was
`full`), `merge` (was `merge_by_key`), `scd` (was `scd_type_2`). `full_merge`, `append` and
`incremental_by_time` are unchanged. There are no aliases — the old names are errors. This
landed after the 2.0.0 tag, so 2.0.0 on PyPI still uses the long names.

**`merge` upserts with a native `MERGE`** on engines that support it (DuckDB ≥ 1.3, Postgres
≥ 15) when the target's column list is known — rows update in place, so surrogate ids,
out-of-query columns and row identity survive, and `UPDATE` triggers fire. Falls back to the
portable `DELETE`+`INSERT` (which keeps the exact insert/update split) otherwise. The source is
not deduplicated: a duplicate key surfaces the engine's cardinality error.

**`scd` takes an optional `time_column`** — validity windows then use the event timestamp (a
new version's `_valid_from` and the closed version's `_valid_to` abut on the event time)
instead of processing time. Keys may be composite. `scd` no longer needs `SELECT * EXCLUDE`:
on engines without it, the model's own columns are enumerated instead, so history tracking
works there too.

**Engines.** Spark adapter (beta, SparkSession transport, tested on local Spark + Delta).
Alpha ADBC adapters for Redshift, Snowflake, BigQuery and MotherDuck — dialect-correct but not
yet validated against a live account. DuckDB-family, `quack` and `postgres` remain the tested
engines. `docs/engines.md` gains an engine × strategy support matrix.

## 2.0.0 (2026-08-05)

**Breaking: materialisation reframe.** `materialise` is now the destination/ownership plane,
and the old `export:` block is gone. Two planes:

- **owned** — `virtual` (was `table`; **now the default**), `view`, `ephemeral`. Full snapshot
  machinery: rebuild-skip, sandboxes, view-swap promotion, rollback, gc, forward-only.
- **terminal** — `table` (**new meaning**: an external `target: <alias>.<schema>.<table>`) and
  `file` (`path:` + `format:`). No snapshot table, no environment view; environment-gated;
  additive schema evolution only, never dropped.

Strategies now apply across both planes. `full` rewrites an owned table (`CREATE OR REPLACE`)
but replaces an external one in place (DELETE all + INSERT, never drops); new `append` strategy
(external `table` only); `incremental_by_time` now works **into an external table** (windowed
DELETE + INSERT), which the old `export:` sink could not do.

**Migration.**

- `materialise: table` (the old owned snapshot) → `materialise: virtual`, or drop it, since
  `virtual` is the default. A bare `materialise: table` without a `target:` now fails loudly
  ("did you mean materialise: virtual?").
- `export: {to: table, target: T, mode: M, key: K}` → `materialise: table, target: T,
  strategy: M, key: K`.
- `export: {to: parquet|csv|json, path: P}` → `materialise: file, format: <fmt>, path: P`.
- A lingering `export:` key or `export=` kwarg raises a migration error naming its replacement.

The API field `ModelInfo`/`ModelDetail.is_sink` is renamed `is_terminal`. `exports.py` is
removed (helpers moved to `sinks.py`); the delivery mode lives on `strategy`, not
`export.mode`.

## 1.0.2 (2026-08-03)

**Security.** The SQL query console (`POST /query`) could read arbitrary local files — and
reach the network on httpfs/S3 deployments — via DuckDB's `query()`/`query_table()` dynamic-SQL
functions, which the name-based deny-list did not match. The console now runs with external
access disabled at the engine level, closing every spelling of the escape hatch. `GET /engines`
and `interlace engines` no longer leak credentials for keyword-form or query-string DSNs
(redaction was URL-only). `interlace serve` on a non-loopback host with no API keys now warns
that the API is open.

**Correctness.** Every scheduled or stream run recorded a redundant promotion generation,
breaking rollback's default target and growing the history table unbounded; `apply`'s
check-edge cycle handling could let a downstream build before its upstream; the 1.0.1 Postgres
streaming fetch deadlocked multi-input Python models (reverted to materialised fetch); rollback
wrongly aborted on a since-deleted ephemeral; `state:modified` failed in `checks run`; the
stream backpressure gauge could be defeated by a mid-flush publish; `GET /models` misreported
engine and language; and the standalone `interlace scheduler` never flushed streams. Promotion
history is now capped by `trim_logs`.

**Cleanup.** Removed dead code — never-raised exceptions, the unrealised `SqlRelation` "logical
plane" and `ir/schema`, unused strategy and decorator fields, the `scd2` alias, the hidden
`list` CLI alias. `__version__` now reads package metadata. The architecture doc is renamed
`architecture.md`, with its roadmap-versus-shipped split corrected.

## 1.0.1 (2026-07-31)

**Rollback.** Every promote records the environment's full mapping as a promotion-history
generation; `interlace env rollback [--to N] [--list]` (and `POST
/environments/{name}/rollback`) repoints an environment's views at any earlier generation —
nothing rebuilds. The UI's environments view gained a history modal.

**CI selection.** `state:modified` selects models whose fingerprint drifted from the target
environment (transitive; affixes compose, as in `state:modified+`); an empty match is a clean
no-op. `interlace impact <model.column>` reports the column-level blast radius, with Python and
`*` consumers called out as opaque.

**Durability.** The stream log now runs `synchronous=FULL`, so "200-OK means fsynced" is
literally true — surviving power loss, not just process crash. Batched publishes amortise the
fsync.

**Performance.** `apply` schedules the true DAG: each model starts when its last in-plan
ancestor finishes, with no level barriers. Postgres fetch streams via ADBC instead of
materialising, so large cross-engine transfers no longer spike RSS. The stream flusher only
touches streams that received a publish.

Also: unscoped runs retire deleted models like `apply` does; default incremental windows are
complete and grain-aligned; incremental models backfill automatically on first build; the
worker logs run lifecycle and `apply` logs per-model failures.

## 1.0.0 (2026-07-31)

First stable release of the rebuilt platform, published to PyPI as `interlaced` (import and
CLI: `interlace`).

**Transformation.** SQL files and Arrow-native Python functions compile to a fingerprinted DAG
over a sqlglot IR. Terraform-style `plan` / `apply` with a breaking-change gate; virtual
environments as views over immutable snapshot tables, production being the unprefixed
namespace; column-pruned rebuild skipping, so a semantic change invalidates only consumers of
the touched columns; `--forward-only` copy-on-write for the history-keeping strategies (`full`,
`merge_by_key`, `full_merge`, `scd_type_2`, `incremental_by_time` with an interval ledger).
Data-quality checks gate promotion.

**Orchestration.** Built-in cron and interval scheduler over a durable run queue with leases,
retries and cooperative cancellation; interval-aware backfill (`run` catches up, `restate`
reprocesses).

**Streaming.** Durable ingestion log with fsync-before-ack and idempotency keys; exactly-once
micro-batch materialisation via an in-warehouse watermark; schema drift modes (reject / evolve
/ quarantine); retention; 429 backpressure.

**Multi-engine.** DuckDB + DuckLake by default, Postgres natively over ADBC, per-model
`engine:` pinning with explicit cross-engine Arrow transfers and an ATTACH fast lane where
possible. Reverse-ETL sinks, environment-gated to production by default.

**One daemon.** `interlace serve` runs the HTTP API (Litestar, scoped API keys), the scheduler,
stream ingestion and a zero-build web UI at `/ui` — lineage canvas with column-level tracing,
live build feedback over SSE, plan/apply, runs, a read-only query console, checks, environments
and system administration.

## Before 1.0

The 0.x line (0.1.0 through 0.2.0, February 2026) was a different codebase, built on ibis with
pandas at every model boundary and in-memory queues for streaming. It was never published and
shares no code with the platform above; three reviews found structural defects that could not
be patched incrementally, and it was replaced rather than refactored. The reasoning is in
`docs/architecture/architecture.md`; the source is on the `v0` branch and its release notes are
in this file's git history.
