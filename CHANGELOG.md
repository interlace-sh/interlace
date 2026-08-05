# Changelog

## 2.0.0 (2026-08-05)

**Breaking: materialisation reframe.** `materialise` is now the destination/ownership
plane, and the old `export:` block is gone. Two planes:

- **virtual** (interlace-owned) — `virtual` (was `table`; **now the default**), `view`,
  `ephemeral`. Full snapshot machinery: rebuild-skip, sandboxes, view-swap promotion,
  rollback, gc, forward-only.
- **terminal** (external, interlace delivers) — `table` (**new meaning**: an external
  `target: <alias>.<schema>.<table>`) and `file` (`path:` + `format:`). No snapshot table,
  no environment view; environment-gated; additive schema evolution only, never dropped.

Strategies now apply across both planes. `full` rewrites an owned table
(`CREATE OR REPLACE`) but replaces an external one in place (DELETE all + INSERT, never
drops); new `append` strategy (external `table`); `incremental_by_time` now works **into an
external table** (windowed DELETE + INSERT), which the old `export:` sink could not do.

**Migration.**

- `materialise: table` (the old interlace-owned snapshot) → `materialise: virtual` (or drop
  it — `virtual` is the default). A bare `materialise: table` without a `target:` now fails
  loudly ("did you mean materialise: virtual?").
- `export: {to: table, target: T, mode: M, key: K}` → `materialise: table, target: T,
  strategy: M, key: K`.
- `export: {to: parquet|csv|json, path: P}` → `materialise: file, format: <fmt>, path: P`.
- A lingering `export:` key (SQL header) or `export=` kwarg (`@model`) raises a migration
  error pointing at the replacement.

The API `ModelInfo`/`ModelDetail` field `is_sink` is renamed `is_terminal`. `exports.py` is
removed (helpers moved to `sinks.py`); the delivery mode lives on `strategy`, not a separate
`export.mode`.

## 1.0.2 (2026-08-03)

**Security.** The SQL query console (`POST /query`) could read arbitrary local
files — and reach the network on httpfs/S3 deployments — via DuckDB's
`query()`/`query_table()` dynamic-SQL functions, which the name-based deny-list
did not match. The console now runs with external access disabled at the engine
level, closing every spelling of the escape hatch. `GET /engines` and
`interlace engines` no longer leak credentials for keyword-form or query-string
DSNs (redaction was URL-only). `interlace serve` on a non-loopback host with no
API keys now warns that the API is open.

**Correctness.** Fixed: every scheduled/stream run recorded a redundant
promotion generation (breaking rollback's default target and growing the
history table unbounded); `apply`'s check-edge cycle handling could let a
downstream build before its upstream; the 1.0.1 Postgres streaming fetch
deadlocked multi-input Python models on the postgres engine (reverted to
materialised fetch); rollback wrongly aborted on a since-deleted ephemeral;
`state:modified` failed in `checks run`; the stream backpressure gauge could be
defeated by a mid-flush publish; `GET /models` misreported engine/language; and
the standalone `interlace scheduler` never flushed streams. Promotion history is
now capped by `trim_logs`.

**Cleanup.** Removed dead code (never-raised exceptions, the unrealised
`SqlRelation` "logical plane" and `ir/schema`, unused strategy/decorator fields,
the `scd2` alias, the hidden `list` CLI alias); `__version__` now reads package
metadata. Docs: the architecture doc is renamed `architecture.md` and its
roadmap-vs-shipped split corrected.

## 1.0.1 (2026-07-31)

**Rollback.** Every promote records the environment's full mapping as a
promotion-history generation; `interlace env rollback [--to N] [--list]` (and
`POST /environments/{name}/rollback`) repoints the environment's views at any
earlier generation — nothing rebuilds. The UI's environments view gained a
history modal with one-click roll back.

**CI selection.** `state:modified` selects models whose fingerprint drifted
from the target environment (transitive; affixes compose: `state:modified+`);
an empty match is a clean no-op. `interlace impact <model.column>` reports the
column-level blast radius, with Python/`*` consumers called out as opaque.
The plan view gained a "changed only" quick-pick.

**Performance.** `apply` schedules the true DAG — each model starts when its
last in-plan ancestor finishes (no level barriers); Postgres fetch streams via
ADBC instead of materialising (large cross-engine transfers no longer spike
RSS); the stream flusher only touches streams that received a publish;
stream→consumer maps and SSE serialisation are computed once.

**Durability.** The stream log now runs `synchronous=FULL`: "200-OK means
fsynced" is literally true, surviving power loss, not just process crash.
Batched publishes amortise the fsync.

**Operability.** The worker logs run lifecycle (start/success/failure with
attempt) and `apply` logs per-model failures, on stdlib logging.

Also: unscoped runs retire deleted models like apply does; default incremental
windows are complete and grain-aligned; incremental models backfill
automatically on first build; the UI says so when an incremental model holds
no data.

## 1.0.0 (2026-07-31)

First stable release of the rebuilt platform, published to PyPI as `interlaced`
(import and CLI: `interlace`).

**Transformation.** SQL files and Arrow-native Python functions compile to a
fingerprinted DAG (sqlglot IR). Terraform-style `plan` / `apply` with a
breaking-change gate; virtual environments as views over immutable snapshot
tables (production is the unprefixed namespace, sandboxes are prefixed);
column-pruned rebuild skipping — a semantic change invalidates only consumers
of the touched columns; `--forward-only` copy-on-write for history-keeping
strategies (full, merge_by_key, full_merge, scd_type_2, incremental_by_time
with an interval ledger). Data-quality checks gate promotion.

**Orchestration.** Built-in scheduler (cron/interval) over a durable run queue
with leases, retries, and cooperative cancellation; interval-aware backfill
(`run` catches up, `restate` reprocesses).

**Streaming.** Durable ingestion log (fsync-before-ack, idempotency keys),
exactly-once micro-batch materialization via an in-warehouse watermark, schema
drift modes (reject / evolve / quarantine), retention, and 429 backpressure.

**Multi-engine.** DuckDB + DuckLake by default; Postgres natively over ADBC;
per-model `engine:` pinning with explicit cross-engine Arrow transfers (ATTACH
fast lane where possible). Reverse-ETL sinks with environment gating
(production-only by default).

**One daemon.** `interlace serve` = HTTP API (Litestar, scoped API keys) +
scheduler + streams + a zero-build web UI at `/ui`: lineage canvas with
column-level tracing, live build feedback over SSE, plan/apply, runs, a
read-only query console, checks, environments, and system administration.

# 0.x line (frozen; unrelated to the v2 rebuild)

## [0.2.0] - 2026-02-24

### Added
- **API key authentication middleware:** Global auth for `interlace serve` with Bearer token and X-API-Key header support, per-key permissions (read/write/execute), path whitelisting, and configurable via `service.auth` in config.yaml.
- **Rate limiting:** In-memory token bucket rate limiter per API key, configurable via `service.auth.rate_limit` in config.yaml.
- **OpenAPI/Swagger documentation:** Full OpenAPI 3.0 specification (32 endpoints, 46 schemas) served at `/api/openapi.yaml` with interactive Swagger UI at `/api/docs`.
- **Quality check executor integration:** Quality checks are now executed post-materialization during `interlace run`. Supports both `@model(quality_checks=[...])` decorator-level and `config.yaml` quality section configuration. Results stored in `interlace.quality_results` state table.
- **`force` parameter on programmatic API:** `run(force=True)` re-executes all models regardless of change detection.
- **Integration tests:** 6 end-to-end tests running the basic example project, verifying model execution, quality checks, change detection, and force re-execution.

### Fixed
- Display singleton no longer crashes when reused across multiple `run()` calls (cleared stale progress state in `set_flow()`).
- Guard against stale Rich progress task IDs from previous runs.

## [0.1.2] - 2026-02-24

### Changed
- **State store schema cleanup:** Removed unused `model_columns` and `stream_publish_log` tables. Removed redundant columns from `model_metadata` and `stream_consumers`. `get_model_columns()` now reads from `schema_history` instead of the removed `model_columns` table.
- **Schema-aware cursor state:** Cursor state table now uses composite primary key `(model_name, schema_name)` to support multi-schema deployments.
- **Flow summary tracking:** Flows table now stores `models_total`, `models_succeeded`, `models_failed`, and `model_selection` columns for richer execution summaries.
- **Task skip tracking:** Tasks now track `skipped_reason` (e.g. `upstream_failed`, `no_changes`, `cached`).

### Fixed
- API responses now sanitise NaN, Inf, pandas Timestamps, and numpy types for valid JSON serialisation.
- Flow list endpoint now computes task aggregate counts from the tasks table instead of relying on in-memory state.
- SSE event stream now drains cleanly on server shutdown via sentinel-based EventBus shutdown.
- View materialiser now checks view existence before skipping recreation on unchanged files.
- Change detector uses O(1) `model_metadata.last_run_at` lookup instead of `MAX(completed_at)` scan on tasks table.
- DLQ timestamp columns use proper SQL TIMESTAMP literals instead of raw epoch floats.
- Schema tracking now correctly persists column nullability instead of hardcoding `TRUE`.
- Stored task dependencies are now parsed from JSON strings in API responses.

### Documentation
- Corrected all three roadmap documents to reflect actual implementation status. `@stream` decorator, testing framework, cursor-based backfill, and forward-only migrations were fully implemented but documented as "planned".

## [0.1.1] - 2026-02-22

### Added
- Docker support with multi-stage Dockerfile and docker-compose configuration.
- Build scripts and workspace configuration from monorepo split.
- GitHub Actions workflows for PyPI publishing and GitHub Pages deployment.

### Fixed
- Resolved all 590 mypy type errors across the codebase.
- Added type stubs for paramiko, aiofiles, and cachetools.
- Ensured mypy passes in CI without requiring optional type stubs.

## [0.1.0] - 2026-02-20

### Added
Initial release of Interlace — a Python/SQL-first data pipeline framework.

**Core Engine:**
- `@model` decorator for Python and SQL models with dependency resolution
- `@stream` decorator for event ingestion with HTTP endpoints, pub/sub, and 5 adapters
- Dynamic parallel execution engine with per-task DuckDB connections
- Cursor-based backfill with `--since` / `--until` CLI flags

**Strategies:** replace, append, merge_by_key, scd_type_2, none

**Materialisation:** table, view, ephemeral

**Connections:** DuckDB, PostgreSQL (asyncpg pooling), S3, Filesystem, SFTP, generic ibis (18+ backends including Snowflake, BigQuery, MySQL)

**Schema:** 5 flexibility modes (strict, safe, flexible, lenient, ignore) with version history tracking

**Quality:** 6 check types (not_null, unique, accepted_values, freshness, row_count, expression) with runner and severity model

**Retry:** RetryPolicy with exponential backoff + jitter, circuit breaker, dead letter queue

**Testing:** `test_model()`, `test_model_sync()`, `mock_dependency()` — pytest plugin with 18 tests

**Observability:** Prometheus metrics, OpenTelemetry tracing, structured logging

**State management:** DuckDB-backed StateStore with 11 tables for flows, tasks, schema history, lineage, file hashes, cursor state, model metadata, migration runs, scheduler state, and stream consumers

**CLI:** `run`, `serve`, `init`, `info`, `plan`, `schema`, `lineage`, `migrate`, `promote`, `config`, `ui`

**REST API:** 20+ endpoints via aiohttp — models, runs, flows, graph, lineage, events (SSE), plan, schema, streams, health

**Web UI:** Svelte 5 + Tailwind CSS 4 with ELK.js DAG visualisation, model browser, run history, real-time SSE monitoring, schema explorer, column lineage views

**Scheduling:** Cron + interval scheduling with persistence, missed-job handling, status API

**Migrations:** Forward-only migration runner with version tracking, dry-run, CLI

**Export:** CSV, Parquet, JSON exporters

**Environments:** Config overlay merging, data promotion CLI

[0.2.0]: https://github.com/marklidenberg/interlace/compare/v0.1.2...v0.2.0
[0.1.2]: https://github.com/marklidenberg/interlace/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/marklidenberg/interlace/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/marklidenberg/interlace/releases/tag/v0.1.0
