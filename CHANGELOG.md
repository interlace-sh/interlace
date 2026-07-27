# Changelog

## 2.0.0a2 (2026-07-28)

Hardening + throughput alpha: everything a first-principles review of a1 found,
plus the CLI growing into a daily driver.

**Performance** — backfills now build DAG-parallel within dependency levels
(`apply(parallelism=4)`; models a check reads are ordered too); snapshot reads
batched (2 queries per apply, not 2N); stream publishing is append-only with a
micro-batching flusher (no warehouse write on the hot path); SSE clients share
one event tail; startup-cached column lineage.

**Correctness** — forward-only is copy-on-write: history moves to the new
fingerprint's table and checks gate before production views move; GC decides
and deletes in one transaction (safe against concurrent promotes from other
processes); interval-trigger idempotency keys survive crash/restart; worker
lease fencing + drain under the apply lock; table sinks evolve their external
target (additive ALTERs + column-list inserts — no more positional breakage);
`apply` works on incremental_by_time models (fills the latest grain interval);
DuckDB secrets/extensions apply once per instance (fixes a catalog write-write
race under parallel builds); Postgres DSNs must name their host — no silent
localhost:5432; DuckLake attach handles are released on close (upstream
DatabaseInstance leak worked around).

**CLI** — live per-model build progress; a build-results table (output,
strategy, engine, dependencies, +new ~updated -deleted rows, time) backed by
strategy-interpreted affected counts, also on the HTTP ApplyResponse; `--json`
on the inspection commands; `checks run` (validate promoted tables without
rebuilding); `interlace list` renamed `models`; run windows + restate over
HTTP (`POST /runs` start/end/restate); shell completion; `INTERLACE_ENV`;
lineage `--format dot`; minimal restyled tables; self-explaining empty states.

**Examples** — new `examples/benchmark` (25M rows through a concurrent fan-out
DAG, incremental windows, Arrow streaming, a Parquet sink — with measured
timings); platform_tour demonstrates sink evolution; stale v0.x example
leftovers removed.

## 2.0.0a1 (2026-07-27)

First alpha of the ground-up v2 rebuild. The 0.x line on PyPI is unrelated to
this codebase.

**Transformation** — sqlglot-AST IR with Arrow as the only interchange format;
fingerprinted snapshot tables with virtual environments (production is the
unprefixed namespace, sandboxes are prefixed); terraform-style plan/apply with
breaking / non-breaking / forward-only classification and provably-identical
downstream models reusing their tables instead of rebuilding; strategies:
full, view, ephemeral, merge_by_key, full_merge, incremental_by_time (interval
ledger, backfill/restate), scd_type_2; schema contracts; column lineage;
data-quality checks (10 built-ins + @check) gating promotion; Python models
over Arrow with cursor/this incremental extraction and keyed strategies;
reference-aware GC.

**Orchestration** — cron/interval triggers over a durable run queue; per-task
leases with crash reclaim, durable retries, timeouts, and cooperative
cancellation; the combined daemon (`interlace serve`): HTTP API (Litestar,
OpenAPI/Scalar, scoped API keys, SSE event log) + scheduler + streams in one
process.

**Streaming** — durable stream log (SQLite WAL) with idempotency-key dedup and
consumer-group lease fencing; schema-validated ingestion with reject / evolve /
quarantine drift modes; exactly-once materialization into `streams.<name>`;
stream-append triggers; retention sweeps.

**Storage & engines** — DuckLake default warehouse (quack-served for
multi-process access); named engines with model pinning (fingerprinted),
native Postgres execution via ADBC, explicit cross-engine transfers with an
attach fast lane; `attach:` federation and table sinks (reverse ETL) with
replace/append/merge_by_key/full_merge delivery.

---

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
