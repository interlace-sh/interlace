# Changelog

All notable changes to Interlace will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.2]: https://github.com/marklidenberg/interlace/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/marklidenberg/interlace/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/marklidenberg/interlace/releases/tag/v0.1.0
