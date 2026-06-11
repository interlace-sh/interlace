# Implementation Status

Quick reference for implementation status across Interlace features.

**Last Updated:** April 2026

## Status Legend

- ✅ **Implemented** - Working code exists and is functional
- 🔄 **In Progress** - Implementation started or partially complete
- 📋 **Planned** - Architecture/spec exists, implementation pending

---

## Core Features

### Strategies ✅
| Strategy | Status | Location |
|----------|--------|----------|
| `merge_by_key` | ✅ Implemented | `strategies/merge_by_key.py` |
| `append` | ✅ Implemented | `strategies/append.py` |
| `replace` | ✅ Implemented | `strategies/replace.py` |
| `none` | ✅ Implemented | `strategies/none.py` |
| `scd_type_2` | ✅ Implemented | `strategies/scd_type_2.py` |
| `merge_stream_with_source` | 📋 Planned | - |

### Materializations ✅
| Type | Status | Location |
|------|--------|----------|
| `table` | ✅ Implemented | `materialization/table.py` |
| `view` | ✅ Implemented | `materialization/view.py` |
| `ephemeral` | ✅ Implemented | `materialization/ephemeral.py` |

### Connections ✅
| Connection | Status | Location |
|------------|--------|----------|
| DuckDB | ✅ Implemented | `connections/duckdb.py` |
| PostgreSQL | ✅ Implemented | `connections/postgres.py` |
| Filesystem | ✅ Implemented | `connections/filesystem.py` |
| SFTP | ✅ Implemented | `connections/sftp.py` (requires `interlace[sftp]`) |
| S3 | ✅ Implemented | `connections/s3.py` (requires `interlace[s3]`) |
| Generic Ibis (18+ backends) | ✅ Implemented | `connections/ibis_generic.py` |

### Connection Features ✅
| Feature | Status | Notes |
|---------|--------|-------|
| Access policies (`read`/`readwrite`) | ✅ Implemented | Per-connection access control |
| Shared connections (`shared: true`) | ✅ Implemented | Connection sharing across models |
| Extended DuckDB ATTACH | ✅ Implemented | MySQL, SQLite, DuckDB cross-file, DuckLake |
| Fallback connection resolution | ✅ Implemented | Virtual environments |

---

## Production Features

### Retry & Resilience ✅
| Feature | Status | Location |
|---------|--------|----------|
| RetryPolicy | ✅ Implemented | `core/retry/policy.py` |
| RetryManager | ✅ Implemented | `core/retry/manager.py` |

### Data Quality ✅
| Check Type | Status | Location |
|------------|--------|----------|
| `unique` | ✅ Implemented | `quality/checks/unique.py` |
| `not_null` | ✅ Implemented | `quality/checks/not_null.py` |
| `accepted_values` | ✅ Implemented | `quality/checks/accepted_values.py` |
| `freshness` | ✅ Implemented | `quality/checks/freshness.py` |
| `row_count` | ✅ Implemented | `quality/checks/row_count.py` |
| `expression` | ✅ Implemented | `quality/checks/expression.py` |

**Note:** Checks run automatically post-materialisation when configured via `@model(checks=...)`, `config.yaml`, `@check` decorator, or SQL check files. Results are persisted to `interlace.check_results`. Run checks standalone with `--checks-only` CLI flag.

### Streaming ✅
| Feature | Status | Location |
|---------|--------|----------|
| `@stream` decorator | ✅ Implemented | `core/stream.py` (773 lines) |
| HTTP endpoint generation | ✅ Implemented | `service/api/handlers/streams.py` |
| `publish()` API | ✅ Implemented | `core/stream.py` |
| Webhook adapter (outbound) | ✅ Implemented | `streaming/adapters/webhook.py` |
| RabbitMQ adapter | ✅ Implemented | `streaming/adapters/rabbitmq.py` (requires `interlace[stream]`) |
| In-memory adapter (testing) | ✅ Implemented | `streaming/adapters/memory.py` |
| Per-stream auth (bearer/api-key) | ✅ Implemented | `service/api/handlers/streams.py` |
| Consumer cursor tracking | ✅ Implemented | `core/state/` |

### Testing Framework ✅
| Feature | Status | Location |
|---------|--------|----------|
| `test_model()` | ✅ Implemented | `testing.py` (220 lines) |
| `test_model_sync()` | ✅ Implemented | `testing.py` |
| `mock_dependency()` | ✅ Implemented | `testing.py` |
| Pytest plugin (auto-registered) | ✅ Implemented | `testing.py` |

### Backfill ✅
| Feature | Status | Location |
|---------|--------|----------|
| Cursor-based backfill | ✅ Implemented | `core/execution/model_executor.py` |
| `--since` / `--until` CLI flags | ✅ Implemented | `cli/run.py` |
| Cursor state save suppression during backfill | ✅ Implemented | `core/execution/model_executor.py` |

### Schema Migrations 🔄
| Feature | Status | Location |
|---------|--------|----------|
| Forward-only migration runner | ✅ Implemented | `migrations/runner.py` (173 lines) |
| Migration CLI (`interlace migrate`) | ✅ Implemented | `migrations/cli.py` (128 lines) |
| Migration utilities | ✅ Implemented | `migrations/utils.py` |
| Version tracking (`migration_runs` table) | ✅ Implemented | `core/state/` |
| Dry-run mode | ✅ Implemented | `migrations/runner.py` |
| Rollback support | 📋 Planned | - |

### Observability ✅
| Feature | Status | Location |
|---------|--------|----------|
| Prometheus Metrics | ✅ Implemented | `observability/metrics.py` |
| OpenTelemetry Tracing | ✅ Implemented | `observability/tracing.py` |
| Structured Logging | ✅ Implemented | `observability/structured_logging.py` |

### Scheduling ✅
| Feature | Status | Location |
|---------|--------|----------|
| Cron expression parser | ✅ Implemented | `service/cron_parser.py` |
| Interval scheduling | ✅ Implemented | `service/server.py` |
| `schedule` parameter on `@model` | ✅ Implemented | `core/model.py` |
| Background model schedule loop | ✅ Implemented | `service/server.py` |
| Scheduler persistence (`last_run_at`) | ✅ Implemented | `core/state/` + `service/server.py` |
| Missed-job handling (run-once misfire) | ✅ Implemented | `service/server.py` |
| Scheduler status API (`/api/v1/scheduler`) | ✅ Implemented | `service/api/handlers/health.py` |

### State Management ✅
| Feature | Status | Location |
|---------|--------|----------|
| StateStore (11 tables) | ✅ Implemented | `core/state/` |
| Execution state persistence | ✅ Implemented | `core/state/` |
| Lineage state persistence | ✅ Implemented | `core/state/` |
| File hash tracking | ✅ Implemented | `core/state/` |
| Schema history tracking | ✅ Implemented | `core/state/` |

### Schema Flexibility ✅
| Feature | Status | Location |
|---------|--------|----------|
| SchemaMode enum (5 modes) | ✅ Implemented | `schema/modes.py` |
| Mode-based validation | ✅ Implemented | `schema/validation.py` |
| Column mapping | ✅ Implemented | `core/execution/data_converter.py` |
| Safe type widening | ✅ Implemented | `schema/validation.py` |

### Export Formats ✅
| Format | Status | Location |
|--------|--------|----------|
| CSV | ✅ Implemented | `export/csv_exporter.py` |
| Parquet | ✅ Implemented | `export/parquet_exporter.py` |
| JSON | ✅ Implemented | `export/json_exporter.py` |

---

## CLI Commands ✅

| Command | Status | Notes |
|---------|--------|-------|
| `interlace run` | ✅ Implemented | Full model execution with parallelism |
| `interlace init` | ✅ Implemented | Project initialization |
| `interlace config` | ✅ Implemented | Environment configuration with overlay merging |
| `interlace serve` | ✅ Implemented | HTTP API + background scheduler |
| `interlace info` | ✅ Implemented | Model information with Rich display |
| `interlace schema` | ✅ Implemented | Schema management with diffing |
| `interlace ui` | ✅ Implemented | Web UI management (build, status, clean) |
| `interlace lineage` | ✅ Implemented | Column-level lineage display |
| `interlace plan` | ✅ Implemented | Impact analysis and execution planning |
| `interlace promote` | ✅ Implemented | Cross-environment data promotion |
| `interlace migrate` | ✅ Implemented | Forward-only schema migrations |

---

## Web UI ✅
**Location:** `ui/` (Svelte 5 + Vite + Tailwind CSS 4)

| Feature | Status |
|---------|--------|
| Pipeline DAG visualization (ELK.js) | ✅ Implemented |
| Model browser with search/filter | ✅ Implemented |
| Run history dashboard | ✅ Implemented |
| Ad-hoc run triggering | ✅ Implemented |
| Real-time execution monitoring (SSE) | ✅ Implemented |
| Schema explorer | ✅ Implemented |
| Column-level lineage views | ✅ Implemented |
| Schema diffing display | ✅ Implemented |

---

## REST API ✅
**Location:** `service/` (aiohttp)

| Endpoint Area | Status | Handler |
|--------------|--------|---------|
| Models (list, detail) | ✅ Implemented | `api/handlers/models.py` |
| Runs (trigger, history, detail) | ✅ Implemented | `api/handlers/runs.py` |
| Flows (tracking) | ✅ Implemented | `api/handlers/flows.py` |
| Graph (DAG data) | ✅ Implemented | `api/handlers/graph.py` |
| Lineage (column-level) | ✅ Implemented | `api/handlers/lineage.py` |
| Events (SSE streaming) | ✅ Implemented | `api/handlers/events.py` |
| Plan (impact analysis) | ✅ Implemented | `api/handlers/plan.py` |
| Schema (history, diffing) | ✅ Implemented | `api/handlers/schema.py` |
| Streams (publish, consume) | ✅ Implemented | `api/handlers/streams.py` |
| Health | ✅ Implemented | `service/server.py` |

---

## Landing Website ✅
**Location:** `interlace.sh/` (SvelteKit)

| Feature | Status |
|---------|--------|
| Product overview | ✅ Implemented |
| Features page | ✅ Implemented |
| Solutions page | ✅ Implemented |
| Documentation (15+ pages) | ✅ Implemented |
| Blog (3 posts) | ✅ Implemented |
| Getting started guide | ✅ Implemented |

---

## Planned Features (Not Yet Implemented)

### API & Service
- 📋 API key authentication and authorization middleware
- 📋 OpenAPI/Swagger API documentation
- 📋 Rate limiting

### Execution Pipeline
- 📋 Quality check integration into `interlace run` (framework exists, wiring missing)
- 📋 Datetime window backfill (`--window` flag)

### Documentation & Developer Experience
- 📋 Auto-generated documentation site (`interlace docs`)
- 📋 User function discovery from `functions/` directory

### Migrations
- 📋 Migration rollback support (`down.sql`)

### Post-1.0
- 📋 DuckLake S3/Parquet storage layer
- 📋 `merge_stream_with_source` strategy
- 📋 Distributed execution
- 📋 VS Code extension
- 📋 PII detection and handling

---

## See Also

- [`ROADMAP.md`](ROADMAP.md) - Detailed development roadmap
- [`OUTSTANDING_TASKS.md`](OUTSTANDING_TASKS.md) - Remaining work items
- [`../README.md`](../README.md) - Documentation index
