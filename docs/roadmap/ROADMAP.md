# Interlace Development Roadmap

This document outlines the development roadmap for Interlace, organised by phases with priorities and milestones.

> **v2 architecture (June 2026):** a first-principles greenfield redesign has been approved —
> sqlglot AST + Arrow as the internal contract, DuckLake-backed snapshots with virtual data
> environments and plan/apply, a durable stream log, and a work-queue orchestrator. See
> [docs/architecture/v2-design.md](../architecture/v2-design.md). The phases below describe the
> v0.x line; new feature work should be evaluated against the v2 design first.

## Roadmap Philosophy

The roadmap is organised into phases, with each phase building on previous work:
- **Phase 0-1**: Core foundation (essential for MVP)
- **Phase 2-3**: Enhanced capabilities (production readiness)
- **Phase 4+**: Advanced features (scaling and enterprise features)

**Principles:**
- Build incrementally: Each phase delivers working, usable features
- Production-ready focus: Prioritise stability and observability
- Community-driven: Features driven by real use cases
- Interoperability: Work well with existing data tools

**Status Legend:**
- `[x]` = **Implemented** - Working code exists and is functional
- `[ ]` = **Not Implemented** - Architecture/spec may exist, but no working implementation

---

## Phase 0: Foundation (MVP) ✅

**Goal:** Core functionality for basic data pipeline execution

### Core Engine ✅
- [x] Unified `@model` decorator (Python and SQL)
- [x] `@stream` decorator for event ingestion — 773-line implementation with 5 adapters and 42 tests
- [x] Dependency graph building from implicit/explicit dependencies
- [x] Dynamic parallel execution engine
- [x] Basic materialisation (table, view, ephemeral) - all three types implemented
- [x] Connection management (DuckDB, Postgres, S3, Filesystem, generic ibis backends)
- [x] Base connection classes and ibis integration
- [x] `IbisConnection` generic class supporting 18+ ibis backends (Snowflake, BigQuery, MySQL, etc.)
- [x] Connection access policies (`access: read | readwrite`, `shared: true`)
- [x] Connection pooling (basic implementation for Postgres, advanced features planned)
- [x] Error handling and retries with exponential backoff

### Performance Optimisations ✅ (Completed January 2025)
- [x] Triple materialisation fix (3-5x faster table materialisation)
- [x] DuckDB `.cursor()` pattern for thread-safety (zero connection overhead, true parallel execution)
- [x] Efficient COUNT() operations (10-100x faster via SQL aggregation)
- [x] Schema application optimisation (66% memory reduction via in-place DataFrame casting)
- [x] Code quality improvements (.gitignore, .dockerignore, pre-commit hooks with ruff)

### Retry Framework ✅ (Completed January 2025)
- [x] `RetryPolicy` dataclass with exponential backoff configuration
- [x] `RetryManager` for async/sync execution with retry
- [x] `retry_policy` parameter on `@model` decorator
- [x] Pre-configured policies: `DEFAULT_RETRY_POLICY`, `API_RETRY_POLICY`, `DATABASE_RETRY_POLICY`
- [x] Integration with model executor for automatic retry on transient failures

### Data Loading Strategies ✅
- [x] Strategy definitions (`merge_by_key`, `append`, `replace`, `none`)
- [x] Strategy implementation and execution - all core strategies implemented
- [x] Incremental controls specification (`@incremental: key | datetime`)
- [x] Delete handling modes specification (`preserve`, `soft`, `hard`, `delete_missing`)
- [x] SCD Type 2 support - hash-based change detection with configurable columns
- [ ] Merge stream with source strategy (`merge_stream_with_source`) - specification only

### Basic Materialisation ✅
- [x] Materialisation specification
- [x] DuckLake architecture (DuckDB/Postgres catalogue + S3/Parquet) - specification
- [x] Materialisation types (`table`, `view`, `ephemeral`) - all three implemented
- [x] Table materialisation with catalogue integration - implemented
- [x] View materialisation - implemented
- [x] Ephemeral materialisation (in-memory registration) - implemented
- [ ] DuckLake full implementation (S3/Parquet storage layer) - architecture only

### CLI ✅
- [x] CLI structure (Typer-based)
- [x] `interlace init` - Project initialisation
- [x] `interlace run` - Execute models
- [x] `interlace serve` - Long-running service with HTTP API + scheduler
- [x] `interlace info` - Model information with Rich terminal display
- [x] `interlace config` - Environment configuration with env overlay merging
- [x] `interlace schema` - Schema management with cross-environment diffing
- [x] `interlace ui` - Web UI management (build, status, clean)
- [x] `interlace lineage` - Column-level lineage display
- [x] `interlace plan` - Impact analysis and execution planning
- [x] `interlace promote` - Cross-environment data promotion
- [x] `interlace migrate` - Forward-only schema migrations

### Configuration ✅
- [x] Environment configuration pattern (`config.yaml`, `config.{env}.yaml`)
- [x] Connection configuration specification
- [x] Configuration file parsing and validation
- [x] Environment variable substitution (`${VAR_NAME}` syntax)
- [x] Connection configuration loading
- [x] Config overlay merging (`config.{env}.yaml` deep-merged over `config.yaml`)
- [x] Environment placeholder resolution (`{env}` substitution)
- [x] Fallback connection resolution (virtual environments)

**Target:** End-to-end pipeline execution with basic strategies and materialisation ✅

---

## Phase 1: Production Essentials ✅

**Goal:** Essential features for production use

### Web UI ✅
**Tech Stack:** Svelte 5 + Tailwind CSS 4 + TypeScript
**Location:** `ui/`

Full pipeline management UI with standard orchestrator features:
- [x] Project setup (`ui/` directory with Svelte 5 + Vite)
- [x] Pipeline DAG visualisation (interactive lineage graph with ELK.js layout)
- [x] Model browser with search/filter
- [x] Run history and status dashboard
- [x] Ad-hoc run triggering
- [x] Real-time execution monitoring (SSE streaming)
- [x] Data preview and schema explorer
- [x] Column-level lineage views
- [x] Schema diffing display

### Scheduling ✅
- [x] Scheduling architecture with in-process scheduler
- [x] Cron parser (standard 5-field crontab expressions) — `service/cron_parser.py`
- [x] Interval scheduling (`every_s` configuration)
- [x] Schedule execution and job management
- [x] `schedule` parameter on `@model` decorator
- [x] Background scheduler loop in `interlace serve`
- [x] Integration with server for automatic model execution
- [x] Scheduler persistence (`last_run_at` via StateStore, survives restarts)
- [x] Missed-job handling (run-once misfire policy on restart)
- [x] Scheduler status API endpoint (`GET /api/v1/scheduler`)
- [x] Dead code cleanup (removed unused vendored schedule.py and cron.py)

### Backfill ✅
- [x] Backfill architecture
- [x] Cursor-based backfill for `@incremental: key` — `model_executor.py`
- [x] CLI backfill (`--since`, `--until`) — `cli/run.py`
- [x] Cursor state save suppression during backfill windows
- [ ] Window enumeration for `@incremental: datetime` (`--window` flag)

### State Management ✅
- [x] State management architecture
- [x] State persistence in database (StateStore with 11 tables)
- [x] Full CRUD operations for execution state
- [x] Lineage state persistence
- [x] File hash tracking for change detection
- [x] Schema history tracking
- [x] State transaction management (only persist on success)
- [x] Scheduler persistence (`get_model_last_run_at` / `set_model_last_run_at`)

### Events and Streaming ✅
- [x] `@stream` decorator with automatic HTTP endpoints — `core/stream.py`
- [x] `publish()` API for programmatic event ingestion — `core/stream.py`
- [x] Stream publish endpoint (`POST /streams/{stream_name}`) — `service/api/handlers/streams.py`
- [x] Webhook adapter — `streaming/adapters/webhook.py`
- [x] Polling adapter — `streaming/adapters/polling.py`
- [x] Pub/Sub adapter — `streaming/adapters/pubsub.py`
- [x] Message queue adapter — `streaming/adapters/message_queue.py`
- [x] Filesystem adapter — `streaming/adapters/filesystem.py`
- [x] Per-stream auth (bearer/api-key) — `service/api/handlers/streams.py`
- [ ] Message deserialisation (Avro, Protobuf) - JSON only currently

### Basic Observability ✅
- [x] Observability architecture
- [x] Monitoring and observability specification (metrics, logging, tracing)
- [x] Database architecture for execution history (runs, jobs, state)
- [x] Structured logging implementation (`observability/structured_logging.py`)
- [x] Run-level metrics implementation (Prometheus metrics export)
- [x] OpenTelemetry tracing integration
- [x] Basic execution history in catalogue

**Target:** Stable production deployment with scheduling, state management, and monitoring ✅

---

## Phase 2: Enhanced Capabilities

**Goal:** Rich feature set for complex pipelines

### Export Formats ✅
- [x] Export format specification
- [x] Multi-materialisation support
- [x] CSV exporter implementation
- [x] Parquet exporter implementation
- [x] JSON exporter implementation (records and lines formats)
- [x] `@model(export={"format": "csv", "path": "output.csv"})` syntax

### Schema Evolution and Metadata
- [x] Schema evolution architecture
- [x] Database schema for metadata catalogue (schema_history, model_metadata)
- [x] Schema versioning and history tracking (implemented in StateStore)
- [x] Schema diffing API (`SchemaHandler` endpoint)
- [x] Schema flexibility modes (`SchemaMode` enum: STRICT, SAFE, FLEXIBLE, LENIENT, IGNORE)
- [x] Mode-based schema validation in `SchemaValidator`
- [ ] Model metadata extraction (docstrings, annotations)

### Testing Framework ✅
- [x] Testing framework architecture
- [x] Unit test support (`test_model`, `test_model_sync`, `mock_dependency`) — `testing.py` (220 lines)
- [x] Pytest plugin (auto-registered) — `testing.py`
- [ ] Integration test support (`test_pipeline`)
- [ ] Test coverage reporting

### Environments and Promotion ✅
- [x] Environment architecture
- [x] Environment configuration pattern (`config.yaml`, `config.{env}.yaml`)
- [x] Config overlay merging (deep-merge `config.{env}.yaml` over `config.yaml`)
- [x] Connection isolation per environment (access policies, shared flags)
- [x] Data promotion CLI (`interlace promote --from <env> --to <env>`)
- [x] Source data copying between environments

### User Functions
- [x] User functions architecture
- [ ] Custom Python function implementation
- [ ] Function discovery from `functions/` directory
- [ ] Function registration for SQL use

**Target:** Feature-complete for most data pipeline use cases

---

## Phase 3: Production Hardening ✅

**Goal:** Reliability, performance, and advanced observability

### Advanced Observability ✅
- [x] Observability architecture (metrics, logging, tracing)
- [x] OpenTelemetry tracing integration (`observability/tracing.py`)
- [x] Prometheus metrics export (`observability/metrics.py`)
- [x] Structured logging (`observability/structured_logging.py`)
- [ ] Custom metrics and business KPIs
- [ ] Alert configuration (freshness, success rate, latency)
- [ ] Dashboard generation

### Error Handling and Recovery ✅
- [x] Retry strategies with exponential backoff (`RetryPolicy`, `RetryManager`)
- [x] Error categorisation and handling strategies
- [x] Automatic recovery from transient failures

### Data Quality ✅
- [x] Data quality test framework (`QualityRunner`, `QualityCheck` base class)
- [x] Automated data quality checks (6 check types implemented)
  - `unique` - Column uniqueness validation
  - `not_null` - Null value detection
  - `accepted_values` - Value enumeration validation
  - `freshness` - Data recency checks
  - `row_count` - Row count thresholds
  - `expression` - Custom SQL expression checks
- [ ] Quality check integration into execution pipeline (framework complete, wiring pending)
- [ ] Data profiling and statistics
- [ ] Anomaly detection

### Security and Governance
- [x] Database architecture for audit logging (execution history)
- [x] Data lineage architecture for compliance
- [ ] API key authentication middleware
- [ ] Secrets management integration (Vault, AWS Secrets Manager)
- [ ] Role-based access control (RBAC)
- [ ] PII detection and handling

**Target:** Enterprise-ready with advanced observability and security

---

## Phase 4: Lineage and Documentation ✅

**Goal:** Rich documentation and lineage tracking

### Lineage ✅
- [x] Lineage architecture
- [x] Database schema for lineage tracking
- [x] Automatic dependency tracking implementation
- [x] Column-level lineage extraction (Python/ibis and SQL models)
- [x] Lineage API endpoints with state persistence
- [x] Impact analysis CLI (`interlace plan`) and API (`PlanHandler`)

### Documentation Generation
- [ ] Auto-generated documentation from metadata
- [ ] HTML documentation with interactive features
- [ ] Documentation serving (`interlace docs serve`)

### REST API ✅
- [x] REST API for programmatic access (20+ endpoints via aiohttp)
- [x] Model listing and detail endpoints
- [x] Run triggering and history endpoints
- [x] Real-time execution updates via SSE
- [x] Lineage and graph data endpoints
- [x] Schema history and diffing endpoints
- [x] Health check and plan endpoints
- [x] CORS support and SPA static serving
- [ ] API documentation (OpenAPI/Swagger)
- [ ] API authentication and authorisation

### Landing Website ✅
**Location:** `interlace.sh/`
**Tech Stack:** SvelteKit + Tailwind CSS

- [x] Product overview and features page
- [x] Getting started documentation
- [x] Blog (3 posts)
- [x] Documentation (15+ pages covering guides and reference)
- [x] Features and solutions pages
- [x] Deployable SvelteKit site

**Target:** Full self-service pipeline exploration and documentation ✅

---

## Phase 5: Advanced Features

**Goal:** Advanced capabilities for complex use cases

### Schema Migrations 🔄
- [x] Forward-only migration runner — `migrations/runner.py` (173 lines)
- [x] Migration CLI (`interlace migrate`) — `migrations/cli.py` (128 lines)
- [x] Migration versioning and tracking — `migration_runs` table in StateStore
- [x] Dry-run mode
- [ ] Migration rollback support (`down.sql` convention)
- [ ] Safe schema change validation

### Multi-Project Support
- [ ] Project-level configuration
- [ ] Cross-project model references
- [ ] Project promotion workflows

### Advanced Streaming
- [x] `@stream` decorator implementation — `core/stream.py` (773 lines)
- [x] HTTP endpoint generation — `service/api/handlers/streams.py`
- [x] 5 stream adapters (webhook, polling, pubsub, message queue, filesystem)
- [ ] Stream processing (windowing, aggregations)
- [ ] Stream-to-stream joins
- [ ] CDC integration implementation

### Advanced Incremental
- [x] Incremental execution architecture
- [x] Cursor-based backfill (`--since`/`--until`)
- [ ] Datetime window backfill (`--window` flag)
- [ ] CDC integration implementation
- [ ] Incremental materialised views

### Connectors and Integrations
- [x] Connection architecture (DuckDB, Postgres, S3, Filesystem)
- [x] `IbisConnection` generic class supporting 18+ backends
- [x] Extended DuckDB ATTACH (MySQL, SQLite, DuckDB cross-file, DuckLake)
- [x] S3 connection (full read/write operations via boto3)
- [ ] SaaS API connectors (Salesforce, HubSpot, etc.)
- [ ] Message queue connectors (Kafka, Kinesis, Pub/Sub)
- [ ] BI tool integrations (Tableau, Power BI, Looker)

**Target:** Advanced capabilities for complex data engineering workflows

---

## Phase 6: Scale and Enterprise

**Goal:** Scale to large organisations and complex deployments

### Distributed Execution
- [ ] Multi-worker execution
- [ ] Distributed task queue (Celery, Ray, etc.)
- [ ] Resource scheduling

### High Availability
- [ ] Scheduler high availability
- [ ] Failover and recovery
- [ ] Multi-region support

### Enterprise Features
- [ ] Multi-tenancy support
- [ ] Resource quotas and limits
- [ ] Cost tracking and optimisation

### Developer Experience
- [ ] VS Code extension
- [ ] IntelliSense and autocomplete
- [ ] Model debugging tools

### Community and Ecosystem
- [ ] Plugin system for custom connectors
- [ ] Model marketplace/sharing
- [ ] Community-contributed functions

**Target:** Enterprise scale with rich developer experience

---

## Priority Matrix

### Must Have (Phase 0-1) ✅
- Core engine and execution ✅
- Basic strategies and materialisation ✅
- CLI and configuration ✅
- Web UI ✅
- Scheduling ✅
- State management ✅
- Basic observability ✅
- Streaming (`@stream` decorator) ✅
- Testing framework ✅
- Backfill (cursor-based) ✅

### Should Have (Phase 2-3) ✅ (Mostly Complete)
- Export formats ✅
- Schema evolution ✅
- Environments and promotion ✅
- Advanced observability ✅
- Error handling ✅
- Quality check integration into execution (pending)
- API authentication (pending)

### Nice to Have (Phase 4-6)
- Auto-generated documentation
- Migration rollback
- Advanced streaming features
- Enterprise scale features (multi-tenancy, RBAC)
- VS Code extension

---

## Milestones

### Milestone 1: MVP ✅ (Completed February 2025)
- Core engine functional ✅
- Basic CLI working ✅
- Simple pipelines executable ✅
- Basic materialisation ✅

### Milestone 2: Production Ready ✅ (Completed February 2026)
- Web UI functional ✅
- REST API complete ✅
- Scheduling operational ✅
- Schema evolution handled ✅
- Export formats supported ✅
- State management complete ✅
- Streaming (`@stream`) complete ✅
- Testing framework complete ✅
- Backfill (cursor-based) complete ✅

### Milestone 3: Feature Complete (In Progress)
- Full observability ✅
- Lineage and documentation ✅
- API authentication (pending)
- Quality check executor integration (pending)
- OpenAPI/Swagger docs (pending)
- Auto-generated documentation (pending)

### Milestone 4: Enterprise Ready (Future)
- Advanced features
- Scale capabilities
- Enterprise features
- Rich ecosystem

---

## Contribution Guidelines

Contributions are welcome! Please:
1. Check existing issues and roadmap
2. Open an issue for discussion before large PRs
3. Follow code style and testing requirements
4. Update documentation with changes
5. Add tests for new features

---

## Notes

- **Flexibility**: Roadmap is subject to change based on community feedback and priorities
- **Iterative**: Features may be delivered incrementally within phases
- **Community-Driven**: Priority given to features requested by users
- **Quality First**: Features must meet quality standards before moving to next phase

---

## How to Track Progress

- **Issues**: GitHub issues track specific features and bugs
- **Project Boards**: Phase-based project boards show progress
- **Releases**: Version releases align with milestones
- **Changelog**: CHANGELOG.md tracks completed features

---

*Last updated: February 2026*
