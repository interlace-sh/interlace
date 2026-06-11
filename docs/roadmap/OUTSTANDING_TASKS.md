# Outstanding Tasks

**Date:** April 2026
**Scope:** Remaining work items prioritised by impact.

---

## 🔴 High Priority (v0.2.0 Beta)

### 1. API Key Authentication Middleware

**Status:** NOT STARTED
**Priority:** High - Production blocker for `interlace serve`

The stream handler already has per-stream bearer/api-key auth (`service/api/handlers/streams.py`). This needs extracting into global middleware.

**Work Required:**
- API key authentication middleware (`service/api/middleware/auth.py`)
- Config via `service.auth` in `config.yaml`
- Whitelist paths (e.g. `/health`)
- Refactor stream-level auth to delegate to shared middleware
- Rate limiting (token bucket, `ErrorCode.RATE_LIMITED` already defined)

**Effort:** Medium (3-4 days)

---

### 2. Quality Check `--quality-only` CLI Flag

**Status:** MINOR REMAINING WORK
**Priority:** Medium

Quality check executor integration is **complete**:
- `@model(quality_checks=...)` decorator parameter (`core/model.py:168`)
- Config-level check merging during initialisation (`core/initialization.py:181-193`)
- Post-materialisation execution via `QualityCheckRunner` (`core/execution/model_executor.py:447-482`)
- Result persistence to `interlace.quality_results` state table (`core/state.py:268-292`)
- Pipeline blocking on ERROR severity with `quality_fail_on_error` flag

**Remaining:**
- `--quality-only` flag for `interlace run` to execute checks without re-materialising

**Effort:** Low (1 day)

---

### 3. OpenAPI/Swagger API Documentation

**Status:** NOT STARTED
**Priority:** High - Adoption requirement for Beta

**Work Required:**
- Hand-write `openapi.yaml` for all 20+ endpoints
- Serve Swagger UI at `/api/docs`
- Add routes for docs and openapi.yaml

**Effort:** Medium (2-3 days)

---

## 🟡 Medium Priority (v1.0 GA)

### 4. Auto-Generated Documentation Site

**Status:** NOT STARTED
**Priority:** Medium - Marquee feature for GA (dbt's strongest feature)

**Work Required:**
- `interlace docs build` — static HTML from model metadata, lineage, schema
- `interlace docs serve` — localhost with live reload
- Jinja2 templates for model catalogue, lineage visualisation, schema docs
- Data from StateStore (model_metadata, column_lineage, schema_history)

**Effort:** High (5-7 days)

---

### 5. Migration Rollback Support

**Status:** FORWARD-ONLY IMPLEMENTED
**Priority:** Medium - Production expectation

Forward-only migrations are fully working (`migrations/runner.py`, `migrations/cli.py`). Rollback needs adding.

**Work Required:**
- File convention: `001_description.up.sql` / `001_description.down.sql`
- `interlace migrate --rollback` to run latest down-migration
- `interlace migrate --to N` for targeted rollback
- Extend `migrations/runner.py` and `migrations/cli.py`

**Effort:** Medium (3-4 days)

---

### 6. User Function Discovery

**Status:** NOT STARTED
**Priority:** Medium - Documented feature

`interlace init` creates a `functions/` directory. The discovery and registration needs implementing.

**Work Required:**
- Function discovery in `utils/discovery.py`
- Register Python functions as DuckDB UDFs
- Register SQL macros from `.sql` files
- Auto-available in SQL models

**Effort:** Medium (2-3 days)

---

## 🟢 Lower Priority (Post-1.0)

### 7. DuckLake S3/Parquet Storage Layer

**Status:** ARCHITECTURE ONLY
**Priority:** Lower - Cloud-native pattern, niche use case

**Effort:** Medium (2 weeks)

---

### 8. VS Code Extension

**Status:** NOT STARTED
**Priority:** Lower - Developer experience enhancement

**Effort:** High (4+ weeks)

---

### 9. Distributed Execution

**Status:** NOT STARTED
**Priority:** Lower - Enterprise scale

**Effort:** Very High (6+ weeks)

---

## ✅ Recently Completed (Reference)

These items were previously listed as outstanding but are now fully implemented:

| Feature | Status | Evidence |
|---------|--------|----------|
| **`@stream` decorator** | ✅ Complete | `core/stream.py` (773 lines), 5 adapters, 42 tests |
| **Testing framework** | ✅ Complete | `testing.py` (220 lines), pytest plugin, 18 tests |
| **Cursor-based backfill** | ✅ Complete | `model_executor.py`, `cli/run.py` `--since`/`--until`, 11 tests |
| **Schema migrations (forward-only)** | ✅ Complete | `migrations/runner.py`, `migrations/cli.py`, version tracking |
| **Web UI** | ✅ Complete | Svelte 5 + ELK.js DAG, model browser, run history, SSE monitoring |
| **REST API** | ✅ Complete | 20+ endpoints, 9 handler modules, SSE streaming, CORS |
| **Landing Website** | ✅ Complete | SvelteKit site with docs, blog, features, solutions pages |
| **Scheduling** | ✅ Complete | Cron + interval scheduling with persistence, misfire handling, status API |
| **Lineage** | ✅ Complete | Column-level extraction (Python/ibis + SQL), API, state persistence |
| **State Management** | ✅ Complete | StateStore with 11 tables, full CRUD |
| **Config Overlay Merging** | ✅ Complete | `config.{env}.yaml` deep-merged over `config.yaml` |
| **Export Formats** | ✅ Complete | CSV, Parquet, JSON exporters via DuckDB COPY |
| **Schema Flexibility** | ✅ Complete | SchemaMode enum (5 modes), mode-based validation |
| **`interlace promote`** | ✅ Complete | Cross-environment data promotion CLI |
| **IbisConnection** | ✅ Complete | Generic class supporting 18+ ibis backends |
| **Connection Policies** | ✅ Complete | `access`, `shared` flags for connection control |
| **DuckDB ATTACH** | ✅ Complete | MySQL, SQLite, DuckDB cross-file, DuckLake |
| **Source Cache TTL** | ✅ Complete | `cache={"ttl": "7d"}` on `@model` |
| **Impact Analysis** | ✅ Complete | `interlace plan` CLI + API endpoint |
| **Schema Diffing** | ✅ Complete | API endpoint + CLI support |
| SCD Type 2 Strategy | ✅ Complete | Hash-based change detection |
| Retry Framework | ✅ Complete | Policy + manager with exponential backoff |
| Data Quality Checks | ✅ Complete | 6 check types + runner (not yet integrated into execution) |
| Observability | ✅ Complete | Prometheus, OpenTelemetry, structured logging |

---

## Recommended Implementation Order

1. **API key auth middleware** — Production blocker for `interlace serve`
2. **Quality check executor integration** — Framework exists, just needs wiring
3. **OpenAPI/Swagger docs** — Adoption requirement for Beta
4. **Auto-generated docs site** — Marquee feature for GA
5. **Migration rollback** — Production expectation
6. **User function discovery** — Documented feature

---

## See Also

- [`ROADMAP.md`](ROADMAP.md) - Full development roadmap
- [`IMPLEMENTATION_STATUS.md`](IMPLEMENTATION_STATUS.md) - Feature status reference
