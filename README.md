# Interlace

A modern, Python/SQL-first data orchestration and pipeline framework.

Interlace is an alternative to dbt and traditional DAG-based frameworks, designed for teams that want to write data pipelines as Python functions or SQL files with automatic dependency resolution, parallel execution, and built-in quality checks.

```bash
pip install interlace
```

**Requires Python 3.12+**

## Quick Start

### 1. Create a project

```bash
interlace init my-project
cd my-project
```

This creates:
```
my-project/
├── config.yaml          # Connection and project configuration
├── models/              # Your model files live here
│   └── example.py
├── tests/
└── .gitignore
```

### 2. Define models

**Python model** (`models/users.py`):
```python
from interlace import model

@model(name="users", materialize="table", strategy="replace")
def users():
    return [
        {"id": 1, "name": "Alice", "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "email": "bob@example.com"},
    ]
```

**SQL model** (`models/active_users.sql`):
```sql
-- @model(name="active_users", materialize="table", dependencies=["users"])
SELECT * FROM users WHERE active = true
```

### 3. Configure connections

**`config.yaml`**:
```yaml
name: my-project

connections:
  default:
    type: duckdb
    path: data/{env}/main.duckdb

state:
  connection: default
  schema: interlace

models:
  default_schema: public
  default_materialize: table
```

### 4. Run

```bash
interlace run                    # Run all models
interlace run users active_users # Run specific models (+ their deps)
interlace run --force            # Bypass change detection
interlace run --since 2024-01-01 # Backfill from date
```

## Features

### Core
- **`@model` decorator** -- define Python or SQL models with rich configuration
- **`@stream` decorator** -- append-only event ingestion tables with webhooks
- **Automatic dependency resolution** -- SQL dependencies parsed via sqlglot
- **Dynamic parallel execution** -- asyncio + thread pool, per-task connections
- **Change detection** -- skip unchanged models via file-hash tracking

### Materialization & Strategies
- **Materializations**: `table`, `view`, `ephemeral`, `none`
- **Strategies**: `replace`, `append`, `merge_by_key`, `scd_type_2`, `none`
- **Schema evolution**: 5 modes (`strict`, `safe`, `flexible`, `lenient`, `ignore`)

### Connections
- **DuckDB** (default, in-memory or file-based)
- **PostgreSQL** (asyncpg with connection pooling)
- **Generic ibis backends** (Snowflake, BigQuery, MySQL, ClickHouse, etc.)
- **Storage**: S3, Filesystem, SFTP

### Reliability
- **Retry framework** with exponential backoff, jitter, and configurable policies
- **Circuit breaker** for fail-fast on cascading failures
- **Dead letter queue** for inspecting failed executions
- **Cursor-based incremental** processing with automatic watermarking

### Quality
- Built-in checks: `not_null`, `unique`, `accepted_values`, `row_count`, `freshness`, `expression`
- Configurable severity: `warn` or `error`

### Observability
- **Prometheus metrics** -- execution time, row counts, error rates
- **OpenTelemetry tracing** -- model execution spans
- **Structured logging** -- JSON logs with correlation IDs
- **Column-level lineage** -- automatic provenance tracking

### Service Mode
- `interlace serve` -- long-running service with REST API, scheduler, and SFTP sync
- **Web UI** (SvelteKit) -- model browser, flow history, lineage graph
- **SSE events** -- real-time execution updates

## Programmatic API

```python
from interlace import run, run_sync

# Async
results = await run(project_dir=".", models=["users"])

# Sync
results = run_sync(project_dir=".", force=True)
```

## Testing

```python
from interlace import test_model, mock_dependency

async def test_my_model():
    users = mock_dependency([{"id": 1, "name": "Alice"}])
    result = await test_model(my_model, dependencies={"raw_users": users})
    assert result.row_count > 0
    assert "name" in result.columns
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `interlace run` | Execute models |
| `interlace serve` | Start service (API + scheduler) |
| `interlace init` | Create new project |
| `interlace info` | Show project/model information |
| `interlace plan` | Preview execution plan |
| `interlace schema` | Inspect and validate schemas |
| `interlace lineage` | View data lineage |
| `interlace config` | Manage configuration |
| `interlace migrate` | Run SQL migrations |
| `interlace promote` | Promote between environments |

## Examples

See the [`examples/`](examples/) directory for complete working projects:

- **basic/** -- CSV ingestion, strategies, simple joins
- **comprehensive/** -- all features (materialization types, strategies, migrations)
- **api/** -- REST API consumption and transformation
- **tpch/** -- TPC-H benchmark (22 standard queries)
- **sftp/** -- SFTP sync with PGP decryption

## Development

```bash
# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest tests/

# Lint & format
ruff check src/ tests/
black src/ tests/

# Type check
mypy src/interlace/

# Build
python -m build
```

## License

MIT
