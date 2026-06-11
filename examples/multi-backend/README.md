# Multi-Backend — Cross-Database Patterns

Query Postgres, SQLite, and CSV data from a single DuckDB pipeline using DuckDB ATTACH. This example shows how Interlace coordinates reads across multiple backends while keeping analytical storage in DuckDB.

## What You'll Learn

- DuckDB ATTACH for Postgres and SQLite databases
- Cross-database joins (Postgres + CSV within DuckDB)
- Environment variable substitution with `${VAR:-default}` syntax
- Environment overlays (`config.dev.yaml`, `config.prod.yaml`)
- Docker Compose setup for a local Postgres instance
- Parquet export

## How DuckDB ATTACH Works

DuckDB can attach external databases and query them as if they were local schemas. In `config.yaml`, the `attach` block under a DuckDB connection tells Interlace to run `ATTACH` statements at connection time:

```yaml
connections:
  default:
    type: duckdb
    path: "data/warehouse.duckdb"
    attach:
      - type: postgres
        name: pg           # referenced as pg.public.table_name
        read_only: true
        config:
          host: ${POSTGRES_HOST:-localhost}
          port: ${POSTGRES_PORT:-5432}
          database: ${POSTGRES_DB:-ecommerce}
          user: ${POSTGRES_USER:-postgres}
          password: ${POSTGRES_PASSWORD:-postgres}
      - type: sqlite
        name: legacy       # referenced as legacy.table_name
        path: "data/legacy.sqlite"
        read_only: true
```

Models then query attached databases with fully-qualified names:

```python
# Postgres table via ATTACH
sql("SELECT * FROM pg.public.customers")

# SQLite table via ATTACH
sql("SELECT * FROM legacy.orders")
```

DuckDB pulls data across the wire, joins it locally, and writes results to its own storage — no ETL scripts, no staging tables, no intermediate files.

## Models

| Model | Source | Strategy | Key Features |
|-------|--------|----------|--------------|
| `pg_customers` | Postgres (ATTACH) | `replace` | Reads from `pg.public.customers` |
| `legacy_orders` | SQLite (ATTACH) | `replace` | Reads from `legacy.orders` |
| `local_enrichment` | CSV file | `replace` | Industry/region enrichment data |
| `enriched_customers` | Cross-database join | `replace` | Postgres customers + CSV enrichment |
| `customer_export` | DuckDB | `replace` | Parquet export of enriched data |

## Run It

### 1. Start Postgres

```bash
cd examples/multi-backend
docker compose up -d
```

This starts a Postgres 16 container seeded with `init.sql` (10 customers, 12 orders).

### 2. Run the pipeline

```bash
interlace run
```

### 3. Check output

The `customer_export` model writes enriched data to `output/customers.parquet`.

## Environment Overlays

### Development — no Postgres required

The dev overlay strips the Postgres attachment so you can test SQLite and CSV models without Docker:

```bash
INTERLACE_ENV=dev interlace run
```

This uses `config.dev.yaml`, which only attaches the SQLite database. The `pg_customers` and `enriched_customers` models will be skipped (their source is unavailable).

### Production — real credentials via env vars

```bash
INTERLACE_ENV=prod \
  POSTGRES_HOST=db.prod.internal \
  POSTGRES_DB=ecommerce \
  POSTGRES_USER=pipeline \
  POSTGRES_PASSWORD=secret \
  interlace run
```

The prod overlay uses `config.prod.yaml` with `WARNING`-level logging and requires all environment variables to be set (no defaults for host, database, user, or password).

## DuckDB-Only Fallback

You do not need Postgres or Docker to experiment with ATTACH patterns. DuckDB can attach any SQLite file:

```yaml
connections:
  default:
    type: duckdb
    path: "data/warehouse.duckdb"
    attach:
      - type: sqlite
        name: legacy
        path: "data/legacy.sqlite"
        read_only: true
```

Create a SQLite database with sample data, attach it, and query it the same way. This is useful for local development, CI pipelines, and environments where running Postgres is impractical.

## Project Structure

```
multi-backend/
├── config.yaml              # Main config: DuckDB + Postgres + SQLite ATTACH
├── config.dev.yaml          # Dev overlay: SQLite only, debug logging
├── config.prod.yaml         # Prod overlay: full backends, warning logging
├── docker-compose.yml       # Postgres 16 with seed data
├── init.sql                 # Postgres seed: customers + orders tables
├── pyproject.toml
├── data/
│   └── enrichment.csv       # 10-row industry/region enrichment
├── models/
│   ├── sources.py           # 3 source models (Postgres, SQLite, CSV)
│   └── analytics.py         # 2 analytics models (cross-join, export)
└── output/                  # Generated: Parquet export
```

## Next Steps

- [ecommerce](../ecommerce/) — full-featured pipeline with all strategies and quality checks
- [data-warehouse](../data-warehouse/) — star schema patterns and slowly changing dimensions
