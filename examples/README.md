# Interlace Examples

A progressive learning path from your first pipeline to production patterns.

## Learning Path

### Tier 1 — Getting Started

| Example | Time | What You'll Learn |
|---------|------|-------------------|
| [quickstart](quickstart/) | 5 min | `@model`, CSV loading, strategies, views |
| [ecommerce](ecommerce/) | 20 min | All strategies, materialisations, checks, testing, SQL models |

### Tier 2 — Production Patterns

| Example | Time | What You'll Learn |
|---------|------|-------------------|
| [testing](testing/) | 15 min | `test_model_sync()`, `mock_dependency()`, `TestResult`, pytest integration |
| [api-ingestion](api-ingestion/) | 15 min | `retry_policy=`, `cache=`, `schedule=`, `cursor=`, async models |
| [streaming](streaming/) | 15 min | `@stream`, `publish()`, `subscribe()`, `consume()`, event-driven pipelines |
| [incremental](incremental/) | 15 min | `cursor=`, `cache=`, `incremental=`, `export=`, `run()` API, backfill |

### Tier 3 — Advanced

| Example | Time | What You'll Learn |
|---------|------|-------------------|
| [data-warehouse](data-warehouse/) | 30 min | `strategy="scd_type_2"`, star schema, freshness checks, relationships checks |
| [multi-backend](multi-backend/) | 20 min | DuckDB ATTACH (Postgres, SQLite), cross-DB joins, environment overlays |

### Benchmarks

| Example | Description |
|---------|-------------|
| [benchmarks/tpch](benchmarks/tpch/) | 22 standard TPC-H queries — SQL model reference |
| [benchmarks/tpcds](benchmarks/tpcds/) | 99 TPC-DS queries — advanced SQL patterns |

## Feature Coverage

| Feature | QS | EC | TST | API | STR | INC | DW | MB |
|---------|:--:|:--:|:---:|:---:|:---:|:---:|:--:|:--:|
| `@model` basics | x | x | x | x | x | x | x | x |
| `@stream` | | | | | **x** | | | |
| `strategy="scd_type_2"` | | | | | | | **x** | |
| `cursor=` | | | | **x** | x | **x** | | |
| `export=` | | **x** | | | | **x** | | x |
| `cache=` | | | | **x** | | **x** | | |
| `schedule=` | | | | **x** | | **x** | | |
| `schema_mode=` | | **x** | | | | | x | |
| `column_mapping=` | | **x** | | | | | | |
| `materialise="none"` | | | | **x** | **x** | **x** | | |
| Model `retry_policy=` | | | | **x** | | x | | |
| Inline `checks=` | | **x** | | | | | x | |
| `interlace.testing` | | x | **x** | | | | | |
| DuckDB ATTACH | | | | | | | | **x** |
| `run()` API | | | x | | | **x** | | |
| `incremental=` | | | | | | **x** | | |
| `strict=True` / `fields=` | | **x** | x | | x | | x | |
| `publish()` / `subscribe()` | | | | | **x** | | | |
| Environment overlays | | x | | | | | | x |
| SQL models | | x | | x | | | | |
| Migrations | | x | | | | | | |
| Tags / owner | | x | | | | | x | |
| Async models | | | x | x | x | | | |
| Checks: freshness | | | | | | | **x** | |
| Checks: relationships | | **x** | | | | | **x** | |
| Checks: pattern | | **x** | | | | | | |
| SQL check files | | **x** | | | | | | |

**Bold** = primary teaching target for that feature.

**Legend:** QS=quickstart, EC=ecommerce, TST=testing, API=api-ingestion, STR=streaming, INC=incremental, DW=data-warehouse, MB=multi-backend

## Quick Start

```bash
pip install interlace   # or: uv add interlace

cd examples/quickstart
interlace run
```

## Project Structure

Every example follows the same layout:

```
example-name/
├── config.yaml     # Connection + pipeline config
├── pyproject.toml   # Python project metadata
├── README.md        # What you'll learn + how to run
├── data/            # CSV seed data
└── models/          # @model and @stream definitions
```
