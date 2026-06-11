# Incremental -- Incremental Processing

A SaaS analytics pipeline demonstrating cursor-based incremental processing, caching strategies, scheduled models, Parquet export, and the programmatic `run()` API with backfill support.

## What You'll Learn

- **Cursor-based processing** -- `cursor="event_date"` tracks the last-processed value so repeat runs only handle new data
- **Incremental config** -- `incremental={"type": "key", "key_column": "event_id"}` for key-based deduplication
- **Caching strategies** -- `cache={"strategy": "if_exists"}` skips re-reads; `cache={"ttl": "6h"}` expires after a time window
- **Scheduled models** -- `schedule={"cron": "0 9 * * 1"}` runs a model on a cron cadence
- **Export to Parquet** -- `export={"format": "parquet", "path": "output/weekly.parquet"}` writes results to file
- **Retry policies** -- `retry_policy=API_RETRY_POLICY` adds exponential backoff for flaky API calls
- **Side-effect models** -- `materialise="none"` for models that log, notify, or call APIs without storing data
- **Programmatic API** -- `run_sync()` and `run()` for embedding pipelines in scripts with backfill via `since`/`until`

## Models

| Model | Type | Strategy | Materialisation | Key Features |
|-------|------|----------|-----------------|--------------|
| `user_events` | Source (CSV) | `append` | table | Event stream |
| `feature_flags` | Source (CSV) | `replace` | table | `cache={"strategy": "if_exists"}` |
| `billing_data` | Source (API mock) | `replace` | table | `cache={"ttl": "6h"}`, `retry_policy` |
| `event_enrichment` | Transform | -- | table | `incremental={"type": "key", "key_column": "event_id"}` |
| `daily_active_users` | Metric | `append` | table | `cursor="event_date"` |
| `usage_notifications` | Side-effect | -- | none | `materialise="none"`, `cursor="event_id"` |
| `weekly_report` | Output | -- | table | `schedule`, `export` to Parquet |

## Run It

```bash
cd examples/incremental
interlace run
```

### Programmatic API

```bash
python run.py
```

### Backfill a date range

Uncomment the backfill section in `run.py`, or run directly:

```python
from pathlib import Path
from interlace import run_sync

result = run_sync(
    project_dir=Path("examples/incremental"),
    since="2024-01-01",
    until="2024-01-31",
    force=True,
)
```

## Project Structure

```
incremental/
├── config.yaml          # Connection + state + defaults
├── pyproject.toml       # Project metadata
├── run.py               # Programmatic API script
├── data/
│   ├── user_events.csv  # 32 SaaS user events (Jan-Mar 2024)
│   └── feature_flags.csv# 8 feature flag configs
└── models/
    ├── sources.py       # user_events + feature_flags + billing_data
    ├── processing.py    # event_enrichment + daily_active_users + usage_notifications
    └── outputs.py       # weekly_report (scheduled + Parquet export)
```

## How Incremental Processing Works

1. **First run** -- All data is processed and cursors are initialised
2. **Subsequent runs** -- Only rows with cursor values greater than the last checkpoint are processed
3. **Backfill** -- Pass `since`/`until` with `force=True` to reprocess a historical window

Cursors are tracked in the state store (DuckDB) and persist across runs.

## Next Steps

- [ecommerce](../ecommerce/) -- Full-featured project with quality checks, schema evolution, and testing
- [testing](../testing/) -- Deep dive into `interlace.testing` utilities
