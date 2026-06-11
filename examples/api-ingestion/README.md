# API Ingestion — REST API Patterns

Fetch live weather data from the [Open-Meteo](https://open-meteo.com/) API and transform it into analytics tables. This example covers every common REST-API integration pattern: retry policies, response caching, scheduled refresh, cursor-based incremental ingestion, and side-effect models.

Open-Meteo is free and requires no API key, so you can run this example immediately.

## What You'll Learn

- Using the `API` helper class for HTTP requests (`api.get()`)
- `retry_policy=API_RETRY_POLICY` — automatic retry with exponential backoff and jitter
- `cache={"ttl": "1h"}` — skip re-fetching when data is still fresh
- `schedule={"every_s": "3600"}` — hourly refresh when running `interlace serve`
- `cursor="date"` — incremental append that tracks the last ingested date
- `materialise="none"` — side-effect models that log warnings without persisting output
- SQL models with metadata comments

## Models

| Model | Type | Key Features |
|-------|------|--------------|
| `weather_current` | Source (API) | `retry_policy`, `cache={"ttl": "1h"}` |
| `weather_forecast` | Source (API) | `schedule={"every_s": "3600"}`, async |
| `weather_historical` | Source (API) | `cursor="date"`, incremental append |
| `weather_alerts` | Side-effect | `materialise="none"`, logs extreme temperatures |
| `temperature_analysis` | Analytics | `strategy="replace"`, ibis aggregations |
| `weekly_summary` | Analytics (SQL) | SQL aggregation by week |

## Run It

```bash
pip install interlace   # or: uv add interlace
cd examples/api-ingestion
interlace run
```

### Scheduled mode

To run the pipeline continuously with hourly forecast refreshes:

```bash
interlace serve
```

## Project Structure

```
api-ingestion/
├── config.yaml          # Connection, retry, circuit breaker, DLQ settings
├── pyproject.toml
├── data/                # Generated: DuckDB database
└── models/
    ├── sources.py       # 4 API source models (current, forecast, historical, alerts)
    ├── analytics.py     # Temperature analysis
    └── weekly_summary.sql
```

## Configuration Highlights

The `config.yaml` sets project-wide retry defaults:

```yaml
retry:
  max_attempts: 5
  initial_delay: 2
  max_delay: 60
  exponential_base: 2.0
  jitter: true
```

Individual models can override these with `retry_policy=RetryPolicy(...)`.

A **circuit breaker** (`failure_threshold: 10`) prevents hammering a failing API,
and a **dead letter queue** (`max_entries: 500`) captures failed runs for
manual inspection.

## API Patterns Explained

### Retry with backoff

`weather_current` uses `API_RETRY_POLICY` — a pre-configured policy that retries
up to 5 times with exponential backoff (2s, 4s, 8s, 16s, 32s) plus random jitter
to prevent thundering-herd effects.

### TTL caching

The `cache={"ttl": "1h"}` on `weather_current` means the model is skipped if its
last successful run was less than one hour ago. Useful for expensive or rate-limited
endpoints.

### Cursor-based incremental ingestion

`weather_historical` sets `cursor="date"`. The executor stores the most recent
`date` value after each run. On the next execution, only rows with `date >
last_cursor_value` are appended, avoiding duplicate data.

### Side-effect models

`weather_alerts` has `materialise="none"` — it reads the forecast, logs warnings
for extreme temperatures, and returns nothing. Side-effect models are useful for
notifications, alerting, and audit logging.

## Next Steps

- [ecommerce](../ecommerce/) — Full-featured project with all strategies and quality checks
- [testing](../testing/) — Deep dive into `interlace.testing` with mocks and assertions
