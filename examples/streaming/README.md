# Streaming — Event-Driven Pipelines

Real-time IoT sensor ingestion with the `@stream` decorator and pub/sub API. Events flow in via `publish()`, get processed by consumer models, and can trigger live alerts via `subscribe()`.

## What You'll Learn

- Defining streams with `@stream` (schema validation, retention, auth, rate limiting)
- Publishing events with `publish_sync()` and `publish()`
- Subscribing to live events with `subscribe()` and `filter_fn`
- Consumer models that depend on streams (stream-to-model dependency chain)
- Materialisation strategies for stream consumers: `append`, `none`, `view`

## Domain

An IoT sensor network with five temperature/humidity sensors. Two streams receive data:

| Stream | Purpose | Features |
|--------|---------|----------|
| `sensor_readings` | Temperature + humidity data | Schema validation, 30-day retention |
| `system_events` | Diagnostic events | API key auth, rate limiting |

Three consumer models process the stream data:

| Model | Strategy | Materialisation | Purpose |
|-------|----------|-----------------|---------|
| `sensor_hourly_avg` | `append` | table | Hourly averages per sensor |
| `sensor_anomalies` | — | `none` | Log anomalous readings (side-effect) |
| `sensor_dashboard` | — | `view` | Latest reading per sensor |

## Usage Flow

### 1. Publish sensor events

```bash
cd examples/streaming
python scripts/publish_events.py
```

This simulates 20 sensor readings using `publish_sync()`. Events are appended to the `sensor_readings` stream table.

### 2. Run the pipeline

```bash
interlace run
```

Consumer models process the stream data:
- `sensor_hourly_avg` aggregates readings by hour
- `sensor_anomalies` logs any out-of-range values
- `sensor_dashboard` builds a view of the latest reading per sensor

### 3. Subscribe for live alerts

In a separate terminal, start the subscriber:

```bash
python scripts/subscribe_demo.py
```

Then publish more events — the subscriber filters for temperatures above 30C and prints alerts in real time.

## How It Works

### Stream Definition

The `@stream` decorator creates an append-only ingestion table. Unlike `@model`, stream functions don't execute logic — they declare the schema, validation rules, and endpoint configuration:

```python
@stream(
    name="sensor_readings",
    fields={"sensor_id": "string", "temperature": "float64", ...},
    validate_schema=True,
    retention={"max_age_days": 30},
)
def sensor_readings():
    pass
```

### Publish/Subscribe

Events enter the system via `publish()` (async) or `publish_sync()`:

```python
from interlace import publish_sync
publish_sync("sensor_readings", {"sensor_id": "s1", "temperature": 22.5, ...})
```

Real-time consumers use `subscribe()` with optional filtering:

```python
from interlace import subscribe
async for event in subscribe("sensor_readings", filter_fn=lambda e: e["temperature"] > 30):
    handle_alert(event)
```

### Stream-to-Model Dependencies

Consumer models declare stream dependencies via ibis.Table parameters, just like regular model dependencies:

```python
@model(name="sensor_hourly_avg", strategy="append")
def sensor_hourly_avg(sensor_readings: ibis.Table) -> ibis.Table:
    ...
```

Interlace resolves the dependency graph so consumer models run after the stream has received data.

## Project Structure

```
streaming/
├── config.yaml              # Connection + defaults
├── models/
│   ├── streams.py           # 2 stream definitions
│   └── consumers.py         # 3 consumer models
└── scripts/
    ├── publish_events.py    # Simulate sensor data
    └── subscribe_demo.py    # Real-time alert subscriber
```

## Next Steps

- [ecommerce](../ecommerce/) — Full-featured project with all strategies and materialisations
- [testing](../testing/) — Unit testing models with `test_model_sync()` and `mock_dependency()`
