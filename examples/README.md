# Examples

Three projects, in reading order:

| project | size | what it shows |
| --- | --- | --- |
| [`getting_started`](getting_started/) | 4 models | The core loop: a DAG over inline seed data, per-model config, a view, column contracts. `plan` → `apply` → query the env views. |
| [`platform_tour`](platform_tour/) | 6 models | Every pillar in one small project: a durable ingestion stream (dedup, retention), an SCD2 dimension, a Python model over Arrow, promotion-gating checks, forward-only changes, and a reverse-ETL sink into an attached database. |
| [`benchmark`](benchmark/) | 10 models, 25M rows | Load: a fan-out DAG that builds branches concurrently, an ephemeral model scanned 4×, incremental windows with catchup/restate, Arrow streaming through Python, and a Parquet sink — with reference timings. |

Each is self-contained: `cd` in and run `interlace apply` (no external services;
data is generated in-engine or seeded inline).
