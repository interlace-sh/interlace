# Examples

Two ways in: **scaffold your own** project from a template, or **read** one of the
reference projects below.

## Start your own — `interlace init`

`interlace init` scaffolds a runnable starter project (each ships in the wheel with a
README that doubles as its walkthrough). List what's available:

```bash
interlace init --list
```

Then scaffold one — `quickstart` is the default:

```bash
interlace init my_project                      # quickstart
interlace init my_pipe --template github       # a specific template
```

| template | needs | what it is |
| --- | --- | --- |
| [`quickstart`](../src/interlace/templates/quickstart/) *(default)* | — | A no-source starter: a SQL seed, a Python model over it, and a SQL rollup with checks. The core `plan` → `apply` loop with zero setup. |
| [`events`](../src/interlace/templates/events/) | `interlaced[service]` | Durable HTTP event ingestion — a `@stream` with exactly-once rollups — run under `interlace serve`. Shows the streaming pillar end to end. |
| [`github`](../src/interlace/templates/github/) | `interlaced[sources]` | Pull GitHub issues incrementally via the REST source client: a real API landed as an ordinary table you can model on (a `merge` source keyed on a cursor). |
| [`postgres`](../src/interlace/templates/postgres/) | Docker + `interlaced[postgres]` | Incrementally pull from a Postgres source; a bundled `docker-compose` seeds a database to pull from. Shows a keyed incremental DB source. |

## Reference projects — read these

Four self-contained projects, in reading order. `cd` in and run `interlace apply` (no
external services; data is generated in-engine or seeded inline).

| project | size | what it shows |
| --- | --- | --- |
| [`getting_started`](getting_started/) | 4 models | The core loop: a DAG over inline seed data, per-model config, a view, column contracts. `plan` → `apply` → query the env views. |
| [`platform_tour`](platform_tour/) | 6 models | Every pillar in one small project: a durable ingestion stream (dedup, retention), an SCD2 dimension, a Python model over Arrow, promotion-gating checks, forward-only changes, and a reverse-ETL delivery into an attached database. |
| [`materialisations`](materialisations/) | 15 models | Reference matrix: **every `materialise` plane × `strategy`** — virtual (full/merge/full_merge/incremental/scd2), view, ephemeral, terminal `table` (full/append/merge/full_merge/incremental) and `file` (parquet/csv/json). |
| [`benchmark`](benchmark/) | 10 models, 25M rows | Load: a fan-out DAG that builds branches concurrently, an ephemeral model scanned 4×, incremental windows with catchup/restate, Arrow streaming through Python, and a Parquet file — with reference timings. |

Templates are the runnable starting points; the reference projects are here to read.
