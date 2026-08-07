# interlace

**Python/SQL-first data platform: transformation, orchestration, and durable streaming — one process.**

interlace is an independent, MIT-licensed alternative to dbt/SQLMesh that also replaces the
orchestrator (no Airflow) and the ingestion layer (Cloudflare-Pipelines-style durable streams).
Models are `.sql` files or Python functions; state is versioned snapshots with virtual
environments and a terraform-style plan/apply; everything runs in a single daemon on
DuckDB + DuckLake by default.

> **Status: 2.0.** Requires Python 3.12+.
> The package is published to PyPI as **`interlaced`**; the import name and CLI are `interlace`.

```bash
pip install 'interlaced[service]'   # the CLI + daemon; core CLI only: pip install interlaced
# more extras: [adbc] postgres/redshift · [spark] · [polars] · [all]
```

## Sixty seconds

```bash
interlace init my-project && cd my-project
interlace plan            # terraform-style preview: added / breaking / non-breaking / reuse
interlace apply           # build changed models, run checks, promote the environment
interlace serve           # the daemon: web UI (/ui) + HTTP API + scheduler + streams, one process
```

Every model builds into a fingerprinted physical table (`interlace__main.orders__a1b2c3`);
environments are views over those tables, so promotion and rollback are atomic view swaps and a
dev environment reuses prod's tables for free. **Production is the unprefixed namespace** —
consumers query `main.orders`; sandboxes are prefixed (`dev__main.orders`). Commands default to
prod; pass `--env dev` while developing.

## Models

**SQL** — a file per model; upstreams referenced by model name, dependencies inferred by parsing
(sqlglot), config in a leading comment block:

```sql
/* interlace:
  strategy: scd
  key: customer_id
  schedule: {cron: "0 * * * *"}
  checks:
    - not_null: customer_id
    - unique: customer_id
*/
SELECT customer_id, name, tier FROM raw_customers
```

**Python** — functions whose parameters name their upstreams; data crosses as Arrow
(never pandas), streamed with bounded memory:

```python
from interlace import model

@model(strategy="merge", key="order_id", cursor="updated_at")
def orders(cursor, this):
    """Incremental API extract: `cursor` is max(updated_at) already in the
    warehouse (None on first run); `this` is the previous materialisation."""
    rows = fetch_orders(since=cursor)          # your code
    return pyarrow.Table.from_pylist(rows)     # or RecordBatchReader / generator of batches
```

**Strategies:** `replace`, `view`, `ephemeral` (CTE-inlined), `merge` (upsert),
`full_merge` (full-state source applied as a minimal diff), `incremental` (one time window
at a time — rewrites the window, or upserts within it if you give it a `key`), `scd`
(history with validity windows).

## Plan / apply

```
$ interlace plan
 Model         Change    Category      Build
 orders        modified  non_breaking  rebuild
 order_stats   modified  non_breaking  reuse      <- output provably identical: not rebuilt
```

- Changes classify **breaking / non-breaking / forward-only**; a plan with breaking changes
  refuses to apply without `--force`. Downstream models whose output is provably identical
  (column-pruned impact analysis) **reuse their existing tables** instead of rebuilding — an
  improvement over model-granular invalidation.
- `apply --forward-only` lets history-keeping models (scd2/merge/incremental) survive a
  definition change: the existing table is copied to the new version, the new logic applies to
  the copy going forward, and checks gate before views move.
- **Checks gate promotion**: 10 built-in types (not_null, unique, accepted_values, row_count,
  freshness, expression, relationships, pattern, range, sql) plus `@check` Python functions —
  an error-severity failure blocks before the environment view moves. `interlace checks run`
  re-runs them ad hoc against any environment's promoted tables.
- **`--select state:modified`** scopes a plan/apply to models whose fingerprint drifted from the
  target environment, plus everything downstream — the CI diff, one flag. **`interlace impact
  model.column`** shows the column-level blast radius of a change.
- **`interlace env rollback`** repoints an environment's views at any earlier promotion — nothing
  rebuilds, the views move; every apply records a generation, so a bad deploy reverts in one
  command. `interlace gc` removes snapshots no environment references (reference-aware: tables
  shared through reuse survive).

## Streaming

Declare a stream; POST to it; rows are durable (SQLite WAL log) before the 200, deduplicated by
idempotency key, and materialized exactly-once into `streams.<name>` — a micro-batch flusher
commits the data and the watermark in one warehouse transaction, and SQL models just read the
table. A flush triggers the models that consume the stream.

```python
from interlace import stream

@stream("orders", schema={"order_id": "string", "total": "double"},
        idempotency_key="order_id", retention="7d", on_schema_drift="evolve")
def orders(event): ...
```

```bash
curl -X POST localhost:8000/streams/orders -d '{"order_id": "o1", "total": 49.5}'
```

Schema drift is yours to choose: `reject` (400), `evolve` (new columns appear), or
`quarantine` (bad events divert to `<stream>__quarantine`). When the warehouse falls behind,
publishes get **429 backpressure** instead of unbounded backlog.

## Reverse ETL

Attach external databases and deliver model results into them — the live table is never
dropped, keyed modes reuse the same merge strategies:

```yaml
# interlace.yaml
attach:
  crm: "postgres:host=... dbname=crm"
```

```sql
/* interlace:
  materialise: table
  target: crm.public.accounts
  strategy: merge
  key: id
*/
SELECT id, tier, lifetime_value FROM account_summary
```

Files work the same way — `materialise: file` with `format: parquet | csv | json` and a
`path`. Terminal models are **environment-gated**: by default the side effect fires only from
prod, so a dev apply never writes to a live external table (opt in with
`environments: [dev, prod]`).

## Multi-engine

Models run on **named engines**: DuckDB/DuckLake by default, Postgres natively over ADBC
(`pip install 'interlaced[adbc]'`), Spark (beta, `[spark]` extra), plus alpha adapters for
MotherDuck, Redshift, Snowflake and BigQuery (wired and dialect-correct, not yet run against a
live account), with per-model pinning:

```yaml
engines:
  pg: {type: postgres, database: "${PG_DSN}"}
```

```sql
/* interlace: {engine: pg, strategy: merge, key: id} */
```

Strategies execute *inside* the pinned engine (no DuckDB middleman); cross-engine dependencies
appear as explicit **transfer** lines in the plan and move as Arrow (or a federated `ATTACH`
fast lane when possible). Contract: `docs/architecture/MULTI_ENGINE.md`.

## The daemon

`interlace serve` runs everything in one process:

- the **web UI** at `/ui` (in-package, zero build step) — ten views: overview, lineage canvas
  with column-level tracing, models, plan/apply with SQL diffs, live runs, query console,
  streams, checks, environments, and system — live over SSE;
- the **HTTP API** (Litestar + msgspec, OpenAPI at `/schema/scalar`) with the same surface as
  the CLI: plan/apply, runs, checks, streams, engines, schedules, lineage, query, gc;
- the **scheduler**: cron/interval triggers enqueue onto a **durable run queue** (leases,
  retries, cooperative cancellation — `interlace cancel <id>` or `POST /runs/{id}/cancel`);
- **stream ingestion** and retention.

Scoped API keys (`interlace apikey create ci --scope read`) lock it down; a durable event log
backs `GET /events/stream` (SSE with `Last-Event-ID` replay).

Add `--quack quack:localhost:4213` to serve the warehouse itself over DuckDB's quack protocol —
other processes (CLI runs, ad-hoc DuckDB clients) then share it concurrently by setting
`database: quack:localhost:4213`.

## Architecture in five lines

- The IR is a **sqlglot AST**; the wire format is an **Arrow RecordBatchReader**; strategies
  are AST builders and dialect appears only at `transpile()`.
- Storage defaults to **DuckLake** (Parquet + SQL catalog) opened as DuckDB's primary database.
- Control plane (snapshots, intervals, queue, events, keys) is **SQLite WAL**; Postgres is the
  scale-out swap.
- Streams live in their own durable log; the materializer commits data + watermark in one
  warehouse transaction — exactly-once without distributed coordination.
- No Jinja, no pandas in core, no external orchestrator.

The full design rationale lives in `docs/architecture/architecture.md`.

## Development

Toolchain is pinned with [proto](https://moonrepo.dev/proto), tasks run via
[moon](https://moonrepo.dev/moon), `uv` owns the virtualenv:

```bash
proto install
moon run interlace:sync      # install deps
moon run interlace:test      # 350+ tests
moon run interlace:check     # black + ruff (CI equivalent)
moon run interlace:typecheck # mypy
```

MIT licensed.
