# interlace

**Python/SQL-first data platform: transformation, orchestration, and durable streaming — one process.**

interlace is an independent, MIT-licensed alternative to dbt/SQLMesh that also replaces the
orchestrator (no Airflow) and the ingestion layer (Cloudflare-Pipelines-style durable streams).
Models are `.sql` files or Python functions; state is versioned snapshots with virtual
environments and a terraform-style plan/apply; everything runs in a single daemon on
DuckDB + DuckLake by default.

> **Status: v2 pre-release.** This branch is a ground-up rebuild (the `interlace` package on
> PyPI is the older 0.x line). APIs may still move. Requires Python 3.12+.

```bash
uv pip install "interlace[service] @ git+https://github.com/interlace-sh/interlace@v2"
```

## Sixty seconds

```bash
interlace init my-project && cd my-project
interlace plan            # terraform-style preview: added / breaking / non-breaking / reuse
interlace apply           # build changed models, run checks, promote the environment
interlace serve           # the daemon: HTTP API + scheduler + stream ingestion, one process
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
  strategy: scd_type_2
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

@model(strategy="merge_by_key", key="order_id", cursor="updated_at")
def orders(cursor, this):
    """Incremental API extract: `cursor` is max(updated_at) already in the
    warehouse (None on first run); `this` is the previous materialisation."""
    rows = fetch_orders(since=cursor)          # your code
    return pyarrow.Table.from_pylist(rows)     # or RecordBatchReader / generator of batches
```

**Strategies:** `full`, `view`, `ephemeral` (CTE-inlined), `merge_by_key` (upsert),
`full_merge` (full-state source applied as a minimal diff), `incremental_by_time`
(windowed, interval-ledger backfill/catchup), `scd_type_2` (history with validity windows).

## Plan / apply

```
$ interlace plan
 Model         Change    Category      Build
 orders        modified  non_breaking  rebuild
 order_stats   modified  non_breaking  reuse      <- output provably identical: not rebuilt
```

- Changes classify **breaking / non-breaking / forward-only**; downstream models whose output
  is provably identical **reuse their existing tables** instead of rebuilding (an improvement
  over model-granular invalidation).
- `apply --forward-only` lets history-keeping models (scd2/merge/incremental) survive a
  definition change: the new logic inherits the existing table and applies going forward.
- **Checks gate promotion**: 10 built-in types (not_null, unique, accepted_values, row_count,
  freshness, expression, relationships, pattern, range, sql) plus `@check` Python functions —
  an error-severity failure blocks before the environment view moves.
- `interlace gc` removes snapshots no environment references (reference-aware: tables shared
  through reuse survive).

## Streaming

Declare a stream; POST to it; rows are durable before the 200, deduplicated by idempotency
key, and materialized exactly-once into `streams.<name>` — where SQL models just read them.
A flush triggers the models that consume the stream.

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
`quarantine` (bad events divert to `<stream>__quarantine`).

## Reverse ETL

Attach external databases and deliver model results into them — the live table is never
dropped, keyed modes reuse the same merge strategies:

```yaml
# interlace.yaml
attach:
  crm: "postgres:host=... dbname=crm"
```

```sql
/* interlace: {export: {to: table, target: crm.public.accounts, mode: merge_by_key, key: id}} */
SELECT id, tier, lifetime_value FROM account_summary
```

File exports (`to: parquet|csv|json`) work the same way.

## The daemon

`interlace serve` runs the HTTP API (Litestar + msgspec, OpenAPI at `/schema/scalar`), the
scheduler (cron/interval triggers → durable run queue), stream ingestion, and retention in one
process. Scoped API keys (`interlace apikey create ci --scope read`) lock it down; a durable
event log backs `GET /events/stream` (SSE with replay).

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

The full design rationale lives in `docs/architecture/v2-design.md`.

## Development

Toolchain is pinned with [proto](https://moonrepo.dev/proto), tasks run via
[moon](https://moonrepo.dev/moon), `uv` owns the virtualenv:

```bash
proto install
moon run interlace:sync      # install deps
moon run interlace:test      # 240+ tests
moon run interlace:check     # black + ruff (CI equivalent)
moon run interlace:typecheck # mypy --strict
```

MIT licensed.
