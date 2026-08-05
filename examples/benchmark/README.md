# benchmark

A project that makes the engine sweat: **25 million synthetic events** generated
in-engine (nothing to download, fully deterministic), pushed through a fan-out
DAG that exercises the pieces that matter for throughput —

```
events (25M rows) ── enriched (ephemeral: inlined into every consumer)
                      ├─ by_user ──── user_ltv (Python, Arrow batches, merge)
                      ├─ by_product ─ top_products (view)
                      ├─ by_device
                      └─ by_day
events ───────────── daily_revenue (incremental_by_time, 1d grain)
                      └─ revenue_report (parquet sink → out/)
```

The four `by_*` branches share no edges, so `apply` builds them **concurrently**
— watch the progress rows overlap. `enriched` is ephemeral, so each branch scans
the full 25M rows through the inlined CTE: the fan-out does real, repeated work.

## Run it

```bash
cd examples/benchmark

time interlace apply                # full build: 25M rows through the whole DAG
interlace models                    # the DAG at a glance

# incremental windows: 30 one-day tasks, then a ledger-driven no-op, then a rewrite
time interlace run     --select daily_revenue --start 2026-06-01 --end 2026-07-01
time interlace run     --select daily_revenue --start 2026-06-01 --end 2026-07-01   # 0 tasks
time interlace restate --select daily_revenue --start 2026-06-08 --end 2026-06-15   # one week, rewritten

# change detection at scale: touch ONE branch, only it rebuilds
sed -i 's/avg_ticket/avg_ticket, min(amount) AS min_ticket/' models/by_device.sql
time interlace apply --force
```

Reference numbers (25M rows, laptop-class 8-core, DuckLake warehouse):

| flow                                   | wall  | cpu    |
| -------------------------------------- | ----- | ------ |
| full build (9 models)                  | ~4.3s | ~12.8s |
| 30-day incremental backfill            | ~1.3s | ~12.9s |
| same window again (ledger catchup)     | ~0.3s | —      |
| restate one week                       | ~0.6s | —      |
| touch one branch (`apply`)             | ~0.4s | —      |

wall ≪ cpu is the point: independent DAG branches build in parallel
(`apply(parallelism=4)`), and DuckDB parallelises inside each query.

## Turn it up

- Scale: raise `range(25000000)` in `models/events.sql` — 100M is ~10 GB of
  scan work per branch; the DAG shape doesn't change.
- Concurrency: add more independent branches over `enriched` and watch wall
  time hold while CPU climbs.
- Ingestion: run `interlace serve` and fire batches at a stream endpoint (see
  `../platform_tour`) while the DAG builds — publishes only append to the
  durable log; a flusher task micro-batches them into the warehouse.

## What it covers (beyond load)

- `materialise: ephemeral` (CTE inlining), views, contracts-by-checks
- `incremental_by_time` + the interval ledger: catchup vs `restate`
- a Python model streaming Arrow `RecordBatch`es with bounded memory,
  upserted via `merge`
- a Parquet file materialisation (`materialise: file, format: parquet`)
- `row_count` / `not_null` checks gating promotion at volume
