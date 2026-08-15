# benchmark

A project that makes the engine sweat: **25 million synthetic events** generated
in-engine (nothing to download, fully deterministic), pushed through a fan-out
DAG that exercises the pieces that matter for throughput —

```
events (25M rows) ── enriched (ephemeral: inlined into every consumer)
                      ├─ by_user ────┬─ user_ltv       (Python, Arrow batches, merge)
                      │              └─ user_history   (scd — Type 2 history)
                      ├─ by_product ─┬─ top_products   (view)
                      │              └─ product_catalog (full_merge, composite key)
                      ├─ by_device
                      └─ by_day
events ───────────── daily_revenue (incremental, 1d grain)
                      ├─ revenue_report (parquet file → out/)
                      └─ daily_feed     (append → external serving.duckdb, reverse ETL)
```

The `by_*` branches share no edges, so `apply` builds them **concurrently** — watch
the progress rows overlap. `enriched` is ephemeral, so each branch scans the full
25M rows through the inlined CTE: the fan-out does real, repeated work. Between them
the models exercise **every strategy** — `replace`, `incremental`, `merge`,
`full_merge`, `scd`, `append` — across `virtual` / `view` / `file` / external `table`.

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

Reference numbers (25M rows, laptop-class 8-core, DuckLake warehouse — this project ships on
the default plain-DuckDB warehouse, so add `database: ducklake:.interlace/warehouse.ducklake`
to `interlace.yaml` to reproduce them exactly):

| flow                                   | wall  | cpu    |
| -------------------------------------- | ----- | ------ |
| full build (12 models, every strategy) | ~5.0s | ~14s   |
| 30-day incremental backfill            | ~1.3s | ~12.9s |
| same window again (ledger catchup)     | ~0.3s | —      |
| restate one week                       | ~0.6s | —      |
| touch one branch (`apply`)             | ~0.4s | —      |

wall ≪ cpu is the point: independent DAG branches build in parallel
(`apply(parallelism=4)`), and DuckDB parallelises inside each query.

## Compared with dbt

Different project, same question. The numbers above are this DAG; the ones below are
[`../jaffle-shop`](../jaffle-shop/) — dbt's own demo project, converted — run against **both**
tools on one machine, so the comparison is like-for-like on identical models and identical data.

The data is `jafgen 8`: **896 MB of CSV**, 3,470,845 orders, 5,299,744 order items. Both write a
plain DuckDB file; dbt runs `threads: 4`, interlace `parallelism: 4`.

| command                        | work                                            | engine | wall   | peak RSS |
| ------------------------------ | ----------------------------------------------- | ------ | ------ | -------- |
| `dbt seed`                     | 896 MB CSV → tables (one-off)                   | 69.7s  | 73.8s  | 9.2 GB   |
| `dbt run`                      | 13 models, from loaded tables                   | 6.3s   | 9.0s   | 3.3 GB   |
| `dbt build`                    | 13 models + 27 data tests + 3 unit + 3 saved    | 7.1s   | 9.9s   | 3.3 GB   |
| `interlace run --select <13>`  | the same 13 models + 27 checks                  | 6.6s   | 7.5s   | 3.3 GB   |
| `interlace run`                | 19 models + 27 checks (re-reads the CSVs)       | 9.2s   | 10.2s  | 3.6 GB   |
| `interlace apply`              | first build, from CSV                           | —      | 10.5s  | 3.5 GB   |

**The engine time is a tie, and that is the expected result.** 6.3s against 6.6s on the same
thirteen models — and interlace's figure includes 27 checks that `dbt run` does not run. Both
hand the same SQL to the same DuckDB, so at this size neither tool is the bottleneck and neither
should claim to be. Outputs were compared table by table and agree to the cent
(`sum(order_total)` = 40,039,903.55 on both).

What differs is everything either tool does *around* the engine:

- **Startup.** `dbt parse` is 1.85s before any SQL runs; `interlace plan` on an unchanged
  project is 0.30s. At 16 MB — the dataset the example actually ships with — that overhead *is*
  the runtime, and the same comparison is 3.05s vs 0.77s. It stops mattering as data grows.
- **Loading the CSVs.** `dbt seed` takes 69.7s and 9.2 GB of RSS for 896 MB, because it round-
  trips the file through Python. DuckDB reads those same six files natively in **3.2s at 2.2 GB**
  — a 22× gap against the engine underneath it. interlace has no seed step: a CSV is a model, so
  `read_csv_auto` runs in the engine like any other query. That is why the from-scratch pipeline
  is ~84s (`dbt seed` + `dbt build`) against 10.5s (`interlace apply`).

  In fairness to dbt: seeds are meant for small reference data, and dbt's own project ships with
  `load_source_data: false` for exactly this reason. Reaching for `dbt seed` at 896 MB is using
  it against its documented intent — but a CSV of that size is an ordinary thing to want to read,
  and that is the comparison being drawn.

Caveats, because this is a comparison and not a benchmark: two runs each, one machine, warm page
cache. `dbt build` also runs 3 unit tests and 3 saved queries that have no interlace equivalent
(its 27 data tests are the like-for-like part). And interlace's warehouse ends up 1.6 GB against
dbt's 1.2 GB, because superseded snapshot tables are kept until `gc` — that is the rollback
mechanism, not per-row overhead.

Reproduce it with `jafgen 8`, dbt's generator, pointed at both projects.

## Turn it up

- Scale: raise `range(25000000)` in `models/events.sql` — 100M is ~10 GB of
  scan work per branch; the DAG shape doesn't change.
- Concurrency: add more independent branches over `enriched` and watch wall
  time hold while CPU climbs.
- Ingestion: run `interlace serve` and fire batches at a stream endpoint (see
  `../platform_tour`) while the DAG builds — publishes only append to the
  durable log; a flusher task micro-batches them into the warehouse.

## What it covers (beyond load)

- **every strategy** in one DAG: `replace`, `incremental` (+ interval
  ledger: catchup vs `restate`), `merge`, `full_merge` (composite key), `scd`
  (Type 2 history), `append`
- every materialisation: `virtual`, `ephemeral` (CTE inlining), `view`,
  `file` (Parquet), and an external `table` (reverse ETL into `serving.duckdb`)
- a Python model streaming Arrow `RecordBatch`es with bounded memory,
  upserted via `merge`
- `row_count` / `not_null` checks gating promotion at volume
