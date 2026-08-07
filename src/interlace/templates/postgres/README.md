# __PROJECT_NAME__ — Postgres → DuckDB with interlace

Incrementally pulls a table from a source Postgres into your warehouse, then models
it with plain SQL:

```
orders (Python source: psycopg pull, incremental, merge-by-id)
  └─ orders_by_status (SQL: counts & revenue, with checks)
```

## Run it

A `docker-compose.yml` is included that stands up a **seeded** source Postgres, so
you can run the whole thing with no external setup:

```bash
pip install "interlaced[postgres]"     # the psycopg driver
docker compose up -d                   # source Postgres on :5456, seeded from init/seed.sql
interlace apply                        # pull + build + promote
interlace query "SELECT * FROM orders_by_status"
docker compose down -v                 # stop and wipe when done
```

Point it at your own database instead by setting `SOURCE_PG_DSN`.

## Incremental, idempotent

`interlace apply` builds a model when its *code* changes. To pull fresh data on
unchanged code, use **`interlace run`** (or put the model on a schedule) — it
re-executes, resuming from the newest `updated_at` already loaded
(`WHERE updated_at > cursor`) and **upserting by `id`**, so you only read what
changed and never get duplicates. Try it — touch a row and re-run:

```bash
docker compose exec source-db psql -U interlace -d shop \
  -c "UPDATE orders SET status='paid', updated_at=now() WHERE id=5;"
interlace run         # only order 5 is re-pulled
```

## Two ways to pull from Postgres

This template uses a **psycopg** model (`models/orders.py`) — explicit, and the
surest fit for row-level incremental logic. The alternative is fully declarative:
register the source Postgres as a named engine and let interlace's Arrow-native
transfer move it, no Python —

```yaml
# interlace.yaml
engines:
  source_pg:
    type: postgres
    dsn: env:SOURCE_PG_DSN
```

```sql
-- models/orders.sql  (engine: source_pg)
/* interlace: engine: source_pg, materialise: table */
SELECT id, customer, amount, status, updated_at FROM orders
```

Reach for the declarative form when a straight `SELECT` is enough; keep the psycopg
model when the extraction needs real code.
