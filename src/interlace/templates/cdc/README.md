# __PROJECT_NAME__ — Postgres CDC

A Docker Postgres table, followed through a logical replication slot, landed in
a durable stream, and folded into a current-state replica:

```
Postgres public.orders
  └─ logical slot interlace_orders (pgoutput)
       └─ @stream orders  →  streams.orders     changelog, one row per change
            └─ orders                            replica (full_merge on id)
                 └─ orders_by_status             rollup
```

`push.py` is the writer. It stands in for the application that inserts, updates,
and deletes in the source database.

## Run it

```bash
pip install "interlaced[postgres,service]"
docker compose up -d --wait          # Postgres on :5457, wal_level=logical
interlace serve                      # reads the slot; scheduler rebuilds the replica
```

The seed rows are written **after** the slot is created, so they are already
waiting in the slot. Within a few seconds of `serve` starting they show up as
the replica. While the daemon is running it holds the warehouse file — query
through it:

```bash
curl -s -X POST localhost:8000/query -H 'content-type: application/json' \
  -d '{"sql":"SELECT id, customer, status, amount FROM orders ORDER BY id"}'
```

The same rows are in the web UI at <http://localhost:8000/ui>. The changelog
itself is `streams.orders` (`_change` is `insert`, `update`, or `delete`).

Then change the source and watch the replica follow — order 6 appears, order 5
becomes paid, order 3 disappears:

```bash
python push.py
```

`orders_by_status` rebuilds with the replica. Stop when you are done:

```bash
docker compose down -v
```

## What it shows

- **The slot is yours.** `init/seed.sql` creates the publication and the
  `pgoutput` slot. Interlace only reads them. A delete carries the primary key
  because that key is the replica identity.
- **Changes land in a stream.** Every value arrives as text (that is how
  `pgoutput` encodes a tuple), so the `@stream` schema is strings. The daemon
  appends one event per change and advances the stored LSN only after that
  event is flushed to `streams.orders`.
- **The replica is a model.** `orders` keeps the latest change per `id` and
  drops a row whose latest change is `delete`. `strategy: full_merge` treats
  that query as the whole table, so a vanished id is deleted. A flush re-runs
  `orders` and `orders_by_status` — leave the scheduler on (the default).

Point the `shop` connection at your own database by editing the `dsn` in
`interlace.yaml`. `push.py` reads `SOURCE_PG_DSN` for the same URL.
