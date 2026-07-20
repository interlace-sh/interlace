# platform tour

Every pillar in one small project: a durable stream, an SCD2 dimension, a
Python model over Arrow, promotion-gating checks, and a reverse-ETL sink.

```bash
interlace serve --path .          # daemon: API + scheduler + stream ingestion
# in another shell:
curl -X POST localhost:8000/streams/orders \
  -H 'content-type: application/json' \
  -d '{"order_id": "o1", "customer_id": 1, "total": 49.5}'

curl -X POST localhost:8000/apply -H 'content-type: application/json' -d '{}'
```

Or without the daemon (the stream table just stays empty):

```bash
interlace apply --env dev --path .
interlace list --path .
interlace lineage customer_value --columns --path .
```

Things to try:

- Re-POST the same `order_id` — it deduplicates (`"deduplicated": 1`).
- Change a tier in `raw_customers.sql`, `interlace apply` — `dim_customers`
  closes the old version and opens a new one; then try the same edit with
  `interlace apply --forward-only` to keep history across a *definition* change.
- `duckdb crm.duckdb "SELECT * FROM customer_scores"` — the sink's upserts.
- `interlace gc --dry-run` after a few changes — superseded snapshots to reap.
