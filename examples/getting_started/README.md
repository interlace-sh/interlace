# getting_started

A minimal interlace project built on inline seed data, so it runs anywhere with
no external source. It shows a multi-level DAG and per-model config (a view).

```
raw_events ─┬─ event_totals ── top_kind
            └─ recent_clicks   (materialise: view)
```

```bash
interlace plan --env dev      # preview what will be built
interlace apply --env dev     # build the models and promote the environment
```

After `apply`, query the results in the warehouse (`.interlace/warehouse.duckdb`)
through the environment views, e.g. `SELECT * FROM dev__main.event_totals`.
