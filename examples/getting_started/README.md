# getting_started

A minimal interlace project: a three-model DAG built on inline seed data, so it
runs anywhere with no external source.

```
raw_events  →  event_totals  →  top_kind
```

```bash
interlace plan --env dev      # preview what will be built
interlace apply --env dev     # build the models and promote the environment
```

After `apply`, query the results in the warehouse (`.interlace/warehouse.duckdb`)
through the environment views, e.g. `SELECT * FROM dev__main.event_totals`.
