# __PROJECT_NAME__

An interlace project — a small **SQL → Python → SQL** pipeline that runs with no
external data:

```
raw_events (SQL seed, 12 rows)
  └─ enriched_events (Python: adds revenue + is_conversion, over Arrow)
       └─ event_summary (SQL: per-country conversions & revenue, with checks)
```

```bash
interlace plan               # preview changes (prod: unprefixed views)
interlace apply              # build models and promote production
interlace apply --env dev    # or a prefixed dev sandbox (dev__main.*)
```

The Python model's parameter is named after `raw_events`, so the dependency is
inferred — no `depends_on` required. Add `depends_on` only for names a parameter
can't spell (a schema-qualified upstream like `raw.accounts`, or a non-model source).
