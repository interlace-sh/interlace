# Models

A model is one node in the DAG — a SQL file or a Python function that produces a relation.
Models are discovered from the paths in `model_paths` (default `["models"]`): `*.sql` files
(named by their path relative to the models root, e.g. `models/raw/orders.sql` → model
`raw.orders`) and `*.py` files with `@model`-decorated functions.

## SQL models

A `.sql` file is a model; its filename is the model name. Dependencies are discovered
automatically from the table references in the query (a reference to another model's name
becomes an edge). Config rides in a leading comment block:

```sql
/* interlace:
  materialise: table
  strategy: merge_by_key
  key: order_id
  checks:
    - not_null: order_id
*/
SELECT order_id, customer_id, total FROM raw.orders
```

## Python models

A `@model` function returns Arrow (a `pyarrow.Table`, `RecordBatch`, `RecordBatchReader`, or
an iterable of batches — generators stream with bounded memory). Each parameter names an
upstream model and arrives as a `RelationHandle` (call `.reader()` or `.table()` to get
Arrow); a schema-qualified upstream `raw.orders` is also addressable as `raw_orders`.

```python
from interlace import model

@model(depends_on=("raw.orders",), strategy="merge_by_key", key=("order_id",))
def orders(raw_orders):
    table = raw_orders.table()
    return table  # pyarrow.Table
```

Two parameter names are **reserved** for incremental extraction and never name an upstream:

- **`cursor`** — the max of the model's declared `cursor` column in its *previous*
  materialisation (`None` on first build). Derived from the warehouse, not a side ledger, so
  it can't drift from committed data; a crash before commit just re-extracts the overlap, and
  a keyed strategy makes the re-load idempotent. Requires `@model(cursor="<column>")`.
- **`this`** — a `RelationHandle` over the model's *previous* materialisation (`None` on
  first build), for anti-join backfills against what the model already produced.

Sync functions run in a worker thread; async functions run on the event loop. A Python model
must materialise as a `table` and can't be a sink or use `incremental_by_time` (use `cursor`
+ a keyed strategy instead).

## Materialisations

| `materialise` | Produces | Notes |
|---|---|---|
| `table` (default) | a physical snapshot table | built by the configured `strategy` |
| `view` | a `CREATE OR REPLACE VIEW` | no data; re-evaluated on read |
| `ephemeral` | nothing | the query is inlined (as a CTE) into downstream models; no table, no view, no snapshot |

For `table`, the `strategy` decides *how* the table is written — see
[strategies](strategies.md).

## `@model` / config keys

Every key below is settable in the SQL comment block or as a `@model(...)` argument.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | str | filename / fn name | Model identifier. |
| `materialise` | str | `table` | `table` \| `view` \| `ephemeral`. |
| `strategy` | str | `full` | For `table`: `full` \| `merge_by_key` \| `full_merge` \| `incremental_by_time` \| `scd_type_2`. |
| `key` | str \| list | — | Key column(s) for keyed strategies. |
| `time_column` | str | — | Partition column for `incremental_by_time`. |
| `interval` | str | — | Grain for `incremental_by_time` (e.g. `1d`, `1h`). |
| `backfill` | str | `auto` | First-build window for `incremental_by_time`: `auto` (derive `[min,max]`), `none` (latest grain only), or an ISO date. |
| `cursor` | str | — | Python models: column whose max is injected into the `cursor` param. |
| `dialect` | str | project `default_dialect` | sqlglot dialect the SQL is authored in. |
| `engine` | str | project `default_engine` | Named engine this model builds on (multi-engine). |
| `depends_on` | list | discovered | Explicit dependencies (Python models, or to force an edge). |
| `tags` | list | — | Labels for `--select tag:x`. |
| `owner` | str | — | Surfaced in the catalog / API. |
| `description` | str | — | Free text (metadata; not fingerprinted into data). |
| `columns` | map | — | Output contract `{column: type|null}`; a built table violating it blocks promotion (`SchemaError`). |
| `schedule` | map | — | `{cron: "0 * * * *"}` or `{every: "5m"}` for the scheduler. |
| `checks` | list | — | Data-quality [checks](checks.md). |
| `export` | map | — | Makes the model a [sink](streaming.md#reverse-etl-sinks) (no table/view). |

## Fingerprints and rebuild-skip

Each model gets a **data fingerprint** — a hash of its canonical SQL (or Python source), its
strategy config, and the sorted fingerprints of its upstreams. Any change that could affect
output changes the fingerprint, which changes the physical table name
(`interlace__<schema>.<model>__<fp>`). This is how `plan` knows what to rebuild.

The differ classifies each changed model as `breaking` (data may differ — rebuild),
`additive` (only new columns appeared — rebuild, downstream stays non-breaking), or `clean`
(output provably identical — **not rebuilt**, the new snapshot reuses the previous physical
table and the view repoints). **Column pruning** extends `clean` to semantic upstream
changes: if a change provably touched only certain output columns and a downstream provably
consumes none of them, the downstream is clean too. Both proofs are conservative — any
ambiguity falls back to "rebuild".
