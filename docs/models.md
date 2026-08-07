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
  strategy: merge
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

@model(depends_on=("raw.orders",), strategy="merge", key=("order_id",))
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
must materialise as `virtual` and can't be terminal (`table`/`file`) or use
`incremental_by_time` (use `cursor` + a keyed strategy instead). To deliver a Python model's
output to an external table/file, write a SQL `materialise: table`/`file` model over it.

## Dynamic / programmatic models

Model `.py` files are **imported and their top-level code runs** every time the project loads
(discovery executes each module), and `@model` registers a model the instant it runs. So a
plain Python loop *is* the mechanism for generating many models from data — e.g. the same
logic per tenant, region, or source, each with its own filter. There is no separate templating
DSL; it's just Python.

**Per-tenant SQL models** — register a `ModelDef` directly for each item in a list:

```python
# models/per_tenant.py
from interlace.dsl.decorators import REGISTRY, ModelDef

def get_tenants():                      # any Python: a DB query, a file, an env var…
    return ["acme", "globex"]

for tenant in get_tenants():
    REGISTRY.register_model(ModelDef(
        name=f"orders_{tenant}",
        sql=f"SELECT order_id, amount FROM raw WHERE tenant_id = '{tenant}'",
        strategy="merge", key=("order_id",),
    ))
```

This produces one snapshot table and environment view per tenant (`orders_acme`,
`orders_globex`, …), each with an independent fingerprint, plan/apply, checks, and incremental
ledger — full isolation between tenants.

**Per-tenant Python models** — use a *factory* so each closure captures its own value (the one
thing to get right):

```python
from interlace import model
import pyarrow.compute as pc

def make(tenant):
    @model(name=f"orders_{tenant}", depends_on=("raw",), strategy="merge", key=("order_id",))
    def _orders(raw, tenant=tenant):          # bind tenant HERE, not via the loop variable
        t = raw.table()
        return t.filter(pc.equal(t["tenant_id"], tenant))
    return _orders

for t in get_tenants():
    make(t)
```

Things to know:

- **Names must be unique** — `register_model` raises on a duplicate, so put the tenant in the
  name.
- **Closure late-binding** — the classic Python trap; bind the loop variable via a factory or
  a default argument (as above), or every generated function filters on the *last* value.
- **`depends_on` for Python models** — a function's parameters must each be a declared
  dependency (SQL models auto-discover dependencies from their table references; Python models
  don't).
- **The generator runs on every command** — `get_tenants()` is called each time `interlace`
  loads the project (plan, apply, models, serve). Keep it fast and deterministic; if it queries
  a database, every CLI call pays that cost. **`interlace serve` compiles once at startup**, so
  a tenant added while the daemon is running only appears after it re-compiles/restarts.
- **Quote interpolated values** — for a trusted internal list, string interpolation into SQL is
  fine; for untrusted input, quote via sqlglot or parameterise.

If instead you want a *single* model carrying a `tenant` column (no per-tenant tables), that's
just an ordinary model — but for the same logic applied per tenant with isolation, the loop
above is the right shape.

## Materialisations

`materialise` is the destination/ownership plane. **Owned** planes (`virtual`/`view`/`ephemeral`)
produce an interlace-managed snapshot read through an environment view; **terminal** planes
(`table`/`file`) deliver to a destination interlace doesn't own (reverse ETL), with no view,
environment-gated, additively evolved but never dropped.

| `materialise` | Plane | Produces | Notes |
|---|---|---|---|
| `virtual` (default) | owned | a physical snapshot table | built by the configured `strategy` |
| `view` | owned | a `CREATE OR REPLACE VIEW` | no data; re-evaluated on read |
| `ephemeral` | owned | nothing | the query is inlined (as a CTE) into downstream models; no table/view/snapshot |
| `table` | terminal | rows in an external table (`target:`) | reverse ETL into an attached DB; `strategy` picks the delivery |
| `file` | terminal | a file (`path:` + `format:`) | overwrite via `COPY`; `parquet`/`csv`/`json` |

For `virtual` (and an external `table`), the `strategy` decides *how* the table is written — see
[strategies](strategies.md).

## `@model` / config keys

Every key below is settable in the SQL comment block or as a `@model(...)` argument.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | str | filename / fn name | Model identifier. |
| `materialise` | str | `virtual` | `virtual` \| `view` \| `ephemeral` (interlace-owned) \| `table` \| `file` (terminal). |
| `strategy` | str | `full` | For `virtual`/`table`: `full` \| `merge` \| `full_merge` \| `hash_merge` \| `incremental_by_time` \| `scd`; `append` is `table`-only. `file` is overwrite (`full`). |
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
| `target` | str | — | `materialise: table`: external `<alias>.<schema>.<table>` to deliver into. |
| `path` | str | — | `materialise: file`: output path. |
| `format` | str | — | `materialise: file`: `parquet` \| `csv` \| `json`. |
| `environments` | list | `[prod]` | Terminal models: which environments actually deliver (the side-effect gate). |

## Fingerprints and rebuild-skip

Each model gets a **data fingerprint** — a hash of its canonical SQL (or Python source), its
strategy config, and the sorted fingerprints of its upstreams. Any change that could affect
output changes the fingerprint, which changes the physical table name
(`interlace__<schema>.<base>__<fp>`, where `<base>` is the schema-stripped model name). This is
how `plan` knows what to rebuild.

The differ classifies each changed model internally as **breaking** (data may differ — rebuild),
**additive** (only new columns appeared — rebuild, downstream stays non-breaking), or **clean**
(output provably identical — **not rebuilt**, the new snapshot reuses the previous physical table
and the view repoints). Note these are the differ's internal labels: a change's `category` on the
wire and in `interlace plan` is only `breaking` / `non_breaking` / `forward_only` (additive and
clean both surface as `non_breaking`; the rebuild-vs-reuse distinction shows in the plan's Build
column). **Column pruning** extends `clean` to semantic upstream changes: if a change provably
touched only certain output columns and a downstream provably consumes none of them, the
downstream is clean too. Both proofs are conservative — any ambiguity falls back to "rebuild".
