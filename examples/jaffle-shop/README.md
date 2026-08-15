# jaffle_shop

[dbt's current demo project](https://github.com/dbt-labs/jaffle-shop), converted to
interlace. Nineteen models and twenty-seven checks — which are jaffle_shop's twenty-seven
dbt data tests, one for one.

```
raw_customers ── stg_customers ──────────────────────┐
raw_stores ───── stg_locations ── locations          │
raw_products ─── stg_products ──┐                    │
raw_supplies ─── stg_supplies ──┼─ order_items ── orders ── customers
raw_items ────── stg_order_items ┘                   │
raw_orders ───── stg_orders ─────────────────────────┘
```

```bash
interlace apply --env prod
```

```
Checks: 27/27 passed
Built 19 model(s); promoted 19 to 'prod'.
```

Needs the network: the raw tables are read straight from dbt's repo (below). The whole
thing builds in about a second and a half.

> The older [`jaffle-shop-classic`](../jaffle-shop-classic/) is also here — five models,
> one Jinja loop, and a walkthrough of the mechanical parts of a migration. This project
> is the interesting one: sources, macros, package macros, and a semantic layer.

## Sources instead of seeds

dbt ships the data as seed CSVs and then **disables them by default**
(`load_source_data: false`), because the real project expects the data to already be in
the warehouse behind `{{ source('ecom', ...) }}`. Two concepts — seeds and sources — for
the same idea: a table this project reads but does not build.

interlace has neither, because a source is a model with no upstreams. DuckDB reads a CSV
over HTTP, so nothing stands between dbt's repo and the DAG:

```sql
SELECT * FROM read_csv_auto('https://raw.githubusercontent.com/.../raw_customers.csv')
```

[`models/raw/sources.py`](models/raw/sources.py) registers all six from a loop, because
the only thing that varies is the file name. Each one is an ordinary SQL model with a
fingerprint, so a change upstream rebuilds what depends on it.

## What the regexes covered

Same as the classic project, plus the source form:

```python
sql = re.sub(r"\{\{\s*source\(\s*'[^']+'\s*,\s*'([^']+)'\s*\)\s*\}\}", r'\1', sql)   # source('ecom','raw_x') -> raw_x
sql = re.sub(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}", r'\1', sql)                    # ref('x') -> x
sql = re.sub(r"\{\{\s*dbt\.date_trunc\('(\w+)','(\w+)'\)\s*\}\}", r"date_trunc('\1', \2)", sql)
sql = re.sub(r"\{\{\s*cents_to_dollars\('(\w+)'\)\s*\}\}", r'cents_to_dollars(\1)', sql)   # a macro here too
```

`dbt.date_trunc` is a cross-database macro — a templating layer that exists to paper over
dialect differences. interlace transpiles one dialect to another with sqlglot instead, so
the macro has nothing to do and the model just writes `date_trunc`.

## Three things the regexes did not cover

**A subdirectory becomes part of the model's name.** `models/staging/stg_customers.sql`
would be `staging.stg_customers`, and every bare ref in the marts would stop resolving.
This project lists the leaf directories in `interlace.yaml` instead, which keeps dbt's
layout *and* dbt's names:

```yaml
model_paths: [models/raw, models/staging, models/marts]
```

The paths must not overlap — listing both `models` and `models/staging` registers the
staging models twice. (The classic example pins `name:` per model instead, because its
models are split between `models/` and `models/staging/`, which cannot be expressed as
non-overlapping paths.)

**A CTE may not take the name of the model it selects from.** dbt's marts open with
`with orders as (select * from {{ ref('orders') }})`, which is safe because `ref()`
renders a fully-qualified relation. Here the reference is the model's bare name, so the
CTE shadows it and DuckDB reports a circular CTE. Two models needed a renamed CTE;
nothing else changed.

**A check cannot point downstream.** dbt tests `order_items.order_id` against `orders` —
but `orders` is built *from* `order_items`, and interlace runs a model's checks when that
model builds, so the check would wait for a model that is waiting for it. It points at
`stg_orders` instead: `orders` is `stg_orders` left-joined to a summary of these very
rows, so it is the same set of `order_id`s and the same check. dbt does not hit this
because `dbt test` is a separate pass over everything.

Sibling references are fine — `stg_order_items` is checked against `stg_orders` and
simply waits for it.

## Macros

dbt writes `cents_to_dollars` five times — `default__`, `postgres__`, `bigquery__`, `fabric__`
and the dispatcher — because Jinja renders *text*, and the text has to differ per engine.

[`macros/jaffle.sql`](macros/jaffle.sql) writes it once:

```sql
CREATE MACRO cents_to_dollars(amount) AS (amount / 100)::numeric(16, 2);
```

Models call it as an ordinary function, and the call is expanded into the model's AST at
compile time — so it is transpiled with everything else:

| engine | rendered |
| --- | --- |
| DuckDB | `CAST((subtotal / 100) AS DECIMAL(16, 2))` |
| Postgres | `CAST((CAST(subtotal AS DOUBLE PRECISION) / NULLIF(100, 0)) AS DECIMAL(16, 2))` |
| BigQuery | `CAST((subtotal / NULLIF(100, 0)) AS NUMERIC)` |

Postgres's variant is the one dbt hand-writes; it comes out of the same one line here.

`dbt_utils.generate_surrogate_key` is the other case — a *package* macro, with no package to
install it from. It is in the same file, as the twelve tokens the package ships, and
`stg_supplies` calls it like any other function.

Because expansion happens before fingerprinting, editing either macro re-plans every model
that calls it:

```
 stg_orders     modified   breaking   rebuild
 stg_products   modified   breaking   rebuild
 stg_supplies   modified   breaking   rebuild
 ... and everything downstream
```

A macro created in the warehouse would not do that — the callers' SQL would be unchanged, so
nothing would rebuild. The trade is that these macros are a build-time abstraction: they do
not exist in the warehouse for ad-hoc queries.

## Tests

All twenty-seven convert. The four types the classic project used map by name, and dbt's
`+materialized: view` on staging becomes `materialise: view`. The one worth noting:

| dbt | interlace |
| --- | --- |
| `dbt_utils.expression_is_true` | `expression` |

That is a **package** test in dbt and a built-in here, so the four row-level invariants
(`order_total - tax_paid = subtotal` and friends) came across without the package:

```yaml
- expression: {expression: order_total - tax_paid = subtotal}
```

## What did not convert

- **Semantic models, metrics and saved queries** (MetricFlow) — no equivalent. They are
  the bulk of the `.yml` files in dbt's project and none of it came across.
  `metricflow_time_spine` converts to an ordinary date spine, and then nothing reads it.
- **Unit tests** (`unit_tests:` in `stg_locations.yml`) — no equivalent. A Python model is
  a plain function you can call in a test; a SQL model is not.
- **`dbt-audit-helper`** — a package of macros for comparing two relations. No equivalent.
