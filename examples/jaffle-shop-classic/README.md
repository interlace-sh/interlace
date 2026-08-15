# jaffle_shop

[dbt's own demo project](https://github.com/dbt-labs/jaffle-shop-classic), converted to
interlace. Eight models and twenty checks — which are jaffle_shop's twenty dbt tests, one
for one.

```
raw_customers ── stg_customers ─┐
raw_orders ───── stg_orders ────┼─ customers
raw_payments ─── stg_payments ──┴─ orders
```

```bash
interlace apply --env prod
```

```
Checks: 20/20 passed
Built 8 model(s); promoted 8 to 'prod'.
```

The walkthrough, with the friction written down, is in
[Migrating jaffle_shop](https://interlace.sh/blog/migrating-jaffle-shop). What follows is
what changed and why.

## Seeds became models

dbt has a separate concept and a separate command for seeds: CSVs in `seeds/`, loaded by
`dbt seed`. interlace has no seed concept, because a seed is a model with no upstreams:

```sql
-- models/raw_customers.sql
SELECT * FROM read_csv_auto('seeds/raw_customers.csv')
```

Three files, one line each. The CSV now participates in the DAG, gets a fingerprint, and
rebuilds downstream models when it changes.

## Four of five models: a two-line regex

The staging models and `customers` are the ordinary case. Two transformations cover them —
strip Jinja comments, and turn `{{ ref('x') }}` into `x`:

```python
sql = re.sub(r'\{#-?.*?-?#\}', '', sql, flags=re.S)
sql = re.sub(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", r'\1', sql)
```

The CTEs, the joins and the column lists are dbt's, untouched — interlace reads the
dependency out of the `FROM` clause instead of asking you to declare it.

One thing the regex does not cover: **a subdirectory becomes part of the model's name.**
`models/staging/stg_customers.sql` is `staging.stg_customers`, so `customers.sql`'s
`from stg_customers` no longer resolves. Either flatten the directory or pin the name in the
model's config block, which is what this project does:

```sql
/*
interlace:
  name: stg_customers
  ...
*/
```

## Tests became checks

All four dbt test types map one-to-one — `unique`, `not_null`, `accepted_values`,
`relationships`. The difference is location: dbt keeps them in a separate `schema.yml`,
interlace puts them in the model's own config block, so the model and its contract are one
file. They also **gate promotion** by default, where `dbt test` is a separate command.

The `relationships` check on `orders.customer_id` reads `customers`, which is a sibling in
the DAG rather than an upstream. interlace schedules the check after `customers` builds; you
do not have to order it yourself.

## `orders` is the real work

`orders.sql` is the one model Jinja was doing real work in: a `{% set %}` list of payment
methods and a `{% for %}` loop generating four pivot columns, twice. There is no regex for
that.

[`models/orders.py`](models/orders.py) is the version to prefer — the `{% set %}` becomes a
Python list, the `{% for %}` becomes a generator expression, and the generated SQL still runs
in the engine. The checks loop over the same list, so adding a payment method adds its column
and its `not_null` together.

The alternative is a `@model` function that pivots in PyArrow. Both were built against this
data and produce identical output, row for row:

```python
# models/orders.py — alternative to the dynamic model this project ships
import pyarrow as pa
import pyarrow.compute as pc
from interlace import model

PAYMENT_METHODS = ["credit_card", "coupon", "bank_transfer", "gift_card"]


@model()
def orders(stg_orders, stg_payments):
    payments = stg_payments.table()

    # The Jinja {% for %} pivot, as a Python loop over Arrow columns.
    cols = {"order_id": payments["order_id"]}
    for m in PAYMENT_METHODS:
        is_m = pc.equal(payments["payment_method"], m)
        cols[f"{m}_amount"] = pc.if_else(is_m, payments["amount"], 0.0)
    cols["amount"] = payments["amount"]

    per_method = (
        pa.table(cols)
        .group_by("order_id")
        .aggregate([(f"{m}_amount", "sum") for m in PAYMENT_METHODS] + [("amount", "sum")])
    )
    per_method = per_method.rename_columns(["order_id"] + [f"{m}_amount" for m in PAYMENT_METHODS] + ["amount"])
    return stg_orders.table().join(per_method, keys="order_id", join_type="left outer")
```

Reach for that one when the logic is heading somewhere SQL cannot follow — a model call, a
rate-limited API, a library with no SQL equivalent. The cost is that the aggregation happens
in the interlace process rather than the engine: irrelevant for 113 payment rows, wrong for
25 million.

Either way the pivot holds:

```bash
interlace query "SELECT count(*) AS mismatched_rows FROM orders
                 WHERE credit_card_amount + coupon_amount + bank_transfer_amount
                     + gift_card_amount <> amount"
# 0
```
