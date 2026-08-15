"""jaffle_shop's `orders` model — the one dbt model that needed real work.

The dbt original uses Jinja for what Jinja is actually for: a `{% set %}` list of
payment methods and a `{% for %}` loop that generates four pivot columns, twice.
There is no regex for that, so it is rewritten rather than converted.

This is the "dynamic model" translation, and the one to prefer: the `{% set %}`
becomes a Python list, the `{% for %}` becomes a generator expression, and the
generated SQL still runs in the engine where the aggregation belongs. Model files
are imported at project load, so registering a ModelDef *is* declaring a model.

The alternative — a `@model` function that pivots in PyArrow — is in the README.
Both produce identical output; that one moves the aggregation into the interlace
process, which is fine for 113 payment rows and wrong for 25 million.
"""

from interlace import CheckSpec
from interlace.dsl.decorators import REGISTRY, ModelDef

PAYMENT_METHODS = ["credit_card", "coupon", "bank_transfer", "gift_card"]

pivot = ",\n        ".join(
    f"sum(case when payment_method = '{m}' then amount else 0 end) as {m}_amount" for m in PAYMENT_METHODS
)
passthrough = ",\n        ".join(f"order_payments.{m}_amount" for m in PAYMENT_METHODS)

REGISTRY.register_model(
    ModelDef(
        name="orders",
        sql=f"""
    with order_payments as (
        select order_id, {pivot}, sum(amount) as total_amount
        from stg_payments group by order_id
    )
    select stg_orders.order_id, stg_orders.customer_id, stg_orders.order_date,
           stg_orders.status, {passthrough},
           order_payments.total_amount as amount
    from stg_orders
    left join order_payments on stg_orders.order_id = order_payments.order_id
    """,
        # The checks loop too: the four not_nulls on the pivot columns come from the
        # same list that generated them, so a new payment method adds its column and
        # its check together. These ten are jaffle_shop's schema.yml for `orders`.
        checks=(
            CheckSpec(type="unique", columns=("order_id",)),
            CheckSpec(type="not_null", columns=("order_id",)),
            CheckSpec(type="not_null", columns=("customer_id",)),
            CheckSpec(type="not_null", columns=("amount",)),
            *(CheckSpec(type="not_null", columns=(f"{m}_amount",)) for m in PAYMENT_METHODS),
            CheckSpec(
                type="accepted_values",
                columns=("status",),
                params={"values": ["placed", "shipped", "completed", "return_pending", "returned"]},
            ),
            CheckSpec(
                type="relationships",
                columns=("customer_id",),
                params={"to": "customers", "field": "customer_id"},
            ),
        ),
    )
)
