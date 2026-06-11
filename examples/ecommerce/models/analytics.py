"""Analytics models — business logic built on staged data.

Demonstrates: joins, aggregations, export, views.
"""

import ibis
from interlace import model


@model(
    name="customer_orders",
    strategy="replace",
    description="Customers joined with their order history",
    tags=["analytics"],
)
def customer_orders(customers: ibis.Table, stg_orders: ibis.Table) -> ibis.Table:
    """Join customers with completed orders."""
    return customers.join(stg_orders, customers.customer_id == stg_orders.customer_id).select(
        stg_orders.order_id,
        customers.customer_id,
        customers.name,
        customers.email,
        customers.segment,
        stg_orders.product_id,
        stg_orders.quantity,
        stg_orders.order_date,
    )


@model(
    name="order_payments",
    strategy="replace",
    description="Orders enriched with payment details",
    tags=["analytics"],
)
def order_payments(stg_orders: ibis.Table, stg_payments: ibis.Table) -> ibis.Table:
    """Join orders with their payments."""
    return stg_orders.join(stg_payments, stg_orders.order_id == stg_payments.order_id).select(
        stg_orders.order_id,
        stg_orders.customer_id,
        stg_orders.product_id,
        stg_orders.quantity,
        stg_orders.order_date,
        stg_payments.amount,
        stg_payments.payment_type,
        stg_payments.paid_at,
    )


@model(
    name="customer_lifetime_value",
    strategy="replace",
    export={"format": "csv", "path": "output/clv.csv"},
    description="Customer lifetime value, exported to CSV",
    tags=["analytics", "export"],
)
def customer_lifetime_value(customer_orders: ibis.Table, order_payments: ibis.Table) -> ibis.Table:
    """Aggregate spend per customer. Result is also exported as CSV."""
    payments_by_customer = order_payments.group_by("customer_id").agg(
        total_spent=order_payments.amount.sum(),
        order_count=order_payments.order_id.nunique(),
        avg_order_value=order_payments.amount.mean(),
    )
    return customer_orders.select("customer_id", "name", "email", "segment").distinct().join(
        payments_by_customer, "customer_id"
    )


@model(
    name="monthly_revenue",
    materialise="view",
    description="Monthly revenue breakdown as a lightweight view",
    tags=["analytics"],
)
def monthly_revenue(order_payments: ibis.Table) -> ibis.Table:
    """Monthly revenue by payment type. View — recomputed on every query."""
    return order_payments.mutate(
        month=order_payments.order_date.cast("string").substr(0, 7),
    ).group_by(["month", "payment_type"]).agg(
        revenue=order_payments.amount.sum(),
        num_orders=order_payments.order_id.nunique(),
    )
