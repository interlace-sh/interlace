"""Analytics models -- business intelligence built on the star schema.

Demonstrates: aggregations on fact tables, querying SCD Type 2 history,
and materialisation as views.
"""

import ibis
from interlace import model


@model(
    name="customer_lifetime_value",
    strategy="replace",
    description="Customer lifetime value aggregated from orders",
    tags=["analytics"],
)
def customer_lifetime_value(
    fact_orders: ibis.Table,
    dim_customer: ibis.Table,
) -> ibis.Table:
    """Aggregate CLV per customer from fact_orders joined with dim_customer.

    Uses current customer records and computes total spend, order count,
    average order value, and first/last order dates.
    """
    current_customers = dim_customer.filter(dim_customer.is_current == True)  # noqa: E712

    order_metrics = fact_orders.group_by("customer_id").agg(
        total_spent=fact_orders.total_amount.sum(),
        order_count=fact_orders.order_id.nunique(),
        avg_order_value=fact_orders.total_amount.mean(),
        first_order=fact_orders.order_date.min(),
        last_order=fact_orders.order_date.max(),
    )

    return (
        current_customers
        .select("customer_id", "name", "email", "segment")
        .join(order_metrics, "customer_id")
    )


@model(
    name="product_price_history",
    strategy="replace",
    description="Product price changes over time from SCD Type 2 history",
    tags=["analytics"],
)
def product_price_history(dim_product: ibis.Table) -> ibis.Table:
    """Query the dim_product SCD2 dimension to show price changes over time.

    Each row represents a historical version of a product, including
    valid_from and valid_to timestamps showing when each price was active.
    This is the key benefit of SCD Type 2 -- full auditability.
    """
    return dim_product.select(
        "product_id",
        "name",
        "category",
        "price",
        "valid_from",
        "valid_to",
        "is_current",
    ).order_by(["product_id", "valid_from"])


@model(
    name="monthly_revenue",
    materialise="view",
    description="Monthly revenue breakdown as a lightweight view",
    tags=["analytics"],
)
def monthly_revenue(fact_orders: ibis.Table) -> ibis.Table:
    """Monthly revenue by product category. Materialised as a view --
    recomputed on every query against the warehouse.
    """
    return (
        fact_orders
        .mutate(month=fact_orders.order_date.cast("string").substr(0, 7))
        .group_by(["month", "product_category"])
        .agg(
            revenue=fact_orders.total_amount.sum(),
            num_orders=fact_orders.order_id.nunique(),
            total_quantity=fact_orders.quantity.sum(),
        )
    )
