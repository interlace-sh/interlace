"""
Transformation models demonstrating ibis operations and dependencies.

Covers:
- Joins
- Aggregations
- Filters
- Window functions
- Complex transformations
"""

from interlace import model
import ibis


@model(
    name="customer_orders",
    schema="public",
    materialise="table",
    strategy="replace",
    description="Join customers with orders"
)
def customer_orders(customers: ibis.Table, orders: ibis.Table) -> ibis.Table:
    """Join customers with orders."""
    return customers.join(orders, customers.customer_id == orders.customer_id).select([
        customers.customer_id,
        customers.name,
        customers.email,
        orders.order_id,
        orders.product_id,
        orders.quantity,
        orders.amount,
        orders.order_date,
        orders.status
    ])


@model(
    name="customer_summary",
    schema="public",
    materialise="table",
    strategy="replace",
    description="Aggregate customer statistics"
)
def customer_summary(customer_orders: ibis.Table) -> ibis.Table:
    """Aggregate customer order statistics."""
    return customer_orders.group_by("customer_id").aggregate([
        customer_orders.count().name("total_orders"),
        customer_orders.amount.sum().name("total_spent"),
        customer_orders.amount.mean().name("avg_order_value"),
        customer_orders.amount.max().name("max_order_value"),
        customer_orders.amount.min().name("min_order_value")
    ])


@model(
    name="product_sales",
    schema="public",
    materialise="table",
    strategy="replace",
    description="Product sales aggregation"
)
def product_sales(customer_orders: ibis.Table, products: ibis.Table) -> ibis.Table:
    """Product sales with product details."""
    # Join the tables
    joined = customer_orders.join(
        products,
        customer_orders.product_id == products.product_id
    )
    # Select all needed columns from the joined table
    # Use the joined table's columns directly (they're already part of the joined relation)
    joined_selected = joined.select([
        products.product_id.name("product_id"),
        products.name.name("name"),
        products.category.name("category"),
        joined.quantity,
        joined.amount,
        joined.order_id
    ])
    # Now group by column names (strings) and aggregate using the selected table
    return joined_selected.group_by([
        "product_id",
        "name",
        "category"
    ]).aggregate([
        joined_selected.quantity.sum().name("total_quantity_sold"),
        joined_selected.amount.sum().name("total_revenue"),
        joined_selected.order_id.count().name("order_count")
    ])


@model(
    name="recent_orders",
    schema="public",
    materialise="view",
    description="View of recent orders"
)
def recent_orders(orders: ibis.Table) -> ibis.Table:
    """View of recent orders (last 30 days)."""
    return orders.filter(orders.order_date >= "2024-01-01")


@model(
    name="high_value_customers",
    schema="public",
    materialise="view",
    description="View of high-value customers"
)
def high_value_customers(customer_summary: ibis.Table) -> ibis.Table:
    """View of customers with total spend > 1000."""
    return customer_summary.filter(customer_summary.total_spent > 1000)

