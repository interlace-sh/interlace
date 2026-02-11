"""
Basic transformation models - demonstrating dependencies and ibis operations.

Demonstrates:
- Implicit dependencies via function parameters
- Joins and aggregations using ibis
- View materialization
"""

from interlace import model
import ibis


@model(
    name="user_orders",
    schema="public",
    materialise="table",
    strategy="append",
    description="Join users with orders"
)
def user_orders(users: ibis.Table, orders: ibis.Table) -> ibis.Table:
    """Join users with orders on user_id."""
    return users.join(orders, users.user_id == orders.user_id).select([
        users.user_id,
        users.name,
        users.email,
        orders.order_id,
        orders.amount,
        orders.status,
        orders.order_date
    ])


@model(
    name="user_summary",
    schema="public",
    materialise="table",
    strategy="replace",
    description="Aggregate summary of user order statistics"
)
def user_summary(user_orders: ibis.Table) -> ibis.Table:
    """Aggregate user order statistics."""
    return user_orders.group_by("user_id").aggregate([
        user_orders.count().name("total_orders"),
        user_orders.amount.sum().name("total_amount"),
        user_orders.amount.mean().name("avg_order_amount")
    ])


@model(
    name="recent_orders",
    schema="public",
    materialise="view",
    description="View of recent orders"
)
def recent_orders(orders: ibis.Table) -> ibis.Table:
    """Filter recent orders (view materialization)."""
    return orders.filter(orders.order_date >= "2024-01-15")

