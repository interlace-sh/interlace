"""Analytics models — transforms on top of source data."""

import ibis
from interlace import model


@model(name="order_totals", materialise="table")
def order_totals(orders: ibis.Table, inventory: ibis.Table) -> ibis.Table:
    """Join orders with inventory to compute the total cost per order line."""
    joined = orders.join(inventory, orders.product_id == inventory.product_id)
    return joined.select(
        "order_id",
        "product_id",
        "product_name",
        "quantity",
        "price",
        "order_date",
        total=joined.quantity * joined.price,
    )


@model(name="low_stock_alerts", materialise="none")
async def low_stock_alerts(inventory: ibis.Table) -> ibis.Table:
    """Flag products with stock below a threshold. Async model with no materialisation (side-effect only)."""
    threshold = 10
    return inventory.filter(inventory.stock_quantity < threshold)
