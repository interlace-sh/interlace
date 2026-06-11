"""Analytics — join sales with menu items to compute daily revenue."""

import ibis
from interlace import model


@model(name="daily_revenue", materialise="view")
def daily_revenue(menu_items: ibis.Table, sales: ibis.Table) -> ibis.Table:
    """Daily revenue by category. Materialised as a view since it's a lightweight query."""
    joined = sales.join(menu_items, sales.item_id == menu_items.item_id)
    return joined.group_by(["sale_date", "category"]).agg(
        total_items=joined.quantity.sum(),
        revenue=(joined.quantity * joined.price).sum(),
    )
