"""Staging models — clean and deduplicate before analytics.

Demonstrates: ephemeral materialisation (never persisted to disk).
"""

import ibis
from interlace import model


@model(name="stg_orders", materialise="ephemeral", tags=["staging"])
def stg_orders(orders: ibis.Table) -> ibis.Table:
    """Filter out cancelled orders. Ephemeral — exists only during execution."""
    return orders.filter(orders.status == "completed")


@model(name="stg_payments", materialise="ephemeral", tags=["staging"])
def stg_payments(payments: ibis.Table) -> ibis.Table:
    """Deduplicate payments by payment_id, keeping the latest. Ephemeral."""
    return payments.distinct(on="payment_id", keep="last")
