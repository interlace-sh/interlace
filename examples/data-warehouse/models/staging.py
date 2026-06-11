"""Staging models -- clean and validate before warehouse loading.

Demonstrates: ephemeral materialisation (never persisted to disk),
data cleansing, and validation filters.
"""

import ibis
from interlace import model


@model(
    name="stg_customers",
    materialise="ephemeral",
    description="Cleaned customers: lowercase email, validated segment",
    tags=["staging"],
)
def stg_customers(raw_customers: ibis.Table) -> ibis.Table:
    """Standardise email to lowercase and validate segment values."""
    valid_segments = ("standard", "premium", "enterprise")
    return raw_customers.mutate(
        email=raw_customers.email.lower(),
    ).filter(raw_customers.segment.isin(valid_segments))


@model(
    name="stg_products",
    materialise="ephemeral",
    description="Validated products: positive price, added margin column",
    tags=["staging"],
)
def stg_products(raw_products: ibis.Table) -> ibis.Table:
    """Filter out invalid prices and add an estimated margin column."""
    return (
        raw_products.filter(raw_products.price > 0)
        .mutate(estimated_margin=ibis.literal(0.3).cast("float64") * raw_products.price)
    )


@model(
    name="stg_orders",
    materialise="ephemeral",
    description="Filtered orders: remove invalid quantities",
    tags=["staging"],
)
def stg_orders(raw_orders: ibis.Table) -> ibis.Table:
    """Filter out orders with non-positive quantity."""
    return raw_orders.filter(raw_orders.quantity > 0)
