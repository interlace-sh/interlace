"""Warehouse models -- star schema with SCD Type 2 dimensions and fact tables.

Demonstrates: SCD Type 2 (scd2_config with tracked_columns and delete_mode),
merge_by_key (SCD Type 1), date dimension generation, and fact tables.
"""

import ibis
from interlace import model


# ---------------------------------------------------------------------------
# Dimension tables
# ---------------------------------------------------------------------------


@model(
    name="dim_customer",
    strategy="scd_type_2",
    primary_key="customer_id",
    scd2_config={
        "tracked_columns": ["name", "email", "address", "segment"],
        "delete_mode": "soft",
    },
    description="Customer dimension with SCD Type 2 history tracking",
    tags=["warehouse", "dimension"],
)
def dim_customer(stg_customers: ibis.Table) -> ibis.Table:
    """SCD Type 2 -- tracks changes to name, email, address, segment.

    When a customer's address or segment changes between runs, the old
    record is closed (valid_to set, is_current=FALSE) and a new current
    record is inserted. This preserves the full history of changes.
    """
    return stg_customers


@model(
    name="dim_product",
    strategy="scd_type_2",
    primary_key="product_id",
    scd2_config={
        "tracked_columns": ["name", "price", "category"],
    },
    description="Product dimension -- tracks price changes over time",
    tags=["warehouse", "dimension"],
)
def dim_product(stg_products: ibis.Table) -> ibis.Table:
    """SCD Type 2 -- tracks price, name, and category changes.

    Particularly useful for analytics that need to know what price
    a product had at the time of an order.
    """
    return stg_products


@model(
    name="dim_supplier",
    strategy="merge_by_key",
    primary_key="supplier_id",
    description="Supplier dimension -- SCD Type 1 (overwrite, no history)",
    tags=["warehouse", "dimension"],
)
def dim_supplier(raw_suppliers: ibis.Table) -> ibis.Table:
    """SCD Type 1 via merge_by_key -- always reflects the latest state.

    Unlike dim_customer and dim_product, supplier changes are overwritten
    in place. Use merge_by_key when history tracking is not required.
    """
    return raw_suppliers


@model(
    name="dim_date",
    strategy="replace",
    description="Date dimension -- reference table covering 2024",
    tags=["warehouse", "dimension"],
)
def dim_date() -> ibis.Table:
    """Generate a date dimension spanning 2024-01-01 to 2024-12-31.

    Includes date_key, full_date, year, quarter, month, month_name,
    day_of_week, day_name, and is_weekend flag.
    """
    import pandas as pd

    dates = pd.date_range("2024-01-01", "2024-12-31", freq="D")
    df = pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.strftime("%Y-%m-%d"),
        "year": dates.year,
        "quarter": dates.quarter,
        "month": dates.month,
        "month_name": dates.strftime("%B"),
        "day_of_week": dates.dayofweek,
        "day_name": dates.strftime("%A"),
        "is_weekend": dates.dayofweek >= 5,
    })
    return df


# ---------------------------------------------------------------------------
# Fact tables
# ---------------------------------------------------------------------------


@model(
    name="fact_orders",
    strategy="append",
    description="Order fact table -- immutable, append-only",
    tags=["warehouse", "fact"],
)
def fact_orders(
    stg_orders: ibis.Table,
    dim_customer: ibis.Table,
    dim_product: ibis.Table,
) -> ibis.Table:
    """Build fact table by enriching staged orders with dimension keys.

    Joins to current dimension records only (is_current=TRUE for SCD2
    dimensions) to attach the latest surrogate context.
    """
    current_customers = dim_customer.filter(dim_customer.is_current == True)  # noqa: E712
    current_products = dim_product.filter(dim_product.is_current == True)  # noqa: E712

    return (
        stg_orders
        .join(current_customers, stg_orders.customer_id == current_customers.customer_id)
        .join(current_products, stg_orders.product_id == current_products.product_id)
        .select(
            stg_orders.order_id,
            stg_orders.customer_id,
            stg_orders.product_id,
            stg_orders.quantity,
            stg_orders.order_date,
            stg_orders.total_amount,
            current_customers.name.name("customer_name"),
            current_customers.segment.name("customer_segment"),
            current_products.name.name("product_name"),
            current_products.category.name("product_category"),
        )
    )


@model(
    name="fact_order_items",
    strategy="append",
    description="Order line items -- one row per order-product combination",
    tags=["warehouse", "fact"],
)
def fact_order_items(
    stg_orders: ibis.Table,
    dim_product: ibis.Table,
) -> ibis.Table:
    """Denormalised order line items with unit price from the product dimension.

    Uses the current product record to attach the unit price at the time
    of the pipeline run.
    """
    current_products = dim_product.filter(dim_product.is_current == True)  # noqa: E712

    return (
        stg_orders
        .join(current_products, stg_orders.product_id == current_products.product_id)
        .select(
            stg_orders.order_id,
            stg_orders.product_id,
            current_products.name.name("product_name"),
            current_products.category.name("category"),
            stg_orders.quantity,
            current_products.price.name("unit_price"),
            stg_orders.total_amount.name("line_total"),
        )
    )
