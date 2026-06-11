"""Source models -- ingest raw data from CSV files.

Demonstrates: replace and append strategies for raw data loading.
"""

from interlace import model, get_connection


@model(
    name="raw_customers",
    strategy="replace",
    description="Raw customer data, fully replaced on each run",
    tags=["source"],
)
def raw_customers():
    """Load customers from CSV. Full replace -- always reflects latest file."""
    return get_connection().read_csv("data/customers.csv")


@model(
    name="raw_products",
    strategy="replace",
    description="Raw product catalogue, fully replaced on each run",
    tags=["source"],
)
def raw_products():
    """Load products from CSV. Full replace -- always reflects latest file."""
    return get_connection().read_csv("data/products.csv")


@model(
    name="raw_suppliers",
    strategy="replace",
    description="Raw supplier data, fully replaced on each run",
    tags=["source"],
)
def raw_suppliers():
    """Load suppliers from CSV. Full replace -- always reflects latest file."""
    return get_connection().read_csv("data/suppliers.csv")


@model(
    name="raw_orders",
    strategy="append",
    description="Raw order transactions, append-only",
    tags=["source"],
)
def raw_orders():
    """Load orders from CSV. New rows are appended on each run."""
    return get_connection().read_csv("data/orders.csv")
