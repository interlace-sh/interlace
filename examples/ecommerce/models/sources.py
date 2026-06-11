"""Source models — ingest raw data from CSV files.

Demonstrates: merge_by_key, append, replace strategies, fields with strict,
column_mapping, schema_mode, and inline checks.
"""

from interlace import model, get_connection


@model(
    name="customers",
    strategy="merge_by_key",
    primary_key="customer_id",
    fields={"customer_id": "int64", "name": "string", "email": "string", "segment": "string", "created_at": "string"},
    strict=True,
    checks=[
        {"type": "not_null", "column": "customer_id"},
        {"type": "unique", "column": "customer_id"},
        {"type": "not_null", "column": "email"},
        {"type": "accepted_values", "column": "segment", "values": ["standard", "premium", "enterprise"]},
        {"type": "pattern", "column": "email", "pattern": "^[^@]+@[^@]+\\.[^@]+$", "severity": "warn"},
    ],
    description="Customer master data, merged by customer_id on each run",
    tags=["source", "customers"],
    owner="data-team",
)
def customers():
    """Load customers from CSV. Existing rows are updated, new rows inserted."""
    return get_connection().read_csv("data/customers.csv")


@model(
    name="orders",
    strategy="append",
    schema_mode="safe",
    checks=[
        {"type": "not_null", "column": "order_id"},
        {"type": "unique", "column": "order_id"},
        {"type": "row_count", "min_count": 1},
        {"type": "relationships", "column": "customer_id", "to_table": "customers", "to_column": "customer_id"},
    ],
    description="Order transactions, append-only",
    tags=["source", "orders"],
)
def orders():
    """Load orders. New rows are appended; schema changes handled safely."""
    return get_connection().read_csv("data/orders.csv")


@model(
    name="products",
    strategy="replace",
    description="Product catalogue, fully replaced on each run",
    tags=["source", "products"],
)
def products():
    """Load product catalogue. Full replace — always reflects latest CSV."""
    return get_connection().read_csv("data/products.csv")


@model(
    name="payments",
    strategy="append",
    column_mapping={"amt": "amount", "pmnt_type": "payment_type"},
    description="Payment records with column remapping",
    tags=["source", "payments"],
)
def payments():
    """Load payments. Columns 'amt' and 'pmnt_type' are renamed on ingestion."""
    return get_connection().read_csv("data/payments.csv")
