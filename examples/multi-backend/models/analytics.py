"""Analytics models — cross-database joins and export.

Demonstrates: joining data from Postgres + CSV, Parquet export.
"""

import ibis
from interlace import model


@model(
    name="enriched_customers",
    strategy="replace",
    description="Postgres customers enriched with CSV industry/region data",
    tags=["analytics"],
)
def enriched_customers(pg_customers: ibis.Table, local_enrichment: ibis.Table) -> ibis.Table:
    """Cross-database join: Postgres customers + CSV enrichment.

    This model pulls customer records from the attached Postgres database
    and joins them with locally-loaded CSV enrichment data, all within
    DuckDB's query engine.
    """
    return pg_customers.join(local_enrichment, "customer_id")


@model(
    name="customer_export",
    strategy="replace",
    export={"format": "parquet", "path": "output/customers.parquet"},
    description="Enriched customers exported to Parquet",
    tags=["analytics", "export"],
)
def customer_export(enriched_customers: ibis.Table) -> ibis.Table:
    """Export enriched customer data to Parquet for downstream consumption."""
    return enriched_customers
