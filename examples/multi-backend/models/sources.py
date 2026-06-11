"""Source models — ingest data from multiple backends via DuckDB ATTACH.

Demonstrates: DuckDB ATTACH for Postgres and SQLite, cross-database reads,
and local CSV enrichment data.
"""

from interlace import model, get_connection, sql


@model(
    name="pg_customers",
    strategy="replace",
    description="Customers read from operational Postgres via DuckDB ATTACH",
    tags=["source", "postgres"],
)
def pg_customers():
    """Read customers from Postgres via DuckDB ATTACH.

    The query references pg.public.customers where 'pg' is the attached
    Postgres database name defined in config.yaml.
    """
    return sql("SELECT * FROM pg.public.customers")


@model(
    name="legacy_orders",
    strategy="replace",
    description="Historical orders read from legacy SQLite via DuckDB ATTACH",
    tags=["source", "sqlite"],
)
def legacy_orders():
    """Read legacy order data from SQLite via DuckDB ATTACH.

    The query references legacy.orders where 'legacy' is the attached
    SQLite database name defined in config.yaml.
    """
    return sql("SELECT * FROM legacy.orders")


@model(
    name="local_enrichment",
    strategy="replace",
    description="Industry and region enrichment data loaded from CSV",
    tags=["source"],
)
def local_enrichment():
    """Load enrichment data from a local CSV file."""
    return get_connection().read_csv("data/enrichment.csv")
