"""The six raw tables, read straight from dbt's repo.

dbt keeps these as seed CSVs loaded by `dbt seed` — and disables them by default
(`load_source_data: false`), because the real project expects the data to already be
in the warehouse behind `{{ source('ecom', ...) }}`. interlace has neither concept: a
source is a model with no upstreams, and DuckDB reads the CSV over HTTP, so no
ingestion step stands between the repo and the DAG.

Each of these is an ordinary SQL model. They are registered from a loop rather than
six near-identical files because the only thing that varies is the file name:

    SELECT * FROM read_csv_auto('.../raw_customers.csv')   -- models/raw/raw_customers.sql
"""

from interlace.dsl.decorators import REGISTRY, ModelDef

JAFFLE_DATA = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop/main/seeds/jaffle-data"

SOURCES = ("raw_customers", "raw_items", "raw_orders", "raw_products", "raw_stores", "raw_supplies")

for source in SOURCES:
    REGISTRY.register_model(
        ModelDef(name=source, sql=f"SELECT * FROM read_csv_auto('{JAFFLE_DATA}/{source}.csv')")
    )
