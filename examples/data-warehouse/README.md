# Data Warehouse -- Star Schema & SCD Type 2

An online retail data warehouse demonstrating SCD Type 2 history tracking, star schema patterns (dimension and fact tables), a staging layer, and quality checks.

## What You'll Learn

- **SCD Type 2** -- `strategy="scd_type_2"` with `scd2_config` for full history tracking of dimension changes
- **Tracked columns** -- `tracked_columns` limits change detection to specific columns (ignoring irrelevant changes)
- **Delete modes** -- `delete_mode="soft"` closes records when a customer is removed from the source, preserving history
- **Star schema** -- Dimension tables (`dim_customer`, `dim_product`, `dim_supplier`, `dim_date`) and fact tables (`fact_orders`, `fact_order_items`)
- **SCD Type 1** -- `strategy="merge_by_key"` for dimensions where history is not needed (overwrites in place)
- **Staging layer** -- `materialise="ephemeral"` for intermediate cleansing models that exist only during execution
- **Quality checks** -- `not_null`, `unique`, `freshness`, `expression`, and `row_count` checks in config
- **Tags** -- Layer-based organisation (`source`, `staging`, `warehouse`, `analytics`) for selective execution

## SCD Type 2 In Depth

The `scd2_config` dictionary is passed as a keyword argument to `@model` and read by the SCD Type 2 strategy:

```python
@model(
    name="dim_customer",
    strategy="scd_type_2",
    primary_key="customer_id",
    scd2_config={
        "tracked_columns": ["name", "email", "address", "segment"],
        "delete_mode": "soft",
    },
)
def dim_customer(stg_customers: ibis.Table) -> ibis.Table:
    return stg_customers
```

The strategy automatically manages these columns (added to the target table):

| Column | Type | Description |
|--------|------|-------------|
| `valid_from` | TIMESTAMP | When this version of the record became active |
| `valid_to` | TIMESTAMP | When this version was superseded (NULL = current) |
| `is_current` | BOOLEAN | Whether this is the current version |
| `_scd2_hash` | VARCHAR | MD5 hash of tracked columns for change detection |

### How It Works

1. **First run** -- All records are inserted with `is_current=TRUE`, `valid_to=NULL`
2. **Subsequent runs** -- The strategy compares hashes of tracked columns:
   - **Changed records**: Old row is closed (`valid_to` set, `is_current=FALSE`), new row inserted
   - **New records**: Inserted as current
   - **Deleted records** (with `delete_mode="soft"`): Closed but preserved in history
   - **Unchanged records**: Left untouched

### Testing SCD2 Changes

Run the data generator twice to see history tracking in action:

```bash
python generate_data.py           # Baseline data
interlace run                     # First load -- all records are current

python generate_data.py --drift   # Simulate address moves, segment upgrades, price changes
interlace run                     # Second load -- changed records create history
```

The `product_price_history` analytics model queries `dim_product` to show all historical price versions.

## Models

| Model | Layer | Strategy | Materialisation | Key Features |
|-------|-------|----------|-----------------|--------------|
| `raw_customers` | Source | `replace` | table | CSV ingestion |
| `raw_products` | Source | `replace` | table | CSV ingestion |
| `raw_suppliers` | Source | `replace` | table | CSV ingestion |
| `raw_orders` | Source | `append` | table | Append-only events |
| `stg_customers` | Staging | -- | ephemeral | Lowercase email, validate segment |
| `stg_products` | Staging | -- | ephemeral | Price validation, margin column |
| `stg_orders` | Staging | -- | ephemeral | Filter invalid quantities |
| `dim_customer` | Warehouse | `scd_type_2` | table | `tracked_columns`, `delete_mode="soft"` |
| `dim_product` | Warehouse | `scd_type_2` | table | Tracks price/name/category changes |
| `dim_supplier` | Warehouse | `merge_by_key` | table | SCD Type 1 (no history) |
| `dim_date` | Warehouse | `replace` | table | Generated date range for 2024 |
| `fact_orders` | Warehouse | `append` | table | Immutable fact table |
| `fact_order_items` | Warehouse | `append` | table | Denormalised order lines |
| `customer_lifetime_value` | Analytics | `replace` | table | CLV aggregation |
| `product_price_history` | Analytics | `replace` | table | SCD2 history query |
| `monthly_revenue` | Analytics | -- | view | Revenue by month and category |

## Quality Checks

Defined in `config.yaml`, these run automatically after materialisation:

- **dim_customer**: `not_null` and `unique` on `customer_id`, `freshness` on `valid_from` (warn if > 7 days)
- **fact_orders**: `not_null` on `order_id`, `expression` check that `total_amount > 0`, `row_count` minimum of 1

## Run It

```bash
cd examples/data-warehouse
interlace run
```

### Run by layer using tags

```bash
interlace run --tags source         # Only source models
interlace run --tags staging        # Only staging models
interlace run --tags warehouse      # Only warehouse (dims + facts)
interlace run --tags analytics      # Only analytics
```

## Project Structure

```
data-warehouse/
├── config.yaml          # Connection + state + quality checks
├── pyproject.toml       # Project metadata
├── generate_data.py     # Regenerate CSVs (--drift for SCD2 testing)
├── data/
│   ├── customers.csv    # 15 customers
│   ├── products.csv     # 12 products
│   ├── suppliers.csv    # 5 suppliers
│   └── orders.csv       # 48 orders (Jan-Jun 2024)
└── models/
    ├── sources.py       # 4 source models (replace + append)
    ├── staging.py       # 3 ephemeral staging models
    ├── warehouse.py     # 6 warehouse models (dims + facts)
    └── analytics.py     # 3 analytics models (CLV, price history, revenue)
```

## Next Steps

- [ecommerce](../ecommerce/) -- Full-featured project with schema evolution, column mapping, and exports
- [incremental](../incremental/) -- Cursor-based incremental processing and caching
- [testing](../testing/) -- Deep dive into `interlace.testing` utilities
