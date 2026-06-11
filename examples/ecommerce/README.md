# Ecommerce — The Canonical Project

A complete online store pipeline: customers, orders, products, and payments through to analytics. This is Interlace's equivalent of dbt's Jaffle Shop.

## What You'll Learn

- All strategies: `replace`, `append`, `merge_by_key` (+ `none` via ephemeral)
- All materialisations: `table`, `view`, `ephemeral`
- `fields=` with `strict=True` for schema enforcement
- `column_mapping=` to rename columns on ingestion
- `schema_mode="safe"` for safe schema evolution
- `export=` to write results to CSV
- Inline `checks=` on models + config-level checks + SQL check files
- SQL models with metadata comments
- `test_model_sync()` / `mock_dependency()` for unit testing
- Environment overlays (`config.dev.yaml`, `config.prod.yaml`)
- Tags, owner, description metadata
- Database migrations

## Models

| Model | Type | Strategy | Materialisation | Key Features |
|-------|------|----------|-----------------|--------------|
| `customers` | Source | `merge_by_key` | table | `fields=`, `strict=True`, inline checks, pattern check |
| `orders` | Source | `append` | table | `schema_mode="safe"` |
| `products` | Source | `replace` | table | |
| `payments` | Source | `append` | table | `column_mapping=` |
| `stg_orders` | Staging | — | **ephemeral** | Filters cancelled orders |
| `stg_payments` | Staging | — | **ephemeral** | Deduplicates by payment_id |
| `customer_orders` | Analytics | `replace` | table | Join customers + orders |
| `order_payments` | Analytics | `replace` | table | Join orders + payments |
| `customer_lifetime_value` | Analytics | `replace` | table | `export=` CSV output |
| `monthly_revenue` | Analytics | — | **view** | Revenue by month |
| `product_performance` | Analytics (SQL) | `replace` | table | SQL model with metadata |
| `customer_segments` | Analytics (SQL) | `replace` | table | SQL window functions |

## Run It

```bash
cd examples/ecommerce
interlace run
```

### Run tests

```bash
cd examples/ecommerce
uv run pytest tests/
```

### Environment overlays

```bash
INTERLACE_ENV=dev interlace run    # Debug logging, check warnings only
INTERLACE_ENV=prod interlace run   # Warning logging, check errors fail
```

## Project Structure

```
ecommerce/
├── config.yaml              # Main configuration
├── config.dev.yaml          # Development overlay
├── config.prod.yaml         # Production overlay
├── data/
│   ├── customers.csv        # 20 customers
│   ├── orders.csv           # 100 orders
│   ├── products.csv         # 15 products
│   └── payments.csv         # 99 payments (1 cancelled order has no payment)
├── migrations/
│   └── 001_add_loyalty_tier.sql
├── checks/
│   └── no_negative_totals.sql  # SQL check example
├── models/
│   ├── sources.py           # 4 source models
│   ├── staging.py           # 2 ephemeral staging models
│   ├── analytics.py         # 4 Python analytics models
│   ├── product_performance.sql
│   └── customer_segments.sql
├── output/                  # Generated: CLV export
└── tests/
    └── test_models.py       # Unit tests with mock_dependency
```

## Next Steps

- [testing](../testing/) — Deep dive into `interlace.testing`
- [api-ingestion](../api-ingestion/) — REST API patterns with retry and caching
