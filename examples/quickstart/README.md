# Quickstart — Your First Pipeline

A coffee shop pipeline in under 5 minutes. Three models, two CSV files, zero configuration headaches.

## What You'll Learn

- Loading CSV data with `@model`
- Strategies: `replace` vs `append`
- Joining tables with ibis
- Materialising results as a `view`

## Models

| Model | Type | Strategy | Materialisation |
|-------|------|----------|-----------------|
| `menu_items` | Source (CSV) | `replace` | table |
| `sales` | Source (CSV) | `append` | table |
| `daily_revenue` | Transform | — | view |

## Run It

```bash
pip install interlace   # or: uv add interlace
cd examples/quickstart
interlace run
```

## Project Structure

```
quickstart/
├── config.yaml          # Connection + defaults
├── data/
│   ├── menu_items.csv   # 10 menu items
│   └── sales.csv        # 50 transactions
└── models/
    ├── sources.py       # menu_items + sales
    └── daily_revenue.py # Join + aggregate
```

## Next Steps

Once this runs, try the [ecommerce](../ecommerce/) example for a full-featured project.
