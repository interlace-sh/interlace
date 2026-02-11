# DummyJSON Example Project

This example project demonstrates how to use Interlace to consume REST APIs and create data transformation pipelines.

## Overview

The project fetches data from [DummyJSON](https://dummyjson.com/docs), a free API for testing and prototyping, and creates various analytical models.

## Project Structure

```
dummyjson_project/
├── models/
│   ├── sources.py          # Source models that fetch data from DummyJSON
│   └── transformations.py  # Transformation models that process the data
└── README.md
```

## Source Models

These models fetch raw data from DummyJSON:

- **products** - Product catalog with prices, ratings, and categories
- **users** - User profiles with demographics
- **carts** - Shopping cart data
- **posts** - Blog posts with reactions
- **comments** - Post comments
- **todos** - Todo items with completion status
- **quotes** - Inspirational quotes
- **recipes** - Recipe data with nutrition info

## Transformation Models

These models process and enrich the source data:

- **product_categories** - Product statistics by category
- **product_price_analysis** - Pricing analysis with discount impact
- **user_activity_summary** - User activity across posts, todos, and carts
- **user_demographics** - Demographic analysis
- **high_value_carts** - Identify high-value shopping carts
- **post_engagement** - Post engagement metrics
- **todo_completion_rates** - Todo completion rates by user
- **quote_categories** - Quote categorization by length
- **recipe_nutrition_summary** - Recipe nutrition summaries

## Running the Project

```bash
# Run all models
interlace run

# Run only source models
interlace run models/sources.py

# Run only transformation models
interlace run models/transformations.py

# Run a specific model
interlace run models/sources.py::products
```

## Example Usage

### Fetching Products

```python
from interlace import model, API

@model(name="products", materialize="table")
async def products():
    async with API(base_url="https://dummyjson.com") as api:
        data = await api.paginated(
            "/products",
            page_size=30,
            page_size_param="limit",
            page_param="skip",
            count_attribute="total",
            data_attribute="products"
        )
        return data
```

### Transforming Data

```python
from interlace import model, get_connection

@model(name="product_categories", materialize="table")
async def product_categories():
    conn = get_connection()
    products = conn.table("products")
    
    result = products.group_by("category").agg(
        total_products=products.count(),
        avg_price=products.price.mean(),
        avg_rating=products.rating.mean()
    )
    
    return result
```

## API Reference

This project uses the [DummyJSON API](https://dummyjson.com/docs), which provides:

- **Pagination**: Use `limit` and `skip` query parameters
- **Filtering**: Various filter options per resource
- **Search**: Search functionality for most resources
- **No Authentication Required**: Public API, no API keys needed

## Notes

- DummyJSON is a free testing API - data is randomly generated
- Rate limits may apply for high-volume usage
- Some transformation models use simplified logic for demonstration
- In production, you'd add error handling, incremental loading, etc.

