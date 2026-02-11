# Basic Test Project

A simple Interlace project demonstrating core functionality.

## Features Demonstrated

- **CSV Ingestion**: Reading data from CSV files
- **Strategies**: `append`, `merge_by_key`, `replace`
- **Materialization**: `table`, `view`
- **Dependencies**: Implicit dependencies via function parameters
- **Ibis Operations**: Joins, aggregations, filters

## Project Structure

```
basic/
├── config.yaml          # Project configuration
├── models/              # Model definitions
│   ├── sources.py       # Source models (CSV ingestion)
│   └── transformations.py # Transformation models
├── data/                # CSV data files
│   ├── users.csv
│   ├── orders.csv
│   └── products.csv
└── README.md
```

## Models

### Source Models (`models/sources.py`)

- **`users`**: Reads user data from CSV (append strategy)
- **`orders`**: Reads order data from CSV (merge_by_key strategy)
- **`products`**: Reads product data from CSV (replace strategy)

### Transformation Models (`models/transformations.py`)

- **`user_orders`**: Joins users and orders (depends on `users`, `orders`)
- **`user_summary`**: Aggregates user order statistics (depends on `user_orders`)
- **`recent_orders`**: Filters recent orders as a view (depends on `orders`)

## Running the Project

```bash
cd projects/basic

# Run all models
interlace run

# Run specific models
interlace run users orders

# Run with environment
interlace run --env dev
```

## Expected Output

The project will:
1. Discover 6 models (3 sources, 3 transformations)
2. Build dependency graph automatically
3. Execute models dynamically as dependencies are ready
4. Create tables and views in DuckDB database

