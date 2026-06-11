# TPC-H Benchmark Example Project

This is a complete TPC-H benchmark implementation using Interlace. TPC-H is a decision support benchmark that consists of a suite of business-oriented ad-hoc queries and concurrent data modifications.

## Overview

TPC-H models a wholesale supplier with 8 tables:
- **Dimension tables**: `nation`, `region`
- **Medium tables**: `supplier`, `customer`, `part`, `partsupp`
- **Fact tables**: `orders`, `lineitem`

The benchmark includes 22 standard queries that test various aspects of decision support systems.

## Project Structure

```
tpch/
├── config.yaml          # Project configuration
├── config.dev.yaml       # Development environment config
├── models/               # Model definitions
│   └── tpch/
│       ├── tpch_data.py  # Data generation model
│       ├── tpch_q01.sql  # Query 1: Pricing Summary Report
│       ├── tpch_q02.sql  # Query 2: Minimum Cost Supplier
│       ├── ...           # Queries 3-21
│       └── tpch_q22.sql  # Query 22: Global Sales Opportunity
├── data/                 # Database files (generated)
└── logs/                 # Execution logs
```

## Models

### Data Generation (`tpch_data`)

The `tpch_data` model generates TPC-H benchmark data using DuckDB's built-in `dbgen` function. It creates all 8 standard TPC-H tables directly in the database.

**Scale Factor**: The default scale factor is 0.1 (≈100MB). You can modify this in `tpch_data.py`:
- `sf=0.1` ≈ 100MB
- `sf=1` ≈ 1GB
- `sf=10` ≈ 10GB
- `sf=100` ≈ 100GB

### Queries (tpch_q01 through tpch_q22)

All 22 standard TPC-H queries are implemented as SQL models:

1. **Q1**: Pricing Summary Report
2. **Q2**: Minimum Cost Supplier
3. **Q3**: Shipping Priority
4. **Q4**: Order Priority Checking
5. **Q5**: Local Supplier Volume
6. **Q6**: Forecasting Revenue Change
7. **Q7**: Volume Shipping
8. **Q8**: National Market Share
9. **Q9**: Product Type Profit Measure
10. **Q10**: Returned Item Reporting
11. **Q11**: Important Stock Identification
12. **Q12**: Shipping Modes and Order Priority
13. **Q13**: Customer Distribution
14. **Q14**: Promotion Effect
15. **Q15**: Top Supplier Query
16. **Q16**: Parts/Supplier Relationship
17. **Q17**: Small-Quantity-Order Revenue
18. **Q18**: Large Volume Customer
19. **Q19**: Discounted Revenue
20. **Q20**: Potential Part Promotion
21. **Q21**: Suppliers Who Kept Orders Waiting
22. **Q22**: Global Sales Opportunity

All queries are materialized as `ephemeral` (not persisted) and depend on `tpch_data` to ensure data is generated first.

## Running the Benchmark

### Option 1: Using the Runner Script

```bash
cd examples/tpch
python run.py
```

### Option 2: Using the CLI

```bash
cd examples/tpch
interlace run
```

### Option 3: Run Specific Models

```bash
# Generate data only
interlace run tpch_data

# Run specific queries
interlace run tpch_data tpch_q01 tpch_q02

# Run all queries
interlace run
```

## Expected Output

The benchmark will:
1. Generate TPC-H data (if not already present)
2. Execute all 22 queries in dependency order
3. Display results and execution times

## Configuration

The project uses DuckDB for all connections. The database file is created at:
- `data/dev/main.duckdb` (for dev environment)

You can change the environment:
```bash
interlace run --env prod
```

This will use `data/prod/main.duckdb` instead.

## Dependencies

All queries automatically depend on `tpch_data` through Interlace's dependency discovery. The executor ensures data is generated before queries run.

## Notes

- The data generation model checks if tables already exist to avoid regenerating data unnecessarily
- All queries use the `main` schema
- Queries are materialized as `ephemeral` (results are not persisted)
- The benchmark follows the standard TPC-H specification


