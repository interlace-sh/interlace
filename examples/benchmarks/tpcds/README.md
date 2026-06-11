# TPC-DS Benchmark Example Project

This is a TPC-DS benchmark implementation framework using Interlace. TPC-DS is a decision support benchmark that models a retail company with 24 tables and 99 queries.

## Overview

TPC-DS models a retail company with 24 tables:
- **Dimension tables**: `date_dim`, `time_dim`, `customer`, `customer_address`, 
  `customer_demographics`, `household_demographics`, `item`, `promotion`,
  `store`, `warehouse`, `ship_mode`, `reason`, `income_band`,
  `call_center`, `web_page`, `web_site`
- **Fact tables**: `store_sales`, `store_returns`, `catalog_sales`, 
  `catalog_returns`, `web_sales`, `web_returns`, `inventory`

The benchmark includes 99 standard queries covering various decision support scenarios.

## Project Structure

```
tpcds/
├── config.yaml          # Project configuration
├── config.dev.yaml       # Development environment config
├── models/               # Model definitions
│   └── tpcds/
│       ├── tpcds_data.py  # Data generation model
│       ├── tpcds_q01.sql # Query 1: Reporting Channel Sales
│       ├── tpcds_q02.sql # Query 2: Best Customers
│       ├── tpcds_q03.sql # Query 3: Customer Segmentation
│       └── ...            # Additional queries (q04-q99)
├── data/                 # Database files (generated)
└── logs/                 # Execution logs
```

## Data Generation

### Using DuckDB (if supported)

The `tpcds_data` model attempts to use DuckDB's built-in dbgen function:

```python
sql(f"CALL dbgen(sf={sf}, benchmark='tpcds')")
```

**Note**: DuckDB may not have built-in TPC-DS support. If this fails, use external tools.

### Using External TPC-DS Tools

If DuckDB doesn't support TPC-DS dbgen, you'll need to:

1. **Download TPC-DS tools** from [TPC.org](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
2. **Generate data files** using the TPC-DS data generator
3. **Load data into DuckDB** using COPY statements:

```sql
COPY date_dim FROM 'data/tpcds/date_dim.dat' (DELIMITER '|');
COPY time_dim FROM 'data/tpcds/time_dim.dat' (DELIMITER '|');
-- ... load other tables
```

Modify `tpcds_data.py` to load from files instead of using dbgen.

## Queries

TPC-DS has 99 standard queries. This project includes example queries (q01-q03) as a framework. 

### Generating All Queries

To generate all 99 TPC-DS queries automatically, use the `generate_queries.py` script:

```bash
cd examples/tpcds
python generate_queries.py
```

This script:
- Connects to DuckDB
- Extracts all queries from `tpcds_queries()` function
- Generates SQL files (`tpcds_q01.sql` through `tpcds_q99.sql`) in `models/tpcds/`
- Adds proper metadata headers to each query

**Note**: This requires DuckDB with TPC-DS support enabled.

### Manual Query Creation

Alternatively, you can manually create queries:

1. Obtain the official TPC-DS query templates from TPC.org
2. Convert them to SQL files following the naming pattern: `tpcds_q##.sql`
3. Add proper metadata headers:
   ```sql
   -- TPC-DS Query ##: Query Name
   -- @name: tpcds_q##
   -- @schema: tpcds
   -- @materialize: ephemeral
   -- @dependencies: tpcds_data
   ```

### Query Categories

TPC-DS queries cover:
- **Reporting queries**: Standard reports and aggregations
- **Ad-hoc queries**: Complex analytical queries
- **Iterative OLAP**: Multi-dimensional analysis
- **Data mining**: Pattern discovery queries

## Running the Benchmark

### Option 1: Using the Runner Script

```bash
cd examples/tpcds
python run.py
```

### Option 2: Using the CLI

```bash
cd examples/tpcds
interlace run
```

### Option 3: Run Specific Models

```bash
# Generate data only
interlace run tpcds_data

# Run specific queries
interlace run tpcds_data tpcds_q01 tpcds_q02

# Run all queries
interlace run
```

## Configuration

The project uses DuckDB for all connections. The database file is created at:
- `data/dev/main.duckdb` (for dev environment)

You can change the environment:
```bash
interlace run --env prod
```

## Scale Factors

TPC-DS scale factors control data size:
- `sf=1` ≈ 1GB
- `sf=10` ≈ 10GB
- `sf=100` ≈ 100GB
- `sf=1000` ≈ 1TB

Adjust the scale factor in `tpcds_data.py` based on your needs.

## Dependencies

All queries automatically depend on `tpcds_data` through Interlace's dependency discovery. The executor ensures data is generated before queries run.

## Notes

- TPC-DS is more complex than TPC-H with 24 tables vs 8 tables
- The benchmark includes 99 queries vs 22 in TPC-H
- Data generation may require external tools if DuckDB doesn't support TPC-DS dbgen
- Queries are materialized as `ephemeral` (results are not persisted)
- The benchmark follows the standard TPC-DS specification

## Resources

- [TPC-DS Specification](https://www.tpc.org/tpcds/)
- [TPC-DS Tools](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)
- [TPC-DS Query Templates](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp)

