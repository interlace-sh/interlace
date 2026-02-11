# Comprehensive Test Project

A comprehensive Interlace project demonstrating all functionality including:
- All materialization types (table, view, ephemeral)
- All strategies (merge_by_key, append, replace, none)
- Schema evolution (explicit fields, automatic column addition)
- Migrations
- SQL models
- Python models
- Change detection
- File hash tracking

## Features Demonstrated

### Materialization Types
- ✅ **Table**: Persistent tables (`customers`, `orders`, `products`)
- ✅ **View**: Virtual tables (`active_customers_view`, `recent_orders`)
- ✅ **Ephemeral**: Temporary tables (`temp_calculation`)

### Strategies
- ✅ **merge_by_key**: Merge/upsert data (`customers`, `orders`)
- ✅ **append**: Append new rows (`events`)
- ✅ **replace**: Full table replacement (`products`, `reference_data`)
- ✅ **none**: No strategy applied (`reference_data`)

### Schema Evolution
- ✅ **Explicit fields**: `orders` model uses explicit fields schema definition
- ✅ **Automatic column addition**: Schema validation adds new columns automatically
- ✅ **Type changes**: Safe type casts handled automatically

### Migrations
- ✅ **Initial schema**: `001_initial_schema.sql` - Creates indexes
- ✅ **Column addition**: `002_add_customer_segment.sql` - Adds customer_segment column
- ✅ **Multiple columns**: `003_add_order_metadata.sql` - Adds multiple metadata columns

### Model Types
- ✅ **Python models**: `01_sources.py`, `02_transformations.py`
- ✅ **SQL models**: `03_sql_models.sql`

### Advanced Features
- ✅ **Dependencies**: Implicit dependencies via function parameters and SQL FROM/JOIN
- ✅ **Ibis operations**: Joins, aggregations, filters, window functions
- ✅ **Change detection**: File hash tracking for views
- ✅ **Multiple schemas**: All models in `public` schema

## Project Structure

```
comprehensive/
├── config.yaml              # Project configuration
├── models/                  # Model definitions
│   ├── 01_sources.py        # Source models (all strategies/materializations)
│   ├── 02_transformations.py # Transformation models (ibis operations)
│   └── 03_sql_models.sql    # SQL models
├── migrations/             # SQL migration files
│   ├── 001_initial_schema.sql
│   ├── 002_add_customer_segment.sql
│   └── 003_add_order_metadata.sql
├── data/                    # CSV data files
│   ├── customers.csv
│   ├── orders.csv
│   ├── products.csv
│   ├── events.csv
│   └── reference_data.csv
└── README.md
```

## Running the Project

### Run All Models

```bash
cd projects/comprehensive

# Run all models
interlace run

# Run with environment
interlace run --env dev

# Force run (bypass change detection)
interlace run --force
```

### Run Migrations

```bash
# Run all pending migrations
interlace migrate

# Run migrations for specific environment
interlace migrate --env dev

# Run specific migration
interlace migrate --migration 002_add_customer_segment.sql

# Dry run (preview without executing)
interlace migrate --dry-run
```

### Run Specific Models

```bash
# Run source models only
interlace run customers orders products

# Run transformation models
interlace run customer_orders customer_summary

# Run SQL models
interlace run product_revenue monthly_sales
```

## Testing Schema Evolution

### Test Automatic Column Addition

1. Modify `models/01_sources.py` to add a new column to `customers` model:
   ```python
   # Add new column to CSV or modify model to include new field
   ```

2. Run the model:
   ```bash
   interlace run customers
   ```

3. Schema validation will automatically add the new column to the table.

### Test Explicit Schema

The `orders` model uses explicit `fields` parameter. This ensures:
- Schema is strictly controlled
- Type changes are validated
- New columns are only added via migrations

### Test Migrations

1. Run migrations:
   ```bash
   interlace migrate
   ```

2. Check that columns were added:
   ```bash
   # Query the database to verify columns exist
   ```

### Test Merge Strategy

The `dynamic_merge_demo` model demonstrates the merge_by_key strategy with dynamic random data:

1. Run the model multiple times:
   ```bash
   interlace run dynamic_merge_demo --force
   interlace run dynamic_merge_demo --force
   interlace run dynamic_merge_demo --force
   ```

2. Observe the results:
   - **First run**: All records are inserted (new) - e.g., "25 rows, 25 inserted"
   - **Subsequent runs**: Mix of updated and inserted records - e.g., "25 rows, 2 inserted, 13 updated"
   - The model generates 10-15 random records with IDs from a pool of 1-25, creating natural overlap
   - Each run uses current timestamp as seed, so data changes each time
   - Overlapping IDs get updated with new values, new IDs get inserted

3. Example output:
   ```
   ✓ dynamic_merge_demo table merge_by_key   0.13s   completed     25 ⇒ +2 ~13
   ```
   - `25` = total rows in table after merge
   - `+2` = 2 new rows inserted
   - `~13` = 13 existing rows updated

4. The model demonstrates:
   - **Merge strategy**: Updates existing records, inserts new ones
   - **Primary key**: Uses `record_id` as primary key for matching
   - **Random data**: Generates different data each run to show merge in action
   - **Overlapping IDs**: Some IDs overlap across runs (updated), some are new (inserted)

## Expected Behavior

### First Run
1. All source models execute (customers, orders, products, events, reference_data)
2. Tables are created with initial schema
3. Transformation models execute after sources complete
4. SQL models execute after dependencies are ready
5. Views are created

### Subsequent Runs
1. Change detection skips unchanged models
2. Models with file changes re-execute
3. Upstream changes trigger downstream re-execution
4. Views only recreate if file hash changed

### Schema Evolution
1. New columns are automatically added (if not using explicit table_schema)
2. Safe type changes are applied automatically
3. Breaking changes require migrations
4. Removed columns are preserved (safe mode)

## Best Practices Demonstrated

1. **Ibis-first**: All transformations use ibis operations
2. **Explicit schemas**: Use `fields` for strict control
3. **Migrations**: Use migrations for breaking changes
4. **Strategies**: Choose appropriate strategy for each use case
5. **Materialization**: Use views for computed data, tables for persisted data
6. **Dependencies**: Leverage implicit dependencies for clean code

