# Interlace Test Projects

This directory contains test projects demonstrating Interlace functionality.

## Projects

### Basic (`basic/`)

A simple project demonstrating core functionality:
- CSV ingestion
- Basic strategies (append, merge_by_key, replace)
- Table and view materialization
- Simple transformations

**Use Case**: Learning Interlace basics, quick testing

### Comprehensive (`comprehensive/`)

A comprehensive project covering all Interlace features:
- ✅ All materialization types (table, view, ephemeral)
- ✅ All strategies (merge_by_key, append, replace, none)
- ✅ Schema evolution (explicit fields, automatic column addition)
- ✅ Migrations (SQL migration files)
- ✅ SQL models
- ✅ Python models
- ✅ Change detection
- ✅ File hash tracking

**Use Case**: Testing all features, reference implementation

### API (`api/`)

Demonstrates API integration:
- REST API consumption
- Paginated API requests
- Async model execution
- API data transformation

**Use Case**: API integration patterns, external data sources

### TPC-H (`tpch/`)

TPC-H benchmark implementation:
- All 22 standard TPC-H queries
- Data generation using DuckDB dbgen
- SQL model examples
- Performance testing

**Use Case**: Benchmarking, SQL query patterns

### TPC-DS (`tpcds/`)

TPC-DS benchmark framework:
- TPC-DS query examples
- Data generation framework
- Complex SQL patterns

**Use Case**: Advanced SQL patterns, data warehouse testing

### TPC-DI (`tpcdi/`)

TPC-DI data integration benchmark:
- Complete ETL pipeline
- Source, staging, and warehouse models
- Dimension and fact tables
- Data integration patterns

**Use Case**: ETL patterns, data warehouse architecture

## Quick Start

### Run Basic Project

```bash
cd projects/basic
interlace run
```

### Run Comprehensive Project

```bash
cd projects/comprehensive

# Run all models
interlace run

# Run migrations
interlace migrate
```

### Run API Project

```bash
cd projects/api
interlace run
```

### Run TPC Projects

```bash
cd projects/tpch
interlace run

# Or
cd projects/tpcds
interlace run

# Or
cd projects/tpcdi
interlace run
```

## Project Structure

Each project follows a standard structure:

```
project_name/
├── config.yaml          # Project configuration
├── models/              # Model definitions (Python and SQL)
├── migrations/          # SQL migration files (if applicable)
├── data/                # Data files (CSV, etc.)
└── README.md            # Project-specific documentation
```

## Best Practices

All projects demonstrate Interlace best practices:

1. **Ibis-first**: All transformations use ibis operations
2. **Explicit schemas**: Use `fields` for strict control when needed
3. **Migrations**: Use migrations for breaking schema changes
4. **Strategies**: Choose appropriate strategy for each use case
5. **Materialization**: Use views for computed data, tables for persisted data
6. **Dependencies**: Leverage implicit dependencies for clean code
7. **File organization**: Group related models in files
8. **Documentation**: Include README with usage instructions

## Testing

These projects serve as:
- **Functional tests**: Verify all features work correctly
- **Integration tests**: Test end-to-end workflows
- **Examples**: Demonstrate best practices
- **Reference**: Implementation patterns for users

## Contributing

When adding new test projects:
1. Follow the standard project structure
2. Include comprehensive README
3. Cover relevant functionality
4. Use best practices
5. Include sample data
6. Document expected behavior

