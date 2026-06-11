# Testing -- Testing Your Models

Patterns for unit testing, async testing, and integration testing Interlace models.

## What You'll Learn

- **Unit testing** with `test_model_sync()` and `mock_dependency()` -- no database required
- **Async testing** with `test_model()` for async models
- **Integration testing** with `run_sync()` to execute the full pipeline
- Using `TestResult` properties: `.row_count`, `.columns`, `.rows`, `.df`, `.status`

## Test Files

| File | What It Covers |
|------|----------------|
| `tests/test_unit.py` | `test_model_sync()`, `mock_dependency()`, edge cases, `TestResult` properties |
| `tests/test_async.py` | `test_model()` (async), boundary values, `asyncio_mode = "auto"` |
| `tests/test_integration.py` | `run_sync()` programmatic API, full pipeline execution |

## Models Under Test

| Model | Type | Description |
|-------|------|-------------|
| `orders` | Source (CSV) | Order transactions, strategy `replace` |
| `inventory` | Source (CSV) | Product catalogue, strategy `replace` |
| `order_totals` | Transform | Joins orders with inventory, computes `total = quantity * price` |
| `low_stock_alerts` | Async | Filters low-stock products, `materialise="none"` (side-effect) |

## Run It

```bash
# Install dependencies
pip install interlace   # or: uv add interlace

# Run the pipeline
cd examples/testing
interlace run

# Run all tests
pytest tests/

# Run only unit tests (fast, no database needed)
pytest tests/ -m unit

# Run integration tests
pytest tests/ -m integration
```

## Project Structure

```
testing/
├── config.yaml          # DuckDB connection + defaults
├── pyproject.toml       # pytest configuration
├── data/
│   ├── orders.csv       # 5 order rows
│   └── inventory.csv    # 5 product rows
├── models/
│   ├── sources.py       # orders + inventory (CSV sources)
│   └── analytics.py     # order_totals + low_stock_alerts
└── tests/
    ├── __init__.py
    ├── test_unit.py      # Sync unit tests with mock data
    ├── test_async.py     # Async model tests
    └── test_integration.py  # Full pipeline with run_sync()
```

## Key API Reference

```python
from interlace import test_model, test_model_sync, mock_dependency, TestResult

# Create a mock ibis.Table from Python data
table = mock_dependency(data, fields=None, strict=False)

# Test a model synchronously (no event loop needed)
result = test_model_sync(func, deps={"name": data}, fields=None)

# Test a model asynchronously (use with asyncio_mode = "auto")
result = await test_model(func, deps={"name": data}, fields=None)

# TestResult properties
result.status      # "success" or "error"
result.error       # Error message (if status == "error")
result.table       # ibis.Table (raw result)
result.row_count   # Number of rows
result.columns     # List of column names
result.rows        # List of dicts
result.df          # pandas DataFrame
result.duration    # Execution time in seconds
```

## Next Steps

- See the [API reference](https://interlace.sh/docs/reference/api) for full `test_model` and `run()` documentation
- Try the [quickstart](../quickstart/) example for a basic pipeline walkthrough
