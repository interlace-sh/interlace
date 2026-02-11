"""
Testing utilities for Interlace models.

Provides lightweight helpers to unit-test individual models with mock data,
without needing a project config.yaml or database connection.

Usage:
    from interlace import model, test_model_sync, mock_dependency

    @model(name="enriched", materialise="table")
    def enriched(users, orders):
        return users.join(orders, users.id == orders.user_id)

    result = test_model_sync(enriched, deps={
        "users": [{"id": 1, "name": "Alice"}],
        "orders": [{"id": 1, "user_id": 1, "amount": 42.0}],
    })
    assert result.status == "success"
    assert result.row_count == 1
"""

import asyncio
import inspect
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

import ibis

from interlace.core.execution.data_converter import DataConverter


@dataclass
class TestResult:
    """Result of a model test execution."""

    table: Optional[ibis.Table] = None
    status: str = "success"
    error: Optional[str] = None
    duration: float = 0.0

    @property
    def row_count(self) -> Optional[int]:
        """Number of rows in the result table."""
        if self.table is None:
            return None
        try:
            return self.table.count().execute()
        except Exception:
            return None

    @property
    def columns(self) -> List[str]:
        """Column names from the result table."""
        if self.table is None:
            return []
        try:
            return list(self.table.columns)
        except Exception:
            return []

    @property
    def df(self):
        """Execute the result and return a pandas DataFrame."""
        if self.table is None:
            return None
        return self.table.execute()

    @property
    def rows(self) -> List[Dict[str, Any]]:
        """Execute the result and return a list of dicts."""
        df = self.df
        if df is None:
            return []
        return df.to_dict("records")


def mock_dependency(
    data: Any,
    fields: Optional[Dict[str, Any]] = None,
    strict: bool = False,
) -> ibis.Table:
    """
    Create a mock ibis.Table from Python data for use as a model dependency.

    Args:
        data: Input data - list of dicts, dict of lists, pandas DataFrame, etc.
        fields: Optional schema definition (e.g. {"id": "int64", "name": "string"}).
        strict: If True, only include columns specified in fields.

    Returns:
        ibis.Table suitable for passing as a model dependency.
    """
    return DataConverter.convert_to_ibis_table(data, fields=fields, strict=strict)


async def test_model(
    func: Any,
    deps: Optional[Dict[str, Any]] = None,
    fields: Optional[Dict[str, Dict[str, Any]]] = None,
) -> TestResult:
    """
    Test a single model function with mock dependencies (async version).

    Args:
        func: Model function (decorated with @model or plain function).
        deps: Dict mapping dependency name to data. Values can be:
            - list of dicts: [{"id": 1, "name": "Alice"}]
            - dict of lists: {"id": [1, 2], "name": ["Alice", "Bob"]}
            - pandas DataFrame
            - ibis.Table (passed through directly)
        fields: Optional per-dependency schema overrides:
            {"users": {"id": "int64", "name": "string"}}

    Returns:
        TestResult with table, status, error, and duration.
    """
    start = time.time()

    try:
        # Convert dependencies to ibis.Table
        dep_tables: Dict[str, ibis.Table] = {}
        for dep_name, dep_data in (deps or {}).items():
            if isinstance(dep_data, ibis.Table):
                dep_tables[dep_name] = dep_data
            else:
                dep_fields = (fields or {}).get(dep_name)
                dep_tables[dep_name] = DataConverter.convert_to_ibis_table(
                    dep_data, fields=dep_fields
                )

        # Unwrap @model decorator if present
        actual_func = func
        if hasattr(func, "_interlace_model"):
            actual_func = func._interlace_model.get("function", func)

        # Call model function
        if inspect.iscoroutinefunction(actual_func):
            result = await actual_func(**dep_tables)
        else:
            result = actual_func(**dep_tables)

        # Convert result to ibis.Table if needed
        if result is None:
            return TestResult(
                table=None,
                status="success",
                duration=time.time() - start,
            )

        if not isinstance(result, ibis.Table):
            result = DataConverter.convert_to_ibis_table(result)

        return TestResult(
            table=result,
            status="success",
            duration=time.time() - start,
        )

    except Exception as e:
        return TestResult(
            table=None,
            status="error",
            error=str(e),
            duration=time.time() - start,
        )


def test_model_sync(
    func: Any,
    deps: Optional[Dict[str, Any]] = None,
    fields: Optional[Dict[str, Dict[str, Any]]] = None,
) -> TestResult:
    """
    Test a single model function with mock dependencies (sync version).

    See test_model() for parameter documentation.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # Already inside an event loop - create a new one in a thread
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(
                asyncio.run, test_model(func, deps=deps, fields=fields)
            ).result()
    else:
        return asyncio.run(test_model(func, deps=deps, fields=fields))


# Prevent pytest from collecting these as test functions
test_model.__test__ = False  # type: ignore[attr-defined]
test_model_sync.__test__ = False  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Pytest fixtures (auto-registered via pytest11 entry point)
# ---------------------------------------------------------------------------

try:
    import pytest

    @pytest.fixture
    def interlace_test_db():
        """Provide an in-memory DuckDB connection via ibis for testing."""
        conn = ibis.duckdb.connect()
        yield conn
        # DuckDB in-memory connections are cleaned up automatically

    @pytest.fixture
    def interlace_mock():
        """Provide the mock_dependency helper as a fixture."""
        return mock_dependency

except ImportError:
    # pytest not installed - fixtures are not registered
    pass
