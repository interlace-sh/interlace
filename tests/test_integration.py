"""
Integration tests for Interlace with DuckDB test database.

These tests verify end-to-end functionality including:
- Model execution with dependencies
- Strategy execution with real database
- Materialization types (table, view, ephemeral)
- Schema evolution
- Field definitions
"""

import pytest
import tempfile
import os
from pathlib import Path
import ibis
from interlace import model
from interlace.core.executor import Executor
from interlace.core.dependencies import build_dependency_graph
from interlace.utils.logging import setup_logging


@pytest.fixture
def temp_db():
    """Create a temporary DuckDB database for testing."""
    # Create a temp file path but don't create the file
    # DuckDB will create the database file itself
    fd, path = tempfile.mkstemp(suffix=".duckdb")
    os.close(fd)
    # Delete the empty file so DuckDB can create it
    if os.path.exists(path):
        os.unlink(path)
    yield path
    # Cleanup
    if os.path.exists(path):
        os.unlink(path)


@pytest.fixture
def test_config(temp_db):
    """Create test configuration with DuckDB connection."""
    return {
        "name": "Test Project",
        "connections": {
            "default": {
                "type": "duckdb",
                "path": temp_db,
            }
        },
        "executor": {
            "max_workers": 2,
        },
    }


@pytest.fixture
def executor(test_config):
    """Create executor instance for testing."""
    setup_logging(level="WARNING")  # Reduce noise in tests
    return Executor(test_config)


class TestModelExecution:
    """Test basic model execution flow."""

    @pytest.mark.asyncio
    async def test_simple_model_execution(self, executor, temp_db):
        """Test executing a simple model."""
        @model(name="users", materialise="table")
        def users():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        models = {
            "users": {
                "name": "users",
                "function": users,
                "schema": "public",
                "materialise": "table",
                "connection": "default",
            }
        }
        graph = build_dependency_graph(models)

        results = await executor.execute_dynamic(models, graph, force=True)

        assert "users" in results
        assert results["users"]["status"] == "success"

        # Verify table was created
        conn = ibis.duckdb.connect(temp_db)
        table = conn.table("users", database="public")
        df = table.execute()
        assert len(df) == 2
        assert "id" in df.columns
        assert "name" in df.columns

    @pytest.mark.asyncio
    async def test_model_with_dependencies(self, executor, temp_db):
        """Test model execution with dependencies."""
        @model(name="users", materialise="table")
        def users():
            return [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        @model(name="user_orders", materialise="table", dependencies=["users"])
        def user_orders(users: ibis.Table):
            return users.select(["id", "name"]).mutate(order_count=ibis.literal(0))

        models = {
            "users": {
                "name": "users",
                "function": users,
                "schema": "public",
                "materialise": "table",
                "connection": "default",
            },
            "user_orders": {
                "name": "user_orders",
                "function": user_orders,
                "schema": "public",
                "materialise": "table",
                "dependencies": ["users"],
                "connection": "default",
            },
        }
        graph = build_dependency_graph(models)

        results = await executor.execute_dynamic(models, graph, force=True)

        assert "users" in results
        assert "user_orders" in results
        assert results["users"]["status"] == "success"
        assert results["user_orders"]["status"] == "success"

        # Verify both tables exist
        conn = ibis.duckdb.connect(temp_db)
        users_table = conn.table("users", database="public")
        orders_table = conn.table("user_orders", database="public")
        assert len(users_table.execute()) == 2
        assert len(orders_table.execute()) == 2

    @pytest.mark.asyncio
    async def test_model_with_fields(self, executor, temp_db):
        """Test model execution with explicit fields definition."""
        @model(name="products", materialise="table", fields={"id": "int64", "price": "float64"})
        def products():
            return [{"id": 1, "name": "Product 1", "price": 10.50}]

        models = {
            "products": {
                "name": "products",
                "function": products,
                "schema": "public",
                "materialise": "table",
                "fields": {"id": "int64", "price": "float64"},
                "connection": "default",
            }
        }
        graph = build_dependency_graph(models)

        results = await executor.execute_dynamic(models, graph, force=True)

        assert "products" in results
        assert results["products"]["status"] == "success"

        # Verify table was created with correct schema
        conn = ibis.duckdb.connect(temp_db)
        table = conn.table("products", database="public")
        schema = table.schema()
        assert "id" in schema
        assert "price" in schema
        assert "name" in schema  # Fields add/replace, don't restrict


class TestStrategies:
    """Test strategy execution with real database."""

    @pytest.mark.asyncio
    async def test_merge_by_key_strategy(self, executor, temp_db):
        """Test merge_by_key strategy with single key."""
        @model(name="accounts", materialise="table", strategy="merge_by_key", primary_key="id")
        def accounts():
            return [{"id": 1, "name": "Account 1", "balance": 100.0}]

        models = {
            "accounts": {
                "name": "accounts",
                "function": accounts,
                "schema": "public",
                "materialise": "table",
                "strategy": "merge_by_key",
                "primary_key": "id",
                "connection": "default",
            }
        }
        graph = build_dependency_graph(models)

        # First run
        results1 = await executor.execute_dynamic(models, graph, force=True)
        assert results1["accounts"]["status"] == "success"

        # Second run with updated data
        @model(name="accounts", materialise="table", strategy="merge_by_key", primary_key="id")
        def accounts_updated():
            return [{"id": 1, "name": "Account 1 Updated", "balance": 150.0}]

        models["accounts"]["function"] = accounts_updated
        results2 = await executor.execute_dynamic(models, graph, force=True)
        assert results2["accounts"]["status"] == "success"

        # Verify merge worked
        conn = ibis.duckdb.connect(temp_db)
        table = conn.table("accounts", database="public")
        df = table.execute()
        assert len(df) == 1
        assert df["name"].iloc[0] == "Account 1 Updated"
        assert df["balance"].iloc[0] == 150.0

    @pytest.mark.asyncio
    async def test_append_strategy(self, executor, temp_db):
        """Test append strategy."""
        @model(name="events", materialise="table", strategy="append")
        def events():
            return [{"id": 1, "event": "start"}]

        models = {
            "events": {
                "name": "events",
                "function": events,
                "schema": "public",
                "materialise": "table",
                "strategy": "append",
                "connection": "default",
            }
        }
        graph = build_dependency_graph(models)

        # First run
        results1 = await executor.execute_dynamic(models, graph, force=True)
        assert results1["events"]["status"] == "success"

        # Second run
        @model(name="events", materialise="table", strategy="append")
        def events_updated():
            return [{"id": 2, "event": "end"}]

        models["events"]["function"] = events_updated
        results2 = await executor.execute_dynamic(models, graph, force=True)
        assert results2["events"]["status"] == "success"

        # Verify append worked
        conn = ibis.duckdb.connect(temp_db)
        table = conn.table("events", database="public")
        df = table.execute()
        assert len(df) == 2

    @pytest.mark.asyncio
    async def test_replace_strategy(self, executor, temp_db):
        """Test replace strategy."""
        @model(name="config", materialise="table", strategy="replace")
        def config():
            return [{"key": "version", "value": "1.0"}]

        models = {
            "config": {
                "name": "config",
                "function": config,
                "schema": "public",
                "materialise": "table",
                "strategy": "replace",
                "connection": "default",
            }
        }
        graph = build_dependency_graph(models)

        # First run
        results1 = await executor.execute_dynamic(models, graph, force=True)
        assert results1["config"]["status"] == "success"

        # Second run with different data
        @model(name="config", materialise="table", strategy="replace")
        def config_updated():
            return [{"key": "version", "value": "2.0"}]

        models["config"]["function"] = config_updated
        results2 = await executor.execute_dynamic(models, graph, force=True)
        assert results2["config"]["status"] == "success"

        # Verify replace worked
        conn = ibis.duckdb.connect(temp_db)
        table = conn.table("config", database="public")
        df = table.execute()
        assert len(df) == 1
        assert df["value"].iloc[0] == "2.0"


class TestMaterialization:
    """Test different materialization types."""

    @pytest.mark.asyncio
    async def test_table_materialization(self, executor, temp_db):
        """Test table materialization."""
        @model(name="table_model", materialise="table")
        def table_model():
            return [{"id": 1, "data": "test"}]

        models = {
            "table_model": {
                "name": "table_model",
                "function": table_model,
                "schema": "public",
                "materialise": "table",
                "connection": "default",
            }
        }
        graph = build_dependency_graph(models)

        results = await executor.execute_dynamic(models, graph, force=True)
        assert results["table_model"]["status"] == "success"

        # Verify table exists
        conn = ibis.duckdb.connect(temp_db)
        table = conn.table("table_model", database="public")
        assert len(table.execute()) == 1

    @pytest.mark.asyncio
    async def test_view_materialization(self, executor, temp_db):
        """Test view materialization."""
        @model(name="base_table", materialise="table")
        def base_table():
            return [{"id": 1, "value": 10}]

        @model(name="view_model", materialise="view", dependencies=["base_table"])
        def view_model(base_table: ibis.Table):
            return base_table.mutate(doubled=base_table.value * 2)

        models = {
            "base_table": {
                "name": "base_table",
                "function": base_table,
                "schema": "public",
                "materialise": "table",
                "connection": "default",
            },
            "view_model": {
                "name": "view_model",
                "function": view_model,
                "schema": "public",
                "materialise": "view",
                "dependencies": ["base_table"],
                "connection": "default",
            },
        }
        graph = build_dependency_graph(models)

        results = await executor.execute_dynamic(models, graph, force=True)
        assert results["base_table"]["status"] == "success"
        assert results["view_model"]["status"] == "success"

        # Verify view exists and works
        conn = ibis.duckdb.connect(temp_db)
        view = conn.table("view_model", database="public")
        df = view.execute()
        assert len(df) == 1
        assert "doubled" in df.columns
        assert df["doubled"].iloc[0] == 20

    @pytest.mark.asyncio
    async def test_ephemeral_materialization(self, executor, temp_db):
        """Test ephemeral materialization."""
        @model(name="ephemeral_model", materialise="ephemeral")
        def ephemeral_model():
            return [{"id": 1, "temp": "data"}]

        @model(name="downstream", materialise="table", dependencies=["ephemeral_model"])
        def downstream(ephemeral_model: ibis.Table):
            return ephemeral_model

        models = {
            "ephemeral_model": {
                "name": "ephemeral_model",
                "function": ephemeral_model,
                "schema": "public",
                "materialise": "ephemeral",
                "connection": "default",
            },
            "downstream": {
                "name": "downstream",
                "function": downstream,
                "schema": "public",
                "materialise": "table",
                "dependencies": ["ephemeral_model"],
                "connection": "default",
            },
        }
        graph = build_dependency_graph(models)

        results = await executor.execute_dynamic(models, graph, force=True)
        assert results["ephemeral_model"]["status"] == "success"
        assert results["downstream"]["status"] == "success"

        # Ephemeral table should not exist, but downstream should
        conn = ibis.duckdb.connect(temp_db)
        downstream_table = conn.table("downstream", database="public")
        assert len(downstream_table.execute()) == 1


class TestSchemaEvolution:
    """Test schema evolution functionality."""

    @pytest.mark.asyncio
    async def test_add_column(self, executor, temp_db):
        """Test automatic column addition."""
        @model(name="schema_test", materialise="table")
        def schema_test():
            return [{"id": 1, "name": "Test"}]

        models = {
            "schema_test": {
                "name": "schema_test",
                "function": schema_test,
                "schema": "public",
                "materialise": "table",
                "connection": "default",
            }
        }
        graph = build_dependency_graph(models)

        # First run
        await executor.execute_dynamic(models, graph, force=True)

        # Second run with new column
        @model(name="schema_test", materialise="table")
        def schema_test_updated():
            return [{"id": 1, "name": "Test", "status": "active"}]

        models["schema_test"]["function"] = schema_test_updated
        await executor.execute_dynamic(models, graph, force=True)

        # Verify new column was added
        conn = ibis.duckdb.connect(temp_db)
        table = conn.table("schema_test", database="public")
        schema = table.schema()
        assert "id" in schema
        assert "name" in schema
        assert "status" in schema


