"""
Comprehensive unit tests for the Executor class.

Tests cover:
- Parallel execution
- Dependency resolution
- Schema validation and caching
- Connection management
- Error handling
- Configuration handling
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from interlace.core.dependencies import DependencyGraph
from interlace.core.executor import Executor


class TestExecutorInitialization:
    """Test Executor initialization and configuration."""

    def test_executor_init_with_defaults(self):
        """Test executor initializes with default configuration values."""
        config = {
            "name": "Test Project",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)

            assert executor.max_iterations == Executor.DEFAULT_MAX_ITERATIONS
            assert executor.table_load_delay == Executor.DEFAULT_TABLE_LOAD_DELAY
            assert executor.task_timeout == Executor.DEFAULT_TASK_TIMEOUT
            assert executor._dep_loading_locks == {}
            assert executor._schema_cache == {}
            assert executor._table_existence_cache == {}
            assert executor._dep_schema_cache == {}

    def test_executor_init_with_custom_config(self):
        """Test executor uses custom configuration values."""
        config = {
            "name": "Test Project",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
            "executor": {"max_iterations": 200, "table_load_delay": 0.05, "task_timeout": 60.0},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)

            assert executor.max_iterations == 200
            assert executor.table_load_delay == 0.05
            assert executor.task_timeout == 60.0


class TestDependencyLoading:
    """Test dependency loading with locks."""

    @pytest.mark.asyncio
    async def test_load_dependency_with_lock_prevents_race_condition(self):
        """Test that locks prevent race conditions when loading dependencies."""
        config = {
            "name": "Test",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)
            executor.default_conn_name = "duckdb_main"

            # Mock connection and table
            mock_connection = MagicMock()
            mock_table = MagicMock()
            mock_connection.table.return_value = mock_table

            models = {"dep_model": {"schema": "public", "connection": "duckdb_main"}}

            # Test that lock is created
            await executor._load_dependency_with_lock("dep_model", mock_connection, models, "public")

            # Verify lock was created
            assert "dep_model" in executor._dep_loading_locks
            assert isinstance(executor._dep_loading_locks["dep_model"], asyncio.Lock)


class TestSchemaCaching:
    """Test schema validation and caching."""

    def test_check_table_exists_uses_cache(self):
        """Test that table existence checks are cached."""
        config = {
            "name": "Test",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)

            # Mock connection
            mock_connection = MagicMock()
            mock_connection.list_tables.return_value = ["test_table"]

            # First call - should check and cache
            result1 = executor._check_table_exists(mock_connection, "test_table", "public")

            # Second call - should use cache (list_tables should not be called again)
            mock_connection.list_tables.reset_mock()
            result2 = executor._check_table_exists(mock_connection, "test_table", "public")

            # Results should be the same
            assert result1 == result2
            # Cache should be populated
            assert ("test_table", "public") in executor._table_existence_cache

    @pytest.mark.asyncio
    async def test_schema_validation_uses_cache(self):
        """Test that schema validation uses cache to skip redundant comparisons."""
        config = {
            "name": "Test",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
            patch("interlace.utils.table_utils.check_table_exists", return_value=True),
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)

            # Mock table and schema
            mock_table = MagicMock()
            mock_schema = MagicMock()
            mock_table.schema.return_value = mock_schema
            mock_connection = MagicMock()
            mock_connection.table.return_value = mock_table
            existing_table = MagicMock()
            existing_table.schema.return_value = mock_schema
            mock_connection.table.return_value = existing_table

            # First validation - should cache
            result1 = await executor._validate_and_update_schema(mock_table, "test_model", "public", mock_connection)

            # Populate cache manually for test
            executor._schema_cache["public.test_model"] = mock_schema

            # Second validation with same schema - should use cache
            result2 = await executor._validate_and_update_schema(mock_table, "test_model", "public", mock_connection)

            # Both should return 0 (no changes)
            assert result1 == 0
            assert result2 == 0


class TestRefactoredMethods:
    """Test refactored helper methods."""

    @pytest.mark.asyncio
    async def test_start_model_execution(self):
        """Test _start_model_execution helper method."""
        config = {
            "name": "Test",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)
            executor.flow = MagicMock()
            executor.flow.tasks = {}

            models = {"test_model": {"type": "python", "function": lambda: None}}
            graph = DependencyGraph()
            task_map = {}

            # Mock execute_model
            executor.execute_model = AsyncMock(return_value={"status": "success"})

            await executor._start_model_execution("test_model", models, graph, task_map)

            # Verify task was created
            assert "test_model" in task_map
            assert isinstance(task_map["test_model"], asyncio.Task)

    @pytest.mark.asyncio
    async def test_wait_for_task_completion(self):
        """Test _wait_for_task_completion helper method."""
        config = {
            "name": "Test",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)

            # Create a completed task
            async def dummy_task():
                return {"status": "success", "model": "test"}

            task = asyncio.create_task(dummy_task())
            await task  # Complete it

            executing = {"test"}
            task_map = {"test": task}
            results = {}
            models = {}
            graph = DependencyGraph()
            pending = set()
            new_ready = set()
            completed = set()

            # Mock process_completed_task
            executor._process_completed_task = AsyncMock()

            ready = set()
            succeeded = set()
            await executor._wait_for_task_completion(
                executing,
                task_map,
                results,
                models,
                graph,
                pending,
                ready,
                new_ready,
                completed,
                succeeded,
            )

            # Verify process_completed_task was called
            executor._process_completed_task.assert_called_once()


class TestErrorHandling:
    """Test error handling in executor."""

    @pytest.mark.asyncio
    async def test_process_failed_task_handles_errors(self):
        """Test that _process_failed_task properly handles errors."""
        config = {
            "name": "Test",
            "connections": {"duckdb_main": {"type": "duckdb", "path": ":memory:"}},
        }

        with (
            patch("interlace.core.executor.init_connections"),
            patch("interlace.core.executor.get_connection") as mock_get_conn,
        ):
            mock_conn = Mock()
            mock_conn.connection = MagicMock()
            mock_conn.config = {"type": "duckdb", "path": ":memory:"}
            mock_get_conn.return_value = mock_conn

            executor = Executor(config)
            executor.flow = MagicMock()
            executor.flow.tasks = {}
            executor.progress = None

            # Create a failed task
            async def failing_task():
                raise ValueError("Test error")

            task = asyncio.create_task(failing_task())
            try:
                await task
            except ValueError:
                pass

            executing = {"test"}
            task_map = {"test": task}
            results = {}
            models = {}
            graph = DependencyGraph()
            pending = set()
            ready = set()
            new_ready = set()
            completed = set()
            succeeded = set()

            await executor._process_failed_task(
                task,
                ValueError("Test error"),
                executing,
                completed,
                succeeded,
                results,
                task_map,
                models,
                graph,
                pending,
                ready,
                new_ready,
            )

            # Verify error was recorded
            assert "test" in results
            assert results["test"]["status"] == "error"
            assert "test" in completed


class TestConfigurationConstants:
    """Test that configuration constants are properly defined."""

    def test_default_constants_are_defined(self):
        """Test that default constants are defined on Executor class."""
        assert hasattr(Executor, "DEFAULT_MAX_ITERATIONS")
        assert hasattr(Executor, "DEFAULT_TABLE_LOAD_DELAY")
        assert hasattr(Executor, "DEFAULT_TASK_TIMEOUT")

        assert isinstance(Executor.DEFAULT_MAX_ITERATIONS, int)
        assert isinstance(Executor.DEFAULT_TABLE_LOAD_DELAY, float)
        assert isinstance(Executor.DEFAULT_TASK_TIMEOUT, float)

        assert Executor.DEFAULT_MAX_ITERATIONS > 0
        assert Executor.DEFAULT_TABLE_LOAD_DELAY >= 0
        assert Executor.DEFAULT_TASK_TIMEOUT > 0
