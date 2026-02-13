"""
Tests for state store and migration runner.
"""

import time
from datetime import UTC, datetime

import pytest

from interlace.core.flow import Flow, Task
from interlace.core.state import StateStore, _escape_sql_string, _sql_value


class TestSQLHelpers:
    """Tests for SQL helper functions."""

    def test_escape_sql_string(self):
        assert _escape_sql_string("hello") == "hello"
        assert _escape_sql_string("it's") == "it''s"
        assert _escape_sql_string("a'b'c") == "a''b''c"

    def test_sql_value_none(self):
        assert _sql_value(None) == "NULL"

    def test_sql_value_int(self):
        assert _sql_value(42) == "42"

    def test_sql_value_float(self):
        assert _sql_value(3.14) == "3.14"

    def test_sql_value_bool(self):
        assert _sql_value(True) == "1"
        assert _sql_value(False) == "0"

    def test_sql_value_string(self):
        assert _sql_value("hello") == "'hello'"
        assert _sql_value("it's") == "'it''s'"

    def test_sql_value_datetime(self):
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = _sql_value(dt)
        assert result.startswith("TIMESTAMP '")
        assert "2024-01-15" in result


class TestStateStoreInit:
    """Tests for StateStore initialization."""

    def test_init_no_state_config(self):
        """StateStore initializes without state config."""
        store = StateStore({"connections": {}})
        assert store.state_db_name is None
        assert store._connection is None

    def test_init_with_state_config(self):
        """StateStore picks up connection name from config."""
        config = {"state": {"connection": "default"}}
        store = StateStore(config)
        assert store.state_db_name == "default"

    def test_get_connection_returns_none_when_no_db_name(self):
        store = StateStore({})
        assert store._get_connection() is None


class TestStateStoreCursor:
    """Tests for cursor state CRUD."""

    @pytest.fixture
    def store_with_duckdb(self, tmp_path):
        """Create a state store backed by an in-memory DuckDB for testing."""
        import ibis

        con = ibis.duckdb.connect()

        store = StateStore({"state": {"connection": "test"}})
        store._connection = con
        store._initialized = False
        store._initialize_schema()
        return store

    def test_cursor_round_trip(self, store_with_duckdb):
        store = store_with_duckdb
        # Initially no cursor value
        assert store.get_cursor_value("my_model") is None

        # Save and retrieve
        store.save_cursor_value("my_model", "event_id", "12345")
        assert store.get_cursor_value("my_model") == "12345"

        # Update
        store.save_cursor_value("my_model", "event_id", "67890")
        assert store.get_cursor_value("my_model") == "67890"

    def test_delete_cursor(self, store_with_duckdb):
        store = store_with_duckdb
        store.save_cursor_value("m1", "col", "100")
        assert store.get_cursor_value("m1") is not None

        store.delete_cursor_value("m1")
        assert store.get_cursor_value("m1") is None

    def test_cursor_special_chars(self, store_with_duckdb):
        """Cursor values with special characters are escaped properly."""
        store = store_with_duckdb
        store.save_cursor_value("model_x", "ts", "2024-01-15T10:30:00")
        assert store.get_cursor_value("model_x") == "2024-01-15T10:30:00"


class TestStateStoreFlowTask:
    """Tests for flow and task persistence."""

    @pytest.fixture
    def store_with_duckdb(self):
        import ibis

        con = ibis.duckdb.connect()
        store = StateStore({"state": {"connection": "test"}})
        store._connection = con
        store._initialized = False
        store._initialize_schema()
        return store

    def test_save_and_query_flow(self, store_with_duckdb):
        store = store_with_duckdb
        flow = Flow(trigger_type="cli")
        flow.started_at = time.time()
        flow.complete(success=True)

        store.save_flow(flow)

        # Verify persisted
        conn = store._get_connection()
        result = conn.sql(f"SELECT * FROM interlace.flows WHERE flow_id = '{flow.flow_id}'").execute()
        assert len(result) == 1
        assert result.iloc[0]["status"] == "completed"

    def test_save_and_query_task(self, store_with_duckdb):
        store = store_with_duckdb
        flow = Flow(trigger_type="api")
        task = Task(
            flow_id=flow.flow_id,
            model_name="test_model",
            schema_name="public",
            materialise="table",
            strategy="replace",
            dependencies=["dep_a"],
        )
        task.start()
        task.rows_processed = 100
        task.complete(success=True)

        store.save_task(task)

        conn = store._get_connection()
        result = conn.sql(f"SELECT * FROM interlace.tasks WHERE task_id = '{task.task_id}'").execute()
        assert len(result) == 1
        row = result.iloc[0]
        assert row["model_name"] == "test_model"
        assert row["status"] == "completed"
        assert row["rows_processed"] == 100


class TestStateStoreLineage:
    """Tests for column lineage storage."""

    @pytest.fixture
    def store_with_duckdb(self):
        import ibis

        con = ibis.duckdb.connect()
        store = StateStore({"state": {"connection": "test"}})
        store._connection = con
        store._initialized = False
        store._initialize_schema()
        return store

    def test_save_and_get_lineage(self, store_with_duckdb):
        store = store_with_duckdb
        store.save_column_lineage(
            output_model="orders",
            output_column="total",
            source_model="raw_orders",
            source_column="amount",
            transformation_type="direct",
        )

        lineage = store.get_column_lineage("orders")
        assert len(lineage) == 1
        assert lineage[0]["source_model"] == "raw_orders"

    def test_save_model_column(self, store_with_duckdb):
        store = store_with_duckdb
        store.save_model_column("users", "id", data_type="BIGINT", is_primary_key=True)
        store.save_model_column("users", "name", data_type="VARCHAR")

        cols = store.get_model_columns("users")
        assert len(cols) == 2
        names = {c["column_name"] for c in cols}
        assert names == {"id", "name"}

    def test_clear_lineage(self, store_with_duckdb):
        store = store_with_duckdb
        store.save_column_lineage("m1", "col1", "src", "scol", "direct")
        store.save_model_column("m1", "col1", "VARCHAR")

        store.clear_model_lineage("m1")

        assert store.get_column_lineage("m1") == []
        assert store.get_model_columns("m1") == []


class TestStateStoreScheduler:
    """Tests for scheduler persistence (last_run_at)."""

    @pytest.fixture
    def store_with_duckdb(self):
        import ibis

        con = ibis.duckdb.connect()
        store = StateStore({"state": {"connection": "test"}})
        store._connection = con
        store._initialized = False
        store._initialize_schema()
        return store

    def test_model_last_run_at_round_trip(self, store_with_duckdb):
        store = store_with_duckdb
        assert store.get_model_last_run_at("my_model") is None

        now = datetime.now(tz=UTC).replace(tzinfo=None)
        store.set_model_last_run_at("my_model", run_at=now)

        last_run = store.get_model_last_run_at("my_model")
        assert last_run is not None
        # Compare with tolerance (timestamp precision)
        assert abs((last_run - now).total_seconds()) < 2

    def test_model_last_run_at_update(self, store_with_duckdb):
        store = store_with_duckdb
        t1 = datetime(2024, 1, 1, 12, 0, 0)
        t2 = datetime(2024, 6, 15, 18, 30, 0)

        store.set_model_last_run_at("m", run_at=t1)
        store.set_model_last_run_at("m", run_at=t2)

        last_run = store.get_model_last_run_at("m")
        assert last_run is not None
        assert last_run.year == 2024
        assert last_run.month == 6
