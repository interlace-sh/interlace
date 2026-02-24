"""
State database for storing flows and tasks.

Automatically creates interlace schema and tables if they don't exist.
"""

import json
from datetime import datetime
from typing import Any

import ibis

from interlace.connections.manager import get_connection
from interlace.core.context import _execute_sql_internal
from interlace.core.flow import Flow, Task
from interlace.utils.logging import get_logger

logger = get_logger("interlace.state")


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for SQL strings."""
    return value.replace("'", "''")


def _sql_value(value: Any) -> str:
    """Convert Python value to SQL string representation."""
    if value is None:
        return "NULL"
    # bool must be checked before int/float because ``isinstance(True, int)``
    # is True in Python (bool is a subclass of int).
    elif isinstance(value, bool):
        return "1" if value else "0"
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, str):
        escaped = _escape_sql_string(value)
        return f"'{escaped}'"
    elif isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat()}'"
    else:
        escaped = _escape_sql_string(str(value))
        return f"'{escaped}'"


class StateStore:
    """Manages state storage in database for flows and tasks."""

    def __init__(self, config: dict[str, Any]):
        """
        Initialize state store.

        Args:
            config: Configuration dictionary with state_db connection name
        """
        self.config = config
        self.state_db_name = config.get("state", {}).get("connection")
        self._connection: ibis.BaseBackend | None = None
        self._initialized = False

    def _get_connection(self) -> ibis.BaseBackend:
        """Get state database connection."""
        if self._connection is None:
            if not self.state_db_name:
                return None
            try:
                # get_connection returns a connection wrapper (e.g., DuckDBConnection)
                # We need to access the .connection property to get the actual ibis backend
                conn_wrapper = get_connection(self.state_db_name)
                self._connection = conn_wrapper.connection
                self._initialize_schema()
            except Exception as e:
                logger.warning(f"Failed to initialize state database: {e}")
                return None
        return self._connection

    def _initialize_schema(self) -> None:
        """Create interlace schema and tables if they don't exist."""
        if self._initialized:
            return

        conn = self._get_connection()
        if conn is None:
            return

        schema_name = "interlace"

        # Create schema
        try:
            _execute_sql_internal(conn, f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        except Exception as e:
            logger.warning(f"Could not create schema {schema_name}: {e}")

        # Create flows table
        flows_table = f"{schema_name}.flows"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {flows_table} (
                    flow_id VARCHAR PRIMARY KEY,
                    trigger_type VARCHAR,
                    trigger_id VARCHAR,
                    status VARCHAR,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    created_by VARCHAR,
                    metadata JSON,
                    duration_seconds DOUBLE,
                    model_selection VARCHAR,
                    models_total INTEGER,
                    models_succeeded INTEGER,
                    models_failed INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
        except Exception as e:
            logger.warning(f"Could not create flows table: {e}")

        # Create tasks table
        tasks_table = f"{schema_name}.tasks"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {tasks_table} (
                    task_id VARCHAR PRIMARY KEY,
                    flow_id VARCHAR,
                    model_name VARCHAR,
                    schema_name VARCHAR,
                    parent_task_ids VARCHAR,
                    status VARCHAR,
                    attempt INTEGER,
                    max_attempts INTEGER,
                    enqueued_at TIMESTAMP,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    materialise VARCHAR,
                    strategy VARCHAR,
                    dependencies VARCHAR,
                    rows_processed INTEGER,
                    rows_ingested INTEGER,
                    rows_inserted INTEGER,
                    rows_updated INTEGER,
                    rows_deleted INTEGER,
                    schema_changes INTEGER,
                    error_type VARCHAR,
                    error_message TEXT,
                    error_stacktrace TEXT,
                    skipped_reason VARCHAR,
                    metadata JSON,
                    duration_seconds DOUBLE,
                    wait_time_seconds DOUBLE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
        except Exception as e:
            logger.warning(f"Could not create tasks table: {e}")

        # Create schema tracking tables
        self._initialize_schema_tracking_tables(conn, schema_name)

        # Create file sync manifest table (SFTP sync + processors)
        self._initialize_file_sync_tables(conn, schema_name)

        # Create dead letter queue table (retry framework)
        self._initialize_dlq_tables(conn, schema_name)

        # Create column lineage tables
        self._initialize_lineage_tables(conn, schema_name)

        # Create stream consumer tracking tables
        self._initialize_stream_tables(conn, schema_name)

        # Create cursor state table (automatic incremental processing)
        self._initialize_cursor_state_table(conn, schema_name)

        self._initialized = True
        # Log at debug level to avoid appearing before display header
        # This initialization happens before display context is entered
        logger.debug(f"State database initialized with schema '{schema_name}'")

    def _initialize_file_sync_tables(self, conn: ibis.BaseBackend, schema_name: str) -> None:
        """Create file sync manifest tables for ingestion jobs (SFTP->S3/filesystem)."""
        manifest_table = f"{schema_name}.file_sync_manifest"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {manifest_table} (
                    job_id VARCHAR NOT NULL,
                    remote_path VARCHAR NOT NULL,
                    remote_mtime BIGINT,
                    remote_size BIGINT,
                    remote_fingerprint VARCHAR, -- optional hash/etag if available
                    dest_uri VARCHAR,           -- e.g., s3://bucket/key or file:///path
                    status VARCHAR,             -- success|error
                    error_message TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (job_id, remote_path, remote_mtime, remote_size)
                )
                """,
            )
        except Exception as e:
            logger.debug(f"Could not create file_sync_manifest table (non-fatal): {e}")

    def _initialize_dlq_tables(self, conn: ibis.BaseBackend, schema_name: str) -> None:
        """Create dead letter queue table for tracking failed model executions."""
        dlq_table = f"{schema_name}.dead_letter_queue"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {dlq_table} (
                    dlq_id VARCHAR PRIMARY KEY,
                    model_name VARCHAR NOT NULL,
                    exception_type VARCHAR NOT NULL,
                    exception_message TEXT,
                    exception_traceback TEXT,
                    total_attempts INTEGER DEFAULT 1,
                    retry_history JSON,
                    first_attempt_time TIMESTAMP,
                    last_attempt_time TIMESTAMP,
                    total_duration DOUBLE DEFAULT 0.0,
                    model_config JSON,
                    model_dependencies JSON,
                    run_id VARCHAR,
                    environment VARCHAR,
                    resolved_at TIMESTAMP,
                    resolution_note TEXT,
                    is_resolved BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """,
            )
        except Exception as e:
            logger.debug(f"Could not create dead_letter_queue table (non-fatal): {e}")

    def _initialize_lineage_tables(self, conn: ibis.BaseBackend, schema_name: str) -> None:
        """Create column-level lineage tracking tables."""
        # Column lineage table - stores lineage edges between columns
        column_lineage_table = f"{schema_name}.column_lineage"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {column_lineage_table} (
                    output_model VARCHAR NOT NULL,
                    output_column VARCHAR NOT NULL,
                    source_model VARCHAR NOT NULL,
                    source_column VARCHAR NOT NULL,
                    transformation_type VARCHAR,
                    transformation_expression TEXT,
                    confidence DOUBLE DEFAULT 1.0,
                    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (output_model, output_column, source_model, source_column)
                )
                """,
            )
        except Exception as e:
            logger.debug(f"Could not create column_lineage table (non-fatal): {e}")

    def _initialize_stream_tables(self, conn: ibis.BaseBackend, schema_name: str) -> None:
        """Create stream consumer tracking tables."""
        # Stream consumers table - tracks cursor position per consumer per stream
        consumers_table = f"{schema_name}.stream_consumers"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {consumers_table} (
                    stream_name VARCHAR NOT NULL,
                    consumer_name VARCHAR NOT NULL,
                    last_cursor BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (stream_name, consumer_name)
                )
                """,
            )
        except Exception as e:
            logger.debug(f"Could not create stream_consumers table (non-fatal): {e}")

    def _initialize_cursor_state_table(self, conn: ibis.BaseBackend, schema_name: str) -> None:
        """Create cursor state table for automatic incremental processing.

        Used by models with the ``cursor`` parameter to track the last
        processed value so that only new rows are passed on subsequent runs.
        """
        cursor_table = f"{schema_name}.cursor_state"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {cursor_table} (
                    model_name VARCHAR NOT NULL,
                    schema_name VARCHAR NOT NULL DEFAULT 'public',
                    cursor_column VARCHAR NOT NULL DEFAULT 'rowid',
                    last_processed_value VARCHAR,
                    last_run_at TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (model_name, schema_name)
                )
                """,
            )
        except Exception as e:
            logger.debug(f"Could not create cursor_state table (non-fatal): {e}")

    # ------------------------------------------------------------------
    # Cursor state CRUD
    # ------------------------------------------------------------------

    def get_cursor_value(self, model_name: str, schema_name: str = "public") -> str | None:
        """Get the last processed cursor value for a model.

        Args:
            model_name: Name of the model
            schema_name: Schema name (default "public")

        Returns:
            The last processed cursor value as a string, or ``None`` if no
            cursor state exists for this model.
        """
        conn = self._get_connection()
        if conn is None:
            return None

        try:
            cursor_table = "interlace.cursor_state"
            safe_name = _escape_sql_string(model_name)
            safe_schema = _escape_sql_string(schema_name)
            result = conn.sql(
                f"SELECT last_processed_value FROM {cursor_table} "
                f"WHERE model_name = '{safe_name}' AND schema_name = '{safe_schema}'"
            ).execute()

            if result is not None and len(result) > 0:
                val = result.iloc[0, 0]
                return val if val is not None else None
            return None
        except Exception as e:
            logger.debug(f"Could not get cursor value for {model_name}: {e}")
            return None

    def delete_cursor_value(self, model_name: str, schema_name: str = "public") -> None:
        """Delete cursor state for a model, forcing full reprocessing next run.

        Args:
            model_name: Name of the model whose cursor state should be deleted.
            schema_name: Schema name (default "public")
        """
        conn = self._get_connection()
        if conn is None:
            return

        try:
            cursor_table = "interlace.cursor_state"
            safe_name = _escape_sql_string(model_name)
            safe_schema = _escape_sql_string(schema_name)
            _execute_sql_internal(
                conn,
                f"DELETE FROM {cursor_table} WHERE model_name = '{safe_name}' AND schema_name = '{safe_schema}'",
            )
        except Exception as e:
            logger.warning(f"Could not delete cursor value for {model_name}: {e}")

    def save_cursor_value(self, model_name: str, cursor_column: str, value: str, schema_name: str = "public") -> None:
        """Save the last processed cursor value for a model.

        Uses delete-then-insert for upsert portability across backends.

        Args:
            model_name: Name of the model
            cursor_column: Name of the cursor column (e.g. ``"event_id"``)
            value: The cursor value to persist (stringified)
            schema_name: Schema name (default "public")
        """
        conn = self._get_connection()
        if conn is None:
            return

        try:
            cursor_table = "interlace.cursor_state"
            safe_name = _escape_sql_string(model_name)
            safe_schema = _escape_sql_string(schema_name)
            safe_col = _escape_sql_string(cursor_column)
            safe_val = _escape_sql_string(str(value))

            # Delete existing row
            try:
                _execute_sql_internal(
                    conn,
                    f"DELETE FROM {cursor_table} WHERE model_name = '{safe_name}' AND schema_name = '{safe_schema}'",
                )
            except Exception:
                pass

            # Insert new row
            _execute_sql_internal(
                conn,
                f"INSERT INTO {cursor_table} "
                f"(model_name, schema_name, cursor_column, last_processed_value, last_run_at, updated_at) "
                f"VALUES ('{safe_name}', '{safe_schema}', '{safe_col}', '{safe_val}', "
                f"CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            )
        except Exception as e:
            logger.warning(f"Could not save cursor value for {model_name}: {e}")

    def _initialize_schema_tracking_tables(self, conn: ibis.BaseBackend, schema_name: str) -> None:
        """Create schema tracking tables for schema evolution."""
        # Schema history table (informative only, no rollback)
        schema_history_table = f"{schema_name}.schema_history"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {schema_history_table} (
                    model_name VARCHAR NOT NULL,
                    schema_name VARCHAR NOT NULL,
                    version INTEGER NOT NULL,
                    column_name VARCHAR NOT NULL,
                    column_type VARCHAR NOT NULL,
                    is_nullable BOOLEAN,
                    is_primary_key BOOLEAN,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (model_name, schema_name, version, column_name)
                )
                """,
            )
        except Exception as e:
            logger.warning(f"Could not create schema_history table: {e}")

        # Model file hash tracking
        model_file_hashes_table = f"{schema_name}.model_file_hashes"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {model_file_hashes_table} (
                    model_name VARCHAR NOT NULL,
                    schema_name VARCHAR NOT NULL,
                    file_path VARCHAR NOT NULL,
                    file_hash VARCHAR NOT NULL,
                    last_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (model_name, schema_name)
                )
                """,
            )
        except Exception as e:
            logger.warning(f"Could not create model_file_hashes table: {e}")

        # Migration execution tracking
        migration_runs_table = f"{schema_name}.migration_runs"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {migration_runs_table} (
                    migration_file VARCHAR NOT NULL,
                    environment VARCHAR NOT NULL,
                    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    executed_by VARCHAR,
                    success BOOLEAN DEFAULT TRUE,
                    error_message TEXT,
                    PRIMARY KEY (migration_file, environment)
                )
                """,
            )
        except Exception as e:
            logger.warning(f"Could not create migration_runs table: {e}")

        # Model metadata table — use CREATE TABLE IF NOT EXISTS + idempotent ALTER
        model_metadata_table = f"{schema_name}.model_metadata"
        try:
            _execute_sql_internal(
                conn,
                f"""
                CREATE TABLE IF NOT EXISTS {model_metadata_table} (
                    model_name VARCHAR NOT NULL,
                    schema_name VARCHAR NOT NULL,
                    materialize VARCHAR,
                    strategy VARCHAR,
                    primary_key VARCHAR,
                    dependencies TEXT[],
                    description TEXT,
                    tags TEXT[],
                    owner VARCHAR,
                    source_file VARCHAR,
                    source_type VARCHAR,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_run_at TIMESTAMP,
                    PRIMARY KEY (model_name, schema_name)
                )
                """,
            )
            # Idempotent column add for existing tables missing last_run_at
            try:
                _execute_sql_internal(conn, f"SELECT last_run_at FROM {model_metadata_table} LIMIT 0")
            except Exception:
                _execute_sql_internal(conn, f"ALTER TABLE {model_metadata_table} ADD COLUMN last_run_at TIMESTAMP")
        except Exception as e:
            # Non-fatal: state tracking is optional, execution can continue
            logger.debug(f"Could not create/update model_metadata table (non-fatal): {e}")

    def save_flow(self, flow: Flow) -> None:
        """Save or update flow in database."""
        conn = self._get_connection()
        if conn is None:
            return

        try:
            schema_name = "interlace"
            flows_table = f"{schema_name}.flows"

            # Compute summary counts from tasks
            summary = flow.get_summary()

            # Build row data
            row_data = {
                "flow_id": flow.flow_id,
                "trigger_type": flow.trigger_type,
                "trigger_id": flow.trigger_id,
                "status": flow.status.value,
                "started_at": (datetime.fromtimestamp(flow.started_at) if flow.started_at else None),
                "completed_at": (datetime.fromtimestamp(flow.completed_at) if flow.completed_at else None),
                "created_by": flow.created_by,
                "metadata": json.dumps(flow.metadata) if flow.metadata else None,
                "duration_seconds": flow.get_duration(),
                "model_selection": flow.model_selection,
                "models_total": summary["total_tasks"],
                "models_succeeded": summary["completed"],
                "models_failed": summary["failed"],
            }

            # Delete existing row if it exists (upsert behavior)
            try:
                _execute_sql_internal(
                    conn,
                    f"DELETE FROM {flows_table} WHERE flow_id = {_sql_value(flow.flow_id)}",
                )
            except Exception:
                pass

            # Insert using explicit column list to handle NULLs properly
            columns = list(row_data.keys())
            values = [_sql_value(row_data.get(col)) for col in columns]
            columns_str = ", ".join(columns)
            values_str = ", ".join(values)

            try:
                _execute_sql_internal(conn, f"INSERT INTO {flows_table} ({columns_str}) VALUES ({values_str})")
            except Exception as insert_error:
                # Log as warning - execution history may be lost
                logger.warning(
                    f"Failed to persist flow state for {flow.flow_id}: {insert_error}. "
                    f"Flow executed successfully but history may not be available."
                )

        except Exception as e:
            logger.error(f"Failed to save flow {flow.flow_id}: {e}", exc_info=True)

    def save_task(self, task: Task) -> None:
        """Save or update task in database."""
        conn = self._get_connection()
        if conn is None:
            return

        try:
            schema_name = "interlace"
            tasks_table = f"{schema_name}.tasks"

            # Build row data
            row_data = {
                "task_id": task.task_id,
                "flow_id": task.flow_id,
                "model_name": task.model_name,
                "schema_name": task.schema_name,
                "parent_task_ids": (json.dumps(task.parent_task_ids) if task.parent_task_ids else None),
                "status": task.status.value,
                "attempt": task.attempt,
                "max_attempts": task.max_attempts,
                "enqueued_at": (datetime.fromtimestamp(task.enqueued_at) if task.enqueued_at else None),
                "started_at": (datetime.fromtimestamp(task.started_at) if task.started_at else None),
                "completed_at": (datetime.fromtimestamp(task.completed_at) if task.completed_at else None),
                "materialise": task.materialise,
                "strategy": task.strategy,
                "dependencies": (json.dumps(task.dependencies) if task.dependencies else None),
                "rows_processed": task.rows_processed,
                "rows_ingested": task.rows_ingested,
                "rows_inserted": task.rows_inserted,
                "rows_updated": task.rows_updated,
                "rows_deleted": task.rows_deleted,
                "schema_changes": task.schema_changes,
                "error_type": task.error_type,
                "error_message": task.error_message,
                "error_stacktrace": task.error_stacktrace,
                "skipped_reason": task.skipped_reason,
                "metadata": json.dumps(task.metadata) if task.metadata else None,
                "duration_seconds": task.get_duration(),
                "wait_time_seconds": task.get_wait_time(),
            }

            # Delete existing row if it exists (upsert behavior)
            try:
                _execute_sql_internal(
                    conn,
                    f"DELETE FROM {tasks_table} WHERE task_id = {_sql_value(task.task_id)}",
                )
            except Exception:
                pass

            # Insert using explicit column list to handle NULLs properly
            # Build column list and values, handling NULLs
            columns = list(row_data.keys())
            values = [_sql_value(row_data.get(col)) for col in columns]
            columns_str = ", ".join(columns)
            values_str = ", ".join(values)

            try:
                _execute_sql_internal(conn, f"INSERT INTO {tasks_table} ({columns_str}) VALUES ({values_str})")
            except Exception as insert_error:
                # Log as debug instead of error to reduce console noise
                logger.debug(
                    f"Failed to insert task {task.task_id}: {insert_error}",
                    exc_info=True,
                )

        except Exception as e:
            logger.error(
                f"Failed to save task {task.task_id} (model: {task.model_name}): {e}",
                exc_info=True,
            )

    def save_column_lineage(
        self,
        output_model: str,
        output_column: str,
        source_model: str,
        source_column: str,
        transformation_type: str = "unknown",
        transformation_expression: str | None = None,
        confidence: float = 1.0,
    ) -> None:
        """
        Save a column lineage edge to the database.

        Args:
            output_model: Model that produces the output column
            output_column: Name of the output column
            source_model: Model that provides the source column
            source_column: Name of the source column
            transformation_type: Type of transformation (direct, derived, etc.)
            transformation_expression: Optional expression or SQL snippet
            confidence: Confidence score (0-1) for inferred lineage
        """
        conn = self._get_connection()
        if conn is None:
            return

        try:
            schema_name = "interlace"
            lineage_table = f"{schema_name}.column_lineage"

            # Delete existing edge if it exists (upsert behavior)
            try:
                _execute_sql_internal(
                    conn,
                    f"""DELETE FROM {lineage_table}
                        WHERE output_model = {_sql_value(output_model)}
                        AND output_column = {_sql_value(output_column)}
                        AND source_model = {_sql_value(source_model)}
                        AND source_column = {_sql_value(source_column)}""",
                )
            except Exception:
                pass

            # Insert the lineage edge
            _execute_sql_internal(
                conn,
                f"""INSERT INTO {lineage_table}
                    (output_model, output_column, source_model, source_column,
                     transformation_type, transformation_expression, confidence)
                    VALUES (
                        {_sql_value(output_model)},
                        {_sql_value(output_column)},
                        {_sql_value(source_model)},
                        {_sql_value(source_column)},
                        {_sql_value(transformation_type)},
                        {_sql_value(transformation_expression)},
                        {confidence}
                    )""",
            )
        except Exception as e:
            logger.debug(f"Failed to save column lineage: {e}")

    def get_column_lineage(self, model_name: str, column_name: str | None = None) -> list[Any]:
        """
        Get column lineage for a model.

        Args:
            model_name: Name of the model
            column_name: Optional specific column (if None, returns all columns)

        Returns:
            List of lineage edges as dictionaries
        """
        conn = self._get_connection()
        if conn is None:
            return []

        try:
            schema_name = "interlace"
            lineage_table = f"{schema_name}.column_lineage"

            if column_name:
                sql = f"""SELECT * FROM {lineage_table}
                         WHERE output_model = {_sql_value(model_name)}
                         AND output_column = {_sql_value(column_name)}"""
            else:
                sql = f"""SELECT * FROM {lineage_table}
                         WHERE output_model = {_sql_value(model_name)}"""

            result = _execute_sql_internal(conn, sql)
            if result is not None:
                # Convert to list of dicts
                if hasattr(result, "to_dict"):
                    return result.to_dict("records")  # type: ignore[no-any-return]
                elif hasattr(result, "fetchall"):
                    columns = [desc[0] for desc in result.description]
                    return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]
            return []
        except Exception as e:
            logger.debug(f"Failed to get column lineage: {e}")
            return []

    def get_model_columns(self, model_name: str) -> list[Any]:
        """
        Get column metadata for a model.

        Args:
            model_name: Name of the model

        Returns:
            List of column metadata as dictionaries
        """
        conn = self._get_connection()
        if conn is None:
            return []

        try:
            # Read from schema_history (populated during materialization) instead of
            # model_columns which is only populated by `interlace lineage refresh`.
            # Use the latest version per schema_name for this model.
            sql = f"""SELECT sh.column_name, sh.column_type AS data_type, sh.is_nullable, sh.is_primary_key
                     FROM interlace.schema_history sh
                     INNER JOIN (
                         SELECT schema_name, MAX(version) AS max_version
                         FROM interlace.schema_history
                         WHERE model_name = {_sql_value(model_name)}
                         GROUP BY schema_name
                     ) latest ON sh.schema_name = latest.schema_name AND sh.version = latest.max_version
                     WHERE sh.model_name = {_sql_value(model_name)}
                     ORDER BY sh.column_name"""

            result = _execute_sql_internal(conn, sql)
            if result is not None:
                if hasattr(result, "to_dict"):
                    return result.to_dict("records")  # type: ignore[no-any-return]
                elif hasattr(result, "fetchall"):
                    columns = [desc[0] for desc in result.description]
                    return [dict(zip(columns, row, strict=False)) for row in result.fetchall()]
            return []
        except Exception as e:
            logger.debug(f"Failed to get model columns: {e}")
            return []

    # ------------------------------------------------------------------
    # Scheduler persistence
    # ------------------------------------------------------------------

    def get_model_last_run_at(self, model_name: str, schema_name: str = "public") -> datetime | None:
        """Get last_run_at timestamp for a model from model_metadata.

        Used by the scheduler to restore state after service restart so that
        next-fire-time can be computed relative to the *persisted* last run
        rather than defaulting to "run immediately".

        Args:
            model_name: Name of the model
            schema_name: Schema name (default "public")

        Returns:
            datetime of last run, or None if model has never run / no metadata
        """
        conn = self._get_connection()
        if conn is None:
            return None

        try:
            safe_name = _escape_sql_string(model_name)
            safe_schema = _escape_sql_string(schema_name)
            result = conn.sql(
                f"SELECT last_run_at FROM interlace.model_metadata "
                f"WHERE model_name = '{safe_name}' AND schema_name = '{safe_schema}'"
            ).execute()

            if result is not None and len(result) > 0:
                val = result.iloc[0, 0]
                if val is not None:
                    # Convert pandas Timestamp to stdlib datetime if needed
                    if hasattr(val, "to_pydatetime"):
                        return val.to_pydatetime()  # type: ignore[no-any-return]
                    if isinstance(val, datetime):
                        return val
            return None
        except Exception as e:
            logger.debug(f"Could not get last_run_at for {model_name}: {e}")
            return None

    def set_model_last_run_at(
        self, model_name: str, schema_name: str = "public", run_at: datetime | None = None
    ) -> None:
        """Persist last_run_at timestamp for a model in model_metadata.

        Creates the row if it doesn't exist (upsert via delete+insert).

        Args:
            model_name: Name of the model
            schema_name: Schema name (default "public")
            run_at: Timestamp to store; defaults to ``datetime.utcnow()``
        """
        conn = self._get_connection()
        if conn is None:
            return

        if run_at is None:
            run_at = datetime.utcnow()

        try:
            safe_name = _escape_sql_string(model_name)
            safe_schema = _escape_sql_string(schema_name)
            ts_str = run_at.isoformat()

            # Try UPDATE first (cheaper than delete+insert when row exists)
            try:
                _execute_sql_internal(
                    conn,
                    f"UPDATE interlace.model_metadata "
                    f"SET last_run_at = TIMESTAMP '{ts_str}', updated_at = CURRENT_TIMESTAMP "
                    f"WHERE model_name = '{safe_name}' AND schema_name = '{safe_schema}'",
                )
                # Check if update affected any rows by verifying the row exists
                verify = conn.sql(
                    f"SELECT 1 FROM interlace.model_metadata "
                    f"WHERE model_name = '{safe_name}' AND schema_name = '{safe_schema}'"
                ).execute()
                if verify is not None and len(verify) > 0:
                    return  # UPDATE succeeded
            except Exception:
                pass

            # Row doesn't exist – insert minimal metadata row
            _execute_sql_internal(
                conn,
                f"INSERT INTO interlace.model_metadata "
                f"(model_name, schema_name, last_run_at, updated_at, discovered_at) "
                f"VALUES ('{safe_name}', '{safe_schema}', TIMESTAMP '{ts_str}', "
                f"CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            )
        except Exception as e:
            logger.warning(f"Could not set last_run_at for {model_name}: {e}")

    def clear_model_lineage(self, model_name: str) -> None:
        """
        Clear all lineage data for a model (before recomputing).

        Args:
            model_name: Name of the model
        """
        conn = self._get_connection()
        if conn is None:
            return

        try:
            schema_name = "interlace"
            lineage_table = f"{schema_name}.column_lineage"

            _execute_sql_internal(
                conn,
                f"DELETE FROM {lineage_table} WHERE output_model = {_sql_value(model_name)}",
            )
        except Exception as e:
            logger.debug(f"Failed to clear model lineage: {e}")
