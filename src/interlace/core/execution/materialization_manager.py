"""
Materialization management for persisting data with strategies.

Phase 0: Extracted from Executor class for better separation of concerns.
"""

from typing import TYPE_CHECKING, Any

import ibis

from interlace.core.context import _execute_sql_internal
from interlace.strategies.base import Strategy
from interlace.utils.logging import get_logger
from interlace.utils.sql_escape import escape_identifier, escape_qualified_name
from interlace.utils.table_utils import check_table_exists

if TYPE_CHECKING:
    from interlace.materialization.base import Materializer

logger = get_logger("interlace.execution.materialization_manager")

# Alias for backward compatibility
_table_exists = check_table_exists


class MaterializationManager:
    """
    Manages data materialization with strategies.

    Phase 0: Handles materialization of data to tables/views/ephemeral storage
    using various strategies (replace, append, merge_by_key, none).
    """

    def __init__(
        self,
        schema_manager: Any,
        data_converter: Any,
        strategies: dict[str, Strategy],
        flow: Any = None,
        get_row_count_func: Any = None,
    ):
        """
        Initialize MaterializationManager.

        Args:
            schema_manager: SchemaManager instance
            data_converter: DataConverter instance
            strategies: Dictionary of strategy instances
            flow: Optional Flow instance for tracking task changes
            get_row_count_func: Function to get row count (from Executor)
        """
        self.schema_manager = schema_manager
        self.data_converter = data_converter
        self.strategies = strategies
        self.flow = flow
        self._get_row_count = get_row_count_func

    async def materialise(
        self,
        materializer: "Materializer",
        data: ibis.Table,
        model_name: str,
        schema: str,
        materialise_type: str,
        connection: ibis.BaseBackend,
        fields: dict[str, Any] | list | ibis.Schema | None = None,
        **kwargs: Any,
    ) -> None:
        """Materialise data using materialiser."""
        # Materialisation is synchronous - call directly (DuckDB connections are not thread-safe)
        # TODO: Consider making ibis operations async or use connection pooling
        # Pass fields and any additional kwargs to materializer
        materialize_kwargs = {"fields": fields}
        materialize_kwargs.update(kwargs)
        materializer.materialise(data, model_name, schema, connection, **materialize_kwargs)

    async def materialise_table_with_strategy(
        self,
        new_data: ibis.Table,
        model_name: str,
        schema: str,
        strategy_name: str,
        model_info: dict[str, Any],
        connection: ibis.BaseBackend,
    ) -> None:
        """
        Materialise table with strategy using SQL-based execution.

        Uses the reference table from step 2 (setup reference) if available.
        For replace/append: Uses connection.insert() with overwrite parameter.
        For merge: Uses reference table in MERGE SQL.

        Flow:
        1. Check if table exists (first run vs subsequent)
        2. Use reference table from step 2 (or create if needed for SQL models)
        3. Execute strategy via connection.insert() or MERGE SQL
        4. Clean up reference table if needed
        """
        strategy = self.strategies.get(strategy_name)
        if not strategy:
            raise ValueError(f"Unknown strategy: {strategy_name}")

        # Check if table exists using list_tables()
        table_exists = check_table_exists(connection, model_name, schema)

        # Create database/schema if needed
        self.schema_manager.ensure_schema_exists(connection, schema)

        # Prepare reference table
        ref_table_name = f"_interlace_tmp_{model_name}"
        source_table, is_reference_table = self.schema_manager.prepare_reference_table(
            new_data, model_name, connection, strategy_name
        )

        # Apply strategy-specific logic
        if strategy_name == "replace":
            await self.apply_replace_strategy(new_data, model_name, schema, connection, table_exists)
        elif strategy_name == "append":
            await self.apply_append_strategy(
                new_data,
                model_name,
                schema,
                connection,
                table_exists,
                strategy,
                is_reference_table,
                ref_table_name,
            )
        elif strategy_name == "merge_by_key":
            await self.apply_merge_strategy(
                source_table,
                model_name,
                schema,
                connection,
                table_exists,
                model_info,
                strategy,
                ref_table_name,
            )
        elif strategy_name == "scd_type_2":
            await self.apply_scd_type_2_strategy(
                source_table,
                model_name,
                schema,
                connection,
                table_exists,
                model_info,
                strategy,
                ref_table_name,
            )
        elif strategy_name == "none":
            # "none" strategy means no strategy - just create/replace table directly
            # This is equivalent to replace but without strategy logic
            await self.apply_replace_strategy(new_data, model_name, schema, connection, table_exists)
        else:
            # Unknown strategy - raise error
            raise ValueError(
                f"Unknown strategy '{strategy_name}' for model '{model_name}'. "
                f"Supported strategies: replace, append, merge_by_key, scd_type_2, none"
            )

    async def apply_replace_strategy(
        self,
        new_data: ibis.Table,
        model_name: str,
        schema: str,
        connection: ibis.BaseBackend,
        table_exists: bool,
    ) -> None:
        """
        Apply replace strategy: replace entire table.
        """
        # Track changes for replace strategy
        existing_count = 0
        if table_exists and self._get_row_count:
            existing_count = self._get_row_count(None, model_name, "table", schema, connection) or 0

        if not table_exists:
            self.schema_manager.create_table_safe(connection, model_name, new_data, schema, overwrite=False)
        else:
            # Subsequent runs: replace entire table
            self.schema_manager.create_table_safe(connection, model_name, new_data, schema, overwrite=True)

        # Update task with change counts
        if self.flow and model_name in self.flow.tasks:
            task = self.flow.tasks[model_name]
            # All ingested rows are inserted (replace)
            task.rows_inserted = task.rows_ingested
            # All existing rows were deleted (if table existed)
            task.rows_deleted = existing_count if table_exists else 0
            task.rows_updated = 0  # Replace doesn't update, it replaces

    async def apply_append_strategy(
        self,
        new_data: ibis.Table,
        model_name: str,
        schema: str,
        connection: ibis.BaseBackend,
        table_exists: bool,
        strategy: Strategy,
        is_reference_table: bool,
        ref_table_name: str,
    ) -> None:
        """
        Apply append strategy: insert new rows.
        """
        if not table_exists:
            # Avoid DataFrame materialization when backend supports ibis.Table directly
            self.schema_manager.create_table_safe(connection, model_name, new_data, schema, overwrite=False)
            # Update task with change counts for first run
            if self.flow and model_name in self.flow.tasks:
                task = self.flow.tasks[model_name]
                task.rows_inserted = task.rows_ingested
                task.rows_updated = 0
                task.rows_deleted = 0
            return

        # Generate and execute INSERT SQL
        if is_reference_table:
            insert_sql = strategy.generate_sql(connection, model_name, schema, ref_table_name)
            if insert_sql:
                _execute_sql_internal(connection, insert_sql)
        else:
            # Create temp table and insert
            temp_table_name = f"_interlace_tmp_{model_name}"
            try:
                connection.create_table(temp_table_name, obj=new_data, temp=True)
            except (TypeError, AttributeError, ValueError, NotImplementedError):
                df = new_data.execute()
                connection.create_table(temp_table_name, obj=df, temp=True)
            insert_sql = strategy.generate_sql(connection, model_name, schema, temp_table_name)
            if insert_sql:
                _execute_sql_internal(connection, insert_sql)
            try:
                connection.drop_table(temp_table_name)
            except Exception:
                pass

        # Update task with change counts
        if self.flow and model_name in self.flow.tasks:
            task = self.flow.tasks[model_name]
            # All ingested rows are inserted (append)
            task.rows_inserted = task.rows_ingested
            task.rows_updated = 0  # Append doesn't update
            task.rows_deleted = 0  # Append doesn't delete

    async def apply_merge_strategy(
        self,
        source_table: ibis.Table,
        model_name: str,
        schema: str,
        connection: ibis.BaseBackend,
        table_exists: bool,
        model_info: dict[str, Any],
        strategy: Strategy,
        ref_table_name: str,
    ) -> None:
        """
        Apply merge_by_key strategy: merge rows based on primary key.
        """
        primary_key = model_info.get("primary_key")
        if primary_key is None:
            raise ValueError(f"primary_key required for merge_by_key strategy on model {model_name}")

        if not table_exists:
            # Avoid DataFrame materialization when backend supports ibis.Table directly
            self.schema_manager.create_table_safe(connection, model_name, source_table, schema, overwrite=False)
            # Update task with change counts for first run
            if self.flow and model_name in self.flow.tasks:
                task = self.flow.tasks[model_name]
                task.rows_inserted = task.rows_ingested
                task.rows_updated = 0
                task.rows_deleted = 0
            return

        delete_mode = model_info.get("delete_mode", "preserve")

        # Pre-compute change counts before MERGE
        # Normalize primary_key to list
        if isinstance(primary_key, str):
            pk_list = [primary_key]
        else:
            pk_list = primary_key

        # Build escaped identifiers to prevent SQL injection
        escaped_target = escape_qualified_name(schema, model_name)
        escaped_source = escape_identifier(ref_table_name)

        # Build ON conditions for queries with escaped column names
        on_conditions = " AND ".join(
            [f"target.{escape_identifier(pk)} = source.{escape_identifier(pk)}" for pk in pk_list]
        )

        # Count inserts: source rows not in target
        insert_count_sql = f"""
            SELECT COUNT(*) as count
            FROM {escaped_source} AS source
            WHERE NOT EXISTS (
                SELECT 1 FROM {escaped_target} AS target
                WHERE {on_conditions}
            )
        """

        # Count updates: source rows that exist in target
        update_count_sql = f"""
            SELECT COUNT(*) as count
            FROM {escaped_source} AS source
            WHERE EXISTS (
                SELECT 1 FROM {escaped_target} AS target
                WHERE {on_conditions}
            )
        """

        # Count deletes: target rows not in source (if delete_mode enabled)
        delete_count_sql = None
        if delete_mode in ("delete_missing", "hard"):
            delete_count_sql = f"""
                SELECT COUNT(*) as count
                FROM {escaped_target} AS target
                WHERE NOT EXISTS (
                    SELECT 1 FROM {escaped_source} AS source
                    WHERE {on_conditions}
                )
            """

        # Execute count queries (can run in parallel, but SQL execution is synchronous)
        # For now, execute sequentially - optimization can be added later
        try:
            insert_result = _execute_sql_internal(connection, insert_count_sql)
            rows_inserted = self.data_converter.extract_count_from_result(insert_result) or 0
        except Exception as e:
            logger.debug(f"Error counting inserts for {model_name}: {e}")
            rows_inserted = 0

        try:
            update_result = _execute_sql_internal(connection, update_count_sql)
            rows_updated = self.data_converter.extract_count_from_result(update_result) or 0
        except Exception as e:
            logger.debug(f"Error counting updates for {model_name}: {e}")
            rows_updated = 0

        rows_deleted = 0
        if delete_count_sql:
            try:
                delete_result = _execute_sql_internal(connection, delete_count_sql)
                rows_deleted = self.data_converter.extract_count_from_result(delete_result) or 0
            except Exception as e:
                logger.debug(f"Error counting deletes for {model_name}: {e}")
                rows_deleted = 0

        # Execute MERGE
        merge_sql = strategy.generate_sql(
            connection,
            model_name,
            schema,
            ref_table_name,
            primary_key=primary_key,
            delete_mode=delete_mode,
        )
        if merge_sql:
            _execute_sql_internal(connection, merge_sql)

        # Update task with change counts
        if self.flow and model_name in self.flow.tasks:
            task = self.flow.tasks[model_name]
            task.rows_inserted = rows_inserted
            task.rows_updated = rows_updated
            task.rows_deleted = rows_deleted

    async def apply_scd_type_2_strategy(
        self,
        source_table: ibis.Table,
        model_name: str,
        schema: str,
        connection: ibis.BaseBackend,
        table_exists: bool,
        model_info: dict[str, Any],
        strategy: Strategy,
        ref_table_name: str,
    ) -> None:
        """
        Apply SCD Type 2 strategy: maintain full history of dimension changes.

        SCD Type 2 tracks all historical versions of records:
        - New records: Inserted with is_current=TRUE, valid_from=NOW()
        - Changed records: Old version closed (valid_to=NOW(), is_current=FALSE),
                          new version inserted
        - Deleted records: Closed (valid_to=NOW(), is_current=FALSE) if delete_mode="soft"

        Requires SCD2 metadata columns in target table:
        - valid_from, valid_to, is_current, _scd2_hash

        Args:
            source_table: Source data (new dimension records)
            model_name: Target table name
            schema: Schema/database name
            connection: Database connection
            table_exists: Whether target table already exists
            model_info: Model configuration (contains primary_key, scd2_config)
            strategy: SCDType2Strategy instance
            ref_table_name: Temporary table name for source data
        """
        primary_key = model_info.get("primary_key")
        if primary_key is None:
            raise ValueError(f"primary_key required for scd_type_2 strategy on model {model_name}")

        scd2_config = model_info.get("scd2_config", {})

        # Get SCD2 column names from config
        valid_from_col = scd2_config.get("valid_from_col", "valid_from")
        valid_to_col = scd2_config.get("valid_to_col", "valid_to")
        is_current_col = scd2_config.get("is_current_col", "is_current")
        hash_col = scd2_config.get("hash_col", "_scd2_hash")

        if not table_exists:
            # First run: Create table with SCD2 columns and insert all as current
            logger.debug(f"SCD2 first run for {model_name} - creating table with history columns")

            # Get source columns to determine table structure
            try:
                source = connection.table(ref_table_name)
                source_schema = source.schema()
                source_columns = list(source_schema.keys())
            except Exception as e:
                raise ValueError(f"Cannot get schema for source table {ref_table_name}: {e}") from e

            # Create table with SCD2 columns using SQL
            # This ensures proper column types for SCD2 metadata
            from interlace.utils.sql_escape import escape_identifier, escape_qualified_name

            escaped_target = escape_qualified_name(schema, model_name)
            escape_identifier(ref_table_name)

            # Build column definitions
            col_defs = []
            for col_name in source_columns:
                col_type = source_schema[col_name]
                # Map ibis types to SQL types
                sql_type = self._ibis_type_to_sql(col_type)
                col_defs.append(f"{escape_identifier(col_name)} {sql_type}")

            # Add SCD2 metadata columns
            col_defs.append(f"{escape_identifier(valid_from_col)} TIMESTAMP")
            col_defs.append(f"{escape_identifier(valid_to_col)} TIMESTAMP")
            col_defs.append(f"{escape_identifier(is_current_col)} BOOLEAN")
            col_defs.append(f"{escape_identifier(hash_col)} VARCHAR")

            # If delete_mode is soft_mark, add is_deleted column
            delete_mode = scd2_config.get("delete_mode", "ignore")
            deleted_col = scd2_config.get("deleted_col", "is_deleted")
            if delete_mode == "soft_mark":
                col_defs.append(f"{escape_identifier(deleted_col)} BOOLEAN DEFAULT FALSE")

            col_defs_str = ",\n    ".join(col_defs)

            create_table_sql = f"""
CREATE TABLE {escaped_target} (
    {col_defs_str}
)"""
            _execute_sql_internal(connection, create_table_sql)

            # Use initial insert SQL from strategy
            insert_sql = strategy.get_initial_insert_sql(  # type: ignore[attr-defined]
                connection,
                model_name,
                schema,
                ref_table_name,
                primary_key=primary_key,
                scd2_config=scd2_config,
            )
            _execute_sql_internal(connection, insert_sql)

            # Update task with counts
            if self.flow and model_name in self.flow.tasks:
                task = self.flow.tasks[model_name]
                task.rows_inserted = task.rows_ingested
                task.rows_updated = 0
                task.rows_deleted = 0
            return

        # Subsequent runs: Apply SCD2 logic (expire changed, insert new versions)
        logger.debug(f"SCD2 update for {model_name} - applying change tracking")

        # Generate SCD2 SQL (multiple statements)
        scd2_sql = strategy.generate_sql(
            connection,
            model_name,
            schema,
            ref_table_name,
            primary_key=primary_key,
            scd2_config=scd2_config,
        )

        # Execute each statement (split by semicolon)
        if not scd2_sql:
            return
        statements = [s.strip() for s in scd2_sql.split(";") if s.strip()]
        for stmt in statements:
            _execute_sql_internal(connection, stmt)

        # Update task with change counts
        # For SCD2, we track expired records as "updated" and new versions as "inserted"
        if self.flow and model_name in self.flow.tasks:
            task = self.flow.tasks[model_name]
            # Approximate counts - exact counts would require additional queries
            task.rows_inserted = task.rows_ingested
            task.rows_updated = 0  # SCD2 doesn't update, it expires and inserts
            task.rows_deleted = 0  # SCD2 uses soft delete via valid_to

    def _ibis_type_to_sql(self, ibis_type: Any) -> str:
        """
        Convert ibis data type to SQL type string.

        Args:
            ibis_type: ibis DataType

        Returns:
            SQL type string (e.g., "VARCHAR", "INTEGER", "TIMESTAMP")
        """
        type_str = str(ibis_type).lower()

        # Map common ibis types to SQL
        type_mapping = {
            "int8": "TINYINT",
            "int16": "SMALLINT",
            "int32": "INTEGER",
            "int64": "BIGINT",
            "uint8": "UTINYINT",
            "uint16": "USMALLINT",
            "uint32": "UINTEGER",
            "uint64": "UBIGINT",
            "float32": "REAL",
            "float64": "DOUBLE",
            "boolean": "BOOLEAN",
            "string": "VARCHAR",
            "date": "DATE",
            "time": "TIME",
            "timestamp": "TIMESTAMP",
            "binary": "BLOB",
        }

        # Check for exact match
        if type_str in type_mapping:
            return type_mapping[type_str]

        # Check for partial match (e.g., "decimal(10, 2)")
        for key, _value in type_mapping.items():
            if type_str.startswith(key):
                return type_str.upper()  # Preserve precision/scale

        # Default to VARCHAR for unknown types
        return "VARCHAR"
