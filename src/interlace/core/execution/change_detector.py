"""
Change detection for determining if models should run.

Phase 0: Extracted from Executor class for better separation of concerns.
"""

from pathlib import Path
from typing import Any

from interlace.core.state import StateStore
from interlace.utils.hashing import (
    calculate_file_hash_async,
    get_stored_file_hash,
    has_file_changed_async,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.execution.change_detector")


class ChangeDetector:
    """
    Determines if models should run based on change detection.

    Phase 0: Handles file hash checking, last run timestamps, and upstream dependency changes.
    """

    def __init__(self, state_store: StateStore | None = None):
        """
        Initialize ChangeDetector.

        Args:
            state_store: Optional StateStore for tracking model runs and file hashes
        """
        self.state_store = state_store
        self._file_hash_cache: dict[tuple, str] = {}  # (model_name, schema) -> hash

    async def should_run_model(
        self,
        model_name: str,
        model_info: dict[str, Any],
        models: dict[str, dict[str, Any]],
        force: bool = False,
    ) -> tuple[bool, str]:
        """
        Check if model should run based on change detection.

        Args:
            model_name: Model name
            model_info: Model info dict
            models: All models dict
            force: Force execution (bypass change detection)

        Returns:
            Tuple of (should_run, reason)
        """
        if force:
            return True, "forced"

        # Check if model file has changed
        file_path = model_info.get("file")
        if file_path:
            try:
                # Get state store connection for checking file hash
                if self.state_store:
                    state_conn = self.state_store._get_connection()
                    if state_conn:
                        schema_name = model_info.get("schema", "public")
                        # Check cache first
                        cache_key = (model_name, schema_name)
                        if cache_key in self._file_hash_cache:
                            # Compare with current file hash (async for better I/O performance)
                            current_hash = await calculate_file_hash_async(Path(file_path))
                            if current_hash and current_hash != self._file_hash_cache[cache_key]:
                                return True, "file_changed"
                        else:
                            # Not in cache, check database (async for better I/O performance)
                            if await has_file_changed_async(state_conn, model_name, schema_name, Path(file_path)):
                                return True, "file_changed"
                            # Cache the hash for future checks (async for better I/O performance)
                            current_hash = await calculate_file_hash_async(Path(file_path))
                            if current_hash:
                                stored_hash = get_stored_file_hash(state_conn, model_name, schema_name)
                                self._file_hash_cache[cache_key] = stored_hash or current_hash
            except Exception as e:
                logger.debug(f"Could not check file hash for {model_name}: {e}")

        # Check if model has ever run (if not, it should run)
        try:
            if self.state_store:
                state_conn = self.state_store._get_connection()
                if state_conn:
                    model_last_run = self._get_model_last_run(
                        state_conn, model_name, model_info.get("schema", "public")
                    )
                    if model_last_run is None:
                        # Model has never run - should run
                        return True, "first_run"
        except Exception as e:
            logger.debug(f"Could not check last run for {model_name}: {e}")

        # Check if upstream models have changed (run timestamps)
        dependencies = model_info.get("dependencies", [])
        if dependencies:
            try:
                if self.state_store:
                    state_conn = self.state_store._get_connection()
                    if state_conn:
                        # Get model's last run timestamp
                        model_last_run = self._get_model_last_run(
                            state_conn, model_name, model_info.get("schema", "public")
                        )

                        # Check if any upstream model ran after this model
                        for dep_name in dependencies:
                            dep_last_run = self._get_model_last_run(
                                state_conn,
                                dep_name,
                                models.get(dep_name, {}).get("schema", "public"),
                            )
                            if dep_last_run and model_last_run:
                                if dep_last_run > model_last_run:
                                    return True, f"upstream_changed:{dep_name}"
                            elif dep_last_run and not model_last_run:
                                # Upstream ran but this model never ran
                                return True, f"upstream_changed:{dep_name}"
            except Exception as e:
                logger.debug(f"Could not check upstream changes for {model_name}: {e}")

        # If no state store is available, default to running (first run behavior)
        # This is important for tests and scenarios where state tracking isn't set up
        if not self.state_store:
            return True, "no_state_store"

        # If no changes detected, skip execution
        return False, "no_changes"

    def _get_model_last_run(self, connection: Any, model_name: str, schema_name: str) -> float | None:
        """Get last run timestamp for a model from model_metadata (O(1) lookup)."""
        try:
            from interlace.core.state import _escape_sql_string

            safe_model_name = _escape_sql_string(model_name)
            safe_schema_name = _escape_sql_string(schema_name)

            query = f"""
                SELECT last_run_at
                FROM interlace.model_metadata
                WHERE model_name = '{safe_model_name}'
                  AND schema_name = '{safe_schema_name}'
            """
            result = connection.sql(query).execute()
            if result is not None and len(result) > 0:
                last_run = result.iloc[0]["last_run_at"] if hasattr(result, "iloc") else None
                if last_run is not None:
                    if hasattr(last_run, "timestamp"):
                        return float(last_run.timestamp())
                    elif isinstance(last_run, (int, float)):
                        return float(last_run)
            return None
        except Exception as e:
            logger.debug(f"Could not get last run for {model_name}: {e}")
            return None

    def update_model_metadata(
        self,
        connection: Any,
        model_name: str,
        schema_name: str,
        model_info: dict[str, Any],
    ) -> None:
        """
        Update model metadata including last_run_at timestamp (UPSERT).

        Args:
            connection: Database connection
            model_name: Model name
            schema_name: Schema name
            model_info: Model info dictionary
        """
        try:
            from interlace.core.context import _execute_sql_internal
            from interlace.core.state import _escape_sql_string

            safe_model_name = _escape_sql_string(model_name)
            safe_schema_name = _escape_sql_string(schema_name)

            # Extract model metadata
            materialize = model_info.get("materialise", "table")
            strategy = model_info.get("strategy")
            primary_key = model_info.get("primary_key")
            dependencies = model_info.get("dependencies", [])
            description = model_info.get("description")
            tags = model_info.get("tags", [])
            owner = model_info.get("owner")
            source_file = model_info.get("file", "")
            source_type = model_info.get("type", "python")

            # Escape and format values
            safe_materialize = _escape_sql_string(materialize) if materialize else "NULL"
            safe_strategy = _escape_sql_string(strategy) if strategy else "NULL"
            safe_primary_key = _escape_sql_string(str(primary_key)) if primary_key else "NULL"
            safe_description = _escape_sql_string(description) if description else "NULL"
            safe_owner = _escape_sql_string(owner) if owner else "NULL"
            safe_source_file = _escape_sql_string(source_file) if source_file else "NULL"
            safe_source_type = _escape_sql_string(source_type) if source_type else "NULL"

            # Format arrays (DuckDB uses ARRAY['val1', 'val2'])
            deps_array = (
                "ARRAY[" + ",".join([f"'{_escape_sql_string(d)}'" for d in dependencies]) + "]"
                if dependencies
                else "NULL"
            )
            tags_array = "ARRAY[" + ",".join([f"'{_escape_sql_string(t)}'" for t in tags]) + "]" if tags else "NULL"

            # Use UPSERT: DELETE then INSERT (DuckDB doesn't support ON CONFLICT reliably)
            # First delete existing row if it exists
            delete_query = f"""
                DELETE FROM interlace.model_metadata
                WHERE model_name = '{safe_model_name}' AND schema_name = '{safe_schema_name}'
            """
            _execute_sql_internal(connection, delete_query)

            # Then insert new row with all metadata
            insert_query = f"""
                INSERT INTO interlace.model_metadata
                (model_name, schema_name, materialize, strategy, primary_key, dependencies,
                 description, tags, owner, source_file, source_type, last_run_at, updated_at, discovered_at)
                VALUES (
                    '{safe_model_name}',
                    '{safe_schema_name}',
                    {f"'{safe_materialize}'" if safe_materialize != "NULL" else "NULL"},
                    {f"'{safe_strategy}'" if safe_strategy != "NULL" else "NULL"},
                    {f"'{safe_primary_key}'" if safe_primary_key != "NULL" else "NULL"},
                    {deps_array},
                    {f"'{safe_description}'" if safe_description != "NULL" else "NULL"},
                    {tags_array},
                    {f"'{safe_owner}'" if safe_owner != "NULL" else "NULL"},
                    {f"'{safe_source_file}'" if safe_source_file != "NULL" else "NULL"},
                    {f"'{safe_source_type}'" if safe_source_type != "NULL" else "NULL"},
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                )
            """
            _execute_sql_internal(connection, insert_query)

            # Update file hash if source_file provided
            if source_file:
                from interlace.utils.hashing import calculate_file_hash, store_file_hash

                file_hash = calculate_file_hash(Path(source_file))
                if file_hash:
                    store_file_hash(connection, model_name, schema_name, source_file, file_hash)
                    # Update cache
                    cache_key = (model_name, schema_name)
                    self._file_hash_cache[cache_key] = file_hash
        except Exception as e:
            logger.debug(f"Could not update model metadata for {model_name}: {e}")
