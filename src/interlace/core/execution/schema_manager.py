"""
Schema management for validating and evolving table schemas.

Phase 0: Extracted from Executor class for better separation of concerns.
"""

from typing import Dict, Any, Optional, Tuple, TYPE_CHECKING, Union
import ibis
import pandas as pd
from cachetools import LRUCache, TTLCache
from interlace.schema.validation import validate_schema
from interlace.schema.evolution import apply_schema_changes, should_apply_schema_changes
from interlace.schema.tracking import track_schema_version
from interlace.utils.schema_utils import fields_to_ibis_schema
from interlace.core.context import _execute_sql_internal
from interlace.utils.table_utils import check_table_exists
from interlace.utils.logging import get_logger

if TYPE_CHECKING:
    from interlace.core.execution.data_converter import DataConverter

logger = get_logger("interlace.execution.schema_manager")


class SchemaManager:
    """
    Manages schema validation and evolution for tables.
    
    Phase 0: Handles schema validation, evolution, table creation, and existence checks.
    """

    def __init__(
        self,
        data_converter: "DataConverter",
        schema_cache: Dict[str, ibis.Schema],
        table_existence_cache: Dict[tuple, bool],
        max_schema_cache_size: int = 1000,
        max_existence_cache_size: int = 2000,
        schema_cache_ttl: Optional[float] = None,
        existence_cache_ttl: Optional[float] = None,
    ):
        """
        Initialize SchemaManager.
        
        Args:
            data_converter: DataConverter instance for converting data to ibis tables
            schema_cache: Dictionary for caching table schemas (will be converted to LRU/TTL if not already)
            table_existence_cache: Dictionary for caching table existence checks (will be converted to LRU/TTL if not already)
            max_schema_cache_size: Maximum size for schema cache (default: 1000)
            max_existence_cache_size: Maximum size for table existence cache (default: 2000)
            schema_cache_ttl: Time-to-live for schema cache entries in seconds (None = no expiration)
            existence_cache_ttl: Time-to-live for existence cache entries in seconds (None = no expiration)
        """
        self.data_converter = data_converter
        
        # Convert to LRU or TTL caches if not already
        if isinstance(schema_cache, (LRUCache, TTLCache)):
            self._schema_cache = schema_cache
        else:
            # Create cache with TTL if specified, otherwise LRU
            if schema_cache_ttl is not None:
                self._schema_cache = TTLCache(maxsize=max_schema_cache_size, ttl=schema_cache_ttl)
            else:
                self._schema_cache = LRUCache(maxsize=max_schema_cache_size)
            self._schema_cache.update(schema_cache)
        
        if isinstance(table_existence_cache, (LRUCache, TTLCache)):
            self._table_existence_cache = table_existence_cache
        else:
            # Create cache with TTL if specified, otherwise LRU
            if existence_cache_ttl is not None:
                self._table_existence_cache = TTLCache(maxsize=max_existence_cache_size, ttl=existence_cache_ttl)
            else:
                self._table_existence_cache = LRUCache(maxsize=max_existence_cache_size)
            self._table_existence_cache.update(table_existence_cache)
    
    def invalidate_schema_cache(self, cache_key: str) -> None:
        """
        Invalidate a schema cache entry.
        
        Args:
            cache_key: Cache key to invalidate (format: "schema.table_name")
        """
        if cache_key in self._schema_cache:
            del self._schema_cache[cache_key]
    
    def invalidate_existence_cache(self, table_name: str, schema: Optional[str] = None) -> None:
        """
        Invalidate a table existence cache entry.
        
        Args:
            table_name: Table name
            schema: Schema name (optional)
        """
        cache_key = (table_name, schema)
        if cache_key in self._table_existence_cache:
            del self._table_existence_cache[cache_key]

    async def setup_reference_table(
        self,
        data: Union[ibis.Table, pd.DataFrame, list, dict],
        model_name: str,
        connection: ibis.BaseBackend,
        model_info: Optional[Dict[str, Any]] = None,
    ) -> ibis.Table:
        """
        Setup reference table for schema validation.

        For dict/list/DataFrame: Converts to ibis.Table via ibis.memtable(), then creates temporary reference table.
        This allows schema validation before materialization.

        Args:
            data: Python data structure (dict/list/DataFrame)
            model_name: Model name
            connection: ibis connection backend
            model_info: Optional model info dict to get fields

        Returns:
            ibis.Table: Reference table (temporary table)
        """
        fields = model_info.get("fields") if model_info else None
        strict = model_info.get("strict", False) if model_info else False
        column_mapping = model_info.get("column_mapping") if model_info else None

        table_expr = self.data_converter.convert_to_ibis_table(
            data,
            fields=fields,
            strict=strict,
            column_mapping=column_mapping,
        )
        ref_table_name = f"_interlace_tmp_{model_name}"

        try:
            # Try to create table directly from ibis.Table if backend supports it
            # This avoids materializing to DataFrame when not necessary
            try:
                # Some backends support creating tables directly from ibis expressions
                # Try this first to avoid DataFrame materialization
                connection.create_table(ref_table_name, obj=table_expr, temp=True)
                return connection.table(ref_table_name)
            except (TypeError, AttributeError, ValueError):
                # Fallback: materialize to DataFrame (required for some backends)
                df = table_expr.execute()
                connection.create_table(ref_table_name, obj=df, temp=True)
                return connection.table(ref_table_name)
        except Exception as e:
            # Add model-specific context to error
            error_msg = f"Failed to materialize reference table for model '{model_name}': {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise RuntimeError(error_msg) from e

    async def validate_and_update_schema(
        self,
        table_ref: ibis.Table,
        model_name: str,
        schema: str,
        connection: ibis.BaseBackend,
        model_info: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Validate schema compatibility and handle schema evolution.

        Uses the new schema validation module to compare schemas and apply changes.
        Respects fields parameter (adds/replaces fields in schema if specified).
        Safe column removal: preserves columns, new data has NULL for missing columns.

        Args:
            table_ref: ibis.Table reference (new data)
            model_name: Model name
            schema: Schema/database name
            connection: ibis connection backend
            model_info: Optional model info dict to get fields

        Returns:
            Number of schema changes applied
        """
        # Check if target table exists using cached check
        table_exists = self.check_table_exists(connection, model_name, schema)

        new_schema = table_ref.schema()
        fields = model_info.get("fields") if model_info else None
        # Convert fields to ibis schema for validation
        fields_schema = fields_to_ibis_schema(fields)

        # Check cache first
        cache_key = f"{schema}.{model_name}"
        if cache_key in self._schema_cache:
            cached_schema = self._schema_cache[cache_key]
            # Compare schemas - if identical, no changes needed
            if cached_schema == new_schema:
                return 0

        # Get existing schema if table exists
        existing_schema = None
        if table_exists:
            try:
                existing_table = connection.table(model_name, database=schema)
                existing_schema = existing_table.schema()
            except Exception:
                # Table doesn't exist or can't be loaded - no schema validation needed
                existing_schema = None

        # Determine schema mode from model info
        schema_mode = None
        if model_info:
            mode_str = model_info.get("schema_mode")
            if mode_str:
                try:
                    from interlace.schema.modes import SchemaMode
                    schema_mode = SchemaMode(mode_str)
                except (ValueError, KeyError):
                    logger.warning(f"Invalid schema_mode '{mode_str}' for {model_name}, using default")

        # Validate schema
        validation_result = validate_schema(existing_schema, new_schema, fields_schema, schema_mode)

        if validation_result.errors:
            # Log errors but don't fail (non-fatal)
            logger.warning(
                f"Schema validation errors for {model_name}: {', '.join(validation_result.errors)}"
            )
            return 0

        # Track schema version (informative only)
        # Track initial schema if table doesn't exist, or track changes if schema changed
        should_track = False
        if existing_schema is None:
            # First time creating table - track initial schema
            should_track = True
        elif should_apply_schema_changes(existing_schema, new_schema, fields_schema):
            # Schema changed - track new version
            should_track = True

        if should_track:
            try:
                primary_key = model_info.get("primary_key") if model_info else None
                track_schema_version(connection, model_name, schema, new_schema, primary_key)
            except Exception as e:
                logger.debug(f"Could not track schema version for {model_name}: {e}")

        # Apply schema changes if needed
        if existing_schema is not None and should_apply_schema_changes(
            existing_schema, new_schema, fields_schema
        ):
            try:
                changes_count = apply_schema_changes(
                    connection, model_name, schema, existing_schema, new_schema, fields_schema
                )
                # Update cache with new schema
                self._schema_cache[cache_key] = new_schema
                return changes_count
            except Exception as e:
                logger.warning(f"Failed to apply schema changes for {model_name}: {e}")
                return 0

        # Update cache even if no changes
        if existing_schema is None:
            self._schema_cache[cache_key] = new_schema

        return 0

    async def batch_check_table_exists(
        self,
        connection: ibis.BaseBackend,
        table_schema_pairs: list[tuple[str, Optional[str]]],
    ) -> Dict[tuple[str, Optional[str]], bool]:
        """
        Batch check table existence for multiple tables.
        
        More efficient than checking tables one by one by using a single query.
        
        Args:
            connection: ibis connection backend
            table_schema_pairs: List of (table_name, schema) tuples
            
        Returns:
            Dictionary mapping (table_name, schema) -> bool
        """
        results = {}
        
        # Group by schema to batch queries
        by_schema: Dict[Optional[str], list[str]] = {}
        for table_name, schema in table_schema_pairs:
            if schema not in by_schema:
                by_schema[schema] = []
            by_schema[schema].append(table_name)
        
        # Check cache first
        for table_name, schema in table_schema_pairs:
            cache_key = (table_name, schema)
            if cache_key in self._table_existence_cache:
                results[cache_key] = self._table_existence_cache[cache_key]
        
        # For each schema, query all tables at once
        for schema, table_names in by_schema.items():
            # Filter out already cached
            uncached = [
                name for name in table_names
                if (name, schema) not in results
            ]
            if not uncached:
                continue
            
            try:
                # Use list_tables to get all tables in schema
                if schema:
                    tables = connection.list_tables(database=schema)
                else:
                    tables = connection.list_tables()
                
                # Check which tables exist
                for table_name in uncached:
                    exists = table_name in tables
                    cache_key = (table_name, schema)
                    results[cache_key] = exists
                    # Update cache
                    self._table_existence_cache[cache_key] = exists
            except Exception as e:
                logger.debug(f"Batch table existence check failed for schema {schema}: {e}")
                # Fall back to individual checks
                for table_name in uncached:
                    cache_key = (table_name, schema)
                    if cache_key not in results:
                        results[cache_key] = self.check_table_exists(connection, table_name, schema)
        
        return results

    def create_table_safe(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        obj: Union[ibis.Table, pd.DataFrame],
        schema: str,
        overwrite: bool = False,
    ) -> None:
        """
        Create a table safely, handling schema/qualified name variations.
        
        Tries to create table with database parameter first, falls back to qualified name
        if that fails (for backends that don't support database parameter).
        
        Args:
            connection: ibis connection backend
            table_name: Name of the table to create
            obj: Data to create table from (DataFrame, ibis.Table, etc.)
            schema: Schema/database name
            overwrite: Whether to overwrite existing table
        """
        try:
            connection.create_table(table_name, obj=obj, database=schema, overwrite=overwrite)
        except (TypeError, AttributeError):
            # Fallback: use qualified name for backends that don't support database parameter
            qualified_name = f"{schema}.{table_name}"
            connection.create_table(qualified_name, obj=obj, overwrite=overwrite)

    def check_table_exists(
        self, connection: ibis.BaseBackend, table_name: str, schema: Optional[str] = None
    ) -> bool:
        """
        Check if a table exists using cached checks.

        Args:
            connection: ibis connection backend
            table_name: Table name to check
            schema: Schema/database name (optional)

        Returns:
            True if table exists, False otherwise
        """
        cache_key = (table_name, schema)
        if cache_key in self._table_existence_cache:
            return self._table_existence_cache[cache_key]

        exists = check_table_exists(connection, table_name, schema)
        self._table_existence_cache[cache_key] = exists
        
        # Invalidate schema cache if table existence changed
        # This handles cases where table is dropped/recreated externally
        schema_cache_key = f"{schema or 'main'}.{table_name}"
        if schema_cache_key in self._schema_cache:
            # If table doesn't exist but we have cached schema, invalidate it
            if not exists:
                del self._schema_cache[schema_cache_key]
        
        return exists

    def ensure_schema_exists(
        self, connection: ibis.BaseBackend, schema: str
    ) -> None:
        """
        Ensure database/schema exists, creating it if needed.
        """
        try:
            if hasattr(connection, "create_database"):
                connection.create_database(schema, force=False)
            else:
                _execute_sql_internal(connection, f"CREATE SCHEMA IF NOT EXISTS {schema}")
        except Exception as e:
            if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                logger.warning(f"Database creation issue (may already exist): {e}")

    def prepare_reference_table(
        self,
        new_data: ibis.Table,
        model_name: str,
        connection: ibis.BaseBackend,
        strategy_name: str,
    ) -> Tuple[ibis.Table, bool]:
        """
        Prepare reference table for strategy execution.

        Returns:
            Tuple of (source_table, is_reference_table)
        """
        ref_table_name = f"_interlace_tmp_{model_name}"
        source_table = new_data
        is_reference_table = False

        if strategy_name in ("merge_by_key", "scd_type_2"):
            # Materialise to reference table for MERGE/SCD2 SQL
            # Always recreate to ensure it has the latest data
            try:
                # Drop existing temp table if it exists
                if check_table_exists(connection, ref_table_name):
                    try:
                        connection.drop_table(ref_table_name)
                    except Exception:
                        pass

                # Prefer creating temp table directly from ibis expression (avoids DataFrame)
                try:
                    connection.create_table(ref_table_name, obj=new_data, temp=True)
                except (TypeError, AttributeError, ValueError, NotImplementedError):
                    df = new_data.execute()
                    connection.create_table(ref_table_name, obj=df, temp=True)
                source_table = connection.table(ref_table_name)
                is_reference_table = True
            except Exception:
                pass

        return source_table, is_reference_table

