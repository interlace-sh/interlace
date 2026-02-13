"""
Dependency loading for model execution.

Phase 0: Extracted from Executor class for better separation of concerns.
Phase 3: Added fallback connection resolution for virtual environments.
"""

import asyncio
from typing import TYPE_CHECKING, Any

import ibis

from interlace.utils.logging import get_logger
from interlace.utils.table_utils import try_load_table

if TYPE_CHECKING:
    from interlace.core.execution.schema_manager import SchemaManager

logger = get_logger("interlace.execution.dependency_loader")


class DependencyLoader:
    """
    Loads model dependencies with proper locking to prevent race conditions.

    Phase 0: Handles loading of dependency tables from materialized tables or ephemeral storage.
    Phase 3: Supports fallback connection resolution -- when a dependency table is not found
    in the current environment's connection, searches through fallback connections
    (e.g. shared source layer, production data) before giving up.
    """

    def __init__(
        self,
        materialised_tables: dict[str, ibis.Table],
        schema_manager: "SchemaManager",
        dep_loading_locks: dict[str, asyncio.Lock],
        dep_schema_cache: dict[str, str | None],
        fallback_connections: list[Any] | None = None,
    ):
        """
        Initialize DependencyLoader.

        Args:
            materialised_tables: Dictionary of materialized tables
            schema_manager: SchemaManager instance for checking table existence
            dep_loading_locks: Dictionary of locks for dependency loading
            dep_schema_cache: Dictionary for caching dependency schemas
            fallback_connections: Optional list of (name, connection_object) tuples
                                 to search when a dependency is not found in the
                                 current environment. Used for virtual environments
                                 and shared source layers.
        """
        self.materialised_tables = materialised_tables
        self.schema_manager = schema_manager
        self._dep_loading_locks = dep_loading_locks
        self._dep_schema_cache = dep_schema_cache
        self._fallback_connections: list[tuple] = fallback_connections or []
        # Track which connection satisfied each dependency (for tracing/debugging)
        self._dependency_sources: dict[str, str] = {}

    async def load_model_dependencies(
        self,
        model_info: dict[str, Any],
        model_conn: ibis.BaseBackend,
        models: dict[str, dict[str, Any]],
        schema: str,
    ) -> dict[str, ibis.Table]:
        """
        Load dependencies for a model in parallel.

        Dependencies are loaded from materialized tables (table/view) or from
        ephemeral storage (ephemeral models). Uses locking to prevent race conditions
        when multiple tasks try to load the same dependency simultaneously.
        Independent dependencies are loaded in parallel for better performance.

        Args:
            model_info: Model info dict
            model_conn: Database connection
            models: All models dict
            schema: Schema name

        Returns:
            Dictionary mapping dependency names to ibis.Table objects
        """
        dependencies = model_info.get("dependencies", [])
        if not dependencies:
            return {}

        # Load all dependencies in parallel
        dep_tasks = [self.load_dependency_with_lock(dep_name, model_conn, models, schema) for dep_name in dependencies]

        # Use gather with return_exceptions to handle individual failures
        dep_results = await asyncio.gather(*dep_tasks, return_exceptions=True)

        # Build result dictionary, handling exceptions
        dependency_tables: dict[str, ibis.Table] = {}
        for dep_name, result in zip(dependencies, dep_results, strict=False):
            if isinstance(result, Exception):
                logger.warning(
                    f"Failed to load dependency '{dep_name}': {result}. "
                    f"Model may fail if this dependency is required."
                )
            elif result is not None:
                dependency_tables[dep_name] = result

        return dependency_tables

    async def load_dependency_with_lock(
        self,
        dep_name: str,
        model_conn: ibis.BaseBackend,
        models: dict[str, dict[str, Any]],
        schema: str,
    ) -> ibis.Table | None:
        """
        Load a dependency table with lock to optimize performance.

        Note: Locks are NOT required for correctness - multiple tasks can safely load
        the same dependency in parallel (they're just reading table references). However,
        the lock prevents redundant work when many tasks need the same dependency:
        - Without lock: 10 tasks might all try multiple schemas, causing 10x redundant queries
        - With lock: Only one task does the lookup, others reuse the cached result

        This is a performance optimization, not a correctness requirement.

        Args:
            dep_name: Name of the dependency to load
            model_conn: Connection to use for loading
            models: All models dictionary
            schema: Current model's schema

        Returns:
            ibis.Table if dependency was loaded successfully, None otherwise
        """
        # Create lock for this dependency if it doesn't exist
        if dep_name not in self._dep_loading_locks:
            self._dep_loading_locks[dep_name] = asyncio.Lock()

        async with self._dep_loading_locks[dep_name]:
            # Check again after acquiring lock (another task may have loaded it)
            if dep_name in self.materialised_tables:
                return self.materialised_tables[dep_name]

            # Try to load from connections
            # Check if dependency model specifies a connection
            dep_model_info = models.get(dep_name, {})
            dep_conn = model_conn  # Use same task connection
            dep_schema = dep_model_info.get("schema", "public")

            # Build schema search order: cached first, then model schema, then fallbacks
            # This reduces schema probing attempts
            try_schemas = []

            # 1. Try cached schema location first (most likely to succeed)
            cached_schema = self._dep_schema_cache.get(dep_name)
            if cached_schema is not None:
                try_schemas.append(cached_schema)

            # 2. Try dependency model's explicit schema (from @schema annotation)
            if dep_schema and dep_schema not in try_schemas:
                try_schemas.append(dep_schema)

            # 3. Try current model's schema (dependencies often in same schema)
            if schema and schema not in try_schemas:
                try_schemas.append(schema)

            # 4. Try common default schemas
            for default_schema in ["main", "public"]:
                if default_schema not in try_schemas:
                    try_schemas.append(default_schema)

            # 5. Try without schema (default schema)
            try_schemas.append(None)

            # Try to load table from dependency's connection and schema
            # Use utility function to reduce duplication, but track which schema worked
            for try_schema in try_schemas:
                dep_table = try_load_table(dep_conn, dep_name, [try_schema])
                if dep_table is not None:
                    # Cache successful schema location
                    self._dep_schema_cache[dep_name] = try_schema
                    # Store in materialised_tables for future use
                    self.materialised_tables[dep_name] = dep_table
                    self._dependency_sources[dep_name] = "primary"
                    return dep_table

            # Phase 3: Try fallback connections (virtual environments / shared source layer)
            if self._fallback_connections:
                for fallback_name, fallback_conn_obj in self._fallback_connections:
                    try:
                        # Get the ibis backend from the connection object
                        if hasattr(fallback_conn_obj, "connection"):
                            fallback_backend = fallback_conn_obj.connection
                        else:
                            fallback_backend = fallback_conn_obj

                        # Try common schemas on the fallback connection
                        fallback_schemas = [dep_schema, "public", "main", None]
                        for fb_schema in fallback_schemas:
                            dep_table = try_load_table(fallback_backend, dep_name, [fb_schema])
                            if dep_table is not None:
                                self._dep_schema_cache[dep_name] = fb_schema
                                self.materialised_tables[dep_name] = dep_table
                                self._dependency_sources[dep_name] = fallback_name
                                logger.info(
                                    f"Dependency '{dep_name}' resolved from fallback "
                                    f"connection '{fallback_name}'"
                                    f"{f' (schema: {fb_schema})' if fb_schema else ''}"
                                )
                                return dep_table
                    except Exception as e:
                        logger.debug(f"Failed to load '{dep_name}' from fallback " f"connection '{fallback_name}': {e}")

            # Failed to load from any connection
            return None

    def get_dependency_sources(self) -> dict[str, str]:
        """
        Get a mapping of dependency names to the connection that satisfied them.        Useful for debugging virtual environments and understanding
        which connection provided each dependency.        Returns:
            Dictionary mapping dependency name to connection name
            (e.g. {"users": "primary", "companies_house": "sources"})
        """
        return dict(self._dependency_sources)
