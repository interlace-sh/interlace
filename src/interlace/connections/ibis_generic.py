"""
Generic ibis connection for any supported backend.

Provides a single connection class that dynamically imports and connects
to any ibis-supported backend. DuckDB and Postgres remain as specialised
subclasses due to their unique behaviours (ATTACH, WAL, connection pooling).

Install backend extras as needed:
    pip install ibis-framework[snowflake]
    pip install ibis-framework[bigquery]
    pip install ibis-framework[mysql]
    etc.
"""

import importlib
from typing import Dict, Any, Optional, Set
import ibis
from interlace.connections.base import BaseConnection
from interlace.utils.logging import get_logger

logger = get_logger("interlace.connections.ibis_generic")


class IbisConnection(BaseConnection):
    """
    Generic connection wrapper for any ibis-supported backend.

    Uses dynamic import to connect to the appropriate ibis backend
    based on the ``type`` field in the connection config. Backends
    that are not installed will raise a clear error message.

    Config example (Snowflake)::

        connections:
          snowflake_wh:
            type: snowflake
            config:
              account: myorg-myaccount
              user: ${SNOWFLAKE_USER}
              password: ${SNOWFLAKE_PASSWORD}
              database: ANALYTICS
              schema: PUBLIC
              warehouse: COMPUTE_WH

    Config example (BigQuery)::

        connections:
          bigquery_prod:
            type: bigquery
            config:
              project_id: my-gcp-project
              dataset_id: analytics

    Config example (MySQL)::

        connections:
          mysql_source:
            type: mysql
            config:
              host: mysql.internal
              port: 3306
              user: reader
              password: ${MYSQL_PASSWORD}
              database: app_production
    """

    # Map of connection type → ibis module path
    BACKEND_MAP: Dict[str, str] = {
        "mysql": "ibis.mysql",
        "sqlite": "ibis.sqlite",
        "snowflake": "ibis.snowflake",
        "bigquery": "ibis.bigquery",
        "clickhouse": "ibis.clickhouse",
        "databricks": "ibis.databricks",
        "trino": "ibis.trino",
        "mssql": "ibis.mssql",
        "oracle": "ibis.oracle",
        "datafusion": "ibis.datafusion",
        "polars": "ibis.polars",
        "deltalake": "ibis.deltalake",
        "risingwave": "ibis.risingwave",
        "flink": "ibis.flink",
        "athena": "ibis.athena",
        "exasol": "ibis.exasol",
        "impala": "ibis.impala",
        "pyspark": "ibis.pyspark",
    }

    # Backends that create new connections per task (cloud services
    # that handle concurrency/pooling internally)
    CLOUD_BACKENDS: Set[str] = {
        "snowflake",
        "bigquery",
        "databricks",
        "clickhouse",
        "trino",
        "athena",
        "exasol",
        "impala",
        "risingwave",
    }

    # Backends that are file-based and NOT thread-safe (need per-task
    # connections for file-based, shared for in-memory)
    FILE_BACKENDS: Set[str] = {
        "sqlite",
        "datafusion",
    }

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self._backend_type: str = config.get("type", "")
        if self._backend_type not in self.BACKEND_MAP:
            raise ValueError(
                f"Unsupported ibis backend type '{self._backend_type}' for connection '{name}'. "
                f"Supported types: {', '.join(sorted(self.BACKEND_MAP.keys()))}"
            )

    @property
    def backend_type(self) -> str:
        """Get the ibis backend type name."""
        return self._backend_type

    @property
    def connection(self) -> ibis.BaseBackend:
        """
        Get ibis backend connection (lazy initialization).

        Dynamically imports the ibis backend module and calls connect()
        with the config parameters.

        Returns:
            ibis.BaseBackend: ibis backend connection

        Raises:
            ImportError: If the backend extra is not installed
            RuntimeError: If connection fails
        """
        if self._connection is None:
            module_path = self.BACKEND_MAP[self._backend_type]
            connect_kwargs = self._build_connect_kwargs()

            # Import the backend module
            try:
                module = importlib.import_module(module_path)
            except ImportError as e:
                raise ImportError(
                    f"Cannot import ibis backend '{self._backend_type}' for connection '{self.name}'. "
                    f"Install the required extra: pip install 'ibis-framework[{self._backend_type}]'\n"
                    f"Original error: {e}"
                ) from e

            # Connect
            try:
                self._connection = module.connect(**connect_kwargs)
                logger.info(
                    f"Connected to {self._backend_type} backend "
                    f"for connection '{self.name}'"
                )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to connect to {self._backend_type} backend "
                    f"for connection '{self.name}': {e}\n"
                    f"Config keys provided: {list(connect_kwargs.keys())}"
                ) from e

        return self._connection

    def _build_connect_kwargs(self) -> Dict[str, Any]:
        """
        Build keyword arguments for ibis backend connect() call.

        Extracts connection parameters from the ``config`` sub-dict
        in the connection configuration. Removes interlace-specific
        keys that are not backend connect() parameters.

        Returns:
            Dictionary of keyword arguments for connect()
        """
        # Connection parameters live under the 'config' key
        connect_kwargs = dict(self.config.get("config", {}))

        # Remove interlace-specific keys that are not ibis connect() params
        interlace_keys = {
            "storage",      # Storage sub-config
            "pool",         # Pool config (handled separately)
            "health_check", # Health check config
        }
        for key in interlace_keys:
            connect_kwargs.pop(key, None)

        return connect_kwargs

    def create_new_connection(self) -> ibis.BaseBackend:
        """
        Create a fresh connection to the same backend.

        Used for per-task connection creation in parallel execution.
        Cloud backends handle concurrency internally so this is safe.
        File-based backends should check for in-memory vs file-based.

        Returns:
            ibis.BaseBackend: New ibis backend connection
        """
        module_path = self.BACKEND_MAP[self._backend_type]
        connect_kwargs = self._build_connect_kwargs()

        try:
            module = importlib.import_module(module_path)
        except ImportError as e:
            raise ImportError(
                f"Cannot import ibis backend '{self._backend_type}': "
                f"pip install 'ibis-framework[{self._backend_type}]'\n"
                f"Original error: {e}"
            ) from e

        return module.connect(**connect_kwargs)

    @property
    def is_cloud_backend(self) -> bool:
        """Whether this backend is a cloud service that handles pooling internally."""
        return self._backend_type in self.CLOUD_BACKENDS

    @property
    def is_file_backend(self) -> bool:
        """Whether this backend is file-based and not thread-safe."""
        return self._backend_type in self.FILE_BACKENDS

    @classmethod
    def supports_type(cls, conn_type: str) -> bool:
        """Check if a connection type is supported by the generic connector."""
        return conn_type in cls.BACKEND_MAP
