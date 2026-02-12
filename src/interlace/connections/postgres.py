"""
Postgres connection via ibis with connection pooling.

Uses connection limiting and reuse for high-concurrency scenarios.
Note: ibis.postgres uses psycopg (v3), so we use a semaphore-based
approach to limit concurrent connections rather than true async pooling.
"""

from typing import Dict, Any, Optional
import asyncio
import ibis
from interlace.connections.base import BaseConnection
from interlace.utils.logging import get_logger
from interlace.utils.monitoring import get_connection_pool_metrics

logger = get_logger("interlace.connections.postgres")


class PostgresConnectionPool:
    """
    Connection pool manager for Postgres using semaphore-based limiting.
    
    Since ibis.postgres uses psycopg (v3), we use a semaphore to limit
    concurrent connections and create ibis connections on demand.
    """

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        database: str,
        max_size: int = 20,
        timeout: float = 10.0,
    ):
        """
        Initialize Postgres connection pool.
        
        Args:
            host: Database host
            port: Database port
            user: Database user
            password: Database password
            database: Database name
            max_size: Maximum concurrent connections
            timeout: Timeout for acquiring connection from pool
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.max_size = max_size
        self.timeout = timeout
        
        self._semaphore = asyncio.Semaphore(max_size)
        self._active_connections = 0
        self._lock = asyncio.Lock()
        self._metrics = {
            "active_connections": 0,
            "max_connections": max_size,
            "wait_time_seconds": 0.0,
            "connection_errors": 0,
            "pool_exhaustions": 0,
            "total_connections_created": 0,
        }

    async def get_connection(self) -> ibis.BaseBackend:
        """
        Get a connection from the pool (async).
        
        Uses semaphore to limit concurrent connections and creates
        ibis connection on demand.
        
        Returns:
            ibis.BaseBackend: ibis Postgres backend
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # Acquire semaphore (limits concurrent connections)
            await asyncio.wait_for(
                self._semaphore.acquire(), timeout=self.timeout
            )
            
            wait_time = asyncio.get_event_loop().time() - start_time
            # Update running average of wait time
            if self._metrics["wait_time_seconds"] == 0:
                self._metrics["wait_time_seconds"] = wait_time
            else:
                self._metrics["wait_time_seconds"] = (
                    self._metrics["wait_time_seconds"] * 0.9 + wait_time * 0.1
                )
            
            # Create ibis connection (synchronous, but we're limiting concurrency)
            try:
                ibis_conn = ibis.postgres.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                )
                
                async with self._lock:
                    self._active_connections += 1
                    self._metrics["active_connections"] = self._active_connections
                    self._metrics["total_connections_created"] += 1
                
                logger.debug(
                    f"Created Postgres connection "
                    f"(active={self._active_connections}/{self.max_size})"
                )
                
                return ibis_conn
                
            except Exception as e:
                # Release semaphore on error
                self._semaphore.release()
                self._metrics["connection_errors"] += 1
                logger.error(f"Failed to create Postgres connection: {e}")
                raise
                
        except asyncio.TimeoutError:
            self._metrics["pool_exhaustions"] += 1
            self._metrics["connection_errors"] += 1
            raise RuntimeError(
                f"Timeout waiting for Postgres connection from pool "
                f"(timeout={self.timeout}s, max_connections={self.max_size}, "
                f"active={self._active_connections})"
            )
        except Exception as e:
            self._metrics["connection_errors"] += 1
            logger.error(f"Failed to acquire Postgres connection from pool: {e}")
            raise

    async def return_connection(self, connection: ibis.BaseBackend):
        """
        Return a connection to the pool.
        
        Closes the ibis connection and releases the semaphore.
        
        Args:
            connection: ibis connection to return
        """
        try:
            if hasattr(connection, "close"):
                connection.close()
        except Exception as e:
            logger.debug(f"Error closing ibis connection: {e}")
        finally:
            # Always release semaphore
            self._semaphore.release()
            async with self._lock:
                self._active_connections = max(0, self._active_connections - 1)
                self._metrics["active_connections"] = self._active_connections

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get pool metrics.
        
        Returns:
            Dictionary with pool metrics
        """
        return self._metrics.copy()

    async def close(self):
        """Close the connection pool."""
        # Wait for all connections to be released
        for _ in range(self.max_size):
            try:
                await asyncio.wait_for(
                    self._semaphore.acquire(), timeout=1.0
                )
            except asyncio.TimeoutError:
                break
        
        logger.debug("Closed Postgres connection pool")


class PostgresConnection(BaseConnection):
    """
    Postgres connection wrapper using ibis with connection pooling.
    
    Uses semaphore-based connection limiting for high-concurrency scenarios.
    Falls back to single connection if pooling is disabled.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self._pool: Optional[PostgresConnectionPool] = None
        self._use_pool = config.get("pool", {}).get("enabled", True)
        
        # Pool configuration
        pool_config = config.get("pool", {})
        self._pool_max_size = pool_config.get("max_size", 20)
        self._pool_timeout = pool_config.get("timeout", 10.0)

    @property
    def connection(self) -> ibis.BaseBackend:
        """
        Get Postgres connection via ibis (lazy initialization).

        Returns:
            ibis.BaseBackend: ibis Postgres backend
        """
        if self._connection is None:
            db_config = self.config.get("config", {})

            # Build connection parameters
            host = db_config.get("host", "localhost")
            port = db_config.get("port", 5432)
            user = db_config.get("user", "")
            password = db_config.get("password", "")
            database = db_config.get("database", "")

            # Connect via ibis Postgres backend
            # If pooling is enabled, we'll use it via async methods
            # For sync access, create a single connection
            self._connection = ibis.postgres.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
            )

        return self._connection

    async def get_pooled_connection(self) -> ibis.BaseBackend:
        """
        Get a connection from the pool (async).
        
        Returns:
            ibis.BaseBackend: ibis Postgres backend from pool
        """
        if not self._use_pool:
            # Fallback to single connection
            return self.connection
        
        if self._pool is None:
            db_config = self.config.get("config", {})
            host = db_config.get("host", "localhost")
            port = db_config.get("port", 5432)
            user = db_config.get("user", "")
            password = db_config.get("password", "")
            database = db_config.get("database", "")
            
            self._pool = PostgresConnectionPool(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                max_size=self._pool_max_size,
                timeout=self._pool_timeout,
            )
            # Register pool for metrics tracking
            pool_metrics = get_connection_pool_metrics()
            pool_metrics.register_pool(f"postgres_{self.name}", self._pool)
        
        return await self._pool.get_connection()

    async def return_pooled_connection(self, connection: ibis.BaseBackend):
        """
        Return a connection to the pool.
        
        Args:
            connection: ibis connection to return
        """
        if self._pool:
            await self._pool.return_connection(connection)

    def get_pool_metrics(self) -> Dict[str, Any]:
        """
        Get connection pool metrics.
        
        Returns:
            Dictionary with pool metrics
        """
        if self._pool:
            return self._pool.get_metrics()
        return {}

    def close(self):
        """Close connection and pool.

        This must be synchronous so that ``BaseConnection.__exit__``
        works correctly.  The pool reference is simply dropped; any
        connections that were checked out remain usable until they are
        returned and garbage-collected.  For a graceful async drain use
        :meth:`close_async`.
        """
        self._pool = None
        super().close()

    async def close_async(self):
        """Async close that gracefully drains the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
        super().close()

    # execute() is inherited from BaseConnection
