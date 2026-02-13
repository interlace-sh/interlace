"""
Monitoring and resource limit utilities.

Provides hooks for monitoring execution and enforcing resource limits.
"""

import asyncio
import functools
import os
import time
from collections.abc import Callable
from datetime import datetime
from typing import Any

try:
    import psutil
except ImportError:
    psutil = None  # type: ignore

from interlace.utils.logging import get_logger

logger = get_logger("interlace.monitoring")


class ResourceMonitor:
    """Monitor resource usage and enforce limits."""

    def __init__(self, config: dict[str, Any] | None = None):
        """
        Initialize resource monitor.

        Args:
            config: Configuration dictionary with limits:
                - max_memory_mb: Maximum memory usage in MB
                - max_execution_time_seconds: Maximum execution time per model
                - max_connections: Maximum number of connections
        """
        self.config = config or {}
        self.max_memory_mb = self.config.get("max_memory_mb")
        self.max_execution_time_seconds = self.config.get("max_execution_time_seconds")
        self.max_connections = self.config.get("max_connections")
        if psutil is None:
            raise ImportError("psutil is required for ResourceMonitor. Install it with: pip install psutil")
        self.process = psutil.Process(os.getpid())
        self.start_time = datetime.now()
        self._connection_count = 0

    def check_memory(self) -> dict[str, Any]:
        """
        Check current memory usage.

        Returns:
            Dictionary with memory usage information
        """
        memory_info = self.process.memory_info()
        memory_mb = memory_info.rss / 1024 / 1024

        result = {
            "memory_mb": memory_mb,
            "memory_limit_mb": self.max_memory_mb,
            "within_limit": True,
        }

        if self.max_memory_mb and memory_mb > self.max_memory_mb:
            result["within_limit"] = False
            logger.warning(f"Memory usage ({memory_mb:.1f} MB) exceeds limit " f"({self.max_memory_mb} MB)")

        return result

    def check_execution_time(self, start_time: datetime) -> dict[str, Any]:
        """
        Check execution time against limit.

        Args:
            start_time: Start time of execution

        Returns:
            Dictionary with execution time information
        """
        elapsed = (datetime.now() - start_time).total_seconds()

        result = {
            "elapsed_seconds": elapsed,
            "time_limit_seconds": self.max_execution_time_seconds,
            "within_limit": True,
        }

        if self.max_execution_time_seconds and elapsed > self.max_execution_time_seconds:
            result["within_limit"] = False
            logger.warning(f"Execution time ({elapsed:.1f}s) exceeds limit " f"({self.max_execution_time_seconds}s)")

        return result

    def track_connection(self, action: str = "open") -> dict[str, Any]:
        """
        Track connection usage.

        Args:
            action: "open" or "close"

        Returns:
            Dictionary with connection count information
        """
        if action == "open":
            self._connection_count += 1
        elif action == "close":
            self._connection_count = max(0, self._connection_count - 1)

        result = {
            "connection_count": self._connection_count,
            "connection_limit": self.max_connections,
            "within_limit": True,
        }

        if self.max_connections and self._connection_count > self.max_connections:
            result["within_limit"] = False
            logger.warning(f"Connection count ({self._connection_count}) exceeds limit " f"({self.max_connections})")

        return result

    def get_metrics(self) -> dict[str, Any]:
        """
        Get all current metrics.

        Returns:
            Dictionary with all monitoring metrics
        """
        memory = self.check_memory()
        uptime = (datetime.now() - self.start_time).total_seconds()
        connections = self.track_connection()  # Just get current state

        return {
            "memory": memory,
            "uptime_seconds": uptime,
            "connections": {
                "count": connections["connection_count"],
                "limit": connections["connection_limit"],
            },
        }


def get_resource_monitor(config: dict[str, Any] | None = None) -> ResourceMonitor:
    """
    Get or create resource monitor instance.

    Args:
        config: Configuration dictionary

    Returns:
        ResourceMonitor instance
    """
    # For now, create new instance each time
    # In future, could use singleton pattern
    return ResourceMonitor(config)


class ConnectionPoolMetrics:
    """Track connection pool metrics."""

    def __init__(self):
        """Initialize metrics tracker."""
        self.pools: dict[str, Any] = {}  # pool_name -> pool instance

    def register_pool(self, name: str, pool: Any):
        """
        Register a connection pool for metrics tracking.

        Args:
            name: Pool name
            pool: Pool instance (must have get_metrics() method)
        """
        self.pools[name] = pool

    def get_all_metrics(self) -> dict[str, Any]:
        """
        Get metrics from all registered pools.

        Returns:
            Dictionary mapping pool names to their metrics
        """
        metrics = {}
        for name, pool in self.pools.items():
            if hasattr(pool, "get_metrics"):
                try:
                    metrics[name] = pool.get_metrics()
                except Exception as e:
                    logger.debug(f"Error getting metrics for pool {name}: {e}")
        return metrics

    def get_pool_metrics(self, name: str) -> dict[str, Any] | None:
        """
        Get metrics for a specific pool.

        Args:
            name: Pool name

        Returns:
            Dictionary with pool metrics or None if pool not found
        """
        pool = self.pools.get(name)
        if pool and hasattr(pool, "get_metrics"):
            try:
                return pool.get_metrics()
            except Exception as e:
                logger.debug(f"Error getting metrics for pool {name}: {e}")
        return None


# Global connection pool metrics instance
_connection_pool_metrics: ConnectionPoolMetrics | None = None


def get_connection_pool_metrics() -> ConnectionPoolMetrics:
    """
    Get global connection pool metrics instance.

    Returns:
        ConnectionPoolMetrics instance
    """
    global _connection_pool_metrics
    if _connection_pool_metrics is None:
        _connection_pool_metrics = ConnectionPoolMetrics()
    return _connection_pool_metrics


def timed(func: Callable | None = None, name: str | None = None, log: bool = False):
    """
    Decorator to time function execution.

    Args:
        func: Function to time (if used as decorator without parentheses)
        name: Optional name for the timing (defaults to function name)
        log: Whether to log timing (default: False)

    Usage:
        @timed
        def my_function():
            ...

        @timed(name="custom_name", log=True)
        async def my_async_function():
            ...
    """

    def decorator(f: Callable) -> Callable:
        timing_name = name or f.__name__

        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = await f(*args, **kwargs)
                elapsed = time.time() - start_time
                if log:
                    logger.debug(f"{timing_name} took {elapsed:.3f}s")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                if log:
                    logger.debug(f"{timing_name} failed after {elapsed:.3f}s: {e}")
                raise

        @functools.wraps(f)
        def sync_wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = f(*args, **kwargs)
                elapsed = time.time() - start_time
                if log:
                    logger.debug(f"{timing_name} took {elapsed:.3f}s")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                if log:
                    logger.debug(f"{timing_name} failed after {elapsed:.3f}s: {e}")
                raise

        # Return appropriate wrapper based on whether function is async
        if asyncio.iscoroutinefunction(f):
            return async_wrapper
        else:
            return sync_wrapper

    # Support both @timed and @timed() usage
    if func is None:
        return decorator
    else:
        return decorator(func)


class PerformanceMonitor:
    """
    Track execution metrics for performance analysis.

    Collects timing, database query counts, cache hit rates, and memory usage.
    """

    def __init__(self, enabled: bool = True):
        """
        Initialize performance monitor.

        Args:
            enabled: Whether monitoring is enabled
        """
        self.enabled = enabled
        self.metrics: dict[str, Any] = {
            "model_execution_times": {},  # model_name -> [times]
            "dependency_loading_times": {},  # model_name -> time
            "schema_validation_times": {},  # model_name -> time
            "materialization_times": {},  # model_name -> time
            "database_queries": {
                "count": 0,
                "total_duration": 0.0,
            },
            "cache_stats": {
                "schema_cache": {"hits": 0, "misses": 0},
                "existence_cache": {"hits": 0, "misses": 0},
                "dependency_cache": {"hits": 0, "misses": 0},
            },
            "memory_usage": {
                "peak_mb": 0.0,
                "average_mb": 0.0,
            },
            "parallelism": {
                "max_concurrent_models": 0,
                "average_concurrent_models": 0.0,
            },
        }
        self._start_time = time.time()
        self._memory_samples = []
        self._concurrent_models = 0
        self._max_concurrent = 0

    def record_model_execution(self, model_name: str, duration: float):
        """Record model execution time."""
        if not self.enabled:
            return
        if model_name not in self.metrics["model_execution_times"]:
            self.metrics["model_execution_times"][model_name] = []
        self.metrics["model_execution_times"][model_name].append(duration)

    def record_dependency_loading(self, model_name: str, duration: float):
        """Record dependency loading time."""
        if not self.enabled:
            return
        self.metrics["dependency_loading_times"][model_name] = duration

    def record_schema_validation(self, model_name: str, duration: float):
        """Record schema validation time."""
        if not self.enabled:
            return
        self.metrics["schema_validation_times"][model_name] = duration

    def record_materialization(self, model_name: str, duration: float):
        """Record materialization time."""
        if not self.enabled:
            return
        self.metrics["materialization_times"][model_name] = duration

    def record_database_query(self, duration: float):
        """Record database query execution."""
        if not self.enabled:
            return
        self.metrics["database_queries"]["count"] += 1
        self.metrics["database_queries"]["total_duration"] += duration

    def record_cache_hit(self, cache_type: str):
        """Record cache hit."""
        if not self.enabled:
            return
        if cache_type in self.metrics["cache_stats"]:
            self.metrics["cache_stats"][cache_type]["hits"] += 1

    def record_cache_miss(self, cache_type: str):
        """Record cache miss."""
        if not self.enabled:
            return
        if cache_type in self.metrics["cache_stats"][cache_type]:
            self.metrics["cache_stats"][cache_type]["misses"] += 1

    def sample_memory(self):
        """Sample current memory usage."""
        if not self.enabled:
            return
        try:
            if psutil is None:
                logger.warning("psutil not available, skipping memory sampling")
                return
            process = psutil.Process(os.getpid())
            memory_mb = process.memory_info().rss / 1024 / 1024
            self._memory_samples.append(memory_mb)
            if memory_mb > self.metrics["memory_usage"]["peak_mb"]:
                self.metrics["memory_usage"]["peak_mb"] = memory_mb
        except Exception:
            pass

    def update_concurrent_models(self, count: int):
        """Update concurrent model count."""
        if not self.enabled:
            return
        self._concurrent_models = count
        if count > self._max_concurrent:
            self._max_concurrent = count

    def get_report(self) -> dict[str, Any]:
        """
        Generate performance report.

        Returns:
            Dictionary with performance metrics and summary
        """
        if not self.enabled:
            return {}

        # Calculate averages
        total_duration = time.time() - self._start_time

        # Average memory
        if self._memory_samples:
            self.metrics["memory_usage"]["average_mb"] = sum(self._memory_samples) / len(self._memory_samples)

        # Parallelism metrics
        self.metrics["parallelism"]["max_concurrent_models"] = self._max_concurrent

        # Model execution summary
        model_times = self.metrics["model_execution_times"]
        total_model_time = sum(sum(times) for times in model_times.values())
        avg_model_time = total_model_time / len(model_times) if model_times else 0

        # Cache hit rates
        cache_stats = self.metrics["cache_stats"]
        cache_hit_rates = {}
        for cache_type, stats in cache_stats.items():
            total = stats["hits"] + stats["misses"]
            if total > 0:
                cache_hit_rates[cache_type] = stats["hits"] / total
            else:
                cache_hit_rates[cache_type] = 0.0

        return {
            "total_duration_seconds": total_duration,
            "model_execution": {
                "total_models": len(model_times),
                "total_time_seconds": total_model_time,
                "average_time_seconds": avg_model_time,
            },
            "dependency_loading": {
                "total_time_seconds": sum(self.metrics["dependency_loading_times"].values()),
                "average_time_seconds": (
                    sum(self.metrics["dependency_loading_times"].values())
                    / len(self.metrics["dependency_loading_times"])
                    if self.metrics["dependency_loading_times"]
                    else 0
                ),
            },
            "database_queries": {
                "count": self.metrics["database_queries"]["count"],
                "total_duration_seconds": self.metrics["database_queries"]["total_duration"],
                "average_duration_seconds": (
                    self.metrics["database_queries"]["total_duration"] / self.metrics["database_queries"]["count"]
                    if self.metrics["database_queries"]["count"] > 0
                    else 0
                ),
            },
            "cache_hit_rates": cache_hit_rates,
            "memory_usage": self.metrics["memory_usage"],
            "parallelism": self.metrics["parallelism"],
        }


# Global performance monitor instance
_performance_monitor: PerformanceMonitor | None = None


def get_performance_monitor(enabled: bool = True) -> PerformanceMonitor:
    """
    Get global performance monitor instance.

    Args:
        enabled: Whether monitoring is enabled

    Returns:
        PerformanceMonitor instance
    """
    global _performance_monitor
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor(enabled=enabled)
    return _performance_monitor
