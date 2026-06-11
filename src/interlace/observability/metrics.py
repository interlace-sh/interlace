"""
Prometheus metrics for Interlace.

Phase 3: Export metrics for model execution, connection pools, and retries.

Usage:
    from interlace.observability import get_metrics_registry

    registry = get_metrics_registry()
    registry.enable()  # Enable metrics collection

    # Metrics are automatically recorded by the executor
    # Start metrics server for Prometheus scraping:
    registry.start_http_server(port=9090)
"""

import threading
import time
from contextlib import contextmanager
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.observability.metrics")

# Try to import prometheus_client (optional dependency)
try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        CollectorRegistry,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
        start_http_server,
    )

    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    logger.debug("prometheus_client not installed. Metrics will use internal tracking only.")


class MetricsRegistry:
    """
    Central registry for all Interlace metrics.

    Supports both Prometheus export and internal tracking.
    When prometheus_client is not available, metrics are tracked internally
    and can be retrieved via get_metrics().
    """

    def __init__(self) -> None:
        """Initialize metrics registry."""
        self._enabled = False
        self._internal_metrics: dict[str, Any] = {
            "model_execution_seconds": {},  # model -> [durations]
            "model_rows_total": {},  # model -> count
            "connection_pool_active": {},  # pool_name -> count
            "connection_pool_max": {},  # pool_name -> max
            "retry_total": {},  # model -> {success: N, failure: N}
        }
        self._lock = threading.Lock()

        # Prometheus metrics (if available)
        if PROMETHEUS_AVAILABLE:
            self._registry = CollectorRegistry()
            self._setup_prometheus_metrics()
        else:
            self._registry = None

    def _setup_prometheus_metrics(self) -> None:
        """Setup Prometheus metrics."""
        # Model execution duration histogram
        self._model_execution_histogram = Histogram(
            "interlace_model_execution_duration_seconds",
            "Model execution duration in seconds",
            ["model", "status", "materialise"],
            registry=self._registry,
            buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0),
        )

        # Rows processed counter
        self._model_rows_counter = Counter(
            "interlace_model_rows_total",
            "Total rows processed by model",
            ["model", "operation"],  # operation: ingested, inserted, updated, deleted
            registry=self._registry,
        )

        # Connection pool gauges
        self._connection_pool_active_gauge = Gauge(
            "interlace_connection_pool_active",
            "Active connections in pool",
            ["connection"],
            registry=self._registry,
        )

        self._connection_pool_max_gauge = Gauge(
            "interlace_connection_pool_max",
            "Maximum connections in pool",
            ["connection"],
            registry=self._registry,
        )

        self._connection_pool_wait_gauge = Gauge(
            "interlace_connection_pool_wait_seconds",
            "Average wait time for connection from pool",
            ["connection"],
            registry=self._registry,
        )

        # Retry counter
        self._retry_counter = Counter(
            "interlace_retry_total",
            "Total retry attempts",
            ["model", "outcome"],  # outcome: success, failure, exhausted
            registry=self._registry,
        )

        # Flow metrics
        self._flow_status_gauge = Gauge(
            "interlace_flow_status",
            "Current flow status (0=pending, 1=running, 2=completed, 3=failed)",
            ["flow_id"],
            registry=self._registry,
        )

        self._task_status_gauge = Gauge(
            "interlace_task_status",
            "Current task status",
            ["flow_id", "model"],
            registry=self._registry,
        )

    def enable(self) -> None:
        """Enable metrics collection."""
        self._enabled = True
        logger.info("Metrics collection enabled")

    def disable(self) -> None:
        """Disable metrics collection."""
        self._enabled = False
        logger.info("Metrics collection disabled")

    @property
    def enabled(self) -> bool:
        """Check if metrics collection is enabled."""
        return self._enabled

    def record_model_execution(
        self,
        model: str,
        duration: float,
        status: str = "success",
        materialise: str = "table",
    ) -> None:
        """
        Record model execution metrics.

        Args:
            model: Model name
            duration: Execution duration in seconds
            status: Execution status (success, error, skipped)
            materialise: Materialization type (table, view, ephemeral)
        """
        if not self._enabled:
            return

        with self._lock:
            # Internal tracking
            if model not in self._internal_metrics["model_execution_seconds"]:
                self._internal_metrics["model_execution_seconds"][model] = []
            self._internal_metrics["model_execution_seconds"][model].append(
                {
                    "duration": duration,
                    "status": status,
                    "timestamp": time.time(),
                }
            )

        # Prometheus tracking
        if PROMETHEUS_AVAILABLE and self._registry:
            self._model_execution_histogram.labels(model=model, status=status, materialise=materialise).observe(
                duration
            )

    def record_rows(
        self,
        model: str,
        operation: str,
        count: int,
    ) -> None:
        """
        Record row counts.

        Args:
            model: Model name
            operation: Operation type (ingested, inserted, updated, deleted)
            count: Number of rows
        """
        if not self._enabled or count is None:
            return

        with self._lock:
            key = f"{model}_{operation}"
            self._internal_metrics["model_rows_total"][key] = (
                self._internal_metrics["model_rows_total"].get(key, 0) + count
            )

        if PROMETHEUS_AVAILABLE and self._registry:
            self._model_rows_counter.labels(model=model, operation=operation).inc(count)

    def record_connection_pool(
        self,
        connection: str,
        active: int,
        max_size: int,
        wait_time: float = 0.0,
    ) -> None:
        """
        Record connection pool metrics.

        Args:
            connection: Connection name
            active: Active connections
            max_size: Maximum pool size
            wait_time: Average wait time in seconds
        """
        if not self._enabled:
            return

        with self._lock:
            self._internal_metrics["connection_pool_active"][connection] = active
            self._internal_metrics["connection_pool_max"][connection] = max_size

        if PROMETHEUS_AVAILABLE and self._registry:
            self._connection_pool_active_gauge.labels(connection=connection).set(active)
            self._connection_pool_max_gauge.labels(connection=connection).set(max_size)
            self._connection_pool_wait_gauge.labels(connection=connection).set(wait_time)

    def record_retry(
        self,
        model: str,
        outcome: str,
    ) -> None:
        """
        Record retry attempt.

        Args:
            model: Model name
            outcome: Outcome (success, failure, exhausted)
        """
        if not self._enabled:
            return

        with self._lock:
            if model not in self._internal_metrics["retry_total"]:
                self._internal_metrics["retry_total"][model] = {}
            self._internal_metrics["retry_total"][model][outcome] = (
                self._internal_metrics["retry_total"][model].get(outcome, 0) + 1
            )

        if PROMETHEUS_AVAILABLE and self._registry:
            self._retry_counter.labels(model=model, outcome=outcome).inc()

    @contextmanager
    def time_model_execution(
        self,
        model: str,
        materialise: str = "table",
    ) -> Any:
        """
        Context manager to time model execution.

        Usage:
            with metrics.time_model_execution("my_model"):
                # execute model
                pass
        """
        start_time = time.time()
        status = "success"
        try:
            yield
        except Exception:
            status = "error"
            raise
        finally:
            duration = time.time() - start_time
            self.record_model_execution(
                model=model,
                duration=duration,
                status=status,
                materialise=materialise,
            )

    def get_metrics(self) -> dict[str, Any]:
        """
        Get all internal metrics.

        Returns:
            Dictionary with all tracked metrics
        """
        with self._lock:
            return {
                "model_execution_seconds": dict(self._internal_metrics["model_execution_seconds"]),
                "model_rows_total": dict(self._internal_metrics["model_rows_total"]),
                "connection_pool_active": dict(self._internal_metrics["connection_pool_active"]),
                "connection_pool_max": dict(self._internal_metrics["connection_pool_max"]),
                "retry_total": dict(self._internal_metrics["retry_total"]),
            }

    def start_http_server(self, port: int = 9090, addr: str = "") -> None:
        """
        Start HTTP server for Prometheus scraping.

        Args:
            port: Port to listen on
            addr: Address to bind to (empty string for all interfaces)
        """
        if not PROMETHEUS_AVAILABLE:
            logger.warning("prometheus_client not installed. " "Install with: pip install prometheus-client")
            return

        start_http_server(port=port, addr=addr, registry=self._registry)
        logger.info(f"Prometheus metrics server started on port {port}")

    def generate_prometheus_metrics(self) -> bytes:
        """
        Generate Prometheus metrics in text format.

        Returns:
            Prometheus metrics as bytes
        """
        if not PROMETHEUS_AVAILABLE:
            return b"# prometheus_client not installed\n"

        return generate_latest(self._registry)  # type: ignore[no-any-return]

    def get_content_type(self) -> str:
        """Get Prometheus content type for HTTP response."""
        if PROMETHEUS_AVAILABLE:
            return CONTENT_TYPE_LATEST  # type: ignore[no-any-return]
        return "text/plain; charset=utf-8"


# Global metrics registry instance
_metrics_registry: MetricsRegistry | None = None


def get_metrics_registry() -> MetricsRegistry:
    """
    Get global metrics registry instance.

    Returns:
        MetricsRegistry instance
    """
    global _metrics_registry
    if _metrics_registry is None:
        _metrics_registry = MetricsRegistry()
    return _metrics_registry


# Convenience functions for common metrics
def model_execution_histogram(
    model: str,
    duration: float,
    status: str = "success",
    materialise: str = "table",
) -> None:
    """Record model execution histogram."""
    get_metrics_registry().record_model_execution(model, duration, status, materialise)


def model_rows_counter(model: str, operation: str, count: int) -> None:
    """Record model rows counter."""
    get_metrics_registry().record_rows(model, operation, count)


def connection_pool_gauge(connection: str, active: int, max_size: int, wait_time: float = 0.0) -> None:
    """Record connection pool gauge."""
    get_metrics_registry().record_connection_pool(connection, active, max_size, wait_time)


def retry_counter(model: str, outcome: str) -> None:
    """Record retry counter."""
    get_metrics_registry().record_retry(model, outcome)
