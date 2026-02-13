"""
Observability module for Interlace.

Phase 3: Prometheus metrics, OpenTelemetry tracing, and structured logging.
"""

from interlace.observability.metrics import (
    MetricsRegistry,
    connection_pool_gauge,
    dlq_counter,
    get_metrics_registry,
    model_execution_histogram,
    model_rows_counter,
    retry_counter,
)
from interlace.observability.structured_logging import (
    StructuredFormatter,
    add_correlation_id,
    get_correlation_id,
    setup_structured_logging,
)
from interlace.observability.tracing import (
    TracingManager,
    get_tracer,
    trace_dependency_loading,
    trace_materialization,
    trace_model_execution,
)

__all__ = [
    # Metrics
    "MetricsRegistry",
    "get_metrics_registry",
    "model_execution_histogram",
    "model_rows_counter",
    "connection_pool_gauge",
    "retry_counter",
    "dlq_counter",
    # Tracing
    "TracingManager",
    "get_tracer",
    "trace_model_execution",
    "trace_dependency_loading",
    "trace_materialization",
    # Structured Logging
    "StructuredFormatter",
    "setup_structured_logging",
    "add_correlation_id",
    "get_correlation_id",
]
