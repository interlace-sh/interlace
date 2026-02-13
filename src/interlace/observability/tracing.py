"""
OpenTelemetry tracing for Interlace.

Phase 3: Distributed tracing for model execution, dependency loading, and materialization.

Usage:
    from interlace.observability import get_tracer, trace_model_execution

    tracer = get_tracer()
    tracer.enable(endpoint="http://localhost:4317")

    # Or use decorators
    @trace_model_execution
    async def execute_model(model_name, ...):
        ...
"""

import asyncio
import functools
from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.observability.tracing")

# Try to import OpenTelemetry (optional dependency)
try:
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.semconv.resource import ResourceAttributes

    OTEL_AVAILABLE = True

    # Try to import OTLP exporter
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        OTLP_AVAILABLE = True
    except ImportError:
        OTLP_AVAILABLE = False
        logger.debug("OTLP exporter not available. Install with: pip install opentelemetry-exporter-otlp")

except ImportError:
    OTEL_AVAILABLE = False
    OTLP_AVAILABLE = False
    logger.debug("OpenTelemetry not installed. Tracing will be disabled.")


class NoOpSpan:
    """No-op span for when tracing is disabled."""

    def __init__(self, name: str = ""):
        self.name = name

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def set_attribute(self, key: str, value: Any):
        pass

    def set_status(self, status):
        pass

    def record_exception(self, exception: Exception):
        pass

    def add_event(self, name: str, attributes: dict[str, Any] | None = None):
        pass


class NoOpTracer:
    """No-op tracer for when OpenTelemetry is not available."""

    def start_span(self, name: str, **kwargs) -> NoOpSpan:
        return NoOpSpan(name)

    @contextmanager
    def start_as_current_span(self, name: str, **kwargs):
        yield NoOpSpan(name)


class TracingManager:
    """
    Manages OpenTelemetry tracing for Interlace.

    Provides distributed tracing across model execution, dependency loading,
    and materialization operations.
    """

    def __init__(self, service_name: str = "interlace"):
        """
        Initialize tracing manager.

        Args:
            service_name: Service name for traces
        """
        self.service_name = service_name
        self._enabled = False
        self._tracer: Any = NoOpTracer()
        self._provider: Any | None = None

    def enable(
        self,
        endpoint: str | None = None,
        console: bool = False,
    ):
        """
        Enable tracing.

        Args:
            endpoint: OTLP endpoint (e.g., "http://localhost:4317")
            console: Enable console exporter for debugging
        """
        if not OTEL_AVAILABLE:
            logger.warning(
                "OpenTelemetry not installed. " "Install with: pip install opentelemetry-sdk opentelemetry-api"
            )
            return

        # Create resource with service info
        resource = Resource.create(
            {
                ResourceAttributes.SERVICE_NAME: self.service_name,
                ResourceAttributes.SERVICE_VERSION: "1.0.0",
            }
        )

        # Create tracer provider
        self._provider = TracerProvider(resource=resource)

        # Add exporters
        if endpoint and OTLP_AVAILABLE:
            otlp_exporter = OTLPSpanExporter(endpoint=endpoint)
            self._provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            logger.info(f"OTLP tracing enabled, sending to {endpoint}")
        elif endpoint and not OTLP_AVAILABLE:
            logger.warning(
                "OTLP endpoint specified but exporter not available. "
                "Install with: pip install opentelemetry-exporter-otlp"
            )

        if console:
            console_exporter = ConsoleSpanExporter()
            self._provider.add_span_processor(BatchSpanProcessor(console_exporter))
            logger.info("Console trace exporter enabled")

        # Set global tracer provider
        trace.set_tracer_provider(self._provider)

        # Get tracer
        self._tracer = trace.get_tracer(self.service_name)
        self._enabled = True
        logger.info("Tracing enabled")

    def disable(self):
        """Disable tracing."""
        self._enabled = False
        self._tracer = NoOpTracer()
        if self._provider and hasattr(self._provider, "shutdown"):
            self._provider.shutdown()
        logger.info("Tracing disabled")

    @property
    def enabled(self) -> bool:
        """Check if tracing is enabled."""
        return self._enabled

    @property
    def tracer(self):
        """Get the tracer instance."""
        return self._tracer

    @contextmanager
    def span(
        self,
        name: str,
        attributes: dict[str, Any] | None = None,
    ):
        """
        Create a span context manager.

        Args:
            name: Span name
            attributes: Optional span attributes

        Yields:
            Span object
        """
        if not self._enabled:
            yield NoOpSpan(name)
            return

        with self._tracer.start_as_current_span(name) as span:
            if attributes:
                for key, value in attributes.items():
                    if value is not None:
                        span.set_attribute(key, str(value))
            yield span

    def model_execution_span(
        self,
        model_name: str,
        model_type: str = "python",
        materialise: str = "table",
        schema: str = "default",
    ):
        """
        Create a span for model execution.

        Args:
            model_name: Model name
            model_type: Model type (python, sql, stream)
            materialise: Materialization type
            schema: Schema name

        Returns:
            Span context manager
        """
        return self.span(
            f"model.execute.{model_name}",
            attributes={
                "interlace.model.name": model_name,
                "interlace.model.type": model_type,
                "interlace.model.materialise": materialise,
                "interlace.model.schema": schema,
            },
        )

    def dependency_loading_span(
        self,
        model_name: str,
        dependencies: list | None = None,
    ):
        """
        Create a span for dependency loading.

        Args:
            model_name: Model name
            dependencies: List of dependency names

        Returns:
            Span context manager
        """
        return self.span(
            f"model.load_dependencies.{model_name}",
            attributes={
                "interlace.model.name": model_name,
                "interlace.dependencies.count": len(dependencies) if dependencies else 0,
                "interlace.dependencies.names": ",".join(dependencies) if dependencies else "",
            },
        )

    def materialization_span(
        self,
        model_name: str,
        materialise: str,
        strategy: str | None = None,
    ):
        """
        Create a span for materialization.

        Args:
            model_name: Model name
            materialise: Materialization type
            strategy: Strategy name (for table materialization)

        Returns:
            Span context manager
        """
        attributes = {
            "interlace.model.name": model_name,
            "interlace.materialise.type": materialise,
        }
        if strategy:
            attributes["interlace.materialise.strategy"] = strategy

        return self.span(f"model.materialise.{model_name}", attributes=attributes)

    def query_span(
        self,
        query_type: str,
        table_name: str | None = None,
    ):
        """
        Create a span for database query.

        Args:
            query_type: Query type (select, insert, update, delete, ddl)
            table_name: Table name

        Returns:
            Span context manager
        """
        attributes = {"db.operation": query_type}
        if table_name:
            attributes["db.table"] = table_name

        return self.span(f"db.{query_type}", attributes=attributes)


# Global tracing manager instance
_tracing_manager: TracingManager | None = None


def get_tracer(service_name: str = "interlace") -> TracingManager:
    """
    Get global tracing manager instance.

    Args:
        service_name: Service name for traces

    Returns:
        TracingManager instance
    """
    global _tracing_manager
    if _tracing_manager is None:
        _tracing_manager = TracingManager(service_name=service_name)
    return _tracing_manager


# Decorators for tracing
def trace_model_execution(func: Callable | None = None, name: str | None = None):
    """
    Decorator to trace model execution.

    Usage:
        @trace_model_execution
        async def execute_model(model_name, ...):
            ...

        @trace_model_execution(name="custom_name")
        def execute_model_sync(model_name, ...):
            ...
    """

    def decorator(f: Callable) -> Callable:
        trace_name = name or f.__name__

        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            tracer = get_tracer()
            model_name = kwargs.get("model_name") or (args[0] if args else "unknown")
            with tracer.span(f"model.execute.{trace_name}", attributes={"model": model_name}):
                return await f(*args, **kwargs)

        @functools.wraps(f)
        def sync_wrapper(*args, **kwargs):
            tracer = get_tracer()
            model_name = kwargs.get("model_name") or (args[0] if args else "unknown")
            with tracer.span(f"model.execute.{trace_name}", attributes={"model": model_name}):
                return f(*args, **kwargs)

        if asyncio.iscoroutinefunction(f):
            return async_wrapper
        else:
            return sync_wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)


def trace_dependency_loading(func: Callable | None = None, name: str | None = None):
    """
    Decorator to trace dependency loading.

    Usage:
        @trace_dependency_loading
        async def load_dependencies(model_name, ...):
            ...
    """

    def decorator(f: Callable) -> Callable:

        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            tracer = get_tracer()
            model_name = kwargs.get("model_name") or "unknown"
            with tracer.dependency_loading_span(model_name):
                return await f(*args, **kwargs)

        @functools.wraps(f)
        def sync_wrapper(*args, **kwargs):
            tracer = get_tracer()
            model_name = kwargs.get("model_name") or "unknown"
            with tracer.dependency_loading_span(model_name):
                return f(*args, **kwargs)

        if asyncio.iscoroutinefunction(f):
            return async_wrapper
        else:
            return sync_wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)


def trace_materialization(func: Callable | None = None, name: str | None = None):
    """
    Decorator to trace materialization.

    Usage:
        @trace_materialization
        async def materialise(model_name, materialise_type, ...):
            ...
    """

    def decorator(f: Callable) -> Callable:

        @functools.wraps(f)
        async def async_wrapper(*args, **kwargs):
            tracer = get_tracer()
            model_name = kwargs.get("model_name") or "unknown"
            materialise_type = kwargs.get("materialise") or "table"
            with tracer.materialization_span(model_name, materialise_type):
                return await f(*args, **kwargs)

        @functools.wraps(f)
        def sync_wrapper(*args, **kwargs):
            tracer = get_tracer()
            model_name = kwargs.get("model_name") or "unknown"
            materialise_type = kwargs.get("materialise") or "table"
            with tracer.materialization_span(model_name, materialise_type):
                return f(*args, **kwargs)

        if asyncio.iscoroutinefunction(f):
            return async_wrapper
        else:
            return sync_wrapper

    if func is None:
        return decorator
    else:
        return decorator(func)
