"""
Tests for the observability module.

Phase 3: Tests for metrics, tracing, and structured logging.
"""

import json
import logging

import pytest

from interlace.observability.metrics import (
    MetricsRegistry,
    get_metrics_registry,
    model_execution_histogram,
    retry_counter,
)
from interlace.observability.structured_logging import (
    HumanReadableFormatter,
    LogContext,
    StructuredFormatter,
    add_correlation_id,
    get_correlation_id,
    set_correlation_id,
    setup_structured_logging,
)
from interlace.observability.tracing import (
    NoOpSpan,
    NoOpTracer,
    TracingManager,
    trace_model_execution,
)


class TestMetricsRegistry:
    """Tests for MetricsRegistry."""

    def test_default_disabled(self):
        """Test metrics are disabled by default."""
        registry = MetricsRegistry()
        assert registry.enabled is False

    def test_enable_disable(self):
        """Test enable/disable metrics."""
        registry = MetricsRegistry()
        registry.enable()
        assert registry.enabled is True
        registry.disable()
        assert registry.enabled is False

    def test_record_model_execution(self):
        """Test recording model execution."""
        registry = MetricsRegistry()
        registry.enable()

        registry.record_model_execution(
            model="test_model",
            duration=1.5,
            status="success",
            materialise="table",
        )

        metrics = registry.get_metrics()
        assert "test_model" in metrics["model_execution_seconds"]
        assert len(metrics["model_execution_seconds"]["test_model"]) == 1
        assert metrics["model_execution_seconds"]["test_model"][0]["duration"] == 1.5

    def test_record_model_execution_disabled(self):
        """Test no recording when disabled."""
        registry = MetricsRegistry()  # Disabled by default

        registry.record_model_execution(
            model="test_model",
            duration=1.5,
            status="success",
        )

        metrics = registry.get_metrics()
        assert len(metrics["model_execution_seconds"]) == 0

    def test_record_rows(self):
        """Test recording row counts."""
        registry = MetricsRegistry()
        registry.enable()

        registry.record_rows(model="test_model", operation="inserted", count=100)
        registry.record_rows(model="test_model", operation="inserted", count=50)

        metrics = registry.get_metrics()
        assert metrics["model_rows_total"]["test_model_inserted"] == 150

    def test_record_connection_pool(self):
        """Test recording connection pool metrics."""
        registry = MetricsRegistry()
        registry.enable()

        registry.record_connection_pool(
            connection="postgres_prod",
            active=5,
            max_size=20,
            wait_time=0.1,
        )

        metrics = registry.get_metrics()
        assert metrics["connection_pool_active"]["postgres_prod"] == 5
        assert metrics["connection_pool_max"]["postgres_prod"] == 20

    def test_record_retry(self):
        """Test recording retry metrics."""
        registry = MetricsRegistry()
        registry.enable()

        registry.record_retry(model="api_model", outcome="success")
        registry.record_retry(model="api_model", outcome="failure")
        registry.record_retry(model="api_model", outcome="success")

        metrics = registry.get_metrics()
        assert metrics["retry_total"]["api_model"]["success"] == 2
        assert metrics["retry_total"]["api_model"]["failure"] == 1

    def test_record_dlq_entry(self):
        """Test recording DLQ entries."""
        registry = MetricsRegistry()
        registry.enable()

        registry.record_dlq_entry(model="failed_model", count=1)
        registry.record_dlq_entry(model="failed_model", count=1)

        metrics = registry.get_metrics()
        assert metrics["dlq_entries"]["failed_model"] == 2

    def test_time_model_execution_context_manager(self):
        """Test timing context manager."""
        registry = MetricsRegistry()
        registry.enable()

        import time

        with registry.time_model_execution(model="timed_model"):
            time.sleep(0.01)

        metrics = registry.get_metrics()
        assert "timed_model" in metrics["model_execution_seconds"]
        assert metrics["model_execution_seconds"]["timed_model"][0]["duration"] >= 0.01


class TestConvenienceFunctions:
    """Tests for convenience metric functions."""

    def test_model_execution_histogram(self):
        """Test model_execution_histogram function."""
        # Reset global registry
        import interlace.observability.metrics as metrics_module

        metrics_module._metrics_registry = None

        registry = get_metrics_registry()
        registry.enable()

        model_execution_histogram("test", 1.0, "success", "table")

        metrics = registry.get_metrics()
        assert "test" in metrics["model_execution_seconds"]

    def test_retry_counter(self):
        """Test retry_counter function."""
        import interlace.observability.metrics as metrics_module

        metrics_module._metrics_registry = None

        registry = get_metrics_registry()
        registry.enable()

        retry_counter("model", "success")

        metrics = registry.get_metrics()
        assert "model" in metrics["retry_total"]


class TestTracingManager:
    """Tests for TracingManager."""

    def test_default_disabled(self):
        """Test tracing is disabled by default."""
        manager = TracingManager()
        assert manager.enabled is False

    def test_noop_tracer_when_disabled(self):
        """Test NoOp tracer when disabled."""
        manager = TracingManager()

        with manager.span("test_span") as span:
            assert isinstance(span, NoOpSpan)
            span.set_attribute("key", "value")  # Should not raise
            span.add_event("test_event")  # Should not raise

    def test_span_context_manager(self):
        """Test span context manager."""
        manager = TracingManager()
        manager._enabled = True  # Enable without OpenTelemetry

        with manager.span("test_span", attributes={"key": "value"}) as span:
            # When OTEL not available, returns NoOpSpan
            assert span is not None

    def test_model_execution_span(self):
        """Test model execution span creation."""
        manager = TracingManager()

        with manager.model_execution_span(
            model_name="users",
            model_type="python",
            materialise="table",
            schema="public",
        ) as span:
            assert span is not None

    def test_dependency_loading_span(self):
        """Test dependency loading span creation."""
        manager = TracingManager()

        with manager.dependency_loading_span(
            model_name="users",
            dependencies=["orders", "products"],
        ) as span:
            assert span is not None

    def test_materialization_span(self):
        """Test materialization span creation."""
        manager = TracingManager()

        with manager.materialization_span(
            model_name="users",
            materialise="table",
            strategy="merge_by_key",
        ) as span:
            assert span is not None


class TestNoOpTracer:
    """Tests for NoOpTracer."""

    def test_start_span(self):
        """Test start_span returns NoOpSpan."""
        tracer = NoOpTracer()
        span = tracer.start_span("test")
        assert isinstance(span, NoOpSpan)

    def test_start_as_current_span(self):
        """Test context manager returns NoOpSpan."""
        tracer = NoOpTracer()
        with tracer.start_as_current_span("test") as span:
            assert isinstance(span, NoOpSpan)


class TestNoOpSpan:
    """Tests for NoOpSpan."""

    def test_context_manager(self):
        """Test NoOpSpan as context manager."""
        span = NoOpSpan("test")
        with span as s:
            assert s is span

    def test_methods_do_not_raise(self):
        """Test NoOpSpan methods don't raise."""
        span = NoOpSpan("test")
        span.set_attribute("key", "value")
        span.set_status(None)
        span.record_exception(Exception("test"))
        span.add_event("test", {"key": "value"})


class TestTraceDecorators:
    """Tests for trace decorators."""

    def test_trace_model_execution_sync(self):
        """Test trace_model_execution with sync function."""

        @trace_model_execution
        def my_func(model_name):
            return f"executed {model_name}"

        result = my_func("test_model")
        assert result == "executed test_model"

    @pytest.mark.asyncio
    async def test_trace_model_execution_async(self):
        """Test trace_model_execution with async function."""

        @trace_model_execution
        async def my_async_func(model_name):
            return f"executed {model_name}"

        result = await my_async_func("test_model")
        assert result == "executed test_model"


class TestStructuredFormatter:
    """Tests for StructuredFormatter."""

    def test_format_basic(self):
        """Test basic JSON formatting."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert data["message"] == "Test message"
        assert data["level"] == "INFO"
        assert data["logger"] == "test"
        assert "timestamp" in data

    def test_format_with_correlation_id(self):
        """Test JSON formatting with correlation ID."""
        formatter = StructuredFormatter()
        set_correlation_id("test_correlation")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert data["correlation_id"] == "test_correlation"

        # Clean up
        set_correlation_id(None)

    def test_format_with_exception(self):
        """Test JSON formatting with exception."""
        formatter = StructuredFormatter()

        try:
            raise ValueError("Test error")
        except ValueError:
            import sys

            exc_info = sys.exc_info()

        record = logging.LogRecord(
            name="test",
            level=logging.ERROR,
            pathname="test.py",
            lineno=10,
            msg="Test error",
            args=(),
            exc_info=exc_info,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert "exception" in data
        assert data["exception"]["type"] == "ValueError"
        assert "Test error" in data["exception"]["message"]

    def test_format_with_extra_fields(self):
        """Test JSON formatting with extra fields."""
        formatter = StructuredFormatter(extra_fields={"service": "interlace"})

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        data = json.loads(output)

        assert data["service"] == "interlace"


class TestHumanReadableFormatter:
    """Tests for HumanReadableFormatter."""

    def test_format_basic(self):
        """Test basic human-readable formatting."""
        formatter = HumanReadableFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        assert "[INFO" in output
        assert "[test]" in output
        assert "Test message" in output

    def test_format_with_correlation_id(self):
        """Test formatting with correlation ID."""
        formatter = HumanReadableFormatter()
        set_correlation_id("abc123")

        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)

        assert "[abc123]" in output

        # Clean up
        set_correlation_id(None)


class TestCorrelationId:
    """Tests for correlation ID management."""

    def test_get_set_correlation_id(self):
        """Test get/set correlation ID."""
        assert get_correlation_id() is None

        set_correlation_id("test_id")
        assert get_correlation_id() == "test_id"

        set_correlation_id(None)
        assert get_correlation_id() is None

    def test_add_correlation_id_context_manager(self):
        """Test add_correlation_id context manager."""
        assert get_correlation_id() is None

        with add_correlation_id("ctx_id") as cid:
            assert cid == "ctx_id"
            assert get_correlation_id() == "ctx_id"

        assert get_correlation_id() is None

    def test_add_correlation_id_auto_generate(self):
        """Test auto-generation of correlation ID."""
        with add_correlation_id() as cid:
            assert cid is not None
            assert len(cid) == 8  # UUID prefix


class TestLogContext:
    """Tests for LogContext."""

    def test_log_context_adds_fields(self):
        """Test LogContext adds fields to log records via factory."""
        with LogContext(model="users", flow_id="abc123"):
            # Create record via logger (uses factory)
            test_logger = logging.getLogger("test_context")
            test_logger.setLevel(logging.INFO)

            # Get the factory and create a record
            factory = logging.getLogRecordFactory()
            record = factory(
                name="test",
                level=logging.INFO,
                pathname="test.py",
                lineno=10,
                msg="Test message",
                args=(),
                exc_info=None,
            )

            # Check that fields are added
            assert hasattr(record, "model")
            assert record.model == "users"
            assert hasattr(record, "flow_id")
            assert record.flow_id == "abc123"


class TestSetupStructuredLogging:
    """Tests for setup_structured_logging."""

    def test_setup_human_readable(self):
        """Test setting up human-readable logging."""
        import io

        stream = io.StringIO()

        setup_structured_logging(level="DEBUG", json_format=False, stream=stream)

        logger = logging.getLogger("test_human")
        logger.info("Test message")

        output = stream.getvalue()
        assert "Test message" in output

    def test_setup_json_format(self):
        """Test setting up JSON logging."""
        import io

        stream = io.StringIO()

        setup_structured_logging(level="DEBUG", json_format=True, stream=stream)

        test_logger = logging.getLogger("test_json_format")
        test_logger.info("Test message")

        output = stream.getvalue()
        # May have multiple lines (debug from setup + our test message)
        # Find our test message line
        for line in output.strip().split("\n"):
            if "Test message" in line:
                data = json.loads(line)
                assert data["message"] == "Test message"
                return

        # If we didn't find it, check the last line
        lines = output.strip().split("\n")
        data = json.loads(lines[-1])
        assert "message" in data
