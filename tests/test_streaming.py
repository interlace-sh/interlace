"""
Tests for the streaming module.

Tests publish/subscribe, message adapters, stream bridge, and stream router.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from interlace.core.stream import (
    stream,
    publish,
    publish_sync,
    subscribe,
    _stream_registry,
    _stream_listeners,
    _notify_listeners,
)
from interlace.streaming.adapters.base import (
    Message,
    MessageAdapter,
    AdapterConfig,
    DeserializationFormat,
)
from interlace.streaming.adapters.memory import InMemoryAdapter
from interlace.streaming.adapters.webhook import WebhookAdapter
from interlace.streaming.bridge import StreamBridge
from interlace.streaming.router import StreamRouter


# ---------------------------------------------------------------------------
# @stream decorator tests
# ---------------------------------------------------------------------------


class TestStreamDecorator:
    def test_basic_stream_decorator(self):
        """Test that @stream creates proper metadata."""
        @stream(name="test_events", schema="events")
        def test_events():
            """Test stream."""
            pass

        assert hasattr(test_events, "_interlace_stream")
        assert hasattr(test_events, "_interlace_model")
        assert test_events._interlace_stream["name"] == "test_events"
        assert test_events._interlace_stream["schema"] == "events"
        assert test_events._interlace_stream["cursor"] == "rowid"

    def test_stream_decorator_defaults(self):
        """Test stream decorator default values."""
        @stream()
        def my_stream():
            pass

        meta = my_stream._interlace_stream
        assert meta["name"] == "my_stream"
        assert meta["schema"] == "events"
        assert meta["cursor"] == "rowid"
        assert meta["connection"] is None
        assert meta["tags"] == []

    def test_stream_model_integration(self):
        """Test stream creates model-like metadata for graph integration."""
        @stream(name="orders", schema="raw")
        def orders():
            pass

        model_meta = orders._interlace_model
        assert model_meta["name"] == "orders"
        assert model_meta["type"] == "stream"
        assert model_meta["materialise"] == "table"
        assert model_meta["strategy"] == "append"

    def test_stream_with_auth(self):
        """Test stream with authentication config."""
        @stream(
            name="secure_events",
            auth={"type": "bearer", "token": "secret123"},
        )
        def secure_events():
            pass

        assert secure_events._interlace_stream["auth"]["type"] == "bearer"
        assert secure_events._interlace_stream["auth"]["token"] == "secret123"

    def test_stream_with_rate_limit(self):
        """Test stream with rate limiting config."""
        @stream(
            name="rate_limited",
            rate_limit={"requests_per_second": 100, "burst": 200},
        )
        def rate_limited():
            pass

        rl = rate_limited._interlace_stream["rate_limit"]
        assert rl["requests_per_second"] == 100
        assert rl["burst"] == 200

    def test_stream_with_fields(self):
        """Test stream with schema fields."""
        @stream(
            name="typed_events",
            fields={"user_id": "string", "amount": "float64", "timestamp": "timestamp"},
        )
        def typed_events():
            pass

        assert typed_events._interlace_stream["fields"]["user_id"] == "string"
        assert typed_events._interlace_stream["fields"]["amount"] == "float64"

    def test_stream_returns_none(self):
        """Test that stream functions return None when called."""
        @stream(name="noop")
        def noop():
            return "should not be returned"

        assert noop() is None

    def test_stream_registers_globally(self):
        """Test that stream is registered in global registry."""
        @stream(name="registered_stream")
        def registered_stream():
            pass

        assert "registered_stream" in _stream_registry


# ---------------------------------------------------------------------------
# publish() tests
# ---------------------------------------------------------------------------


class TestPublish:
    @pytest.mark.asyncio
    async def test_publish_requires_connection(self):
        """Test publish raises error when no connection available."""
        with pytest.raises(RuntimeError, match="no connection available"):
            await publish("nonexistent_stream", {"key": "value"})

    @pytest.mark.asyncio
    async def test_publish_normalizes_dict_to_list(self):
        """Test publish converts single dict to list of dicts."""
        mock_conn = MagicMock()
        mock_conn.list_tables.return_value = ["test_stream"]
        mock_conn.create_database = MagicMock()

        with patch("interlace.core.stream._insert_rows") as mock_insert:
            with patch("interlace.core.stream._resolve_connection", return_value=mock_conn):
                result = await publish(
                    "test_stream",
                    {"user_id": "123"},
                    _connection_obj=mock_conn,
                )

                assert result["rows_received"] == 1
                assert result["status"] == "accepted"
                assert result["stream"] == "test_stream"
                assert "publish_id" in result

    @pytest.mark.asyncio
    async def test_publish_batch(self):
        """Test publishing a batch of events."""
        mock_conn = MagicMock()
        mock_conn.list_tables.return_value = ["batch_stream"]

        with patch("interlace.core.stream._insert_rows"):
            result = await publish(
                "batch_stream",
                [{"id": 1}, {"id": 2}, {"id": 3}],
                _connection_obj=mock_conn,
            )

            assert result["rows_received"] == 3

    @pytest.mark.asyncio
    async def test_publish_empty_data(self):
        """Test publishing empty data returns early."""
        result = await publish("empty_stream", [])
        assert result["rows_received"] == 0

    @pytest.mark.asyncio
    async def test_publish_invalid_data(self):
        """Test publish rejects invalid data types."""
        with pytest.raises(ValueError, match="dict or list of dicts"):
            await publish("test", "invalid")

    @pytest.mark.asyncio
    async def test_publish_from_function_reference(self):
        """Test publish accepts decorated function reference."""
        @stream(name="ref_stream")
        def ref_stream():
            pass

        mock_conn = MagicMock()
        mock_conn.list_tables.return_value = ["ref_stream"]

        with patch("interlace.core.stream._insert_rows"):
            result = await publish(
                ref_stream,
                {"event": "test"},
                _connection_obj=mock_conn,
            )
            assert result["stream"] == "ref_stream"

    @pytest.mark.asyncio
    async def test_publish_adds_metadata_columns(self):
        """Test that publish adds interlace metadata to rows."""
        mock_conn = MagicMock()
        mock_conn.list_tables.return_value = ["meta_stream"]

        captured_rows = []

        def capture_insert(con, name, schema, rows):
            captured_rows.extend(rows)

        with patch("interlace.core.stream._insert_rows", side_effect=capture_insert):
            await publish(
                "meta_stream",
                {"key": "value"},
                _connection_obj=mock_conn,
            )

        assert len(captured_rows) == 1
        assert "_interlace_published_at" in captured_rows[0]
        assert "_interlace_publish_id" in captured_rows[0]

    @pytest.mark.asyncio
    async def test_publish_triggers_downstream(self):
        """Test that publish triggers downstream model execution."""
        mock_conn = MagicMock()
        mock_conn.list_tables.return_value = ["trigger_stream"]

        mock_graph = MagicMock()
        mock_graph.get_dependents.return_value = ["model_a", "model_b"]

        mock_enqueue = AsyncMock()

        with patch("interlace.core.stream._insert_rows"):
            result = await publish(
                "trigger_stream",
                {"event": "test"},
                _connection_obj=mock_conn,
                _graph=mock_graph,
                _enqueue_run=mock_enqueue,
            )

        assert result["triggered_models"] == ["model_a", "model_b"]
        mock_enqueue.assert_called_once_with(["model_a", "model_b"], force=True)

    @pytest.mark.asyncio
    async def test_publish_emits_to_event_bus(self):
        """Test that publish emits event to EventBus for SSE."""
        mock_conn = MagicMock()
        mock_conn.list_tables.return_value = ["bus_stream"]

        mock_bus = AsyncMock()

        with patch("interlace.core.stream._insert_rows"):
            await publish(
                "bus_stream",
                {"event": "test"},
                _connection_obj=mock_conn,
                _event_bus=mock_bus,
            )

        mock_bus.emit.assert_called_once()
        call_args = mock_bus.emit.call_args
        assert call_args[0][0] == "stream.published"


# ---------------------------------------------------------------------------
# subscribe() tests
# ---------------------------------------------------------------------------


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_subscribe_receives_events(self):
        """Test subscribe receives published events."""
        received = []

        async def consumer():
            async for event in subscribe("sub_test", timeout=0.5):
                received.append(event)
                if len(received) >= 2:
                    break

        async def producer():
            await asyncio.sleep(0.1)
            await _notify_listeners("sub_test", [{"id": 1}], "p1")
            await asyncio.sleep(0.1)
            await _notify_listeners("sub_test", [{"id": 2}], "p2")

        # Run both
        consumer_task = asyncio.create_task(consumer())
        producer_task = asyncio.create_task(producer())

        await asyncio.wait_for(
            asyncio.gather(consumer_task, producer_task),
            timeout=5.0,
        )

        assert len(received) == 2
        assert received[0]["id"] == 1
        assert received[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_subscribe_with_filter(self):
        """Test subscribe with filter function."""
        received = []

        async def consumer():
            async for event in subscribe(
                "filter_test",
                timeout=0.5,
                filter_fn=lambda e: e.get("type") == "important",
            ):
                received.append(event)
                if len(received) >= 1:
                    break

        async def producer():
            await asyncio.sleep(0.1)
            await _notify_listeners(
                "filter_test",
                [
                    {"type": "boring", "id": 1},
                    {"type": "important", "id": 2},
                ],
                "p1",
            )

        consumer_task = asyncio.create_task(consumer())
        producer_task = asyncio.create_task(producer())

        await asyncio.wait_for(
            asyncio.gather(consumer_task, producer_task),
            timeout=5.0,
        )

        assert len(received) == 1
        assert received[0]["type"] == "important"

    @pytest.mark.asyncio
    async def test_subscribe_cleanup(self):
        """Test that subscribe cleans up listeners on exit."""
        stream_name = "cleanup_test"

        async def short_consumer():
            async for event in subscribe(stream_name, timeout=0.2):
                break  # Exit immediately

        await asyncio.wait_for(short_consumer(), timeout=3.0)

        # Listener should be cleaned up
        assert stream_name not in _stream_listeners or len(_stream_listeners.get(stream_name, [])) == 0


# ---------------------------------------------------------------------------
# Message and adapter tests
# ---------------------------------------------------------------------------


class TestMessage:
    def test_message_to_dict_from_dict_value(self):
        """Test Message.to_dict() with dict value."""
        msg = Message(
            key="order-123",
            value={"order_id": "123", "total": 99.99},
            topic="orders",
            timestamp=datetime(2025, 1, 15, tzinfo=timezone.utc),
        )
        result = msg.to_dict()
        assert result["order_id"] == "123"
        assert result["total"] == 99.99
        assert result["_message_key"] == "order-123"
        assert result["_source_topic"] == "orders"

    def test_message_to_dict_from_string_value(self):
        """Test Message.to_dict() with non-dict value."""
        msg = Message(value="raw payload")
        result = msg.to_dict()
        assert result["payload"] == "raw payload"


class TestInMemoryAdapter:
    @pytest.mark.asyncio
    async def test_produce_and_consume(self):
        """Test basic produce and consume with InMemoryAdapter."""
        adapter = InMemoryAdapter()
        await adapter.connect()

        # Produce
        await adapter.produce("test", Message(value={"id": 1}))
        await adapter.produce("test", Message(value={"id": 2}))

        # Consume from beginning
        received = []
        async for msg in adapter.consume("test", from_beginning=True):
            received.append(msg)
            if len(received) >= 2:
                break

        assert len(received) == 2
        assert received[0].value["id"] == 1
        assert received[1].value["id"] == 2

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_produce_batch(self):
        """Test batch produce."""
        adapter = InMemoryAdapter()
        await adapter.connect()

        messages = [
            Message(value={"id": i}) for i in range(5)
        ]
        count = await adapter.produce_batch("batch-test", messages)
        assert count == 5
        assert len(adapter.get_topic_messages("batch-test")) == 5

        await adapter.disconnect()

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test async context manager."""
        async with InMemoryAdapter() as adapter:
            await adapter.produce("ctx", Message(value={"test": True}))
            assert len(adapter.get_topic_messages("ctx")) == 1

    @pytest.mark.asyncio
    async def test_clear_topic(self):
        """Test clearing a topic."""
        adapter = InMemoryAdapter()
        await adapter.connect()

        await adapter.produce("clear", Message(value={"id": 1}))
        assert len(adapter.get_topic_messages("clear")) == 1

        adapter.clear_topic("clear")
        assert len(adapter.get_topic_messages("clear")) == 0

        await adapter.disconnect()


# ---------------------------------------------------------------------------
# WebhookAdapter tests
# ---------------------------------------------------------------------------


class TestWebhookAdapter:
    @pytest.mark.asyncio
    async def test_consume_not_supported(self):
        """Test that WebhookAdapter.consume() raises NotImplementedError."""
        adapter = WebhookAdapter()
        with pytest.raises(NotImplementedError):
            async for _ in adapter.consume("test"):
                pass

    def test_signing_secret_config(self):
        """Test webhook adapter with signing secret."""
        adapter = WebhookAdapter(
            endpoints={"orders": "https://example.com/webhook"},
            signing_secret="my-secret",
        )
        assert adapter.signing_secret == "my-secret"
        assert adapter.endpoints["orders"] == "https://example.com/webhook"


# ---------------------------------------------------------------------------
# AdapterConfig tests
# ---------------------------------------------------------------------------


class TestAdapterConfig:
    def test_default_config(self):
        """Test default adapter configuration."""
        config = AdapterConfig()
        assert config.format == DeserializationFormat.JSON
        assert config.batch_size == 100
        assert config.batch_timeout_ms == 1000
        assert config.max_retries == 3
        assert config.transform_fn is None
        assert config.filter_fn is None

    def test_custom_config(self):
        """Test custom adapter configuration."""
        config = AdapterConfig(
            format=DeserializationFormat.MSGPACK,
            batch_size=50,
            max_messages_per_second=1000,
            transform_fn=lambda x: x,
        )
        assert config.format == DeserializationFormat.MSGPACK
        assert config.batch_size == 50
        assert config.max_messages_per_second == 1000
        assert config.transform_fn is not None


# ---------------------------------------------------------------------------
# StreamBridge tests
# ---------------------------------------------------------------------------


class TestStreamBridge:
    def test_add_inbound_route(self):
        """Test adding an inbound route."""
        bridge = StreamBridge()
        adapter = InMemoryAdapter()

        bridge.add_inbound(adapter, "external-topic", "internal_stream")

        assert len(bridge.routes) == 1
        assert bridge.routes[0].direction == "inbound"
        assert bridge.routes[0].source == "external-topic"
        assert bridge.routes[0].target == "internal_stream"

    def test_add_outbound_route(self):
        """Test adding an outbound route."""
        bridge = StreamBridge()
        adapter = InMemoryAdapter()

        bridge.add_outbound(adapter, "internal_stream", "external-topic")

        assert len(bridge.routes) == 1
        assert bridge.routes[0].direction == "outbound"
        assert bridge.routes[0].source == "internal_stream"
        assert bridge.routes[0].target == "external-topic"

    def test_chaining(self):
        """Test method chaining."""
        bridge = StreamBridge()
        adapter1 = InMemoryAdapter()
        adapter2 = InMemoryAdapter()

        result = (
            bridge
            .add_inbound(adapter1, "topic1", "stream1")
            .add_outbound(adapter2, "stream2", "topic2")
        )

        assert result is bridge
        assert len(bridge.routes) == 2

    def test_status(self):
        """Test bridge status reporting."""
        bridge = StreamBridge()
        adapter = InMemoryAdapter()

        bridge.add_inbound(adapter, "t1", "s1")
        bridge.add_inbound(adapter, "t2", "s2")
        bridge.add_outbound(adapter, "s3", "t3")

        status = bridge.status()
        assert status["routes"] == 3
        assert status["inbound"] == 2
        assert status["outbound"] == 1
        assert status["running"] is False


# ---------------------------------------------------------------------------
# StreamRouter tests
# ---------------------------------------------------------------------------


class TestStreamRouter:
    def test_on_decorator(self):
        """Test @router.on() decorator."""
        router = StreamRouter()

        @router.on("test_events")
        async def handle(event):
            pass

        assert len(router.routes) == 1
        assert router.routes[0].stream_name == "test_events"
        assert router.routes[0].handler is handle

    def test_on_with_filter(self):
        """Test @router.on() with filter."""
        router = StreamRouter()

        @router.on("events", filter=lambda e: e.get("type") == "signup")
        async def handle_signup(event):
            pass

        assert router.routes[0].filter_fn is not None
        assert router.routes[0].filter_fn({"type": "signup"}) is True
        assert router.routes[0].filter_fn({"type": "login"}) is False

    def test_forward(self):
        """Test router.forward()."""
        router = StreamRouter()

        router.forward("raw_events", to="cleaned_events")

        assert len(router.routes) == 1
        assert router.routes[0].forward_to == "cleaned_events"
        assert router.routes[0].stream_name == "raw_events"

    def test_forward_with_transform(self):
        """Test router.forward() with transformation."""
        router = StreamRouter()

        def clean(event):
            return {k: v for k, v in event.items() if not k.startswith("_")}

        router.forward("raw", to="clean", transform=clean)

        assert router.routes[0].transform_fn is clean


# ---------------------------------------------------------------------------
# Integration-style tests
# ---------------------------------------------------------------------------


class TestStreamIntegration:
    @pytest.mark.asyncio
    async def test_publish_subscribe_flow(self):
        """Test end-to-end publish -> subscribe flow."""
        stream_name = "integration_test_stream"
        received = []

        async def consumer():
            async for event in subscribe(stream_name, timeout=1.0):
                received.append(event)
                if len(received) >= 3:
                    break

        async def producer():
            await asyncio.sleep(0.1)
            # Simulate what publish does internally for in-process listeners
            await _notify_listeners(
                stream_name,
                [
                    {"id": 1, "type": "create"},
                    {"id": 2, "type": "update"},
                    {"id": 3, "type": "delete"},
                ],
                "pub-1",
            )

        consumer_task = asyncio.create_task(consumer())
        producer_task = asyncio.create_task(producer())

        await asyncio.wait_for(
            asyncio.gather(consumer_task, producer_task),
            timeout=5.0,
        )

        assert len(received) == 3
        assert [e["type"] for e in received] == ["create", "update", "delete"]

    @pytest.mark.asyncio
    async def test_memory_adapter_consume_to_stream_bridge(self):
        """Test InMemoryAdapter.consume_to_stream() bridges to publish."""
        adapter = InMemoryAdapter()
        await adapter.connect()

        # Pre-populate topic
        for i in range(3):
            await adapter.produce("source", Message(value={"id": i}))

        # consume_to_stream will call publish() which needs a connection
        # We test that the adapter correctly iterates and calls publish
        with patch("interlace.core.stream.publish", new_callable=AsyncMock) as mock_publish:
            # Run consume_to_stream with a quick timeout
            adapter._running = True

            async def stop_after_delay():
                await asyncio.sleep(0.5)
                adapter._running = False

            consume_task = asyncio.create_task(
                adapter.consume_to_stream(
                    "source",
                    "target_stream",
                    from_beginning=True,
                    config=AdapterConfig(batch_size=10, batch_timeout_ms=200),
                )
            )
            stop_task = asyncio.create_task(stop_after_delay())

            await asyncio.wait_for(
                asyncio.gather(consume_task, stop_task),
                timeout=5.0,
            )

            # Verify publish was called with batched events
            assert mock_publish.called

        await adapter.disconnect()


# ---------------------------------------------------------------------------
# publish_sync tests
# ---------------------------------------------------------------------------


class TestPublishSync:
    def test_publish_sync_rejects_invalid_data(self):
        """Test publish_sync rejects invalid data."""
        with pytest.raises(ValueError, match="dict or list of dicts"):
            publish_sync("test", 123)
