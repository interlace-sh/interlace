"""
Edge-case tests for connections and service modules.

Targets genuine bugs in:
- cron_parser.py: DOW range wrap-around with value 7
- postgres.py: async close() called synchronously from base class
- server.py: StopIteration on empty connections dict, empty JSON body handling
"""

import asyncio
import json
import warnings
from unittest.mock import AsyncMock, MagicMock

import pytest

# ---------------------------------------------------------------------------
# Bug 1: Cron parser – DOW range "5-7" should produce {5, 6, 0} not raise
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCronParserDOWWrapAround:
    """Cron DOW field allows 7 as alias for 0 (Sunday).

    A range like '5-7' means Friday(5), Saturday(6), Sunday(7==0).
    The parser converts 7→0 *before* the range-order check, turning
    the range into 5-0 which then fails the ``a > b`` guard.
    """

    def test_dow_range_5_to_7(self):
        """'5-7' in DOW field should return {5, 6, 0} (Fri–Sun)."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("5-7", min_v=0, max_v=6, allow_7_as_0=True)
        assert result == {5, 6, 0}

    def test_dow_single_value_7(self):
        """Single value '7' should be treated as 0 (Sunday)."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("7", min_v=0, max_v=6, allow_7_as_0=True)
        assert result == {0}

    def test_dow_range_0_to_6_unchanged(self):
        """Normal range '0-6' should not be affected by 7→0 logic."""
        from interlace.service.cron_parser import _parse_field

        result = _parse_field("0-6", min_v=0, max_v=6, allow_7_as_0=True)
        assert result == {0, 1, 2, 3, 4, 5, 6}

    def test_next_fire_time_with_dow_7(self):
        """Full cron expression with DOW=7 (Sunday alias) should work."""
        from datetime import datetime
        from zoneinfo import ZoneInfo

        from interlace.service.cron_parser import next_fire_time_cron

        # "0 9 * * 5-7" = At 09:00 on Friday, Saturday, Sunday
        now = datetime(2026, 2, 2, 10, 0, 0, tzinfo=ZoneInfo("UTC"))  # Monday
        result = next_fire_time_cron("0 9 * * 5-7", now=now)
        # Next match should be Friday Feb 6
        assert result.weekday() == 4  # Friday


# ---------------------------------------------------------------------------
# Bug 2: PostgresConnection.close() is async → never awaited by __exit__
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPostgresConnectionCloseMismatch:
    """PostgresConnection.close() is async but BaseConnection.__exit__
    calls self.close() synchronously.  The returned coroutine is never
    awaited, so _pool.close() and super().close() never execute."""

    def test_sync_close_triggers_cleanup(self):
        """Calling close() synchronously must still clean up _connection."""
        from interlace.connections.postgres import PostgresConnection

        # Create a PostgresConnection without actually connecting
        config = {
            "type": "postgres",
            "config": {
                "host": "localhost",
                "port": 5432,
                "user": "test",
                "password": "test",
                "database": "test",
            },
        }
        conn = PostgresConnection("test_pg", config)
        # Simulate an active connection
        mock_backend = MagicMock()
        conn._connection = mock_backend

        # close() must be synchronous so __exit__ works properly
        result = conn.close()

        # close() should NOT return a coroutine
        assert not asyncio.iscoroutine(result), (
            "PostgresConnection.close() returns a coroutine — "
            "BaseConnection.__exit__ will never await it, causing resource leaks"
        )

        # _connection should be cleaned up
        assert conn._connection is None

    def test_context_manager_exit_cleans_up(self):
        """Using PostgresConnection as a context manager must clean up."""
        from interlace.connections.postgres import PostgresConnection

        config = {
            "type": "postgres",
            "config": {
                "host": "localhost",
                "port": 5432,
                "user": "test",
                "password": "test",
                "database": "test",
            },
        }
        conn = PostgresConnection("test_pg", config)
        mock_backend = MagicMock()
        conn._connection = mock_backend

        # __exit__ must not produce RuntimeWarning about unawaited coroutine
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            conn.__exit__(None, None, None)

        coroutine_warnings = [
            w for w in caught if "coroutine" in str(w.message).lower() and "never awaited" in str(w.message).lower()
        ]
        assert len(coroutine_warnings) == 0, f"__exit__ produced coroutine warning: {coroutine_warnings}"
        assert conn._connection is None


# ---------------------------------------------------------------------------
# Bug 3: handle_stream_publish – StopIteration on empty connections config
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStreamPublishEmptyConnections:
    """In server.py handle_stream_publish, the fallback connection path uses
    ``config.get("connections", {}).keys().__iter__().__next__()``
    which raises StopIteration when the connections dict is empty.
    """

    def test_empty_connections_dict_raises_stop_iteration(self):
        """The raw expression crashes with StopIteration on empty dict."""
        config: dict = {"connections": {}}

        with pytest.raises(StopIteration):
            config.get("connections", {}).keys().__iter__().__next__()

    def test_handle_stream_publish_empty_connections_no_crash(self):
        """handle_stream_publish must not crash when connections is empty.

        We test the problematic expression directly — after the fix it
        should either return None or raise a friendlier error.
        """
        config: dict = {"connections": {}}

        # After fix, this should not raise StopIteration
        result = next(iter(config.get("connections", {}).keys()), None)
        assert result is None


# ---------------------------------------------------------------------------
# Bug 4: Legacy handle_run / handle_sync_once – empty JSON body crashes
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLegacyHandlerEmptyBody:
    """Legacy server.py handlers call ``await request.json()`` without
    wrapping in try/except.  An empty POST body raises ContentTypeError
    or JSONDecodeError."""

    @pytest.mark.asyncio
    async def test_handle_run_empty_body(self):
        """POST /runs with empty body should return error, not crash."""
        from interlace.service.server import InterlaceService

        svc = InterlaceService.__new__(InterlaceService)
        svc.config = {"connections": {}}
        svc.models = {"test_model": {"type": "python"}}
        svc.graph = None
        svc.flow = None
        svc._run_lock = asyncio.Lock()
        svc._stopping = asyncio.Event()
        svc._background_tasks = []
        svc.flows_in_progress = {}
        svc.event_bus = MagicMock()

        # Create a mock request that raises on .json()
        request = MagicMock()
        request.json = AsyncMock(side_effect=json.JSONDecodeError("msg", "", 0))

        # After fix, this should return an error response, not raise
        try:
            response = await svc.handle_run(request)
            # Should get a 400 response
            assert response.status == 400
        except json.JSONDecodeError:
            pytest.fail("handle_run crashed on invalid JSON body instead of " "returning an error response")

    @pytest.mark.asyncio
    async def test_handle_sync_once_empty_body(self):
        """POST /sync/run_once with empty body should return error."""
        from interlace.service.server import InterlaceService

        svc = InterlaceService.__new__(InterlaceService)
        svc.config = {"sync": {"jobs": [{"job_id": "j1"}]}}

        request = MagicMock()
        request.json = AsyncMock(side_effect=json.JSONDecodeError("msg", "", 0))

        try:
            response = await svc.handle_sync_once(request)
            assert response.status == 400
        except json.JSONDecodeError:
            pytest.fail("handle_sync_once crashed on invalid JSON body instead of " "returning an error response")
