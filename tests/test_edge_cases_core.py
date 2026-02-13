"""
Edge case and bug tests for the core execution engine.

Tests for bugs found through code review of:
- core/execution/change_detector.py — Missing SQL quotes in _get_model_last_run
- core/execution/model_executor.py — Wrong method name in _check_cache_policy
- core/model.py — parse_ttl crashes on non-string input
- core/execution/data_converter.py — Silent data corruption with mixed dict values
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.unit
class TestChangeDetectorSQLQuotes:
    """Bug: _get_model_last_run generates SQL without quotes around string values.

    _escape_sql_string only escapes single quotes inside a string but does NOT
    wrap the result in SQL quotes.  The query interpolation therefore produces
    invalid SQL like:
        WHERE model_name = my_model
    instead of:
        WHERE model_name = 'my_model'
    """

    def test_get_model_last_run_sql_has_quoted_model_name(self):
        """SQL WHERE clause must quote string values to prevent syntax errors."""
        from interlace.core.execution.change_detector import ChangeDetector

        detector = ChangeDetector(state_store=None)

        with patch("interlace.core.context._execute_sql_internal") as mock_exec:
            mock_exec.return_value = None
            detector._get_model_last_run(MagicMock(), "my_model", "public")

            assert mock_exec.called, "_execute_sql_internal should be called"
            sql = mock_exec.call_args[0][1]

            # Model name and schema name must be wrapped in SQL string quotes
            assert "'my_model'" in sql, f"SQL should have quoted model_name. Got: {sql}"
            assert "'public'" in sql, f"SQL should have quoted schema_name. Got: {sql}"


@pytest.mark.unit
class TestParseTTLEdgeCases:
    """Bug: parse_ttl crashes with AttributeError on non-string input.

    YAML parses ``ttl: 30`` as integer 30, not string ``"30"``.
    Calling ``parse_ttl(30)`` invokes ``30.strip()`` → AttributeError.
    The function should raise ``ValueError`` with a clear message instead.
    """

    def test_parse_ttl_integer_input_raises_value_error(self):
        """parse_ttl(30) should raise ValueError, not AttributeError."""
        from interlace.core.model import parse_ttl

        with pytest.raises(ValueError, match="(?i)string"):
            parse_ttl(30)

    def test_parse_ttl_float_input_raises_value_error(self):
        """parse_ttl(3.5) should raise ValueError, not AttributeError."""
        from interlace.core.model import parse_ttl

        with pytest.raises(ValueError, match="(?i)string"):
            parse_ttl(3.5)


@pytest.mark.unit
class TestCachePolicyTTL:
    """Bug: _check_cache_policy calls non-existent StateStore.get_model_last_run.

    The correct method is ``get_model_last_run_at(model_name, schema_name)``.
    Because the wrong method name is used, the AttributeError is silently caught
    by a broad ``except Exception``, and TTL caching never actually works.
    """

    def test_state_store_does_not_have_get_model_last_run(self):
        """StateStore has get_model_last_run_at, NOT get_model_last_run."""
        from interlace.core.state import StateStore

        assert hasattr(StateStore, "get_model_last_run_at"), "StateStore should have get_model_last_run_at"
        assert not hasattr(StateStore, "get_model_last_run"), (
            "StateStore should NOT have get_model_last_run — "
            "model_executor._check_cache_policy calls this non-existent method"
        )

    def test_ttl_cache_skips_model_with_recent_run(self):
        """TTL cache should return a skip reason when last run is within TTL.

        Before fix: calls get_model_last_run (doesn't exist) → AttributeError
        silently caught → returns None → model never skipped.
        After fix: calls get_model_last_run_at → returns datetime → elapsed < TTL
        → returns skip reason string.
        """
        from interlace.core.execution.model_executor import ModelExecutor

        # Build a minimal ModelExecutor without running __init__
        executor = ModelExecutor.__new__(ModelExecutor)

        # Mock state_store — get_model_last_run_at returns a very recent datetime
        mock_state = MagicMock()
        mock_state.get_model_last_run_at.return_value = datetime.now()
        executor.state_store = mock_state
        executor.connection_manager = MagicMock()

        model_info = {
            "cache": {"ttl": "24h", "strategy": "ttl"},
            "schema": "public",
        }

        result = executor._check_cache_policy("test_model", model_info)

        # Should return a skip reason string, not None
        assert result is not None, (
            "TTL cache should skip model with recent run. "
            "If None, the TTL check silently failed (wrong method name)."
        )
        assert "not expired" in result


@pytest.mark.unit
class TestDataConverterMixedDict:
    """Bug: Dict with mixed list/scalar values silently corrupts data.

    ``convert_to_ibis_table`` checks only the *first* dict value to decide
    whether the dict is columnar (dict-of-lists) or a single row.  If the
    first value is a list but other values are strings, it indexes into the
    strings character-by-character, silently corrupting data.

    Example: ``{"names": ["Alice", "Bob"], "type": "ab"}``
    Bug produces: ``[{"names": "Alice", "type": "a"}, {"names": "Bob", "type": "b"}]``
    Correct: treat as single-row dict since not ALL values are lists.
    """

    def test_dict_mixed_list_and_string_values_not_corrupted(self):
        """Dict with mixed list/scalar values must not silently corrupt data."""
        from interlace.core.execution.data_converter import DataConverter

        data = {"names": ["Alice", "Bob"], "type": "ab"}
        table = DataConverter.convert_to_ibis_table(data)
        result = table.execute()

        # Should be 1 row (single-row dict), NOT 2 rows with corrupted values
        assert len(result) == 1, (
            f"Expected 1 row (single-row dict), got {len(result)}. "
            f"Mixed list/scalar dict should not be split by indexing into strings."
        )

    def test_dict_all_list_values_still_works(self):
        """Dict where ALL values are lists should still be treated as columnar."""
        from interlace.core.execution.data_converter import DataConverter

        data = {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]}
        table = DataConverter.convert_to_ibis_table(data)
        result = table.execute()

        assert len(result) == 3
        assert list(result["id"]) == [1, 2, 3]
        assert list(result["name"]) == ["Alice", "Bob", "Charlie"]
