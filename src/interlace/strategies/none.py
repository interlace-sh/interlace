"""
None strategy - no automatic loading, user handles manually.

Phase 0: Basic none strategy (placeholder).
"""

from typing import TYPE_CHECKING, Any

from interlace.strategies.base import Strategy

if TYPE_CHECKING:
    import ibis


class NoneStrategy(Strategy):
    """None strategy - returns new data as-is (user handles persistence)."""

    def generate_sql(
        self, connection: "ibis.BaseBackend", target_table: str, schema: str, source_table: str, **kwargs: Any
    ) -> str | None:
        """No SQL generation needed for none strategy."""
        return None
