"""
None strategy - no automatic loading, user handles manually.

Phase 0: Basic none strategy (placeholder).
"""

from interlace.strategies.base import Strategy


class NoneStrategy(Strategy):
    """None strategy - returns new data as-is (user handles persistence)."""

    def generate_sql(self, connection, target_table, schema, source_table, **kwargs):
        """No SQL generation needed for none strategy."""
        return None
