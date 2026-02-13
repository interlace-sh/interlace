"""
Replace strategy - replace entire table with new data.

Phase 0: SQL-based replace using connection.insert() with overwrite=True.
"""

from interlace.strategies.base import Strategy


class ReplaceStrategy(Strategy):
    """Replace strategy - replaces entire table using connection.insert(overwrite=True)."""

    def generate_sql(self, connection, target_table, schema, source_table, **kwargs):
        """No SQL generation needed for replace strategy (uses connection.insert)."""
        return None
