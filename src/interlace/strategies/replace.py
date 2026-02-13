"""
Replace strategy - replace entire table with new data.

Phase 0: SQL-based replace using connection.insert() with overwrite=True.
"""

from typing import TYPE_CHECKING, Any

from interlace.strategies.base import Strategy

if TYPE_CHECKING:
    import ibis


class ReplaceStrategy(Strategy):
    """Replace strategy - replaces entire table using connection.insert(overwrite=True)."""

    def generate_sql(
        self, connection: "ibis.BaseBackend", target_table: str, schema: str, source_table: str, **kwargs: Any
    ) -> str | None:
        """No SQL generation needed for replace strategy (uses connection.insert)."""
        return None
