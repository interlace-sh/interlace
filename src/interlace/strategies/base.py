"""
Base strategy interface.

Phase 0: Abstract base class for all strategies.
"""

from abc import ABC
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import ibis


class Strategy(ABC):
    """Base class for data loading strategies."""

    def generate_sql(
        self,
        connection: "ibis.BaseBackend",
        target_table: str,
        schema: str,
        source_table: str,
        **kwargs,
    ) -> Optional[str]:
        """
        Generate SQL for strategy execution.

        Args:
            connection: ibis connection backend
            target_table: Target table name
            schema: Schema/database name
            source_table: Source table name (temp table with new data)
            **kwargs: Strategy-specific parameters

        Returns:
            SQL string to execute, or None if strategy doesn't need SQL
            (e.g., replace/append use connection.insert() instead)
        """
        return None

    def needs_temp_table(self) -> bool:
        """
        Whether this strategy needs a temp table.

        Returns:
            True if strategy requires temp table for SQL generation
        """
        return False
