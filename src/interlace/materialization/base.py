"""
Base materialiser interface.

Phase 0: Abstract base class for all materialisers.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import ibis


class Materializer(ABC):
    """Base class for materialisers."""

    @abstractmethod
    def materialise(self, data: "ibis.Table", model_name: str, schema: str, connection: Any, **kwargs) -> None:
        """
        Materialise data to target.

        Args:
            data: Table to materialise
            model_name: Model name
            schema: Schema name
            connection: Connection object
            **kwargs: Materialisation-specific parameters
        """
        pass
