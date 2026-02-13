"""
Materialization - how data is persisted.

Phase 0: Basic materialization (table, view, ephemeral).
"""

from interlace.materialization.base import Materializer
from interlace.materialization.ephemeral import EphemeralMaterializer
from interlace.materialization.table import TableMaterializer
from interlace.materialization.view import ViewMaterializer

__all__ = [
    "Materializer",
    "TableMaterializer",
    "ViewMaterializer",
    "EphemeralMaterializer",
]

# Materializer registry
MATERIALIZERS = {
    "table": TableMaterializer,
    "view": ViewMaterializer,
    "ephemeral": EphemeralMaterializer,
}


def get_materializer(materialize_type: str) -> Materializer:
    """Get materializer by type."""
    if materialize_type not in MATERIALIZERS:
        raise ValueError(f"Unknown materialization type: {materialize_type}")
    return MATERIALIZERS[materialize_type]()  # type: ignore[abstract]
