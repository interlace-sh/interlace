"""Canonical schema representation.

Apache Arrow is the single source of truth for column names and types across the
IR. Engine adapters map this to and from their native catalogs; nothing else in
the framework invents its own type system.
"""

from __future__ import annotations

from typing import TypeAlias

import pyarrow as pa

# The canonical schema type used throughout the IR.
ArrowSchema: TypeAlias = pa.Schema


def schema_from_fields(fields: dict[str, pa.DataType]) -> ArrowSchema:
    """Build an Arrow schema from an ordered mapping of name to Arrow type."""
    return pa.schema([pa.field(name, dtype) for name, dtype in fields.items()])


def empty_schema() -> ArrowSchema:
    """An empty schema (no columns) — used when a relation's shape is not yet known."""
    return pa.schema([])
