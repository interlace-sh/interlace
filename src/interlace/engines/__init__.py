"""Engine adapters: one per backend, the only home for dialect-specific code."""

from __future__ import annotations

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode
from interlace.engines.duckdb import DuckDBAdapter
from interlace.engines.registry import EngineRegistry, as_registry

__all__ = [
    "DuckDBAdapter",
    "EngineAdapter",
    "EngineCaps",
    "EngineRegistry",
    "LoadMode",
    "as_registry",
]
