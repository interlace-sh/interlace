"""
Processor registry for ingestion-stage file processing.

Phase A: Used by sync jobs to resolve `processors: [{name, config}]` specs.
"""

from __future__ import annotations

from typing import Any

from interlace.sync.types import FileProcessor, ProcessorSpec
from interlace.utils.logging import get_logger

logger = get_logger("interlace.processors.registry")


def build_default_processor_registry() -> dict[str, FileProcessor]:
    """
    Build registry of built-in processors.
    """
    registry: dict[str, FileProcessor] = {}
    try:
        from interlace.processors.pgp import PGPDecryptProcessor

        proc = PGPDecryptProcessor()
        registry[proc.name] = proc
    except Exception as e:
        # PGP is optional; registry can still be used without it.
        logger.debug(f"PGP processor not available: {e}")

    return registry


def resolve_processors(
    specs: list[ProcessorSpec] | None,
    *,
    registry: dict[str, FileProcessor],
) -> list[tuple[FileProcessor, dict[str, Any]]]:
    """
    Resolve processor specs into concrete processors + per-processor config.
    """
    if not specs:
        return []
    resolved: list[tuple[FileProcessor, dict[str, Any]]] = []
    for spec in specs:
        proc = registry.get(spec.name)
        if proc is None:
            raise ValueError(f"Unknown processor '{spec.name}'. Available: {list(registry.keys())}")
        resolved.append((proc, spec.config or {}))
    return resolved
