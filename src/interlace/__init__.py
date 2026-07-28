"""Interlace v2 — Python/SQL-first data platform.

Transformation (sqlmesh-grade snapshots, virtual environments, plan/apply),
built-in orchestration (durable work queue + unified triggers), and durable
streaming ingestion — in one process. See docs/architecture/v2-design.md.

This package is under active greenfield construction; the public surface is the
``@model`` / ``@stream`` / ``@check`` decorators plus the core IR types.
"""

from __future__ import annotations

from interlace.dsl.decorators import check, model, stream
from interlace.ir.relation import EngineRef, SqlRelation, TableRef

__version__ = "2.0.0a3"

__all__ = [
    "EngineRef",
    "SqlRelation",
    "TableRef",
    "__version__",
    "check",
    "model",
    "stream",
]
