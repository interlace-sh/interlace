"""Interlace — Python/SQL-first data platform.

Transformation (sqlmesh-grade snapshots, virtual environments, plan/apply),
built-in orchestration (durable work queue + unified triggers), and durable
streaming ingestion — in one process. See docs/architecture/architecture.md.

The public surface is the ``@model`` / ``@stream`` / ``@check`` decorators.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from interlace.checks.spec import CheckSpec
from interlace.dsl.decorators import check, model, stream

try:
    __version__ = version("interlaced")
except PackageNotFoundError:  # running from a source tree without an install
    __version__ = "0+unknown"

__all__ = [
    "__version__",
    "CheckSpec",
    "check",
    "model",
    "stream",
]
