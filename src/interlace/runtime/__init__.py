"""Python model runtime: lazy relation handles and in-process execution lanes.

Imports stay lazy. Importing a submodule (the scratch database, a handle) must
not pull the planner: ``runtime`` → ``python_model`` → ``plan`` → ``backfill``
→ ``python_model`` is a cycle when this package is the first import.
"""

from __future__ import annotations

from typing import Any

__all__ = ["RelationHandle", "build_python_model", "session"]


def __getattr__(name: str) -> Any:
    if name == "RelationHandle":
        from interlace.runtime.handles import RelationHandle

        return RelationHandle
    if name == "build_python_model":
        from interlace.runtime.python_model import build_python_model

        return build_python_model
    if name == "session":
        from interlace.runtime.scratch import session

        return session
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
