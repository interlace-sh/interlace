"""Python model runtime: lazy relation handles and in-process execution lanes."""

from interlace.runtime.handles import RelationHandle
from interlace.runtime.python_model import build_python_model

__all__ = ["RelationHandle", "build_python_model"]
