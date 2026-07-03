"""Lazy handles passed into Python model functions.

A model function declares its upstream models as parameters and receives one
:class:`RelationHandle` per upstream. Data crosses the boundary as Arrow —
``reader()`` for bounded-memory batch streaming, ``table()`` for eager
convenience — and never as pandas.
"""

from __future__ import annotations

import pyarrow as pa

from interlace.exceptions import DefinitionError


class RelationHandle:
    """A single-pass Arrow view of an upstream relation.

    The underlying stream can be consumed once: call ``reader()`` (streaming)
    or ``table()`` (eager), not both.
    """

    def __init__(self, name: str, reader: pa.RecordBatchReader) -> None:
        self._name = name
        self._reader: pa.RecordBatchReader | None = reader

    def _take(self) -> pa.RecordBatchReader:
        if self._reader is None:
            raise DefinitionError(f"upstream {self._name!r} was already consumed; read it once")
        reader, self._reader = self._reader, None
        return reader

    def reader(self) -> pa.RecordBatchReader:
        """Stream the upstream as Arrow record batches (bounded memory)."""
        return self._take()

    def table(self) -> pa.Table:
        """Read the whole upstream into an Arrow table."""
        return self._take().read_all()

    @property
    def schema(self) -> pa.Schema:
        if self._reader is None:
            raise DefinitionError(f"upstream {self._name!r} was already consumed")
        return self._reader.schema

    def __repr__(self) -> str:
        return f"RelationHandle({self._name!r})"
