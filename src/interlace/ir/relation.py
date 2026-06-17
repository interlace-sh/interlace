"""The universal model-output contract.

Every model produces a :class:`Relation`. A relation is *logical* until a sink
forces it: a :class:`SqlRelation` carries a sqlglot AST that the owning engine
evaluates natively (zero rows enter Python), while a :class:`StreamRelation`
carries an Arrow ``RecordBatchReader`` for the cases where Python actually
transforms data in bounded-memory batches.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import ClassVar, Protocol, runtime_checkable

import pyarrow as pa
from sqlglot import exp

from interlace.ir.schema import ArrowSchema


@dataclass(frozen=True)
class TableRef:
    """A fully-qualified table identifier, dialect-agnostic until transpiled."""

    schema: str
    name: str
    catalog: str | None = None

    def qualified(self) -> str:
        parts = [p for p in (self.catalog, self.schema, self.name) if p]
        return ".".join(parts)

    def __str__(self) -> str:
        return self.qualified()


@dataclass(frozen=True)
class EngineRef:
    """Identifies which engine (gateway) can evaluate a relation, and its dialect."""

    name: str  # connection/gateway name from config
    dialect: str  # sqlglot dialect name


@runtime_checkable
class Relation(Protocol):
    """What every model produces. Logical until a sink forces it."""

    @property
    def schema(self) -> ArrowSchema: ...

    @property
    def plane(self) -> str: ...


@dataclass(frozen=True)
class SqlRelation:
    """Logical plane: a sqlglot AST bound to the engine that can evaluate it.

    The AST is canonical (qualified, type-annotated, dialect-neutral); the engine
    adapter transpiles it at execution time. Composing SQL models never leaves
    this plane, so no data is materialised until a sink runs a single statement.
    """

    ast: exp.Expression
    engine: EngineRef
    schema: ArrowSchema

    plane: ClassVar[str] = "logical"


@dataclass
class StreamRelation:
    """Physical plane: a single-pass, batched Arrow reader.

    Produced by Python models that actually transform data (Path B in the design).
    Sinks consume the reader directly — DuckDB registers it zero-copy; remote
    engines bulk-ingest via ADBC.
    """

    schema: ArrowSchema
    reader_factory: Callable[[], pa.RecordBatchReader]

    plane: ClassVar[str] = "physical"

    def reader(self) -> pa.RecordBatchReader:
        """Open the batch reader. Single-pass: call once per materialisation."""
        return self.reader_factory()
