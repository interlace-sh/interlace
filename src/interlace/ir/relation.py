"""The universal model-output contract.

Every model produces a :class:`Relation`. A relation is *logical* until a sink
forces it: a :class:`SqlRelation` carries a sqlglot AST that the owning engine
evaluates natively (zero rows enter Python). Python models move data as plain
Arrow ``RecordBatchReader`` handles (see ``runtime/``), not as relations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import ClassVar, Protocol, runtime_checkable

from sqlglot import exp

from interlace.ir.schema import ArrowSchema


@dataclass(frozen=True)
class TableRef:
    """A fully-qualified table identifier, dialect-agnostic until transpiled."""

    schema: str
    name: str
    catalog: str | None = None

    def to_expr(self) -> exp.Table:
        """This table as a sqlglot Table node (identifier-safe, dialect-agnostic)."""
        return exp.table_(self.name, db=self.schema, catalog=self.catalog)

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
