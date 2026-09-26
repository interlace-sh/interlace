"""Core IR value types: table identifiers and the SQL-model relation.

A SQL model's output is a :class:`SqlRelation` — a sqlglot AST the owning engine
evaluates natively (zero rows enter Python) — until a strategy or sink writes it.
Python models move data as plain Arrow ``RecordBatchReader`` handles (see
``runtime/``), not as relations.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp


@dataclass(frozen=True)
class TableRef:
    """A fully-qualified table identifier, dialect-agnostic until transpiled."""

    schema: str
    name: str
    catalog: str | None = None

    def to_expr(self) -> exp.Table:
        """This table as a sqlglot Table node (identifier-safe, dialect-agnostic)."""
        return exp.table_(self.name, db=self.schema, catalog=self.catalog)


@dataclass(frozen=True)
class EngineRef:
    """Identifies which engine (gateway) can evaluate a relation, and its dialect."""

    name: str  # connection/gateway name from config
    dialect: str  # sqlglot dialect name


@dataclass(frozen=True)
class SqlRelation:
    """A SQL model's output as a canonical sqlglot AST. Composing SQL models never
    leaves this form — the engine adapter transpiles the AST at execution time, so
    no data is materialised until a strategy or sink runs a single statement."""

    ast: exp.Query

    @classmethod
    def from_sql(cls, sql: str, dialect: str = "duckdb") -> SqlRelation:
        from interlace.ir.canonicalize import as_query, parse

        return cls(ast=as_query(parse(sql, dialect)))


def drop(target: TableRef | exp.Expr, *, kind: str, exists: bool = True) -> exp.Drop:
    """DROP TABLE/VIEW/INDEX/CONSTRAINT.

    sqlglot 30 names the object in ``tables``, not ``this``. ``Expr.__init__`` still
    stores a ``this=`` kwarg, but the generator only reads ``tables``, so
    ``DROP VIEW IF EXISTS`` renders with no target. Always go through this helper.
    """
    node = target.to_expr() if isinstance(target, TableRef) else target
    return exp.Drop(tables=[node], kind=kind, exists=exists)
