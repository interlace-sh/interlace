"""The engine adapter — the only place dialect-specific code is allowed to live.

Every backend (DuckDB, DuckLake, Postgres, Snowflake, BigQuery) implements this
one interface. The planner and strategies stay dialect-neutral by emitting
canonical sqlglot ASTs; :meth:`EngineAdapter.transpile` is the single seam where
a dialect reappears. :class:`EngineCaps` lets strategies degrade gracefully
(e.g. rewrite ``MERGE`` to ``DELETE`` + ``INSERT``) on engines that lack a feature.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

import pyarrow as pa
import sqlglot
from sqlglot import exp

from interlace.ir.relation import TableRef

LoadMode = Literal["create", "append"]


@dataclass(frozen=True)
class EngineCaps:
    """Feature flags that drive strategy fallbacks. Conservative defaults (all off)."""

    supports_create_or_replace: bool = False
    supports_star_exclude: bool = False  # SELECT * EXCLUDE (...) — scd_type_2 needs it


class EngineAdapter(ABC):
    """Executes canonical ASTs and moves Arrow data in and out of one backend."""

    dialect: str
    caps: EngineCaps

    @abstractmethod
    async def execute(self, ast: exp.Expression) -> None:
        """Run a statement (DDL/DML) with no result set."""

    @abstractmethod
    async def fetch(self, ast: exp.Expression) -> pa.RecordBatchReader:
        """Extract: evaluate a query and stream the result as Arrow batches."""

    @abstractmethod
    async def load(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> None:
        """Load: write Arrow batches into a table, creating or appending."""

    @abstractmethod
    async def create_view(self, name: TableRef, target: TableRef) -> None:
        """Point a virtual-environment view at a physical snapshot table."""

    @abstractmethod
    async def create_schema(self, name: str) -> None:
        """Create a schema/namespace if it does not already exist."""

    @abstractmethod
    async def describe(self, table: TableRef) -> dict[str, str]:
        """Return a table or view's columns as an ordered ``{name: type}`` mapping."""

    async def table_exists(self, table: TableRef) -> bool:
        """Whether the table (or view) exists. Adapters override with a direct probe."""
        return bool(await self.describe(table))

    async def execute_all(self, statements: Sequence[exp.Expression]) -> None:
        """Run statements in order. Override to make the batch atomic (one transaction)."""
        for statement in statements:
            await self.execute(statement)

    async def execute_sql(self, sql: str) -> None:
        """Run one raw SQL statement written in this engine's dialect."""
        await self.execute(sqlglot.parse_one(sql, read=self.dialect))

    async def fetch_sql(self, sql: str) -> pa.RecordBatchReader:
        """Evaluate one raw SQL query written in this engine's dialect."""
        return await self.fetch(sqlglot.parse_one(sql, read=self.dialect))

    def transpile(self, ast: exp.Expression) -> str:
        """Canonical AST -> this engine's SQL. The one place dialect leaks back in."""
        return ast.sql(dialect=self.dialect)
