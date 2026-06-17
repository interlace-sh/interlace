"""Materialisation strategies as AST builders.

A strategy turns "this relation, into this table, for this interval" into a list
of canonical sqlglot statements. It never returns SQL strings and never hard-codes
a dialect — that is what made v0.x strategies DuckDB-only. ``EngineCaps`` lets a
strategy choose a portable fallback (e.g. ``DELETE`` + ``INSERT`` when ``MERGE``
is unavailable).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import ClassVar

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval


class Strategy(ABC):
    """Builds the statements that write a relation into its target table."""

    name: ClassVar[str]

    @abstractmethod
    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
    ) -> list[exp.Expression]:
        """Return canonical-dialect ASTs; the engine adapter transpiles them."""
