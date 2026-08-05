"""Materialisation strategies as AST builders.

A strategy turns "this relation, into this table, for this interval" into a list
of canonical sqlglot statements. It never returns SQL strings and never hard-codes
a dialect — that is what made v0.x strategies DuckDB-only. ``EngineCaps`` lets a
strategy choose a portable fallback (e.g. ``DELETE`` + ``INSERT`` when ``MERGE``
is unavailable).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval


def table_expr(target: TableRef) -> exp.Table:
    """A sqlglot Table node for a target, for building DDL statements."""
    return target.to_expr()


@dataclass(frozen=True)
class RowCounts:
    """What a build did to its target's rows, as the strategy interprets it."""

    inserted: int = 0
    updated: int = 0
    deleted: int = 0

    def __add__(self, other: RowCounts) -> RowCounts:
        return RowCounts(self.inserted + other.inserted, self.updated + other.updated, self.deleted + other.deleted)

    def __bool__(self) -> bool:
        return bool(self.inserted or self.updated or self.deleted)


def _at(counts: Sequence[int], index: int) -> int:
    return max(0, counts[index]) if index < len(counts) else 0


class Strategy(ABC):
    """Builds the statements that write a relation into its target table."""

    # Bookkeeping columns the strategy itself adds to the target (never present in
    # the model's own output) — alignment/evolution must leave them alone.
    managed_columns: ClassVar[tuple[str, ...]] = ()

    @abstractmethod
    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> list[exp.Expression]:
        """Return canonical-dialect ASTs; the engine adapter transpiles them.

        ``columns`` is the target's aligned column order when apply already knows it
        (the staged delivery paths ``describe`` the target); ``None`` otherwise. Only
        strategies that need a column list — ``merge``'s native ``MERGE`` — use it;
        the rest ignore it and stay column-agnostic."""

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        """Interpret the engine's per-statement affected-row counts (index-aligned
        with ``plan_statements``' list) into inserted/updated/deleted. Each strategy
        knows what its own statements mean; the default reads the last statement as
        the write."""
        return RowCounts(inserted=_at(counts, len(counts) - 1) if counts else 0)
