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
from typing import ClassVar, overload

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


def interpret_counts(roles: Sequence[str], counts: Sequence[int]) -> RowCounts:
    """Turn per-statement affected-row counts into inserted/updated/deleted.

    Roles, aligned with the statement list: ``ignore`` (ensure, drop, stage),
    ``insert``, ``update``, ``delete`` (rows actually removed), ``upsert_delete``
    (keys deleted and then re-inserted — an update, subtracted from the insert),
    and ``merge`` (one native MERGE count, reported as inserted).
    """
    inserted = updated = deleted = upsert_deleted = 0
    for index, role in enumerate(roles):
        count = max(0, counts[index]) if index < len(counts) else 0
        if role == "insert" or role == "merge":
            inserted += count
        elif role == "update":
            updated += count
        elif role == "delete":
            deleted += count
        elif role == "upsert_delete":
            upsert_deleted += count
    if upsert_deleted:
        updated += upsert_deleted
        inserted = max(0, inserted - upsert_deleted)
    return RowCounts(inserted=inserted, updated=updated, deleted=deleted)


class WritePlan(Sequence[exp.Expr]):
    """The statements a strategy will run, and what each statement's row count means.

    Iterates and indexes as the statement list, so callers execute it directly.
    ``row_counts`` reads ``roles`` — never the length of the list — so a DROP or
    a temp table inserted ahead of the writes cannot shift the interpretation.
    """

    def __init__(self, statements: Sequence[exp.Expr], roles: Sequence[str]) -> None:
        if len(statements) != len(roles):
            raise ValueError("every planned statement needs a role")
        self.statements = tuple(statements)
        self.roles = tuple(roles)

    def __len__(self) -> int:
        return len(self.statements)

    @overload
    def __getitem__(self, index: int) -> exp.Expr: ...

    @overload
    def __getitem__(self, index: slice) -> tuple[exp.Expr, ...]: ...

    def __getitem__(self, index: int | slice) -> exp.Expr | tuple[exp.Expr, ...]:
        return self.statements[index]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        return interpret_counts(self.roles, counts)


class Strategy(ABC):
    """Builds the statements that write a relation into its target table."""

    # Bookkeeping columns the strategy itself adds to the target (never present in
    # the model's own output) — alignment/evolution must leave them alone.
    managed_columns: ClassVar[tuple[str, ...]] = ()
    # A rebuild would destroy rows this strategy has accumulated (history, upserts).
    accumulates: ClassVar[bool] = False
    # The constructor rejects an empty key. Incremental's key is optional.
    requires_key: ClassVar[bool] = False
    # plan_statements raises without an interval (incremental).
    requires_interval: ClassVar[bool] = False

    @property
    def writes_named_columns(self) -> bool:
        """Whether every write this strategy emits names its columns explicitly.

        True means apply can hand it *only* the columns the model actually produces
        (see ``plan.apply.Alignment``): the writes bind by name, so columns the model
        does not produce are left out of the SET list and out of the INSERT column
        list — a co-owned target's own columns survive an update, and its DEFAULTs
        apply on an insert. False means the strategy binds positionally or compares
        whole rows (``INSERT ... SELECT *``, ``EXCEPT``), so it needs the source
        widened to the target's full column set, NULL-filling what is missing."""
        return False

    @abstractmethod
    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> WritePlan:
        """Return canonical-dialect ASTs; the engine adapter transpiles them.

        ``columns`` is the target's column order when apply already knows it
        (it has described the target, or aligned a stage). ``None`` on a first
        build, where there is nothing to preserve and the ensure-create matches
        the source. Strategies that name their columns (``writes_named_columns``)
        use the list for native ``MERGE`` or an explicit INSERT column list; the
        rest ignore it and stay column-agnostic."""
