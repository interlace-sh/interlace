"""Incremental-by-time strategy.

Processes one time window ``[start, end)`` at a time: ensure the target exists,
delete that window, then insert the query filtered to it. Re-processing a window
is idempotent (delete + reinsert), which is what makes backfill and catchup safe.
The concrete interval comes from the scheduler/planner; the grain (``interval``
config) lives there, not here.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar, cast

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr


class IncrementalByTime(Strategy):
    """``CREATE IF NOT EXISTS`` + ``DELETE`` the window + ``INSERT`` the window's rows."""

    name: ClassVar[str] = "incremental_by_time"

    def __init__(self, time_column: str) -> None:
        if not time_column:
            raise PlanError("incremental_by_time requires a time_column")
        self.time_column = time_column

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
    ) -> list[exp.Expression]:
        if interval is None:
            raise PlanError("incremental_by_time requires an interval to process")
        query = relation.ast
        table = table_expr(target)

        def derived() -> exp.Subquery:
            return cast("exp.Query", query.copy()).subquery("_s")

        def window() -> exp.Expression:
            column = exp.column(self.time_column)
            return exp.And(
                this=exp.GTE(this=column.copy(), expression=exp.Literal.string(interval.start.isoformat())),
                expression=exp.LT(this=column.copy(), expression=exp.Literal.string(interval.end.isoformat())),
            )

        ensure = exp.Create(
            this=table.copy(), kind="TABLE", exists=True, expression=exp.select("*").from_(derived()).limit(0)
        )
        delete = exp.Delete(this=table.copy(), where=exp.Where(this=window()))
        insert = exp.Insert(this=table.copy(), expression=exp.select("*").from_(derived()).where(window()))
        return [ensure, delete, insert]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # [ensure, delete window, insert window]: catchup deletes 0; restate rewrites
        return RowCounts(inserted=_at(counts, 2), deleted=_at(counts, 1))
