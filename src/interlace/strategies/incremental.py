"""Incremental strategy — one time window at a time.

Both modes read only the rows whose ``time_column`` falls in the window
``[start, end)`` the scheduler/planner supplies. They differ in what the window
means for the target:

- **without ``key`` (the default)** — the window is *authoritative*. Delete
  everything already in ``[start, end)``, then insert the window's rows. A row
  that vanished from the source disappears from the target. Delete-then-reinsert
  is what makes reprocessing a window idempotent, and therefore what makes
  backfill and ``restate`` safe.

- **with ``key``** — the window only *bounds what is read*. Rows are upserted by
  key, so a target row inside the window that the source no longer produces is
  left alone. This is the mode for late-arriving corrections to rows you have
  already published, where the window is a cheap way to avoid rescanning history
  rather than a statement about what the period should contain.

The grain (``interval`` config) lives with the planner, not here.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr
from interlace.strategies.merge import Merge


class Incremental(Strategy):
    """``DELETE`` the window + ``INSERT`` it, or — with a ``key`` — upsert within it."""

    def __init__(self, time_column: str, key: tuple[str, ...] = ()) -> None:
        if not time_column:
            raise PlanError("incremental requires a time_column")
        self.time_column = time_column
        self.key = key
        # Keyed mode is a merge whose source happens to be window-filtered, so the
        # upsert itself (native MERGE, or the portable DELETE+INSERT fallback) is
        # the one in Merge rather than a second copy of it here.
        self._merge = Merge(key) if key else None

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> list[exp.Expression]:
        if interval is None:
            raise PlanError("incremental requires an interval to process")
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

        if self._merge is not None:
            # The window bounds the source; the key decides what is written.
            windowed = exp.select("*").from_(derived()).where(window())
            return self._merge.plan_statements(SqlRelation(ast=windowed), target, caps, interval, columns)

        ensure = exp.Create(
            this=table.copy(), kind="TABLE", exists=True, expression=exp.select("*").from_(derived()).limit(0)
        )
        delete = exp.Delete(this=table.copy(), where=exp.Where(this=window()))
        insert = exp.Insert(this=table.copy(), expression=exp.select("*").from_(derived()).where(window()))
        return [ensure, delete, insert]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        if self._merge is not None:
            return self._merge.row_counts(counts)
        # [ensure, delete window, insert window]: catchup deletes 0; restate rewrites
        return RowCounts(inserted=_at(counts, 2), deleted=_at(counts, 1))
