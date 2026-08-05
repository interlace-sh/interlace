"""Append strategy — add the query's rows to an externally-owned table.

``CREATE IF NOT EXISTS`` (first delivery) + ``INSERT`` the current query; nothing
is deleted. Only valid for ``materialise: table`` (a growing external log). The
insert binds positionally — safe because apply stages and aligns the source to the
target's column order before delivery.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr


class Append(Strategy):
    """``CREATE IF NOT EXISTS`` + ``INSERT`` — accumulates rows, deletes nothing."""

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> list[exp.Expression]:
        query = relation.ast
        table = table_expr(target)
        ensure = exp.Create(
            this=table.copy(),
            kind="TABLE",
            exists=True,
            expression=exp.select("*").from_(cast("exp.Query", query.copy()).subquery("_s")).limit(0),
        )
        insert = exp.Insert(this=table.copy(), expression=query.copy())
        return [ensure, insert]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # [ensure, insert]
        return RowCounts(inserted=_at(counts, 1))
