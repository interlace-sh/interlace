"""Append strategy — add the query's rows to an externally-owned table.

``CREATE IF NOT EXISTS`` (first delivery) + ``INSERT`` the current query; nothing
is deleted. Only valid for ``materialise: table`` (a growing external log). Once
apply has aligned the source to an existing target the insert names its columns, so
a target column the model doesn't produce takes its DEFAULT rather than a NULL; on
a first delivery it binds positionally against the table the ensure just created.
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

    @property
    def writes_named_columns(self) -> bool:
        return True

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
        into: exp.Expression = table.copy()
        if columns:  # aligned against an existing target: bind by name, leave the rest to DEFAULT
            into = exp.Schema(this=table.copy(), expressions=[exp.column(c) for c in columns])
        insert = exp.Insert(this=into, expression=query.copy())
        return [ensure, insert]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # [ensure, insert]
        return RowCounts(inserted=_at(counts, 1))
