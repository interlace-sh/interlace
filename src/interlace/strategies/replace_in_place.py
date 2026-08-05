"""Replace-in-place strategy — for an externally-owned table (``materialise: table``).

The equivalent of ``replace`` for a table interlace does *not* own: the live table is
emptied and re-filled, but **never dropped**, so grants, indexes, RLS and readers
survive. ``CREATE IF NOT EXISTS`` (first delivery), ``DELETE`` all rows, then
``INSERT`` the current query. The insert binds positionally — safe because apply
stages and aligns the source to the target's column order before delivery.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import cast

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr


class ReplaceInPlace(Strategy):
    """``CREATE IF NOT EXISTS`` + ``DELETE`` all + ``INSERT`` — never drops the table."""

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
        wipe = exp.Delete(this=table.copy())  # empty in place, never drop
        insert = exp.Insert(this=table.copy(), expression=query.copy())
        return [ensure, wipe, insert]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # [ensure, wipe, insert]: the wipe clears the previous delivery
        return RowCounts(inserted=_at(counts, 2), deleted=_at(counts, 1))
