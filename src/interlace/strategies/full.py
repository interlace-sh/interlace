"""Full-refresh strategy: replace the whole table from the model's query."""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr


class FullRefresh(Strategy):
    """``CREATE OR REPLACE TABLE target AS <query>``, with a DROP+CREATE fallback."""

    name: ClassVar[str] = "full"

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
    ) -> list[exp.Expression]:
        table = table_expr(target)
        if caps.supports_create_or_replace:
            return [exp.Create(this=table, kind="TABLE", replace=True, expression=relation.ast)]
        return [
            exp.Drop(this=table, kind="TABLE", exists=True),
            exp.Create(this=table, kind="TABLE", expression=relation.ast),
        ]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # single CREATE (OR REPLACE) AS, or DROP + CREATE: the create writes every row
        return RowCounts(inserted=_at(counts, len(counts) - 1))
