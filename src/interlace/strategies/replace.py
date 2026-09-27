"""Replace strategy: rewrite the whole table from the model's query.

The default for an interlace-owned (``virtual``) table: ``CREATE OR REPLACE TABLE
target AS <query>`` (a DROP+CREATE fallback where that is unavailable). For an
externally-owned ``table`` the sibling :class:`~interlace.strategies.replace_in_place.ReplaceInPlace`
empties and refills instead, so the table is never dropped.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef, drop
from interlace.state.interval import Interval
from interlace.strategies.base import Strategy, WritePlan, table_expr


class Replace(Strategy):
    """``CREATE OR REPLACE TABLE target AS <query>``, with a DROP+CREATE fallback."""

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> WritePlan:
        table = table_expr(target)
        if caps.supports_create_or_replace:
            create = exp.Create(this=table, kind="TABLE", replace=True, expression=relation.ast)
            return WritePlan([create], ["insert"])
        return WritePlan(
            [drop(table, kind="TABLE"), exp.Create(this=table, kind="TABLE", expression=relation.ast)],
            ["ignore", "insert"],
        )
