"""View strategy: the physical layer is a view over the model's query."""

from __future__ import annotations

from collections.abc import Sequence

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef, drop
from interlace.state.interval import Interval
from interlace.strategies.base import Strategy, WritePlan, table_expr


class View(Strategy):
    """``CREATE OR REPLACE VIEW target AS <query>``, with a DROP+CREATE fallback."""

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> WritePlan:
        view = table_expr(target)
        if caps.supports_create_or_replace:
            return WritePlan([exp.Create(this=view, kind="VIEW", replace=True, expression=relation.ast)], ["ignore"])
        return WritePlan(
            [drop(view, kind="VIEW"), exp.Create(this=view, kind="VIEW", expression=relation.ast)],
            ["ignore", "ignore"],
        )
