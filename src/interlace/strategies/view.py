"""View strategy: the physical layer is a view over the model's query."""

from __future__ import annotations

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import Strategy, table_expr


class View(Strategy):
    """``CREATE OR REPLACE VIEW target AS <query>``, with a DROP+CREATE fallback."""

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
    ) -> list[exp.Expression]:
        view = table_expr(target)
        if caps.supports_create_or_replace:
            return [exp.Create(this=view, kind="VIEW", replace=True, expression=relation.ast)]
        return [
            exp.Drop(this=view, kind="VIEW", exists=True),
            exp.Create(this=view, kind="VIEW", expression=relation.ast),
        ]
