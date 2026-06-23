"""Merge-by-key (upsert) strategy.

Keyed upsert without needing the model's column list (type annotation is
deferred): ensure the target exists with the query's shape, delete the rows whose
keys are about to be re-supplied, then insert the current rows. Portable across
engines and column-agnostic. apply runs the statements atomically.

A native single-statement ``MERGE`` (when ``caps.supports_merge``) is a later
optimisation — it needs the column list to build its SET clause, which arrives
with column lineage.
"""

from __future__ import annotations

from typing import ClassVar, cast

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import Strategy, table_expr


class MergeByKey(Strategy):
    """``CREATE IF NOT EXISTS`` + ``DELETE`` matching keys + ``INSERT`` current rows."""

    name: ClassVar[str] = "merge_by_key"

    def __init__(self, key: tuple[str, ...]) -> None:
        if not key:
            raise PlanError("merge_by_key requires a non-empty key")
        self.key = key

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
    ) -> list[exp.Expression]:
        query = relation.ast
        table = table_expr(target)

        def derived() -> exp.Subquery:  # a fresh "(<query>) AS _s" each time (no shared nodes)
            return cast("exp.Query", query.copy()).subquery("_s")

        ensure = exp.Create(
            this=table.copy(),
            kind="TABLE",
            exists=True,
            expression=exp.select("*").from_(derived()).limit(0),
        )
        key_source = exp.select(*self.key).from_(derived())
        left: exp.Expression = (
            exp.column(self.key[0]) if len(self.key) == 1 else exp.Tuple(expressions=[exp.column(k) for k in self.key])
        )
        delete = exp.Delete(
            this=table.copy(), where=exp.Where(this=exp.In(this=left, query=exp.Subquery(this=key_source)))
        )
        insert = exp.Insert(this=table.copy(), expression=query.copy())
        return [ensure, delete, insert]
