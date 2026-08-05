"""Merge (keyed upsert) strategy.

Two implementations behind one strategy, chosen by what the engine and the caller
can offer:

- **native ``MERGE``** (``caps.supports_merge`` and the target column list is known):
  a single ``MERGE INTO target USING (<query>) ON key WHEN MATCHED THEN UPDATE
  WHEN NOT MATCHED THEN INSERT``. Matched rows are updated in place — surrogate ids,
  untouched columns and row identity survive, and the engine fires UPDATE (not
  DELETE+INSERT) triggers. Needs the non-key column list to build the SET clause,
  which apply already has in hand on the staged delivery paths (it ``describe``s the
  target to align the source).

- **portable fallback** (no column list, or an engine without ``MERGE``): ensure the
  target exists, ``DELETE`` the rows whose keys are about to be re-supplied, then
  ``INSERT`` the current rows — column-agnostic, works everywhere.

The source is not deduplicated: two source rows matching one target row is a real
"your key isn't unique" bug, and native ``MERGE`` surfaces it as a cardinality
error rather than us paying for a distinct pass on every run. apply runs the
statements atomically.
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

_TARGET, _SOURCE = "_t", "_s"


class Merge(Strategy):
    """Native ``MERGE`` upsert, or ``CREATE IF NOT EXISTS`` + ``DELETE`` + ``INSERT``."""

    def __init__(self, key: tuple[str, ...]) -> None:
        if not key:
            raise PlanError("merge requires a non-empty key")
        self.key = key

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> list[exp.Expression]:
        query = relation.ast
        if caps.supports_merge and columns:
            return [self._merge(query, target, columns)]
        return self._delete_insert(query, target)

    def _merge(self, query: exp.Expression, target: TableRef, columns: Sequence[str]) -> exp.Merge:
        """One ``MERGE INTO`` upsert. ``columns`` is the target's full column order."""
        tgt = exp.table_(target.name, db=target.schema, catalog=target.catalog, alias=_TARGET)
        source = cast("exp.Query", query.copy()).subquery(_SOURCE)

        on: exp.Expression | None = None
        for k in self.key:
            match = exp.EQ(this=exp.column(k, table=_TARGET), expression=exp.column(k, table=_SOURCE))
            on = match if on is None else exp.And(this=on, expression=match)

        key_set = set(self.key)
        whens: list[exp.When] = []
        non_key = [c for c in columns if c not in key_set]
        if non_key:  # a key-only table has nothing to update on a match — INSERT only
            sets = [exp.EQ(this=exp.column(c), expression=exp.column(c, table=_SOURCE)) for c in non_key]
            whens.append(exp.When(matched=True, then=exp.Update(expressions=sets)))
        whens.append(
            exp.When(
                matched=False,
                then=exp.Insert(
                    this=exp.Tuple(expressions=[exp.column(c) for c in columns]),
                    expression=exp.Tuple(expressions=[exp.column(c, table=_SOURCE) for c in columns]),
                ),
            )
        )
        return exp.Merge(this=tgt, using=source, on=on, whens=exp.Whens(expressions=whens))

    def _delete_insert(self, query: exp.Expression, target: TableRef) -> list[exp.Expression]:
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

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        if len(counts) == 1:  # native MERGE: one combined affected-row count, no insert/update split
            return RowCounts(inserted=_at(counts, 0))
        # [ensure, delete existing keys, insert]: a deleted key was re-inserted -> update
        updated = _at(counts, 1)
        return RowCounts(inserted=max(0, _at(counts, 2) - updated), updated=updated)
