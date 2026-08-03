"""Full-merge strategy — a full-state source applied as a minimal diff.

For sources that can only supply the complete current state (an API list
endpoint with no updated-since filter, a snapshot export), a plain full refresh
rewrites every row on every run. ``full_merge`` treats the query as the desired
state and applies only the difference, using set difference — column-agnostic,
no column list or row hash needed (EXCEPT *is* the hash):

- source rows with no exact target match (new keys, or changed content) are
  the **fresh** set: their keys' old versions are deleted, then they are inserted;
- target rows whose key vanished from the source are **deleted** (the source is
  the full state, so absence means deletion upstream).

An unchanged row appears in no difference, so a run over identical data writes
nothing — on snapshotting stores (DuckLake) that means no new files. Keys must
be non-NULL (a NULL key never compares equal, so it would churn every run).
Duplicate source rows collapse via EXCEPT's distinct semantics, like scd_type_2.
apply runs the statements atomically.
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


class FullMerge(Strategy):
    """``CREATE IF NOT EXISTS`` + delete changed/vanished keys + insert new versions."""

    def __init__(self, key: tuple[str, ...]) -> None:
        if not key:
            raise PlanError("full_merge requires a non-empty key")
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

        def source() -> exp.Select:  # fresh nodes each use
            return exp.select("*").from_(cast("exp.Query", query.copy()).subquery("_s"))

        def current() -> exp.Select:
            return exp.select("*").from_(table.copy())

        def fresh_keys() -> exp.Select:  # keys of source rows with no exact target match
            fresh = exp.Except(this=source(), expression=current(), distinct=True)
            return exp.select(*self.key).from_(exp.Subquery(this=fresh, alias=exp.TableAlias(this="_fresh")))

        key_expr: exp.Expression = (
            exp.column(self.key[0]) if len(self.key) == 1 else exp.Tuple(expressions=[exp.column(k) for k in self.key])
        )

        ensure = exp.Create(
            this=table.copy(),
            kind="TABLE",
            exists=True,
            expression=source().limit(0),
        )
        # old versions of changed rows (their key is in the fresh set)
        delete_changed = exp.Delete(
            this=table.copy(), where=exp.Where(this=exp.In(this=key_expr, query=exp.Subquery(this=fresh_keys())))
        )
        # keys absent from the source were deleted upstream
        source_keys = exp.select(*self.key).from_(cast("exp.Query", query.copy()).subquery("_s"))
        delete_missing = exp.Delete(
            this=table.copy(),
            where=exp.Where(
                this=exp.Not(this=exp.In(this=key_expr.copy(), query=exp.Subquery(this=source_keys))),
            ),
        )
        # recomputed after the deletes: exactly the new keys and new versions
        fresh = exp.Except(this=source(), expression=current(), distinct=True)
        insert = exp.Insert(
            this=table.copy(),
            expression=exp.select("*").from_(exp.Subquery(this=fresh, alias=exp.TableAlias(this="_fresh"))),
        )
        return [ensure, delete_changed, delete_missing, insert]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # [ensure, delete changed keys, delete vanished keys, insert fresh versions]
        updated = _at(counts, 1)
        return RowCounts(inserted=max(0, _at(counts, 3) - updated), updated=updated, deleted=_at(counts, 2))
