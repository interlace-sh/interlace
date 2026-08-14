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

- **portable in-place fallback** (column list known, engine without ``MERGE``):
  ``UPDATE target SET <columns> FROM (<query>) WHERE <key match>``, then ``INSERT``
  the rows whose key is absent. Two statements instead of one, but the same
  semantics as native ``MERGE`` — the SET list and the INSERT column list are both
  explicit, so a column outside the model's output is never touched.

- **column-agnostic fallback** (no column list at all — a first delivery, where the
  target does not exist yet and there is nothing to preserve): ensure the target
  exists, ``DELETE`` the rows whose keys are about to be re-supplied, then ``INSERT``
  the current rows.

Both keyed paths write only the columns they are given. On a target interlace
shares with another writer, apply passes just the model's own columns, so the
other writer's columns survive each run.

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
        if columns:
            if caps.supports_merge:
                return [self._merge(query, target, columns)]
            return self._update_insert(query, target, columns)
        return self._delete_insert(query, target)

    def _merge(self, query: exp.Expression, target: TableRef, columns: Sequence[str]) -> exp.Merge:
        """One ``MERGE INTO`` upsert over ``columns`` — the model's own columns, in the
        target's order. Columns outside the list are neither set nor inserted."""
        tgt = exp.table_(target.name, db=target.schema, catalog=target.catalog, alias=_TARGET)
        source = cast("exp.Query", query.copy()).subquery(_SOURCE)
        on = self._key_predicate(_TARGET)

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

    def _key_predicate(self, table_alias: str) -> exp.Expression:
        """``<table>.k = _s.k`` ANDed across the key."""
        match: exp.Expression | None = None
        for k in self.key:
            eq = exp.column(k, table=table_alias).eq(exp.column(k, table=_SOURCE))
            match = eq if match is None else exp.and_(match, eq)
        assert match is not None  # the constructor rejects an empty key
        return match

    def _unmatched(self, target: TableRef) -> exp.Expression:
        """``NOT EXISTS (SELECT 1 FROM target WHERE target.k = _s.k)`` — the source rows
        with no target row for their key.

        NOT EXISTS rather than ``key NOT IN (SELECT key FROM target)``: a single NULL key
        already in the target makes the ``IN`` subquery return NULL for every row, so the
        predicate is never true and *nothing* is inserted."""
        probe = exp.select(exp.Literal.number(1)).from_(table_expr(target)).where(self._key_predicate(target.name))
        return exp.Not(this=exp.Exists(this=probe))

    def _update_insert(self, query: exp.Expression, target: TableRef, columns: Sequence[str]) -> list[exp.Expression]:
        """The portable in-place upsert: UPDATE matched keys, INSERT unmatched ones.

        Both statements name ``columns`` explicitly, so — unlike ``_delete_insert`` —
        a matched row keeps any column outside that list, and an inserted row takes
        the target's DEFAULTs for them. The target must already exist (apply only
        knows a column list once it has described it).

        Unlike native ``MERGE`` this cannot raise on a non-unique key: ``UPDATE … FROM``
        picks one of the duplicate source rows rather than erroring. ``UPDATE … FROM`` is
        also not universal SQL (fine on Postgres/DuckDB/Redshift/Snowflake) — but every
        engine that would take this path instead of native ``MERGE`` is hypothetical
        today, since all shipped adapters advertise ``supports_merge``."""
        table = table_expr(target)

        def derived() -> exp.Subquery:  # a fresh "(<query>) AS _s" each time
            return cast("exp.Query", query.copy()).subquery(_SOURCE)

        statements: list[exp.Expression] = []
        non_key = [c for c in columns if c not in set(self.key)]
        if non_key:  # a key-only table has nothing to update on a match — INSERT only
            update = exp.Update(
                this=table.copy(),
                expressions=[exp.EQ(this=exp.column(c), expression=exp.column(c, table=_SOURCE)) for c in non_key],
            )
            update.set("from_", exp.From(this=derived()))
            update.set("where", exp.Where(this=self._key_predicate(target.name)))
            statements.append(update)

        insert = exp.Insert(
            this=exp.Schema(this=table.copy(), expressions=[exp.column(c) for c in columns]),
            expression=exp.select(*[exp.column(c, table=_SOURCE) for c in columns])
            .from_(derived())
            .where(self._unmatched(target)),
        )
        statements.append(insert)
        return statements

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
        if len(counts) == 2:  # [update matched keys, insert unmatched keys]
            return RowCounts(inserted=_at(counts, 1), updated=_at(counts, 0))
        # [ensure, delete existing keys, insert]: a deleted key was re-inserted -> update
        updated = _at(counts, 1)
        return RowCounts(inserted=max(0, _at(counts, 2) - updated), updated=updated)
