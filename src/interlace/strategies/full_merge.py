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
Duplicate source rows collapse via EXCEPT's distinct semantics, like scd.
apply runs the statements atomically.

The two deletes read key sets from temporary tables, not from an inlined
``EXCEPT`` or source scan. On DuckLake, a ``DELETE`` whose subquery is
``source EXCEPT target`` aborts the process: ``DuckLakeDelete::Finalize``
throws ``Could not find matching file for written delete file``, the catalog
is invalidated, and the following ``ROLLBACK`` is a fatal exception thrown off
the Python thread (exit 134). A delete that only reads a local temp table does
not. The insert's ``EXCEPT`` stays a read — that path does not go through the
delete finalizer.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef, drop
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr

# Session-local. Dropped before create so a long-lived connection (Postgres)
# can run full_merge again; DuckDB cursors are fresh per batch either way.
_CHANGED_KEYS = "_interlace_fm_changed"
_VANISHED_KEYS = "_interlace_fm_vanished"


class FullMerge(Strategy):
    """``CREATE IF NOT EXISTS`` + stage key sets + delete + insert new versions."""

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
        columns: Sequence[str] | None = None,
    ) -> list[exp.Expr]:
        query = relation.ast
        table = table_expr(target)

        def source() -> exp.Select:  # fresh nodes each use
            return exp.select("*").from_(query.copy().subquery("_s"))

        def current() -> exp.Select:
            return exp.select("*").from_(table.copy())

        def fresh_keys() -> exp.Select:  # keys of source rows with no exact target match
            fresh = exp.Except(this=source(), expression=current(), distinct=True)
            return exp.select(*self.key).from_(exp.Subquery(this=fresh, alias=exp.TableAlias(this="_fresh")))

        def vanished_keys() -> exp.Select:  # target keys absent from the source
            source_keys = exp.select(*self.key).from_(query.copy().subquery("_s"))
            return (
                exp.select(*self.key)
                .from_(table.copy())
                .where(exp.Not(this=exp.In(this=_key(), query=exp.Subquery(this=source_keys))))
            )

        def _key() -> exp.Expr:
            if len(self.key) == 1:
                return exp.column(self.key[0])
            return exp.Tuple(expressions=[exp.column(k) for k in self.key])

        def _temp(name: str) -> exp.Table:
            return exp.Table(this=exp.to_identifier(name))

        def stage(name: str, keys: exp.Query) -> list[exp.Expr]:
            return [
                drop(_temp(name), kind="TABLE"),
                exp.Create(
                    this=_temp(name),
                    kind="TABLE",
                    properties=exp.Properties(expressions=[exp.TemporaryProperty()]),
                    expression=keys,
                ),
            ]

        def delete_in(name: str) -> exp.Delete:
            keys = exp.select(*self.key).from_(_temp(name))
            return exp.Delete(
                this=table.copy(),
                where=exp.Where(this=exp.In(this=_key(), query=exp.Subquery(this=keys))),
            )

        ensure = exp.Create(
            this=table.copy(),
            kind="TABLE",
            exists=True,
            expression=source().limit(0),
        )
        # recomputed after the deletes: exactly the new keys and new versions
        fresh = exp.Except(this=source(), expression=current(), distinct=True)
        insert = exp.Insert(
            this=table.copy(),
            expression=exp.select("*").from_(exp.Subquery(this=fresh, alias=exp.TableAlias(this="_fresh"))),
        )
        # Both key sets are computed from the pre-image, then applied.
        return [
            ensure,
            *stage(_CHANGED_KEYS, fresh_keys()),
            *stage(_VANISHED_KEYS, vanished_keys()),
            delete_in(_CHANGED_KEYS),
            delete_in(_VANISHED_KEYS),
            insert,
        ]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # [ensure, drop+stage changed keys, drop+stage vanished keys,
        #  delete changed, delete vanished, insert fresh versions]
        updated = _at(counts, 5)
        deleted = _at(counts, 6)
        return RowCounts(inserted=max(0, _at(counts, 7) - updated), updated=updated, deleted=deleted)
