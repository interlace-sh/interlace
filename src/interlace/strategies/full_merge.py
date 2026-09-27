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

On engines that can put ``EXCEPT`` inside a ``DELETE``, the changed-key delete
inlines ``source EXCEPT target``. DuckLake cannot: ``DuckLakeDelete::Finalize``
throws ``Could not find matching file for written delete file``, the catalog
is invalidated, and the following ``ROLLBACK`` is a fatal exception thrown off
the Python thread (exit 134). That engine (``except_in_delete`` off) stages
both key sets into temporary tables first; each ``DELETE`` then reads only
the temp table. The insert's ``EXCEPT`` stays a read either way — that path
does not go through the delete finalizer. An engine that rejects every
subquery in a ``DELETE`` (Spark/Delta) raises at plan time.
"""

from __future__ import annotations

from collections.abc import Sequence

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef, drop
from interlace.state.interval import Interval
from interlace.strategies.base import Strategy, WritePlan, table_expr

# Session-local. Dropped before create so a long-lived connection (Postgres)
# can run full_merge again; DuckDB cursors are fresh per batch either way.
_CHANGED_KEYS = "_interlace_fm_changed"
_VANISHED_KEYS = "_interlace_fm_vanished"


class FullMerge(Strategy):
    """``CREATE IF NOT EXISTS`` + delete changed and vanished keys + insert new versions."""

    accumulates = True
    requires_key = True

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
    ) -> WritePlan:
        if not caps.supports_mutation_subquery:
            raise PlanError(
                "full_merge deletes with a subquery in the DELETE condition; this engine rejects those. "
                "Use merge, or a catalog that allows subqueries in DELETE."
            )
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

        def delete_keys(keys: exp.Query) -> exp.Delete:
            return exp.Delete(
                this=table.copy(),
                where=exp.Where(this=exp.In(this=_key(), query=exp.Subquery(this=keys))),
            )

        def delete_in(name: str) -> exp.Delete:
            return delete_keys(exp.select(*self.key).from_(_temp(name)))

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
        # upsert_delete: changed keys are removed and re-inserted (an update).
        # delete: vanished keys are gone. The insert count includes both new keys
        # and new versions, so interpret_counts subtracts the changed-key deletes.
        if caps.except_in_delete:
            return WritePlan(
                [ensure, delete_keys(fresh_keys()), delete_keys(vanished_keys()), insert],
                ["ignore", "upsert_delete", "delete", "insert"],
            )
        # Both key sets are computed from the pre-image, then applied. The deletes
        # read only the temp tables — DuckLake aborts if EXCEPT is inside the DELETE.
        return WritePlan(
            [
                ensure,
                *stage(_CHANGED_KEYS, fresh_keys()),
                *stage(_VANISHED_KEYS, vanished_keys()),
                delete_in(_CHANGED_KEYS),
                delete_in(_VANISHED_KEYS),
                insert,
            ],
            ["ignore", "ignore", "ignore", "ignore", "ignore", "upsert_delete", "delete", "insert"],
        )
