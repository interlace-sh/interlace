"""Hash-merge strategy — a keyed upsert that writes only what actually changed.

Like ``merge`` it upserts on a key and keeps rows whose key is absent from the
source (it is an upsert, not a full-state sync — that is ``full_merge``). Unlike
``merge`` it stores an ``_hash`` column (an ``md5`` of the non-key columns) and
compares it, so:

- a **new** key is inserted;
- an **existing** key whose ``_hash`` differs is updated;
- an **existing** key whose ``_hash`` matches is skipped — no write.

So a run over identical data writes nothing (idempotent — no new files on a
snapshotting store), the reported counts split cleanly into inserted vs updated,
and change detection is an O(key) hash compare rather than ``full_merge``'s
whole-row ``EXCEPT`` — cheaper on wide tables. ``_hash`` is an ordinary stored
column, visible to consumers. Keys must be non-NULL. A ``SELECT *`` model needs
its columns spelled out (the hash is built from the projection). apply runs the
statements atomically.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar, cast

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr

HASH_COLUMN = "_hash"
_SOURCE = "_s"


class HashMerge(Strategy):
    """``CREATE IF NOT EXISTS`` + update-where-hash-differs + insert-new-keys."""

    managed_columns: ClassVar[tuple[str, ...]] = (HASH_COLUMN,)

    def __init__(self, key: tuple[str, ...]) -> None:
        if not key:
            raise PlanError("hash_merge requires a non-empty key")
        self.key = key

    def _payload_columns(self, query: exp.Expression, columns: Sequence[str] | None) -> list[str]:
        """The non-key columns the hash is built from — from the aligned target column
        list when apply knows it, else read off the query's projections."""
        key_set = set(self.key)
        if columns is not None:
            return [c for c in columns if c not in key_set and c != HASH_COLUMN]
        projections = cast("exp.Query", query).selects
        names = [p.alias_or_name for p in projections]
        if not names or any(isinstance(p, exp.Star) or not n for p, n in zip(projections, names, strict=True)):
            raise PlanError(
                "hash_merge needs the model's columns spelled out — give it an explicit projection instead of SELECT *"
            )
        return [n for n in names if n not in key_set]

    def _hash_expr(self, payload: Sequence[str]) -> exp.Expression:
        """``MD5(CONCAT_WS('||', COALESCE(CAST(col AS VARCHAR), '')...))`` over the
        payload columns. COALESCE so a NULL never collapses the separator layout."""
        if not payload:  # key-only table: nothing to compare, a constant hash (never "changes")
            return exp.func("MD5", exp.Literal.string(""))
        parts = [exp.func("COALESCE", exp.cast(exp.column(c), "VARCHAR"), exp.Literal.string("")) for c in payload]
        return exp.func("MD5", exp.func("CONCAT_WS", exp.Literal.string("||"), *parts))

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
        columns: Sequence[str] | None = None,
    ) -> list[exp.Expression]:
        query = relation.ast
        table = table_expr(target)
        payload = self._payload_columns(query, columns)

        def source_hashed() -> exp.Select:  # the model's rows + a computed _hash; fresh nodes each use
            inner = cast("exp.Query", query.copy()).subquery("_src")
            return exp.select(exp.Star(), exp.alias_(self._hash_expr(payload), HASH_COLUMN)).from_(inner)

        source_key: exp.Expression = (
            exp.column(self.key[0], table=_SOURCE)
            if len(self.key) == 1
            else exp.Tuple(expressions=[exp.column(k, table=_SOURCE) for k in self.key])
        )

        ensure = exp.Create(this=table.copy(), kind="TABLE", exists=True, expression=source_hashed().limit(0))

        # UPDATE the payload + hash for keys whose stored hash differs from the source's
        match: exp.Expression = exp.column(HASH_COLUMN, table=target.name).neq(exp.column(HASH_COLUMN, table=_SOURCE))
        for k in self.key:
            match = exp.and_(match, exp.column(k, table=target.name).eq(exp.column(k, table=_SOURCE)))
        update = exp.Update(
            this=table.copy(),
            expressions=[
                exp.EQ(this=exp.column(c), expression=exp.column(c, table=_SOURCE)) for c in (*payload, HASH_COLUMN)
            ],
        )
        update.set("from_", exp.From(this=exp.Subquery(this=source_hashed(), alias=exp.TableAlias(this=_SOURCE))))
        update.set("where", exp.Where(this=match))

        # INSERT source rows whose key is not already present
        target_keys = exp.select(*self.key).from_(table.copy())
        insert = exp.Insert(
            this=table.copy(),
            expression=exp.select("*")
            .from_(exp.Subquery(this=source_hashed(), alias=exp.TableAlias(this=_SOURCE)))
            .where(exp.Not(this=exp.In(this=source_key, query=exp.Subquery(this=target_keys)))),
        )
        return [ensure, update, insert]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # [ensure, update changed keys, insert new keys]
        return RowCounts(inserted=_at(counts, 2), updated=_at(counts, 1))
