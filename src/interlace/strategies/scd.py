"""SCD (Type 2) strategy — keyed history tracking with validity windows.

The target carries the query's columns plus ``_valid_from`` / ``_valid_to``
(NULL = current). Each run compares the source against the *open* rows using
set difference — column-agnostic, no column list or row hash needed:

- open rows with no exact source match (content changed, or the key vanished)
  are **closed** (``_valid_to`` set);
- source rows with no exact open match (new keys, or the new version of a
  changed key) are **inserted** as current.

An unchanged row appears in neither difference, so re-running is a no-op. The key
may be composite. Requires star-exclude support to project the open rows without
the validity columns (DuckDB/Snowflake/BigQuery); apply runs the statements
atomically.

By default the windows are stamped with processing time (``CURRENT_TIMESTAMP``).
Pass a ``time_column`` (an event timestamp carried in the source) and the windows
follow the data instead: a new version's ``_valid_from`` is its own event time,
and the version it supersedes is closed at *that same* event time, so the windows
abut on when the change actually happened rather than when interlace saw it. A key
that vanishes upstream has no succeeding event, so it is still closed at
processing time.

History lives in the fingerprint's physical table: data changes under a stable
definition accumulate history across ``interlace run``; a *definition* change
mints a new fingerprint and starts a fresh table (snapshot semantics) — unless
applied with ``--forward-only``, which copies the history onto the new version
(copy-on-write: checks still gate, the old table remains the rollback until gc)
and the new logic applies going forward.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import ClassVar

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import RowCounts, Strategy, _at, table_expr

VALID_FROM = "_valid_from"
VALID_TO = "_valid_to"


def _null_timestamp() -> exp.Cast:
    return exp.Cast(this=exp.Null(), to=exp.DataType.build("TIMESTAMP"))


class Scd(Strategy):
    """``CREATE IF NOT EXISTS`` + close changed/vanished rows + insert new versions."""

    managed_columns: ClassVar[tuple[str, ...]] = (VALID_FROM, VALID_TO)

    def __init__(self, key: tuple[str, ...], time_column: str | None = None) -> None:
        if not key:
            raise PlanError("scd requires a non-empty key")
        self.key = key
        self.time_column = time_column

    def _valid_from(self) -> exp.Expr:
        """A fresh node for the validity-start value: the event time, or now()."""
        if self.time_column:
            return exp.cast(exp.column(self.time_column), "TIMESTAMP")
        return exp.CurrentTimestamp()

    def _open_columns(self, query: exp.Query, caps: EngineCaps) -> list[str] | None:
        """The model's own output columns, for projecting open rows without star-EXCLUDE.

        ``None`` when the engine has ``SELECT * EXCLUDE`` (we use it — and it copes with
        ``SELECT *`` models). Otherwise the columns are read off the query's projections
        so the same open-row comparison works on Postgres/Redshift; a ``SELECT *`` model
        can't be enumerated there and raises a clear error."""
        if caps.supports_star_exclude:
            return None
        projections = query.selects
        names = [projection.alias_or_name for projection in projections]
        if not names or any(isinstance(p, exp.Star) or not n for p, n in zip(projections, names, strict=True)):
            raise PlanError(
                "scd on an engine without SELECT * EXCLUDE (e.g. Postgres/Redshift) needs the model's "
                "columns spelled out — give it an explicit projection instead of SELECT *"
            )
        return names

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
        open_cols = self._open_columns(query, caps)  # None -> use SELECT * EXCLUDE

        def source() -> exp.Select:  # fresh nodes each use
            return exp.select("*").from_(query.copy().subquery("_s"))

        def open_rows() -> exp.Select:  # current rows, projected to the source's shape
            # SELECT * EXCLUDE(validity) where the engine has it; otherwise enumerate the
            # model's own columns explicitly — same projection, no star-exclude needed.
            selection: list[exp.Expr] = (
                [exp.Star(except_=[exp.column(VALID_FROM), exp.column(VALID_TO)])]
                if open_cols is None
                else [exp.column(c) for c in open_cols]
            )
            return exp.select(*selection).from_(table.copy()).where(exp.column(VALID_TO).is_(exp.Null()))

        def fresh_subquery() -> exp.Subquery:  # source rows with no exact open match
            fresh = exp.Except(this=source(), expression=open_rows(), distinct=True)
            return exp.Subquery(this=fresh, alias=exp.TableAlias(this="_fresh"))

        key_expr: exp.Expr = (
            exp.column(self.key[0]) if len(self.key) == 1 else exp.Tuple(expressions=[exp.column(k) for k in self.key])
        )

        ensure = exp.Create(
            this=table.copy(),
            kind="TABLE",
            exists=True,
            expression=exp.select(
                exp.Star(),
                exp.alias_(self._valid_from(), VALID_FROM),
                exp.alias_(_null_timestamp(), VALID_TO),
            )
            .from_(query.copy().subquery("_s"))
            .limit(0),
        )

        closes = self._closes(query, table, target, key_expr, source, open_rows)

        # Name the insert's columns when apply knows them. The validity pair is only
        # physically last on a table scd created in one shot: an additive ALTER (the
        # model grew a column) appends the new column AFTER them, and a positional
        # insert would then write it into _valid_from.
        into: exp.Expr = table.copy()
        if columns is not None:
            written = [*(c for c in columns if c not in self.managed_columns), VALID_FROM, VALID_TO]
            into = exp.Schema(this=table.copy(), expressions=[exp.column(c) for c in written])
        insert = exp.Insert(
            this=into,
            expression=exp.select(exp.Star(), self._valid_from(), _null_timestamp()).from_(fresh_subquery()),
        )
        return [ensure, *closes, insert]

    def _closes(
        self,
        query: exp.Query,
        table: exp.Table,
        target: TableRef,
        key_expr: exp.Expr,
        source: Callable[[], exp.Select],
        open_rows: Callable[[], exp.Select],
    ) -> list[exp.Expr]:
        """The UPDATE(s) that close open rows the source no longer matches."""
        if not self.time_column:
            # One UPDATE closes both changed and vanished keys at processing time.
            stale = exp.Except(this=open_rows(), expression=source(), distinct=True)
            stale_keys = exp.select(*self.key).from_(exp.Subquery(this=stale, alias=exp.TableAlias(this="_stale")))
            close = exp.Update(
                this=table.copy(),
                expressions=[exp.EQ(this=exp.column(VALID_TO), expression=exp.CurrentTimestamp())],
                where=exp.Where(
                    this=exp.and_(
                        exp.column(VALID_TO).is_(exp.Null()),
                        exp.In(this=key_expr, query=exp.Subquery(this=stale_keys)),
                    )
                ),
            )
            return [close]

        # Event-time: a changed key's open row closes at the succeeding version's event
        # time (join to `fresh`, which holds exactly the new versions); a vanished key
        # has no succeeding event, so close it at processing time.
        fresh = exp.Except(this=source(), expression=open_rows(), distinct=True)
        join_on: exp.Expr | None = exp.column(VALID_TO, table=target.name).is_(exp.Null())
        for k in self.key:
            match = exp.EQ(this=exp.column(k, table=target.name), expression=exp.column(k, table="_f"))
            join_on = exp.and_(join_on, match)
        succeeding = exp.cast(exp.column(self.time_column, table="_f"), "TIMESTAMP")
        close_changed = exp.Update(
            this=table.copy(),
            expressions=[exp.EQ(this=exp.column(VALID_TO), expression=succeeding)],
        )
        close_changed.set("from_", exp.From(this=exp.Subquery(this=fresh, alias=exp.TableAlias(this="_f"))))
        close_changed.set("where", exp.Where(this=join_on))

        source_keys = exp.select(*self.key).from_(query.copy().subquery("_s"))
        close_vanished = exp.Update(
            this=table.copy(),
            expressions=[exp.EQ(this=exp.column(VALID_TO), expression=exp.CurrentTimestamp())],
            where=exp.Where(
                this=exp.and_(
                    exp.column(VALID_TO).is_(exp.Null()),
                    exp.Not(this=exp.In(this=key_expr, query=exp.Subquery(this=source_keys))),
                )
            ),
        )
        return [close_changed, close_vanished]

    def row_counts(self, counts: Sequence[int]) -> RowCounts:
        # processing-time: [ensure, close, insert]; event-time: [ensure, close_changed,
        # close_vanished, insert]. Every close is a history row end-dated (an update).
        inserted = _at(counts, len(counts) - 1)
        updated = sum(_at(counts, i) for i in range(1, len(counts) - 1))
        return RowCounts(inserted=inserted, updated=updated)
