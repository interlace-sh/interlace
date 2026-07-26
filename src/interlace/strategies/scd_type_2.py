"""SCD Type 2 strategy — keyed history tracking with validity windows.

The target carries the query's columns plus ``_valid_from`` / ``_valid_to``
(NULL = current). Each run compares the source against the *open* rows using
set difference — column-agnostic, no column list or row hash needed:

- open rows with no exact source match (content changed, or the key vanished)
  are **closed** (``_valid_to = now``);
- source rows with no exact open match (new keys, or the new version of a
  changed key) are **inserted** as current.

An unchanged row appears in neither difference, so re-running is a no-op.
Requires star-exclude support to project the open rows without the validity
columns (DuckDB/Snowflake/BigQuery); apply runs the statements atomically.

History lives in the fingerprint's physical table: data changes under a stable
definition accumulate history across ``interlace run``; a *definition* change
mints a new fingerprint and starts a fresh table (snapshot semantics) — unless
applied with ``--forward-only``, which inherits the previous table so history
survives and the new logic applies going forward.
"""

from __future__ import annotations

from typing import ClassVar, cast

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.state.interval import Interval
from interlace.strategies.base import Strategy, table_expr

VALID_FROM = "_valid_from"
VALID_TO = "_valid_to"


def _null_timestamp() -> exp.Cast:
    return exp.Cast(this=exp.Null(), to=exp.DataType.build("TIMESTAMP"))


class ScdType2(Strategy):
    """``CREATE IF NOT EXISTS`` + close changed/vanished rows + insert new versions."""

    name: ClassVar[str] = "scd_type_2"

    def __init__(self, key: tuple[str, ...]) -> None:
        if not key:
            raise PlanError("scd_type_2 requires a non-empty key")
        self.key = key

    def plan_statements(
        self,
        relation: SqlRelation,
        target: TableRef,
        caps: EngineCaps,
        interval: Interval | None = None,
    ) -> list[exp.Expression]:
        if not caps.supports_star_exclude:
            raise PlanError(
                "scd_type_2 needs star-EXCLUDE projections, which this engine lacks "
                "(DuckDB-family/Snowflake/BigQuery only for now)"
            )
        query = relation.ast
        table = table_expr(target)

        def source() -> exp.Select:  # fresh nodes each use
            return exp.select("*").from_(cast("exp.Query", query.copy()).subquery("_s"))

        def open_rows() -> exp.Select:  # current rows, projected to the source's shape
            star = exp.Star(except_=[exp.column(VALID_FROM), exp.column(VALID_TO)])
            return exp.select(star).from_(table.copy()).where(exp.column(VALID_TO).is_(exp.Null()))

        ensure = exp.Create(
            this=table.copy(),
            kind="TABLE",
            exists=True,
            expression=exp.select(
                exp.Star(),
                exp.alias_(exp.CurrentTimestamp(), VALID_FROM),
                exp.alias_(_null_timestamp(), VALID_TO),
            )
            .from_(cast("exp.Query", query.copy()).subquery("_s"))
            .limit(0),
        )

        # keys whose open row no longer matches any source row exactly (changed or deleted)
        stale = exp.Except(this=open_rows(), expression=source(), distinct=True)
        stale_keys = exp.select(*self.key).from_(exp.Subquery(this=stale, alias=exp.TableAlias(this="_stale")))
        key_expr: exp.Expression = (
            exp.column(self.key[0]) if len(self.key) == 1 else exp.Tuple(expressions=[exp.column(k) for k in self.key])
        )
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

        # source rows with no exact open match: brand-new keys or new versions
        fresh = exp.Except(this=source(), expression=open_rows(), distinct=True)
        insert = exp.Insert(
            this=table.copy(),
            expression=exp.select(
                exp.Star(),
                exp.CurrentTimestamp(),
                _null_timestamp(),
            ).from_(exp.Subquery(this=fresh, alias=exp.TableAlias(this="_fresh"))),
        )
        return [ensure, close, insert]
