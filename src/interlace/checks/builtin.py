"""Built-in checks as AST builders.

Every check compiles to one SELECT returning a single ``failures`` bigint —
0 means pass. Row-predicated checks count violating rows; table-level checks
(row_count, freshness) collapse their condition to 0/1. Queries are sqlglot
expressions end to end, so they transpile with the engine dialect like any
strategy output.

NULL semantics (deliberate change from v0.x): ``pattern``, ``range``,
``accepted_values``, and ``relationships`` ignore NULLs — ``not_null`` is the
dedicated null check, so combine them when NULLs should also fail (v0.x counted
NULLs as pattern/range failures, which conflated the two).
"""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Any

from sqlglot import exp, parse_one

from interlace.checks.spec import CheckSpec
from interlace.exceptions import DefinitionError
from interlace.ir.relation import TableRef

_FAILURES = "failures"
_AGE_RE = re.compile(r"^\s*(\d+)\s*([smhdw])\s*$")
_AGE_UNITS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY", "w": "WEEK"}

ResolveTable = Callable[[str], TableRef]
"""Maps an upstream model name to its physical table (for ``relationships``)."""


def _table(ref: TableRef) -> exp.Table:
    return exp.table_(ref.name, db=ref.schema or None, catalog=ref.catalog)


def _count_where(table: TableRef, condition: exp.Expression) -> exp.Select:
    return exp.select(exp.alias_(exp.Count(this=exp.Star()), _FAILURES)).from_(_table(table)).where(condition)


def _flag(table: TableRef, condition: exp.Expression) -> exp.Select:
    """1 if ``condition`` holds over the whole table, else 0."""
    flag = exp.Case().when(condition, exp.Literal.number(1)).else_(exp.Literal.number(0))
    return exp.select(exp.alias_(flag, _FAILURES)).from_(_table(table))


def _require(spec: CheckSpec, model: str, *, columns: int | None = None, params: tuple[str, ...] = ()) -> None:
    if columns is not None and len(spec.columns) < columns:
        raise DefinitionError(f"check {spec.type!r} on {model!r} needs a column")
    for name in params:
        if name not in spec.params:
            raise DefinitionError(f"check {spec.type!r} on {model!r} needs {name!r}")


def _parse_expr(sql: str, dialect: str) -> exp.Expression:
    return parse_one(sql, read=dialect)


def _interval(max_age: str) -> exp.Interval:
    match = _AGE_RE.fullmatch(str(max_age))
    if match is None:
        raise DefinitionError(f"invalid max_age {max_age!r}; expected like '2h', '30m', '1d'")
    return exp.Interval(this=exp.Literal.string(match.group(1)), unit=exp.Var(this=_AGE_UNITS[match.group(2)]))


def build_check_query(
    spec: CheckSpec, table: TableRef, model: str, dialect: str, resolve: ResolveTable
) -> exp.Expression:
    """Compile ``spec`` against ``table`` into a query returning ``failures``."""
    cols = [exp.column(c) for c in spec.columns]
    params: dict[str, Any] = spec.params

    if spec.type == "not_null":
        _require(spec, model, columns=1)
        condition: exp.Expression = exp.or_(*[col.is_(exp.null()) for col in cols])
        return _count_where(table, condition)

    if spec.type == "unique":
        _require(spec, model, columns=1)
        grouped = (
            exp.select(*cols)
            .from_(_table(table))
            .group_by(*cols)
            .having(exp.GT(this=exp.Count(this=exp.Star()), expression=exp.Literal.number(1)))
        )
        return exp.select(exp.alias_(exp.Count(this=exp.Star()), _FAILURES)).from_(grouped.subquery("dup"))

    if spec.type == "accepted_values":
        _require(spec, model, columns=1, params=("values",))
        values = [exp.Literal.string(v) if isinstance(v, str) else exp.Literal.number(v) for v in params["values"]]
        col = cols[0]
        return _count_where(table, exp.and_(col.is_(exp.null()).not_(), exp.In(this=col, expressions=values).not_()))

    if spec.type == "range":
        _require(spec, model, columns=1)
        if "min" not in params and "max" not in params:
            raise DefinitionError(f"check 'range' on {model!r} needs min and/or max")
        col = cols[0]
        bounds: list[exp.Expression] = []
        if "min" in params:
            bounds.append(exp.LT(this=col, expression=exp.Literal.number(params["min"])))
        if "max" in params:
            bounds.append(exp.GT(this=col, expression=exp.Literal.number(params["max"])))
        return _count_where(table, exp.or_(*bounds))

    if spec.type == "pattern":
        _require(spec, model, columns=1, params=("regex",))
        col = cols[0]
        matches = exp.RegexpLike(this=col, expression=exp.Literal.string(str(params["regex"])))
        return _count_where(table, exp.and_(col.is_(exp.null()).not_(), matches.not_()))

    if spec.type == "expression":
        _require(spec, model, params=("expression",))
        predicate = _parse_expr(str(params["expression"]), dialect)
        return _count_where(table, exp.paren(predicate).not_())

    if spec.type == "relationships":
        _require(spec, model, columns=1, params=("to", "field"))
        col = cols[0]
        parent = exp.select(exp.column(str(params["field"]))).from_(_table(resolve(str(params["to"]))))
        orphan = exp.In(this=col, query=exp.Subquery(this=parent)).not_()
        return _count_where(table, exp.and_(col.is_(exp.null()).not_(), orphan))

    if spec.type == "row_count":
        if "min" not in params and "max" not in params:
            raise DefinitionError(f"check 'row_count' on {model!r} needs min and/or max")
        count = exp.Count(this=exp.Star())
        counts: list[exp.Expression] = []
        if "min" in params:
            counts.append(exp.LT(this=count, expression=exp.Literal.number(params["min"])))
        if "max" in params:
            counts.append(exp.GT(this=count.copy(), expression=exp.Literal.number(params["max"])))
        return _flag(table, exp.or_(*counts))

    if spec.type == "freshness":
        _require(spec, model, columns=1, params=("max_age",))
        threshold = exp.Sub(this=exp.CurrentTimestamp(), expression=_interval(str(params["max_age"])))
        newest = exp.Max(this=cols[0])
        stale = exp.or_(exp.LT(this=newest, expression=threshold), newest.copy().is_(exp.null()))  # no rows = stale
        return _flag(table, stale)

    if spec.type == "sql":
        _require(spec, model, params=("query",))
        sql = str(params["query"]).replace("{table}", _table(table).sql(dialect=dialect))
        inner = _parse_expr(sql, dialect)
        return exp.select(exp.alias_(exp.Count(this=exp.Star()), _FAILURES)).from_(
            exp.Subquery(this=inner, alias=exp.TableAlias(this=exp.to_identifier("q")))
        )

    raise DefinitionError(f"unknown check type {spec.type!r} on {model!r}")
