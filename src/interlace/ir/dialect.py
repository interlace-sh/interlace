"""Dialect fixes applied at transpile time.

Strategies emit one canonical AST. sqlglot renders it, but a few DuckDB forms
survive that render as SQL Postgres and Redshift reject: ``round`` of a float
with a scale (cast to ``DECIMAL``, then back to ``DOUBLE`` so the result is not
an unconstrained ``NUMERIC``), ``unnest(generate_series(...))`` (Postgres' series is already a
set), a computed ``INTERVAL <expr> <unit>`` (the generator drops ``<expr>``),
and ``range(n)`` when the model was parsed as the engine dialect rather than
DuckDB. DuckDB's ``hash()`` has no Postgres equivalent; it becomes a
non-negative ``hashtextextended``, which is a different function.
"""

from __future__ import annotations

from typing import cast

from sqlglot import exp

_POSTGRES_FAMILY = frozenset({"postgres", "redshift"})
_SERIES = frozenset({"GenerateSeries", "ExplodingGenerateSeries"})
_NUMERIC = frozenset({exp.DataType.Type.DECIMAL, exp.DataType.Type.BIGDECIMAL})
# Clear the sign bit. abs() of the most-negative bigint overflows in Postgres.
_SIGN_MASK = (1 << 63) - 1


def render_sql(ast: exp.Expr, dialect: str) -> str:
    """Canonical AST as ``dialect`` SQL, with the fixes above applied first."""
    if dialect in _POSTGRES_FAMILY:
        # Children first. sqlglot's transform visits parents first and, when a
        # parent is replaced, never rewrites the copy of its children.
        ast = _rewrite_tree(ast.copy(), dialect)
    return ast.sql(dialect=dialect)


def _rewrite_tree(node: exp.Expr, dialect: str) -> exp.Expr:
    for key, value in list(node.args.items()):
        if isinstance(value, exp.Expr):
            node.set(key, _rewrite_tree(value, dialect))
        elif isinstance(value, list):
            node.set(
                key,
                [_rewrite_tree(item, dialect) if isinstance(item, exp.Expr) else item for item in value],
            )
    return _rewrite(node, dialect)


def _rewrite(node: exp.Expr, dialect: str) -> exp.Expr:
    if dialect == "postgres" and isinstance(node, exp.Anonymous) and _name(node) == "hash" and node.expressions:
        return _postgres_hash(node)
    if isinstance(node, exp.Anonymous) and _name(node) == "range" and node.expressions:
        series = _range_series(node)
        if series is not None:
            return series
    if isinstance(node, exp.Explode) and type(node.this).__name__ in _SERIES:
        return cast(exp.Expr, node.this)
    if isinstance(node, exp.Interval) and not isinstance(node.this, exp.Literal):
        return _interval_multiply(node)
    if isinstance(node, exp.Round) and node.args.get("decimals") is not None and not _is_numeric(node.this):
        # Postgres round() returns unconstrained NUMERIC, which ADBC hands to
        # Python as an opaque string. Cast back to a float so the column type
        # matches DuckDB's round of a float expression and stays computable.
        rewritten = node.copy()
        rewritten.set("this", exp.Cast(this=node.this.copy(), to=exp.DataType.build("DECIMAL")))
        return exp.Cast(this=rewritten, to=exp.DataType.build("DOUBLE"))
    return node


def _name(node: exp.Anonymous) -> str:
    return str(node.this).casefold()


def _postgres_hash(node: exp.Anonymous) -> exp.Expr:
    """A non-negative stand-in for DuckDB ``hash()``. The values do not match."""
    arg: exp.Expr = node.expressions[0]
    if len(node.expressions) > 1:
        arg = exp.Concat(expressions=[e.copy() for e in node.expressions])
    else:
        arg = arg.copy()
    hashed = exp.Anonymous(
        this="hashtextextended",
        expressions=[exp.Cast(this=arg, to=exp.DataType.build("TEXT")), exp.Literal.number(0)],
    )
    return exp.Paren(this=exp.BitwiseAnd(this=hashed, expression=exp.Literal.number(_SIGN_MASK)))


def _range_series(node: exp.Anonymous) -> exp.Expr | None:
    """DuckDB ``range`` is exclusive at the end. One or two arguments only."""
    args = node.expressions
    if len(args) == 1:
        start: exp.Expr = exp.Literal.number(0)
        end = args[0]
    elif len(args) == 2:
        start, end = args
    else:
        return None
    return cast(exp.Expr, exp.GenerateSeries(start=start.copy(), end=end.copy(), is_end_exclusive=True))


def _interval_multiply(node: exp.Interval) -> exp.Expr:
    """``INTERVAL <expr> UNIT`` → ``(<expr>) * INTERVAL '1' UNIT``.

    sqlglot's Postgres generator emits ``INTERVAL UNIT`` and drops ``<expr>``
    unless the value is a string literal.
    """
    unit = node.args.get("unit")
    one = exp.Interval(this=exp.Literal.string("1"), unit=unit.copy() if isinstance(unit, exp.Expr) else unit)
    return exp.Paren(this=exp.Mul(this=node.this.copy(), expression=one))


def _is_numeric(node: exp.Expr) -> bool:
    return isinstance(node, exp.Cast) and isinstance(node.to, exp.DataType) and node.to.this in _NUMERIC
