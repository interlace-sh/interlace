"""Typed project vars, substituted into model SQL before anything else reads it.

A SQL model reads a value declared under ``vars:`` with ``var('name')``::

    SELECT * FROM orders WHERE region = var('region')

The call is a real function node in the AST. Compile replaces it with a typed
literal — a string, number, boolean, or a date/timestamp cast — before the
fingerprint, lineage, and transpile. Editing the value therefore changes the
canonical SQL of every model that names it, and an unknown name fails at
compile instead of reaching the warehouse.

``var(column)`` is left alone: only a single string-literal argument is a
lookup. ``@name`` is not the syntax, because DuckDB already uses ``@`` for
absolute value.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from sqlglot import exp

from interlace.exceptions import DefinitionError


class TypedVar(Protocol):
    """The ``type`` / ``value`` pair ``VarConfig`` already provides.

    Read-only properties so a ``dict[str, VarConfig]`` is a ``Mapping`` of this
    protocol: a plain attribute on a Protocol is invariant, and ``VarConfig.type``
    is a literal, not ``str``.
    """

    @property
    def type(self) -> str: ...

    @property
    def value(self) -> object: ...


def expand_vars(ast: exp.Expr, variables: Mapping[str, TypedVar], model: str) -> exp.Expr:
    """Replace every ``var('name')`` in ``ast`` with its typed literal."""

    def substitute(node: exp.Expr) -> exp.Expr:
        if not isinstance(node, exp.Anonymous) or node.name.casefold() != "var":
            return node
        name = _lookup_name(node)
        if name is None:
            return node
        spec = variables.get(name)
        if spec is None:
            raise DefinitionError(
                f"model {model!r} references unknown var {name!r}",
                details={"model": model, "var": name, "vars": sorted(variables)},
            )
        return _literal(spec, model, name)

    return ast.transform(substitute)


def _lookup_name(node: exp.Anonymous) -> str | None:
    args = node.expressions
    if len(args) != 1 or not isinstance(args[0], exp.Literal) or not args[0].is_string:
        return None
    return str(args[0].this)


def _literal(spec: TypedVar, model: str, name: str) -> exp.Expr:
    value = spec.value
    kind = spec.type
    if kind == "string" and isinstance(value, str):
        return exp.Literal.string(value)
    if kind == "int" and type(value) is int:
        return exp.Literal.number(value)
    if kind == "float" and type(value) in (int, float):
        return exp.Literal.number(value)
    if kind == "bool" and type(value) is bool:
        return exp.Boolean(this=value)
    if kind == "date" and isinstance(value, str):
        return exp.Cast(this=exp.Literal.string(value), to=exp.DataType.build("DATE"))
    if kind == "timestamp" and isinstance(value, str):
        return exp.Cast(this=exp.Literal.string(value), to=exp.DataType.build("TIMESTAMP"))
    raise DefinitionError(
        f"model {model!r} var {name!r} has unknown type {kind!r}",
        details={"model": model, "var": name, "type": kind},
    )
