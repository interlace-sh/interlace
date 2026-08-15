"""SQL macros: one definition, expanded into the IR before anything else sees it.

A macro is written in SQL, in the engine's own ``CREATE MACRO`` syntax::

    CREATE MACRO cents_to_dollars(amount) AS (amount / 100)::numeric(16, 2);

and called like a function from any model::

    SELECT cents_to_dollars(subtotal) AS subtotal FROM raw_orders

The call is substituted for the macro's body while the model is compiled — before the
fingerprint, before lineage, before transpilation. That ordering is the whole design:

- **The fingerprint covers the macro.** A model's fingerprint is its canonical SQL, and
  the expansion is part of it, so editing a macro rebuilds every model that calls it.
  A macro created in the warehouse instead would be invisible to the fingerprint, and a
  changed one would leave stale tables behind with nothing to notice.
- **Lineage sees through it.** Column lineage reads the AST, and the AST no longer has
  an opaque function call in it.
- **One definition, every engine.** The expansion happens in dialect-agnostic AST, so
  the transpiler renders it per engine. This is the job dbt does with adapter dispatch —
  ``postgres__cents_to_dollars``, ``bigquery__cents_to_dollars`` — and it is not needed
  here: ``(amount / 100)`` becomes integer division on Postgres by itself, so sqlglot
  emits ``CAST(amount AS DOUBLE PRECISION) / NULLIF(100, 0)`` and the macro stays one
  line.

The cost, and it is a real one: the macro exists at build time, not in the warehouse.
Someone querying the tables by hand cannot call it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from interlace.exceptions import DefinitionError

_MAX_DEPTH = 10  # macros may call macros; this is the runaway/recursion stop


@dataclass(frozen=True)
class Macro:
    """A parsed ``CREATE MACRO``: its name, parameter names, and body expression."""

    name: str
    params: tuple[str, ...]
    body: exp.Expression
    source: str  # the file it came from, for error messages


def parse_macros(sql: str, dialect: str, source: str) -> list[Macro]:
    """Parse the ``CREATE MACRO`` statements in one file."""
    try:
        statements = sqlglot.parse(sql, read=dialect)
    except Exception as exc:  # noqa: BLE001 — sqlglot raises a family of parse errors
        raise DefinitionError(f"could not parse macros in {source}: {exc}", details={"source": source}) from exc

    macros: list[Macro] = []
    for statement in statements:
        if statement is None:
            continue
        if isinstance(statement, exp.Command):
            # sqlglot could not parse it — a table macro (AS TABLE SELECT ...) lands here
            raise DefinitionError(
                f"{source}: could not read this as a macro definition: {statement.sql()[:80]!r}. A macro is a "
                f"scalar expression — CREATE MACRO name(args) AS <expression>. A table macro has no call site "
                f"to expand into; write it as a model.",
                details={"source": source},
            )
        udf = statement.this if isinstance(statement, exp.Create) else None
        if not isinstance(udf, exp.UserDefinedFunction):
            raise DefinitionError(
                f"{source}: expected only CREATE MACRO statements, found {type(statement).__name__.upper()}",
                details={"source": source},
            )
        body = statement.expression
        if body is None or isinstance(body, exp.Query):
            raise DefinitionError(
                f"{source}: macro {udf.this.name!r} must be a scalar expression — a table macro "
                f"(AS TABLE SELECT ...) has no call site to expand into; write it as a model",
                details={"source": source, "macro": udf.this.name},
            )
        # the UDF's `this` is a Table node wrapping the identifier, so the name is a
        # level down from where UserDefinedFunction.name looks
        name = udf.this.name if isinstance(udf.this, exp.Expression) else str(udf.this)
        macros.append(
            Macro(
                name=name,
                params=tuple(param.name for param in udf.expressions),
                body=body,
                source=source,
            )
        )
    return macros


def expand_macros(ast: exp.Expression, macros: Mapping[str, Macro], model: str) -> exp.Expression:
    """Substitute every macro call in ``ast`` for its body. Returns a new expression."""
    if not macros:
        return ast
    expanded = ast
    for _ in range(_MAX_DEPTH):
        expanded, hits = _expand_once(expanded, macros, model)
        if not hits:
            return expanded
    raise DefinitionError(
        f"model {model!r}: macro expansion did not settle after {_MAX_DEPTH} passes — a macro calls itself, "
        f"directly or through another",
        details={"model": model},
    )


def _expand_once(ast: exp.Expression, macros: Mapping[str, Macro], model: str) -> tuple[exp.Expression, int]:
    hits = 0

    def substitute(node: exp.Expression) -> exp.Expression:
        nonlocal hits
        if not isinstance(node, exp.Anonymous):
            return node
        macro = macros.get(node.name.casefold())
        if macro is None:
            return node  # an ordinary engine function
        args = list(node.expressions)
        if len(args) != len(macro.params):
            raise DefinitionError(
                f"model {model!r}: macro {macro.name!r} takes {len(macro.params)} argument(s) "
                f"({', '.join(macro.params) or 'none'}), called with {len(args)}",
                details={"model": model, "macro": macro.name, "source": macro.source},
            )
        hits += 1
        return exp.paren(_bind(macro, args))

    return ast.transform(substitute, copy=True), hits


def _bind(macro: Macro, args: list[exp.Expression]) -> exp.Expression:
    """The macro's body with each parameter reference replaced by its argument."""
    body = macro.body.copy()
    if not macro.params:
        return body
    by_name = {param.casefold(): arg for param, arg in zip(macro.params, args, strict=True)}

    def replace(node: exp.Expression) -> exp.Expression:
        if isinstance(node, exp.Column) and not node.table:
            argument = by_name.get(node.name.casefold())
            if argument is not None:
                return argument.copy()
        return node

    bound: exp.Expression = body.transform(replace, copy=False)
    return bound
