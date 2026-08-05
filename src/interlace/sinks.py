"""Terminal-materialisation helpers: parse an external table target and build a
file COPY.

A ``materialise: table`` model delivers into an external, interlace-does-not-own
table named ``<alias>.<schema>.<table>`` (``alias`` is a database attached via the
project's ``attach:`` config); a ``materialise: file`` model writes its result to a
``path`` in ``csv`` / ``parquet`` / ``json``. Both are *terminal* — no managed
snapshot table, no environment view, environment-gated side effects (see
architecture.md §6). The delivery statements themselves live in the strategy layer
(``ReplaceInPlace`` / ``Append`` / the reused keyed builders); this module only
holds the target parse and the file COPY.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp

from interlace.exceptions import PlanError
from interlace.ir.relation import TableRef

FILE_FORMATS = frozenset({"parquet", "csv", "json"})


def target_ref(target: str) -> TableRef:
    """Parse a ``materialise: table`` target into a :class:`TableRef`.

    ``<alias>.<schema>.<table>``, or ``<alias>.<table>`` for the attached
    database's ``main`` schema.
    """
    parts = target.split(".")
    if len(parts) == 3:
        return TableRef(catalog=parts[0], schema=parts[1], name=parts[2])
    if len(parts) == 2:
        return TableRef(catalog=parts[0], schema="main", name=parts[1])
    raise PlanError(
        f"materialise: table target {target!r} must be <alias>.<schema>.<table> "
        f"(or <alias>.<table> for the main schema)"
    )


def file_statements(fmt: str, query: exp.Expression, resolved_path: str, dialect: str) -> list[exp.Expression]:
    """Build the ``COPY (...) TO`` that writes ``query`` to a file (overwrite)."""
    if fmt not in FILE_FORMATS:
        raise PlanError(f"unsupported file format: {fmt!r}", details={"format": fmt})
    options = "FORMAT csv, HEADER" if fmt == "csv" else f"FORMAT {fmt}"
    query_sql = query.sql(dialect=dialect)
    escaped = resolved_path.replace("'", "''")
    return [sqlglot.parse_one(f"COPY ({query_sql}) TO '{escaped}' ({options})", read=dialect)]
