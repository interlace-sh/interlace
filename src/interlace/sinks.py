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

import re
from datetime import UTC, datetime

import sqlglot
from sqlglot import exp

from interlace.exceptions import PlanError
from interlace.ir.relation import TableRef

FILE_FORMATS = frozenset({"parquet", "csv", "json"})
_PATH_TOKEN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
_PATH_TOKEN_VALUES = frozenset({"date", "datetime", "workspace"})


def expand_path_tokens(path: str, *, workspace: str, now: datetime | None = None) -> str:
    """Expand ``${date}``, ``${datetime}``, and ``${workspace}`` in a file path.

    ``date`` is ``YYYY-MM-DD`` and ``datetime`` is ``YYYYMMDDTHHMMSSZ``, both UTC.
    Any other ``${...}`` is an error — this is not a general template language.
    """
    moment = now or datetime.now(UTC)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    moment = moment.astimezone(UTC)
    values = {
        "date": moment.strftime("%Y-%m-%d"),
        "datetime": moment.strftime("%Y%m%dT%H%M%SZ"),
        "workspace": workspace,
    }

    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in _PATH_TOKEN_VALUES:
            raise PlanError(f"unknown path token ${{{key}}}; file paths accept {', '.join(sorted(_PATH_TOKEN_VALUES))}")
        return values[key]

    return _PATH_TOKEN.sub(replace, path)


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


def file_statements(fmt: str, query: exp.Expr, resolved_path: str, dialect: str) -> list[exp.Expr]:
    """Build the ``COPY (...) TO`` that writes ``query`` to a file (overwrite)."""
    if fmt not in FILE_FORMATS:
        raise PlanError(f"unsupported file format: {fmt!r}", details={"format": fmt})
    options = "FORMAT csv, HEADER" if fmt == "csv" else f"FORMAT {fmt}"
    query_sql = query.sql(dialect=dialect)
    escaped = resolved_path.replace("'", "''")
    return [sqlglot.parse_one(f"COPY ({query_sql}) TO '{escaped}' ({options})", read=dialect)]
