"""Sinks / exports — pushing a model's result to an external destination.

A model with an ``export`` block is a *sink*: it produces no managed table and no
environment view; instead its (resolved) query result is written to a
destination. Two families:

- **files** — ``to: parquet|csv|json`` + ``path``, via DuckDB ``COPY``.
- **tables** (reverse ETL) — ``to: table`` + ``target: <alias>.<schema>.<table>``
  where ``alias`` is a database attached via the project's ``attach:`` config
  (Postgres, SQLite, another DuckDB, ...). ``mode`` picks the delivery:
  ``replace`` (DELETE all + INSERT — the live table is never dropped, so grants
  and readers survive), ``append``, or the keyed ``merge_by_key`` /
  ``full_merge`` — which reuse the *same strategy AST builders* as managed
  models, pointed at the external catalog.

Exports are side-effecting — no view-swap rollback (see architecture.md §6).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import sqlglot
from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.exceptions import ConfigurationError, PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies.base import RowCounts

_FILE_FORMATS = frozenset({"parquet", "csv", "json"})
_TABLE_MODES = frozenset({"replace", "append", "merge_by_key", "full_merge"})


@dataclass(frozen=True)
class ExportConfig:
    """Where a sink writes. ``to`` is the destination type; ``path`` (files) or
    ``target`` + ``mode`` (+ ``key`` for keyed modes) for tables.

    ``environments`` gates the side effect: the export only *executes* when the
    plan's environment is listed. Default is production only — a dev apply must
    never fire reverse-ETL at a live external table (architecture.md §6 calls this the
    property that matters most). In a gated-off environment the sink's snapshot
    is still recorded so the plan settles; nothing leaves the warehouse.
    """

    to: str
    path: str = ""
    target: str = ""
    mode: str = "replace"
    key: tuple[str, ...] = ()
    environments: tuple[str, ...] = ("prod",)  # PRODUCTION_ENV; literal to avoid a plan-layer import

    @classmethod
    def from_dict(cls, data: Any) -> ExportConfig:
        if not isinstance(data, dict) or "to" not in data:
            raise ConfigurationError("export requires 'to'", details={"got": data})
        to = str(data["to"])

        def _names(field: str, default: tuple[str, ...]) -> tuple[str, ...]:
            value = data.get(field, default)
            if isinstance(value, str):
                return (value,)
            if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value):
                return tuple(value)
            raise ConfigurationError(f"export {field!r} must be a string or a list of strings", details={"got": value})

        config = cls(
            to=to,
            path=str(data.get("path", "")),
            target=str(data.get("target", "")),
            mode=str(data.get("mode", "replace")),
            key=_names("key", ()) if data.get("key") else (),
            environments=_names("environments", ("prod",)),
        )
        if to in _FILE_FORMATS and not config.path:
            raise ConfigurationError(f"export to {to!r} requires 'path'", details={"got": data})
        if to == "table":
            if not config.target:
                raise ConfigurationError("export to table requires 'target'", details={"got": data})
            if config.mode not in _TABLE_MODES:
                raise ConfigurationError(f"unknown export mode {config.mode!r}; expected one of {sorted(_TABLE_MODES)}")
            if config.mode in ("merge_by_key", "full_merge") and not config.key:
                raise ConfigurationError(f"export mode {config.mode!r} requires 'key'", details={"got": data})
        return config


def export_target_ref(target: str) -> TableRef:
    parts = target.split(".")
    if len(parts) == 3:
        return TableRef(catalog=parts[0], schema=parts[1], name=parts[2])
    if len(parts) == 2:
        return TableRef(catalog=parts[0], schema="main", name=parts[1])
    raise PlanError(
        f"export target {target!r} must be <alias>.<schema>.<table> (or <alias>.<table> for the main schema)"
    )


def table_export_statements(
    export: ExportConfig,
    query: exp.Expression,
    columns: Sequence[str] | None = None,
) -> list[exp.Expression]:
    """Deliver ``query`` into the external table — never DROP it (grants/readers survive).

    ``columns`` names the target's column order when the source has been aligned to an
    existing target (see ``plan.apply._deliver_table_export``): replace/append inserts
    then carry an explicit column list. The keyed modes reuse the strategy builders,
    whose inserts bind positionally — safe because the aligned projection reproduces
    the target's column order exactly."""
    from interlace.strategies import FullMerge, MergeByKey  # runtime import: strategies build on ir like this module

    target = export_target_ref(export.target)
    table = target.to_expr()
    relation = SqlRelation(ast=query)

    if export.mode == "merge_by_key":
        return MergeByKey(export.key).plan_statements(relation, target, EngineCaps())
    if export.mode == "full_merge":
        return FullMerge(export.key).plan_statements(relation, target, EngineCaps())

    derived = exp.select("*").from_(exp.Subquery(this=query.copy(), alias=exp.TableAlias(this="_s")))
    ensure = exp.Create(this=table.copy(), kind="TABLE", exists=True, expression=derived.copy().limit(0))
    insert_this: exp.Expression = table.copy()
    if columns:
        insert_this = exp.Schema(this=table.copy(), expressions=[exp.to_identifier(c) for c in columns])
    insert = exp.Insert(this=insert_this, expression=query.copy())
    if export.mode == "append":
        return [ensure, insert]
    wipe = exp.Delete(this=table.copy())  # replace: empty in place, never drop
    return [ensure, wipe, insert]


def export_row_counts(export: ExportConfig, counts: Sequence[int]) -> RowCounts:
    """Interpret a table delivery's per-statement counts for its mode."""
    from interlace.strategies import FullMerge, MergeByKey

    if export.mode == "merge_by_key":
        return MergeByKey(export.key).row_counts(counts)
    if export.mode == "full_merge":
        return FullMerge(export.key).row_counts(counts)
    if export.mode == "append":  # [ensure, insert]
        return RowCounts(inserted=counts[1] if len(counts) > 1 else 0)
    # replace: [ensure, wipe, insert] — the wipe clears the previous delivery
    return RowCounts(inserted=counts[2] if len(counts) > 2 else 0, deleted=counts[1] if len(counts) > 1 else 0)


def export_statements(
    export: ExportConfig, query: exp.Expression, resolved_path: str, dialect: str
) -> list[exp.Expression]:
    """Build the statements that write ``query`` to a file export destination."""
    if export.to not in _FILE_FORMATS:
        raise PlanError(f"unsupported export destination: {export.to!r}", details={"to": export.to})
    options = "FORMAT csv, HEADER" if export.to == "csv" else f"FORMAT {export.to}"
    query_sql = query.sql(dialect=dialect)
    escaped = resolved_path.replace("'", "''")
    return [sqlglot.parse_one(f"COPY ({query_sql}) TO '{escaped}' ({options})", read=dialect)]
