"""Local file scans on an engine that cannot read them.

``read_csv_auto`` and the other DuckDB file readers are how a seed is written.
On DuckDB they run in place. On Postgres (and the other remote engines) the
scan runs in a short-lived DuckDB, against the project root, and the batches
are loaded into ``interlace__xfer`` for the model query to read.
"""

from __future__ import annotations

import contextlib
from pathlib import Path

from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.ir.layout import XFER_SCHEMA
from interlace.ir.relation import TableRef, drop

_SCANS = frozenset({"read_csv", "read_csv_auto", "read_parquet", "read_json", "read_json_auto"})


def _scan_name(node: exp.Expr) -> str | None:
    if isinstance(node, exp.Anonymous):
        return str(node.this).casefold()
    if isinstance(node, exp.Func):
        return node.sql_name().casefold()
    return None


def _is_file_scan(table: exp.Table) -> bool:
    return _scan_name(table.this) in _SCANS


def _stage_name(model_name: str, index: int) -> str:
    stem = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in model_name.casefold()) or "model"
    return f"{stem}__file_{index}"


async def stage_file_scans(
    engine: EngineAdapter, query: exp.Query, root: Path, model_name: str
) -> tuple[exp.Query, list[TableRef]]:
    """Replace file-scan table functions with tables loaded on ``engine``.

    DuckDB-family engines (dialect ``duckdb``) already resolve the paths, so
    the query is returned unchanged. The caller drops the staged tables once
    the model has read them.
    """
    if engine.dialect == "duckdb" or not any(_is_file_scan(table) for table in query.find_all(exp.Table)):
        return query, []

    from interlace.engines.duckdb import DuckDBAdapter

    staged_query = query.copy()
    tables = [table for table in staged_query.find_all(exp.Table) if _is_file_scan(table)]
    duck = DuckDBAdapter.connect(":memory:")
    duck.search_files_from(str(root))
    stages: list[TableRef] = []
    try:
        await engine.create_schema(XFER_SCHEMA)
        for index, table in enumerate(tables):
            reader = await duck.fetch_sql(f"SELECT * FROM {table.this.sql(dialect='duckdb')}")
            stage = TableRef(schema=XFER_SCHEMA, name=_stage_name(model_name, index))
            await engine.load(stage, reader, "create")
            replacement = stage.to_expr()
            if table.args.get("alias") is not None:
                replacement.set("alias", table.args["alias"].copy())
            table.replace(replacement)
            stages.append(stage)
    finally:
        duck.close()
    return staged_query, stages


async def drop_file_stages(engine: EngineAdapter, stages: list[TableRef]) -> None:
    """Drop tables :func:`stage_file_scans` loaded. A failure here is not the build."""
    for stage in stages:
        with contextlib.suppress(Exception):
            await engine.execute(drop(stage.to_expr(), kind="TABLE"))
