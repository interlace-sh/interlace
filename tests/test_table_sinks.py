"""Table sinks (reverse ETL): deliver a model's result into an attached database
with replace / append / merge_by_key / full_merge — never dropping the live table."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import ConfigurationError
from interlace.exports import ExportConfig
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    engine.attach("ext", ":memory:")  # the "external" destination database
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


async def _apply(env: tuple[DuckDBAdapter, SqliteStateStore], sql: str, export: ExportConfig) -> None:
    engine, store = env
    compiled = compile_models([ModelDef(name="push", sql=sql, export=export)])
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)


def _values(rows: str) -> str:
    return f"SELECT * FROM (VALUES {rows}) AS t (id, v)"


def test_export_config_validation() -> None:
    with pytest.raises(ConfigurationError, match="requires 'target'"):
        ExportConfig.from_dict({"to": "table"})
    with pytest.raises(ConfigurationError, match="requires 'key'"):
        ExportConfig.from_dict({"to": "table", "target": "ext.t", "mode": "merge_by_key"})
    with pytest.raises(ConfigurationError, match="unknown export mode"):
        ExportConfig.from_dict({"to": "table", "target": "ext.t", "mode": "truncate"})
    with pytest.raises(ConfigurationError, match="requires 'path'"):
        ExportConfig.from_dict({"to": "parquet"})
    config = ExportConfig.from_dict({"to": "table", "target": "ext.main.t", "mode": "merge_by_key", "key": "id"})
    assert config.key == ("id",)


async def test_replace_empties_in_place(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.dest", mode="replace")
    await _apply(env, _values("(1, 'a'), (2, 'b')"), export)
    await _apply(env, _values("(3, 'c')"), export)  # changed model: delivers the new state

    assert await _rows(engine, "SELECT id, v FROM ext.main.dest ORDER BY id") == [{"id": 3, "v": "c"}]


async def test_append_accumulates(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.log", mode="append")
    await _apply(env, _values("(1, 'a')"), export)
    await _apply(env, _values("(2, 'b')"), export)

    assert [r["id"] for r in await _rows(engine, "SELECT id FROM ext.main.log ORDER BY id")] == [1, 2]


async def test_merge_by_key_upserts(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.accounts", mode="merge_by_key", key=("id",))
    await _apply(env, _values("(1, 'a'), (2, 'b')"), export)
    await _apply(env, _values("(2, 'B'), (3, 'c')"), export)  # 2 updated, 3 new, 1 untouched

    assert await _rows(engine, "SELECT id, v FROM ext.main.accounts ORDER BY id") == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "B"},
        {"id": 3, "v": "c"},
    ]


async def test_full_merge_deletes_vanished_keys(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.state", mode="full_merge", key=("id",))
    await _apply(env, _values("(1, 'a'), (2, 'b')"), export)
    await _apply(env, _values("(1, 'a')"), export)  # 2 vanished from the full state

    assert await _rows(engine, "SELECT id FROM ext.main.state") == [{"id": 1}]


async def test_sink_evolves_added_column(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """The user-hit regression: the sink model grows a column — the external table must
    gain it (never 'has 3 columns but 4 values were supplied')."""
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.scores", mode="replace")
    await _apply(env, "SELECT 1 AS id, 'a' AS v", export)
    await _apply(env, "SELECT 2 AS id, 'b' AS v, 9.5 AS score", export)  # grew a column

    assert await _rows(engine, "SELECT id, v, score FROM ext.main.scores") == [{"id": 2, "v": "b", "score": 9.5}]


async def test_sink_null_fills_vanished_column(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.trimmed", mode="replace")
    await _apply(env, "SELECT 1 AS id, 'a' AS v", export)
    await _apply(env, "SELECT 2 AS id", export)  # v vanished from the model

    assert await _rows(engine, "SELECT id, v FROM ext.main.trimmed") == [{"id": 2, "v": None}]


async def test_sink_reordered_columns_land_by_name(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Positional-insert regression: same columns, different order — values must land in
    the right columns, not silently swap."""
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.swapped", mode="append")
    await _apply(env, "SELECT 1 AS id, 'a' AS v", export)
    await _apply(env, "SELECT 'b' AS v, 2 AS id", export)  # reordered projection

    assert await _rows(engine, "SELECT id, v FROM ext.main.swapped ORDER BY id") == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
    ]


async def test_sink_merge_evolves_added_column(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    export = ExportConfig(to="table", target="ext.main.members", mode="merge_by_key", key=("id",))
    await _apply(env, _values("(1, 'a'), (2, 'b')"), export)
    await _apply(env, "SELECT 2 AS id, 'B' AS v, 'gold' AS tier", export)  # upsert with a new column

    assert await _rows(engine, "SELECT id, v, tier FROM ext.main.members ORDER BY id") == [
        {"id": 1, "v": "a", "tier": None},
        {"id": 2, "v": "B", "tier": "gold"},
    ]


async def test_project_attach_reaches_external_database(tmp_path: Path) -> None:
    """attach: config wires an external db; a sink model delivers into it."""
    import duckdb

    from interlace.project import Project

    project_dir = tmp_path / "proj"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "interlace.yaml").write_text("name: attach_demo\ndatabase: ':memory:'\nattach:\n  crm: crm.duckdb\n")
    (project_dir / "models" / "push.sql").write_text(
        "/* interlace: {export: {to: table, target: crm.main.contacts, mode: merge_by_key, key: id}} */\n"
        "SELECT 1 AS id, 'ada' AS name"
    )

    project = Project.load(project_dir)
    compiled = project.compile()
    engine = project.open_engine()
    store = await project.open_state()
    try:
        await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    finally:
        await store.close()
        engine.close()

    external = duckdb.connect(str(project_dir / "crm.duckdb"))  # relative path resolved against the project
    assert external.execute("SELECT id, name FROM contacts").fetchall() == [(1, "ada")]
    external.close()
