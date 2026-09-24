"""Indexes, constraints, and external-table schema policy."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.base import EngineCaps
from interlace.engines.duckdb import DuckDBAdapter
from interlace.engines.registry import as_registry
from interlace.exceptions import DefinitionError, ExecutionError, PlanError
from interlace.graph.project import compile_models
from interlace.physical.annotate import annotate_plan
from interlace.physical.reconcile import LOGICAL_CAPS, model_objects
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


def test_physical_spec_rejected_without_a_table() -> None:
    with pytest.raises(DefinitionError, match="nothing to alter"):
        sql_model("v", "SELECT 1 AS id", materialise="view", indexes=[{"columns": ["id"]}])
    with pytest.raises(DefinitionError, match="nothing to alter"):
        sql_model("e", "SELECT 1 AS id", materialise="ephemeral", constraints=[{"not_null": "id"}])
    with pytest.raises(DefinitionError, match="nothing to alter"):
        sql_model(
            "f",
            "SELECT 1 AS id",
            materialise="file",
            path="out.parquet",
            format="parquet",
            schema_policy={"columns": "reject"},
        )


def test_unknown_constraint_and_schema_policy() -> None:
    with pytest.raises(DefinitionError, match="unknown constraint type"):
        sql_model("m", "SELECT 1 AS id", constraints=[{"nope": "id"}])
    with pytest.raises(DefinitionError, match="schema.columns"):
        sql_model("m", "SELECT 1 AS id", schema_policy={"columns": "drop"})


def test_ignored_physical_policy_creates_nothing() -> None:
    compiled = compile_models(
        [
            sql_model(
                "orders",
                "SELECT 1 AS id",
                indexes=[{"columns": ["id"]}],
                constraints=[{"primary_key": "id"}],
                schema_policy={"indexes": "ignore", "constraints": "ignore"},
            )
        ]
    )
    objects, warnings = model_objects(compiled.models["orders"], LOGICAL_CAPS)
    assert objects == ()
    assert warnings == []
    objects, warnings = model_objects(compiled.models["orders"], EngineCaps())
    assert objects == ()
    assert warnings == []


def test_index_is_not_part_of_the_data_fingerprint() -> None:
    plain = compile_models([sql_model("orders", "SELECT 1 AS id, 'a' AS status")])
    indexed = compile_models([sql_model("orders", "SELECT 1 AS id, 'a' AS status", indexes=[{"columns": ["id"]}])])
    assert plain.models["orders"].fingerprint == indexed.models["orders"].fingerprint
    assert plain.models["orders"].physical_hash == ""
    assert indexed.models["orders"].physical_hash
    assert indexed.models["orders"].indexes[0].object_name("orders") == "il__orders__id"


async def test_index_change_does_not_rebuild(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    first = compile_models([sql_model("orders", "SELECT 1 AS id, 'a' AS status")])
    await apply(await diff(first, "prod", store), compiled=first, engine=engine, state=store)
    snapshot = await store.get_snapshot("orders", first.models["orders"].fingerprint)
    assert snapshot is not None
    table = snapshot.physical_table

    second = compile_models(
        [sql_model("orders", "SELECT 1 AS id, 'a' AS status", indexes=[{"columns": ["id"], "name": "orders_by_id"}])]
    )
    plan = await diff(second, "prod", store)
    assert plan.backfills == []
    assert plan.changes == []
    assert [(c.op, c.kind, c.name) for action in plan.physical for c in action.changes] == [
        ("add", "index", "orders_by_id")
    ]
    assert plan.physical[0].standalone
    assert second.models["orders"].fingerprint == first.models["orders"].fingerprint

    await apply(plan, compiled=second, engine=engine, state=store)
    recorded = await store.get_snapshot("orders", second.models["orders"].fingerprint)
    assert recorded is not None
    assert recorded.physical_table == table
    assert await _index_names(engine, table.name) == ["orders_by_id"]
    assert await _rows(engine, "SELECT id, status FROM main.orders") == [{"id": 1, "status": "a"}]


async def test_only_recorded_indexes_are_dropped(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("orders", "SELECT 1 AS id", indexes=[{"columns": ["id"]}])])
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    snapshot = await store.get_snapshot("orders", project.models["orders"].fingerprint)
    assert snapshot is not None
    qualified = f"{snapshot.physical_table.schema}.{snapshot.physical_table.name}"
    await engine.execute_sql(f"CREATE INDEX keep_me ON {qualified} (id)")

    removed = compile_models([sql_model("orders", "SELECT 1 AS id")])
    plan = await diff(removed, "prod", store)
    await apply(plan, compiled=removed, engine=engine, state=store)

    names = await _index_names(engine, snapshot.physical_table.name)
    assert "keep_me" in names
    assert "il__orders__id" not in names
    assert any("keep_me" in note.message for note in plan.drift)


async def test_unenforced_primary_key_becomes_an_index(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("orders", "SELECT 1 AS id", constraints=[{"primary_key": "id"}])])
    plan = await diff(project, "prod", store)
    await apply(plan, compiled=project, engine=engine, state=store)
    assert any("not enforced" in warning for warning in plan.warnings)
    snapshot = await store.get_snapshot("orders", project.models["orders"].fingerprint)
    assert snapshot is not None
    assert await _index_names(engine, snapshot.physical_table.name) == ["il__orders__pk"]


async def test_not_null_blocks_promotion(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models(
        [
            sql_model(
                "orders",
                "SELECT 1 AS id, CAST(NULL AS VARCHAR) AS status",
                constraints=[{"not_null": "status"}],
            )
        ]
    )
    with pytest.raises(ExecutionError, match="NOT NULL"):
        await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    tables = await _rows(engine, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
    assert tables == []


async def test_external_index_reconcile_leaves_foreign_indexes(tmp_path: Path) -> None:
    engine = DuckDBAdapter.in_memory()
    engine.attach("ext", ":memory:")
    store = await SqliteStateStore.open(tmp_path / "state.db")
    try:
        project = compile_models(
            [
                sql_model(
                    "push",
                    "SELECT 1 AS id, 'a' AS v",
                    materialise="table",
                    target="ext.main.dest",
                    indexes=[{"columns": ["id"]}],
                )
            ]
        )
        await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
        await engine.execute_sql("CREATE INDEX keep_me ON ext.main.dest (v)")
        assert set(await _index_names(engine, "dest")) == {"il__push__id", "keep_me"}

        removed = compile_models(
            [sql_model("push", "SELECT 1 AS id, 'a' AS v", materialise="table", target="ext.main.dest")]
        )
        plan = await diff(removed, "prod", store)
        await apply(plan, compiled=removed, engine=engine, state=store)
        assert await _index_names(engine, "dest") == ["keep_me"]
        assert any("keep_me" in note.message and not note.blocking for note in plan.drift)
    finally:
        await store.close()
        engine.close()


async def test_external_columns_reject_does_not_write(tmp_path: Path) -> None:
    engine = DuckDBAdapter.in_memory()
    engine.attach("ext", ":memory:")
    store = await SqliteStateStore.open(tmp_path / "state.db")
    try:
        first = compile_models(
            [sql_model("push", "SELECT 1 AS id, 'a' AS v", materialise="table", target="ext.main.dest")]
        )
        await apply(await diff(first, "prod", store), compiled=first, engine=engine, state=store)

        grown = compile_models(
            [
                sql_model(
                    "push",
                    "SELECT 1 AS id, 'a' AS v, 2 AS extra",
                    materialise="table",
                    target="ext.main.dest",
                    schema_policy={"columns": "reject"},
                )
            ]
        )
        with pytest.raises(PlanError, match="reject"):
            await apply(await diff(grown, "prod", store), compiled=grown, engine=engine, state=store)
        columns = await _rows(
            engine,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'dest' ORDER BY column_name",
        )
        assert [row["column_name"] for row in columns] == ["id", "v"]
        assert await _rows(engine, "SELECT id, v FROM ext.main.dest") == [{"id": 1, "v": "a"}]
    finally:
        await store.close()
        engine.close()


async def test_external_columns_ignore_does_not_alter(tmp_path: Path) -> None:
    engine = DuckDBAdapter.in_memory()
    engine.attach("ext", ":memory:")
    store = await SqliteStateStore.open(tmp_path / "state.db")
    try:
        first = compile_models(
            [sql_model("push", "SELECT 1 AS id, 'a' AS v", materialise="table", target="ext.main.dest")]
        )
        await apply(await diff(first, "prod", store), compiled=first, engine=engine, state=store)
        ignored = compile_models(
            [
                sql_model(
                    "push",
                    "SELECT 1 AS id, 'b' AS v, 2 AS extra",
                    materialise="table",
                    target="ext.main.dest",
                    schema_policy={"columns": "ignore"},
                )
            ]
        )
        with pytest.raises(ExecutionError):
            await apply(await diff(ignored, "prod", store), compiled=ignored, engine=engine, state=store)
        assert await _rows(engine, "SELECT id, v FROM ext.main.dest") == [{"id": 1, "v": "a"}]
        columns = await _rows(
            engine,
            "SELECT column_name FROM information_schema.columns WHERE table_name = 'dest' ORDER BY column_name",
        )
        assert [row["column_name"] for row in columns] == ["id", "v"]
    finally:
        await store.close()
        engine.close()


async def test_contract_reject_is_reported_before_apply(tmp_path: Path) -> None:
    engine = DuckDBAdapter.in_memory()
    engine.attach("ext", ":memory:")
    store = await SqliteStateStore.open(tmp_path / "state.db")
    try:
        first = compile_models([sql_model("push", "SELECT 1 AS id", materialise="table", target="ext.main.dest")])
        await apply(await diff(first, "prod", store), compiled=first, engine=engine, state=store)
        contracted = compile_models(
            [
                sql_model(
                    "push",
                    "SELECT 1 AS id",
                    materialise="table",
                    target="ext.main.dest",
                    columns={"id": "INTEGER", "missing": "INTEGER"},
                    schema_policy={"columns": "reject"},
                )
            ]
        )
        plan = await diff(contracted, "prod", store)
        await annotate_plan(plan, contracted, as_registry(engine, None))
        assert plan.blocking
        assert any("missing" in message for message in plan.blocking)
        with pytest.raises(PlanError, match="schema drift blocks apply"):
            await apply(plan, compiled=contracted, engine=engine, state=store)
    finally:
        await store.close()
        engine.close()


async def _index_names(engine: DuckDBAdapter, table: str) -> list[str]:
    rows = await _rows(
        engine, f"SELECT index_name FROM duckdb_indexes() WHERE table_name = '{table}' ORDER BY index_name"
    )
    return [row["index_name"] for row in rows]
