"""Multi-engine core: the engine registry, model→engine routing through apply,
per-engine snapshots/views/GC, and the same-engine-graph guard."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.base import EngineAdapter
from interlace.engines.duckdb import DuckDBAdapter
from interlace.engines.registry import EngineRegistry, as_registry
from interlace.exceptions import ConfigurationError, DefinitionError, PlanError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project
from interlace.state.janitor import gc
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

ENGINES = {"default", "second"}


# --- registry semantics ---------------------------------------------------------


def test_registry_opens_lazily_and_caches() -> None:
    opened: list[str] = []

    def opener(name: str) -> EngineAdapter:
        opened.append(name)
        return DuckDBAdapter.in_memory()

    registry = EngineRegistry(ENGINES, opener)
    assert opened == []  # nothing opened at construction
    first = registry.get("second")
    assert opened == ["second"]
    assert registry.get("second") is first  # cached
    assert registry.get() is registry.get("default")  # None -> default
    registry.close()
    registry.close()  # idempotent


def test_registry_rejects_unknown_names() -> None:
    registry = EngineRegistry({"default"}, lambda _: DuckDBAdapter.in_memory())
    with pytest.raises(ConfigurationError, match="unknown engine"):
        registry.get("ghost")
    with pytest.raises(PlanError, match="not configured"):
        registry.require("ghost", model="orders")
    with pytest.raises(ConfigurationError, match="default_engine"):
        EngineRegistry({"a"}, lambda _: DuckDBAdapter.in_memory(), default="b")


def test_as_registry_back_compat() -> None:
    engine = DuckDBAdapter.in_memory()
    registry = as_registry(engine, None)
    assert registry.get() is engine  # bare engine= becomes the default entry
    with pytest.raises(PlanError, match="engine= or engines="):
        as_registry(None, None)
    engine.close()


# --- routing through apply -------------------------------------------------------


def _model(name: str, sql: str, engine: str | None = None, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, engine=engine, **kwargs)  # type: ignore[arg-type]


def _compile(models: list[ModelDef]):
    return compile_models(models, known_engines=ENGINES, engine_dialects=dict.fromkeys(ENGINES, "duckdb"))


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[EngineRegistry, SqliteStateStore]]:
    adapters = {name: DuckDBAdapter.in_memory() for name in ENGINES}
    registry = EngineRegistry(ENGINES, lambda name: adapters[name])
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield registry, store
    await store.close()
    registry.close()


async def test_models_build_on_their_declared_engines(env: tuple[EngineRegistry, SqliteStateStore]) -> None:
    registry, store = env
    compiled = _compile(
        [
            _model("a", "SELECT 1 AS x"),  # default engine
            _model("b", "SELECT 2 AS y", engine="second"),
        ]
    )
    await apply(await diff(compiled, "dev", store), compiled=compiled, engines=registry, state=store)

    assert await _rows(registry.get("default"), "SELECT x FROM dev__main.a") == [{"x": 1}]
    assert await _rows(registry.get("second"), "SELECT y FROM dev__main.b") == [{"y": 2}]
    # neither engine has the other's objects
    for engine, missing in ((registry.get("default"), "b"), (registry.get("second"), "a")):
        exists = await engine.table_exists(compiled.models[missing].physical_table)
        assert not exists

    engines_recorded = {row["name"]: row["engine"] for row in await store.list_snapshot_rows()}
    assert engines_recorded == {"a": "default", "b": "second"}


async def test_engine_move_refingerprints_and_gc_drops_old_home(
    env: tuple[EngineRegistry, SqliteStateStore],
) -> None:
    registry, store = env
    v1 = _compile([_model("m", "SELECT 1 AS x", engine="second")])
    await apply(await diff(v1, "dev", store), compiled=v1, engines=registry, state=store)
    old_physical = v1.models["m"].physical_table

    v2 = _compile([_model("m", "SELECT 1 AS x")])  # moved to default
    assert v2.models["m"].fingerprint != v1.models["m"].fingerprint  # engine is fingerprinted
    plan = await diff(v2, "dev", store)
    assert plan.changes[0].category is ChangeCategory.BREAKING  # a move is a rebuild
    await apply(plan, compiled=v2, engines=registry, state=store)
    assert await _rows(registry.get("default"), "SELECT x FROM dev__main.m") == [{"x": 1}]

    result = await gc(store, engines=registry, grace=timedelta(0))
    assert result.dropped_tables == [f"second:{old_physical.schema}.{old_physical.name}"]
    assert not await registry.get("second").table_exists(old_physical)  # dropped on the OLD engine


def test_cross_engine_dependency_compiles_and_plans_a_transfer() -> None:
    compiled = _compile(
        [
            _model("up", "SELECT 1 AS x"),
            _model("down", "SELECT x FROM up", engine="second"),
        ]
    )
    assert compiled.models["down"].dependencies == ("up",)  # allowed: the planner inserts a transfer


def test_cross_engine_ephemeral_still_rejected() -> None:
    with pytest.raises(DefinitionError, match="ephemeral"):
        _compile(
            [
                _model("stage", "SELECT 1 AS x", materialise="ephemeral"),
                _model("down", "SELECT x FROM stage", engine="second"),
            ]
        )


def test_unknown_engine_rejected_at_compile() -> None:
    with pytest.raises(DefinitionError, match="ghost"):
        _compile([_model("m", "SELECT 1 AS x", engine="ghost")])


# --- single-engine projects are unchanged ---------------------------------------


def test_default_only_project_synthesises_one_engine(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "interlace.yaml").write_text("name: solo\ndatabase: ':memory:'\n")
    (tmp_path / "models" / "m.sql").write_text("SELECT 1 AS x")

    project = Project.load(tmp_path)
    registry = project.open_engines()
    assert list(registry) == ["default"]
    compiled = project.compile()
    assert compiled.models["m"].engine == "default"
    registry.close()


async def test_project_level_two_engine_apply(tmp_path: Path) -> None:
    project_dir = tmp_path / "proj"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "interlace.yaml").write_text(
        "name: multi\n"
        "database: primary.duckdb\n"
        "engines:\n"
        "  analytics:\n"
        "    type: duckdb\n"
        "    database: analytics.duckdb\n"
    )
    (project_dir / "models" / "core.sql").write_text("SELECT 1 AS x")
    (project_dir / "models" / "report.sql").write_text("/* interlace: {engine: analytics} */\nSELECT 2 AS y")

    project = Project.load(project_dir)
    compiled = project.compile()
    registry = project.open_engines()
    store = await project.open_state()
    try:
        await apply(await diff(compiled, "dev", store), compiled=compiled, engines=registry, state=store)
    finally:
        await store.close()
        registry.close()

    primary = duckdb.connect(str(project_dir / "primary.duckdb"))
    assert primary.execute("SELECT x FROM dev__main.core").fetchall() == [(1,)]
    primary.close()
    analytics = duckdb.connect(str(project_dir / "analytics.duckdb"))
    assert analytics.execute("SELECT y FROM dev__main.report").fetchall() == [(2,)]
    analytics.close()
