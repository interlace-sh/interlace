"""Models registered while a Python model runs are compiled and built in that apply."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import fetch_rows

from interlace.checks.spec import CheckSpec
from interlace.dsl.decorators import REGISTRY, ModelDef
from interlace.dsl.dynamic import apply_with_registrations, capturing_registrations, registering, write_dynamic_models
from interlace.exceptions import DefinitionError
from interlace.physical.spec import ConstraintSpec, IndexSpec, SchemaPolicy
from interlace.plan.run import run_plan
from interlace.project import Project

pytestmark = pytest.mark.unit


def test_run_cannot_replace_a_file_model() -> None:
    REGISTRY.clear()
    try:
        REGISTRY.register_model(ModelDef(name="orders", sql="SELECT 1 AS id"))
        with registering("sync"), pytest.raises(DefinitionError, match="already defined"):
            REGISTRY.register_model(ModelDef(name="orders", sql="SELECT 2 AS id"))
    finally:
        REGISTRY.clear()


def test_run_replaces_a_model_it_registered() -> None:
    REGISTRY.clear()
    try:
        with capturing_registrations() as batch, registering("sync"):
            REGISTRY.register_model(ModelDef(name="orders", sql="SELECT 1 AS amount"))
            REGISTRY.register_model(ModelDef(name="orders", sql="SELECT 2 AS amount"))
        assert REGISTRY.models["orders"].sql == "SELECT 2 AS amount"
        assert batch == ["orders"]
        assert "orders" in REGISTRY.dynamic
    finally:
        REGISTRY.clear()


def test_dynamic_sql_reloads_with_its_strategy(tmp_path: Path) -> None:
    REGISTRY.clear()
    try:
        with registering("sync"):
            REGISTRY.register_model(ModelDef(name="orders", sql="SELECT 1 AS id", strategy="merge", key=("id",)))
        write_dynamic_models(tmp_path, ["orders"])
        (tmp_path / "interlace.yaml").write_text("name: dyn\n")
        loaded = Project.load(tmp_path)
        model = next(item for item in loaded.models if item.name == "orders")
        assert model.sql is not None and "SELECT 1 AS id" in model.sql
        assert model.strategy == "merge"
        assert model.key == ("id",)
        assert "orders" in REGISTRY.dynamic
    finally:
        REGISTRY.clear()


def test_python_function_is_not_written(tmp_path: Path) -> None:
    REGISTRY.clear()
    try:
        REGISTRY.register_model(ModelDef(name="computed", fn=lambda: None))
        write_dynamic_models(tmp_path, ["computed"])
        assert not (tmp_path / ".interlace" / "dynamic").exists()
    finally:
        REGISTRY.clear()


async def test_periodic_run_builds_models_registered_during_the_run(tmp_path: Path) -> None:
    models = tmp_path / "models"
    models.mkdir()
    (tmp_path / "interlace.yaml").write_text("name: dyn\ndatabase: ':memory:'\n")
    (models / "sync.py").write_text(
        "import pyarrow as pa\n"
        "from interlace import model\n"
        "from interlace.dsl.decorators import REGISTRY, ModelDef\n"
        "\n"
        "WAVE = 1\n"
        "\n"
        "@model(name='sync_tenants')\n"
        "def sync_tenants():\n"
        "    tenants = [('acme', 1)]\n"
        "    if WAVE == 2:\n"
        "        tenants.append(('globex', 2))\n"
        "    for tenant, amount in tenants:\n"
        "        REGISTRY.register_model(ModelDef(\n"
        "            name=f'orders_{tenant}',\n"
        "            sql=f\"SELECT {amount} AS amount, '{tenant}' AS tenant\",\n"
        "        ))\n"
        "    return pa.table({'n': [len(tenants)]})\n"
    )
    project = Project.load(tmp_path)
    compiled = project.compile()
    engine = project.open_engine()
    store = await project.open_state()
    try:
        first = await apply_with_registrations(
            await run_plan(compiled, "prod", store),
            compiled=compiled,
            engine=engine,
            state=store,
            base_path=project.root,
            project=project,
        )
        assert "orders_acme" in first.built
        assert await fetch_rows(engine, "SELECT amount, tenant FROM main.orders_acme") == [
            {"amount": 1, "tenant": "acme"}
        ]

        fn = compiled.models["sync_tenants"].fn
        assert fn is not None
        fn.__globals__["WAVE"] = 2
        second = await apply_with_registrations(
            await run_plan(compiled, "prod", store),
            compiled=compiled,
            engine=engine,
            state=store,
            base_path=project.root,
            project=project,
        )
        assert "orders_globex" in second.built
        assert await fetch_rows(engine, "SELECT amount, tenant FROM main.orders_acme") == [
            {"amount": 1, "tenant": "acme"}
        ]
        assert await fetch_rows(engine, "SELECT amount, tenant FROM main.orders_globex") == [
            {"amount": 2, "tenant": "globex"}
        ]
    finally:
        await store.close()
        engine.close()

    try:
        reloaded = Project.load(tmp_path)
        assert {"sync_tenants", "orders_acme", "orders_globex"} <= {item.name for item in reloaded.models}
    finally:
        REGISTRY.clear()


def test_compile_keeps_registrations_until_the_next_load(tmp_path: Path) -> None:
    models = tmp_path / "models"
    models.mkdir()
    (tmp_path / "interlace.yaml").write_text("name: dyn\n")
    (models / "keep.sql").write_text("SELECT 1 AS id\n")
    project = Project.load(tmp_path)
    try:
        REGISTRY.register_model(ModelDef(name="orders", sql="SELECT 2 AS id"))
        assert "orders" in project.compile().models
        other = tmp_path / "other"
        (other / "models").mkdir(parents=True)
        (other / "interlace.yaml").write_text("name: other\n")
        (other / "models" / "only.sql").write_text("SELECT 3 AS id\n")
        Project.load(other)
        compiled = project.compile()
        assert "orders" not in compiled.models
        assert "keep" in compiled.models
    finally:
        REGISTRY.clear()


def test_sql_config_round_trips(tmp_path: Path) -> None:
    definition = ModelDef(
        name="orders",
        sql="SELECT 1 AS id",
        strategy="merge",
        key=("id",),
        dialect="duckdb",
        engine="warehouse",
        depends_on=("raw",),
        interval="1d",
        time_column="ts",
        backfill="none",
        tags=("finance",),
        owner="data",
        description="orders",
        columns={"id": "int"},
        environments=("prod", "dev"),
        schedule={"every": "5m"},
        checks=(CheckSpec(type="not_null", columns=("id",), severity="warn"),),
        indexes=(IndexSpec(columns=("id",), unique=True, name="orders_id"),),
        constraints=(ConstraintSpec(type="check", columns=(), name="positive", expression="id > 0"),),
        schema_policy=SchemaPolicy(columns="reject", indexes="ignore", constraints="ignore"),
    )
    REGISTRY.clear()
    try:
        with registering("sync"):
            REGISTRY.register_model(definition)
        write_dynamic_models(tmp_path, ["orders"])
        (tmp_path / "interlace.yaml").write_text("name: dyn\n")
        loaded = Project.load(tmp_path)
        model = next(item for item in loaded.models if item.name == "orders")
        assert model.strategy == definition.strategy
        assert model.key == definition.key
        assert model.dialect == definition.dialect
        assert model.engine == definition.engine
        assert model.depends_on == definition.depends_on
        assert model.interval == definition.interval
        assert model.time_column == definition.time_column
        assert model.backfill == definition.backfill
        assert model.tags == definition.tags
        assert model.owner == definition.owner
        assert model.description == definition.description
        assert model.columns == definition.columns
        assert model.environments == definition.environments
        assert model.schedule == definition.schedule
        assert model.checks == definition.checks
        assert model.indexes == definition.indexes
        assert model.constraints == definition.constraints
        assert model.schema_policy == definition.schema_policy
    finally:
        REGISTRY.clear()


async def test_failed_registered_model_is_not_persisted(tmp_path: Path) -> None:
    models = tmp_path / "models"
    models.mkdir()
    (tmp_path / "interlace.yaml").write_text("name: dyn\ndatabase: ':memory:'\n")
    (models / "sync.py").write_text(
        "import pyarrow as pa\n"
        "from interlace import model\n"
        "from interlace.dsl.decorators import REGISTRY, ModelDef\n"
        "\n"
        "@model(name='sync_tenants')\n"
        "def sync_tenants():\n"
        "    REGISTRY.register_model(ModelDef(name='broken', sql='SELECT * FROM no_such_table'))\n"
        "    return pa.table({'n': [1]})\n"
    )
    project = Project.load(tmp_path)
    compiled = project.compile()
    engine = project.open_engine()
    store = await project.open_state()
    try:
        with pytest.raises(Exception, match="broken"):
            await apply_with_registrations(
                await run_plan(compiled, "prod", store),
                compiled=compiled,
                engine=engine,
                state=store,
                base_path=project.root,
                project=project,
            )
        assert not (tmp_path / ".interlace" / "dynamic" / "broken.sql").exists()
    finally:
        await store.close()
        engine.close()
        REGISTRY.clear()
