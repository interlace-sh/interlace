"""Models registered while a Python model runs are compiled and built in that apply."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import fetch_rows

from interlace.dsl.decorators import REGISTRY, ModelDef
from interlace.dsl.dynamic import capturing_registrations, registering, write_dynamic_models
from interlace.exceptions import DefinitionError
from interlace.plan.apply import apply
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
        first = await apply(
            await run_plan(compiled, "prod", store),
            compiled=compiled,
            engine=engine,
            state=store,
            base_path=project.root,
            loaded=project,
        )
        assert "orders_acme" in first.built
        assert await fetch_rows(engine, "SELECT amount, tenant FROM main.orders_acme") == [
            {"amount": 1, "tenant": "acme"}
        ]

        fn = compiled.models["sync_tenants"].fn
        assert fn is not None
        fn.__globals__["WAVE"] = 2
        second = await apply(
            await run_plan(compiled, "prod", store),
            compiled=compiled,
            engine=engine,
            state=store,
            base_path=project.root,
            loaded=project,
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
