"""Postgres engine adapter: caps-driven fallbacks (unit) and native apply (live).

Live tests need a reachable Postgres — INTERLACE_TEST_PG_DSN, or the local
disposable container (docker run --name interlace-pg -e POSTGRES_PASSWORD=pg
-p 5455:5432 postgres:16). They skip cleanly when nothing answers.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.base import EngineCaps
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import ConfigurationError, PlanError
from interlace.graph.project import compile_models
from interlace.ir.relation import EngineRef, SqlRelation, TableRef
from interlace.ir.schema import empty_schema
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.store import SqliteStateStore
from interlace.strategies import FullRefresh, ScdType2

DSN = os.environ.get("INTERLACE_TEST_PG_DSN", "postgresql://postgres:pg@localhost:5455/postgres")


def _pg_available() -> bool:
    try:
        import adbc_driver_postgresql.dbapi as dbapi

        with dbapi.connect(DSN) as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False


requires_pg = pytest.mark.skipif(not _pg_available(), reason="no reachable Postgres (INTERLACE_TEST_PG_DSN)")


# --- unit: caps honesty, no server needed ---------------------------------------


def _relation(sql: str, dialect: str = "postgres") -> SqlRelation:
    return SqlRelation(ast=sqlglot.parse_one(sql), engine=EngineRef(name="pg", dialect=dialect), schema=empty_schema())


@pytest.mark.unit
def test_full_refresh_falls_back_without_create_or_replace() -> None:
    target = TableRef(schema="interlace__main", name="t__abc")
    statements = FullRefresh().plan_statements(_relation("SELECT 1 AS x"), target, EngineCaps())
    sqls = [s.sql(dialect="postgres") for s in statements]
    assert sqls[0].startswith("DROP TABLE IF EXISTS")
    assert sqls[1].startswith("CREATE TABLE") and "OR REPLACE" not in sqls[1]


@pytest.mark.unit
def test_scd2_refuses_engines_without_star_exclude() -> None:
    target = TableRef(schema="interlace__main", name="dim__abc")
    with pytest.raises(PlanError, match="star-EXCLUDE"):
        ScdType2(("id",)).plan_statements(_relation("SELECT 1 AS id"), target, EngineCaps())


@pytest.mark.unit
def test_postgres_engine_config_requires_dsn(tmp_path: Path) -> None:
    from interlace.project import Project

    (tmp_path / "models").mkdir()
    (tmp_path / "interlace.yaml").write_text("name: p\ndatabase: ':memory:'\nengines:\n  pg:\n    type: postgres\n")
    project = Project.load(tmp_path)
    with pytest.raises(ConfigurationError, match="needs a DSN"):
        project.open_engine("pg")


# --- live: native apply inside Postgres ------------------------------------------


@pytest.fixture()
async def pg_env(tmp_path: Path) -> AsyncIterator[tuple[EngineRegistry, SqliteStateStore, str]]:
    from interlace.engines.duckdb import DuckDBAdapter
    from interlace.engines.postgres import PostgresAdapter

    marker = uuid.uuid4().hex[:8]
    adapters = {"default": DuckDBAdapter.in_memory(), "pg": PostgresAdapter.connect(DSN)}
    registry = EngineRegistry({"default", "pg"}, lambda name: adapters[name])
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield registry, store, marker
    # drop everything this test created (schemas are shared in the container)
    pg = adapters["pg"]
    for schema in ("interlace__main", f"dev_{marker}__main"):
        await pg.execute_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await store.close()
    registry.close()


def _compile(models: list[ModelDef]):
    return compile_models(
        models,
        known_engines={"default", "pg"},
        engine_dialects={"default": "duckdb", "pg": "postgres"},
    )


@requires_pg
@pytest.mark.requires_db
async def test_full_and_merge_apply_natively_in_postgres(
    pg_env: tuple[EngineRegistry, SqliteStateStore, str],
) -> None:
    registry, store, marker = pg_env
    env_name = f"dev_{marker}"
    models = [
        ModelDef(name="seed", sql="SELECT 1 AS id, 'ada' AS name", engine="pg"),
        ModelDef(
            name="people",
            sql="SELECT id, name FROM seed",
            engine="pg",
            strategy="merge_by_key",
            key=("id",),
            checks=(),
        ),
    ]
    compiled = _compile(models)
    await apply(await diff(compiled, env_name, store), compiled=compiled, engines=registry, state=store)

    pg = registry.get("pg")
    reader = await pg.fetch_sql(f'SELECT id, name FROM "{env_name}__main".people')
    assert reader.read_all().to_pylist() == [{"id": 1, "name": "ada"}]

    rows = {r["name"]: r["engine"] for r in await store.list_snapshot_rows()}
    assert rows == {"seed": "pg", "people": "pg"}


@requires_pg
@pytest.mark.requires_db
async def test_checks_gate_promotion_on_postgres(pg_env: tuple[EngineRegistry, SqliteStateStore, str]) -> None:
    from interlace.checks.spec import parse_checks
    from interlace.exceptions import CheckError

    registry, store, marker = pg_env
    bad = ModelDef(
        name="seed",
        sql="SELECT * FROM (VALUES (1), (1)) AS t (id)",  # duplicate key
        engine="pg",
        checks=parse_checks([{"unique": "id"}], "seed"),
    )
    compiled = _compile([bad])
    with pytest.raises(CheckError, match="unique_id"):
        await apply(await diff(compiled, f"dev_{marker}", store), compiled=compiled, engines=registry, state=store)
    assert await store.get_environment(f"dev_{marker}") == {}  # nothing promoted


@requires_pg
@pytest.mark.requires_db
async def test_python_model_loads_arrow_into_postgres(
    pg_env: tuple[EngineRegistry, SqliteStateStore, str],
) -> None:
    import pyarrow as pa

    registry, store, marker = pg_env

    def generated() -> pa.Table:
        return pa.table({"n": [1, 2, 3]})

    compiled = _compile([ModelDef(name="generated", fn=generated, engine="pg")])
    await apply(await diff(compiled, f"dev_{marker}", store), compiled=compiled, engines=registry, state=store)

    reader = await registry.get("pg").fetch_sql(
        f'SELECT sum(n)::bigint AS total FROM "dev_{marker}__main".generated'  # sum(bigint) is NUMERIC in PG
    )
    assert reader.read_all().to_pylist() == [{"total": 6}]
