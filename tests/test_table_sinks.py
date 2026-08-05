"""Terminal `materialise: table` (reverse ETL): deliver a model's result into an
attached database with full(replace) / append / merge_by_key / full_merge /
incremental_by_time — never dropping the live table."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef, validate_materialise
from interlace.dsl.discovery import discover_models
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import DefinitionError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.run import run_plan
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


def _table(sql: str, target: str, strategy: str = "full", key: tuple[str, ...] = ()) -> ModelDef:
    return ModelDef(name="push", sql=sql, materialise="table", target=target, strategy=strategy, key=key)


async def _apply(
    env: tuple[DuckDBAdapter, SqliteStateStore],
    sql: str,
    target: str,
    strategy: str = "full",
    key: tuple[str, ...] = (),
) -> None:
    engine, store = env
    compiled = compile_models([_table(sql, target, strategy, key)])
    await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)


def _values(rows: str) -> str:
    return f"SELECT * FROM (VALUES {rows}) AS t (id, v)"


def test_config_validation() -> None:
    with pytest.raises(DefinitionError, match="did you mean materialise: virtual"):
        validate_materialise("m", materialise="table", strategy="full", target=None, path=None, format=None, key=())
    with pytest.raises(DefinitionError, match="requires key"):
        validate_materialise(
            "m", materialise="table", strategy="merge_by_key", target="ext.t", path=None, format=None, key=()
        )
    with pytest.raises(DefinitionError, match="append requires materialise: table"):
        validate_materialise("m", materialise="virtual", strategy="append", target=None, path=None, format=None, key=())


def test_removed_export_key_raises_migration_error(tmp_path: Path) -> None:
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "m.sql").write_text(
        "/* interlace: {export: {to: table, target: ext.main.t, mode: replace}} */\nSELECT 1 AS id"
    )
    with pytest.raises(DefinitionError, match="export: was removed in 2.0"):
        discover_models(tmp_path, ["models"], "duckdb")


async def test_full_replaces_in_place(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, _values("(1, 'a'), (2, 'b')"), "ext.main.dest")
    await _apply(env, _values("(3, 'c')"), "ext.main.dest")  # changed model: delivers the new state

    assert await _rows(engine, "SELECT id, v FROM ext.main.dest ORDER BY id") == [{"id": 3, "v": "c"}]


async def test_append_accumulates(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, _values("(1, 'a')"), "ext.main.log", strategy="append")
    await _apply(env, _values("(2, 'b')"), "ext.main.log", strategy="append")

    assert [r["id"] for r in await _rows(engine, "SELECT id FROM ext.main.log ORDER BY id")] == [1, 2]


async def test_merge_by_key_upserts(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, _values("(1, 'a'), (2, 'b')"), "ext.main.accounts", "merge_by_key", ("id",))
    await _apply(env, _values("(2, 'B'), (3, 'c')"), "ext.main.accounts", "merge_by_key", ("id",))

    assert await _rows(engine, "SELECT id, v FROM ext.main.accounts ORDER BY id") == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "B"},
        {"id": 3, "v": "c"},
    ]


async def test_full_merge_deletes_vanished_keys(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, _values("(1, 'a'), (2, 'b')"), "ext.main.state", "full_merge", ("id",))
    await _apply(env, _values("(1, 'a')"), "ext.main.state", "full_merge", ("id",))  # 2 vanished

    assert await _rows(engine, "SELECT id FROM ext.main.state") == [{"id": 1}]


async def test_incremental_by_time_into_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """The capability the reframe unlocks: windowed DELETE+INSERT into an external table.
    A forced run over a two-day window delivers each grain; the external table is never
    dropped, and re-running the same window is idempotent."""
    engine, store = env
    day1 = datetime(2024, 1, 1)
    day2 = datetime(2024, 1, 2)
    rows = "SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 09:00'), (2, TIMESTAMP '2024-01-02 09:00')) AS t (id, day)"
    model = ModelDef(
        name="push",
        sql=rows,
        materialise="table",
        target="ext.main.events",
        strategy="incremental_by_time",
        time_column="day",
        interval="1d",
    )
    compiled = compile_models([model])
    plan = await run_plan(compiled, "prod", store, start=day1, end=day2 + timedelta(days=1))
    assert plan.virtual_updates == []  # terminal: no env view
    await apply(plan, compiled=compiled, engine=engine, state=store)
    assert [r["id"] for r in await _rows(engine, "SELECT id FROM ext.main.events ORDER BY id")] == [1, 2]

    # catchup: re-running the same window skips filled intervals (delivers nothing new)
    plan2 = await run_plan(compiled, "prod", store, start=day1, end=day2 + timedelta(days=1))
    assert plan2.backfills == []


async def test_evolves_added_column(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """The user-hit regression: the model grows a column — the external table must
    gain it (never 'has 3 columns but 4 values were supplied')."""
    engine, _ = env
    await _apply(env, "SELECT 1 AS id, 'a' AS v", "ext.main.scores")
    await _apply(env, "SELECT 2 AS id, 'b' AS v, 9.5 AS score", "ext.main.scores")  # grew a column

    assert await _rows(engine, "SELECT id, v, score FROM ext.main.scores") == [{"id": 2, "v": "b", "score": 9.5}]


async def test_null_fills_vanished_column(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, "SELECT 1 AS id, 'a' AS v", "ext.main.trimmed")
    await _apply(env, "SELECT 2 AS id", "ext.main.trimmed")  # v vanished from the model

    assert await _rows(engine, "SELECT id, v FROM ext.main.trimmed") == [{"id": 2, "v": None}]


async def test_reordered_columns_land_by_name(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Positional-insert regression: same columns, different order — values must land in
    the right columns, not silently swap."""
    engine, _ = env
    await _apply(env, "SELECT 1 AS id, 'a' AS v", "ext.main.swapped", strategy="append")
    await _apply(env, "SELECT 'b' AS v, 2 AS id", "ext.main.swapped", strategy="append")  # reordered projection

    assert await _rows(engine, "SELECT id, v FROM ext.main.swapped ORDER BY id") == [
        {"id": 1, "v": "a"},
        {"id": 2, "v": "b"},
    ]


async def test_merge_evolves_added_column(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, _ = env
    await _apply(env, _values("(1, 'a'), (2, 'b')"), "ext.main.members", "merge_by_key", ("id",))
    await _apply(env, "SELECT 2 AS id, 'B' AS v, 'gold' AS tier", "ext.main.members", "merge_by_key", ("id",))

    assert await _rows(engine, "SELECT id, v, tier FROM ext.main.members ORDER BY id") == [
        {"id": 1, "v": "a", "tier": None},
        {"id": 2, "v": "B", "tier": "gold"},
    ]


async def test_project_attach_reaches_external_database(tmp_path: Path) -> None:
    """attach: config wires an external db; a table model delivers into it."""
    import duckdb

    from interlace.project import Project

    project_dir = tmp_path / "proj"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "interlace.yaml").write_text("name: attach_demo\ndatabase: ':memory:'\nattach:\n  crm: crm.duckdb\n")
    (project_dir / "models" / "push.sql").write_text(
        "/* interlace: {materialise: table, target: crm.main.contacts, strategy: merge_by_key, key: id} */\n"
        "SELECT 1 AS id, 'ada' AS name"
    )

    project = Project.load(project_dir)
    compiled = project.compile()
    engine = project.open_engine()
    store = await project.open_state()
    try:
        await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
    finally:
        await store.close()
        engine.close()

    external = duckdb.connect(str(project_dir / "crm.duckdb"))  # relative path resolved against the project
    assert external.execute("SELECT id, name FROM contacts").fetchall() == [(1, "ada")]
    external.close()


async def test_is_environment_gated(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A dev apply must not fire reverse-ETL at the live destination: the terminal's
    snapshot is recorded (plan settles) but nothing is delivered."""
    engine, store = env
    compiled = compile_models([_table(_values("(1, 'a')"), "ext.main.gated")])
    result = await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)

    assert result.gated == ["push"] and result.built == []
    assert (await diff(compiled, "dev", store)).is_empty  # recorded: no rescheduling loop
    tables = await _rows(engine, "SELECT table_name FROM information_schema.tables WHERE table_catalog = 'ext'")
    assert all(t["table_name"] != "gated" for t in tables)  # nothing left the warehouse

    # the same fingerprint delivers when prod applies it
    prod = await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
    assert prod.built == ["push"]
    assert await _rows(engine, "SELECT id FROM ext.main.gated") == [{"id": 1}]


async def test_environments_opt_in(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """environments= widens the gate explicitly."""
    engine, store = env
    model = ModelDef(
        name="push", sql=_values("(7, 'z')"), materialise="table", target="ext.main.dev_ok", environments=("dev",)
    )
    compiled = compile_models([model])
    result = await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)

    assert result.built == ["push"] and result.gated == []
    assert await _rows(engine, "SELECT id FROM ext.main.dev_ok") == [{"id": 7}]


def _incr(target: str) -> ModelDef:
    src = "SELECT * FROM (VALUES (1, TIMESTAMP '2024-01-01 09:00'), (2, TIMESTAMP '2024-01-02 09:00')) AS t (id, day)"
    return ModelDef(
        name="push",
        sql=src,
        materialise="table",
        target=target,
        strategy="incremental_by_time",
        time_column="day",
        interval="1d",
    )


async def test_incremental_restate_does_not_stage_per_window(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A windowed incremental delivery runs straight against the target — it must NOT
    re-stage the whole source per window (that made a wide restate O(windows × source))."""
    engine, store = env
    compiled = compile_models([_incr("ext.main.evt")])
    await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)  # bootstrap
    # restate several windows with the target already existing — the direct path
    plan = await run_plan(compiled, "prod", store, start=datetime(2024, 1, 1), end=datetime(2024, 1, 3), restate=True)
    assert len(plan.backfills) == 2  # two 1d windows
    await apply(plan, compiled=compiled, engine=engine, state=store)

    assert [r["id"] for r in await _rows(engine, "SELECT id FROM ext.main.evt ORDER BY id")] == [1, 2]
    # no per-window stage table was ever left behind (the staging path is skipped for incrementals)
    staged = await _rows(
        engine, "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '%sink_stage%'"
    )
    assert staged == []


async def test_wide_window_range_warns(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A restate spanning far more windows than intended surfaces a heads-up warning."""
    _, store = env
    compiled = compile_models([_incr("ext.main.wide")])
    plan = await run_plan(compiled, "prod", store, start=datetime(2020, 1, 1), end=datetime(2024, 1, 1), restate=True)
    assert any("push" in w and "windows" in w for w in plan.warnings)


async def test_table_checks_gate_delivery(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A materialise: table model can carry checks — they run against the delivered
    external table and gate promotion (a failing error-severity check fails the apply)."""
    from interlace.checks.spec import CheckSpec
    from interlace.exceptions import CheckError

    engine, store = env
    check = (CheckSpec(type="not_null", columns=("id",), severity="error", params={}),)

    good = compile_models(
        [ModelDef(name="push", sql="SELECT 1 AS id", materialise="table", target="ext.main.checked", checks=check)]
    )
    result = await apply(await diff(good, "prod", store), compiled=good, engine=engine, state=store)
    assert result.built == ["push"]
    assert result.checks and all(not o.blocking for o in result.checks)  # ran + passed against the external table

    bad = compile_models(
        [
            ModelDef(
                name="push2",
                sql="SELECT CAST(NULL AS INTEGER) AS id",
                materialise="table",
                target="ext.main.badcheck",
                checks=check,
            )
        ]
    )
    with pytest.raises(CheckError):  # failing check gates: apply raises, environment not promoted
        await apply(await diff(bad, "prod", store), compiled=bad, engine=engine, state=store)


async def test_downstream_reads_a_table_models_external_output(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A table model is a normal DAG node: a downstream can depend on it and reads its
    delivered external table (built after it, by the dependency edge)."""
    engine, store = env
    models = [
        ModelDef(name="push", sql=_values("(1, 'a'), (2, 'b')"), materialise="table", target="ext.main.pushed"),
        ModelDef(name="derived", sql="SELECT id, v FROM push WHERE id = 2"),  # virtual, reads the external table
    ]
    compiled = compile_models(models)
    assert "push" in compiled.models["derived"].dependencies
    await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)

    # derived (a normal virtual snapshot + prod view) reflects push's delivered rows
    assert await _rows(engine, "SELECT id, v FROM main.derived ORDER BY id") == [{"id": 2, "v": "b"}]
