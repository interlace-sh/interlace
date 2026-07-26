"""Cross-engine transfers (T2): DAGs span engines; every move is an explicit
plan line-item executed as Arrow fetch → staged load on the consumer's engine."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import sqlglot

from interlace.dsl.decorators import ModelDef
from interlace.engines.base import EngineAdapter
from interlace.engines.duckdb import DuckDBAdapter
from interlace.engines.registry import EngineRegistry
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

ENGINES = {"default", "second"}


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


async def _rows(engine: EngineAdapter, sql: str) -> list[dict]:
    return (await engine.fetch(sqlglot.parse_one(sql))).read_all().to_pylist()


async def test_transfer_planned_and_executed(env: tuple[EngineRegistry, SqliteStateStore]) -> None:
    registry, store = env
    compiled = _compile(
        [
            _model("up", "SELECT * FROM (VALUES (1), (2), (3)) AS t (x)"),
            _model("down", "SELECT sum(x) AS total FROM up", engine="second"),
        ]
    )
    plan = await diff(compiled, "dev", store)
    assert [(t.model, t.source.name, t.target.name, t.via) for t in plan.transfers] == [
        ("up", "default", "second", "arrow")
    ]

    result = await apply(plan, compiled=compiled, engines=registry, state=store)
    assert result.transfers == ["up: default -> second (interlace__xfer.up, arrow)"]
    assert await _rows(registry.get("second"), "SELECT total FROM dev__main.down") == [{"total": 6}]
    # the staged copy lives on the consumer's engine
    assert await _rows(registry.get("second"), "SELECT count(*) AS n FROM interlace__xfer.up") == [{"n": 3}]


async def test_transfer_deduplicates_per_target(env: tuple[EngineRegistry, SqliteStateStore]) -> None:
    registry, store = env
    compiled = _compile(
        [
            _model("up", "SELECT 1 AS x"),
            _model("a", "SELECT x FROM up", engine="second"),
            _model("b", "SELECT x + 1 AS y FROM up", engine="second"),
        ]
    )
    plan = await diff(compiled, "dev", store)
    assert len(plan.transfers) == 1  # two consumers, one engine: one move

    result = await apply(plan, compiled=compiled, engines=registry, state=store)
    assert len(result.transfers) == 1
    assert await _rows(registry.get("second"), "SELECT y FROM dev__main.b") == [{"y": 2}]


async def test_transfer_refreshes_when_upstream_reruns(env: tuple[EngineRegistry, SqliteStateStore]) -> None:
    """Staging is replaced on every apply that builds the consumer — a re-run
    upstream (merge/incremental semantics) is never read stale."""
    from interlace.plan.run import run_plan

    registry, store = env
    models = [
        _model("up", "SELECT * FROM (VALUES (1), (2)) AS t (x)"),
        _model("down", "SELECT count(*) AS n FROM up", engine="second"),
    ]
    compiled = _compile(models)
    await apply(await diff(compiled, "dev", store), compiled=compiled, engines=registry, state=store)
    assert await _rows(registry.get("second"), "SELECT n FROM dev__main.down") == [{"n": 2}]

    # same fingerprints, forced re-run: transfer must move the fresh rows
    plan = await run_plan(compiled, "dev", store)
    assert [t.model for t in plan.transfers] == ["up"]
    result = await apply(plan, compiled=compiled, engines=registry, state=store)
    assert len(result.transfers) == 1


async def test_same_engine_consumers_keep_reading_the_original(
    env: tuple[EngineRegistry, SqliteStateStore],
) -> None:
    """The staged override is scoped to the cross-engine consumer, not global."""
    registry, store = env
    compiled = _compile(
        [
            _model("up", "SELECT 5 AS x"),
            _model("local_down", "SELECT x FROM up"),  # same engine: no transfer involved
            _model("remote_down", "SELECT x FROM up", engine="second"),
        ]
    )
    plan = await diff(compiled, "dev", store)
    assert len(plan.transfers) == 1
    await apply(plan, compiled=compiled, engines=registry, state=store)

    assert await _rows(registry.get("default"), "SELECT x FROM dev__main.local_down") == [{"x": 5}]
    assert await _rows(registry.get("second"), "SELECT x FROM dev__main.remote_down") == [{"x": 5}]
    # no staging leaked onto the default engine
    assert not await registry.get("default").table_exists(plan.transfers[0].table)


async def test_python_consumer_reads_staged_cross_engine_upstream(
    env: tuple[EngineRegistry, SqliteStateStore],
) -> None:
    import pyarrow as pa
    import pyarrow.compute as pc

    def doubled(up) -> pa.Table:
        table = up.table()
        return table.set_column(0, "x", pc.multiply(table["x"], 2))

    registry, store = env
    compiled = _compile(
        [
            _model("up", "SELECT * FROM (VALUES (10), (20)) AS t (x)"),
            ModelDef(name="doubled", fn=doubled, depends_on=("up",), engine="second"),
        ]
    )
    plan = await diff(compiled, "dev", store)
    assert [t.model for t in plan.transfers] == ["up"]
    await apply(plan, compiled=compiled, engines=registry, state=store)
    rows = await _rows(registry.get("second"), "SELECT x FROM dev__main.doubled ORDER BY x")
    assert [r["x"] for r in rows] == [20, 40]


async def test_attach_fast_lane_for_file_backed_engines(tmp_path: Path) -> None:
    """When engines are file-backed in one process, the transfer upgrades to a
    federated CTAS (via=attach) — no Arrow hop. Falls back to Arrow otherwise."""
    from interlace.project import Project

    project_dir = tmp_path / "proj"
    (project_dir / "models").mkdir(parents=True)
    (project_dir / "interlace.yaml").write_text(
        "name: fastlane\n"
        "database: primary.duckdb\n"
        "engines:\n"
        "  analytics:\n"
        "    type: duckdb\n"
        "    database: analytics.duckdb\n"
    )
    (project_dir / "models" / "up.sql").write_text("SELECT * FROM (VALUES (1), (2)) AS t (x)")
    (project_dir / "models" / "down.sql").write_text(
        "/* interlace: {engine: analytics} */\nSELECT sum(x) AS total FROM up"
    )

    project = Project.load(project_dir)
    compiled = project.compile()

    # first apply builds `up` too, so its engine is open: the fast lane must
    # detect the file-handle conflict and fall back to Arrow
    registry = project.open_engines()
    store = await project.open_state()
    try:
        first = await apply(await diff(compiled, "dev", store), compiled=compiled, engines=registry, state=store)
        assert first.transfers == ["up: default -> analytics (interlace__xfer.up, arrow)"]
    finally:
        await store.close()
        registry.close()

    # change only the consumer: the source engine is never opened this apply,
    # so the transfer upgrades to a federated CTAS (attach) — and DETACHes after
    (project_dir / "models" / "down.sql").write_text(
        "/* interlace: {engine: analytics} */\nSELECT sum(x) + 100 AS total FROM up"
    )
    project = Project.load(project_dir)
    compiled = project.compile()
    registry = project.open_engines()
    store = await project.open_state()
    try:
        second = await apply(await diff(compiled, "dev", store), compiled=compiled, engines=registry, state=store)
        assert second.built == ["down"]
        assert second.transfers == ["up: default -> analytics (interlace__xfer.up, attach)"]
        rows = await _rows(registry.get("analytics"), "SELECT total FROM dev__main.down")
        assert rows == [{"total": 103}]
        # the DETACH left the source openable: its own adapter still works
        rows = await _rows(registry.get("default"), "SELECT count(*) AS n FROM dev__main.up")
        assert rows == [{"n": 2}]
    finally:
        await store.close()
        registry.close()
