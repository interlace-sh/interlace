"""End-to-end plan -> apply against a real DuckDB engine + SQLite state."""

from __future__ import annotations

import pytest
import sqlglot
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import ExecutionError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.plan.plan import ChangeType
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


def sql_model(name: str, sql: str, **kwargs: object) -> ModelDef:
    return ModelDef(name=name, sql=sql, **kwargs)  # type: ignore[arg-type]


async def test_apply_builds_dependency_chain_and_env_views(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models(
        [sql_model("a", "SELECT 1 AS id, 10 AS v"), sql_model("b", "SELECT id, v * 2 AS v2 FROM a")]
    )

    result = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    assert set(result.built) == {"a", "b"}
    # every built model carries its wall-clock build time
    assert set(result.timings) == {"a", "b"}
    assert all(seconds >= 0 for seconds in result.timings.values())
    # the downstream model read through to the upstream's physical table and the env view resolves
    assert await _rows(engine, "SELECT id, v2 FROM main.b") == [{"id": 1, "v2": 20}]


async def test_apply_reports_row_counts(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Each strategy interprets the engine's affected-row counts: replace = inserted,
    merge = a single combined written count on the native-MERGE path (DuckDB), scd =
    closed vs new versions. The merge insert/update split lives on the DELETE+INSERT
    fallback (see test_strategies)."""
    from dataclasses import replace as dc_replace

    from interlace.plan.run import run_plan
    from interlace.strategies.base import RowCounts

    engine, store = env
    await engine.execute_sql("CREATE SCHEMA raw")
    await engine.execute_sql("CREATE TABLE raw.src AS SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, v)")
    models = [
        sql_model("plain", "SELECT id, v FROM raw.src"),
        sql_model("merged", "SELECT id, v FROM raw.src", strategy="merge", key=("id",)),
        sql_model("dim", "SELECT id, v FROM raw.src", strategy="scd", key=("id",)),
    ]
    project = compile_models(models)
    result = await apply(await diff(project, "dev", store), compiled=project, engine=engine, state=store)
    assert result.rows["plain"] == RowCounts(inserted=2)
    assert result.rows["merged"] == RowCounts(inserted=2)  # nothing pre-existing: all new
    assert result.rows["dim"] == RowCounts(inserted=2)

    # second pass over changed data: 1 changed key + 1 new key
    await engine.execute_sql("UPDATE raw.src SET v = 'B' WHERE id = 2")
    await engine.execute_sql("INSERT INTO raw.src VALUES (3, 'c')")
    rerun = await apply(await run_plan(project, "dev", store), compiled=project, engine=engine, state=store)
    assert rerun.rows["plain"] == RowCounts(inserted=3)  # full refresh rewrites everything
    assert rerun.rows["merged"] == RowCounts(inserted=3)  # native MERGE: one combined written count (2 upsert + 1 new)
    assert rerun.rows["dim"] == RowCounts(inserted=2, updated=1)  # id=2 closed + reopened, id=3 new
    assert dc_replace(rerun.rows["dim"], updated=0)  # smoke: RowCounts is a plain dataclass


async def test_apply_reports_progress_events(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("a", "SELECT 1 AS x"), sql_model("b", "SELECT x FROM a")])
    events: list[tuple[str, str]] = []

    await apply(
        await diff(project, "dev", store),
        compiled=project,
        engine=engine,
        state=store,
        on_progress=lambda model, event: events.append((model, event)),
    )

    for name in ("a", "b"):
        assert events.index((name, "start")) < events.index((name, "done"))
    assert events.index(("a", "done")) < events.index(("b", "start"))  # b waits for its upstream


async def test_apply_reports_failed_progress_event(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("broken", "SELECT x FROM does_not_exist_anywhere")])
    events: list[tuple[str, str]] = []

    # a build failure surfaces as a clean ExecutionError naming the model — not a raw
    # engine traceback — which the CLI renders as one `error:` line
    with pytest.raises(ExecutionError) as excinfo:
        await apply(
            await diff(project, "dev", store),
            compiled=project,
            engine=engine,
            state=store,
            on_progress=lambda model, event: events.append((model, event)),
        )

    assert "broken" in excinfo.value.message and excinfo.value.details.get("model") == "broken"
    assert excinfo.value.__cause__ is not None  # original engine error preserved for --debug
    assert events == [("broken", "start"), ("broken", "failed")]


async def test_re_apply_is_a_no_op(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    models = [sql_model("a", "SELECT 1 AS x")]
    await apply(
        await diff(compile_models(models), "prod", store), compiled=compile_models(models), engine=engine, state=store
    )

    plan = await diff(compile_models(models), "prod", store)
    assert plan.is_empty


async def test_ephemeral_models_are_not_counted_as_promoted(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """An ephemeral model is inlined into its consumers — it has no promotable table, so
    it must not inflate the promoted count over the models that actually built."""
    engine, store = env
    project = compile_models(
        [
            sql_model("base", "SELECT 1 AS id"),
            sql_model("mid", "SELECT id FROM base", materialise="ephemeral"),
            sql_model("out", "SELECT id FROM mid"),
        ]
    )
    result = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert set(result.built) == {"base", "out"}  # mid is inlined, never built
    assert result.promoted == 2  # base + out — the ephemeral mid is tracked but not counted


async def test_second_environment_reuses_the_shared_table_instead_of_rebuilding(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """Snapshots are content-addressed and shared across environments, so promoting
    the same logic to a second env is a view-swap onto the already-built table — not
    a rebuild. The differ marks it ADDED, but apply reuses rather than recomputes."""
    engine, store = env
    project = compile_models(
        [sql_model("a", "SELECT 1 AS id, 10 AS v"), sql_model("b", "SELECT id, v * 2 AS v2 FROM a")]
    )
    prod = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert set(prod.built) == {"a", "b"} and not prod.reused

    dev_plan = await diff(project, "dev", store)
    assert all(task.reuse_existing for task in dev_plan.backfills)  # both fingerprints already materialised
    dev = await apply(dev_plan, compiled=project, engine=engine, state=store)
    assert not dev.built and set(dev.reused) == {"a", "b"} and dev.promoted == 2
    # the sandbox view resolves through to the same shared physical table
    assert await _rows(engine, "SELECT id, v2 FROM dev__main.b") == [{"id": 1, "v2": 20}]


async def test_reuse_path_still_gates_on_checks(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A reused snapshot must still have its checks run — a fingerprint recorded with a
    failing check (built, but promotion-blocked in the first env) cannot slip into a
    second env unchecked."""
    from interlace.checks.spec import parse_checks
    from interlace.exceptions import CheckError

    engine, store = env
    # a check that always fails: id is never 999
    models = [
        sql_model(
            "m", "SELECT 1 AS id", checks=parse_checks([{"accepted_values": {"column": "id", "values": [999]}}], "m")
        )
    ]
    project = compile_models(models)
    with pytest.raises(CheckError):  # first env: builds the table, then the check blocks promotion
        await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    dev_plan = await diff(project, "dev", store)
    assert dev_plan.backfills[0].reuse_existing  # the table exists from prod's (blocked) build
    with pytest.raises(CheckError):  # reuse still runs the check, so the second env is gated too
        await apply(dev_plan, compiled=project, engine=engine, state=store)


async def test_view_materialisation(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("answer", "SELECT 42 AS n", materialise="view")])
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    assert await _rows(engine, "SELECT n FROM main.answer") == [{"n": 42}]


async def test_merge_upserts_across_runs(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    # Drives the strategy + atomic execute_all directly (as a scheduled re-run would),
    # since the differ only re-runs a model when its definition changes.
    from interlace.engines.base import EngineCaps
    from interlace.ir.relation import SqlRelation, TableRef
    from interlace.strategies import Merge

    engine, _ = env
    target = TableRef(schema="main", name="dim")
    strategy = Merge(("id",))
    caps = EngineCaps(supports_create_or_replace=True)

    def relation(sql: str) -> SqlRelation:
        return SqlRelation(ast=sqlglot.parse_one(sql))

    await engine.execute_all(
        strategy.plan_statements(relation("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) v(id, name)"), target, caps)
    )
    assert sorted(await _rows(engine, "SELECT id, name FROM main.dim"), key=lambda r: r["id"]) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]

    await engine.execute_all(
        strategy.plan_statements(relation("SELECT * FROM (VALUES (2, 'B'), (3, 'c')) v(id, name)"), target, caps)
    )
    assert sorted(await _rows(engine, "SELECT id, name FROM main.dim"), key=lambda r: r["id"]) == [
        {"id": 1, "name": "a"},  # untouched
        {"id": 2, "name": "B"},  # updated
        {"id": 3, "name": "c"},  # inserted
    ]


async def test_apply_merge_model_first_build(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models(
        [sql_model("dim", "SELECT * FROM (VALUES (1, 'a')) v(id, name)", strategy="merge", key=("id",))]
    )
    await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert await _rows(engine, "SELECT id, name FROM main.dim") == [{"id": 1, "name": "a"}]


async def test_apply_passes_a_satisfied_contract(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    project = compile_models([sql_model("c", "SELECT 1 AS id, 'x' AS name", columns={"id": None, "name": None})])
    result = await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)
    assert result.built == ["c"]


async def test_apply_blocks_on_contract_drift(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    from interlace.exceptions import SchemaError

    engine, store = env
    # contract demands a column the query does not produce
    project = compile_models([sql_model("c", "SELECT 1 AS id", columns={"id": None, "missing": None})])
    with pytest.raises(SchemaError):
        await apply(await diff(project, "prod", store), compiled=project, engine=engine, state=store)

    # promotion did not happen: the model is still pending in a fresh plan
    assert not (await diff(project, "prod", store)).is_empty


async def test_modify_then_reapply_rebuilds_and_repoints(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    v1 = compile_models([sql_model("a", "SELECT 1 AS x")])
    await apply(await diff(v1, "prod", store), compiled=v1, engine=engine, state=store)

    v2 = compile_models([sql_model("a", "SELECT 2 AS x")])
    plan = await diff(v2, "prod", store)
    assert plan.changes[0].change_type is ChangeType.MODIFIED
    await apply(plan, compiled=v2, engine=engine, state=store)

    assert await _rows(engine, "SELECT x FROM main.a") == [{"x": 2}]


async def test_removed_model_demotes_and_drops_its_view(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Deleting a model must not leave its env view serving the last snapshot
    forever (and pinning it against gc)."""
    engine, store = env
    v1 = compile_models([sql_model("keep", "SELECT 1 AS x"), sql_model("gone", "SELECT 2 AS y")])
    await apply(await diff(v1, "prod", store), compiled=v1, engine=engine, state=store)
    assert await _rows(engine, "SELECT y FROM main.gone") == [{"y": 2}]

    v2 = compile_models([sql_model("keep", "SELECT 1 AS x")])
    plan = await diff(v2, "prod", store)
    assert [c.name for c in plan.changes if c.change_type is ChangeType.REMOVED] == ["gone"]
    await apply(plan, compiled=v2, engine=engine, state=store)

    assert "gone" not in await store.get_environment("prod")  # demoted
    import duckdb as duckdb_mod

    with pytest.raises(duckdb_mod.CatalogException):
        await _rows(engine, "SELECT y FROM main.gone")  # view dropped
    assert await _rows(engine, "SELECT x FROM main.keep") == [{"x": 1}]  # survivor untouched


async def test_removal_only_plan_prunes_the_environment_row(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A plan whose ONLY change is a removal must still settle: nothing to build,
    but the environment row goes, so the next plan is clean. Regression for a
    removal that reported itself forever and masked the next one."""
    engine, store = env
    v1 = compile_models([sql_model("keep", "SELECT 1 AS x"), sql_model("raw.gone", "SELECT 2 AS y")])
    await apply(await diff(v1, "prod", store), compiled=v1, engine=engine, state=store)

    v2 = compile_models([sql_model("keep", "SELECT 1 AS x")])  # schema-qualified model deleted
    plan = await diff(v2, "prod", store)
    assert not plan.backfills and not plan.reuses  # removal-only: nothing to build
    assert [c.name for c in plan.changes] == ["raw.gone"]

    await apply(plan, compiled=v2, engine=engine, state=store)
    assert "raw.gone" not in await store.get_environment("prod")
    assert (await diff(v2, "prod", store)).is_empty  # the next plan is clean


async def test_unscoped_run_prunes_removals_like_apply(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """A scheduler-driven project only ever calls run_plan; an unscoped run
    promotes every model, so it must retire deleted ones too."""
    from interlace.plan.run import run_plan

    engine, store = env
    v1 = compile_models([sql_model("keep", "SELECT 1 AS x"), sql_model("raw.gone", "SELECT 2 AS y")])
    await apply(await diff(v1, "prod", store), compiled=v1, engine=engine, state=store)

    v2 = compile_models([sql_model("keep", "SELECT 1 AS x")])
    plan = await run_plan(v2, "prod", store)
    assert [c.name for c in plan.changes if c.change_type is ChangeType.REMOVED] == ["raw.gone"]
    await apply(plan, compiled=v2, engine=engine, state=store)
    assert "raw.gone" not in await store.get_environment("prod")

    # a SCOPED run must not touch unrelated environment rows
    v3 = compile_models([sql_model("keep", "SELECT 1 AS x"), sql_model("raw.other", "SELECT 3 AS z")])
    await apply(await diff(v3, "prod", store), compiled=v3, engine=engine, state=store)
    scoped = await run_plan(v2, "prod", store, select={"keep"})
    assert not [c for c in scoped.changes if c.change_type is ChangeType.REMOVED]
