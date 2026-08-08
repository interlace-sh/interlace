"""Python model execution: Arrow in (RelationHandles), Arrow out, loaded at the sink."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pytest
from conftest import fetch_rows as _rows

from interlace.dsl.decorators import ModelDef, model
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import DefinitionError, PlanError, SchemaError
from interlace.graph.project import compile_models
from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.runtime.handles import RelationHandle
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit


async def _build(env: tuple[DuckDBAdapter, SqliteStateStore], models: list[ModelDef]) -> None:
    engine, store = env
    compiled = compile_models(models)
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)


RAW = ModelDef(name="raw", sql="SELECT * FROM (VALUES (1, 10), (2, 20), (3, 5)) AS t (id, amount)")


async def test_sync_function_returning_table(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def doubled(raw: RelationHandle) -> pa.Table:
        table = raw.table()
        return table.set_column(1, "amount", pc.multiply(table["amount"], 2))

    await _build(env, [RAW, ModelDef(name="doubled", fn=doubled, depends_on=("raw",))])
    rows = await _rows(env[0], "SELECT * FROM dev__main.doubled ORDER BY id")
    assert rows == [{"id": 1, "amount": 20}, {"id": 2, "amount": 40}, {"id": 3, "amount": 10}]


async def test_parameter_named_after_a_model_links_without_depends_on(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    # No depends_on: the `raw` parameter matches the `raw` model, so the edge is inferred
    # (the same reference-implies-dependency rule SQL models get).
    def doubled(raw: RelationHandle) -> pa.Table:
        table = raw.table()
        return table.set_column(1, "amount", pc.multiply(table["amount"], 2))

    await _build(env, [RAW, ModelDef(name="doubled", fn=doubled)])
    rows = await _rows(env[0], "SELECT * FROM dev__main.doubled ORDER BY id")
    assert rows == [{"id": 1, "amount": 20}, {"id": 2, "amount": 40}, {"id": 3, "amount": 10}]


async def test_async_function_returning_reader(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    async def passthrough(raw: RelationHandle) -> pa.RecordBatchReader:
        return raw.reader()

    await _build(env, [RAW, ModelDef(name="passthrough", fn=passthrough, depends_on=("raw",))])
    rows = await _rows(env[0], "SELECT count(*) AS n FROM dev__main.passthrough")
    assert rows == [{"n": 3}]


async def test_generator_streams_batches(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def filtered(raw: RelationHandle) -> Iterator[pa.RecordBatch]:
        for batch in raw.reader():
            mask = pc.greater(batch.column("amount"), 8)
            yield batch.filter(mask)

    await _build(env, [RAW, ModelDef(name="filtered", fn=filtered, depends_on=("raw",))])
    rows = await _rows(env[0], "SELECT id FROM dev__main.filtered ORDER BY id")
    assert [r["id"] for r in rows] == [1, 2]


async def test_python_model_over_ephemeral_upstream(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    staged = ModelDef(name="staged", sql="SELECT id, amount FROM raw WHERE amount >= 10", materialise="ephemeral")

    def total(staged: RelationHandle) -> pa.Table:
        return pa.table({"total": [pc.sum(staged.table()["amount"]).as_py()]})

    await _build(env, [RAW, staged, ModelDef(name="total", fn=total, depends_on=("staged",))])
    assert await _rows(env[0], "SELECT total FROM dev__main.total") == [{"total": 30}]


async def test_downstream_sql_reads_python_output(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def doubled(raw: RelationHandle) -> pa.Table:
        table = raw.table()
        return table.set_column(1, "amount", pc.multiply(table["amount"], 2))

    await _build(
        env,
        [
            RAW,
            ModelDef(name="doubled", fn=doubled, depends_on=("raw",)),
            ModelDef(name="big", sql="SELECT id FROM doubled WHERE amount > 15"),
        ],
    )
    rows = await _rows(env[0], "SELECT id FROM dev__main.big ORDER BY id")
    assert [r["id"] for r in rows] == [1, 2]


async def test_contract_validated_on_python_output(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def bad(raw: RelationHandle) -> pa.Table:
        return raw.table().drop_columns(["amount"])

    bad_model = ModelDef(name="bad", fn=bad, depends_on=("raw",), columns={"id": None, "amount": None})
    with pytest.raises(SchemaError):
        await _build(env, [RAW, bad_model])


async def test_unknown_parameter_is_rejected(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def broken(nonexistent: RelationHandle) -> pa.Table:
        return nonexistent.table()

    with pytest.raises(DefinitionError, match="match no upstream model or declared dependency"):
        await _build(env, [RAW, ModelDef(name="broken", fn=broken, depends_on=("raw",))])


async def test_qualified_dependency_injects_by_underscore_spelling(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """A schema-qualified upstream has no legal Python parameter name, so it is
    addressable with dots replaced by underscores."""
    landed = ModelDef(name="raw.landed", sql="SELECT * FROM (VALUES (1, 10), (2, 20)) AS t (id, amount)")

    def curated(raw_landed: RelationHandle) -> pa.Table:
        table = raw_landed.table()
        return table.set_column(1, "amount", pc.multiply(table["amount"], 3))

    await _build(env, [landed, ModelDef(name="seccl.curated", fn=curated, depends_on=("raw.landed",))])
    rows = await _rows(env[0], "SELECT * FROM dev__seccl.curated ORDER BY id")
    assert rows == [{"id": 1, "amount": 30}, {"id": 2, "amount": 60}]


async def test_exact_dependency_name_wins_over_another_models_alias(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """``raw_landed`` as a model name must not be displaced by ``raw.landed``'s alias."""
    dotted = ModelDef(name="raw.landed", sql="SELECT 1 AS id, 10 AS amount")
    flat = ModelDef(name="raw_landed", sql="SELECT 2 AS id, 99 AS amount")

    def curated(raw_landed: RelationHandle) -> pa.Table:
        return raw_landed.table()

    await _build(
        env,
        [dotted, flat, ModelDef(name="picked", fn=curated, depends_on=("raw.landed", "raw_landed"))],
    )
    assert await _rows(env[0], "SELECT amount FROM dev__main.picked") == [{"amount": 99}]


async def test_python_model_supports_keyed_strategies(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def keyed(raw: RelationHandle) -> pa.Table:
        return raw.table()

    keyed_model = ModelDef(name="keyed", fn=keyed, depends_on=("raw",), strategy="merge", key=("id",))
    await _build(env, [RAW, keyed_model])
    rows = await _rows(env[0], "SELECT id FROM dev__main.keyed ORDER BY id")
    assert [row["id"] for row in rows] == [1, 2, 3]


async def test_python_hash_merge_builds_first_time_and_detects_change(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """hash_merge on a Python model must build on the FIRST run — its _hash is built from
    the staged output's columns, since the target doesn't exist yet — then write only the
    delta on re-runs (idempotent when nothing changed, an update when a row's value flips)."""
    from interlace.plan.run import run_plan

    engine, store = env
    state = {"tier": "gold"}

    def dim(raw: RelationHandle) -> pa.Table:
        table = raw.table()
        return table.append_column("tier", pa.array([state["tier"]] * table.num_rows))

    model = ModelDef(name="dim", fn=dim, depends_on=("raw",), strategy="hash_merge", key=("id",))
    await _build(env, [RAW, model])  # first build: would raise "needs explicit columns" before the fix
    assert await _rows(engine, "SELECT count(*) AS n FROM dev__main.dim") == [{"n": 3}]
    assert await _rows(engine, "SELECT count(*) AS n FROM dev__main.dim WHERE _hash IS NOT NULL") == [{"n": 3}]

    compiled = compile_models([RAW, model])
    rerun = await apply(await run_plan(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    assert not rerun.rows.get("dim")  # unchanged data: nothing written

    state["tier"] = "silver"  # every row's payload changes
    delta = await apply(await run_plan(compiled, "dev", store), compiled=compiled, engine=engine, state=store)
    assert (delta.rows["dim"].inserted, delta.rows["dim"].updated) == (0, 3)  # all updates, no inserts
    assert await _rows(engine, "SELECT DISTINCT tier FROM dev__main.dim") == [{"tier": "silver"}]


async def test_python_scd2_model_survives_reruns(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Regression: stage-to-target alignment used to NULL-fill scd2's validity columns
    into the source, breaking the strategy's EXCEPT arity on every run after the first."""
    from interlace.plan.run import run_plan

    state = {"tier": "gold"}

    def dim(raw: RelationHandle) -> pa.Table:
        table = raw.table()
        return table.append_column("tier", pa.array([state["tier"]] * table.num_rows))

    model = ModelDef(name="dim", fn=dim, depends_on=("raw",), strategy="scd", key=("id",))
    await _build(env, [RAW, model])

    state["tier"] = "silver"  # every key changes: old versions close, new ones open
    engine, store = env
    compiled = compile_models([RAW, model])
    await apply(await run_plan(compiled, "dev", store), compiled=compiled, engine=engine, state=store)

    rows = await _rows(engine, "SELECT tier, _valid_to IS NULL AS open FROM dev__main.dim WHERE id = 1 ORDER BY open")
    assert [(r["tier"], r["open"]) for r in rows] == [("gold", False), ("silver", True)]


async def test_python_model_rejects_incremental(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def windowed(raw: RelationHandle) -> pa.Table:
        return raw.table()

    windowed_model = ModelDef(
        name="windowed", fn=windowed, depends_on=("raw",), strategy="incremental", time_column="ts"
    )
    with pytest.raises(PlanError, match="incremental"):
        await _build(env, [RAW, windowed_model])


async def test_handle_is_single_pass(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def greedy(raw: RelationHandle) -> pa.Table:
        raw.table()
        return raw.table()  # second read must fail loudly

    with pytest.raises(DefinitionError, match="already consumed"):
        await _build(env, [RAW, ModelDef(name="greedy", fn=greedy, depends_on=("raw",))])


def test_decorator_rejects_python_views() -> None:
    with pytest.raises(DefinitionError, match="cannot be views"):

        @model(materialise="view")
        def as_view() -> None: ...


async def test_python_incremental_requires_a_key(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    """Unkeyed is refused, and the error says why rather than just redirecting."""

    def windowed(raw: RelationHandle) -> pa.Table:
        return raw.table()

    windowed_model = ModelDef(
        name="windowed", fn=windowed, depends_on=("raw",), strategy="incremental", time_column="ts"
    )
    with pytest.raises(PlanError, match="without a key"):
        await _build(env, [RAW, windowed_model])


async def test_python_keyed_incremental_upserts_within_the_window(
    env: tuple[DuckDBAdapter, SqliteStateStore],
) -> None:
    """The window bounds which staged rows are written; keys it does not supply survive.

    Same contract as the SQL path, reached differently: the Arrow output is staged and
    the strategy runs against that stage. The window is driven explicitly so both runs
    hit the same fingerprint — and therefore the same physical table, which is the only
    way a "row survived" assertion means anything.
    """
    from datetime import datetime

    from interlace.plan.differ import snapshot_of
    from interlace.plan.plan import BackfillTask, Plan, ViewSwap, env_view
    from interlace.state.interval import Interval
    from interlace.state.snapshot import ChangeCategory

    engine, store = env
    await engine.execute_sql("CREATE SCHEMA IF NOT EXISTS main")
    await engine.execute_sql(
        "CREATE TABLE main.events AS SELECT * FROM (VALUES "
        "(1, DATE '2026-01-01', 'a'), (2, DATE '2026-01-01', 'b')) v(id, ts, val)"
    )

    def scored(events: RelationHandle) -> pa.Table:
        return events.table()

    project = compile_models(
        [
            ModelDef(name="events", sql="SELECT id, ts, val FROM main.events"),
            ModelDef(
                name="scored",
                fn=scored,
                depends_on=("events",),
                strategy="incremental",
                time_column="ts",
                interval="1d",
                key=("id",),
            ),
        ]
    )
    window = Interval(datetime(2026, 1, 1), datetime(2026, 1, 2))

    async def run_scored() -> None:
        model = project.models["scored"]
        snap = snapshot_of(model, ChangeCategory.BREAKING)
        plan = Plan(environment="prod")
        plan.backfills = [BackfillTask(snapshot=snap, interval=window)]
        plan.virtual_updates = [ViewSwap(env_view("prod", model.name), model.physical_table)]
        await apply(plan, compiled=project, engine=engine, state=store)

    # The Python model reads its upstream's physical table, so seed that directly:
    # rebuilding `events` is not the subject here.
    upstream = project.models["events"].physical_table
    await engine.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {upstream.schema}")
    await engine.execute_sql(f"CREATE OR REPLACE TABLE {upstream.to_expr().sql()} AS SELECT * FROM main.events")

    await run_scored()
    assert len(await _rows(engine, "SELECT id FROM main.scored")) == 2

    # id 2 leaves the source, id 1 changes. Same window, same fingerprint.
    await engine.execute_sql(
        f"CREATE OR REPLACE TABLE {upstream.to_expr().sql()} AS "
        "SELECT * FROM (VALUES (1, DATE '2026-01-01', 'a2')) v(id, ts, val)"
    )
    await run_scored()

    rows = await _rows(engine, "SELECT id, val FROM main.scored ORDER BY id")
    assert [(r["id"], r["val"]) for r in rows] == [
        (1, "a2"),
        (2, "b"),
    ], "the upsert updated id 1 and left id 2 alone — the keyed window never rewrites the period"
