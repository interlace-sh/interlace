"""Spark engine adapter: SQL-shape/caps (unit) and native apply (live, local Spark).

The live tests spin up a **local** Spark session backed by Delta Lake (so the
mutating strategies have row-level DELETE/MERGE) — no cluster, but they need the
``spark`` extra (PySpark + delta-spark) and a JVM, and Delta pulls its jar from
Maven on first session start. They skip cleanly when the extra isn't installed,
and are marked ``slow`` (JVM startup)."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest
import sqlglot

from interlace.engines.base import EngineCaps
from interlace.engines.spark import spark_type_name
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies.merge import Merge
from interlace.strategies.scd import Scd


def _spark_available() -> bool:
    try:
        import delta  # noqa: F401
        import pyspark  # noqa: F401

        return True
    except ImportError:
        return False


requires_spark = pytest.mark.skipif(not _spark_available(), reason="no 'spark' extra (pyspark + delta-spark)")


# --- unit: dialect + caps, no session needed ------------------------------------


@pytest.mark.unit
def test_spark_dialect_and_caps() -> None:
    from interlace.engines.spark import SparkAdapter

    assert SparkAdapter.dialect == "spark"
    assert SparkAdapter.caps.supports_merge  # native MERGE on Delta/Iceberg
    assert not SparkAdapter.caps.supports_star_exclude  # scd enumerates instead
    assert not SparkAdapter.caps.supports_create_or_replace  # DROP + CREATE


@pytest.mark.unit
def test_spark_type_mapping() -> None:
    assert spark_type_name("int") == "INTEGER"
    assert spark_type_name("bigint") == "BIGINT"
    assert spark_type_name("double") == "DOUBLE"
    assert spark_type_name("string") == "VARCHAR"
    assert spark_type_name("boolean") == "BOOLEAN"
    assert spark_type_name("timestamp") == "TIMESTAMP"
    assert spark_type_name("decimal(10,2)") == "DECIMAL"


@pytest.mark.unit
def test_merge_and_scd_transpile_to_spark() -> None:
    target = TableRef(schema="s", name="t")
    merge = Merge(("id",)).plan_statements(
        SqlRelation(ast=sqlglot.parse_one("SELECT id, v FROM src")),
        target,
        EngineCaps(supports_merge=True),
        None,
        columns=["id", "v"],
    )
    assert merge[0].sql(dialect="spark").startswith("MERGE INTO")
    scd = Scd(("id",)).plan_statements(
        SqlRelation(ast=sqlglot.parse_one("SELECT id, tier FROM src")), target, EngineCaps()
    )
    for statement in scd:  # scd enumerates (no star-EXCLUDE) — must render in the spark dialect
        assert statement.sql(dialect="spark")


# --- live: native apply inside a local Spark (Delta-backed) ---------------------


@pytest.fixture
def spark_engine(tmp_path: pytest.TempPathFactory) -> Iterator:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    from interlace.engines.spark import SparkAdapter

    warehouse = str(tmp_path)  # type: ignore[call-overload]
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("interlace-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.warehouse.dir", warehouse)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")
    )
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:  # Delta jar can't resolve (offline/version) -> skip, don't fail
        pytest.skip(f"could not start a Delta-backed Spark session: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    engine = SparkAdapter(session)
    try:
        yield engine
    finally:
        engine.close()


async def _rows(engine, sql: str) -> list[dict]:
    reader = await engine.fetch_sql(sql)
    return reader.read_all().to_pylist()


@requires_spark
@pytest.mark.slow
@pytest.mark.requires_db
async def test_merge_upserts_natively_in_spark(spark_engine) -> None:
    engine = spark_engine
    await engine.create_schema("s")
    await engine.load(TableRef(schema="s", name="dim"), pa.table({"id": [1, 2], "v": ["a", "b"]}).to_reader(), "create")
    src = SqlRelation(ast=sqlglot.parse_one("SELECT * FROM VALUES (2, 'B'), (3, 'c') AS s(id, v)"))
    statements = Merge(("id",)).plan_statements(src, TableRef(schema="s", name="dim"), engine.caps, None, ["id", "v"])
    assert engine.transpile(statements[0]).startswith("MERGE INTO")  # native single-statement path
    await engine.execute_all(statements)
    assert sorted(await _rows(engine, "SELECT id, v FROM s.dim"), key=lambda r: r["id"]) == [
        {"id": 1, "v": "a"},  # untouched
        {"id": 2, "v": "B"},  # updated in place
        {"id": 3, "v": "c"},  # inserted
    ]


@requires_spark
@pytest.mark.slow
@pytest.mark.requires_db
async def test_incremental_windows_in_spark(spark_engine) -> None:
    # Windowed DELETE+INSERT uses literal predicates (no subquery), so it runs on Delta —
    # unlike scd/full_merge, whose mutation conditions use subqueries Delta rejects
    # (DELTA_UNSUPPORTED_SUBQUERY). Reprocessing a window is idempotent.
    from datetime import datetime

    from interlace.state.interval import Interval
    from interlace.strategies.incremental import Incremental

    engine = spark_engine
    await engine.create_schema("w")
    target = TableRef(schema="w", name="events")
    strategy = Incremental("ts")
    query = SqlRelation(ast=sqlglot.parse_one("SELECT * FROM VALUES (CAST('2026-01-01' AS DATE), 5) AS e(ts, n)"))
    window = Interval(datetime(2026, 1, 1), datetime(2026, 1, 2))
    await engine.execute_all(strategy.plan_statements(query, target, engine.caps, window))
    await engine.execute_all(strategy.plan_statements(query, target, engine.caps, window))  # rerun = idempotent

    rows = await _rows(engine, "SELECT n FROM w.events")
    assert [r["n"] for r in rows] == [5]  # exactly one row after two runs of the same window
