"""SCD Type 2: history tracking with validity windows, column-agnostic."""

from __future__ import annotations

import pytest
import sqlglot
from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import PlanError
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies import Scd, resolve_strategy

pytestmark = pytest.mark.unit

TARGET = TableRef(schema="main", name="dim_customers")


def _relation(sql: str) -> SqlRelation:
    return SqlRelation(ast=sqlglot.parse_one(sql))


def _source(rows: list[tuple[int, str, str]]) -> str:
    values = ", ".join(f"({i}, '{name}', '{tier}')" for i, name, tier in rows)
    return f"SELECT * FROM (VALUES {values}) AS t (id, name, tier)"


async def _run(engine: DuckDBAdapter, strategy: Scd, rows: list[tuple[int, str, str]]) -> None:
    statements = strategy.plan_statements(_relation(_source(rows)), TARGET, engine.caps)
    await engine.execute_all(statements)


async def _state(engine: DuckDBAdapter) -> list[tuple]:
    reader = await engine.fetch_sql(
        "SELECT id, name, tier, _valid_to IS NULL AS open FROM main.dim_customers ORDER BY id, _valid_from, open"
    )
    return [tuple(r.values()) for r in reader.read_all().to_pylist()]


async def test_scd2_lifecycle() -> None:
    engine = DuckDBAdapter.in_memory()
    strategy = Scd(("id",))

    # initial load: everything open
    await _run(engine, strategy, [(1, "ada", "gold"), (2, "bob", "silver")])
    assert await _state(engine) == [(1, "ada", "gold", True), (2, "bob", "silver", True)]

    # identical rerun: a no-op — no new versions, nothing closed
    await _run(engine, strategy, [(1, "ada", "gold"), (2, "bob", "silver")])
    assert await _state(engine) == [(1, "ada", "gold", True), (2, "bob", "silver", True)]

    # change bob's tier: old version closed, new version open; ada untouched
    await _run(engine, strategy, [(1, "ada", "gold"), (2, "bob", "gold")])
    assert await _state(engine) == [
        (1, "ada", "gold", True),
        (2, "bob", "silver", False),
        (2, "bob", "gold", True),
    ]

    # delete bob, add cli: bob's current version closes, cli appears
    await _run(engine, strategy, [(1, "ada", "gold"), (3, "cli", "bronze")])
    assert await _state(engine) == [
        (1, "ada", "gold", True),
        (2, "bob", "silver", False),
        (2, "bob", "gold", False),
        (3, "cli", "bronze", True),
    ]

    # bob returns with his old values: a fresh version opens (history is honest)
    await _run(engine, strategy, [(1, "ada", "gold"), (2, "bob", "gold"), (3, "cli", "bronze")])
    rows = await _state(engine)
    assert (2, "bob", "gold", True) in rows and len([r for r in rows if r[0] == 2]) == 3
    engine.close()


async def test_scd2_multi_column_key() -> None:
    engine = DuckDBAdapter.in_memory()
    strategy = Scd(("id", "name"))
    await _run(engine, strategy, [(1, "ada", "gold"), (1, "ada2", "gold")])  # same id, distinct key
    await _run(engine, strategy, [(1, "ada", "platinum"), (1, "ada2", "gold")])

    rows = await _state(engine)
    assert (1, "ada", "gold", False) in rows  # only the changed composite key got a new version
    assert (1, "ada", "platinum", True) in rows
    assert (1, "ada2", "gold", True) in rows and len(rows) == 3
    engine.close()


async def test_scd_event_time_windows_follow_the_data() -> None:
    engine = DuckDBAdapter.in_memory()
    strategy = Scd(("id",), time_column="ts")

    def src(rows: list[tuple[int, str, str]]) -> SqlRelation:
        values = ", ".join(f"({i}, '{tier}', '{ts}')" for i, tier, ts in rows)
        return SqlRelation(ast=sqlglot.parse_one(f"SELECT * FROM (VALUES {values}) AS t (id, tier, ts)"))

    await engine.execute_all(
        strategy.plan_statements(src([(1, "gold", "2024-01-01"), (2, "silver", "2024-01-02")]), TARGET, engine.caps)
    )
    # id1 changes (event time 2024-02-01); id2 vanishes upstream; id3 is new (2024-02-03)
    await engine.execute_all(
        strategy.plan_statements(src([(1, "platinum", "2024-02-01"), (3, "bronze", "2024-02-03")]), TARGET, engine.caps)
    )

    reader = await engine.fetch_sql(
        "SELECT id, tier, CAST(_valid_from AS VARCHAR) vf, CAST(_valid_to AS VARCHAR) vt "
        "FROM main.dim_customers ORDER BY id, _valid_from"
    )
    rows = [tuple(r.values()) for r in reader.read_all().to_pylist()]
    # a changed key's old version closes at exactly the succeeding version's event time; windows abut
    assert (1, "gold", "2024-01-01 00:00:00", "2024-02-01 00:00:00") in rows
    assert (1, "platinum", "2024-02-01 00:00:00", None) in rows
    # id3 new + open, stamped with its own event time
    assert (3, "bronze", "2024-02-03 00:00:00", None) in rows
    # id2 vanished: no succeeding event, so closed at processing time (not a 2024 event time)
    (id2,) = [r for r in rows if r[0] == 2]
    assert id2[3] is not None and not id2[3].startswith("2024")
    engine.close()


def test_scd_event_time_statement_shape() -> None:
    statements = Scd(("id",), time_column="ts").plan_statements(
        _relation("SELECT 1 AS id, TIMESTAMP '2024-01-01' AS ts"), TARGET, EngineCaps(supports_star_exclude=True)
    )
    kinds = [type(s) for s in statements]
    assert kinds == [exp.Create, exp.Update, exp.Update, exp.Insert]  # ensure, close_changed, close_vanished, insert
    assert "_valid_to = CAST(_f.ts AS TIMESTAMP)" in statements[1].sql(dialect="duckdb")


def test_resolver_and_validation() -> None:
    assert isinstance(resolve_strategy("table", "scd", key=("id",)), Scd)
    with pytest.raises(PlanError, match="requires a key"):
        resolve_strategy("table", "scd")


def test_statements_shape() -> None:
    statements = Scd(("id",)).plan_statements(
        _relation("SELECT 1 AS id"), TARGET, EngineCaps(supports_star_exclude=True)
    )
    kinds = [type(s) for s in statements]
    assert kinds == [exp.Create, exp.Update, exp.Insert]
    create, update, insert = (s.sql(dialect="duckdb") for s in statements)
    assert "IF NOT EXISTS" in create and "_valid_from" in create
    assert "EXCLUDE (_valid_from, _valid_to)" in update and "EXCEPT" in update
    assert insert.startswith("INSERT INTO")
