"""Data-quality checks: declaration parsing, built-in SQL builders (run live on
DuckDB through apply), Python @check functions, and the promotion gate."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from interlace.checks.spec import CheckSpec, parse_checks
from interlace.dsl.decorators import CheckDef, ModelDef
from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import CheckError, DefinitionError
from interlace.graph.project import compile_models
from interlace.plan.apply import ApplyResult, apply
from interlace.plan.differ import diff
from interlace.runtime.handles import RelationHandle
from interlace.state.store import SqliteStateStore

pytestmark = pytest.mark.unit

ORDERS_SQL = """
SELECT * FROM (VALUES
    (1, 'open',   10,  'a@x.io',  1),
    (2, 'closed', 20,  'b@x.io',  2),
    (3, 'open',   -5,  'nope',    9),
    (3, 'weird',  NULL, NULL,     1)
) AS t (order_id, status, amount, email, customer_id)
"""
CUSTOMERS_SQL = "SELECT * FROM (VALUES (1), (2), (3)) AS t (customer_id)"


@pytest.fixture()
async def env(tmp_path: Path) -> AsyncIterator[tuple[DuckDBAdapter, SqliteStateStore]]:
    engine = DuckDBAdapter.in_memory()
    store = await SqliteStateStore.open(tmp_path / "state.db")
    yield engine, store
    await store.close()
    engine.close()


async def _apply_orders(
    env: tuple[DuckDBAdapter, SqliteStateStore], checks: list[dict[str, Any]], python_checks: tuple[CheckDef, ...] = ()
) -> ApplyResult:
    engine, store = env
    models = [
        ModelDef(name="customers", sql=CUSTOMERS_SQL),
        ModelDef(name="orders", sql=ORDERS_SQL, checks=parse_checks(checks, "orders")),
    ]
    compiled = compile_models(models, checks=python_checks)
    return await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)


def _outcome(result: ApplyResult, name: str) -> Any:
    return next(c for c in result.checks if c.name == name)


# --- spec parsing -------------------------------------------------------------


def test_parse_shorthand_and_explicit_forms() -> None:
    specs = parse_checks(
        [
            {"not_null": "order_id"},
            {"unique": ["order_id", "status"]},
            {"type": "range", "column": "amount", "min": 0, "severity": "warn"},
            {"row_count": {"min": 1}},
        ],
        "m",
    )
    assert [s.type for s in specs] == ["not_null", "unique", "range", "row_count"]
    assert specs[1].columns == ("order_id", "status")
    assert specs[2].severity == "warn" and specs[2].params == {"min": 0}
    assert specs[3].columns == () and specs[3].params == {"min": 1}


def test_parse_rejects_bad_entries() -> None:
    with pytest.raises(DefinitionError, match="unknown check type"):
        parse_checks([{"nope": "col"}], "m")
    with pytest.raises(DefinitionError, match="severity"):
        parse_checks([{"type": "not_null", "column": "id", "severity": "fatal"}], "m")
    with pytest.raises(DefinitionError, match="needs a column"):
        parse_checks(["not_null"], "m")


# --- built-ins, run live through apply -----------------------------------------


async def test_passing_checks_promote(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    result = await _apply_orders(
        env,
        [
            {"not_null": "order_id"},
            {"accepted_values": {"column": "status", "values": ["open", "closed", "weird"]}},
            {"row_count": {"min": 1, "max": 100}},
            {"relationships": {"column": "customer_id", "to": "customers", "field": "customer_id", "severity": "warn"}},
        ],
    )
    assert result.promoted == 2
    statuses = {c.name: c.status for c in result.checks}
    assert statuses["not_null_order_id"] == "passed"
    assert statuses["accepted_values_status"] == "passed"
    assert statuses["row_count"] == "passed"
    assert statuses["relationships_customer_id"] == "failed"  # customer 9 is an orphan (warn only)
    assert _outcome(result, "relationships_customer_id").failures == 1


async def test_error_failure_blocks_promotion(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    with pytest.raises(CheckError, match="unique_order_id"):
        await _apply_orders(env, [{"unique": "order_id"}])  # order_id 3 is duplicated
    assert await store.get_environment("dev") == {}  # nothing promoted

    recorded = await store.list_check_results()
    assert [(r["check_name"], r["status"]) for r in recorded] == [("unique_order_id", "failed")]
    assert recorded[0]["failures"] == 1  # one duplicated key


async def test_null_ignoring_and_flag_checks(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    result = await _apply_orders(
        env,
        [
            {"range": {"column": "amount", "min": 0, "severity": "warn"}},  # -5 fails; NULL ignored
            {"pattern": {"column": "email", "regex": ".+@.+", "severity": "warn"}},  # 'nope' fails; NULL ignored
            {"expression": {"expression": "amount IS NULL OR amount >= -10", "severity": "warn"}},
            {"freshness": {"column": "order_id", "max_age": "1h", "severity": "warn"}},  # int max < now-1h
            {"sql": {"query": "SELECT * FROM {table} WHERE amount < -100", "severity": "warn"}},
        ],
    )
    by_name = {c.name: c for c in result.checks}
    assert by_name["range_amount"].status == "failed" and by_name["range_amount"].failures == 1
    assert by_name["pattern_email"].status == "failed" and by_name["pattern_email"].failures == 1
    assert by_name["expression"].status == "passed"
    assert by_name["freshness_order_id"].status in ("failed", "error")  # int column: stale or type error, never silent
    assert by_name["sql"].status == "passed"


async def test_python_check_gates(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def no_negative_amounts(orders: RelationHandle) -> pa.Table:
        table = orders.table()
        import pyarrow.compute as pc

        return table.filter(pc.less(pc.fill_null(table["amount"], 0), 0))  # failing rows

    check = CheckDef(name="no_negative_amounts", model="orders", fn=no_negative_amounts, severity="error")
    with pytest.raises(CheckError, match="no_negative_amounts"):
        await _apply_orders(env, [], python_checks=[check])


async def test_python_check_bool_pass(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    def has_rows(orders: RelationHandle) -> bool:
        return orders.table().num_rows > 0

    check = CheckDef(name="has_rows", model="orders", fn=has_rows, severity="error")
    result = await _apply_orders(env, [], python_checks=[check])
    assert _outcome(result, "has_rows").status == "passed"


async def test_unknown_python_check_model_rejected() -> None:
    check = CheckDef(name="x", model="ghost", fn=lambda t: True)
    with pytest.raises(DefinitionError, match="unknown model 'ghost'"):
        compile_models([ModelDef(name="real", sql="SELECT 1 AS x")], checks=[check])


async def test_changing_a_check_does_not_rebuild(env: tuple[DuckDBAdapter, SqliteStateStore]) -> None:
    engine, store = env
    models = [ModelDef(name="m", sql="SELECT 1 AS x")]
    compiled = compile_models(models)
    await apply(await diff(compiled, "dev", store), compiled=compiled, engine=engine, state=store)

    checked = compile_models([ModelDef(name="m", sql="SELECT 1 AS x", checks=parse_checks([{"not_null": "x"}], "m"))])
    assert checked.models["m"].fingerprint == compiled.models["m"].fingerprint  # data fingerprint unchanged
    plan = await diff(checked, "dev", store)
    assert plan.changes == []  # no rebuild scheduled


def test_freshness_query_shape() -> None:
    from interlace.checks.builtin import build_check_query
    from interlace.ir.relation import TableRef

    spec = CheckSpec(type="freshness", columns=("updated_at",), params={"max_age": "2h"})
    query = build_check_query(
        spec, TableRef(schema="s", name="t"), "m", "duckdb", lambda n: TableRef(schema="s", name=n)
    )
    sql = query.sql(dialect="duckdb")
    assert "INTERVAL '2' HOUR" in sql and "MAX" in sql


def test_sql_check_substitutes_table() -> None:
    from interlace.checks.builtin import build_check_query
    from interlace.ir.relation import TableRef

    spec = CheckSpec(type="sql", params={"query": "SELECT * FROM {table} WHERE x < 0"})
    query = build_check_query(
        spec, TableRef(schema="s", name="t"), "m", "duckdb", lambda n: TableRef(schema="s", name=n)
    )
    assert "FROM s.t" in query.sql(dialect="duckdb")
