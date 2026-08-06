"""Read-only query preparation — the fence shared by the web console and
`interlace query`."""

from __future__ import annotations

import pytest

from interlace.exceptions import QueryError
from interlace.query import prepare_readonly

pytestmark = pytest.mark.unit


def test_bounds_a_select_and_reports_the_cap() -> None:
    bounded, cap = prepare_readonly("SELECT * FROM raw_events", "duckdb", 100)
    assert cap == 100
    sql = bounded.sql(dialect="duckdb")
    assert "raw_events" in sql and "LIMIT 101" in sql  # cap + 1, so callers can detect truncation


def test_caps_the_limit() -> None:
    _, cap = prepare_readonly("SELECT 1 AS x", "duckdb", 1_000_000)
    assert cap == 10_000


@pytest.mark.parametrize("sql", ["SELECT * FROM range(10) t(i)", "SELECT * FROM generate_series(1, 5)"])
def test_allows_pure_generators(sql: str) -> None:
    prepare_readonly(sql, "duckdb", 100)  # must not raise


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM read_csv('/etc/hostname')",
        "SELECT * FROM read_parquet('/data/x.parquet')",
        "SELECT * FROM query('SELECT 1')",
        "SELECT * FROM query_table('main.raw_events')",
        "SELECT * FROM glob('/etc/*')",
        "SELECT * FROM some_future_reader('/x')",  # unknown table function — the allowlist still blocks it
        "SELECT read_text('/etc/hostname')",  # scalar-position reader — the denylist backstop
    ],
)
def test_rejects_reaching_outside_the_warehouse(sql: str) -> None:
    with pytest.raises(QueryError):
        prepare_readonly(sql, "duckdb", 100)


@pytest.mark.parametrize(
    "sql",
    ["DROP TABLE raw_events", "INSERT INTO raw_events VALUES (1)", "SELECT 1; SELECT 2", "SELECT FROM WHERE"],
)
def test_rejects_non_select(sql: str) -> None:
    with pytest.raises(QueryError):
        prepare_readonly(sql, "duckdb", 100)
