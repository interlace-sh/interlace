"""Postgres/Redshift rendering of DuckDB forms sqlglot would otherwise emit illegally."""

from __future__ import annotations

from pathlib import Path

from sqlglot import parse_one

from interlace.ir.dialect import render_sql
from interlace.project import Project

SPINE = "select cast(unnest(generate_series(date '2000-01-01', date '2030-01-01', interval 1 day)) as date) as date_day"
INTERVAL = "SELECT TIMESTAMP '2026-06-01 00:00:00' + INTERVAL (hash(r) % 10) SECOND AS ts FROM range(3) AS t(r)"
ROUND = "SELECT round(sum(revenue), 2) AS revenue FROM t"


def test_postgres_unwraps_generate_series_and_keeps_a_computed_interval() -> None:
    spine = render_sql(parse_one(SPINE, read="postgres"), "postgres")
    assert "UNNEST" not in spine
    assert "GENERATE_SERIES" in spine

    rendered = render_sql(parse_one(INTERVAL, read="postgres"), "postgres")
    assert "INTERVAL SECOND" not in rendered
    assert "INTERVAL '1 SECOND'" in rendered
    assert "GENERATE_SERIES" in rendered
    assert "RANGE(" not in rendered
    assert "HASHTEXTEXTENDED" in rendered


def test_round_of_a_float_casts_to_decimal_on_postgres_and_redshift() -> None:
    for dialect in ("postgres", "redshift"):
        rendered = render_sql(parse_one(ROUND, read="duckdb"), dialect)
        assert "ROUND(CAST(SUM(revenue) AS DECIMAL), 2)" in rendered
        assert "AS DOUBLE" in rendered


def test_duckdb_render_is_unchanged() -> None:
    rendered = render_sql(parse_one(INTERVAL, read="duckdb"), "duckdb")
    assert "HASHTEXTEXTENDED" not in rendered
    assert "INTERVAL" in rendered and "SECOND" in rendered


def test_explicit_postgres_engine_keeps_project_attach_as_a_sink(tmp_path: Path) -> None:
    """engines.default does not drop top-level attach:; a Postgres warehouse cannot ATTACH it."""
    (tmp_path / "interlace.yaml").write_text(
        "name: demo\n"
        "attach:\n"
        "  ext: external.duckdb\n"
        "engines:\n"
        "  default:\n"
        "    type: postgres\n"
        "    database: postgresql://postgres:pg@127.0.0.1:5455/postgres\n"
    )
    sinks = Project.load(tmp_path).open_engines().sinks
    assert sinks["ext"] == str((tmp_path / "external.duckdb").resolve())
