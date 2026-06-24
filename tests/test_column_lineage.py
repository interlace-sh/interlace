"""Column-level lineage extraction."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.dsl.decorators import ModelDef
from interlace.graph.column_lineage import column_lineage
from interlace.graph.project import compile_models

pytestmark = pytest.mark.unit


def test_traces_output_columns_to_upstream_model() -> None:
    project = compile_models(
        [
            ModelDef(name="raw", sql="SELECT 1 AS id, 10 AS amount"),
            ModelDef(name="agg", sql="SELECT id, sum(amount) AS total FROM raw GROUP BY id"),
        ]
    )
    lineage = column_lineage(project)
    assert lineage["agg"]["id"] == [("raw", "id")]
    assert lineage["agg"]["total"] == [("raw", "amount")]
    assert lineage["raw"]["id"] == []  # a literal has no source column


def test_join_attributes_columns_to_their_tables() -> None:
    project = compile_models(
        [
            ModelDef(name="o", sql="SELECT 1 AS id, 2 AS cid, 5 AS amt"),
            ModelDef(name="c", sql="SELECT 2 AS cid, 'x' AS name"),
            ModelDef(name="j", sql="SELECT o.amt, c.name FROM o JOIN c ON o.cid = c.cid"),
        ]
    )
    lineage = column_lineage(project)
    assert lineage["j"]["amt"] == [("o", "amt")]
    assert lineage["j"]["name"] == [("c", "name")]


def test_external_table_columns_are_attributed() -> None:
    project = compile_models([ModelDef(name="m", sql="SELECT ts, val FROM main.events")])
    lineage = column_lineage(project)
    assert lineage["m"]["ts"] == [("main.events", "ts")]
    assert lineage["m"]["val"] == [("main.events", "val")]


def test_derived_column_with_multiple_inputs() -> None:
    project = compile_models([ModelDef(name="m", sql="SELECT a + b AS total FROM (SELECT 1 AS a, 2 AS b) s")])
    lineage = column_lineage(project)
    assert set(lineage["m"]["total"]) == {("s", "a"), ("s", "b")}


def test_python_model_has_no_column_lineage() -> None:
    def _build() -> None: ...

    project = compile_models([ModelDef(name="p", fn=_build)])
    assert column_lineage(project)["p"] == {}


def test_cli_column_lineage(tmp_path: Path) -> None:
    example = Path(__file__).resolve().parents[1] / "examples" / "getting_started"
    project_dir = tmp_path / "getting_started"
    shutil.copytree(example, project_dir)

    result = CliRunner().invoke(app, ["lineage", "event_totals", "--path", str(project_dir), "--columns"])
    assert result.exit_code == 0, result.output
    assert "kind" in result.output
    assert "raw_events" in result.output  # traced to the upstream model
