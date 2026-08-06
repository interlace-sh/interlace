"""Column-level lineage extraction."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from typer.testing import CliRunner

from interlace.cli.main import app
from interlace.dsl.decorators import ModelDef
from interlace.graph.column_lineage import column_impact, column_lineage
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


def test_isolated_python_model_has_no_column_lineage() -> None:
    def _build() -> None: ...

    project = compile_models([ModelDef(name="p", fn=_build)])
    assert column_lineage(project)["p"] == {}  # nothing to trace: no upstream, no declared columns


def _chain_through_python() -> object:
    """raw (SQL) -> enrich (Python, param `raw`) -> agg/summary (SQL over enrich)."""

    def enrich(raw): ...  # noqa: ANN001, ANN202 — a stand-in Python model body

    return compile_models(
        [
            ModelDef(name="raw", sql="SELECT 1 AS id, 10 AS amount, 'US' AS country"),
            ModelDef(name="enrich", fn=enrich),
            ModelDef(name="agg", sql="SELECT id, sum(amount) AS total FROM enrich GROUP BY id"),
        ]
    )


def test_traces_through_a_python_model_by_passthrough() -> None:
    # No warehouse hint: a Python model's columns are the union of its upstreams'
    # (name-passthrough), which is enough to qualify a SQL model that reads them.
    lineage = column_lineage(_chain_through_python())
    assert lineage["enrich"]["amount"] == [("raw", "amount")]  # opaque model, columns passed through by name
    assert lineage["agg"]["total"] == [("enrich", "amount")]  # the SQL downstream qualifies and traces precisely
    assert lineage["agg"]["id"] == [("enrich", "id")]


def test_known_columns_hint_traces_a_derived_python_column() -> None:
    # A column the Python model introduces (`revenue`) can't be known statically;
    # supply it via the hint (as the service does from the warehouse) and a SQL
    # model reading it now qualifies and traces to the Python model.
    project = compile_models(
        [
            ModelDef(name="raw", sql="SELECT 1 AS id, 10 AS amount"),
            ModelDef(name="enrich", fn=lambda raw: None),
            ModelDef(name="summary", sql="SELECT sum(revenue) AS revenue FROM enrich"),
        ]
    )
    hint = {"enrich": ["id", "amount", "revenue"]}
    lineage = column_lineage(project, known_columns=hint)
    assert lineage["enrich"]["revenue"] == []  # introduced here — no upstream of that name
    assert lineage["summary"]["revenue"] == [("enrich", "revenue")]


def test_count_star_is_not_mistaken_for_a_star_projection() -> None:
    # A model we can't qualify (reads a column the opaque upstream doesn't expose)
    # falls back to its own projection names — and `count(*)` must not make that look
    # like `SELECT *` and swallow every name.
    project = compile_models(
        [
            ModelDef(name="raw", sql="SELECT 'US' AS country"),
            ModelDef(name="enrich", fn=lambda raw: None),
            ModelDef(name="summary", sql="SELECT country, count(*) AS events, sum(revenue) AS revenue FROM enrich"),
        ]
    )
    lineage = column_lineage(project)  # no hint: `revenue` unknown -> summary is opaque
    assert list(lineage["summary"]) == ["country", "events", "revenue"]  # count(*) didn't collapse to []
    assert lineage["summary"]["country"] == [("enrich", "country")]
    assert lineage["summary"]["events"] == []


def test_impact_flags_a_python_consumer_as_opaque() -> None:
    # Blast radius through a Python model must still warn: name-passthrough can't see
    # a derivation like revenue<-amount, so the consumer is flagged "check it whole".
    result = column_impact(_chain_through_python(), "raw", "amount")
    impacted = {(row["model"], row["column"]) for row in result["impacted"]}
    assert ("enrich", "amount") in impacted and ("agg", "total") in impacted
    assert result["opaque_consumers"] == ["enrich"]


def test_cli_column_lineage(tmp_path: Path) -> None:
    example = Path(__file__).resolve().parents[1] / "examples" / "getting_started"
    project_dir = tmp_path / "getting_started"
    shutil.copytree(example, project_dir)

    result = CliRunner().invoke(app, ["lineage", "event_totals", "--path", str(project_dir), "--columns"])
    assert result.exit_code == 0, result.output
    assert "kind" in result.output
    assert "raw_events" in result.output  # traced to the upstream model
