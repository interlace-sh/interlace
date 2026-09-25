"""Fixture tests build an ephemeral DuckDB and diff a golden CSV."""

from __future__ import annotations

from pathlib import Path

import pytest

from interlace.project import Project
from interlace.testing.golden import run_fixture_tests

pytestmark = pytest.mark.unit


def _project(tmp_path: Path) -> Path:
    models = tmp_path / "models"
    models.mkdir()
    (models / "raw.sql").write_text("SELECT 1 AS id")
    (models / "orders.sql").write_text("SELECT id, id + 1 AS n FROM raw")
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True)
    (fixtures / "raw.csv").write_text("id\n2\n")
    return tmp_path


def test_fixture_replaces_upstream_and_golden_roundtrips(tmp_path: Path) -> None:
    root = _project(tmp_path)
    compiled = Project.load(root).compile()
    written = run_fixture_tests(compiled, root, select={"orders"}, update=True)
    assert written.ok
    golden = (root / "tests" / "golden" / "orders.csv").read_text().splitlines()
    assert golden[0].startswith("id:")
    assert golden[1].startswith("2,")
    assert run_fixture_tests(compiled, root, select={"orders"}).ok


def test_golden_mismatch_is_reported(tmp_path: Path) -> None:
    root = _project(tmp_path)
    compiled = Project.load(root).compile()
    run_fixture_tests(compiled, root, select={"orders"}, update=True)
    (root / "tests" / "fixtures" / "raw.csv").write_text("id\n3\n")
    report = run_fixture_tests(compiled, root, select={"orders"})
    assert not report.ok
    assert "orders" in report.messages[0]
