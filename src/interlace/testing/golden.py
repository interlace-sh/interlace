"""Build selected models against CSV fixtures and diff the result to a golden CSV.

``tests/fixtures/<model>.csv`` stands in for an upstream model, so the test does
not build it. ``tests/golden/<model>.csv`` is the expected result: a header of
``column:type`` and one row per output row (``\\N`` is null). This does not run
live checks or the promotion gate.
"""

from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow as pa

from interlace.engines.duckdb import DuckDBAdapter
from interlace.exceptions import PlanError
from interlace.graph.project import CompiledProject
from interlace.ir.relation import TableRef
from interlace.plan.resolve import resolve_model_query

_NULL = r"\N"


@dataclass
class FixtureReport:
    """Models that matched, and the mismatch lines for those that did not."""

    passed: list[str] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.messages


def run_fixture_tests(
    compiled: CompiledProject,
    root: Path,
    *,
    select: set[str] | None = None,
    update: bool = False,
) -> FixtureReport:
    """Diff ``select`` (default: models that already have a golden) against golden CSVs.

    ``update`` rewrites the golden file from the actual result instead of diffing.
    """
    chosen = select if select is not None else _models_with_golden(compiled, root)
    if not chosen:
        return FixtureReport(messages=["no golden files under tests/golden; pass --select or --update-golden"])
    unknown = sorted(chosen - set(compiled.models))
    if unknown:
        raise PlanError(f"unknown model(s) in the test selection: {', '.join(unknown)}")

    report = FixtureReport()
    engine = DuckDBAdapter.in_memory()
    try:
        built = _materialise(engine, compiled, root, chosen)
        for name in sorted(chosen):
            table = built[name]
            golden = _golden_path(root, name)
            rendered = _render(table)
            if update:
                golden.parent.mkdir(parents=True, exist_ok=True)
                golden.write_text(rendered)
                report.passed.append(name)
                continue
            if not golden.exists():
                report.messages.append(
                    f"{name}: no golden file at {golden.relative_to(root)}; re-run with --update-golden"
                )
                continue
            if golden.read_text() == rendered:
                report.passed.append(name)
                continue
            report.messages.append(f"{name}: {_describe_diff(golden.read_text(), rendered)}")
    finally:
        engine.close()
    return report


def _models_with_golden(compiled: CompiledProject, root: Path) -> set[str]:
    directory = root / "tests" / "golden"
    if not directory.exists():
        return set()
    found: set[str] = set()
    for path in directory.glob("*.csv"):
        name = path.stem
        if name in compiled.models:
            found.add(name)
    return found


def _golden_path(root: Path, name: str) -> Path:
    return root / "tests" / "golden" / f"{name}.csv"


def _fixture_path(root: Path, name: str) -> Path:
    return root / "tests" / "fixtures" / f"{name}.csv"


def _table_name(name: str) -> str:
    return "g_" + re.sub(r"[^A-Za-z0-9_]", "_", name)


def _materialise(
    engine: DuckDBAdapter, compiled: CompiledProject, root: Path, selected: set[str]
) -> dict[str, pa.Table]:
    needed: set[str] = set(selected)
    for name in selected:
        needed.update(compiled.graph.ancestors(name))
    physical: dict[str, TableRef] = {}
    for model in compiled.ordered():
        if model.name not in needed or model.materialise == "ephemeral":
            continue
        relation = _table_name(model.name)
        physical[model.name] = TableRef(schema="main", name=relation)
        fixture = _fixture_path(root, model.name)
        if model.name not in selected and fixture.exists():
            _load_fixture(engine, relation, fixture)
            continue
        if model.ast is None:
            raise PlanError(
                f"model {model.name!r} is Python; add tests/fixtures/{model.name}.csv "
                f"so the test can load it without building it"
            )
        if model.materialise == "ephemeral":
            continue
        resolved = resolve_model_query(model, compiled, physical)
        engine.execute_sync(f"CREATE TABLE {relation} AS {resolved.sql(dialect='duckdb')}")

    built: dict[str, pa.Table] = {}
    for name in selected:
        model = compiled.models[name]
        if model.materialise == "ephemeral":
            raise PlanError(f"model {name!r} is ephemeral and has no result to diff")
        built_at = physical.get(name)
        if built_at is None:
            raise PlanError(f"model {name!r} was not built")
        built[name] = engine.fetch_sync(f"SELECT * FROM {built_at.name}").read_all()
    return built


def _load_fixture(engine: DuckDBAdapter, relation: str, path: Path) -> None:
    escaped = str(path).replace("'", "''")
    engine.execute_sync(
        f"CREATE TABLE {relation} AS SELECT * FROM read_csv('{escaped}', header=true, auto_detect=true)"
    )


def _render(table: pa.Table) -> str:
    header = [f"{field.name}:{field.type}" for field in table.schema]
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(header)
    for row in table.to_pylist():
        writer.writerow([_cell(row[field.name]) for field in table.schema])
    return buffer.getvalue()


def _cell(value: object) -> str:
    if value is None:
        return _NULL
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float):
        return format(value, ".12g")
    return str(value)


def _describe_diff(expected: str, actual: str) -> str:
    expected_rows = expected.splitlines()
    actual_rows = actual.splitlines()
    if expected_rows[:1] != actual_rows[:1]:
        return f"columns differ: expected {expected_rows[:1]} actual {actual_rows[:1]}"
    if len(expected_rows) != len(actual_rows):
        return f"row count differs: expected {len(expected_rows) - 1} actual {len(actual_rows) - 1}"
    for index, (left, right) in enumerate(zip(expected_rows, actual_rows, strict=True)):
        if left != right:
            return f"row {index} differs: expected {left} actual {right}"
    return "result differs"
