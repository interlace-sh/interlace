"""The benchmark example compiles and its full DAG builds end-to-end (at reduced
scale — the 25M default is for humans with a stopwatch, not the test suite)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "benchmark"


async def test_benchmark_example_builds_end_to_end(tmp_path: Path) -> None:
    project_dir = tmp_path / "benchmark"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace", "out"))
    events = project_dir / "models" / "events.sql"
    events.write_text(events.read_text().replace("range(25000000)", "range(50000)"))
    by_user = project_dir / "models" / "by_user.sql"  # the row_count floor assumes full scale
    by_user.write_text(by_user.read_text().replace("min: 90000", "min: 1"))

    project = Project.load(project_dir)
    compiled = project.compile()
    assert {"events", "by_user", "daily_revenue", "user_ltv", "revenue_report", "top_products"} <= set(compiled.models)

    engines = project.open_engines()
    store = await project.open_state()
    try:
        result = await apply(
            await diff(compiled, "prod", store), compiled=compiled, engines=engines, state=store, base_path=project.root
        )
        # every non-ephemeral model built; the parquet sink landed
        assert {"events", "by_user", "by_product", "by_device", "by_day", "user_ltv"} <= set(result.built)
        assert (project_dir / "out" / "daily_revenue.parquet").exists()
        assert all(check.status == "passed" for check in result.checks)
    finally:
        await store.close()
        engines.close()
