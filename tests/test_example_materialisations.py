"""The materialisations example exercises every plane × strategy and builds/delivers
end-to-end: virtual (full/merge/full_merge/incremental/scd2), view, ephemeral, terminal
table (full/append/merge/full_merge/incremental) and file (parquet/csv/json)."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import sqlglot

from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "materialisations"

VIRTUAL = {
    "customers",
    "customers_view",
    "accounts_merge",
    "accounts_full_merge",
    "events_incremental",
    "customer_history",
}
TERMINAL = {
    "crm_replace",
    "crm_append",
    "crm_upsert",
    "crm_full_merge",
    "crm_incremental",
    "export_parquet",
    "export_csv",
    "export_json",
}
EXTERNAL_TABLES = {"crm_snapshot", "crm_log", "crm_accounts", "crm_state", "crm_events"}


async def test_materialisations_example_builds_and_delivers(tmp_path: Path) -> None:
    project_dir = tmp_path / "materialisations"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace", "*.duckdb*", "out"))

    project = Project.load(project_dir)
    compiled = project.compile()
    # ephemeral seed compiles but never gets its own snapshot; a terminal has no readable output
    assert compiled.models["seed"].materialise == "ephemeral"
    assert compiled.models["crm_upsert"].is_terminal and compiled.models["crm_upsert"].materialise == "table"
    assert compiled.models["export_parquet"].is_terminal

    engine = project.open_engine()
    state = await project.open_state()
    try:
        result = await apply(
            await diff(compiled, "prod", state), compiled=compiled, engine=engine, state=state, base_path=project.root
        )
        # every non-ephemeral model built/delivered
        assert VIRTUAL <= set(result.built)
        assert TERMINAL <= set(result.built)

        # reverse ETL: each external table received the 3 seed rows, never dropped
        for table in EXTERNAL_TABLES:
            reader = await engine.fetch(sqlglot.parse_one(f"SELECT count(*) AS n FROM ext.main.{table}"))
            assert reader.read_all().to_pylist() == [{"n": 3}], table

        # file exports landed in all three formats
        for name in ("customers.parquet", "customers.csv", "customers.json"):
            assert (project_dir / "out" / name).exists(), name

        # scd2 opened a validity window per key; the view reads through to customers
        history = await engine.fetch(sqlglot.parse_one("SELECT count(*) AS n FROM main.customer_history"))
        assert history.read_all().to_pylist() == [{"n": 3}]

        # re-plan/apply is a clean no-op: terminals are not re-delivered, virtuals reused
        assert (await diff(project.compile(), "prod", state)).is_empty
    finally:
        await state.close()
        engine.close()
