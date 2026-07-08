"""Guard the committed example project: it must plan and apply cleanly."""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import sqlglot

from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


async def test_getting_started_applies_end_to_end(tmp_path: Path) -> None:
    # Copy into a temp dir so running the example never dirties the source tree.
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))

    project = Project.load(project_dir)
    compiled = project.compile()
    assert compiled.models["top_kind"].dependencies == ("event_totals",)
    assert compiled.models["recent_clicks"].materialise == "view"  # per-model SQL config

    engine = project.open_engine()
    state = await project.open_state()
    try:
        result = await apply(await diff(compiled, "dev", state), compiled=compiled, engine=engine, state=state)
        assert set(result.built) == {"raw_events", "event_totals", "top_kind", "recent_clicks"}

        reader = await engine.fetch(sqlglot.parse_one("SELECT kind FROM dev__main.top_kind"))
        assert reader.read_all().to_pylist() == [{"kind": "purchase"}]  # highest total_amount

        clicks = await engine.fetch(sqlglot.parse_one("SELECT count(*) AS n FROM dev__main.recent_clicks"))
        assert clicks.read_all().to_pylist() == [{"n": 2}]  # two 'click' events in the seed

        # re-plan is clean
        assert (await diff(project.compile(), "dev", state)).is_empty
    finally:
        await state.close()
        engine.close()
