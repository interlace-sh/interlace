"""A long-lived process recompiles models and refuses runtime-config drift."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from interlace.exceptions import ConfigurationError
from interlace.project import Project
from interlace.scheduler.daemon import reload_if_stale, remember_runtime

pytestmark = pytest.mark.unit


def _project(tmp_path: Path, yaml: str = "name: demo\n") -> SimpleNamespace:
    (tmp_path / "models").mkdir(exist_ok=True)
    (tmp_path / "models" / "orders.sql").write_text("SELECT 1 AS id")
    (tmp_path / "interlace.yaml").write_text(yaml)
    project = Project.load(tmp_path)
    state = SimpleNamespace(
        root=tmp_path,
        model_paths=project.config.model_paths,
        source_mtime=0.0,
        reload_lock=asyncio.Lock(),
        streams={},
        describe_cache={},
    )
    remember_runtime(state, project.config)
    return state


async def test_a_model_edit_recompiles(tmp_path: Path) -> None:
    state = _project(tmp_path)
    await reload_if_stale(state)
    first = state.compiled.models["orders"].fingerprint
    (tmp_path / "models" / "orders.sql").write_text("SELECT 2 AS id")
    await reload_if_stale(state)
    assert state.compiled.models["orders"].fingerprint != first
    assert state.restart_required is None


async def test_an_engine_change_requires_a_restart(tmp_path: Path) -> None:
    state = _project(tmp_path)
    await reload_if_stale(state)
    (tmp_path / "interlace.yaml").write_text(
        "name: demo\nengines:\n  pg:\n    type: postgres\n    database: postgresql://u@db.internal:5432/app\n"
    )
    with pytest.raises(ConfigurationError, match="engines"):
        await reload_if_stale(state)
    assert "orders" in state.compiled.models  # the opened graph stays


async def test_reverting_the_config_clears_the_restart(tmp_path: Path) -> None:
    state = _project(tmp_path)
    await reload_if_stale(state)
    original = (tmp_path / "interlace.yaml").read_text()
    (tmp_path / "interlace.yaml").write_text(
        original + "connections:\n  api:\n    type: http\n    base_url: https://example.com\n"
    )
    with pytest.raises(ConfigurationError, match="connections"):
        await reload_if_stale(state)
    (tmp_path / "interlace.yaml").write_text(original)
    await reload_if_stale(state)
    assert state.restart_required is None
