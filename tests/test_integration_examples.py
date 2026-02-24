"""
Integration tests that run interlace against example projects.

These tests verify end-to-end behaviour: model discovery, execution,
state store population, and quality checks running post-materialization.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).parent.parent / "examples" / "basic"


def _has_example() -> bool:
    """Check if the basic example project exists with data files."""
    return (
        EXAMPLES_DIR.exists()
        and (EXAMPLES_DIR / "config.yaml").exists()
        and (EXAMPLES_DIR / "data" / "users.csv").exists()
    )


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not _has_example(), reason="examples/basic not available"),
]


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    """Copy the basic example to a temp dir for isolation."""
    dest = tmp_path / "basic"
    shutil.copytree(EXAMPLES_DIR, dest, ignore=shutil.ignore_patterns("__pycache__", "*.egg-info", "logs", ".venv"))
    return dest


class TestBasicProjectExecution:
    """Run the basic example project and verify results."""

    async def test_run_returns_results(self, project_dir: Path) -> None:
        """Run should return results for all models."""
        from interlace.core.api import run

        old_cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            result = await run(project_dir=project_dir, force=True)
        finally:
            os.chdir(old_cwd)

        assert isinstance(result, dict)
        assert len(result) >= 5
        # Check key models are present
        assert "users" in result
        assert "orders" in result
        assert "products" in result
        assert "user_orders" in result
        assert "user_summary" in result

    async def test_all_models_succeed(self, project_dir: Path) -> None:
        """All models should complete successfully."""
        from interlace.core.api import run

        old_cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            result = await run(project_dir=project_dir, force=True)
        finally:
            os.chdir(old_cwd)

        for model_name, model_result in result.items():
            assert model_result.get("status") == "success", f"Model '{model_name}' failed: {model_result}"

    async def test_models_produce_rows(self, project_dir: Path) -> None:
        """Models should produce non-zero rows."""
        from interlace.core.api import run

        old_cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            result = await run(project_dir=project_dir, force=True)
        finally:
            os.chdir(old_cwd)

        for model_name in ["users", "orders", "products"]:
            rows = result[model_name].get("rows")
            assert rows is not None and rows > 0, f"Model '{model_name}' produced no rows: {result[model_name]}"

    async def test_quality_checks_execute(self, project_dir: Path) -> None:
        """Quality checks configured in config.yaml should execute."""
        from interlace.core.api import run

        old_cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            await run(project_dir=project_dir, force=True)
        finally:
            os.chdir(old_cwd)

        # The basic example configures quality checks for: users, orders, products
        # All should succeed (pass). Check via the state store by re-initializing.
        from interlace.core.initialization import initialize

        old_cwd2 = os.getcwd()
        os.chdir(project_dir)
        try:
            config, state_store, _, _ = initialize(project_dir)
        finally:
            os.chdir(old_cwd2)

        assert state_store is not None
        conn = state_store._get_connection()
        assert conn is not None

        # Quality results should exist
        qr = conn.sql("SELECT * FROM interlace.quality_results ORDER BY model_name, check_name").execute()
        assert len(qr) >= 3, f"Expected >= 3 quality results, got {len(qr)}"

        # Check specific models
        models_checked = set(qr["model_name"].tolist())
        assert "orders" in models_checked
        assert "products" in models_checked

        # All checks should have passed
        statuses = qr["status"].tolist()
        assert all(s == "passed" for s in statuses), f"Some checks failed: {qr.to_dict('records')}"

    async def test_second_run_skips_unchanged(self, project_dir: Path) -> None:
        """Second run without force should skip unchanged models."""
        from interlace.core.api import run

        old_cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            await run(project_dir=project_dir, force=True)
            result2 = await run(project_dir=project_dir)
        finally:
            os.chdir(old_cwd)

        # At least some models should be skipped on second run
        skipped = [k for k, v in result2.items() if v.get("status") == "skipped"]
        assert len(skipped) > 0, f"No models were skipped on second run: {result2}"

    async def test_force_re_executes(self, project_dir: Path) -> None:
        """Force run should re-execute all models even after previous run."""
        from interlace.core.api import run

        old_cwd = os.getcwd()
        os.chdir(project_dir)
        try:
            await run(project_dir=project_dir, force=True)
            result2 = await run(project_dir=project_dir, force=True)
        finally:
            os.chdir(old_cwd)

        # All models should succeed (not be skipped)
        for model_name, model_result in result2.items():
            assert (
                model_result.get("status") == "success"
            ), f"Model '{model_name}' was not re-executed on force: {model_result}"
