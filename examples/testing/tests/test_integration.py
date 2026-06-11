"""
Integration tests — run the full pipeline with run().

Uses the programmatic API to execute all models against the real
config.yaml and DuckDB database, then inspects the result dict.

Note: run() uses the @dual decorator, so it works in both sync and
async contexts. In a sync context (like pytest without asyncio_mode),
it blocks and returns the result dict directly.
"""

from pathlib import Path

import pytest

from interlace import run


PROJECT_DIR = Path(__file__).parent.parent


@pytest.mark.integration
class TestFullPipeline:
    """Run the entire pipeline and verify outputs."""

    def test_run_all_models(self):
        """run() should execute all models and return a result per model."""
        results = run(project_dir=PROJECT_DIR, force=True)

        assert isinstance(results, dict)
        assert len(results) > 0

        # Every discovered model should appear in the results
        expected_models = {"orders", "inventory", "order_totals", "low_stock_alerts"}
        assert expected_models.issubset(results.keys())

    def test_run_single_model(self):
        """run() can target a single model (plus its dependencies)."""
        results = run(models="order_totals", project_dir=PROJECT_DIR, force=True)

        assert isinstance(results, dict)
        # order_totals depends on orders + inventory, so at least 3 models run
        assert "order_totals" in results

    def test_result_structure(self):
        """Each model result should contain status and elapsed time."""
        results = run(project_dir=PROJECT_DIR, force=True)

        for model_name, result in results.items():
            assert "status" in result, f"{model_name} missing 'status'"
            assert "elapsed" in result, f"{model_name} missing 'elapsed'"
