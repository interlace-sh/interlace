"""The jaffle-shop-classic example — dbt's original demo project — builds end to end.

The numbers are the ones the migration post publishes: eight models, twenty checks
(jaffle_shop's twenty dbt tests, one for one), and a pivot that reconciles.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from conftest import fetch_rows as _rows

from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "jaffle-shop-classic"

MODELS = {
    "raw_customers",
    "raw_orders",
    "raw_payments",
    "stg_customers",
    "stg_orders",
    "stg_payments",
    "customers",
    "orders",
}
PAYMENT_METHODS = ("credit_card", "coupon", "bank_transfer", "gift_card")


async def test_jaffle_shop_classic_example_builds_end_to_end(tmp_path: Path) -> None:
    project_dir = tmp_path / "jaffle-shop-classic"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))

    project = Project.load(project_dir)
    compiled = project.compile()
    # the staging models pin `name:`, so a subdirectory does not become part of the name
    assert MODELS == set(compiled.models)

    engine = project.open_engine()
    store = await project.open_state()
    try:
        result = await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
        assert set(result.built) == MODELS

        # jaffle_shop's twenty dbt tests, all passing — the post's headline number
        assert len(result.checks) == 20
        assert all(check.status == "passed" for check in result.checks)

        # the seed CSVs resolved against the project root, not the process CWD
        assert await _rows(engine, "SELECT count(*) AS n FROM main.raw_payments") == [{"n": 113}]

        # the pivot reconciles: every payment-method column sums to the order total
        pivot = " + ".join(f"{m}_amount" for m in PAYMENT_METHODS)
        mismatched = await _rows(engine, f"SELECT count(*) AS n FROM main.orders WHERE {pivot} <> amount")
        assert mismatched == [{"n": 0}]
    finally:
        await store.close()
        engine.close()
