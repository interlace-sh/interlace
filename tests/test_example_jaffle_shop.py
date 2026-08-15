"""The jaffle-shop example — dbt's current demo project — builds end to end.

Reads its six raw tables straight from dbt's repo over HTTP, so it needs the network
and is excluded from the pre-push gate. Skipped rather than failed when the data is
unreachable: an offline `pytest` should not report a broken example.
"""

from __future__ import annotations

import shutil
import urllib.error
import urllib.request
from pathlib import Path

import pytest
from conftest import fetch_rows as _rows

from interlace.plan.apply import apply
from interlace.plan.differ import diff
from interlace.project import Project

pytestmark = [pytest.mark.integration, pytest.mark.requires_network]

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "jaffle-shop"

RAW = {"raw_customers", "raw_items", "raw_orders", "raw_products", "raw_stores", "raw_supplies"}
STAGING = {"stg_customers", "stg_locations", "stg_order_items", "stg_orders", "stg_products", "stg_supplies"}
MARTS = {"customers", "locations", "metricflow_time_spine", "order_items", "orders", "products", "supplies"}


def _jaffle_data_reachable() -> bool:
    url = "https://raw.githubusercontent.com/dbt-labs/jaffle-shop/main/seeds/jaffle-data/raw_stores.csv"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310 — fixed https URL
            return bool(response.status == 200)
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


@pytest.fixture(scope="module")
def jaffle_data() -> None:
    if not _jaffle_data_reachable():
        pytest.skip("dbt's jaffle-data is not reachable")


async def test_jaffle_shop_example_builds_end_to_end(tmp_path: Path, jaffle_data: None) -> None:
    project_dir = tmp_path / "jaffle-shop"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))

    project = Project.load(project_dir)
    compiled = project.compile()
    # model_paths lists the leaf directories, so dbt's layout survives without the
    # subdirectory becoming part of every name
    assert set(compiled.models) == RAW | STAGING | MARTS

    engine = project.open_engine()
    store = await project.open_state()
    try:
        result = await apply(await diff(compiled, "prod", store), compiled=compiled, engine=engine, state=store)
        assert set(result.built) == RAW | STAGING | MARTS

        # jaffle_shop's twenty-seven dbt data tests, all passing
        assert len(result.checks) == 27
        assert all(check.status == "passed" for check in result.checks)

        # the full upstream dataset, not a slice of it
        assert await _rows(engine, "SELECT count(*) AS n FROM main.orders") == [{"n": 61948}]
        assert await _rows(engine, "SELECT count(*) AS n FROM main.order_items") == [{"n": 90900}]

        # stg_supplies' surrogate key stands in for dbt_utils.generate_surrogate_key
        supplies = await _rows(engine, "SELECT count(DISTINCT supply_uuid) AS n FROM main.supplies")
        assert supplies == [{"n": 65}]
    finally:
        await store.close()
        engine.close()
