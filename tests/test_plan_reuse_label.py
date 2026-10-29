"""A new environment reuses tables another environment already built. Say so."""

from __future__ import annotations

import pytest

from interlace.ir.relation import TableRef
from interlace.plan.comment import plan_markdown
from interlace.plan.plan import BackfillTask, ChangeType, ModelChange, Plan
from interlace.state.snapshot import ChangeCategory, Snapshot

pytestmark = pytest.mark.unit


def test_already_materialised_fingerprint_is_labelled_reuse() -> None:
    snapshot = Snapshot(
        name="orders",
        fingerprint="abc",
        metadata_hash="abc",
        physical_table=TableRef("main", "orders__abc"),
        change_category=ChangeCategory.BREAKING,
    )
    plan = Plan(
        environment="dev",
        changes=[ModelChange("orders", ChangeType.ADDED, None, None, "abc")],
        backfills=[BackfillTask(snapshot=snapshot, reuse_existing=True)],
    )
    text = plan_markdown(plan, "dev")
    assert "| `orders` | added | — | reuse |" in text
    assert plan.reused_names() == {"orders"}
