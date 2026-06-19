"""Plan/apply: diff models, classify changes, compute backfills, swap views."""

from __future__ import annotations

from interlace.plan.differ import diff, snapshot_of
from interlace.plan.plan import BackfillTask, ChangeType, ModelChange, Plan, TransferEdge, ViewSwap

__all__ = [
    "BackfillTask",
    "ChangeType",
    "ModelChange",
    "Plan",
    "TransferEdge",
    "ViewSwap",
    "diff",
    "snapshot_of",
]
