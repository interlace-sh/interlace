"""Plan/apply: diff models, classify changes, compute backfills, swap views."""

from __future__ import annotations

from interlace.plan.apply import ApplyResult, apply
from interlace.plan.differ import diff, snapshot_of
from interlace.plan.plan import BackfillTask, ChangeType, ModelChange, Plan, TransferEdge, ViewSwap

__all__ = [
    "ApplyResult",
    "BackfillTask",
    "ChangeType",
    "ModelChange",
    "Plan",
    "TransferEdge",
    "ViewSwap",
    "apply",
    "diff",
    "snapshot_of",
]
