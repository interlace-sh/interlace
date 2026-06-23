"""Plan/apply: diff models, classify changes, compute backfills, swap views."""

from __future__ import annotations

from interlace.plan.apply import ApplyResult, apply
from interlace.plan.differ import diff, snapshot_of
from interlace.plan.plan import BackfillTask, ChangeType, ModelChange, Plan, TransferEdge, ViewSwap, env_view
from interlace.plan.run import forced_plan

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
    "env_view",
    "forced_plan",
    "snapshot_of",
]
