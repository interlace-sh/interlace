"""Plan/apply: diff models, classify changes, compute backfills, swap views."""

from __future__ import annotations

from interlace.plan.apply import ApplyResult, apply
from interlace.plan.differ import diff, snapshot_of
from interlace.plan.orchestrate import compute_plan, plan_and_apply, run_and_apply
from interlace.plan.plan import BackfillTask, ChangeType, ModelChange, Plan, TransferEdge, ViewSwap, env_view
from interlace.plan.run import run_plan

__all__ = [
    "ApplyResult",
    "BackfillTask",
    "ChangeType",
    "ModelChange",
    "Plan",
    "TransferEdge",
    "ViewSwap",
    "apply",
    "compute_plan",
    "diff",
    "env_view",
    "plan_and_apply",
    "run_and_apply",
    "run_plan",
    "snapshot_of",
]
