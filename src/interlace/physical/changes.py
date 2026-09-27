"""Index, constraint, and external-drift facts the planner records on a plan.

These are physical observations, not planner decisions. ``plan`` imports them;
``physical`` does not import ``plan``.
"""

from __future__ import annotations

from dataclasses import dataclass

from interlace.physical.spec import PhysicalObject


@dataclass(frozen=True)
class PhysicalChange:
    """One index or constraint to add or drop. ``kind`` is ``index`` or ``constraint``."""

    op: str  # add | drop
    kind: str
    name: str


@dataclass(frozen=True)
class PhysicalAction:
    """Reconcile interlace-owned indexes and constraints on one model's table.

    ``standalone`` means the data fingerprint did not change, so apply runs this
    without rebuilding. Otherwise the build path applies it and these lines are
    what ``plan`` shows.
    """

    name: str
    standalone: bool
    previous: tuple[PhysicalObject, ...] = ()
    changes: tuple[PhysicalChange, ...] = ()
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class DriftNote:
    """Schema drift on an external table. Blocking notes fail the plan before any write."""

    model: str
    message: str
    blocking: bool = False
