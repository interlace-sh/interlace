"""The plan/apply data model.

``interlace plan`` produces a :class:`Plan`: the classified set of model changes,
the exact ``(snapshot, interval)`` work needed to back them, any explicit
cross-engine transfers, and the virtual-view repointing that promotes the result.
Apply executes it; nothing here runs SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from interlace.ir.relation import EngineRef, TableRef
from interlace.state.interval import Interval
from interlace.state.snapshot import ChangeCategory, Snapshot


@dataclass(frozen=True)
class ModelChange:
    """A model added, modified, or removed relative to the target environment."""

    name: str
    category: ChangeCategory
    previous_fingerprint: str | None
    new_fingerprint: str | None
    impacted_columns: tuple[str, ...] = ()  # columns whose lineage changed (drives narrowing)


@dataclass(frozen=True)
class BackfillTask:
    """One unit of physical work: fill ``interval`` for ``snapshot``."""

    snapshot: Snapshot
    interval: Interval


@dataclass(frozen=True)
class TransferEdge:
    """Explicit cross-engine data movement, surfaced in plan output (never silent)."""

    source: EngineRef
    target: EngineRef
    table: TableRef
    via: str  # "attach" | "adbc"


@dataclass(frozen=True)
class ViewSwap:
    """Repoint an environment view at a (new) physical snapshot table."""

    view: TableRef
    target: TableRef


@dataclass
class Plan:
    """The full preview of an apply against one environment."""

    environment: str
    changes: list[ModelChange] = field(default_factory=list)
    backfills: list[BackfillTask] = field(default_factory=list)
    transfers: list[TransferEdge] = field(default_factory=list)
    virtual_updates: list[ViewSwap] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.changes or self.backfills or self.virtual_updates)

    @property
    def has_breaking_changes(self) -> bool:
        return any(c.category is ChangeCategory.BREAKING for c in self.changes)
