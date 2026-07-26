"""The plan/apply data model.

``interlace plan`` produces a :class:`Plan`: the classified set of model changes,
the exact ``(snapshot, interval)`` work needed to back them, any explicit
cross-engine transfers, and the virtual-view repointing that promotes the result.
Apply executes it; nothing here runs SQL.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from interlace.ir.relation import EngineRef, TableRef
from interlace.state.interval import Interval
from interlace.state.snapshot import ChangeCategory, Snapshot

if TYPE_CHECKING:
    from interlace.graph.project import CompiledModel


class ChangeType(Enum):
    """A model's status relative to the target environment."""

    ADDED = "added"
    MODIFIED = "modified"
    REMOVED = "removed"
    UNCHANGED = "unchanged"


@dataclass(frozen=True)
class ModelChange:
    """A model added, modified, or removed relative to the target environment."""

    name: str
    change_type: ChangeType
    category: ChangeCategory | None  # severity of a MODIFIED change; None for added/removed
    previous_fingerprint: str | None
    new_fingerprint: str | None
    impacted_columns: tuple[str, ...] = ()  # columns whose lineage changed (drives narrowing)


@dataclass(frozen=True)
class BackfillTask:
    """One unit of physical work: build ``snapshot`` (optionally for one ``interval``)."""

    snapshot: Snapshot
    interval: Interval | None = None  # None = full refresh (non-incremental models)


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
    engine: str = "default"  # named engine that hosts the view


def env_view(environment: str, model_name: str) -> TableRef:
    """The virtual-environment view name for a model: ``<env>__<schema>.<model>``."""
    schema, _, base = model_name.rpartition(".")
    return TableRef(schema=f"{environment}__{schema or 'main'}", name=base)


def schedule_build(plan: Plan, model: CompiledModel, snapshot: Snapshot, environment: str) -> None:
    """Add the right tasks for a model: ephemeral builds nothing; a sink builds but
    gets no view; a table/view builds and is repointed by an environment view."""
    if model.materialise == "ephemeral":  # inlined into consumers, never built
        return
    plan.backfills.append(BackfillTask(snapshot=snapshot))
    if model.export is None and model.materialise in ("table", "view"):  # sinks have no view
        # the snapshot's table, not the fingerprint-derived one: a forward-only
        # snapshot builds into (and the view must point at) its inherited table
        plan.virtual_updates.append(
            ViewSwap(env_view(environment, model.name), snapshot.physical_table, engine=model.engine)
        )


@dataclass
class Plan:
    """The full preview of an apply against one environment."""

    environment: str
    changes: list[ModelChange] = field(default_factory=list)
    backfills: list[BackfillTask] = field(default_factory=list)
    transfers: list[TransferEdge] = field(default_factory=list)
    virtual_updates: list[ViewSwap] = field(default_factory=list)
    promote: list[str] = field(default_factory=list)  # model names whose fingerprints to promote
    # Indirectly-changed models whose output is provably identical: their new
    # snapshot points at the previous physical table — recorded, never rebuilt.
    reuses: list[Snapshot] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.changes or self.backfills or self.virtual_updates)

    @property
    def has_breaking_changes(self) -> bool:
        return any(c.category is ChangeCategory.BREAKING for c in self.changes)
