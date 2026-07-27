"""The plan/apply data model.

``interlace plan`` produces a :class:`Plan`: the classified set of model changes,
the exact ``(snapshot, interval)`` work needed to back them, any explicit
cross-engine transfers, and the virtual-view repointing that promotes the result.
Apply executes it; nothing here runs SQL.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

from interlace.ir.relation import EngineRef, TableRef
from interlace.state.interval import Interval
from interlace.state.snapshot import ChangeCategory, Snapshot

if TYPE_CHECKING:
    from interlace.graph.project import CompiledModel, CompiledProject


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
    # Forward-only: copy this table into the snapshot's (new) physical table before
    # the strategy runs — history moves to the new fingerprint, the old table stays
    # as the rollback until gc.
    seed_from: TableRef | None = None


@dataclass(frozen=True)
class TransferEdge:
    """Explicit cross-engine data movement, surfaced in plan output (never silent)."""

    source: EngineRef
    target: EngineRef
    table: TableRef  # the staging table on the target engine
    via: str  # "arrow" (generic fetch->load) | "attach" (future optimisation)
    model: str = ""  # the upstream model being moved


XFER_SCHEMA = "interlace__xfer"


def staging_table(upstream: str) -> TableRef:
    """Where a transferred upstream lands on the consumer's engine (replaced on every transfer)."""
    return TableRef(schema=XFER_SCHEMA, name=upstream.replace(".", "__"))


def collect_transfers(compiled: CompiledProject, build_names: Iterable[str]) -> list[TransferEdge]:
    """One edge per (upstream, target engine) needed by the scheduled builds."""
    edges: dict[tuple[str, str], TransferEdge] = {}
    for name in build_names:
        model = compiled.models[name]
        for dep in model.dependencies:
            upstream = compiled.models[dep]
            if upstream.engine == model.engine or upstream.materialise == "ephemeral":
                continue
            key = (dep, model.engine)
            if key not in edges:
                edges[key] = TransferEdge(
                    source=EngineRef(name=upstream.engine, dialect=upstream.dialect),
                    target=EngineRef(name=model.engine, dialect=model.dialect),
                    table=staging_table(dep),
                    via="arrow",
                    model=dep,
                )
    return list(edges.values())


@dataclass(frozen=True)
class ViewSwap:
    """Repoint an environment view at a (new) physical snapshot table."""

    view: TableRef
    target: TableRef
    engine: str = "default"  # named engine that hosts the view


PRODUCTION_ENV = "prod"
"""The production environment lives at the *unprefixed* schema (``main.orders``):
that's what BI tools and consumers connect to. Every other environment is a
prefixed sandbox (``dev__main.orders``) over the same physical snapshots."""


def env_view(environment: str, model_name: str) -> TableRef:
    """The virtual-environment view for a model: ``<schema>.<model>`` in
    production, ``<env>__<schema>.<model>`` everywhere else."""
    schema, _, base = model_name.rpartition(".")
    prefix = "" if environment == PRODUCTION_ENV else f"{environment}__"
    return TableRef(schema=f"{prefix}{schema or 'main'}", name=base)


def schedule_build(
    plan: Plan, model: CompiledModel, snapshot: Snapshot, environment: str, *, seed_from: TableRef | None = None
) -> None:
    """Add the right tasks for a model: ephemeral builds nothing; a sink builds but
    gets no view; a table/view builds and is repointed by an environment view.

    An incremental_by_time model cannot build without a window, so an apply fills
    the latest grain interval — the same default as ``interlace run`` — leaving
    history to ``run --start/--end``. A forward-only snapshot's inherited ledger
    skips windows it already carries.
    """
    if model.materialise == "ephemeral":  # inlined into consumers, never built
        return
    if model.strategy == "incremental_by_time" and model.export is None:
        from datetime import datetime

        from interlace.state.interval import parse_grain

        grain = parse_grain(model.interval or "1d")
        now = datetime.now()
        window = Interval(now - grain, now)
        if not snapshot.intervals.covers(window):
            plan.backfills.append(BackfillTask(snapshot=snapshot, interval=window, seed_from=seed_from))
        plan.virtual_updates.append(
            ViewSwap(env_view(environment, model.name), snapshot.physical_table, engine=model.engine)
        )
        return
    plan.backfills.append(BackfillTask(snapshot=snapshot, seed_from=seed_from))
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
