"""Forced execution — the plan behind ``interlace run``.

Unlike ``diff`` (which only schedules models whose fingerprint changed), a forced
run rebuilds every model regardless of change detection. Because it rebuilds at
the *same* fingerprint when the SQL is unchanged, the physical table is the same
across runs — so a ``full`` model is replaced and a ``merge_by_key`` model
upserts, letting both pick up new source data. This is the CLI precursor to
scheduler-driven runs.
"""

from __future__ import annotations

from interlace.graph.project import CompiledProject
from interlace.plan.differ import snapshot_of
from interlace.plan.plan import BackfillTask, ChangeType, ModelChange, Plan, ViewSwap, env_view
from interlace.state.snapshot import ChangeCategory


def forced_plan(compiled: CompiledProject, environment: str) -> Plan:
    """A plan that rebuilds and repoints every model, in dependency order."""
    plan = Plan(environment=environment)
    for model in compiled.ordered():
        plan.changes.append(ModelChange(model.name, ChangeType.MODIFIED, None, None, model.fingerprint))
        plan.backfills.append(BackfillTask(snapshot=snapshot_of(model, ChangeCategory.BREAKING)))
        plan.virtual_updates.append(ViewSwap(env_view(environment, model.name), model.physical_table))
    return plan
