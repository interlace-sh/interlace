"""Forced / windowed execution — the plan behind ``interlace run``.

Unlike ``diff`` (which schedules only models whose fingerprint changed), a run
rebuilds every model regardless of change detection. Incremental_by_time models
are expanded over a time window into one task per grain interval, skipping
intervals already recorded in the ledger (catchup); non-incremental models get a
single full task. Because unchanged SQL keeps the same fingerprint (and physical
table), a full model is replaced and a merge model upserts — both pick up new
source data.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from interlace.graph.project import CompiledProject
from interlace.plan.differ import snapshot_of
from interlace.plan.plan import (
    BackfillTask,
    ChangeType,
    ModelChange,
    Plan,
    ViewSwap,
    collect_transfers,
    env_view,
    schedule_build,
)
from interlace.state.interval import Interval, parse_grain, slice_interval
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import StateStore


def _window(start: datetime | None, end: datetime | None, grain: timedelta) -> Interval:
    finish = end or datetime.now()
    begin = start or (finish - grain)
    return Interval(begin, finish)


async def run_plan(
    compiled: CompiledProject,
    environment: str,
    state: StateStore,
    *,
    start: datetime | None = None,
    end: datetime | None = None,
    select: set[str] | None = None,
    restate: bool = False,
) -> Plan:
    """Build a forced plan; incremental models are expanded over ``[start, end)``.

    ``select`` limits which models run (None = all). ``restate`` reprocesses every
    interval in the window instead of skipping the ones already filled (catchup).
    """
    selected = set(compiled.models) if select is None else select
    if select is not None:
        # a selected model must never resolve an upstream fingerprint that was
        # never materialised: pull changed ancestors into the run (same rule as diff)
        from interlace.plan.differ import _expand_to_changed_ancestors

        selected = _expand_to_changed_ancestors(compiled, selected, await state.get_environment(environment))
    plan = Plan(environment=environment)
    for model in compiled.ordered():
        if model.name not in selected:
            continue
        plan.changes.append(ModelChange(model.name, ChangeType.MODIFIED, None, None, model.fingerprint))

        is_incremental = (
            model.strategy == "incremental_by_time" and model.export is None and model.materialise != "ephemeral"
        )
        if is_incremental:
            grain = parse_grain(model.interval or "1d")
            filled = await state.get_intervals(model.name, model.fingerprint)
            snapshot = snapshot_of(model, ChangeCategory.BREAKING)
            if start is None and end is None and not len(filled) and model.backfill != "none":
                # nothing filled yet and no window given: bootstrap — apply derives
                # the source's time-column range and fills it as one interval
                plan.backfills.append(BackfillTask(snapshot=snapshot, bootstrap=True))
                plan.virtual_updates.append(
                    ViewSwap(env_view(environment, model.name), model.physical_table, engine=model.engine)
                )
                continue
            if start is None and end is None:
                window_hint = _window(start, end, grain)
                plan.warnings.append(
                    f"{model.name}: no --start/--end given — only the most recent "
                    f"{model.interval or '1d'} window ({window_hint.start:%Y-%m-%d %H:%M} → "
                    f"{window_hint.end:%Y-%m-%d %H:%M}) is considered; historical data needs an explicit window"
                )
            for window in slice_interval(_window(start, end, grain), grain):
                if restate or not filled.covers(window):  # restate reprocesses; otherwise catch up
                    plan.backfills.append(BackfillTask(snapshot=snapshot, interval=window))
            plan.virtual_updates.append(
                ViewSwap(env_view(environment, model.name), model.physical_table, engine=model.engine)
            )
        else:
            schedule_build(plan, model, snapshot_of(model, ChangeCategory.BREAKING), environment)

    plan.promote = sorted(selected)
    plan.transfers = collect_transfers(compiled, [task.snapshot.name for task in plan.backfills])
    return plan
