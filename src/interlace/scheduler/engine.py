"""The trigger engine — we own the scheduling loop.

Each tick, every trigger is asked what's due given when it last fired; due runs
are enqueued onto the durable work queue and the trigger's last-fired time is
persisted. State lives in our state DB (not an external scheduler's jobstore), so
it survives restarts and is unified with runs and snapshots.
"""

from __future__ import annotations

from datetime import datetime

from interlace.exceptions import DefinitionError
from interlace.graph.project import CompiledProject
from interlace.scheduler.triggers import CronTrigger, IntervalTrigger, Trigger
from interlace.state.interval import parse_grain
from interlace.state.store import SqliteStateStore


def build_triggers(project: CompiledProject) -> list[Trigger]:
    """Construct triggers from each model's ``schedule`` config."""
    triggers: list[Trigger] = []
    for model in project.models.values():
        schedule = model.schedule
        if not schedule:
            continue
        if "cron" in schedule:
            triggers.append(CronTrigger(model.name, schedule["cron"]))
        elif "every" in schedule:
            triggers.append(IntervalTrigger(model.name, parse_grain(schedule["every"])))
        else:
            raise DefinitionError(
                f"model {model.name!r}: schedule needs 'cron' or 'every'", details={"schedule": schedule}
            )
    return triggers


class TriggerEngine:
    """Evaluates triggers on each tick and enqueues due runs."""

    def __init__(self, triggers: list[Trigger], store: SqliteStateStore) -> None:
        self.triggers = triggers
        self.store = store

    async def tick(self, now: datetime) -> int:
        """Enqueue all runs due at ``now``; returns how many were newly enqueued."""
        enqueued = 0
        for trigger in self.triggers:
            last_fired = await self.store.get_trigger_last_fired(trigger.id)
            requests = trigger.due(now, last_fired)
            for request in requests:
                partition = (
                    (request.partition.start.isoformat(), request.partition.end.isoformat())
                    if request.partition is not None
                    else None
                )
                if await self.store.enqueue_run(
                    request.idempotency_key, request.flow_selector, partition, request.priority
                ):
                    enqueued += 1
                    await self.store.append_event(
                        "run.enqueued", entity=request.idempotency_key, payload={"models": request.flow_selector}
                    )
            if requests:
                await self.store.set_trigger_last_fired(trigger.id, now)
        return enqueued
