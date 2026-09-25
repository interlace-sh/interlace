"""The trigger engine — we own the scheduling loop.

Each tick, every trigger is asked what's due given when it last fired; due runs
are enqueued onto the durable work queue and the trigger's last-fired time is
persisted. State lives in our state DB (not an external scheduler's jobstore), so
it survives restarts and is unified with runs and snapshots.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from interlace.exceptions import DefinitionError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.scheduler.triggers import CronTrigger, IntervalTrigger, Trigger, WatchTrigger
from interlace.state.interval import parse_grain
from interlace.state.store import SqliteStateStore

_SCHEDULE_KINDS = ("cron", "every", "watch", "webhook")


def schedule_kind(model: str, schedule: dict[str, str]) -> tuple[str, str]:
    """The one schedule key and its value. Cron, interval, file watch, or webhook."""
    present = [key for key in _SCHEDULE_KINDS if key in schedule]
    if len(present) != 1:
        raise DefinitionError(
            f"model {model!r}: schedule needs exactly one of {', '.join(_SCHEDULE_KINDS)}",
            details={"schedule": schedule},
        )
    return present[0], schedule[present[0]]


def webhook_targets(project: CompiledProject) -> dict[str, str]:
    """Map a webhook name to the model ``POST /hooks/{name}`` enqueues."""
    found: dict[str, str] = {}
    for model in project.models.values():
        if not model.schedule or "webhook" not in model.schedule:
            continue
        kind, name = schedule_kind(model.name, model.schedule)
        if kind != "webhook":
            continue
        other = found.get(name)
        if other is not None:
            raise DefinitionError(f"webhook {name!r} is declared by both {other!r} and {model.name!r}")
        found[name] = model.name
    return found


def build_triggers(project: CompiledProject, *, root: Path | None = None) -> list[Trigger]:
    """Construct triggers from each model's ``schedule`` config.

    ``root`` is the project directory a ``watch:`` glob is relative to. Webhook
    schedules are validated here and enqueued by ``POST /hooks/{name}``, so they
    do not become ticking triggers.
    """
    triggers: list[Trigger] = []
    for model in project.models.values():
        trigger = _trigger_for(model, root)
        if trigger is not None:
            triggers.append(trigger)
    webhook_targets(project)  # reject two models sharing one hook name
    return triggers


def _trigger_for(model: CompiledModel, root: Path | None) -> Trigger | None:
    schedule = model.schedule
    if not schedule:
        return None
    kind, value = schedule_kind(model.name, schedule)
    if kind == "cron":
        return CronTrigger(model.name, value)
    if kind == "every":
        return IntervalTrigger(model.name, parse_grain(value))
    if kind == "watch":
        if root is None:
            raise DefinitionError(f"model {model.name!r}: a watch schedule needs the project root")
        return WatchTrigger(model.name, value, root)
    return None


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
