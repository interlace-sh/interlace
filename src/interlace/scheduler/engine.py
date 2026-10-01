"""The trigger engine — we own the scheduling loop.

Each tick, every trigger is asked what's due given when it last fired; due runs
are enqueued onto the durable work queue and the trigger's last-fired time is
persisted. State lives in our state DB (not an external scheduler's jobstore), so
it survives restarts and is unified with runs and snapshots.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

from interlace.engines.base import EngineAdapter, relation_is_absent
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import DefinitionError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.canonicalize import table_references
from interlace.ir.layout import env_view
from interlace.ir.relation import TableRef
from interlace.scheduler.triggers import (
    CronTrigger,
    FreshTrigger,
    IntervalTrigger,
    OnChangeTrigger,
    RunRequest,
    Trigger,
    WatchTrigger,
    change_query,
    fresh_query,
)
from interlace.state.interval import parse_grain
from interlace.state.store import SqliteStateStore

_SCHEDULE_KINDS = ("cron", "every", "watch", "on_change", "fresh", "webhook")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def schedule_kind(model: str, schedule: dict[str, str]) -> tuple[str, str]:
    """The one schedule key and its value. Cron, interval, file watch, table change, freshness, or webhook."""
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


def build_triggers(project: CompiledProject, *, root: Path | None = None, environment: str = "prod") -> list[Trigger]:
    """Construct triggers from each model's ``schedule`` config.

    ``root`` is the project directory a ``watch:`` glob is relative to.
    ``environment`` names the view an ``on_change`` or ``fresh`` schedule reads when its
    source is another model. Webhook schedules are validated here and enqueued
    by ``POST /hooks/{name}``, so they do not become ticking triggers.
    """
    triggers: list[Trigger] = []
    for model in project.models.values():
        trigger = _trigger_for(model, project, root, environment)
        if trigger is not None:
            triggers.append(trigger)
    webhook_targets(project)  # reject two models sharing one hook name
    return triggers


def _trigger_for(model: CompiledModel, project: CompiledProject, root: Path | None, environment: str) -> Trigger | None:
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
    if kind == "on_change":
        table, column, engine = resolve_on_change(model, project, environment, value)
        return OnChangeTrigger(model.name, column, table, engine)
    if kind == "fresh":
        column_spec, window = _fresh_parts(model.name, value)
        within = _fresh_window(model.name, window)
        table, column, engine = resolve_on_change(model, project, environment, column_spec, label="fresh")
        return FreshTrigger(model.name, column, table, engine, window, within)
    return None


def _fresh_parts(model: str, value: str) -> tuple[str, str]:
    column_spec, _, window = value.strip().rpartition(" ")
    if not column_spec.strip() or not window.strip():
        raise DefinitionError(f"model {model!r}: fresh {value!r} must look like 'updated_at 2h'")
    return column_spec.strip(), window.strip()


def _fresh_window(model: str, window: str) -> timedelta:
    try:
        return parse_grain(window)
    except ValueError as exc:
        raise DefinitionError(f"model {model!r}: {exc}") from exc


def resolve_on_change(
    model: CompiledModel, project: CompiledProject, environment: str, value: str, *, label: str = "on_change"
) -> tuple[TableRef, str, str]:
    """The table, column, and engine a column schedule probes.

    A bare column is read from the one table the model reads. ``table.column``
    and ``schema.table.column`` name it. A name that is a model is that model's
    environment view; anything else is a table on the model's own engine.
    """
    parts = value.split(".")
    if not parts or any(not _IDENT.match(part) for part in parts):
        raise DefinitionError(
            f"model {model.name!r}: {label} {value!r} must be a column, table.column, or schema.table.column"
        )
    column = parts[-1]
    if len(parts) == 1:
        table, engine = _infer_change_table(model, project, environment, label=label)
        return table, column, engine
    if len(parts) == 2:
        schema = None
        relation = parts[0]
    elif len(parts) == 3:
        schema, relation = parts[0], parts[1]
    else:
        raise DefinitionError(
            f"model {model.name!r}: {label} {value!r} must be a column, table.column, or schema.table.column"
        )
    qualified = f"{schema}.{relation}" if schema else relation
    if qualified in project.models:
        upstream = project.models[qualified]
        return env_view(environment, qualified), column, upstream.engine
    return TableRef(schema=schema or "main", name=relation), column, model.engine


def _infer_change_table(
    model: CompiledModel, project: CompiledProject, environment: str, *, label: str = "on_change"
) -> tuple[TableRef, str]:
    refs = table_references(model.ast) if model.ast is not None else []
    external = [ref for ref in refs if ref not in project.models]
    models = [ref for ref in refs if ref in project.models]
    if model.ast is None:
        external = []
        models = list(model.dependencies)
    if len(external) == 1 and not models:
        ref = external[0]
        schema, _, name = ref.rpartition(".")
        return TableRef(schema=schema or "main", name=name), model.engine
    if not external and len(models) == 1:
        upstream = project.models[models[0]]
        return env_view(environment, models[0]), upstream.engine
    raise DefinitionError(
        f"model {model.name!r}: {label} needs one source table; write table.column or schema.table.column",
        details={"reads": refs or list(model.dependencies)},
    )


def scheduled_closure(project: CompiledProject, models: list[str]) -> list[str]:
    """The triggered models plus every model downstream of them.

    A cron, interval, file watch, table change, freshness, or webhook means that model's inputs changed.
    Downstream snapshots would otherwise keep the previous build. An explicit
    ``run --select`` is not expanded here: ``model``, ``model+``, and ``+model``
    stay exactly what was asked for.
    """
    chosen: set[str] = set()
    for name in models:
        chosen.add(name)
        if name in project.models:
            chosen |= project.graph.descendants(name)
    return sorted(chosen)


class TriggerEngine:
    """Evaluates triggers on each tick and enqueues due runs."""

    def __init__(
        self,
        triggers: list[Trigger],
        store: SqliteStateStore,
        project: CompiledProject,
        *,
        engines: EngineRegistry | EngineAdapter | None = None,
    ) -> None:
        self.triggers = triggers
        self.store = store
        self.project = project
        self.engines = engines

    async def tick(self, now: datetime) -> int:
        """Enqueue all runs due at ``now``; returns how many were newly enqueued."""
        enqueued = 0
        for trigger in self.triggers:
            last_fired = await self.store.get_trigger_last_fired(trigger.id)
            requests = await self._due(trigger, now, last_fired)
            for request in requests:
                selector = scheduled_closure(self.project, request.flow_selector)
                partition = (
                    (request.partition.start.isoformat(), request.partition.end.isoformat())
                    if request.partition is not None
                    else None
                )
                if await self.store.enqueue_run(request.idempotency_key, selector, partition, request.priority):
                    enqueued += 1
                    await self.store.append_event(
                        "run.enqueued", entity=request.idempotency_key, payload={"models": selector}
                    )
            if requests:
                await self.store.set_trigger_last_fired(trigger.id, now)
        return enqueued

    async def _due(self, trigger: Trigger, now: datetime, last_fired: datetime | None) -> list[RunRequest]:
        if isinstance(trigger, OnChangeTrigger):
            watermark = await self._watermark(trigger)
            if watermark is None:
                return []
            return [trigger.request(watermark)]
        if isinstance(trigger, FreshTrigger):
            if not await self._is_stale(trigger):
                return []
            return [trigger.request(now)]
        return trigger.due(now, last_fired)

    async def _watermark(self, trigger: OnChangeTrigger) -> str | None:
        """``max(column)`` as text, or None when the source table is not there yet."""
        engine = self._engine(trigger.engine)
        if not await engine.table_exists(trigger.table):
            return None
        try:
            reader = await engine.fetch(change_query(trigger.table, trigger.column))
        except Exception as exc:
            if relation_is_absent(exc):
                return None
            raise
        rows = reader.read_all()
        if rows.num_rows == 0:
            return None
        value = rows.column(0)[0].as_py()
        if value is None:
            return "null"
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _engine(self, name: str) -> EngineAdapter:
        engines = self.engines
        if isinstance(engines, EngineRegistry):
            return engines.get(name)
        if isinstance(engines, EngineAdapter):
            return engines
        raise DefinitionError("a column schedule needs the project's engines")

    async def _is_stale(self, trigger: FreshTrigger) -> bool:
        """Whether ``max(column)`` is missing or older than the window.

        A source table that does not exist yet is not stale: the project may
        still be applying. An empty table is stale.
        """
        engine = self._engine(trigger.engine)
        if not await engine.table_exists(trigger.table):
            return False
        try:
            reader = await engine.fetch(fresh_query(trigger.table, trigger.column, trigger.window))
        except Exception as exc:
            if relation_is_absent(exc):
                return False
            raise
        rows = reader.read_all()
        if rows.num_rows == 0:
            return False
        return bool(rows.column(0)[0].as_py())
