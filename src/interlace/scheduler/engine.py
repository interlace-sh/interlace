"""The trigger engine — we own the scheduling loop.

Each tick, every trigger is asked what's due given when it last fired; due runs
are enqueued onto the durable work queue and the trigger's last-fired time is
persisted. State lives in our state DB (not an external scheduler's jobstore), so
it survives restarts and is unified with runs and snapshots.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import cast

from sqlglot import exp

from interlace.engines.base import EngineAdapter, relation_is_absent
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import DefinitionError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.canonicalize import table_references
from interlace.ir.layout import env_view
from interlace.ir.relation import TableRef
from interlace.scheduler.completion import after_waiters, schedule_kind, scheduled_closure
from interlace.scheduler.triggers import (
    ABSENT,
    CronTrigger,
    FreshTrigger,
    IntervalTrigger,
    OnChangeTrigger,
    Trigger,
    WatchTrigger,
)
from interlace.state.interval import Grain, as_grain, parse_grain
from interlace.state.store import SqliteStateStore

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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
    by ``POST /hooks/{name}``. ``after`` is validated here and enqueued when the
    upstream model finishes. Neither becomes a ticking trigger.
    """
    triggers: list[Trigger] = []
    for model in project.models.values():
        trigger = _trigger_for(model, project, root, environment)
        if trigger is not None:
            triggers.append(trigger)
    webhook_targets(project)  # reject two models sharing one hook name
    after_waiters(project)  # reject unknown names and cycles
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
        table, column, engine = resolve_column(model, project, environment, value)
        return OnChangeTrigger(model.name, column, table, engine)
    if kind == "fresh":
        column_spec, window = _fresh_parts(model.name, value)
        table, column, engine = resolve_column(model, project, environment, column_spec, label="fresh")
        return FreshTrigger(model.name, column, table, engine, _fresh_grain(model.name, window))
    return None


def _fresh_parts(model: str, value: str) -> tuple[str, str]:
    column_spec, _, window = value.strip().rpartition(" ")
    if not column_spec.strip() or not window.strip():
        raise DefinitionError(f"model {model!r}: fresh {value!r} must look like 'updated_at 2h'")
    return column_spec.strip(), window.strip()


def _fresh_grain(model: str, window: str) -> Grain:
    try:
        return as_grain(window)
    except ValueError as exc:
        raise DefinitionError(f"model {model!r}: {exc}") from exc


def resolve_column(
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
            requests = await trigger.due(now, last_fired, self)
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

    async def scalar(self, engine: str, table: TableRef, query: exp.Expression) -> object | None:
        """First cell of ``query``. ``ABSENT`` when the table is not there yet; ``None`` when the cell is NULL."""
        adapter = self._engine(engine)
        if not await adapter.table_exists(table):
            return ABSENT
        try:
            reader = await adapter.fetch(query)
        except Exception as exc:
            if relation_is_absent(exc):
                return ABSENT
            raise
        rows = reader.read_all()
        if rows.num_rows == 0:
            return ABSENT
        return cast(object | None, rows.column(0)[0].as_py())

    def _engine(self, name: str) -> EngineAdapter:
        engines = self.engines
        if isinstance(engines, EngineRegistry):
            return engines.get(name)
        if isinstance(engines, EngineAdapter):
            return engines
        raise DefinitionError("a column schedule needs the project's engines")
