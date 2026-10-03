"""Run a model when another model finishes.

``schedule: {after: …}`` is not a clock tick. Apply calls :func:`wake_after` once
a build reaches ``model.done``. The trigger engine only validates the graph.
"""

from __future__ import annotations

import time

from interlace.exceptions import DefinitionError
from interlace.graph.project import CompiledProject
from interlace.state.store import SqliteStateStore

_SCHEDULE_KINDS = ("cron", "every", "watch", "on_change", "fresh", "after", "webhook")


def schedule_kind(model: str, schedule: dict[str, str]) -> tuple[str, str]:
    """The one schedule key and its value.

    Cron, interval, file watch, table change, freshness, upstream completion, or webhook.
    """
    present = [key for key in _SCHEDULE_KINDS if key in schedule]
    if len(present) != 1:
        raise DefinitionError(
            f"model {model!r}: schedule needs exactly one of {', '.join(_SCHEDULE_KINDS)}",
            details={"schedule": schedule},
        )
    return present[0], schedule[present[0]]


def scheduled_closure(project: CompiledProject, models: list[str]) -> list[str]:
    """The triggered models plus every model downstream of them.

    A cron, interval, file watch, table change, freshness, upstream completion, or webhook
    means that model's inputs changed. Downstream snapshots would otherwise keep the
    previous build. An explicit ``run --select`` is not expanded here: ``model``,
    ``model+``, and ``+model`` stay exactly what was asked for.
    """
    chosen: set[str] = set()
    for name in models:
        chosen.add(name)
        if name in project.models:
            chosen |= project.graph.descendants(name)
    return sorted(chosen)


def after_waiters(project: CompiledProject) -> dict[str, list[str]]:
    """Map an upstream model to the models that run when it finishes.

    ``schedule: {after: raw}`` or ``{after: [raw, staging]}`` (stored as
    ``"raw,staging"``). A model cannot name itself, and the ``after`` edges
    cannot cycle — otherwise two models would enqueue each other forever.
    """
    found: dict[str, list[str]] = {}
    for model in project.models.values():
        if not model.schedule or "after" not in model.schedule:
            continue
        kind, value = schedule_kind(model.name, model.schedule)
        if kind != "after":
            continue
        names = list(dict.fromkeys(part.strip() for part in value.split(",") if part.strip()))
        if not names:
            raise DefinitionError(f"model {model.name!r}: after must name a model")
        if model.name in names:
            raise DefinitionError(f"model {model.name!r}: after cannot name itself")
        missing = [name for name in names if name not in project.models]
        if missing:
            raise DefinitionError(
                f"model {model.name!r}: after names unknown models: {', '.join(missing)}",
                details={"missing": missing},
            )
        for name in names:
            found.setdefault(name, []).append(model.name)
    _reject_after_cycles(found)
    return found


def _reject_after_cycles(waiters: dict[str, list[str]]) -> None:
    visiting: set[str] = set()
    visited: set[str] = set()

    def walk(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise DefinitionError(f"schedule after has a cycle through {node!r}")
        visiting.add(node)
        for nxt in waiters.get(node, ()):
            walk(nxt)
        visiting.remove(node)
        visited.add(node)

    for node in list(waiters):
        walk(node)


async def wake_after(
    store: SqliteStateStore,
    project: CompiledProject,
    upstream: str,
    *,
    building: set[str],
    waiters: dict[str, list[str]] | None = None,
    stamp: str | None = None,
) -> int:
    """Enqueue models waiting on ``upstream`` after it reaches ``model.done``.

    ``building`` is the set already in this apply, so a model the plan is
    constructing is not queued again. One stamp shared by the waiters of a
    single finish: a retry of the same finish dedupes, a later finish does not.
    Returns how many runs were newly enqueued.
    """
    table = after_waiters(project) if waiters is None else waiters
    pending = [name for name in table.get(upstream, ()) if name not in building]
    if not pending:
        return 0
    order = {model.name: index for index, model in enumerate(project.ordered())}
    pending.sort(key=lambda name: order.get(name, 0))
    token = stamp if stamp is not None else str(time.time_ns())
    covered: set[str] = set(building)
    enqueued = 0
    for name in pending:
        if name in covered:
            continue
        selector = scheduled_closure(project, [name])
        key = f"after:{name}:{upstream}:{token}"
        if await store.enqueue_run(key, selector, None, 0):
            enqueued += 1
            await store.append_event("run.enqueued", entity=key, payload={"models": selector, "after": upstream})
        covered.update(selector)
    return enqueued
