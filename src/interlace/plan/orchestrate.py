"""Plan-then-apply under the warehouse lock.

CLI, HTTP, and MCP each used to: resolve selectors → ``diff`` → ``annotate_plan``
→ ``hold_apply_lock`` → ``apply_with_registrations``. One helper owns the lock,
the breaking/blocking gates, and the follow-up dynamic-model build so those
three copies cannot drift.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

from interlace.dsl.dynamic import apply_with_registrations
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import BreakingPlanError, PlanError
from interlace.graph.project import CompiledProject
from interlace.graph.selectors import select_models, wants_state
from interlace.physical.annotate import annotate_plan
from interlace.plan.apply import ApplyResult, ProgressCallback
from interlace.plan.differ import diff
from interlace.plan.plan import Plan
from interlace.plan.run import run_plan
from interlace.project import Project
from interlace.state.locks import hold_apply_lock
from interlace.state.snapshot import ChangeCategory
from interlace.state.store import SqliteStateStore, StateStore

PrepareFn = Callable[[], Awaitable[None]]
PlanHook = Callable[[Plan], Awaitable[None] | None]
ResultHook = Callable[[ApplyResult], Awaitable[None] | None]


async def resolve_selection(
    compiled: CompiledProject,
    store: StateStore,
    environment: str,
    selectors: Sequence[str],
    *,
    default_all: bool = False,
) -> set[str] | None:
    """Resolve CLI/HTTP/MCP selectors. ``None`` means every model (``diff`` default)."""
    listed = list(selectors)
    if not listed:
        return set(compiled.models) if default_all else None
    promoted = await store.get_environment(environment) if wants_state(listed) else None
    return select_models(listed, compiled, promoted=promoted)


async def compute_plan(
    compiled: CompiledProject,
    environment: str,
    store: StateStore,
    engines: EngineRegistry,
    *,
    select: set[str] | None = None,
    forward_only: bool = False,
) -> Plan:
    """``diff`` plus live physical/drift annotation. Does not take the apply lock."""
    plan = await diff(compiled, environment, store, select=select, forward_only=forward_only)
    await annotate_plan(plan, compiled, engines)
    return plan


async def plan_and_apply(
    compiled: CompiledProject,
    *,
    environment: str,
    project: Project,
    engines: EngineRegistry,
    state: SqliteStateStore,
    lock_owner: str,
    selectors: Sequence[str] = (),
    select: set[str] | None = None,
    forward_only: bool = False,
    force: bool = False,
    parallelism: int | None = None,
    connections: Mapping[str, Any] | None = None,
    on_progress: ProgressCallback | None = None,
    on_compiled: Callable[[CompiledProject], None] | None = None,
    on_plan: PlanHook | None = None,
    on_start: PlanHook | None = None,
    on_finish: ResultHook | None = None,
    prepare: PrepareFn | None = None,
) -> tuple[Plan, ApplyResult | None]:
    """Lock, (re)compute the plan, refuse breaking/blocking, apply.

    Returns ``(plan, None)`` when the plan is empty. Breaking without ``force``
    raises :class:`BreakingPlanError`; blocking schema drift raises :class:`PlanError`.
    """
    chosen = select if select is not None else await resolve_selection(compiled, state, environment, selectors)

    async with hold_apply_lock(state, owner=lock_owner):
        if prepare is not None:
            await prepare()
        plan = await compute_plan(compiled, environment, state, engines, select=chosen, forward_only=forward_only)
        await _call_hook(on_plan, plan)
        _refuse_unsafe(plan, force=force)
        if plan.is_empty:
            return plan, None
        await _call_hook(on_start, plan)
        result = await apply_with_registrations(
            plan,
            compiled=compiled,
            project=project,
            engines=engines,
            state=state,
            base_path=project.root,
            parallelism=parallelism if parallelism is not None else project.config.parallelism,
            on_progress=on_progress,
            connections=connections if connections is not None else project.config.connections,
            on_compiled=on_compiled,
        )
        await _call_hook(on_finish, result)
        return plan, result


async def run_and_apply(
    compiled: CompiledProject,
    *,
    environment: str,
    project: Project,
    engines: EngineRegistry,
    state: SqliteStateStore,
    lock_owner: str,
    selectors: Sequence[str] = (),
    select: set[str] | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    restate: bool = False,
    parallelism: int | None = None,
    connections: Mapping[str, Any] | None = None,
    on_progress: ProgressCallback | None = None,
    on_compiled: Callable[[CompiledProject], None] | None = None,
    on_plan: PlanHook | None = None,
    on_start: PlanHook | None = None,
    on_finish: ResultHook | None = None,
    prepare: PrepareFn | None = None,
) -> tuple[Plan, ApplyResult]:
    """Lock, build a forced ``run_plan``, apply. No breaking-change gate."""
    chosen = select
    if chosen is None:
        chosen = await resolve_selection(compiled, state, environment, selectors, default_all=True)
    async with hold_apply_lock(state, owner=lock_owner):
        if prepare is not None:
            await prepare()
        plan = await run_plan(compiled, environment, state, start=start, end=end, select=chosen, restate=restate)
        await _call_hook(on_plan, plan)
        await _call_hook(on_start, plan)
        result = await apply_with_registrations(
            plan,
            compiled=compiled,
            project=project,
            engines=engines,
            state=state,
            base_path=project.root,
            parallelism=parallelism if parallelism is not None else project.config.parallelism,
            on_progress=on_progress,
            connections=connections if connections is not None else project.config.connections,
            on_compiled=on_compiled,
        )
        await _call_hook(on_finish, result)
        return plan, result


def _refuse_unsafe(plan: Plan, *, force: bool) -> None:
    if plan.blocking:
        raise PlanError("schema drift blocks apply: " + "; ".join(plan.blocking))
    if plan.has_breaking_changes and not force:
        names = [change.name for change in plan.changes if change.category is ChangeCategory.BREAKING]
        raise BreakingPlanError(names)


async def _call_hook(hook: Callable[..., Awaitable[None] | None] | None, argument: Any) -> None:
    if hook is None:
        return
    result = hook(argument)
    if result is not None:
        await result
