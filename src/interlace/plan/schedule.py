"""DAG scheduling for one apply.

Each model starts when its in-plan ancestors finish. Check edges that would
cycle are dropped; every other cross-model check waits. Promotion is not here.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp

from interlace.connections import bind_connections, unbind_connections
from interlace.dsl.dynamic import capturing_registrations
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import ExecutionError, InterlaceError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import TableRef
from interlace.plan.backfill import _run_backfill
from interlace.plan.plan import BackfillTask, Plan
from interlace.plan.result import ApplyResult, ProgressCallback, _build_detail, _failure_detail
from interlace.state.store import StateStore

logger = logging.getLogger("interlace.apply")


def _check_references(model: CompiledModel, compiled: CompiledProject) -> set[str]:
    """Other models a check reads (``relationships`` targets, tables in ``sql``
    checks): they must be built before this model's checks can run. Table refs
    are matched to models by their ``db.name`` key or bare name, the same way
    dependencies resolve — so a check reading ``raw.orders`` finds model
    ``raw.orders`` (or ``orders``), not nothing."""

    def _model_for(key: str) -> str | None:
        if key in compiled.models:
            return key
        tail = key.rsplit(".", 1)[-1]
        return tail if tail in compiled.models else None

    refs: set[str] = set()
    for spec in model.checks:
        if spec.type == "relationships":
            match = _model_for(str(spec.params.get("to", "")))
            if match:
                refs.add(match)
        elif spec.type == "sql":
            with contextlib.suppress(Exception):
                parsed = sqlglot.parse_one(str(spec.params.get("query", "")))
                for table in parsed.find_all(exp.Table):
                    match = _model_for(f"{table.db}.{table.name}" if table.db else table.name)
                    if match:
                        refs.add(match)
    return refs


async def schedule_builds(  # noqa: C901
    plan: Plan,
    compiled: CompiledProject,
    registry: EngineRegistry,
    physical: dict[str, TableRef],
    state: StateStore,
    base_path: Path | None,
    result: ApplyResult,
    parallelism: int,
    on_progress: ProgressCallback | None,
    connections: Mapping[str, Any] | None,
) -> None:
    """Build every backfill. Sets ``result.registered`` from models created mid-run."""
    # True DAG scheduling: every model starts the moment its last in-plan
    # ancestor finishes — no level barriers, so one slow branch never stalls an
    # independent one; wall-clock tracks the critical path. Tasks still group
    # per model (a model's interval windows stay ordered) and a semaphore bounds
    # concurrency. Waiting happens BEFORE the semaphore, so a blocked model
    # never holds a build slot its own upstream needs.
    staged: set[tuple[str, str]] = set()  # (upstream, target engine) pairs moved this apply
    stage_lock = asyncio.Lock()
    per_model: dict[str, list[BackfillTask]] = {}
    for task in plan.backfills:  # differ/run emit tasks in topological order
        per_model.setdefault(task.snapshot.name, []).append(task)
    # Blocking edges project the dependency graph onto the models building now,
    # walking THROUGH models that aren't (ephemeral, reused): a Python model over
    # an ephemeral view of a building table must still wait for that table.
    data_deps = {model.name: set(model.dependencies) - {model.name} for model in compiled.models.values()}

    def in_plan_ancestors(name: str) -> set[str]:
        """The building models this model must wait for, following DATA edges only
        (through non-building intermediaries). Data edges are acyclic, so this is
        always safe to enforce."""
        found: set[str] = set()
        seen = {name}
        stack = list(data_deps.get(name, ()))
        while stack:
            dep = stack.pop()
            if dep in seen:
                continue
            seen.add(dep)
            if dep in per_model:
                found.add(dep)
            else:  # not building: look through it
                stack.extend(data_deps.get(dep, ()))
        return found

    # Checks that read *other* models (relationships, sql) add scheduling edges the
    # data DAG doesn't have. Keep one unless it would close a cycle — which it does
    # exactly when the reader is already an ancestor of what its check reads (a check
    # pointing downstream, e.g. order_items testing against orders, which is built
    # from order_items). Those are dropped: the check runs before its target exists
    # and fails, but a cycle would hang the whole apply, and the model can point its
    # check at an upstream instead. Everything else is a plain sibling reference and
    # is enforced — dropping those on a topological-order heuristic failed checks
    # whose target simply happened to sort later.
    blocking: dict[str, set[str]] = {}
    for name in per_model:
        deps = in_plan_ancestors(name)
        for ref in _check_references(compiled.models[name], compiled):
            if ref in per_model and name not in compiled.graph.ancestors(ref):
                deps.add(ref)
        blocking[name] = deps

    finished = {name: asyncio.Event() for name in per_model}
    build_slots = asyncio.Semaphore(max(1, parallelism))

    async def run_model(name: str) -> None:  # noqa: C901
        try:
            for dep in blocking[name]:
                await finished[dep].wait()
            async with build_slots:
                if on_progress is not None:
                    on_progress(name, "start", {})
                for model_task in per_model[name]:
                    await _run_backfill(
                        model_task, plan, compiled, registry, physical, staged, stage_lock, state, base_path, result
                    )
        except asyncio.CancelledError:  # a SIBLING failed; this model is collateral
            if on_progress is not None:
                on_progress(name, "cancelled", {})
            raise
        except BaseException as exc:
            # Name the failing model as live feedback; the full message is surfaced once
            # by the caller (the CLI prints it, the API returns it) — don't duplicate it here.
            logger.warning("model %s failed (%s)", name, type(exc).__name__)
            detail = _failure_detail(exc)
            if on_progress is not None:
                on_progress(name, "failed", detail)
            # Wrap a plain build error (engine/SQL/Python-model exception) so it reads as one
            # clean "error: model … failed: …" line, not a raw traceback. InterlaceErrors
            # (checks, contracts) already carry a good message; other BaseExceptions
            # (KeyboardInterrupt, CancelledError) must propagate untouched.
            if isinstance(exc, Exception) and not isinstance(exc, InterlaceError):
                wrapped = {"model": name}
                if "statement" in detail:
                    wrapped["statement"] = detail["statement"]
                raise ExecutionError(f"model {name!r} failed: {detail['message']}", details=wrapped) from exc
            if isinstance(exc, InterlaceError) and "statement" in detail:
                exc.details.setdefault("statement", detail["statement"])
            raise
        finished[name].set()
        if on_progress is not None:
            on_progress(name, "done", _build_detail(result, name))

    # Copied into each build task at creation, so a Python model that registers
    # models (including from a worker thread) appends to this same list.
    with capturing_registrations() as batch:
        bound = bind_connections(connections)
        try:
            try:
                async with asyncio.TaskGroup() as group:
                    for name in per_model:
                        group.create_task(run_model(name))
            except ExceptionGroup as failures:  # single failure keeps apply()'s plain-exception contract
                if len(failures.exceptions) == 1:
                    # re-raise the plain single exception, preserving its own cause (the build error,
                    # kept for --debug) rather than re-chaining the ExceptionGroup
                    failure = failures.exceptions[0]
                    raise failure from failure.__cause__
                raise
        finally:
            unbind_connections(bound)
    result.registered = list(batch)
