"""Run a model's checks against its freshly built physical table.

Declarative checks compile to one ``failures``-count query each and run on the
engine. Python ``@check`` functions receive a :class:`RelationHandle` over the
built table and pass by returning a truthy success (``True``/``0``) — return
``False`` or a failure count to fail. A check that itself crashes is recorded
as ``error`` status (an engine problem, not a data-quality verdict).
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa
from sqlglot import exp

from interlace.checks.builtin import build_check_query
from interlace.checks.spec import CheckSpec
from interlace.dsl.decorators import CheckDef
from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import DefinitionError, PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import TableRef
from interlace.runtime.handles import RelationHandle
from interlace.sinks import target_ref
from interlace.state.store import SqliteStateStore


@dataclass(frozen=True)
class CheckOutcome:
    """The result of one check run."""

    model: str
    name: str
    type: str
    severity: str
    status: str  # "passed" | "failed" | "error"
    failures: int = 0
    message: str | None = None

    @property
    def blocking(self) -> bool:
        return self.status != "passed" and self.severity == "error"


async def _run_declared(
    spec: CheckSpec,
    model: CompiledModel,
    compiled: CompiledProject,
    engine: EngineAdapter,
    table: TableRef,
    physical: Mapping[str, TableRef] | None,
) -> CheckOutcome:
    def resolve(name: str) -> TableRef:
        upstream = compiled.models.get(name)
        if upstream is None:
            raise DefinitionError(f"check on {model.name!r} references unknown model {name!r}")
        return (physical or {}).get(name, upstream.physical_table)

    try:
        query = build_check_query(spec, table, model.name, model.dialect, resolve)
        reader = await engine.fetch(query)
        row = reader.read_all().to_pylist()[0]
        failures = int(row["failures"] or 0)
    except DefinitionError:
        raise  # a misdeclared check is a definition problem, not a data problem
    except Exception as error:
        return CheckOutcome(model.name, spec.name, spec.type, spec.severity, "error", message=str(error))
    status = "passed" if failures == 0 else "failed"
    return CheckOutcome(model.name, spec.name, spec.type, spec.severity, status, failures=failures)


async def _run_python(check: CheckDef, engine: EngineAdapter, table: TableRef, model: str) -> CheckOutcome:
    try:
        query = exp.select("*").from_(table.to_expr())
        handle = RelationHandle(model, await engine.fetch(query))
        if inspect.iscoroutinefunction(check.fn):
            result = await check.fn(handle)
        else:
            result = await asyncio.to_thread(check.fn, handle)
    except Exception as error:
        return CheckOutcome(model, check.name, "python", check.severity, "error", message=str(error))
    if isinstance(result, pa.Table):  # returned failing rows: empty = pass
        result = result.num_rows
    if result is True or result is None or result == 0:
        return CheckOutcome(model, check.name, "python", check.severity, "passed")
    failures = int(result) if isinstance(result, int) else 1
    return CheckOutcome(model, check.name, "python", check.severity, "failed", failures=failures)


async def run_checks(
    model: CompiledModel,
    compiled: CompiledProject,
    engine: EngineAdapter,
    table: TableRef,
    python_checks: tuple[CheckDef, ...] = (),
    physical: Mapping[str, TableRef] | None = None,
) -> list[CheckOutcome]:
    """Run all of ``model``'s checks against ``table``; returns every outcome."""
    outcomes = [await _run_declared(spec, model, compiled, engine, table, physical) for spec in model.checks]
    outcomes += [await _run_python(check, engine, table, model.name) for check in python_checks]
    return outcomes


async def run_promoted_checks(
    compiled: CompiledProject,
    store: SqliteStateStore,
    engines: EngineRegistry,
    environment: str,
    selectors: Sequence[str] = (),
) -> tuple[list[CheckOutcome], list[str]]:
    """Re-check an environment's promoted tables. ``state:`` selectors see the ledger.

    Returns ``(outcomes, skipped)``. Models that were never promoted in this
    environment are skipped. A missing     environment is a plan error.
    """
    from interlace.plan.orchestrate import resolve_selection

    chosen = await resolve_selection(compiled, store, environment, selectors)
    promoted = await store.get_environment(environment)
    if not promoted:
        raise PlanError(f"no environment {environment!r} — run `interlace apply` first")
    snapshots = await store.get_snapshots(promoted.items())
    physical = {name: snapshot.physical_table for (name, _), snapshot in snapshots.items()}
    outcomes: list[CheckOutcome] = []
    skipped: list[str] = []
    for name, model in compiled.models.items():
        if chosen is not None and name not in chosen:
            continue
        if not model.checks and not compiled.python_checks.get(name):
            continue
        if model.materialise == "file" or (model.is_terminal and environment not in model.environments):
            continue
        snapshot = snapshots.get((name, promoted.get(name, "")))
        if snapshot is None:
            skipped.append(name)
            continue
        engine = engines.require(snapshot.engine, model=name)
        check_table = target_ref(model.target or "") if model.materialise == "table" else snapshot.physical_table
        results = await run_checks(model, compiled, engine, check_table, compiled.python_checks.get(name, ()), physical)
        if results:
            await store.record_check_results(environment, snapshot.fingerprint, results)
        outcomes.extend(results)
    return outcomes, skipped
