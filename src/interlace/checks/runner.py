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
from collections.abc import Mapping
from dataclasses import dataclass

import pyarrow as pa
from sqlglot import exp

from interlace.checks.builtin import build_check_query
from interlace.checks.spec import CheckSpec
from interlace.dsl.decorators import CheckDef
from interlace.engines.base import EngineAdapter
from interlace.exceptions import DefinitionError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import TableRef
from interlace.runtime.handles import RelationHandle


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
        query = exp.select("*").from_(exp.table_(table.name, db=table.schema, catalog=table.catalog))
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
