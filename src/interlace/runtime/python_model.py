"""Execute a Python model: Arrow in, Arrow out, one load at the sink.

The model function's parameters name its upstream models; each becomes a
:class:`RelationHandle` streaming that upstream's physical table (or, for an
ephemeral upstream, its inlined query) as Arrow. Two parameter names are
reserved for incremental extraction and never name an upstream:

- ``cursor`` — the max of the model's declared ``cursor`` column in its
  *previous* materialisation (``None`` on first build). The value is derived
  from the warehouse, not a side ledger, so it can never drift from committed
  data: a crash before commit simply re-extracts the overlap, and a keyed
  strategy makes the re-load idempotent.
- ``this`` — a :class:`RelationHandle` over the previous materialisation
  (``None`` on first build), for anti-join style backfills against what the
  model has already produced.

The return value — a ``pyarrow.Table``, ``RecordBatchReader``, ``RecordBatch``,
or an iterable of batches (generators stream with bounded memory) — is loaded
at the sink by the caller (``plan.apply``): directly for ``full``, or via a
stage table for keyed strategies. Sync functions run in a worker thread; async
functions run on the event loop.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Iterable, Iterator, Mapping
from typing import Any

import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.exceptions import DefinitionError, PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import TableRef
from interlace.plan.resolve import resolve_model_query
from interlace.runtime.handles import RelationHandle

RESERVED_PARAMS = frozenset({"cursor", "this"})


async def _upstream_reader(
    upstream: CompiledModel,
    compiled: CompiledProject,
    engine: EngineAdapter,
    physical: Mapping[str, TableRef] | None,
) -> pa.RecordBatchReader:
    if upstream.export is not None:
        raise PlanError(f"model {upstream.name!r} is a sink; it has no readable output")
    if upstream.materialise == "ephemeral":  # no physical table: run its (inlined) query
        return await engine.fetch(resolve_model_query(upstream, compiled, physical))
    table = (physical or {}).get(upstream.name, upstream.physical_table)
    return await engine.fetch(exp.select("*").from_(exp.table_(table.name, db=table.schema, catalog=table.catalog)))


def _to_reader(model_name: str, result: Any) -> pa.RecordBatchReader:
    if isinstance(result, pa.RecordBatchReader):
        return result
    if isinstance(result, pa.Table):
        return result.to_reader()
    if isinstance(result, pa.RecordBatch):
        return pa.RecordBatchReader.from_batches(result.schema, [result])
    if isinstance(result, Iterable) and not isinstance(result, (str, bytes, dict)):
        iterator: Iterator[Any] = iter(result)
        try:
            first = next(iterator)
        except StopIteration:
            raise DefinitionError(f"Python model {model_name!r} yielded no batches") from None
        if not isinstance(first, pa.RecordBatch):
            raise DefinitionError(
                f"Python model {model_name!r} must yield pyarrow.RecordBatch items, got {type(first).__name__}"
            )

        def batches() -> Iterator[pa.RecordBatch]:
            yield first
            yield from iterator

        return pa.RecordBatchReader.from_batches(first.schema, batches())
    raise DefinitionError(
        f"Python model {model_name!r} must return a pyarrow Table, RecordBatch, RecordBatchReader, "
        f"or an iterable of RecordBatches; got {type(result).__name__}"
    )


def _table_query(table: TableRef) -> exp.Select:
    return exp.select("*").from_(exp.table_(table.name, db=table.schema, catalog=table.catalog))


async def _cursor_value(model: CompiledModel, engine: EngineAdapter, previous: TableRef | None) -> Any:
    if model.cursor is None:
        raise DefinitionError(
            f"Python model {model.name!r} takes a `cursor` parameter but declares no cursor column; "
            f"set @model(cursor='<column>')"
        )
    if previous is None:
        return None
    query = exp.select(exp.func("max", exp.column(model.cursor))).from_(
        exp.table_(previous.name, db=previous.schema, catalog=previous.catalog)
    )
    reader = await engine.fetch(query)
    value = reader.read_all().column(0)[0]
    return value.as_py()


async def run_python_model(
    model: CompiledModel,
    compiled: CompiledProject,
    engine: EngineAdapter,
    physical: Mapping[str, TableRef] | None = None,
    previous: TableRef | None = None,
) -> pa.RecordBatchReader:
    """Run ``model``'s function over its upstreams and return its output as Arrow.

    ``previous`` is the model's previously-committed physical table (None on
    first build); it backs the reserved ``cursor`` and ``this`` parameters.
    """
    if model.fn is None:
        raise PlanError(f"model {model.name!r} has no function to execute")

    parameters = list(inspect.signature(model.fn).parameters)
    unknown = [p for p in parameters if p not in model.dependencies and p not in RESERVED_PARAMS]
    if unknown:
        raise DefinitionError(
            f"Python model {model.name!r} takes parameters {unknown} that are not declared dependencies; "
            f"declare them with depends_on or name them after upstream models "
            f"(`cursor` and `this` are reserved for incremental state)"
        )

    arguments: dict[str, Any] = {}
    for param in parameters:
        if param == "cursor":
            arguments[param] = await _cursor_value(model, engine, previous)
        elif param == "this":
            arguments[param] = (
                RelationHandle(model.name, await engine.fetch(_table_query(previous))) if previous else None
            )
        else:
            upstream = compiled.models[param]
            arguments[param] = RelationHandle(param, await _upstream_reader(upstream, compiled, engine, physical))

    if inspect.iscoroutinefunction(model.fn):
        result = await model.fn(**arguments)
    else:
        result = await asyncio.to_thread(model.fn, **arguments)

    return _to_reader(model.name, result)


async def build_python_model(
    model: CompiledModel,
    compiled: CompiledProject,
    engine: EngineAdapter,
    target: TableRef,
    physical: Mapping[str, TableRef] | None = None,
    previous: TableRef | None = None,
) -> None:
    """Run ``model``'s function over its upstreams and load the result into ``target``."""
    reader = await run_python_model(model, compiled, engine, physical, previous)
    await engine.load(target, reader, "create")
