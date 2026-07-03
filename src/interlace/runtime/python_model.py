"""Execute a Python model: Arrow in, Arrow out, one load at the sink.

The model function's parameters name its upstream models; each becomes a
:class:`RelationHandle` streaming that upstream's physical table (or, for an
ephemeral upstream, its inlined query) as Arrow. The return value — a
``pyarrow.Table``, ``RecordBatchReader``, ``RecordBatch``, or an iterable of
batches (generators stream with bounded memory) — is loaded into the model's
physical snapshot table with a single ``CREATE TABLE AS``. Sync functions run
in a worker thread; async functions run on the event loop.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Iterable, Iterator
from typing import Any

import pyarrow as pa
from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.exceptions import DefinitionError, PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import TableRef
from interlace.plan.resolve import resolve_model_query
from interlace.runtime.handles import RelationHandle


async def _upstream_reader(
    upstream: CompiledModel, compiled: CompiledProject, engine: EngineAdapter
) -> pa.RecordBatchReader:
    if upstream.export is not None:
        raise PlanError(f"model {upstream.name!r} is a sink; it has no readable output")
    if upstream.materialise == "ephemeral":  # no physical table: run its (inlined) query
        return await engine.fetch(resolve_model_query(upstream, compiled))
    table = upstream.physical_table
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


async def build_python_model(
    model: CompiledModel, compiled: CompiledProject, engine: EngineAdapter, target: TableRef
) -> None:
    """Run ``model``'s function over its upstreams and load the result into ``target``."""
    if model.fn is None:
        raise PlanError(f"model {model.name!r} has no function to execute")

    parameters = list(inspect.signature(model.fn).parameters)
    unknown = [p for p in parameters if p not in model.dependencies]
    if unknown:
        raise DefinitionError(
            f"Python model {model.name!r} takes parameters {unknown} that are not declared dependencies; "
            f"declare them with depends_on or name them after upstream models"
        )
    handles = {
        name: RelationHandle(name, await _upstream_reader(compiled.models[name], compiled, engine))
        for name in parameters
    }

    if inspect.iscoroutinefunction(model.fn):
        result = await model.fn(**handles)
    else:
        result = await asyncio.to_thread(model.fn, **handles)

    await engine.load(target, _to_reader(model.name, result), "create")
