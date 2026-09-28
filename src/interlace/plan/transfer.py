"""Stage one upstream onto the consumer's engine.

Each (upstream, target engine) pair moves once per apply. An attachable source
uses a federated CTAS; any failure falls back to Arrow fetch then load.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Mapping

from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.layout import staging_table
from interlace.ir.relation import TableRef
from interlace.plan.result import ApplyResult


async def _stage_cross_engine_inputs(
    model: CompiledModel,
    compiled: CompiledProject,
    registry: EngineRegistry,
    physical: Mapping[str, TableRef],
    staged: set[tuple[str, str]],
    stage_lock: asyncio.Lock,
    result: ApplyResult,
) -> dict[str, TableRef]:
    """Move cross-engine upstreams into staging tables on the model's engine.

    Returns the model's *local* resolution map: cross-engine deps point at their
    staged copies; everything else keeps the global physical map. Each
    (upstream, target-engine) pair transfers once per apply — always replaced,
    so a re-run upstream (merge/incremental) is never read stale. The lock is
    held across the transfer so a concurrent consumer of the same upstream
    never reads a half-populated stage table.
    """
    local = dict(physical)
    for dep in model.dependencies:
        upstream = compiled.models[dep]
        if upstream.engine == model.engine or upstream.materialise == "ephemeral":
            continue
        stage = staging_table(dep)
        local[dep] = stage
        key = (dep, model.engine)
        async with stage_lock:
            if key in staged:
                continue
            target = registry.require(model.engine, model=model.name)
            origin = physical.get(dep, upstream.physical_table)
            await target.create_schema(stage.schema)
            via = "arrow"
            if await _attach_transfer(
                target, registry.attach_uris.get(upstream.engine), upstream.engine, origin, stage
            ):
                via = "attach"  # federated CTAS: no Python hop at all
            else:
                source_engine = registry.require(upstream.engine, model=dep)
                reader = await source_engine.fetch(exp.select("*").from_(origin.to_expr()))
                await target.load(stage, reader, "create")
            staged.add(key)
            result.transfers.append(f"{dep}: {upstream.engine} -> {model.engine} ({stage.schema}.{stage.name}, {via})")
    return local


async def _attach_transfer(
    target: EngineAdapter, uri: str | None, source_name: str, origin: TableRef, stage: TableRef
) -> bool:
    """Fast lane: when the target can ATTACH the source, stage with one federated CTAS.
    Opportunistic — any failure falls back to Arrow."""
    if uri is None or not target.caps.supports_attach:
        return False
    alias = f"__xfer_{source_name}"
    src = exp.table_(origin.name, db=origin.schema, catalog=alias).sql(dialect="duckdb")
    dst = exp.table_(stage.name, db=stage.schema).sql(dialect="duckdb")
    try:
        target.attach(alias, uri)
        await target.execute_sql(f"CREATE OR REPLACE TABLE {dst} AS SELECT * FROM {src}")
    except Exception:
        return False  # e.g. the source file is held open by its own adapter -> Arrow path
    finally:
        # release the handle either way: the source engine must stay openable
        # by its own adapter later in this (long-lived daemon) process
        with contextlib.suppress(Exception):
            await target.execute_sql(f"DETACH {exp.to_identifier(alias).sql('duckdb')}")
    return True
