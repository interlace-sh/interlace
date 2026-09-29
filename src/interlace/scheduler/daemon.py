"""The process loops shared by ``interlace scheduler`` and ``interlace serve``.

One policy: recompile when sources change, tick triggers, trim logs, drain the
queue, flush streams outside the drain lock, sweep retention, and (when the
project declares it) copy Postgres CDC slots. The HTTP app adds SSE and shutdown
on top; it does not grow a second scheduler.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from interlace.dsl.decorators import StreamDef
from interlace.dsl.dynamic import DYNAMIC_ROOT
from interlace.exceptions import ConfigurationError
from interlace.graph.column_lineage import column_lineage
from interlace.scheduler.engine import TriggerEngine, build_triggers
from interlace.scheduler.worker import drain
from interlace.state.locks import hold_apply_lock
from interlace.streaming.materializer import flush_streams, stream_consumers, stream_watermark, sweep_streams

logger = logging.getLogger("interlace.daemon")


def source_mtime(root: Path, model_paths: list[str]) -> float:
    """Newest mtime across the config file and every model source."""
    from interlace.config.config import CONFIG_FILE

    latest = 0.0
    config = root / CONFIG_FILE
    if config.exists():
        latest = config.stat().st_mtime
    for relative in model_paths:
        directory = root / relative
        if not directory.is_dir():
            continue
        for file in directory.rglob("*"):
            if file.suffix in (".sql", ".py") and file.is_file():
                latest = max(latest, file.stat().st_mtime)
    dynamic = root / DYNAMIC_ROOT
    if dynamic.is_dir():
        for file in dynamic.rglob("*.sql"):
            if file.is_file():
                latest = max(latest, file.stat().st_mtime)
    return latest


_RUNTIME_FIELDS = (
    "engines",
    "connections",
    "cdc",
    "inputs",
    "database",
    "attach",
    "secrets",
    "data_path",
    "metadata_schema",
    "alias",
    "quack_token",
    "default_engine",
)
_WAREHOUSE_FIELDS = (
    "database",
    "attach",
    "secrets",
    "data_path",
    "metadata_schema",
    "alias",
    "quack_token",
    "default_engine",
)


def runtime_parts(config: Any) -> dict[str, Any]:
    """The config a running process opened and cannot swap underneath itself."""
    dumped = config.model_dump(mode="json")
    return {key: dumped[key] for key in _RUNTIME_FIELDS}


def remember_runtime(state: Any, config: Any) -> None:
    """Record the runtime config this process opened. Later drift refuses to apply."""
    state.runtime_opened = runtime_parts(config)
    state.restart_required = None


def runtime_restart_reason(opened: Mapping[str, Any], current: Mapping[str, Any]) -> str | None:
    """Why this process must be restarted, or None when the opened config still matches."""
    changed: list[str] = []
    for key in ("engines", "connections", "cdc", "inputs"):
        if opened.get(key) != current.get(key):
            changed.append(key)
    if any(opened.get(key) != current.get(key) for key in _WAREHOUSE_FIELDS):
        changed.append("warehouse")
    if not changed:
        return None
    return f"{', '.join(changed)} changed in interlace.yaml; restart this process to pick that up"


def _note_restart(state: Any, message: str) -> None:
    state.restart_required = message
    if getattr(state, "restart_logged", None) != message:
        logger.error("%s", message)
        state.restart_logged = message


def publish_compiled(state: Any, compiled: Any) -> None:
    """Swap the live graph. Lineage and the stream-consumer map follow it."""
    state.compiled = compiled
    state.lineage = column_lineage(compiled)
    state.stream_consumer_map = {name: sorted(stream_consumers(compiled, name)) for name in state.streams}
    state.describe_cache = {}


async def reload_if_stale(state: Any) -> None:
    """Recompile when a model source is newer than the graph this process holds.

    Model files are picked up. Engines, connections, inputs, CDC, and the
    warehouse were opened with this process and are not swapped in place: a
    change there raises until the process restarts. Reverting the file clears
    the refusal.
    """
    from interlace.project import Project

    async with state.reload_lock:
        mtime = await asyncio.to_thread(source_mtime, state.root, state.model_paths)
        if mtime <= state.source_mtime:
            return
        project = await asyncio.to_thread(Project.load, state.root)
        opened = getattr(state, "runtime_opened", None)
        if opened is not None:
            reason = runtime_restart_reason(opened, runtime_parts(project.config))
            if reason:
                _note_restart(state, reason)
                raise ConfigurationError(reason)
            state.restart_required = None
        compiled = await asyncio.to_thread(project.compile)
        state.project = project
        publish_compiled(state, compiled)
        state.source_mtime = mtime
        state.connections = project.config.connections
        state.cdc = project.config.cdc


async def enqueue_stream_consumers(state: Any, stream: StreamDef) -> None:
    """A flush advanced the stream table: enqueue the models that read it."""
    consumers = state.stream_consumer_map.get(stream.name, [])
    if not consumers:
        return
    watermark = await stream_watermark(stream, state.engine)
    key = f"stream:{stream.name}:{watermark}"
    if await state.store.enqueue_run(key, sorted(consumers), None, 0):
        await state.store.append_event("run.enqueued", entity=key, payload={"models": sorted(consumers)})
        state.drain_wanted.set()


async def flush_once(state: Any) -> None:
    """One coalesced flush of the dirty streams, then the consumer enqueues it earns."""
    dirty = set(state.flush_dirty)
    state.flush_dirty.clear()
    targets = [target for target in state.flush_targets if target.name in dirty]
    if not targets:
        return
    pre_flush = {target.name: state.log_heads.get(target.name, 0) for target in targets}
    try:
        async with hold_apply_lock(state.store, owner=state.lock_owner):
            flushed = await flush_streams(targets, state.stream_log, state.engine)
    except BaseException:
        state.flush_dirty |= dirty
        raise
    for target in targets:
        state.flushed_heads[target.name] = pre_flush[target.name]
    for stream_name, rows in flushed.items():
        await state.store.append_event("stream.flushed", entity=stream_name, payload={"rows": rows})
        landed = state.streams.get(stream_name)
        if landed is not None:
            await enqueue_stream_consumers(state, landed)
    if state.cdc:
        from interlace.cdc.publish import confirm_flushed

        for source in state.cdc.values():
            landed = state.streams.get(source.stream)
            if landed is None:
                continue
            watermark = await stream_watermark(landed, state.engine)
            await confirm_flushed(state.store, source.stream, watermark)


async def flusher_loop(state: Any, *, flush_interval: float) -> None:
    """Micro-batch materializer. A publish sets ``flush_wanted``; this coalesces the burst."""
    while True:
        await state.flush_wanted.wait()
        await asyncio.sleep(flush_interval)
        state.flush_wanted.clear()
        try:
            await flush_once(state)
        except Exception:
            logger.exception("stream flush failed; will retry")
            state.flush_wanted.set()
            await asyncio.sleep(1.0)


async def scheduler_loop(
    state: Any,
    *,
    interval: float,
    once: bool = False,
    on_ran: Callable[[int], Awaitable[None] | None] | None = None,
) -> None:
    """Tick triggers, drain the queue, trim logs, and sweep stream retention."""
    next_trim = asyncio.get_running_loop().time()
    while True:
        state.drain_wanted.clear()
        ran = 0
        try:
            await reload_if_stale(state)
            compiled = state.compiled
            await TriggerEngine(build_triggers(compiled, root=state.project.root), state.store, compiled).tick(
                datetime.now()
            )
            if asyncio.get_running_loop().time() >= next_trim:
                await state.store.trim_logs()
                next_trim = asyncio.get_running_loop().time() + 6 * 3600
            async with hold_apply_lock(state.store, owner=state.lock_owner):
                ran = await drain(
                    state.store,
                    compiled,
                    engines=state.engines,
                    environment=state.environment,
                    base_path=state.project.root,
                    parallelism=state.project.config.parallelism,
                    connections=state.project.config.connections,
                    loaded=state.project,
                    on_compiled=lambda fresh: publish_compiled(state, fresh),
                )
            if state.streams:
                state.flush_wanted.set()
                await sweep_streams(state.streams.values(), state.stream_log, state.engine)
        except ConfigurationError as exc:
            _note_restart(state, exc.message)
            if once:
                raise
        except Exception:
            logger.exception("scheduler tick failed; retrying next interval")
        if on_ran is not None and ran:
            result = on_ran(ran)
            if result is not None:
                await result
        if once:
            return
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(state.drain_wanted.wait(), timeout=interval)


async def cdc_loop(state: Any) -> None:
    """Copy Postgres slots into their streams. LSN feedback waits until the flush lands."""
    from interlace.cdc.publish import publish_changes
    from interlace.cdc.slot import SlotReader
    from interlace.config.config import PostgresConnection

    readers: dict[str, SlotReader] = {}
    try:
        while True:
            for name, source in state.cdc.items():
                conn = state.connections.get(source.connection)
                declared = state.streams.get(source.stream)
                if not isinstance(conn, PostgresConnection) or declared is None:
                    logger.error("cdc %s needs a postgres connection and a declared stream", name)
                    continue
                try:
                    reader = readers.get(name)
                    if reader is None:
                        reader = SlotReader(conn.dsn, source)
                        readers[name] = reader
                    confirmed = await state.store.cdc.cdc_confirmed_lsn(source.stream)
                    changes = await asyncio.to_thread(reader.poll, confirmed)
                    if changes:
                        await publish_changes(state.stream_log, state.store, source.stream, changes)
                        state.flush_dirty.add(source.stream)
                        state.flush_wanted.set()
                    advanced = await state.store.cdc.cdc_confirmed_lsn(source.stream)
                    if advanced:
                        await asyncio.to_thread(reader.feedback, advanced)
                except Exception:
                    logger.exception("cdc %s failed; reconnecting", name)
                    failed = readers.pop(name, None)
                    if failed is not None:
                        await asyncio.to_thread(failed.close)
            await asyncio.sleep(1)
    finally:
        for reader in readers.values():
            await asyncio.to_thread(reader.close)
