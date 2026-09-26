"""Garbage-collect unreferenced snapshots, drop environments, and reset.

A snapshot row is garbage when **no environment points at its fingerprint** and
it is older than the grace window (protecting applies in flight and very recent
rollback targets). A physical table is dropped only when **no surviving
snapshot row references it** — this is what makes GC safe under the rebuild-skip
optimisation, where a newer fingerprint's snapshot can point at an *older*
fingerprint's table: the old row goes, the shared table stays.

``reset`` is the fresh-start counterpart: drop every Interlace-owned object and
wipe the control plane, leaving ``materialise: table`` / ``file`` destinations
untouched.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry, as_registry
from interlace.graph.project import PHYSICAL_SCHEMA_PREFIX
from interlace.ir.relation import TableRef, drop
from interlace.plan.plan import XFER_SCHEMA
from interlace.state.store import SqliteStateStore
from interlace.streaming.log import StreamLog

_STREAMS_SCHEMA = "streams"
_STREAMS_WATERMARKS = TableRef(schema=_STREAMS_SCHEMA, name="_watermarks")


@dataclass
class GcResult:
    removed_snapshots: list[tuple[str, str]] = field(default_factory=list)  # (model, fingerprint)
    dropped_tables: list[str] = field(default_factory=list)  # engine:schema.name
    kept_snapshots: int = 0
    swept_staging: list[str] = field(default_factory=list)  # engine:interlace__xfer.name


@dataclass
class ResetResult:
    """What ``reset`` removed — or would remove, when ``dry_run``."""

    dropped_views: list[str] = field(default_factory=list)  # engine:schema.name
    dropped_schemas: list[str] = field(default_factory=list)  # engine:schema
    cleared_snapshots: int = 0
    kept_terminals: list[str] = field(default_factory=list)
    environments: list[str] = field(default_factory=list)
    stream_log_cleared: bool = False
    dry_run: bool = False


def _table_key(row: dict[str, str]) -> str:
    engine = row.get("engine") or "default"
    return f"{engine}:{row['physical_schema']}.{row['physical_name']}"


def _quoted_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


async def _drop_schema(adapter: EngineAdapter, schema: str) -> None:
    """Drop a schema we own. ``CASCADE`` is required: snapshot schemas hold tables
    *and* views (``materialise: view``), and sandbox env schemas hold env views."""
    await adapter.execute_sql(f"DROP SCHEMA IF EXISTS {_quoted_ident(schema)} CASCADE")


async def _owned_schema_names(adapter: EngineAdapter) -> list[str]:
    """Schemas whose names start with ``interlace__``. Filtered in Python so a
    SQL ``LIKE`` cannot treat the underscores as wildcards."""
    reader = await adapter.fetch_sql("SELECT schema_name FROM information_schema.schemata")
    names = {str(row["schema_name"]) for row in reader.read_all().to_pylist()}
    return sorted(name for name in names if name.startswith(PHYSICAL_SCHEMA_PREFIX))


async def _drop_relation(adapter: EngineAdapter, schema: str, name: str) -> None:
    """Drop a snapshot's physical object — a table (virtual plane) or a **view**
    (view plane) — picking the kind from the catalog first. ``DROP TABLE`` on a view
    (or vice versa) raises rather than no-ops, so a blind ``DROP TABLE`` could never
    reclaim view-materialised snapshots."""
    reader = await adapter.fetch_sql(
        f"SELECT table_type FROM information_schema.tables "  # noqa: S608 — internal interlace__ names, not user input
        f"WHERE table_schema = '{schema}' AND table_name = '{name}'"
    )
    rows = reader.read_all().to_pylist()
    if not rows:
        return  # already gone
    kind = "VIEW" if str(rows[0]["table_type"]).upper() == "VIEW" else "TABLE"
    await adapter.execute(drop(TableRef(schema=schema, name=name), kind=kind))


async def rollback_environment(
    state: SqliteStateStore,
    engine: EngineAdapter | None = None,
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None = None,
    environment: str,
    to_generation: int | None = None,
) -> dict[str, object]:
    """Repoint an environment at an earlier promotion generation.

    Every promote records the environment's full mapping as a generation;
    rollback restores generation N (default: the one before the latest) by
    recreating each env view on its recorded snapshot's physical table, dropping
    views for models that did not exist then, and replacing the environment's
    promotion rows. The snapshots must still exist — a rollback target older
    than the gc grace window may already be reclaimed, and that is reported
    per-model rather than half-applied.
    """
    from interlace.exceptions import PlanError
    from interlace.plan.plan import env_view

    registry = as_registry(engine, engines)
    generations = await state.list_generations(environment)
    if not generations:
        raise PlanError(f"environment {environment!r} has no promotion history")
    latest = int(str(generations[0]["generation"]))
    target = latest - 1 if to_generation is None else to_generation
    if target < 1 or target >= latest:
        known = f"1..{latest - 1}" if latest > 1 else "none yet — only one generation recorded"
        raise PlanError(f"cannot roll {environment!r} back to generation {target}; valid targets: {known}")
    mapping = await state.get_generation(environment, target)

    snapshots = await state.get_snapshots(mapping.items())
    # A mapping entry with no snapshot for its fingerprint is either an ephemeral
    # (promotion pointer only, never had a snapshot — fine) or a table/view whose
    # snapshot gc reclaimed (can't rebuild the view — fail). Tell them apart by
    # whether the model has ANY snapshot recorded, not by the CURRENT project's
    # materialise (the model may since have been deleted or converted).
    missing: list[str] = []
    for name, fingerprint in mapping.items():
        if (name, fingerprint) in snapshots:
            continue
        if await state.list_snapshots(name):  # has other versions -> this one was reclaimed
            missing.append(name)
    if missing:
        raise PlanError(
            f"rollback target reclaimed by gc: no snapshot for {', '.join(sorted(missing))} — "
            f"rebuild instead (apply an older git state)"
        )

    repointed: list[str] = []
    for name, fingerprint in mapping.items():
        snapshot = snapshots.get((name, fingerprint))
        if snapshot is None:
            continue  # ephemeral: promotion pointer only, no view
        adapter = registry.require(snapshot.engine)
        if not await adapter.table_exists(snapshot.physical_table):
            continue  # sinks: recorded fingerprint, no physical relation to serve
        view = env_view(environment, name)
        await adapter.create_schema(view.schema)
        await adapter.create_view(view, snapshot.physical_table)
        repointed.append(name)

    current = await state.get_environment(environment)
    removed = sorted(set(current) - set(mapping))
    if removed:
        gone = await state.get_snapshots((name, current[name]) for name in removed)
        for name in removed:
            snapshot = gone.get((name, current[name]))
            adapter = registry.require(snapshot.engine if snapshot is not None else registry.default)
            view = env_view(environment, name)
            await adapter.execute(drop(view, kind="VIEW"))

    await state.set_environment(environment, mapping)
    return {"environment": environment, "generation": target, "repointed": repointed, "removed_views": removed}


async def drop_environment(
    state: SqliteStateStore,
    engine: EngineAdapter | None = None,
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None = None,
    environment: str,
) -> list[str]:
    """Remove an environment: drop its views (on each model's engine), delete its
    promotion rows, and — for prefixed sandboxes — drop the now-empty env schemas.
    Returns the dropped view names. The environment's snapshots become
    unreferenced, so a later ``gc`` reclaims their tables.
    """
    from interlace.plan.plan import PRODUCTION_ENV, env_view

    registry = as_registry(engine, engines)
    mapping = await state.get_environment(environment)
    dropped: list[str] = []
    schemas: dict[str, set[str]] = {}  # engine -> env schemas touched
    for model, fingerprint in mapping.items():
        snapshot = await state.get_snapshot(model, fingerprint)
        engine_name = snapshot.engine if snapshot is not None else registry.default
        view = env_view(environment, model)
        adapter = registry.require(engine_name)
        await adapter.execute(drop(view, kind="VIEW"))
        dropped.append(f"{engine_name}:{view.schema}.{view.name}")
        if environment != PRODUCTION_ENV:  # never touch the natural schemas
            schemas.setdefault(engine_name, set()).add(view.schema)
    for engine_name, names in schemas.items():
        adapter = registry.require(engine_name)
        for schema in sorted(names):  # exclusively env-owned (prefixed): safe to cascade
            await _drop_schema(adapter, schema)
    await state.delete_environment(environment)
    return dropped


async def reset(
    state: SqliteStateStore,
    engine: EngineAdapter | None = None,
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None = None,
    keep_models: Iterable[str] = (),
    stream_log: StreamLog | None = None,
    clear_streams: bool = False,
    dry_run: bool = False,
) -> ResetResult:
    """Wipe Interlace-owned warehouse objects and control-plane state.

    Drops environment views, ``interlace__*`` snapshot schemas (and transfer
    staging), and — when they look like ours — the ``streams`` landing schema.
    Does **not** drop ``materialise: table`` / ``file`` destinations: those are
    not ours. Snapshot, interval, and environment rows for ``keep_models``
    (current terminal models) stay, so the next apply treats them as unchanged
    and will not re-deliver into them. API keys, advisory locks, and trigger
    last-fired times are kept (a live scheduler must not immediately force-run
    terminals). The stream log is cleared when provided.
    """
    from interlace.plan.plan import PRODUCTION_ENV, env_view

    registry = as_registry(engine, engines)
    keep = frozenset(keep_models)
    environments = await state.list_environments()
    snapshot_rows = await state.list_snapshot_rows()
    cleared_snapshots = sum(1 for row in snapshot_rows if row["name"] not in keep)

    dropped_views: list[str] = []
    sandbox_schemas: dict[str, set[str]] = {}  # engine -> env schemas
    for environment in environments:
        mapping = await state.get_environment(environment)
        for model, fingerprint in mapping.items():
            snapshot = await state.get_snapshot(model, fingerprint)
            engine_name = snapshot.engine if snapshot is not None else registry.default
            view = env_view(environment, model)
            if model not in keep:
                dropped_views.append(f"{engine_name}:{view.schema}.{view.name}")
            if environment != PRODUCTION_ENV:
                # drop the prefixed sandbox schema even if only terminals remain
                sandbox_schemas.setdefault(engine_name, set()).add(view.schema)

    owned_schemas: dict[str, set[str]] = {name: set() for name in registry}
    for row in snapshot_rows:
        schema = row["physical_schema"]
        if schema.startswith(PHYSICAL_SCHEMA_PREFIX):
            owned_schemas.setdefault(row.get("engine") or "default", set()).add(schema)
    for engine_name in registry:
        adapter = registry.require(engine_name)
        for schema in await _owned_schema_names(adapter):
            owned_schemas.setdefault(engine_name, set()).add(schema)

    default_adapter = registry.require(registry.default)
    drop_streams = clear_streams or await default_adapter.table_exists(_STREAMS_WATERMARKS)
    dropped_schemas: list[str] = []
    for engine_name, names in owned_schemas.items():
        dropped_schemas.extend(f"{engine_name}:{schema}" for schema in sorted(names))
    for engine_name, names in sandbox_schemas.items():
        dropped_schemas.extend(f"{engine_name}:{schema}" for schema in sorted(names))
    if drop_streams:
        dropped_schemas.append(f"{registry.default}:{_STREAMS_SCHEMA}")
    dropped_schemas = sorted(set(dropped_schemas))

    result = ResetResult(
        dropped_views=dropped_views,
        dropped_schemas=dropped_schemas,
        cleared_snapshots=cleared_snapshots,
        kept_terminals=sorted(keep),
        environments=list(environments),
        stream_log_cleared=stream_log is not None,
        dry_run=dry_run,
    )
    if dry_run:
        return result

    for environment in environments:
        mapping = await state.get_environment(environment)
        for model, fingerprint in mapping.items():
            if model in keep:
                continue
            snapshot = await state.get_snapshot(model, fingerprint)
            engine_name = snapshot.engine if snapshot is not None else registry.default
            view = env_view(environment, model)
            adapter = registry.require(engine_name)
            await adapter.execute(drop(view, kind="VIEW"))
    for engine_name, names in sandbox_schemas.items():
        adapter = registry.require(engine_name)
        for schema in sorted(names):
            await _drop_schema(adapter, schema)
    for engine_name, names in owned_schemas.items():
        if engine_name not in registry:
            continue  # snapshot from an engine no longer configured: leave its schema
        adapter = registry.require(engine_name)
        for schema in sorted(names):
            await _drop_schema(adapter, schema)
    if drop_streams:
        await _drop_schema(default_adapter, _STREAMS_SCHEMA)
    if stream_log is not None:
        await stream_log.clear()
    await state.reset_control_plane(keep)
    return result


async def gc(
    state: SqliteStateStore,
    engine: EngineAdapter | None = None,
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None = None,
    grace: timedelta = timedelta(days=7),
    dry_run: bool = False,
) -> GcResult:
    """Remove unreferenced snapshots past ``grace`` and drop their orphaned tables.

    The decide-and-delete happens in ONE state-store transaction, so a concurrent
    promote — from this process or another (a CLI apply while the daemon GCs) —
    either lands before the check (the row is referenced, it survives) or after
    the delete (and a promote can only reference fingerprints whose snapshot rows
    exist, which the doomed ones no longer do). Physical tables are then dropped
    on the engine recorded on each deleted row (multi-engine).
    """
    registry = as_registry(engine, engines)
    cutoff = datetime.now(UTC) - grace
    doomed, surviving = await state.collect_snapshot_garbage(cutoff, delete=not dry_run)
    rows = doomed + surviving

    live_tables = {_table_key(row) for row in surviving}
    dead_keys = sorted({_table_key(row) for row in doomed} - live_tables)
    # Map key → engine for drop routing (first doomed row wins; keys include engine).
    key_engine = {_table_key(row): (row.get("engine") or "default") for row in doomed}

    result = GcResult(
        removed_snapshots=[(row["name"], row["fingerprint"]) for row in doomed],
        dropped_tables=dead_keys,
        kept_snapshots=len(surviving),
    )

    # transfer staging is scratch: rebuilt on demand by the next apply that needs it
    engine_names = {row.get("engine") or "default" for row in rows} | {registry.default}
    for engine_name in sorted(engine_names):
        if engine_name not in registry:
            continue  # snapshots from an engine no longer configured: leave its staging alone
        adapter = registry.require(engine_name)
        reader = await adapter.fetch_sql(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{XFER_SCHEMA}'"
        )
        for row in reader.read_all().to_pylist():
            result.swept_staging.append(f"{engine_name}:{XFER_SCHEMA}.{row['table_name']}")

    if dry_run or not (doomed or result.swept_staging):
        return result

    for staged in result.swept_staging:
        engine_name, rest = staged.split(":", 1)
        schema, name = rest.split(".", 1)
        await registry.require(engine_name).execute(drop(TableRef(schema=schema, name=name), kind="TABLE"))

    # Re-check right before dropping: a concurrent apply in ANOTHER process may have
    # recorded a rebuild-skip reuse row over one of these tables after our transaction
    # committed. The remaining window (this query -> DROP) is milliseconds, guarded in
    # practice by the grace period; the doomed ROWS stay deleted either way.
    still_live = {_table_key(row) for row in await state.list_snapshot_rows()}
    for table_key in dead_keys:
        if table_key in still_live:
            result.dropped_tables.remove(table_key)
            continue
        eng_name, rest = table_key.split(":", 1)
        schema, name = rest.split(".", 1)
        target = registry.require(key_engine.get(table_key, eng_name))
        await _drop_relation(target, schema, name)
    return result
