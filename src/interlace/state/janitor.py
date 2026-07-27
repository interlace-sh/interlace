"""Garbage-collect unreferenced snapshots and their physical tables.

A snapshot row is garbage when **no environment points at its fingerprint** and
it is older than the grace window (protecting applies in flight and very recent
rollback targets). A physical table is dropped only when **no surviving
snapshot row references it** — this is what makes GC safe under the rebuild-skip
optimisation, where a newer fingerprint's snapshot can point at an *older*
fingerprint's table: the old row goes, the shared table stays.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry, as_registry
from interlace.plan.plan import XFER_SCHEMA
from interlace.state.store import SqliteStateStore


@dataclass
class GcResult:
    removed_snapshots: list[tuple[str, str]] = field(default_factory=list)  # (model, fingerprint)
    dropped_tables: list[str] = field(default_factory=list)  # engine:schema.name
    kept_snapshots: int = 0
    swept_staging: list[str] = field(default_factory=list)  # engine:interlace__xfer.name


def _table_key(row: dict[str, str]) -> str:
    engine = row.get("engine") or "default"
    return f"{engine}:{row['physical_schema']}.{row['physical_name']}"


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
        await adapter.execute(exp.Drop(this=exp.table_(view.name, db=view.schema), kind="VIEW", exists=True))
        dropped.append(f"{engine_name}:{view.schema}.{view.name}")
        if environment != PRODUCTION_ENV:  # never touch the natural schemas
            schemas.setdefault(engine_name, set()).add(view.schema)
    for engine_name, names in schemas.items():
        adapter = registry.require(engine_name)
        for schema in sorted(names):  # exclusively env-owned (prefixed): safe to cascade
            await adapter.execute_sql(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    await state.delete_environment(environment)
    return dropped


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
        await registry.require(engine_name).execute(
            exp.Drop(this=exp.table_(name, db=schema), kind="TABLE", exists=True)
        )

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
        # a snapshot's physical object is a table or (for view materialise) a view
        for kind in ("TABLE", "VIEW"):
            await target.execute(exp.Drop(this=exp.table_(name, db=schema), kind=kind, exists=True))
    return result
