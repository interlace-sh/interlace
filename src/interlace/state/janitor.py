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
from interlace.state.store import SqliteStateStore


@dataclass
class GcResult:
    removed_snapshots: list[tuple[str, str]] = field(default_factory=list)  # (model, fingerprint)
    dropped_tables: list[str] = field(default_factory=list)  # engine:schema.name
    kept_snapshots: int = 0


def _table_key(row: dict[str, str]) -> str:
    engine = row.get("engine") or "default"
    return f"{engine}:{row['physical_schema']}.{row['physical_name']}"


async def gc(
    state: SqliteStateStore,
    engine: EngineAdapter | None = None,
    *,
    engines: EngineRegistry | dict[str, EngineAdapter] | None = None,
    grace: timedelta = timedelta(days=7),
    dry_run: bool = False,
) -> GcResult:
    """Remove unreferenced snapshots past ``grace`` and drop their orphaned tables.

    Tables are dropped on the engine recorded on each snapshot row (multi-engine).
    """
    registry = as_registry(engine, engines)
    referenced = await state.referenced_snapshots()
    rows = await state.list_snapshot_rows()
    cutoff = datetime.now(UTC) - grace

    doomed: list[dict[str, str]] = []
    surviving: list[dict[str, str]] = []
    for row in rows:
        key = (row["name"], row["fingerprint"])
        created = datetime.fromisoformat(row["created_at"])
        if key not in referenced and created < cutoff:
            doomed.append(row)
        else:
            surviving.append(row)

    live_tables = {_table_key(row) for row in surviving}
    dead_keys = sorted({_table_key(row) for row in doomed} - live_tables)
    # Map key → engine for drop routing (first doomed row wins; keys include engine).
    key_engine = {_table_key(row): (row.get("engine") or "default") for row in doomed}

    result = GcResult(
        removed_snapshots=[(row["name"], row["fingerprint"]) for row in doomed],
        dropped_tables=dead_keys,
        kept_snapshots=len(surviving),
    )
    if dry_run or not doomed:
        return result

    await state.delete_snapshots(result.removed_snapshots)
    for table_key in dead_keys:
        eng_name, rest = table_key.split(":", 1)
        schema, name = rest.split(".", 1)
        target = registry.require(key_engine.get(table_key, eng_name))
        # a snapshot's physical object is a table or (for view materialise) a view
        for kind in ("TABLE", "VIEW"):
            await target.execute(exp.Drop(this=exp.table_(name, db=schema), kind=kind, exists=True))
    return result
