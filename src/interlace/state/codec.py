"""Row codecs and the event-actor context shared by the control-plane stores."""

from __future__ import annotations

import contextvars
import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import NotRequired, TypedDict

from interlace.ir.relation import TableRef
from interlace.physical.spec import PhysicalObject
from interlace.state.interval import Interval, IntervalSet
from interlace.state.snapshot import ChangeCategory, Snapshot

event_actor: contextvars.ContextVar[str | None] = contextvars.ContextVar("interlace_event_actor", default=None)


class RunRecord(TypedDict):
    """A work-queue row as returned by ``list_runs`` / ``get_run``."""

    id: int
    idempotency_key: str | None
    flow_selector: list[str]
    partition_start: str | None
    partition_end: str | None
    priority: int
    state: str
    attempts: int
    error: str | None
    enqueued_at: str | None
    restate: bool
    # derived from the run's events by list_runs (absent from get_run, which carries
    # the full event list instead): the wall-clock span and the env it built into
    started_at: NotRequired[str | None]
    finished_at: NotRequired[str | None]
    environment: NotRequired[str | None]


@dataclass
class QueuedRun:
    """A claimed run from the work queue."""

    id: int
    flow_selector: list[str]
    partition_start: str | None
    partition_end: str | None
    priority: int
    attempts: int
    restate: bool = False


def _stamp_actor(type: str, payload: dict[str, object] | None) -> dict[str, object] | None:
    """Record who triggered an apply or run. Other event types stay as written."""
    actor = event_actor.get()
    if not actor or not type.startswith(("apply.", "run.")):
        return payload
    stamped = dict(payload or {})
    stamped.setdefault("api_key", actor)
    return stamped


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _snapshot_to_row(
    snapshot: Snapshot,
) -> tuple[str, str, str, str, str | None, str | None, str, str, str, str, str, str, str]:
    t = snapshot.physical_table
    return (
        snapshot.name,
        snapshot.fingerprint,
        snapshot.local_fingerprint,
        snapshot.metadata_hash,
        snapshot.definition_sql,
        t.catalog,
        t.schema,
        t.name,
        snapshot.change_category.value,
        _now_iso(),
        snapshot.engine,
        snapshot.physical_hash,
        _objects_to_json(snapshot.physical_objects),
    )


def _objects_to_json(objects: tuple[PhysicalObject, ...]) -> str:
    return json.dumps(
        [{"kind": obj.kind, "name": obj.name, "column": obj.column} for obj in objects],
        separators=(",", ":"),
    )


def _objects_from_json(raw: str | None) -> tuple[PhysicalObject, ...]:
    if not raw:
        return ()
    return tuple(
        PhysicalObject(kind=item["kind"], name=item["name"], column=item.get("column")) for item in json.loads(raw)
    )


def _snapshot_from_row(row: sqlite3.Row, intervals: IntervalSet) -> Snapshot:
    keys = row.keys()
    return Snapshot(
        name=row["name"],
        fingerprint=row["fingerprint"],
        metadata_hash=row["metadata_hash"],
        physical_table=TableRef(
            schema=row["physical_schema"], name=row["physical_name"], catalog=row["physical_catalog"]
        ),
        change_category=ChangeCategory(row["change_category"]),
        intervals=intervals,
        local_fingerprint=row["local_fingerprint"],
        definition_sql=row["definition_sql"],
        engine=row["engine"] if "engine" in keys else "default",
        physical_hash=row["physical_hash"] if "physical_hash" in keys else "",
        physical_objects=_objects_from_json(row["physical_objects"]) if "physical_objects" in keys else (),
    )


def _intervals_from_rows(rows: Iterable[sqlite3.Row]) -> IntervalSet:
    return IntervalSet(
        Interval(datetime.fromisoformat(r["start_ts"]), datetime.fromisoformat(r["end_ts"])) for r in rows
    )
