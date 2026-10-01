"""Triggers — when a model should run.

One abstraction (``Trigger.due``) for every schedule. A cron or interval trigger
is pure: given the current time and when it last fired, it returns the runs that
are now due. A file watch hashes matching files. A table change and a freshness
window read one cell through a :class:`Probe` (the engine supplies it). Inbound
webhooks are not triggers; ``POST /hooks/{name}`` enqueues them.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

from cronsim import CronSim
from sqlglot import exp

from interlace.checks.builtin import max_is_stale, sql_interval
from interlace.exceptions import DefinitionError
from interlace.ir.relation import TableRef
from interlace.state.interval import Grain, Interval


@dataclass(frozen=True)
class RunRequest:
    """A request to run a selection of models, optionally for one data interval."""

    flow_selector: list[str]
    partition: Interval | None = None
    priority: int = 0
    idempotency_key: str = ""  # dedupes refires, e.g. "cron:daily_sales:2026-06-24T00:00:00"


class Absent:
    """Probe result: the source table does not exist yet. Distinct from a SQL NULL."""


ABSENT = Absent()


class Probe(Protocol):
    """Reads one cell. ``ABSENT`` when the table is not there; ``None`` when the cell is NULL."""

    async def scalar(self, engine: str, table: TableRef, query: exp.Expression) -> object | None: ...


class Trigger(Protocol):
    """Returns the runs due at ``now`` given when it last fired.

    ``probe`` is how a trigger reads a warehouse. Cron, interval, and file watch ignore it.
    """

    id: str

    async def due(self, now: datetime, last_fired: datetime | None, probe: Probe | None = None) -> list[RunRequest]: ...


def slot_stamp(now: datetime, every: timedelta) -> str:
    """UTC timestamp of the interval slot ``now`` falls in.

    A crash between enqueue and the last-fired write re-lands on the same key.
    UTC, because a naive local stamp repeats across the DST fall-back and would
    collapse two slots into one key.
    """
    seconds = max(1, int(every.total_seconds()))
    slot = int(now.timestamp()) // seconds * seconds
    return datetime.fromtimestamp(slot, tz=UTC).isoformat()


@dataclass
class CronTrigger:
    """Fires when a cron-scheduled time has passed since the last fire."""

    model: str
    expression: str
    id: str = field(init=False)

    def __post_init__(self) -> None:
        self.id = f"cron:{self.model}"
        try:
            CronSim(self.expression, datetime(2000, 1, 1))  # validate the expression
        except Exception as exc:
            raise DefinitionError(f"invalid cron {self.expression!r} for model {self.model!r}") from exc

    async def due(self, now: datetime, last_fired: datetime | None, probe: Probe | None = None) -> list[RunRequest]:
        del probe
        base = last_fired if last_fired is not None else now - timedelta(seconds=1)
        fire = next(CronSim(self.expression, base))
        if fire <= now:
            return [RunRequest([self.model], idempotency_key=f"cron:{self.model}:{fire.isoformat()}")]
        return []


@dataclass
class IntervalTrigger:
    """Fires once per ``every`` elapsed (and immediately on first sight)."""

    model: str
    every: timedelta
    id: str = field(init=False)

    def __post_init__(self) -> None:
        self.id = f"interval:{self.model}"

    async def due(self, now: datetime, last_fired: datetime | None, probe: Probe | None = None) -> list[RunRequest]:
        del probe
        if last_fired is None or now - last_fired >= self.every:
            # Key by the slot on the interval grid, not by ``now``: a crash between
            # enqueue and the last-fired write re-lands on the same key next start.
            stamp = slot_stamp(now, self.every)
            return [RunRequest([self.model], idempotency_key=f"interval:{self.model}:{stamp}")]
        return []


def file_fingerprint(root: Path, pattern: str) -> str | None:
    """Hash of each matching file's path, size, and mtime. ``None`` when nothing matches.

    Stdlib glob only — a directory watcher is deliberately not a dependency.
    """
    if Path(pattern).is_absolute():
        raise DefinitionError(f"watch pattern {pattern!r} must be relative to the project")
    matches = sorted(path for path in root.glob(pattern) if path.is_file())
    if not matches:
        return None
    digest = hashlib.sha256()
    for path in matches:
        stat = path.stat()
        relative = path.relative_to(root).as_posix()
        digest.update(f"{relative}\0{stat.st_size}\0{stat.st_mtime_ns}\n".encode())
    return digest.hexdigest()[:16]


@dataclass
class WatchTrigger:
    """Enqueues when the files matching ``pattern`` change (path, size, or mtime)."""

    model: str
    pattern: str
    root: Path
    id: str = field(init=False)

    def __post_init__(self) -> None:
        self.id = f"watch:{self.model}"
        file_fingerprint(self.root, self.pattern)  # reject an absolute pattern at construction

    async def due(self, now: datetime, last_fired: datetime | None, probe: Probe | None = None) -> list[RunRequest]:
        del now, last_fired, probe
        digest = file_fingerprint(self.root, self.pattern)
        if digest is None:
            return []
        return [RunRequest([self.model], idempotency_key=f"watch:{self.model}:{digest}")]


@dataclass
class OnChangeTrigger:
    """Enqueues when ``max(column)`` on ``table`` changes.

    A missing table waits. A null maximum is a watermark of ``"null"``, so the
    first rows still fire.
    """

    model: str
    column: str
    table: TableRef
    engine: str
    id: str = field(init=False)

    def __post_init__(self) -> None:
        self.id = f"change:{self.model}"

    async def due(self, now: datetime, last_fired: datetime | None, probe: Probe | None = None) -> list[RunRequest]:
        del now, last_fired
        value = await _cell(probe, self.engine, self.table, change_query(self.table, self.column))
        if value is ABSENT:
            return []
        return [self.request(_watermark(value))]

    def request(self, watermark: str) -> RunRequest:
        return RunRequest([self.model], idempotency_key=f"change:{self.model}:{watermark}")


def change_query(table: TableRef, column: str) -> exp.Select:
    """``SELECT max(column) AS watermark FROM table``."""
    return exp.select(exp.alias_(exp.Max(this=exp.to_identifier(column)), "watermark")).from_(table.to_expr())


def _watermark(value: object | None) -> str:
    if value is None:
        return "null"
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


async def _cell(probe: Probe | None, engine: str, table: TableRef, query: exp.Expression) -> object | None:
    if probe is None:
        raise DefinitionError("a column schedule needs the project's engines")
    return await probe.scalar(engine, table, query)


@dataclass
class FreshTrigger:
    """Enqueues when ``max(column)`` is older than ``grain``.

    A missing source table waits. An empty table, or a null maximum, is stale.
    While it stays stale, one run is enqueued per window. The column is a timestamp.
    """

    model: str
    column: str
    table: TableRef
    engine: str
    grain: Grain
    id: str = field(init=False)

    def __post_init__(self) -> None:
        self.id = f"fresh:{self.model}"

    async def due(self, now: datetime, last_fired: datetime | None, probe: Probe | None = None) -> list[RunRequest]:
        del last_fired
        stale = await _cell(probe, self.engine, self.table, fresh_query(self.table, self.column, self.grain))
        if stale is ABSENT or not stale:
            return []
        return [self.request(now)]

    def request(self, now: datetime) -> RunRequest:
        stamp = slot_stamp(now, self.grain.every)
        return RunRequest([self.model], idempotency_key=f"fresh:{self.model}:{stamp}")


def fresh_query(table: TableRef, column: str, grain: Grain) -> exp.Select:
    """``max(column)`` is null or older than ``grain`` (already parsed; a grain like ``2h``)."""
    stale = max_is_stale(exp.to_identifier(column), sql_interval(grain.amount, grain.sql_unit))
    return exp.select(exp.alias_(stale, "stale")).from_(table.to_expr())
