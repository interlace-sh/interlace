"""Triggers — when a model should run.

One abstraction (``Trigger.due``) for cron, interval, and file watch. A cron or
interval trigger is pure: given the current time and when it last fired, it
returns the runs that are now due. A file watch hashes matching files. A table
change (``OnChangeTrigger``) is probed by the engine, which reads ``max(column)``
from a source table. A freshness trigger (``FreshTrigger``) enqueues when that
maximum is older than a window. Inbound webhooks are not triggers;
``POST /hooks/{name}`` enqueues them.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from cronsim import CronSim
from sqlglot import exp

from interlace.exceptions import DefinitionError
from interlace.ir.relation import TableRef
from interlace.state.interval import Interval


@dataclass(frozen=True)
class RunRequest:
    """A request to run a selection of models, optionally for one data interval."""

    flow_selector: list[str]
    partition: Interval | None = None
    priority: int = 0
    idempotency_key: str = ""  # dedupes refires, e.g. "cron:daily_sales:2026-06-24T00:00:00"


class Trigger(Protocol):
    """Returns the runs due at ``now`` given when it last fired."""

    id: str

    def due(self, now: datetime, last_fired: datetime | None) -> list[RunRequest]: ...


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

    def due(self, now: datetime, last_fired: datetime | None) -> list[RunRequest]:
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

    def due(self, now: datetime, last_fired: datetime | None) -> list[RunRequest]:
        if last_fired is None or now - last_fired >= self.every:
            # Key by the slot on the interval grid, not by ``now``: a crash between
            # enqueue and the last-fired write re-lands on the SAME key next start,
            # so the durable queue dedupes instead of running the model twice.
            # Stamped in UTC — a naive local stamp repeats across the DST fall-back,
            # which would collide two different slots into one key (a missed fire).
            seconds = max(1, int(self.every.total_seconds()))
            slot = int(now.timestamp()) // seconds * seconds
            stamp = datetime.fromtimestamp(slot, tz=UTC).isoformat()
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

    def due(self, now: datetime, last_fired: datetime | None) -> list[RunRequest]:
        del now, last_fired
        digest = file_fingerprint(self.root, self.pattern)
        if digest is None:
            return []
        return [RunRequest([self.model], idempotency_key=f"watch:{self.model}:{digest}")]


@dataclass
class OnChangeTrigger:
    """Enqueued by the engine when ``max(column)`` on ``table`` changes.

    ``due`` is unused. The engine probes the warehouse and calls :meth:`request`.
    """

    model: str
    column: str
    table: TableRef
    engine: str
    id: str = field(init=False)

    def __post_init__(self) -> None:
        self.id = f"change:{self.model}"

    def due(self, now: datetime, last_fired: datetime | None) -> list[RunRequest]:
        del now, last_fired
        return []

    def request(self, watermark: str) -> RunRequest:
        return RunRequest([self.model], idempotency_key=f"change:{self.model}:{watermark}")


def change_query(table: TableRef, column: str) -> exp.Select:
    """``SELECT max(column) AS watermark FROM table``."""
    return exp.select(exp.alias_(exp.Max(this=exp.to_identifier(column)), "watermark")).from_(table.to_expr())


_FRESH_UNITS = {"s": "SECOND", "m": "MINUTE", "h": "HOUR", "d": "DAY", "w": "WEEK"}
_FRESH_RE = re.compile(r"^(\d+)([smhdw])$")


def normalize_schedule(schedule: Mapping[str, Any] | None, *, model: str) -> dict[str, str] | None:
    """Schedules are string values. ``fresh: {column, within}`` becomes ``"column within"``."""
    if not schedule:
        return None
    normalised: dict[str, str] = {}
    for key, value in schedule.items():
        if key == "fresh" and isinstance(value, dict):
            column = value.get("column")
            window = value.get("within")
            if not isinstance(column, str) or not column.strip():
                raise DefinitionError(f"model {model!r}: fresh.column must name a timestamp column")
            if not isinstance(window, str) or not window.strip():
                raise DefinitionError(f"model {model!r}: fresh.within must be a window like '2h'")
            normalised[key] = f"{column.strip()} {window.strip()}"
            continue
        if not isinstance(value, str):
            raise DefinitionError(f"model {model!r}: schedule {key!r} must be a string")
        normalised[key] = value
    return normalised


@dataclass
class FreshTrigger:
    """Enqueued by the engine when ``max(column)`` is older than ``window``.

    A missing source table waits. An empty table, or a null maximum, is stale.
    While it stays stale, one run is enqueued per window.
    """

    model: str
    column: str
    table: TableRef
    engine: str
    window: str
    within: timedelta
    id: str = field(init=False)

    def __post_init__(self) -> None:
        self.id = f"fresh:{self.model}"

    def due(self, now: datetime, last_fired: datetime | None) -> list[RunRequest]:
        del now, last_fired
        return []

    def request(self, now: datetime) -> RunRequest:
        seconds = max(1, int(self.within.total_seconds()))
        slot = int(now.timestamp()) // seconds * seconds
        stamp = datetime.fromtimestamp(slot, tz=UTC).isoformat()
        return RunRequest([self.model], idempotency_key=f"fresh:{self.model}:{stamp}")


def fresh_query(table: TableRef, column: str, window: str) -> exp.Select:
    """``max(column)`` is null or older than ``window`` (a grain like ``2h``)."""
    match = _FRESH_RE.fullmatch(window.strip())
    if match is None:
        raise DefinitionError(f"invalid freshness window {window!r}; expected like '2h', '30m', '1d'")
    newest = exp.Max(this=exp.to_identifier(column))
    interval = exp.Interval(this=exp.Literal.string(match.group(1)), unit=exp.Var(this=_FRESH_UNITS[match.group(2)]))
    threshold = exp.Sub(this=exp.CurrentTimestamp(), expression=interval)
    stale = exp.or_(exp.LT(this=newest, expression=threshold), newest.copy().is_(exp.null()))
    return exp.select(exp.alias_(stale, "stale")).from_(table.to_expr())
