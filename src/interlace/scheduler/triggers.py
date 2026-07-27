"""Triggers — when a model should run.

One abstraction (``Trigger.due``) for all kinds; v1 ships cron and interval.
Cron expressions are parsed by ``cronsim`` (we own the loop — see TriggerEngine —
rather than delegating scheduling to APScheduler). A trigger is pure: given the
current time and when it last fired, it returns the runs that are now due.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

from cronsim import CronSim

from interlace.exceptions import DefinitionError
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
