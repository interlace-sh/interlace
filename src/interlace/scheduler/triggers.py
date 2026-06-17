"""The unified trigger abstraction.

Cron, interval, stream-append, table-freshness, upstream-completion, and webhook
are all :class:`Trigger` implementations that, given the current time and their
persisted state, emit zero or more :class:`RunRequest`s. This replaces v0.x's
cron-only scheduler loop and makes event-driven pipelines first-class.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from interlace.state.interval import Interval


@dataclass(frozen=True)
class RunRequest:
    """A request to run a selection of models, optionally for one data interval."""

    flow_selector: list[str]  # model names / tags / selector syntax
    partition: Interval | None = None
    priority: int = 0  # backfills enqueue negative so live work wins
    idempotency_key: str = ""  # dedupes refires, e.g. "cron:daily_sales:2026-06-12"


@dataclass
class TriggerState:
    """Durable per-trigger bookkeeping (last fire, cursor) owned by the state store."""

    trigger_id: str
    last_fired_at: datetime | None = None
    cursor: str | None = None  # e.g. last consumed event seq / log offset
    extra: dict[str, str] = field(default_factory=dict)


class Trigger(Protocol):
    """Evaluates whether work is due. Pure given (now, state) — side effects live elsewhere."""

    id: str

    def due(self, now: datetime, state: TriggerState) -> list[RunRequest]: ...
