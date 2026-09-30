"""Apply bookkeeping: what a plan execution built, reused, and how long it took."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from interlace.checks.runner import CheckOutcome
from interlace.engines.base import statement_of
from interlace.exceptions import InterlaceError
from interlace.strategies.base import RowCounts


@dataclass
class ApplyResult:
    built: list[str] = field(default_factory=list)
    reused: list[str] = field(default_factory=list)  # recorded over their previous physical table
    gated: list[str] = field(default_factory=list)  # terminals recorded but not delivered (environment gate)
    transfers: list[str] = field(default_factory=list)  # executed cross-engine transfers
    promoted: int = 0
    checks: list[CheckOutcome] = field(default_factory=list)
    # Wall-clock build seconds per built model (extraction + strategy + checks).
    timings: dict[str, float] = field(default_factory=dict)
    # What each build did to its target's rows, as its strategy interprets the
    # engine's affected-row counts. Interval windows accumulate.
    rows: dict[str, RowCounts] = field(default_factory=dict)
    registered: list[str] = field(default_factory=list)  # models a Python model registered while building

    def record_rows(self, name: str, counts: RowCounts) -> None:
        self.rows[name] = self.rows.get(name, RowCounts()) + counts


ProgressCallback = Callable[[str, str, dict[str, Any]], None]


def build_detail(result: ApplyResult, name: str) -> dict[str, Any]:
    """What a finished model did: wall-clock seconds and the row delta."""
    detail: dict[str, Any] = {"seconds": round(result.timings.get(name, 0.0), 3)}
    counts = result.rows.get(name)
    if counts is not None:
        detail["rows"] = {"inserted": counts.inserted, "updated": counts.updated, "deleted": counts.deleted}
    return detail


def failure_detail(exc: BaseException) -> dict[str, Any]:
    """The message and, when an engine statement failed, the SQL that failed."""
    if isinstance(exc, InterlaceError):
        message = exc.message
    else:
        message = (str(exc).strip().splitlines() or [type(exc).__name__])[0]
    detail: dict[str, Any] = {"message": message}
    statement = statement_of(exc)
    if statement:
        detail["statement"] = statement
    return detail


def record_timing(result: ApplyResult, name: str, started: float) -> None:
    result.timings[name] = result.timings.get(name, 0.0) + (time.perf_counter() - started)


def record_build(result: ApplyResult, name: str, started: float) -> None:
    """One ``built`` entry per model, timings summed across interval windows."""
    if name not in result.built:
        result.built.append(name)
    record_timing(result, name, started)
