"""Half-open time intervals and sets of them.

The interval ledger is how backfill, catchup, and restatement become set
arithmetic instead of bespoke logic (sqlmesh's strongest idea): a snapshot
records which ``[start, end)`` ranges are filled, and "what needs running" is
just ``target.difference(filled)``. Stream cursors reuse the same structure with
offset-shaped intervals.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime, timedelta

_GRAIN_UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}
_GRAIN_RE = re.compile(r"(\d+)([smhdw])")


@dataclass(frozen=True, order=True)
class Interval:
    """A half-open interval ``[start, end)``. ``start`` must be strictly before ``end``."""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError(f"interval start must be before end: {self.start!r} >= {self.end!r}")

    def overlaps(self, other: Interval) -> bool:
        return other.start < self.end and other.end > self.start

    def subtract(self, other: Interval) -> list[Interval]:
        """Remove ``other`` from this interval, yielding 0, 1, or 2 remaining pieces."""
        if not self.overlaps(other):
            return [self]
        pieces = []
        if self.start < other.start:
            pieces.append(Interval(self.start, min(self.end, other.start)))
        if other.end < self.end:
            pieces.append(Interval(max(self.start, other.end), self.end))
        return pieces


def _normalise(intervals: Iterable[Interval]) -> tuple[Interval, ...]:
    """Sort and merge overlapping or touching intervals into a canonical form."""
    ordered = sorted(intervals)
    if not ordered:
        return ()
    merged: list[Interval] = [ordered[0]]
    for iv in ordered[1:]:
        last = merged[-1]
        if iv.start <= last.end:  # overlapping or adjacent (half-open) -> merge
            merged[-1] = Interval(last.start, max(last.end, iv.end))
        else:
            merged.append(iv)
    return tuple(merged)


class IntervalSet:
    """An immutable, normalised set of non-overlapping intervals."""

    __slots__ = ("_intervals",)

    def __init__(self, intervals: Iterable[Interval] = ()) -> None:
        self._intervals = _normalise(intervals)

    def __iter__(self) -> Iterator[Interval]:
        return iter(self._intervals)

    def __len__(self) -> int:
        return len(self._intervals)

    def __bool__(self) -> bool:
        return bool(self._intervals)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, IntervalSet):
            return NotImplemented
        return self._intervals == other._intervals

    def __repr__(self) -> str:
        inner = ", ".join(f"[{iv.start.isoformat()}, {iv.end.isoformat()})" for iv in self._intervals)
        return f"IntervalSet({inner})"

    @property
    def is_empty(self) -> bool:
        return not self._intervals

    def add(self, interval: Interval) -> IntervalSet:
        """Return a new set with ``interval`` merged in."""
        return IntervalSet([*self._intervals, interval])

    def union(self, other: IntervalSet) -> IntervalSet:
        return IntervalSet([*self._intervals, *other._intervals])

    def difference(self, other: IntervalSet) -> IntervalSet:
        """Return the parts of this set not covered by ``other``."""
        result: list[Interval] = []
        for iv in self._intervals:
            segments = [iv]
            for o in other._intervals:
                segments = [piece for seg in segments for piece in seg.subtract(o)]
            result.extend(segments)
        return IntervalSet(result)

    def missing(self, target: Interval) -> IntervalSet:
        """The gaps within ``target`` that this set does not yet cover."""
        return IntervalSet([target]).difference(self)

    def covers(self, target: Interval) -> bool:
        """True if ``target`` is fully contained in this set."""
        return self.missing(target).is_empty


def parse_grain(grain: str) -> timedelta:
    """Parse a grain like ``1d``, ``6h``, ``15m`` into a timedelta. (``m`` = minutes.)"""
    match = _GRAIN_RE.fullmatch(grain.strip())
    if match is None:
        raise ValueError(f"invalid grain {grain!r}; expected like '1d', '6h', '15m'")
    return timedelta(**{_GRAIN_UNITS[match.group(2)]: int(match.group(1))})


def slice_interval(interval: Interval, grain: timedelta) -> list[Interval]:
    """Split ``interval`` into consecutive grain-sized buckets (last may be partial)."""
    buckets: list[Interval] = []
    start = interval.start
    while start < interval.end:
        end = min(start + grain, interval.end)
        buckets.append(Interval(start, end))
        start = end
    return buckets
