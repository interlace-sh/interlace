"""Interval-set arithmetic — the backbone of backfill/catchup."""

from __future__ import annotations

from datetime import datetime

import pytest

from interlace.state.interval import Interval, IntervalSet

pytestmark = pytest.mark.unit


def d(day: int) -> datetime:
    return datetime(2026, 1, day)


def test_interval_rejects_empty_or_inverted() -> None:
    with pytest.raises(ValueError):
        Interval(d(2), d(2))
    with pytest.raises(ValueError):
        Interval(d(3), d(2))


def test_add_merges_overlapping_and_adjacent() -> None:
    s = IntervalSet([Interval(d(1), d(3)), Interval(d(3), d(5))])  # adjacent -> merge
    assert list(s) == [Interval(d(1), d(5))]
    s = s.add(Interval(d(2), d(4)))  # overlapping, already covered
    assert list(s) == [Interval(d(1), d(5))]


def test_difference_carves_out_the_middle() -> None:
    s = IntervalSet([Interval(d(1), d(10))])
    result = s.difference(IntervalSet([Interval(d(3), d(5))]))
    assert list(result) == [Interval(d(1), d(3)), Interval(d(5), d(10))]


def test_missing_finds_the_gaps_within_a_target() -> None:
    filled = IntervalSet([Interval(d(1), d(3)), Interval(d(6), d(8))])
    gaps = filled.missing(Interval(d(1), d(10)))
    assert list(gaps) == [Interval(d(3), d(6)), Interval(d(8), d(10))]


def test_covers_is_true_only_when_fully_filled() -> None:
    filled = IntervalSet([Interval(d(1), d(5))])
    assert filled.covers(Interval(d(2), d(4)))
    assert not filled.covers(Interval(d(2), d(6)))


def test_union_and_emptiness() -> None:
    a = IntervalSet([Interval(d(1), d(2))])
    b = IntervalSet([Interval(d(4), d(5))])
    assert len(a.union(b)) == 2
    assert IntervalSet().is_empty
    assert not a.is_empty
