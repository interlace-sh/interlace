"""Interval-set arithmetic — the backbone of backfill/catchup."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from interlace.state.interval import Interval, IntervalSet, parse_grain, slice_interval

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


def test_parse_grain() -> None:
    assert parse_grain("1d") == timedelta(days=1)
    assert parse_grain("6h") == timedelta(hours=6)
    assert parse_grain("15m") == timedelta(minutes=15)
    with pytest.raises(ValueError):
        parse_grain("nonsense")


def test_slice_interval_into_grain_buckets() -> None:
    buckets = slice_interval(Interval(d(1), d(4)), parse_grain("1d"))
    assert buckets == [Interval(d(1), d(2)), Interval(d(2), d(3)), Interval(d(3), d(4))]


def test_slice_interval_clamps_partial_last_bucket() -> None:
    buckets = slice_interval(Interval(d(1), d(2)), timedelta(hours=10))
    assert len(buckets) == 3  # 10h, 10h, 4h
    assert buckets[-1].end == d(2)
