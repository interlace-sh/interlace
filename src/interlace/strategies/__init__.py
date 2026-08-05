"""Materialisation strategies and the resolver from a model's config to one."""

from __future__ import annotations

from collections.abc import Sequence

from interlace.exceptions import PlanError
from interlace.strategies.append import Append
from interlace.strategies.base import Strategy, table_expr
from interlace.strategies.full import FullRefresh
from interlace.strategies.full_merge import FullMerge
from interlace.strategies.incremental_by_time import IncrementalByTime
from interlace.strategies.merge_by_key import MergeByKey
from interlace.strategies.replace_in_place import ReplaceInPlace
from interlace.strategies.scd_type_2 import ScdType2
from interlace.strategies.view import View

__all__ = [
    "Append",
    "FullMerge",
    "FullRefresh",
    "IncrementalByTime",
    "MergeByKey",
    "ReplaceInPlace",
    "ScdType2",
    "Strategy",
    "View",
    "resolve_strategy",
    "table_expr",
]


def resolve_strategy(
    materialise: str,
    strategy: str,
    key: Sequence[str] = (),
    time_column: str | None = None,
) -> Strategy:
    """Pick the strategy for a model's ``materialise``/``strategy`` config.

    Two planes carry data strategies. ``virtual`` (interlace owns a fresh
    fingerprinted table it may replace) and ``table`` (an external table interlace
    delivers into but never drops) share the keyed and windowed strategies; they
    differ only in ``full``: ``virtual`` rewrites the whole table (``FullRefresh``,
    ``CREATE OR REPLACE``), ``table`` empties and re-fills it in place
    (``ReplaceInPlace``). ``append`` is external-only. ``view`` is virtual-plane
    only. ``file`` is delivered by a COPY, not a :class:`Strategy` (see
    ``sinks.file_statements``).
    """
    if materialise == "view":
        return View()
    if materialise in ("virtual", "table"):
        owned = materialise == "virtual"
        if strategy == "full":
            return FullRefresh() if owned else ReplaceInPlace()
        if strategy == "append":
            if owned:
                raise PlanError("append requires materialise: table (an external table)")
            return Append()
        if strategy == "merge_by_key":
            if not key:
                raise PlanError("merge_by_key requires a key", details={"materialise": materialise})
            return MergeByKey(tuple(key))
        if strategy == "full_merge":
            if not key:
                raise PlanError("full_merge requires a key", details={"materialise": materialise})
            return FullMerge(tuple(key))
        if strategy == "incremental_by_time":
            if not time_column:
                raise PlanError("incremental_by_time requires a time_column", details={"materialise": materialise})
            return IncrementalByTime(time_column)
        if strategy == "scd_type_2":
            if not key:
                raise PlanError("scd_type_2 requires a key", details={"materialise": materialise})
            return ScdType2(tuple(key))
    raise PlanError(
        f"unsupported materialise/strategy combination: {materialise!r}/{strategy!r}",
        details={"materialise": materialise, "strategy": strategy},
    )
