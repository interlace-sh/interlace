"""Materialisation strategies and the resolver from a model's config to one."""

from __future__ import annotations

from collections.abc import Sequence

from interlace.exceptions import PlanError
from interlace.strategies.base import Strategy, table_expr
from interlace.strategies.full import FullRefresh
from interlace.strategies.incremental_by_time import IncrementalByTime
from interlace.strategies.merge_by_key import MergeByKey
from interlace.strategies.scd_type_2 import ScdType2
from interlace.strategies.view import View

__all__ = [
    "FullRefresh",
    "IncrementalByTime",
    "MergeByKey",
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

    Supports ``view``; ``table`` + ``full`` / ``merge_by_key`` /
    ``incremental_by_time`` / ``scd_type_2`` (alias ``scd2``).
    """
    if materialise == "view":
        return View()
    if materialise == "table":
        if strategy == "full":
            return FullRefresh()
        if strategy == "merge_by_key":
            if not key:
                raise PlanError("merge_by_key requires a key", details={"materialise": materialise})
            return MergeByKey(tuple(key))
        if strategy == "incremental_by_time":
            if not time_column:
                raise PlanError("incremental_by_time requires a time_column", details={"materialise": materialise})
            return IncrementalByTime(time_column)
        if strategy in ("scd_type_2", "scd2"):
            if not key:
                raise PlanError("scd_type_2 requires a key", details={"materialise": materialise})
            return ScdType2(tuple(key))
    raise PlanError(
        f"unsupported materialise/strategy combination: {materialise!r}/{strategy!r}",
        details={"materialise": materialise, "strategy": strategy},
    )
