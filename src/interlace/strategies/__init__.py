"""Materialisation strategies and the resolver from a model's config to one."""

from __future__ import annotations

from collections.abc import Sequence

from interlace.exceptions import PlanError
from interlace.strategies.base import Strategy, table_expr
from interlace.strategies.full import FullRefresh
from interlace.strategies.merge_by_key import MergeByKey
from interlace.strategies.view import View

__all__ = ["FullRefresh", "MergeByKey", "Strategy", "View", "resolve_strategy", "table_expr"]


def resolve_strategy(materialise: str, strategy: str, key: Sequence[str] = ()) -> Strategy:
    """Pick the strategy for a model's ``materialise``/``strategy`` config.

    v1 supports ``view``, ``table`` + ``full``, and ``table`` + ``merge_by_key``;
    scd2 and incremental_by_time land in later phases.
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
    raise PlanError(
        f"unsupported materialise/strategy combination: {materialise!r}/{strategy!r}",
        details={"materialise": materialise, "strategy": strategy},
    )
