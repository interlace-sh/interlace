"""Materialisation strategies and the resolver from a model's config to one."""

from __future__ import annotations

from interlace.exceptions import PlanError
from interlace.strategies.base import Strategy, table_expr
from interlace.strategies.full import FullRefresh
from interlace.strategies.view import View

__all__ = ["FullRefresh", "Strategy", "View", "resolve_strategy", "table_expr"]


def resolve_strategy(materialise: str, strategy: str) -> Strategy:
    """Pick the strategy for a model's ``materialise``/``strategy`` config.

    v1 supports ``view`` and ``table`` + ``full``; merge_by_key, scd2,
    incremental, ephemeral, and none land in later phases.
    """
    if materialise == "view":
        return View()
    if materialise == "table" and strategy == "full":
        return FullRefresh()
    raise PlanError(
        f"unsupported materialise/strategy combination: {materialise!r}/{strategy!r}",
        details={"materialise": materialise, "strategy": strategy},
    )
