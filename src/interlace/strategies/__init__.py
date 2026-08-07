"""Materialisation strategies and the resolver from a model's config to one."""

from __future__ import annotations

from collections.abc import Sequence

from interlace.exceptions import PlanError
from interlace.strategies.append import Append
from interlace.strategies.base import Strategy, table_expr
from interlace.strategies.full_merge import FullMerge
from interlace.strategies.hash_merge import HashMerge
from interlace.strategies.incremental import Incremental
from interlace.strategies.merge import Merge
from interlace.strategies.replace import Replace
from interlace.strategies.replace_in_place import ReplaceInPlace
from interlace.strategies.scd import Scd
from interlace.strategies.view import View

__all__ = [
    "Append",
    "FullMerge",
    "HashMerge",
    "Incremental",
    "Merge",
    "Replace",
    "ReplaceInPlace",
    "Scd",
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
    differ only in ``replace``: ``virtual`` rewrites the whole table (``Replace``,
    ``CREATE OR REPLACE``), ``table`` empties and re-fills it in place
    (``ReplaceInPlace``). ``append`` is external-only. ``view`` is virtual-plane
    only. ``file`` is delivered by a COPY, not a :class:`Strategy` (see
    ``sinks.file_statements``).
    """
    if materialise == "view":
        return View()
    if materialise in ("virtual", "table"):
        owned = materialise == "virtual"
        if strategy == "replace":
            return Replace() if owned else ReplaceInPlace()
        if strategy == "append":
            if owned:
                raise PlanError("append requires materialise: table (an external table)")
            return Append()
        if strategy == "merge":
            if not key:
                raise PlanError("merge requires a key", details={"materialise": materialise})
            return Merge(tuple(key))
        if strategy == "full_merge":
            if not key:
                raise PlanError("full_merge requires a key", details={"materialise": materialise})
            return FullMerge(tuple(key))
        if strategy == "hash_merge":
            if not key:
                raise PlanError("hash_merge requires a key", details={"materialise": materialise})
            return HashMerge(tuple(key))
        if strategy == "incremental_by_time":
            raise PlanError(
                "strategy: incremental_by_time was renamed to incremental — the behaviour is unchanged, "
                "and `key:` now additionally makes it upsert within the window instead of rewriting it",
                details={"materialise": materialise, "strategy": strategy},
            )
        if strategy == "incremental":
            if not time_column:
                raise PlanError("incremental requires a time_column", details={"materialise": materialise})
            return Incremental(time_column, tuple(key))
        if strategy == "scd":
            if not key:
                raise PlanError("scd requires a key", details={"materialise": materialise})
            return Scd(tuple(key), time_column)
    raise PlanError(
        f"unsupported materialise/strategy combination: {materialise!r}/{strategy!r}",
        details={"materialise": materialise, "strategy": strategy},
    )
