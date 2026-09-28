"""Fit a staged source onto an existing target table.

Additive column adds, numeric widens, and casts live here so delivery and Python
merges share one alignment. Strategies that name their columns see only the
model's own projection; whole-row strategies see the target's full width.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass, field

from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.exceptions import PlanError
from interlace.graph.project import CompiledModel
from interlace.ir.relation import TableRef
from interlace.physical.drift import same_type, widens
from interlace.strategies import Strategy

logger = logging.getLogger("interlace.apply")


@dataclass(frozen=True)
class Alignment:
    """A staged source fitted to an existing target, in both widths.

    ``source``/``columns`` cover the target's full column set — what a strategy that
    binds positionally or compares whole rows (``INSERT ... SELECT *``, ``EXCEPT``)
    needs, with columns the model doesn't produce NULL-filled in.
    ``produced_source``/``produced`` cover only the model's own columns; a strategy
    that names its columns takes these instead, so the rest of the target is left
    alone on an update and takes its DEFAULTs on an insert. ``unproduced`` names the
    difference — target columns this model has no value for."""

    pre_statements: list[exp.Expr]
    source: exp.Query
    columns: list[str]
    produced_source: exp.Query
    produced: list[str]
    unproduced: list[str]
    added: list[str] = field(default_factory=list)  # columns the model has that the target does not
    casts: list[str] = field(default_factory=list)  # type mismatches that are not numeric widens

    def for_strategy(self, strategy: Strategy) -> tuple[exp.Query, list[str]]:
        """The (source, column list) pair this strategy should be planned against."""
        if strategy.writes_named_columns:
            return self.produced_source, self.produced
        return self.source, self.columns


def _fit_columns(
    source_columns: Mapping[str, str],
    source_from: exp.Expression,
    target: TableRef,
    target_columns: dict[str, str],
    strategy: Strategy,
) -> Alignment:
    """Fit a described source to an existing target.

    Additive ALTERs for new columns, widening promotions in place, and a projection
    over ``source_from`` (a stage table, or the query itself) in the target's final
    column order. Managed bookkeeping columns stay on the target and out of both
    projections — the strategy owns them."""
    _require_managed_columns(strategy, target, target_columns)
    exclude = strategy.managed_columns
    for column in exclude:
        target_columns.pop(column, None)
    if clash := [c for c in source_columns if c in exclude]:
        raise PlanError(
            f"model output column {clash[0]!r} collides with a column the "
            f"{_strategy_name(strategy)} strategy manages — rename it in the model",
            details={"target": target.to_expr().sql(), "columns": clash},
        )
    target_expr = target.to_expr()
    pre_statements: list[exp.Expr] = []
    added: list[str] = []
    for column, dtype in source_columns.items():
        if column not in target_columns:
            added.append(column)
            pre_statements.append(
                exp.Alter(
                    this=target_expr.copy(),
                    kind="TABLE",
                    actions=[
                        exp.ColumnDef(this=exp.to_identifier(column), kind=exp.DataType.build(dtype)),
                    ],
                )
            )
            target_columns[column] = dtype
        elif not same_type(dtype, target_columns[column]) and widens(target_columns[column], dtype):
            # Source type drifted wider (int -> bigint -> double): promote in place.
            pre_statements.append(
                exp.Alter(
                    this=target_expr.copy(),
                    kind="TABLE",
                    actions=[exp.AlterColumn(this=exp.column(column), dtype=exp.DataType.build(dtype))],
                )
            )
            target_columns[column] = dtype
    # Any remaining type mismatch (e.g. a numeric field arriving as VARCHAR) is cast
    # to the target's type — deterministic, and loudly fails the run on values that
    # genuinely don't convert rather than silently corrupting the column.
    projection: list[exp.Expr] = []
    produced_projection: list[exp.Expr] = []
    produced: list[str] = []
    unproduced: list[str] = []
    casts: list[str] = []
    for column, dtype in target_columns.items():
        if column not in source_columns:
            projection.append(exp.alias_(exp.Cast(this=exp.Null(), to=exp.DataType.build(dtype)), column))
            unproduced.append(column)
            continue
        if not same_type(source_columns[column], dtype):
            casts.append(f"{column} {source_columns[column]} -> {dtype}")
            fitted: exp.Expr = exp.alias_(exp.Cast(this=exp.column(column), to=exp.DataType.build(dtype)), column)
        else:
            fitted = exp.column(column)
        projection.append(fitted)
        produced_projection.append(fitted.copy())
        produced.append(column)
    origin = source_from.copy()
    return Alignment(
        pre_statements=pre_statements,
        source=exp.select(*projection).from_(origin),
        columns=list(target_columns),
        produced_source=exp.select(*produced_projection).from_(origin.copy()),
        produced=produced,
        unproduced=unproduced,
        added=added,
        casts=casts,
    )


async def _align_stage_to_target(
    engine: EngineAdapter, stage: TableRef, target: TableRef, strategy: Strategy
) -> Alignment:
    """Describe a staged source and fit it to an existing target. See :func:`_fit_columns`."""
    return _fit_columns(
        await engine.describe(stage), stage.to_expr(), target, dict(await engine.describe(target)), strategy
    )


def _strategy_name(strategy: Strategy) -> str:
    """The strategy's config keyword (``HashMerge`` -> ``hash_merge``), for messages."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", type(strategy).__name__).lower()


def _require_managed_columns(strategy: Strategy, target: TableRef, target_columns: Mapping[str, str]) -> None:
    """A strategy that keeps bookkeeping columns can only take over a table that already
    carries them. Missing ones would otherwise surface as an engine binder error deep in
    the strategy's UPDATE — most likely an externally-owned table that interlace did not
    create, or a strategy swapped under an existing delivery target.

    Nothing to say when the target describes empty: it isn't really there (a snapshot row
    can outlive its table), so the caller is not taking over anything. Matching is
    case-insensitive — an engine that folds unquoted identifiers (Snowflake, BigQuery)
    stores ``_hash`` as ``_HASH`` and would otherwise trip this on every run."""
    if not target_columns:
        return
    present = {c.casefold() for c in target_columns}
    missing = [c for c in strategy.managed_columns if c.casefold() not in present]
    if missing:
        name = _strategy_name(strategy)
        qualified = target.to_expr().sql()
        raise PlanError(
            f"{name} needs its bookkeeping column(s) {', '.join(missing)} on {qualified}, which already "
            f"exists without them — interlace did not create this table. Use strategy: merge, or add "
            f"the column(s) to it.",
            details={"target": qualified, "missing": missing, "strategy": name},
        )


def _remember(notes: list[str] | None, warning: str) -> None:
    if notes is not None and warning not in notes:
        notes.append(warning)


def _prepare_alignment(
    model: CompiledModel,
    strategy: Strategy,
    source_columns: Mapping[str, str],
    source_from: exp.Expression,
    target: TableRef,
    target_columns: Mapping[str, str],
    notes: list[str] | None,
) -> tuple[list[exp.Expr], exp.Query, list[str] | None]:
    """Fit an existing target to a described source. ``ignore`` skips ALTERs."""
    policy = model.schema_policy.columns
    qualified = target.to_expr().sql()
    if policy == "ignore":
        _require_managed_columns(strategy, target, target_columns)
        extras = [
            column
            for column in target_columns
            if column not in source_columns and column not in strategy.managed_columns
        ]
        if extras:
            _remember(
                notes,
                f"{model.name}: {qualified} has columns the model does not produce ({', '.join(extras)}); left in place",
            )
        names = list(source_columns) if strategy.writes_named_columns else None
        return [], exp.select("*").from_(source_from.copy()), names
    alignment = _fit_columns(source_columns, source_from, target, dict(target_columns), strategy)
    if policy == "reject" and (alignment.added or alignment.casts):
        missing = ", ".join(alignment.added) or "none"
        drifted = ", ".join(alignment.casts) or "none"
        raise PlanError(
            f"{model.name}: schema.columns is reject and {qualified} is not a compatible "
            f"superset (missing columns: {missing}; type drift: {drifted})",
            details={"model": model.name, "target": model.target},
        )
    if alignment.unproduced:
        _remember(
            notes,
            f"{model.name}: {qualified} has columns the model does not produce "
            f"({', '.join(alignment.unproduced)}); left in place",
        )
    if alignment.unproduced and not strategy.writes_named_columns:
        logger.warning(
            "%s: strategy %s writes whole rows into %s — it resets columns this model does not "
            "produce (%s) on every delivery, and (replace, full_merge) deletes rows it does not "
            "supply. Use merge, hash_merge or append if another writer owns part of this table.",
            model.name,
            model.strategy,
            qualified,
            ", ".join(alignment.unproduced),
        )
    aligned, columns = alignment.for_strategy(strategy)
    return alignment.pre_statements, aligned, columns
