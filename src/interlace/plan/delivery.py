"""Deliver a resolved query into an external table.

The target is never dropped. A first delivery runs the strategy against the
query; a later one stages or probes, then aligns columns before the write.
"""

from __future__ import annotations

from collections.abc import Mapping

from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.exceptions import PlanError
from interlace.graph.project import CompiledModel
from interlace.ir.layout import XFER_SCHEMA
from interlace.ir.relation import SqlRelation, TableRef, drop
from interlace.physical.reconcile import model_objects, object_changes, reconcile_statements
from interlace.physical.spec import PhysicalObject
from interlace.plan.fit import _prepare_alignment, _remember
from interlace.sinks import target_ref
from interlace.state.interval import Interval
from interlace.strategies import Strategy
from interlace.strategies.base import RowCounts


async def _physical_ddl(
    engine: EngineAdapter,
    model: CompiledModel,
    table: TableRef,
    previous: tuple[PhysicalObject, ...],
    *,
    same_table: bool,
) -> tuple[list[exp.Expr], tuple[PhysicalObject, ...], list[str]]:
    """Statements that reconcile interlace-owned indexes and constraints, plus the set to record.

    ``same_table`` is false for a brand-new snapshot table: the previous table's
    indexes die with it (or with gc) and must not be dropped here.
    """
    desired, warnings = model_objects(model, engine.caps)
    drops = object_changes(desired, previous)[1] if same_table else ()
    existing = set(await engine.list_constraints(table)) if same_table else set()
    statements = reconcile_statements(table, model, desired, drops, existing_constraints=existing)
    return statements, desired, warnings


async def _probe_columns(engine: EngineAdapter, name: str, query: exp.Query) -> dict[str, str]:
    """Column types of ``query`` from a ``LIMIT 0`` table, so a windowed delivery can fit
    without copying the source once per window."""
    probe = TableRef(schema=XFER_SCHEMA, name=f"{name.replace('.', '_')}__probe")
    await engine.create_schema(probe.schema)
    await engine.execute(exp.Create(this=probe.to_expr(), kind="TABLE", replace=True, expression=query.copy().limit(0)))
    try:
        return await engine.describe(probe)
    finally:
        await engine.execute(drop(probe.to_expr(), kind="TABLE"))


async def _execute_delivery(
    model: CompiledModel,
    engine: EngineAdapter,
    strategy: Strategy,
    resolved: exp.Query,
    interval: Interval | None,
    previous: tuple[PhysicalObject, ...],
    notes: list[str] | None,
    target: TableRef,
    *,
    same_table: bool,
    source_columns: Mapping[str, str] | None = None,
    source_from: exp.Expression | None = None,
) -> tuple[RowCounts, tuple[PhysicalObject, ...]]:
    """Align when the target already exists, then run the strategy and its DDL together."""
    pre: list[exp.Expr] = []
    aligned: exp.Query = resolved
    columns: list[str] | None = None
    if source_columns is not None and source_from is not None:
        pre, aligned, columns = _prepare_alignment(
            model, strategy, source_columns, source_from, target, await engine.describe(target), notes
        )
    ddl, objects, warnings = await _physical_ddl(engine, model, target, previous, same_table=same_table)
    for warning in warnings:
        _remember(notes, warning)
    planned = strategy.plan_statements(SqlRelation(ast=aligned), target, engine.caps, interval, columns)
    if source_columns is None:
        # The strategy's ensure-create makes the table; indexes land after it exists.
        counts = await engine.execute_all([*planned, *ddl])
        return planned.row_counts(counts[: len(planned)]), objects
    counts = await engine.execute_all([*pre, *ddl, *planned])
    return planned.row_counts(counts[len(pre) + len(ddl) :]), objects


class ExternalDelivery:
    """Where a terminal table is written, and the query that feeds the strategy.

    ``owned`` is a database opened because the model engine cannot ATTACH the
    target. ``close`` drops the staged source and closes that connection.
    """

    def __init__(
        self,
        engine: EngineAdapter,
        query: exp.Query,
        table: TableRef,
        *,
        owned: EngineAdapter | None = None,
        stage: TableRef | None = None,
    ) -> None:
        self.engine = engine
        self.query = query
        self.table = table
        self._owned = owned
        self._stage = stage

    async def close(self) -> None:
        if self._owned is None:
            return
        try:
            if self._stage is not None:
                await self._owned.execute(drop(self._stage.to_expr(), kind="TABLE"))
        finally:
            _close_engine(self._owned)


def _close_engine(engine: EngineAdapter) -> None:
    close = getattr(engine, "close", None)
    if callable(close):
        close()


def open_sink(uri: str) -> EngineAdapter:
    """Open a reverse-ETL target that the warehouse could not ATTACH."""
    if uri.startswith(("postgresql://", "postgres://")):
        from interlace.engines.postgres import PostgresAdapter

        return PostgresAdapter.connect(uri)
    if uri.startswith("postgres:"):
        raise PlanError(
            f"attach URI {uri!r} is DuckDB's postgres ATTACH form. A warehouse that cannot "
            f"ATTACH needs a postgresql:// URI to open that database itself."
        )
    from interlace.engines.duckdb import DuckDBAdapter

    return DuckDBAdapter.connect(uri)


async def open_external_delivery(
    model: CompiledModel,
    source: EngineAdapter,
    query: exp.Query,
    sinks: Mapping[str, str],
) -> ExternalDelivery:
    """Point delivery at the attached database, opening it when SQL cannot.

    DuckDB-family engines write ``alias.schema.table`` in one statement. Other
    engines fetch the model as Arrow and apply the strategy inside the target
    database, so a Postgres warehouse can still fill ``ext.main.crm_log``.
    """
    target = target_ref(model.target or "")
    if source.caps.supports_attach or not target.catalog:
        return ExternalDelivery(source, query, target)
    uri = sinks.get(target.catalog)
    if uri is None:
        raise PlanError(
            f"model {model.name!r} delivers to {model.target}, and its engine cannot ATTACH "
            f"{target.catalog!r}. Set attach.{target.catalog} to that database."
        )
    owned = open_sink(uri)
    stage = TableRef(schema=XFER_SCHEMA, name=f"{model.name.replace('.', '_')}__src")
    try:
        await owned.create_schema(stage.schema)
        await owned.load(stage, await source.fetch(query), "create")
    except Exception:
        _close_engine(owned)
        raise
    local = TableRef(schema=target.schema, name=target.name)
    staged: exp.Query = exp.select("*").from_(stage.to_expr())
    return ExternalDelivery(owned, staged, local, owned=owned, stage=stage)


async def _deliver_table(
    model: CompiledModel,
    engine: EngineAdapter,
    resolved: exp.Query,
    strategy: Strategy,
    interval: Interval | None,
    previous: tuple[PhysicalObject, ...] = (),
    notes: list[str] | None = None,
    destination: TableRef | None = None,
) -> tuple[RowCounts, tuple[PhysicalObject, ...]]:
    """Deliver ``resolved`` into an external table (``materialise: table``) via
    ``strategy`` (replace / append / merge / full_merge / incremental).

    The external target is never dropped (grants and readers survive). When it already
    exists the source is staged in the warehouse and aligned to the target (additive
    ALTERs, widening, casts) so a model that grows or reorders columns evolves the
    destination instead of breaking it or positionally corrupting it. ``schema.columns``
    selects that behaviour: ``additive`` (default), ``reject`` (fail before writing if
    the live table is not a compatible superset), or ``ignore`` (no ALTER; the insert
    fails if it does not fit). A strategy that names its columns is then planned against
    the model's own columns alone, leaving the rest of the target untouched (see
    :class:`Alignment`); a whole-row one gets the source widened to the target with
    NULLs, and binds positionally in the target's order.

    Indexes and constraints interlace recorded are reconciled in the same transaction
    as the delivery. Anything else on the table is left alone.

    The first delivery runs the strategy directly against the query (the ensure-create
    matches the source). A later windowed delivery probes ``LIMIT 0`` for column types
    and projects the query — it does not CTAS the whole source once per window. A later
    full delivery stages the source so the strategy reads a frozen copy."""
    target = destination or target_ref(model.target or "")
    exists = await engine.table_exists(target)
    if not exists:
        return await _execute_delivery(
            model, engine, strategy, resolved, interval, previous, notes, target, same_table=False
        )
    if interval is not None:
        source_columns = await _probe_columns(engine, model.name, resolved)
        source_from = exp.Subquery(this=resolved.copy(), alias=exp.TableAlias(this=exp.to_identifier("_src")))
        return await _execute_delivery(
            model,
            engine,
            strategy,
            resolved,
            interval,
            previous,
            notes,
            target,
            same_table=True,
            source_columns=source_columns,
            source_from=source_from,
        )
    stage = TableRef(schema=XFER_SCHEMA, name=f"{model.name}__sink_stage")
    await engine.create_schema(stage.schema)
    await engine.execute(exp.Create(this=stage.to_expr(), kind="TABLE", replace=True, expression=resolved.copy()))
    try:
        return await _execute_delivery(
            model,
            engine,
            strategy,
            resolved,
            interval,
            previous,
            notes,
            target,
            same_table=True,
            source_columns=await engine.describe(stage),
            source_from=stage.to_expr(),
        )
    finally:
        # The stage lives in the warehouse and is dropped outside the delivery
        # transaction: one transaction may write only one attached database.
        # A leftover is harmless — the next delivery CREATE OR REPLACEs it.
        await engine.execute(drop(stage.to_expr(), kind="TABLE"))
