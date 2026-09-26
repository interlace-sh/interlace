"""Schema + row compare of two tables (or a model across two environments).

The SQLMesh / dbt Fusion Compare week-one hole: ``interlace diff`` joins on a
key (the model's ``key:``, ``--on``, or every common column) and reports schema
drift, counts, and a bounded sample of left-only / right-only / changed rows.
Queries are ASTs we build against tables the project already owns.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import cast

from sqlglot import exp

from interlace.engines.base import EngineAdapter
from interlace.engines.registry import EngineRegistry
from interlace.exceptions import PlanError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.inspect import json_cell, model_relation
from interlace.ir.relation import TableRef
from interlace.state.store import StateStore

SAMPLE_LIMIT = 20


@dataclass
class SchemaDelta:
    added: list[tuple[str, str]] = field(default_factory=list)  # (name, type) on the right
    removed: list[tuple[str, str]] = field(default_factory=list)  # on the left
    type_changed: list[tuple[str, str, str]] = field(default_factory=list)  # name, left, right

    @property
    def empty(self) -> bool:
        return not (self.added or self.removed or self.type_changed)


@dataclass
class RowDelta:
    left_count: int
    right_count: int
    left_only: int
    right_only: int
    changed: int
    matched: int
    keys: list[str]
    left_only_sample: list[dict[str, object]] = field(default_factory=list)
    right_only_sample: list[dict[str, object]] = field(default_factory=list)
    changed_sample: list[dict[str, object]] = field(default_factory=list)


@dataclass
class TableDiff:
    left: str
    right: str
    model: str | None
    schema: SchemaDelta
    rows: RowDelta | None
    message: str | None = None

    @property
    def differs(self) -> bool:
        if self.message:
            return True
        if not self.schema.empty:
            return True
        if self.rows is None:
            return False
        return bool(self.rows.left_only or self.rows.right_only or self.rows.changed)


def parse_table_ref(value: str) -> TableRef:
    """``catalog.schema.table``, ``schema.table``, or ``table`` (schema ``main``)."""
    parts = [part for part in value.split(".") if part]
    if not parts or len(parts) > 3:
        raise PlanError(f"table {value!r} must be table, schema.table, or catalog.schema.table")
    if len(parts) == 3:
        return TableRef(catalog=parts[0], schema=parts[1], name=parts[2])
    if len(parts) == 2:
        return TableRef(schema=parts[0], name=parts[1])
    return TableRef(schema="main", name=parts[0])


def _qualify(table: TableRef) -> str:
    return table.to_expr().sql()


async def diff_tables(
    engine: EngineAdapter,
    left: TableRef,
    right: TableRef,
    *,
    keys: list[str] | None = None,
    sample: int = SAMPLE_LIMIT,
) -> TableDiff:
    """Compare two live tables on ``engine``."""
    left_schema = await engine.describe(left)
    right_schema = await engine.describe(right)
    if not left_schema and not right_schema:
        raise PlanError(f"neither {_qualify(left)} nor {_qualify(right)} exists")
    schema = _schema_delta(left_schema, right_schema)
    if not left_schema or not right_schema:
        missing = _qualify(left) if not left_schema else _qualify(right)
        return TableDiff(
            left=_qualify(left),
            right=_qualify(right),
            model=None,
            schema=schema,
            rows=None,
            message=f"{missing} does not exist",
        )
    join_on = _keys(keys, left_schema, right_schema)
    rows = await _row_delta(engine, left, right, left_schema, right_schema, join_on, sample)
    return TableDiff(left=_qualify(left), right=_qualify(right), model=None, schema=schema, rows=rows)


async def diff_model(
    model: CompiledModel,
    *,
    left_env: str,
    right_env: str,
    store: StateStore,
    engine: EngineAdapter,
    keys: list[str] | None = None,
    sample: int = SAMPLE_LIMIT,
) -> TableDiff:
    """Compare one model as built in two environments."""
    left_table = await model_relation(model, store, engine, left_env)
    right_table = await model_relation(model, store, engine, right_env)
    label = model.name
    if left_table is None and right_table is None:
        return TableDiff(
            left=left_env,
            right=right_env,
            model=label,
            schema=SchemaDelta(),
            rows=None,
            message=f"{label} is not built in {left_env} or {right_env}",
        )
    if left_table is None or right_table is None:
        missing_env = left_env if left_table is None else right_env
        present = right_table if left_table is None else left_table
        described = await engine.describe(present) if present is not None else {}
        schema = (
            SchemaDelta(removed=list(described.items()))
            if left_table is None
            else SchemaDelta(added=list(described.items()))
        )
        return TableDiff(
            left=_qualify(left_table) if left_table else left_env,
            right=_qualify(right_table) if right_table else right_env,
            model=label,
            schema=schema,
            rows=None,
            message=f"{label} is not built in {missing_env}",
        )
    grain = keys if keys else list(model.key)
    result = await diff_tables(engine, left_table, right_table, keys=grain or None, sample=sample)
    result.model = label
    return result


async def diff_environments(
    compiled: CompiledProject,
    *,
    left_env: str,
    right_env: str,
    store: StateStore,
    engines: EngineRegistry,
    select: set[str] | None = None,
    keys: list[str] | None = None,
    sample: int = SAMPLE_LIMIT,
) -> list[TableDiff]:
    """Compare every selected (or every) model across two environments."""
    names = (
        compiled.graph.topological_sort()
        if select is None
        else [name for name in compiled.graph.topological_sort() if name in select]
    )
    out: list[TableDiff] = []
    for name in names:
        model = compiled.models[name]
        if model.materialise in ("ephemeral", "file"):
            continue
        out.append(
            await diff_model(
                model,
                left_env=left_env,
                right_env=right_env,
                store=store,
                engine=engines.require(model.engine, model=model.name),
                keys=keys,
                sample=sample,
            )
        )
    return out


def _schema_delta(left: dict[str, str], right: dict[str, str]) -> SchemaDelta:
    added = [(name, right[name]) for name in right if name not in left]
    removed = [(name, left[name]) for name in left if name not in right]
    type_changed = [(name, left[name], right[name]) for name in left if name in right and left[name] != right[name]]
    return SchemaDelta(added=added, removed=removed, type_changed=type_changed)


def _keys(explicit: list[str] | None, left: dict[str, str], right: dict[str, str]) -> list[str]:
    common = [name for name in left if name in right]
    if explicit:
        missing = [name for name in explicit if name not in left or name not in right]
        if missing:
            raise PlanError(f"join key {missing[0]!r} is not on both tables")
        return list(explicit)
    if not common:
        raise PlanError("tables share no columns to join on; pass --on")
    return common


async def _row_delta(
    engine: EngineAdapter,
    left: TableRef,
    right: TableRef,
    left_schema: dict[str, str],
    right_schema: dict[str, str],
    keys: list[str],
    sample: int,
) -> RowDelta:
    left_count = await _count(engine, left)
    right_count = await _count(engine, right)
    left_only = await _count_query(engine, _anti_join(left, right, keys, left_alias="l", right_alias="r"))
    right_only = await _count_query(engine, _anti_join(right, left, keys, left_alias="r", right_alias="l"))
    compared = [name for name in left_schema if name in right_schema and name not in keys]
    changed = await _count_query(engine, _changed(left, right, keys, compared)) if compared else 0
    matched = max(0, min(left_count, right_count) - left_only - right_only - changed)
    cap = max(1, sample)
    return RowDelta(
        left_count=left_count,
        right_count=right_count,
        left_only=left_only,
        right_only=right_only,
        changed=changed,
        matched=matched,
        keys=keys,
        left_only_sample=await _sample(engine, _anti_join(left, right, keys, left_alias="l", right_alias="r"), cap),
        right_only_sample=await _sample(engine, _anti_join(right, left, keys, left_alias="r", right_alias="l"), cap),
        changed_sample=await _sample(engine, _changed(left, right, keys, compared), cap) if compared else [],
    )


async def _count(engine: EngineAdapter, table: TableRef) -> int:
    query = exp.select(exp.alias_(exp.Count(this=exp.Star()), "n")).from_(table.to_expr())
    reader = await engine.fetch(query)
    arrow = await asyncio.to_thread(reader.read_all)
    return int(arrow.column(0)[0].as_py() or 0)


async def _count_query(engine: EngineAdapter, query: exp.Expression) -> int:
    wrapped = exp.select(exp.alias_(exp.Count(this=exp.Star()), "n")).from_(cast("exp.Query", query).subquery("_n"))
    reader = await engine.fetch(wrapped)
    table = await asyncio.to_thread(reader.read_all)
    return int(table.column(0)[0].as_py() or 0)


async def _sample(engine: EngineAdapter, query: exp.Expression, limit: int) -> list[dict[str, object]]:
    bounded = exp.select(exp.Star()).from_(cast("exp.Query", query).subquery("_diff")).limit(limit)
    reader = await engine.fetch(bounded)
    table = await asyncio.to_thread(reader.read_all)
    return [{name: json_cell(row[name]) for name in table.column_names} for row in table.to_pylist()]


def _anti_join(keep: TableRef, drop: TableRef, keys: list[str], *, left_alias: str, right_alias: str) -> exp.Select:
    left = keep.to_expr().as_(left_alias)
    right = drop.to_expr().as_(right_alias)
    on = _join_on(left_alias, right_alias, keys)
    missing = exp.column(keys[0], table=right_alias).is_(exp.null())
    return exp.select(exp.Star()).from_(left).join(right, on=on, join_type="LEFT").where(missing)


def _changed(left: TableRef, right: TableRef, keys: list[str], compared: list[str]) -> exp.Select:
    ltab = left.to_expr().as_("l")
    rtab = right.to_expr().as_("r")
    on = _join_on("l", "r", keys)
    if not compared:
        return exp.select(exp.Star()).from_(ltab).join(rtab, on=on, join_type="INNER").where(exp.false())
    drift = [
        exp.not_(exp.NullSafeEQ(this=exp.column(name, table="l"), expression=exp.column(name, table="r")))
        for name in compared
    ]
    predicate = drift[0] if len(drift) == 1 else exp.or_(*drift)
    selected = (
        [exp.column(name, table="l") for name in keys]
        + [exp.alias_(exp.column(name, table="l"), f"left_{name}") for name in compared]
        + [exp.alias_(exp.column(name, table="r"), f"right_{name}") for name in compared]
    )
    return exp.select(*selected).from_(ltab).join(rtab, on=on, join_type="INNER").where(predicate)


def _join_on(left_alias: str, right_alias: str, keys: list[str]) -> exp.Expression:
    parts = [
        exp.EQ(this=exp.column(name, table=left_alias), expression=exp.column(name, table=right_alias)) for name in keys
    ]
    return parts[0] if len(parts) == 1 else exp.and_(*parts)
