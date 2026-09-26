"""Read a built model: a row sample, a column profile, and the rows a check rejected.

Shared by the HTTP API and the MCP server. Every query is an AST we build ourselves
against a table the project already owns — never SQL the caller typed.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

from sqlglot import exp

from interlace.checks.builtin import build_failing_rows
from interlace.engines.base import EngineAdapter
from interlace.exceptions import DefinitionError
from interlace.graph.project import CompiledModel, CompiledProject
from interlace.ir.relation import TableRef
from interlace.plan.plan import env_view
from interlace.sinks import target_ref
from interlace.state.snapshot import Snapshot


class _InspectStore(Protocol):
    """The control-plane reads a preview needs, beyond the plan/apply store slice."""

    async def latest_model_build(self, model: str) -> dict[str, object] | None: ...
    async def get_environment(self, environment: str) -> dict[str, str]: ...
    async def get_snapshot(self, name: str, fingerprint: str) -> Snapshot | None: ...
    async def get_snapshots(self, pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], Snapshot]: ...
    async def list_check_results(self, model: str | None = None, limit: int = 200) -> list[dict[str, object]]: ...


PREVIEW_CAP = 100


def json_cell(value: object) -> object:
    """A warehouse cell as JSON: scalars pass through, everything else is text."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


@dataclass
class ColumnProfile:
    column: str
    type: str
    nulls: int
    distinct: int
    min: str | None = None
    max: str | None = None


@dataclass
class LastBuild:
    status: str
    at: str
    seconds: float | None = None
    rows: dict[str, int] | None = None
    message: str | None = None
    statement: str | None = None


@dataclass
class Tabular:
    """A bounded read: columns, cells, and whether the cap cut the result off."""

    columns: list[str] = field(default_factory=list)
    types: list[str] = field(default_factory=list)
    rows: list[list[object]] = field(default_factory=list)
    truncated: bool = False


@dataclass
class ModelPreview:
    available: bool
    message: str | None = None
    relation: str | None = None
    sample: Tabular = field(default_factory=Tabular)
    profile: list[ColumnProfile] = field(default_factory=list)
    last_build: LastBuild | None = None


@dataclass
class FailingRows:
    available: bool
    message: str | None = None
    sample: Tabular = field(default_factory=Tabular)


def _cap(limit: int) -> int:
    return max(1, min(limit, PREVIEW_CAP))


def _relation_name(table: TableRef) -> str:
    return table.to_expr().sql()


async def last_build(store: _InspectStore, name: str) -> LastBuild | None:
    event = await store.latest_model_build(name)
    if event is None:
        return None
    payload = event.get("payload")
    body = payload if isinstance(payload, dict) else {}
    rows = body.get("rows")
    seconds = body.get("seconds")
    status = str(event["type"]).removeprefix("model.")
    return LastBuild(
        status=status,
        at=str(event["ts"]),
        seconds=float(seconds) if isinstance(seconds, (int, float)) else None,
        rows={key: int(value) for key, value in rows.items()} if isinstance(rows, dict) else None,
        message=body.get("message") if isinstance(body.get("message"), str) else None,
        statement=body.get("statement") if isinstance(body.get("statement"), str) else None,
    )


async def _built_table(
    model: CompiledModel, store: _InspectStore, engine: EngineAdapter, environment: str
) -> tuple[TableRef, str] | None:
    """The relation a preview should read, and why (environment view or snapshot)."""
    if model.materialise == "ephemeral" or model.materialise == "file":
        return None
    if model.is_terminal and model.materialise == "table":
        if not model.target:
            return None
        ref = target_ref(model.target)
        return (ref, "table") if await engine.table_exists(ref) else None

    promoted = await store.get_environment(environment)
    fingerprint = promoted.get(model.name)
    if fingerprint:
        view = env_view(environment, model.name)
        if await engine.table_exists(view):
            return view, "environment"
        snapshot = await store.get_snapshot(model.name, fingerprint)
        if snapshot is not None and await engine.table_exists(snapshot.physical_table):
            return snapshot.physical_table, "snapshot"
    snapshot = await store.get_snapshot(model.name, model.fingerprint)
    if snapshot is not None and await engine.table_exists(snapshot.physical_table):
        return snapshot.physical_table, "snapshot"
    return None


def _unavailable(model: CompiledModel) -> str:
    if model.materialise == "ephemeral":
        return "ephemeral models are inlined into the models that read them"
    if model.materialise == "file":
        return "a file output has no table to preview"
    return "not built in this environment yet — apply first"


async def _read(engine: EngineAdapter, query: exp.Expression, limit: int) -> Tabular:
    bounded = exp.select(exp.Star()).from_(cast("exp.Query", query).subquery("_preview")).limit(limit + 1)
    reader = await engine.fetch(bounded)
    table = await asyncio.to_thread(reader.read_all)
    names = list(table.column_names)
    records = table.to_pylist()
    truncated = len(records) > limit
    rows = [[json_cell(record[name]) for name in names] for record in records[:limit]]
    return Tabular(
        columns=names,
        types=[str(field.type) for field in table.schema],
        rows=rows,
        truncated=truncated,
    )


def _profile_query(table: TableRef, columns: list[str], *, bounds: bool) -> exp.Select:
    selected: list[exp.Expression] = [exp.alias_(exp.Count(this=exp.Star()), "n")]
    for index, name in enumerate(columns):
        column = exp.column(name)
        selected.append(exp.alias_(exp.Count(this=column), f"p{index}"))
        selected.append(exp.alias_(exp.Count(this=exp.Distinct(expressions=[column.copy()])), f"d{index}"))
        if bounds:
            cast = exp.DataType.build("VARCHAR")
            selected.append(exp.alias_(exp.Cast(this=exp.Min(this=column.copy()), to=cast), f"lo{index}"))
            selected.append(exp.alias_(exp.Cast(this=exp.Max(this=column.copy()), to=cast.copy()), f"hi{index}"))
    return exp.select(*selected).from_(table.to_expr())


async def _profile(engine: EngineAdapter, table: TableRef, described: dict[str, str]) -> list[ColumnProfile]:
    columns = list(described)
    if not columns:
        return []

    async def once(bounds: bool) -> dict[str, Any]:
        reader = await engine.fetch(_profile_query(table, columns, bounds=bounds))
        arrow = await asyncio.to_thread(reader.read_all)
        raw = arrow.to_pylist()[0]
        if not isinstance(raw, dict):
            return {}
        return {str(key): value for key, value in raw.items()}

    bounds = True
    try:
        row = await once(True)
    except Exception:
        bounds = False
        try:
            row = await once(False)
        except Exception:
            return []
    total = int(row.get("n") or 0)
    profiles: list[ColumnProfile] = []
    for index, name in enumerate(columns):
        present = int(row.get(f"p{index}") or 0)
        lo = row.get(f"lo{index}") if bounds else None
        hi = row.get(f"hi{index}") if bounds else None
        profiles.append(
            ColumnProfile(
                column=name,
                type=described[name],
                nulls=total - present,
                distinct=int(row.get(f"d{index}") or 0),
                min=None if lo is None else str(lo),
                max=None if hi is None else str(hi),
            )
        )
    return profiles


async def preview_model(
    compiled: CompiledProject,
    store: _InspectStore,
    engine: EngineAdapter,
    name: str,
    environment: str,
    limit: int,
) -> ModelPreview:
    model = compiled.models[name]
    build = await last_build(store, name)
    found = await _built_table(model, store, engine, environment)
    if found is None:
        return ModelPreview(available=False, message=_unavailable(model), last_build=build)
    table, _kind = found
    cap = _cap(limit)
    sample = await _read(engine, exp.select(exp.Star()).from_(table.to_expr()), cap)
    described = await engine.describe(table)
    profile = await _profile(engine, table, described)
    return ModelPreview(
        available=True,
        relation=_relation_name(table),
        sample=sample,
        profile=profile,
        last_build=build,
    )


async def _check_table(
    model: CompiledModel, store: _InspectStore, engine: EngineAdapter, environment: str
) -> TableRef | None:
    """The table the latest check ran against: its snapshot, even if promotion was blocked."""
    for result in await store.list_check_results(model.name):
        if result.get("environment") != environment:
            continue
        fingerprint = result.get("fingerprint")
        if not isinstance(fingerprint, str):
            continue
        snapshot = await store.get_snapshot(model.name, fingerprint)
        if snapshot is not None and await engine.table_exists(snapshot.physical_table):
            return snapshot.physical_table
        break
    found = await _built_table(model, store, engine, environment)
    return found[0] if found else None


async def failing_rows(
    compiled: CompiledProject,
    store: _InspectStore,
    engine: EngineAdapter,
    name: str,
    check_name: str,
    environment: str,
    limit: int,
) -> FailingRows:
    model = compiled.models[name]
    spec = next((item for item in model.checks if item.name == check_name), None)
    if spec is None:
        if any(check.name == check_name for check in compiled.python_checks.get(name, ())):
            return FailingRows(available=False, message="a Python check does not keep the failing rows")
        raise DefinitionError(f"unknown check {check_name!r} on {name!r}")
    if spec.type in ("row_count", "freshness"):
        return FailingRows(available=False, message="this check judges the table, not individual rows")
    table = await _check_table(model, store, engine, environment)
    if table is None:
        return FailingRows(available=False, message="not built in this environment yet — apply first")

    # The check ran against the tables that were actually promoted, which can be an
    # older snapshot than the fingerprint the current source would build.
    promoted = await store.get_environment(environment)
    built = await store.get_snapshots(promoted.items())
    physical = {model_name: snapshot.physical_table for (model_name, _), snapshot in built.items()}

    def resolve(upstream: str) -> TableRef:
        other = compiled.models.get(upstream)
        if other is None:
            raise DefinitionError(f"check on {name!r} references unknown model {upstream!r}")
        return physical.get(upstream, other.physical_table)

    query = build_failing_rows(spec, table, name, model.dialect, resolve)
    if query is None:
        return FailingRows(available=False, message="this check judges the table, not individual rows")
    sample = await _read(engine, query, _cap(limit))
    return FailingRows(available=True, sample=sample)
