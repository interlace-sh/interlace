"""Models registered while a Python model is running.

A scheduled model can query a database and call ``REGISTRY.register_model`` for
each result. Those definitions are compiled and built in the same apply. SQL
definitions are written under ``.interlace/dynamic`` so the next plan, the UI,
and a restart still see them. A name that already came from a source file is
rejected; a name this run registered before is replaced, so the next periodic
run can change it.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from interlace.checks.spec import CheckSpec
from interlace.dsl.decorators import REGISTRY, ModelDef, dynamic_batch, dynamic_owner
from interlace.exceptions import DefinitionError, PlanError
from interlace.physical.spec import ConstraintSpec, IndexSpec, SchemaPolicy

if TYPE_CHECKING:
    from interlace.engines.base import EngineAdapter
    from interlace.engines.registry import EngineRegistry
    from interlace.graph.project import CompiledProject
    from interlace.plan.apply import ApplyResult
    from interlace.plan.plan import Plan
    from interlace.project import Project
    from interlace.state.store import StateStore

DYNAMIC_ROOT = Path(".interlace") / "dynamic"
dynamic_depth: ContextVar[int] = ContextVar("interlace_dynamic_depth", default=0)
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")
_MAX_DEPTH = 8

# Defaults discovery applies when a SQL config omits the key. The serializer
# omits the same values, so a new field is one entry here plus the ModelDef line.
MATERIALISE_DEFAULT = "virtual"
STRATEGY_DEFAULT = "replace"
BACKFILL_DEFAULT = "auto"
ENVIRONMENTS_DEFAULT = ("prod",)


@contextmanager
def registering(owner: str) -> Iterator[None]:
    """Mark ``register_model`` calls as coming from the Python model ``owner``."""
    token = dynamic_owner.set(owner)
    try:
        yield
    finally:
        dynamic_owner.reset(token)


@contextmanager
def capturing_registrations() -> Iterator[list[str]]:
    """Collect model names registered on this context while the block runs."""
    found: list[str] = []
    token = dynamic_batch.set(found)
    try:
        yield found
    finally:
        dynamic_batch.reset(token)


def write_dynamic_models(root: Path, names: list[str]) -> None:
    """Persist SQL models registered during a run. Python functions stay in memory."""
    directory = root / DYNAMIC_ROOT
    for name in names:
        definition = REGISTRY.models.get(name)
        if definition is None or definition.sql is None:
            continue
        if not _NAME.fullmatch(name) or ".." in name:
            raise DefinitionError(
                f"dynamic model name {name!r} must be an identifier (letters, digits, underscore, dot)"
            )
        rendered = render_model_sql(definition)
        target = directory / f"{name}.sql"
        if target.is_file() and target.read_text() == rendered:
            continue  # leave mtime alone so an unchanged re-register does not force a reload
        directory.mkdir(parents=True, exist_ok=True)
        target.write_text(rendered)


def render_model_sql(definition: ModelDef) -> str:
    """A ``.sql`` model file that discovery reads back into the same definition."""
    if definition.sql is None:
        raise DefinitionError(f"model {definition.name!r} has no SQL to persist")
    header = yaml.safe_dump({"interlace": model_to_config(definition)}, sort_keys=False).strip()
    return f"/*\n{header}\n*/\n{definition.sql.strip()}\n"


def model_to_config(definition: ModelDef) -> dict[str, Any]:
    """The ``interlace:`` mapping ``_sql_model`` reads. Defaults are omitted."""
    config: dict[str, Any] = {"name": definition.name}
    _keep(config, "materialise", definition.materialise, MATERIALISE_DEFAULT)
    _keep(config, "strategy", definition.strategy, STRATEGY_DEFAULT)
    if definition.key:
        config["key"] = list(definition.key)
    _keep(config, "dialect", definition.dialect, None)
    _keep(config, "engine", definition.engine, None)
    if definition.depends_on:
        config["depends_on"] = list(definition.depends_on)
    _keep(config, "interval", definition.interval, None)
    _keep(config, "time_column", definition.time_column, None)
    _keep(config, "backfill", definition.backfill, BACKFILL_DEFAULT)
    if definition.tags:
        config["tags"] = list(definition.tags)
    _keep(config, "owner", definition.owner, None)
    _keep(config, "description", definition.description, None)
    if definition.columns:
        config["columns"] = definition.columns
    _keep(config, "target", definition.target, None)
    _keep(config, "path", definition.path, None)
    _keep(config, "format", definition.format, None)
    if definition.environments != ENVIRONMENTS_DEFAULT:
        config["environments"] = list(definition.environments)
    if definition.schedule:
        config["schedule"] = definition.schedule
    if definition.checks:
        config["checks"] = [_check_config(check) for check in definition.checks]
    if definition.indexes:
        config["indexes"] = [_index_config(index) for index in definition.indexes]
    if definition.constraints:
        config["constraints"] = [_constraint_config(constraint) for constraint in definition.constraints]
    if definition.schema_policy != SchemaPolicy():
        policy = definition.schema_policy
        config["schema"] = {"columns": policy.columns, "indexes": policy.indexes, "constraints": policy.constraints}
    return config


def _keep(config: dict[str, Any], key: str, value: Any, default: Any) -> None:
    if value is not None and value != default:
        config[key] = value


def _check_config(check: CheckSpec) -> dict[str, Any]:
    entry: dict[str, Any] = {"type": check.type}
    if len(check.columns) == 1:
        entry["column"] = check.columns[0]
    elif check.columns:
        entry["columns"] = list(check.columns)
    if check.severity != "error":
        entry["severity"] = check.severity
    entry.update(check.params)
    return entry


def _index_config(index: IndexSpec) -> dict[str, Any]:
    entry: dict[str, Any] = {"columns": list(index.columns)}
    if index.unique:
        entry["unique"] = True
    if index.name:
        entry["name"] = index.name
    return entry


def _constraint_config(constraint: ConstraintSpec) -> dict[str, Any]:
    entry: dict[str, Any] = {"type": constraint.type}
    if constraint.columns:
        entry["columns"] = list(constraint.columns)
    if constraint.name:
        entry["name"] = constraint.name
    if constraint.expression:
        entry["expression"] = constraint.expression
    if constraint.reference:
        entry["to"] = constraint.reference
    if constraint.fields:
        entry["fields"] = list(constraint.fields)
    return entry


async def apply_with_registrations(
    plan: Plan,
    *,
    compiled: CompiledProject,
    project: Project,
    on_compiled: Callable[[CompiledProject], None] | None = None,
    engine: EngineAdapter | None = None,
    engines: Mapping[str, EngineAdapter] | EngineRegistry | None = None,
    state: StateStore,
    base_path: Path | None = None,
    parallelism: int = 4,
    on_progress: Callable[[str, str, dict[str, Any]], None] | None = None,
    connections: Mapping[str, Any] | None = None,
) -> ApplyResult:
    """Apply ``plan``, then build any SQL models the run registered.

    Files and the daemon graph are updated only after that follow-up build
    succeeds, so a failure leaves the warehouse, the dynamic SQL, and the
    served graph where they were.
    """
    from interlace.plan.apply import apply

    result = await apply(
        plan,
        compiled=compiled,
        engine=engine,
        engines=engines,
        state=state,
        base_path=base_path,
        parallelism=parallelism,
        on_progress=on_progress,
        connections=connections,
    )
    if not result.registered:
        return result
    await _expand_registered(
        result,
        plan=plan,
        project=project,
        on_compiled=on_compiled,
        engine=engine,
        engines=engines,
        state=state,
        base_path=base_path,
        parallelism=parallelism,
        on_progress=on_progress,
        connections=connections,
    )
    return result


async def _expand_registered(
    result: ApplyResult,
    *,
    plan: Plan,
    project: Project,
    on_compiled: Callable[[CompiledProject], None] | None,
    engine: EngineAdapter | None,
    engines: Mapping[str, EngineAdapter] | EngineRegistry | None,
    state: StateStore,
    base_path: Path | None,
    parallelism: int,
    on_progress: Callable[[str, str, dict[str, Any]], None] | None,
    connections: Mapping[str, Any] | None,
) -> None:
    from interlace.plan.apply import apply
    from interlace.plan.differ import diff

    depth = dynamic_depth.get()
    if depth >= _MAX_DEPTH:
        raise PlanError(
            "dynamic model registration nested more than 8 levels; "
            "a model registered during a run registered further models until the cap"
        )
    names = list(result.registered)
    compiled = await asyncio.to_thread(project.compile)
    follow = await diff(compiled, plan.environment, state, select=set(names))
    child: ApplyResult | None = None
    if not follow.is_empty:
        child = await apply(
            follow,
            compiled=compiled,
            engine=engine,
            engines=engines,
            state=state,
            base_path=base_path,
            parallelism=parallelism,
            on_progress=on_progress,
            connections=connections,
        )
    await asyncio.to_thread(write_dynamic_models, project.root, names)
    if on_compiled is not None:
        on_compiled(compiled)
    if child is not None and child.registered:
        token = dynamic_depth.set(depth + 1)
        try:
            await _expand_registered(
                child,
                plan=follow,
                project=project,
                on_compiled=on_compiled,
                engine=engine,
                engines=engines,
                state=state,
                base_path=base_path,
                parallelism=parallelism,
                on_progress=on_progress,
                connections=connections,
            )
        finally:
            dynamic_depth.reset(token)
    if child is not None:
        _merge_apply(result, child)


def _merge_apply(result: ApplyResult, child: ApplyResult) -> None:
    result.built.extend(name for name in child.built if name not in result.built)
    result.reused.extend(name for name in child.reused if name not in result.reused)
    result.gated.extend(name for name in child.gated if name not in result.gated)
    result.transfers.extend(name for name in child.transfers if name not in result.transfers)
    result.promoted += child.promoted
    result.checks.extend(child.checks)
    result.timings.update(child.timings)
    for name, counts in child.rows.items():
        result.record_rows(name, counts)
