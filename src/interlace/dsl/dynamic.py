"""Models registered while a Python model is running.

A scheduled model can query a database and call ``REGISTRY.register_model`` for
each result. Those definitions are compiled and built in the same apply. SQL
definitions are written under ``.interlace/dynamic`` so the next plan, the UI,
and a restart still see them. A name that already came from a source file is
rejected; a name this run registered before is replaced, so the next periodic
run can change it.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Any

import yaml

from interlace.checks.spec import CheckSpec
from interlace.dsl.decorators import REGISTRY, ModelDef, dynamic_batch, dynamic_owner
from interlace.exceptions import DefinitionError
from interlace.physical.spec import ConstraintSpec, IndexSpec, SchemaPolicy

DYNAMIC_ROOT = Path(".interlace") / "dynamic"
dynamic_depth: ContextVar[int] = ContextVar("interlace_dynamic_depth", default=0)
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


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
    config: dict[str, Any] = {"name": definition.name}
    if definition.materialise != "virtual":
        config["materialise"] = definition.materialise
    if definition.strategy != "replace":
        config["strategy"] = definition.strategy
    if definition.key:
        config["key"] = list(definition.key)
    if definition.dialect:
        config["dialect"] = definition.dialect
    if definition.engine:
        config["engine"] = definition.engine
    if definition.depends_on:
        config["depends_on"] = list(definition.depends_on)
    if definition.interval:
        config["interval"] = definition.interval
    if definition.time_column:
        config["time_column"] = definition.time_column
    if definition.backfill != "auto":
        config["backfill"] = definition.backfill
    if definition.tags:
        config["tags"] = list(definition.tags)
    if definition.owner:
        config["owner"] = definition.owner
    if definition.description:
        config["description"] = definition.description
    if definition.columns:
        config["columns"] = definition.columns
    if definition.target:
        config["target"] = definition.target
    if definition.path:
        config["path"] = definition.path
    if definition.format:
        config["format"] = definition.format
    if definition.environments != ("prod",):
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
    header = yaml.safe_dump({"interlace": config}, sort_keys=False).strip()
    return f"/*\n{header}\n*/\n{definition.sql.strip()}\n"


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
