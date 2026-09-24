"""Diff a model's physical spec against the objects interlace previously recorded.

Drops are limited to that recorded set. An index or constraint someone else
created is reported by the drift pass and never dropped here.
"""

from __future__ import annotations

from sqlglot import exp

from interlace.engines.base import EngineCaps
from interlace.graph.project import CompiledModel
from interlace.ir.relation import TableRef
from interlace.physical.ddl import create_statements, desired_objects, drop_statement
from interlace.physical.spec import PhysicalObject

# Used when the plan is rendered before an engine is open: show constraints as
# constraints. Apply rewrites the lines with the real engine's caps.
LOGICAL_CAPS = EngineCaps(
    enforced_constraints=frozenset({"primary_key", "unique", "not_null", "check", "foreign_key"}),
)


def model_objects(model: CompiledModel, caps: EngineCaps) -> tuple[tuple[PhysicalObject, ...], list[str]]:
    """What this model wants on its table, under ``caps`` and its schema policy."""
    return desired_objects(
        model.name,
        model.indexes,
        model.constraints,
        manage_indexes=model.schema_policy.indexes == "manage",
        manage_constraints=model.schema_policy.constraints == "manage",
        caps=caps,
    )


def object_changes(
    desired: tuple[PhysicalObject, ...], previous: tuple[PhysicalObject, ...]
) -> tuple[tuple[tuple[str, str, str], ...], tuple[PhysicalObject, ...]]:
    """``((op, kind, name), ...)`` for display, and the previous objects to drop.

    ``kind`` in the display triple is ``index`` or ``constraint`` (``not_null``
    is a constraint). The second tuple is the recorded objects whose names are
    no longer desired — those are the only drops.
    """
    desired_names = {obj.name for obj in desired}
    previous_names = {obj.name for obj in previous}
    changes: list[tuple[str, str, str]] = []
    for obj in desired:
        if obj.name not in previous_names:
            changes.append(("add", "constraint" if obj.kind in {"constraint", "not_null"} else "index", obj.name))
    drops = tuple(obj for obj in previous if obj.name not in desired_names)
    for obj in drops:
        changes.append(("drop", "constraint" if obj.kind in {"constraint", "not_null"} else "index", obj.name))
    return tuple(changes), drops


def reconcile_statements(
    table: TableRef,
    model: CompiledModel,
    desired: tuple[PhysicalObject, ...],
    drops: tuple[PhysicalObject, ...],
    *,
    existing_constraints: set[str],
) -> list[exp.Expression]:
    """Drop recorded objects that left the spec, then create what is missing.

    Constraint adds already present in the catalog are skipped (Postgres has no
    ``ADD CONSTRAINT IF NOT EXISTS``). Indexes and DuckDB ``SET NOT NULL`` are
    idempotent and always emitted.
    """
    statements = [drop_statement(table, obj, model.dialect) for obj in drops]
    pending = tuple(obj for obj in desired if not (obj.kind == "constraint" and obj.name in existing_constraints))
    statements.extend(
        create_statements(table, model.name, model.indexes, model.constraints, pending, dialect=model.dialect)
    )
    return statements
