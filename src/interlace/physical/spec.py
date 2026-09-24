"""Index, constraint, and external-schema policy declarations.

A model declares physical objects in its config (SQL block or ``@model``)::

    indexes:
      - columns: [customer_id, ordered_at]
      - unique: true
        columns: [order_id]
        name: orders_by_id
    constraints:
      - primary_key: order_id
      - unique: [customer_id, ordered_at]
      - not_null: status
      - check: {expression: "amount >= 0"}
      - foreign_key: {columns: [customer_id], to: customers, fields: [id]}
    schema:
      columns: additive    # additive | reject | ignore  (external tables)
      indexes: manage      # manage | ignore
      constraints: manage  # manage | ignore

Names interlace creates are ``il__<model>__…``. That name is how a later plan
tells an object it created from one someone else added. ``key:`` is not a
primary key, and ``checks:`` are not promoted into constraints.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from interlace.exceptions import DefinitionError

COLUMN_POLICIES = frozenset({"additive", "reject", "ignore"})
OBJECT_POLICIES = frozenset({"manage", "ignore"})
CONSTRAINT_TYPES = frozenset({"primary_key", "unique", "not_null", "check", "foreign_key"})
_NO_TABLE = frozenset({"view", "ephemeral", "file"})
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_IDENT = 63  # Postgres NAMEDATALEN - 1; longer names are truncated with a hash


@dataclass(frozen=True)
class IndexSpec:
    """One index on a model's table."""

    columns: tuple[str, ...]
    unique: bool = False
    name: str | None = None  # explicit; None → il__<model>__<columns>

    def object_name(self, model: str) -> str:
        if self.name:
            return self.name
        suffix = "__".join(self.columns)
        if self.unique:
            suffix = f"uniq__{suffix}"
        return _generated_name(model, suffix)


@dataclass(frozen=True)
class ConstraintSpec:
    """One table constraint. Enforcement depends on the engine."""

    type: str
    columns: tuple[str, ...] = ()
    name: str | None = None
    expression: str | None = None  # check
    reference: str | None = None  # foreign_key target table, as written
    fields: tuple[str, ...] = ()  # foreign_key referenced columns

    def object_name(self, model: str) -> str:
        if self.name:
            return self.name
        if self.type == "primary_key":
            return _generated_name(model, "pk")
        if self.type == "check":
            digest = hashlib.sha256((self.expression or "").encode()).hexdigest()[:8]
            return _generated_name(model, f"chk__{digest}")
        tag = {"unique": "uniq", "not_null": "nn", "foreign_key": "fk"}[self.type]
        suffix = "__".join(self.columns) if self.columns else tag
        return _generated_name(model, f"{tag}__{suffix}" if self.columns else tag)


@dataclass(frozen=True)
class SchemaPolicy:
    """How an external table's drift is treated. Owned snapshots ignore ``columns``."""

    columns: str = "additive"
    indexes: str = "manage"
    constraints: str = "manage"


@dataclass(frozen=True)
class PhysicalObject:
    """An index or constraint interlace created, recorded so only those are dropped."""

    kind: str  # index | constraint | not_null
    name: str
    column: str | None = None  # not_null drops address the column, not a constraint name


def _model_base(model: str) -> str:
    return model.rsplit(".", 1)[-1]


def _generated_name(model: str, suffix: str) -> str:
    raw = f"il__{_model_base(model)}__{suffix}"
    if len(raw) <= _MAX_IDENT:
        return raw
    digest = hashlib.sha256(raw.encode()).hexdigest()[:8]
    keep = _MAX_IDENT - len(digest) - 2
    return f"{raw[:keep]}__{digest}"


def _ident(value: str, what: str, model: str) -> str:
    if not _IDENT.fullmatch(value):
        raise DefinitionError(f"model {model!r}: {what} {value!r} must be an identifier [A-Za-z_][A-Za-z0-9_]*")
    if len(value) > _MAX_IDENT:
        raise DefinitionError(f"model {model!r}: {what} {value!r} is longer than {_MAX_IDENT} characters")
    return value


def _as_columns(value: Any, model: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_ident(value, "column", model),)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(_ident(str(v), "column", model) for v in value)
    raise DefinitionError(f"model {model!r}: column must be a name or list of names, got {type(value).__name__}")


def _explicit_name(value: Any, model: str) -> str | None:
    if value is None:
        return None
    return _ident(str(value), "name", model)


def parse_indexes(value: Any, model: str) -> tuple[IndexSpec, ...]:
    """Normalise a model's ``indexes:`` config."""
    if value is None:
        return ()
    if isinstance(value, IndexSpec):
        return (value,)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise DefinitionError(f"indexes on {model!r} must be a list")
    return tuple(entry if isinstance(entry, IndexSpec) else _parse_index(entry, model) for entry in value)


def _parse_index(entry: Any, model: str) -> IndexSpec:
    if isinstance(entry, str) or (isinstance(entry, Sequence) and not isinstance(entry, (str, bytes, dict))):
        columns = _as_columns(entry, model)
        if not columns:
            raise DefinitionError(f"index on {model!r} needs at least one column")
        return IndexSpec(columns=columns)
    if not isinstance(entry, dict) or not entry:
        raise DefinitionError(f"invalid index entry on {model!r}: {entry!r}")
    columns = _as_columns(entry.get("columns", entry.get("column")), model)
    if not columns:
        raise DefinitionError(f"index on {model!r} needs columns:")
    unique = entry.get("unique", False)
    if not isinstance(unique, bool):
        raise DefinitionError(f"index unique on {model!r} must be true or false")
    unknown = set(entry) - {"columns", "column", "unique", "name"}
    if unknown:
        raise DefinitionError(f"unknown index key(s) {sorted(unknown)} on {model!r}")
    return IndexSpec(columns=columns, unique=unique, name=_explicit_name(entry.get("name"), model))


def parse_constraints(value: Any, model: str) -> tuple[ConstraintSpec, ...]:
    """Normalise a model's ``constraints:`` config."""
    if value is None:
        return ()
    if isinstance(value, ConstraintSpec):
        return (value,)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise DefinitionError(f"constraints on {model!r} must be a list")
    return tuple(entry if isinstance(entry, ConstraintSpec) else _parse_constraint(entry, model) for entry in value)


def _parse_constraint(entry: Any, model: str) -> ConstraintSpec:
    if not isinstance(entry, dict) or not entry:
        raise DefinitionError(f"invalid constraint entry on {model!r}: {entry!r}")
    if "type" in entry:
        params = dict(entry)
        constraint_type = str(params.pop("type"))
        columns = _as_columns(params.pop("column", None) or params.pop("columns", None), model)
    else:
        if len(entry) != 1:
            raise DefinitionError(f"ambiguous constraint entry on {model!r}: {entry!r}; use the {{type: ...}} form")
        constraint_type, raw = next(iter(entry.items()))
        constraint_type = str(constraint_type)
        params = {}
        if isinstance(raw, dict):
            params = dict(raw)
            columns = _as_columns(params.pop("column", None) or params.pop("columns", None), model)
        else:
            columns = _as_columns(raw, model)
    if constraint_type not in CONSTRAINT_TYPES:
        raise DefinitionError(
            f"unknown constraint type {constraint_type!r} on {model!r}; expected one of {sorted(CONSTRAINT_TYPES)}"
        )
    name = _explicit_name(params.pop("name", None), model)
    expression = params.pop("expression", None)
    reference = params.pop("to", None)
    fields = _as_columns(params.pop("fields", None) or params.pop("field", None), model)
    unknown = set(params) - {"severity"}  # severity is a check concept; reject it so it isn't a silent no-op
    if "severity" in params:
        raise DefinitionError(
            f"constraint {constraint_type!r} on {model!r} does not take severity; that is a check, not a constraint"
        )
    if unknown:
        raise DefinitionError(f"unknown constraint key(s) {sorted(unknown)} on {model!r}")
    if constraint_type == "check":
        if not expression or not isinstance(expression, str):
            raise DefinitionError(f"check constraint on {model!r} needs expression:")
        columns = ()
    elif constraint_type == "foreign_key":
        if not columns or not reference or not fields:
            raise DefinitionError(f"foreign_key on {model!r} needs columns, to, and fields")
        if not isinstance(reference, str):
            raise DefinitionError(f"foreign_key to on {model!r} must be a table name")
    elif not columns:
        raise DefinitionError(f"constraint {constraint_type!r} on {model!r} needs a column")
    if constraint_type != "check":
        expression = None
    return ConstraintSpec(
        type=constraint_type,
        columns=columns,
        name=name,
        expression=str(expression) if expression else None,
        reference=str(reference) if reference else None,
        fields=fields,
    )


def parse_schema_policy(value: Any, model: str) -> SchemaPolicy:
    """Normalise a model's ``schema:`` drift policy. Defaults match today's external evolution."""
    if value is None:
        return SchemaPolicy()
    if isinstance(value, SchemaPolicy):
        return value
    if not isinstance(value, dict):
        raise DefinitionError(f"schema on {model!r} must be a mapping")
    unknown = set(value) - {"columns", "indexes", "constraints"}
    if unknown:
        raise DefinitionError(
            f"unknown schema key(s) {sorted(unknown)} on {model!r}; expected columns, indexes, constraints"
        )
    columns = str(value.get("columns", "additive"))
    indexes = str(value.get("indexes", "manage"))
    constraints = str(value.get("constraints", "manage"))
    if columns not in COLUMN_POLICIES:
        raise DefinitionError(f"schema.columns {columns!r} on {model!r} must be one of {sorted(COLUMN_POLICIES)}")
    if indexes not in OBJECT_POLICIES:
        raise DefinitionError(f"schema.indexes {indexes!r} on {model!r} must be one of {sorted(OBJECT_POLICIES)}")
    if constraints not in OBJECT_POLICIES:
        raise DefinitionError(
            f"schema.constraints {constraints!r} on {model!r} must be one of {sorted(OBJECT_POLICIES)}"
        )
    return SchemaPolicy(columns=columns, indexes=indexes, constraints=constraints)


def validate_physical_allowed(
    name: str,
    materialise: str,
    indexes: tuple[IndexSpec, ...],
    constraints: tuple[ConstraintSpec, ...],
    policy: SchemaPolicy,
) -> None:
    """Indexes and constraints need a real table. Views, ephemeral models, and files have none."""
    if materialise not in _NO_TABLE:
        return
    if indexes or constraints or policy != SchemaPolicy():
        raise DefinitionError(
            f"model {name!r}: indexes, constraints, and schema policy need a table "
            f"(materialise: virtual or table); materialise: {materialise} has nothing to alter"
        )


def physical_payload(
    indexes: tuple[IndexSpec, ...],
    constraints: tuple[ConstraintSpec, ...],
    policy: SchemaPolicy,
) -> dict[str, Any]:
    """Stable document hashed into ``physical_hash``. Empty when there is nothing to manage,
    so a model with no physical spec matches snapshots written before this column existed."""
    payload: dict[str, Any] = {}
    if indexes:
        payload["indexes"] = [
            {"columns": list(spec.columns), "unique": spec.unique, "name": spec.name} for spec in indexes
        ]
    if constraints:
        payload["constraints"] = [
            {
                "type": spec.type,
                "columns": list(spec.columns),
                "name": spec.name,
                "expression": spec.expression,
                "reference": spec.reference,
                "fields": list(spec.fields),
            }
            for spec in constraints
        ]
    if policy != SchemaPolicy():
        payload["schema"] = {"columns": policy.columns, "indexes": policy.indexes, "constraints": policy.constraints}
    return payload
