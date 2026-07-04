"""Check declarations.

A model declares checks in its config (SQL block or ``@model(checks=[...])``)::

    checks:
      - not_null: order_id                 # shorthand: {type: column}
      - unique: [order_id, customer_id]
      - accepted_values: {column: status, values: [open, closed]}
      - relationships: {column: customer_id, to: customers, field: customer_id}
      - expression: {expression: "amount >= 0", severity: warn}
      - row_count: {min: 1}
      - range: {column: amount, min: 0}
      - pattern: {column: email, regex: ".+@.+"}
      - freshness: {column: updated_at, max_age: 2h}
      - sql: {query: "SELECT * FROM {table} WHERE total < 0"}

Each entry normalises to a :class:`CheckSpec`. Severity: ``error`` (default)
fails the apply before promotion; ``warn``/``info`` record and continue.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from interlace.exceptions import DefinitionError

SEVERITIES = frozenset({"error", "warn", "info"})
CHECK_TYPES = frozenset(
    {
        "not_null",
        "unique",
        "accepted_values",
        "row_count",
        "freshness",
        "expression",
        "relationships",
        "pattern",
        "range",
        "sql",
    }
)
_COLUMN_SHORTHAND = frozenset({"not_null", "unique", "range", "pattern", "freshness", "accepted_values"})


@dataclass(frozen=True)
class CheckSpec:
    """One declared check on a model."""

    type: str
    columns: tuple[str, ...] = ()  # subject column(s); empty for table-level checks
    severity: str = "error"
    params: dict[str, Any] = field(default_factory=dict)  # type-specific settings

    @property
    def name(self) -> str:
        return "_".join([self.type, *self.columns]) if self.columns else self.type


def _as_columns(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(v) for v in value)
    raise DefinitionError(f"check column must be a name or list of names, got {type(value).__name__}")


def _parse_one(entry: Any, model: str) -> CheckSpec:
    if isinstance(entry, str):  # bare "not_null" is meaningless without a column
        raise DefinitionError(f"check {entry!r} on {model!r} needs a column or config mapping")
    if not isinstance(entry, dict) or not entry:
        raise DefinitionError(f"invalid check entry on {model!r}: {entry!r}")

    if "type" in entry:  # explicit form: {type: not_null, column: id, ...}
        params = dict(entry)
        check_type = str(params.pop("type"))
        value: Any = params.pop("column", None) or params.pop("columns", None)
    else:  # shorthand form: {not_null: id} or {row_count: {min: 1}}
        if len(entry) != 1:
            raise DefinitionError(f"ambiguous check entry on {model!r}: {entry!r}; use the {{type: ...}} form")
        check_type, value = next(iter(entry.items()))
        check_type = str(check_type)
        if check_type not in CHECK_TYPES:
            raise DefinitionError(
                f"unknown check type {check_type!r} on {model!r}; expected one of {sorted(CHECK_TYPES)}"
            )
        params = {}
        if isinstance(value, dict):  # {accepted_values: {column: status, values: [...]}}
            params = dict(value)
            value = params.pop("column", None) or params.pop("columns", None)
        elif check_type not in _COLUMN_SHORTHAND:
            raise DefinitionError(f"check {check_type!r} on {model!r} needs a config mapping")

    if check_type not in CHECK_TYPES:
        raise DefinitionError(f"unknown check type {check_type!r} on {model!r}; expected one of {sorted(CHECK_TYPES)}")
    severity = str(params.pop("severity", "error"))
    if severity not in SEVERITIES:
        raise DefinitionError(f"unknown check severity {severity!r} on {model!r}; expected one of {sorted(SEVERITIES)}")
    return CheckSpec(type=check_type, columns=_as_columns(value), severity=severity, params=params)


def parse_checks(value: Any, model: str) -> tuple[CheckSpec, ...]:
    """Normalise a model's ``checks:`` config into :class:`CheckSpec` tuples."""
    if value is None:
        return ()
    if isinstance(value, CheckSpec):
        return (value,)
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise DefinitionError(f"checks on {model!r} must be a list")
    return tuple(entry if isinstance(entry, CheckSpec) else _parse_one(entry, model) for entry in value)
