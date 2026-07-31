"""Stream schemas: publish-time validation and the Arrow mapping.

A ``@stream`` declares ``schema={"field": "type", ...}``. Inbound payloads are
validated *before* they are durable; ``on_schema_drift`` picks the policy —
``reject`` refuses unknown fields and wrong types, ``evolve`` welcomes new
fields (they become real columns at flush), ``quarantine`` diverts failing
payloads to a ``<stream>__quarantine`` shadow stream. Missing fields become
NULL. The same declaration maps to the Arrow schema the materializer loads
into the warehouse.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import pyarrow as pa

from interlace.dsl.decorators import StreamDef
from interlace.exceptions import StreamError

_ARROW_TYPES: dict[str, pa.DataType] = {
    "int": pa.int64(),
    "integer": pa.int64(),
    "bigint": pa.int64(),
    "double": pa.float64(),
    "float": pa.float64(),
    "decimal": pa.float64(),
    "text": pa.string(),
    "string": pa.string(),
    "varchar": pa.string(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("us"),
    "json": pa.string(),
}
_SQL_TYPES: dict[str, str] = {
    "int": "BIGINT",
    "integer": "BIGINT",
    "bigint": "BIGINT",
    "double": "DOUBLE",
    "float": "DOUBLE",
    "decimal": "DOUBLE",
    "text": "TEXT",
    "string": "TEXT",
    "varchar": "TEXT",
    "bool": "BOOLEAN",
    "boolean": "BOOLEAN",
    "timestamp": "TIMESTAMP",
    "json": "TEXT",
}
_PY_TYPES: dict[str, tuple[type, ...]] = {
    "int": (int,),
    "integer": (int,),
    "bigint": (int,),
    "double": (int, float),
    "float": (int, float),
    "decimal": (int, float),
    "text": (str,),
    "string": (str,),
    "varchar": (str,),
    "bool": (bool,),
    "boolean": (bool,),
    "timestamp": (str,),  # ISO strings; parsed by Arrow on load
    "json": (str, dict, list),
}


def arrow_schema(stream: StreamDef) -> pa.Schema:
    """The warehouse-facing schema: declared fields + ingestion metadata."""
    fields = []
    for name, type_name in stream.schema.items():
        arrow_type = _ARROW_TYPES.get(type_name.lower())
        if arrow_type is None:
            raise StreamError(
                f"stream {stream.name!r} field {name!r} has unknown type {type_name!r}; "
                f"expected one of {sorted(_ARROW_TYPES)}"
            )
        fields.append(pa.field(name, arrow_type))
    fields += [pa.field("_offset", pa.int64()), pa.field("_ingested_at", pa.timestamp("us"))]
    return pa.schema(fields)


def coerce_value(type_name: str, value: Any) -> Any:
    """Convert a validated JSON value to what the Arrow builder expects."""
    if value is not None and type_name.lower() == "timestamp" and isinstance(value, str):
        return datetime.fromisoformat(value)
    if value is not None and type_name.lower() == "json" and not isinstance(value, str):
        return json.dumps(value)
    return value


def sql_columns(stream: StreamDef) -> list[tuple[str, str]]:
    """(name, SQL type) pairs for the warehouse table: declared fields + metadata."""
    columns = []
    for name, type_name in stream.schema.items():
        sql_type = _SQL_TYPES.get(type_name.lower())
        if sql_type is None:
            raise StreamError(
                f"stream {stream.name!r} field {name!r} has unknown type {type_name!r}; "
                f"expected one of {sorted(_SQL_TYPES)}"
            )
        columns.append((name, sql_type))
    return [*columns, ("_offset", "BIGINT"), ("_ingested_at", "TIMESTAMP")]


def _row_error(stream: StreamDef, row: Any, *, allow_unknown: bool) -> str | None:
    """The first validation problem in ``row``, or None if it conforms."""
    declared = stream.schema
    if not isinstance(row, dict):
        return "event must be an object"
    if not allow_unknown:
        unknown = set(row) - set(declared)
        if unknown:
            return f"undeclared fields {sorted(unknown)}"
    for name, value in row.items():
        if value is None or name not in declared:
            continue
        type_name = declared[name].lower()
        if allow_unknown and _coerce_declared(type_name, value) is not _UNCOERCIBLE:
            continue  # evolve: safe coercion satisfies the declared type
        expected = _PY_TYPES[type_name]
        if isinstance(value, bool) and bool not in expected:  # bool is an int subtype; keep them apart
            return f"field {name!r} must be {declared[name]}"
        if not isinstance(value, expected):
            return f"field {name!r} must be {declared[name]}, got {type(value).__name__}"
        # publish/flush PARITY: anything accepted here must actually coerce at
        # materialization, or one durable event freezes the stream's watermark
        # forever (a "not-a-timestamp" string is a str, but fromisoformat fails)
        try:
            materialized = coerce_value(type_name, value)
            if type_name in ("int", "integer", "bigint") and not (-(2**63) <= materialized < 2**63):
                return f"field {name!r} overflows BIGINT"
        except (ValueError, TypeError, OverflowError):
            return f"field {name!r} is not a valid {declared[name]}: {value!r}"
    return None


def validate_rows(stream: StreamDef, rows: list[dict[str, Any]]) -> None:
    """``on_schema_drift: reject`` — raise on the first drifting payload.
    Missing fields are allowed and load as NULL."""
    for index, row in enumerate(rows):
        error = _row_error(stream, row, allow_unknown=False)
        if error:
            raise StreamError(f"stream {stream.name!r} event {index} has {error} (on_schema_drift: reject)")


def validate_rows_evolve(stream: StreamDef, rows: list[dict[str, Any]]) -> None:
    """``on_schema_drift: evolve`` — unknown fields are welcome (they become
    columns at flush time); declared fields accept safe coercions; an
    *incompatible* type change still rejects — evolution never hides breakage."""
    for index, row in enumerate(rows):
        error = _row_error(stream, row, allow_unknown=True)
        if error:
            raise StreamError(f"stream {stream.name!r} event {index} has {error} (on_schema_drift: evolve)")


def partition_rows(stream: StreamDef, rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[tuple[Any, str]]]:
    """``on_schema_drift: quarantine`` — split into (conforming, [(row, error), ...])."""
    valid: list[dict[str, Any]] = []
    failed: list[tuple[Any, str]] = []
    for row in rows:
        error = _row_error(stream, row, allow_unknown=False)
        if error:
            failed.append((row, error))
        else:
            valid.append(row)
    return valid, failed


_UNCOERCIBLE = object()


def _coerce_declared(type_name: str, value: Any) -> Any:
    """Coerce ``value`` toward a declared type, or ``_UNCOERCIBLE``. Widening only:
    int fits a double; scalars stringify into text/json; nothing narrows."""
    expected = _PY_TYPES[type_name]
    if isinstance(value, bool):
        return value if bool in expected else _UNCOERCIBLE
    if isinstance(value, expected):
        return value
    if type_name in ("double", "float", "decimal") and isinstance(value, int):
        return float(value)
    if type_name in ("text", "string", "varchar", "json") and isinstance(value, (int, float, bool, dict, list)):
        return json.dumps(value) if isinstance(value, (dict, list)) else str(value)
    return _UNCOERCIBLE


def infer_sql_type(value: Any) -> str:
    """SQL type for a field first seen in the data (evolve mode)."""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "BIGINT"
    if isinstance(value, float):
        return "DOUBLE"
    return "TEXT"  # strings, and dict/list stored as JSON text


def evolved_columns(stream: StreamDef, rows: list[dict[str, Any]]) -> dict[str, str]:
    """Undeclared fields present in ``rows`` -> inferred SQL type. Conflicting
    inferences across a batch widen to TEXT."""
    extras: dict[str, str] = {}
    for row in rows:
        for name, value in row.items():
            if name in stream.schema or value is None:
                continue
            inferred = infer_sql_type(value)
            if name in extras and extras[name] != inferred:
                extras[name] = "TEXT"
            else:
                extras.setdefault(name, inferred)
    return extras


def coerce_row(stream: StreamDef, row: dict[str, Any], extras: dict[str, str]) -> dict[str, Any]:
    """A row ready for the Arrow builder: declared fields coerced, extras stringified as needed."""
    out: dict[str, Any] = {}
    for name, type_name in stream.schema.items():
        value = row.get(name)
        coerced = _coerce_declared(type_name.lower(), value) if value is not None else None
        out[name] = coerce_value(type_name, coerced if coerced is not _UNCOERCIBLE else None)
    for name, sql_type in extras.items():
        value = row.get(name)
        if value is not None and sql_type == "TEXT" and not isinstance(value, str):
            value = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        out[name] = value
    return out
