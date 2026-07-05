"""Stream schemas: publish-time validation and the Arrow mapping.

A ``@stream`` declares ``schema={"field": "type", ...}``. Inbound payloads are
validated *before* they are durable — unknown fields and wrong types are
rejected (``on_schema_drift: reject``; evolve/quarantine are future modes),
missing fields become NULL. The same declaration maps to the Arrow schema the
materializer loads into the warehouse.
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


def validate_rows(stream: StreamDef, rows: list[dict[str, Any]]) -> None:
    """Reject payloads that drift from the declared schema (extra fields, wrong
    types). Missing fields are allowed and load as NULL."""
    declared = stream.schema
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise StreamError(f"stream {stream.name!r} event {index} must be an object")
        unknown = set(row) - set(declared)
        if unknown:
            raise StreamError(
                f"stream {stream.name!r} event {index} has undeclared fields {sorted(unknown)} "
                f"(on_schema_drift: reject)"
            )
        for name, value in row.items():
            if value is None:
                continue
            expected = _PY_TYPES[declared[name].lower()]
            if isinstance(value, bool) and bool not in expected:  # bool is an int subtype; keep them apart
                raise StreamError(f"stream {stream.name!r} event {index} field {name!r} must be {declared[name]}")
            if not isinstance(value, expected):
                raise StreamError(
                    f"stream {stream.name!r} event {index} field {name!r} must be {declared[name]}, "
                    f"got {type(value).__name__}"
                )
