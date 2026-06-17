"""Intermediate representation: the canonical sqlglot AST + Arrow contract."""

from __future__ import annotations

from interlace.ir.fingerprint import canonical_sql, data_fingerprint, metadata_fingerprint
from interlace.ir.relation import EngineRef, Relation, SqlRelation, StreamRelation, TableRef
from interlace.ir.schema import ArrowSchema, empty_schema, schema_from_fields

__all__ = [
    "ArrowSchema",
    "EngineRef",
    "Relation",
    "SqlRelation",
    "StreamRelation",
    "TableRef",
    "canonical_sql",
    "data_fingerprint",
    "empty_schema",
    "metadata_fingerprint",
    "schema_from_fields",
]
