"""
Type definitions for Interlace.

Provides TypedDict and type aliases for better type safety.
"""

from __future__ import annotations

from typing import Any, Literal, TypedDict

import ibis
import pandas

# ---------------------------------------------------------------------------
# Domain type aliases — use these instead of ``Any`` where possible
# ---------------------------------------------------------------------------

#: Any ibis backend connection
ConnectionBackend = ibis.BaseBackend

#: The output a model function may return before conversion
ModelOutput = ibis.Table | pandas.DataFrame | list | dict | None

#: Materialization strategy names
StrategyName = Literal["merge_by_key", "append", "replace", "scd_type_2", "none"]

#: Materialization type names
MaterializeType = Literal["table", "view", "ephemeral", "none"]

#: Model type discriminator
ModelType = Literal["python", "sql", "stream"]

#: Schema evolution mode names
SchemaMode = Literal["strict", "safe", "flexible", "lenient", "ignore"]

#: Field definitions in the formats accepted by @model(fields=...)
FieldsDef = dict[str, Any] | list[tuple[str, str]] | ibis.Schema | None


class ModelInfo(TypedDict, total=False):
    """Type definition for model information dictionary."""

    name: str
    schema: str
    connection: str | None
    materialise: str  # UK spelling
    strategy: str | None
    primary_key: str | list[str] | None
    dependencies: list[str]
    incremental: dict[str, Any] | None
    description: str | None
    tags: list[str] | None
    owner: str | None
    fields: dict[str, Any] | list[tuple] | ibis.Schema | None
    file_path: str | None
    type: str  # "python" or "sql"


class ConnectionConfig(TypedDict, total=False):
    """Type definition for connection configuration."""

    type: str
    path: str | None
    config: dict[str, Any]
    attach: list[dict[str, Any]]
    pool: dict[str, Any]


class ExecutorConfig(TypedDict, total=False):
    """Type definition for executor configuration."""

    max_iterations: int
    table_load_delay: float
    task_timeout: float
    max_workers: int | str | None
    cache: dict[str, int]


class MaterialisationResult(TypedDict):
    """Type definition for materialisation result."""

    status: str
    model: str
    rows: int | None
    elapsed: float
    reason: str | None


class ExecutionResult(TypedDict):
    """Type definition for execution result."""

    status: str
    model: str
    rows: int | None
    elapsed: float
    error: str | None
