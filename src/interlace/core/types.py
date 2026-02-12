"""
Type definitions for Interlace.

Provides TypedDict and type aliases for better type safety.
"""

from __future__ import annotations

from typing import TypedDict, Optional, List, Dict, Any, Callable, Literal, Union
import ibis

# ---------------------------------------------------------------------------
# Domain type aliases — use these instead of ``Any`` where possible
# ---------------------------------------------------------------------------

#: Any ibis backend connection
ConnectionBackend = ibis.BaseBackend

#: The output a model function may return before conversion
ModelOutput = Union[ibis.Table, "pandas.DataFrame", list, dict, None]

#: Materialization strategy names
StrategyName = Literal["merge_by_key", "append", "replace", "scd_type_2", "none"]

#: Materialization type names
MaterializeType = Literal["table", "view", "ephemeral", "none"]

#: Model type discriminator
ModelType = Literal["python", "sql", "stream"]

#: Schema evolution mode names
SchemaMode = Literal["strict", "safe", "flexible", "lenient", "ignore"]

#: Field definitions in the formats accepted by @model(fields=...)
FieldsDef = Union[Dict[str, Any], list[tuple[str, str]], ibis.Schema, None]


class ModelInfo(TypedDict, total=False):
    """Type definition for model information dictionary."""
    
    name: str
    schema: str
    connection: Optional[str]
    materialise: str  # UK spelling
    strategy: Optional[str]
    primary_key: Optional[Union[str, List[str]]]
    dependencies: List[str]
    incremental: Optional[Dict[str, Any]]
    description: Optional[str]
    tags: Optional[List[str]]
    owner: Optional[str]
    fields: Optional[Union[Dict[str, Any], List[tuple], ibis.Schema]]
    file_path: Optional[str]
    type: str  # "python" or "sql"


class ConnectionConfig(TypedDict, total=False):
    """Type definition for connection configuration."""
    
    type: str
    path: Optional[str]
    config: Dict[str, Any]
    attach: List[Dict[str, Any]]
    pool: Dict[str, Any]


class ExecutorConfig(TypedDict, total=False):
    """Type definition for executor configuration."""
    
    max_iterations: int
    table_load_delay: float
    task_timeout: float
    max_workers: Optional[Union[int, str]]
    cache: Dict[str, int]


class MaterialisationResult(TypedDict):
    """Type definition for materialisation result."""
    
    status: str
    model: str
    rows: Optional[int]
    elapsed: float
    reason: Optional[str]


class ExecutionResult(TypedDict):
    """Type definition for execution result."""
    
    status: str
    model: str
    rows: Optional[int]
    elapsed: float
    error: Optional[str]
