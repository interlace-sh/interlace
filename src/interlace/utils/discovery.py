"""
Model discovery utilities.

Phase 0: Discover models from Python files and SQL files with dependency extraction.
"""

import importlib.util
import inspect
import logging
import re
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp

from interlace.utils.hashing import calculate_file_hash, store_file_hash

# In-memory cache for file hashes during discovery to avoid re-reading files
_file_hash_cache: dict[str, str] = {}


def discover_python_models(models_dir: Path, connection: Any | None = None) -> dict[str, dict[str, Any]]:
    """
    Discover Python models from directory.

    Extracts dependencies from function parameters (implicit dependencies).
    Calculates and stores file hashes for change detection.
    Uses in-memory cache to avoid re-reading files when multiple models are in the same file.

    Args:
        models_dir: Directory containing model files
        connection: Optional database connection for storing file hashes

    Returns:
        Dictionary mapping model names to model info
    """
    models = {}
    logger = logging.getLogger("interlace.utils.discovery")
    global _file_hash_cache

    for py_file in models_dir.glob("**/*.py"):
        if py_file.name.startswith("test_"):
            continue

        try:
            spec = importlib.util.spec_from_file_location(py_file.stem, py_file)
            if spec is None or spec.loader is None:
                continue
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Find functions with _interlace_model attribute (models and streams)
            for name in dir(module):
                obj = getattr(module, name)
                if callable(obj) and hasattr(obj, "_interlace_model"):
                    model_info = obj._interlace_model.copy()
                    model_info["file"] = str(py_file)
                    model_info["function"] = obj
                    # Type is set by decorator; default to python if missing
                    model_info["type"] = model_info.get("type", "python")

                    # Extract dependencies from function parameters (only for python/sql-like models)
                    # Streams declare no dependencies by default.
                    if model_info.get("type", "python") == "python" and model_info.get("dependencies") is None:
                        deps = _extract_python_dependencies(obj)
                        if deps:
                            model_info["dependencies"] = deps

                    # Calculate and store file hash (use cache to avoid re-reading)
                    file_path_str = str(py_file)
                    if file_path_str not in _file_hash_cache:
                        _file_hash_cache[file_path_str] = calculate_file_hash(py_file)
                    file_hash = _file_hash_cache[file_path_str]
                    model_info["file_hash"] = file_hash
                    if connection:
                        try:
                            model_name = model_info.get("name")
                            schema_name = model_info.get("schema", "public")
                            if model_name:
                                store_file_hash(connection, model_name, schema_name, str(py_file), file_hash)
                        except Exception as e:
                            logger.debug(f"Could not store file hash for {model_name}: {e}")

                    models[model_info["name"]] = model_info
        except Exception as e:
            logger.error(f"Error loading {py_file}: {e}", exc_info=True)

    return models


def _extract_python_dependencies(func: Any) -> list[str]:
    """
    Extract model dependencies from Python function parameters.

    Function parameters that match model names are treated as dependencies.
    """
    sig = inspect.signature(func)
    dependencies = []

    for param_name, param in sig.parameters.items():
        # Skip special parameters (self, cls, connection, *args, **kwargs)
        if param_name in ("self", "cls", "connection") or param.kind in (
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        ):
            continue

        # Parameter name is treated as a potential model dependency
        dependencies.append(param_name)

    return dependencies


def discover_sql_models(models_dir: Path, connection: Any | None = None) -> dict[str, dict[str, Any]]:
    """
    Discover SQL models from directory.

    Extracts dependencies from FROM/JOIN clauses using sqlglot.
    Calculates and stores file hashes for change detection.
    Supports multiple models in a single SQL file.
    Uses in-memory cache to avoid re-reading files when multiple models are in the same file.

    Args:
        models_dir: Directory containing model files
        connection: Optional database connection for storing file hashes

    Returns:
        Dictionary mapping model names to model info
    """
    models = {}
    logger = logging.getLogger("interlace.utils.discovery")
    global _file_hash_cache

    for sql_file in models_dir.glob("**/*.sql"):
        try:
            content = sql_file.read_text()

            # Parse all model definitions from the file (supports multiple models per file)
            model_definitions = _parse_multiple_sql_models(content)

            for model_info in model_definitions:
                if "name" in model_info:
                    model_info["file"] = str(sql_file)
                    model_info["type"] = "sql"

                    # Extract dependencies from the specific query for this model
                    query = model_info.get("query", "")
                    if model_info.get("dependencies") is None:
                        deps = _extract_sql_dependencies(query)
                        if deps:
                            model_info["dependencies"] = deps

                    # Calculate and store file hash (use cache to avoid re-reading)
                    file_path_str = str(sql_file)
                    if file_path_str not in _file_hash_cache:
                        _file_hash_cache[file_path_str] = calculate_file_hash(sql_file)
                    file_hash = _file_hash_cache[file_path_str]
                    model_info["file_hash"] = file_hash
                    if connection:
                        try:
                            model_name = model_info.get("name")
                            schema_name = model_info.get("schema", "public")
                            if model_name:
                                store_file_hash(connection, model_name, schema_name, str(sql_file), file_hash)
                        except Exception as e:
                            logger.debug(f"Could not store file hash for {model_name}: {e}")

                    models[model_info["name"]] = model_info
        except Exception as e:
            logger.error(f"Error loading {sql_file}: {e}", exc_info=True)

    return models


def _extract_sql_dependencies(sql_content: str) -> list[str]:
    """
    Extract model dependencies from SQL query using sqlglot.

    Looks for table references in FROM and JOIN clauses.
    Filters out system tables, functions, and non-model references.

    Note: Cross-connection references (e.g., "pg_db.public.orders") are handled via
    DuckDB ATTACH. The database.table format (pg_db.public) is detected but the
    connection name (pg_db) is not used as a dependency - only the table name.
    """
    dependencies: set[str] = set()

    try:
        # Parse SQL - handle multiple statements
        statements = sqlglot.parse(sql_content, read="duckdb")

        for statement in statements:
            if not statement:
                continue

            # Find all table references
            for table in statement.find_all(exp.Table):
                table_name = table.name

                # Skip system tables and functions
                if table_name.startswith("_"):
                    continue

                # Skip read_* functions (read_csv, read_parquet, etc.)
                if table_name.startswith("read_"):
                    continue

                # Handle qualified names:
                # - database.table (cross-connection via ATTACH, e.g., pg_db.public.orders)
                # - schema.table (same connection, e.g., public.orders)
                # For dependencies, use just the table name (connection routing handled separately)
                # The database.table format is handled by DuckDB ATTACH configuration

                dependencies.add(table_name)
    except Exception as e:
        # If parsing fails, fall back to regex-based extraction
        logger = logging.getLogger("interlace.utils.discovery")
        logger.warning(f"SQL parsing failed, using regex fallback: {e}")
        dependencies.update(_extract_sql_dependencies_regex(sql_content))

    return sorted(dependencies)


def _extract_sql_dependencies_regex(sql_content: str) -> set[str]:
    """
    Fallback regex-based dependency extraction.

    Looks for table names in FROM and JOIN clauses.
    """
    dependencies: set[str] = set()

    # Pattern to match FROM/JOIN table references
    # FROM table_name, FROM schema.table_name, JOIN table_name, etc.
    patterns = [
        r"(?:FROM|JOIN)\s+(?:(\w+)\.)?(\w+)",  # FROM schema.table or FROM table
        r"FROM\s+(\w+)\s*\(",  # FROM function() - skip these
    ]

    for pattern in patterns:
        for match in re.finditer(pattern, sql_content, re.IGNORECASE):
            if match.lastindex is not None and match.lastindex >= 2:
                table = match.group(2)
            elif match.lastindex is not None and match.lastindex >= 1:
                table = match.group(1)
            else:
                continue

            # Skip read_* functions
            if table and not table.startswith("read_"):
                dependencies.add(table)

    return dependencies


def _parse_sql_annotations(content: str) -> dict[str, Any]:
    """Parse SQL annotations from comments."""
    annotations: dict[str, Any] = {}

    # Match -- @key: value
    pattern = r"--\s*@(\w+):\s*(.+)"

    for match in re.finditer(pattern, content):
        key = match.group(1)
        value = match.group(2).strip()

        # Handle materialise/materialize (normalize spelling)
        if key in ["materialise", "materialize"]:
            key = "materialise"  # Normalize to British spelling

        # Handle list values (comma-separated)
        if "," in value and key in ["materialise", "dependencies", "tags"]:
            annotations[key] = [v.strip() for v in value.split(",")]
        # Handle dependencies/tags as single value (still make it a list)
        elif key in ["dependencies", "tags"]:
            # Always make dependencies/tags a list, even if single value
            annotations[key] = [value.strip()]
        # Handle fields (JSON dict format or native ibis schema formats)
        elif key == "fields":
            try:
                import json

                annotations[key] = json.loads(value)
            except json.JSONDecodeError:
                # If not valid JSON, try to parse as simple key:value pairs
                # Format: "col1:str,col2:int"
                schema_dict = {}
                for pair in value.split(","):
                    if ":" in pair:
                        col_name, col_type = pair.split(":", 1)
                        schema_dict[col_name.strip()] = col_type.strip()
                annotations[key] = schema_dict if schema_dict else None
        # Handle boolean values
        elif value.lower() in ("true", "false"):
            annotations[key] = value.lower() == "true"
        else:
            annotations[key] = value

    return annotations


def _parse_multiple_sql_models(content: str) -> list[dict[str, Any]]:
    """
    Parse multiple SQL model definitions from a single file.

    Each model is defined by annotations (-- @name:, -- @schema:, etc.) followed by SQL.
    Models are separated by the next set of annotations or end of file.

    Returns:
        List of model info dictionaries, each with its own query extracted.
    """
    models = []
    lines = content.split("\n")

    current_model = None
    current_query_start = None
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if this line starts a new model definition
        name_match = re.match(r"--\s*@name:\s*(.+)", line)
        if name_match:
            # Save previous model if exists
            if current_model and current_query_start is not None:
                # Extract query from start to before this line
                query_lines = lines[current_query_start:i]
                current_model["query"] = "\n".join(query_lines).strip()
                if current_model.get("query"):
                    models.append(current_model)

            # Start new model
            current_model = {}
            # Parse all annotations for this model (they come before the SQL)
            annotation_lines = []
            j = i
            while j < len(lines) and (lines[j].strip().startswith("--") or not lines[j].strip()):
                annotation_lines.append(lines[j])
                j += 1

            annotation_block = "\n".join(annotation_lines)
            current_model = _parse_sql_annotations(annotation_block)
            current_query_start = j  # SQL starts after annotations
            i = j
            continue

        i += 1

    # Save last model
    if current_model and current_query_start is not None:
        query_lines = lines[current_query_start:]
        current_model["query"] = "\n".join(query_lines).strip()
        if current_model.get("query"):
            models.append(current_model)

    # Fallback: if no models found, try single model (SQL file without @name annotation)
    if not models:
        model_info = _parse_sql_annotations(content)
        if "name" in model_info:
            model_info["query"] = content
            models.append(model_info)

    return models


def discover_models(models_dir: Path, connection: Any | None = None) -> dict[str, dict[str, Any]]:
    """
    Discover all models (Python and SQL) from directory.

    Args:
        models_dir: Directory containing model files
        connection: Optional database connection for storing file hashes

    Returns:
        Dictionary mapping model names to model info
    """
    global _file_hash_cache
    # Clear cache at start of discovery to ensure fresh hashes
    _file_hash_cache.clear()

    all_models = {}

    # Discover Python models
    python_models = discover_python_models(models_dir, connection)
    all_models.update(python_models)

    # Discover SQL models
    sql_models = discover_sql_models(models_dir, connection)
    all_models.update(sql_models)

    return all_models
