"""
Ibis expression tree lineage extractor.

Extracts column-level lineage by walking ibis expression trees
to trace column origins through transformations.
"""

import inspect
import logging
from datetime import datetime
from typing import Any

import ibis
from ibis.expr import types as ir

from interlace.lineage.extractor import (
    ColumnInfo,
    ColumnLineage,
    ColumnLineageEdge,
    LineageExtractor,
    TransformationType,
)

logger = logging.getLogger("interlace.lineage.ibis")


class IbisLineageExtractor(LineageExtractor):
    """
    Extracts column-level lineage from Python models using ibis.

    Analyzes ibis expression trees to trace how columns flow from
    input tables to output columns.
    """

    def extract(
        self,
        model_name: str,
        model_info: dict[str, Any],
        available_models: dict[str, dict[str, Any]],
    ) -> ColumnLineage:
        """
        Extract column lineage from a Python/ibis model.

        Args:
            model_name: Name of the model
            model_info: Model metadata including the function
            available_models: All available models for resolving dependencies

        Returns:
            ColumnLineage for this model
        """
        columns: list[ColumnInfo] = []
        dependencies = model_info.get("dependencies", [])

        # Try to analyze the function
        func = model_info.get("function")
        if func is None:
            # No function available, use field definitions
            return self._lineage_from_fields(model_name, model_info, dependencies)

        # Create mock ibis tables for dependencies
        mock_tables = self._create_mock_tables(dependencies, available_models)

        try:
            # Try to call the function with mock tables
            result = self._call_with_mocks(func, mock_tables)

            if result is not None and isinstance(result, ir.Table):
                columns = self._analyze_ibis_table(result, model_name, dependencies, available_models)
        except Exception as e:
            logger.debug(f"Could not analyze ibis expression for {model_name}: {e}")
            # Fall back to field-based lineage
            return self._lineage_from_fields(model_name, model_info, dependencies)

        return ColumnLineage(
            model_name=model_name,
            columns=columns,
            computed_at=datetime.utcnow(),
        )

    def _create_mock_tables(
        self,
        dependencies: list[str],
        available_models: dict[str, dict[str, Any]],
    ) -> dict[str, ir.Table]:
        """
        Create mock ibis tables for dependencies.

        Uses schema information from model metadata to create
        properly typed mock tables.
        """
        mock_tables = {}

        for dep_name in dependencies:
            schema_dict = self._get_schema_for_model(dep_name, available_models)
            if schema_dict:
                try:
                    # Create memtable with empty data but proper schema
                    data: dict[str, list[Any]] = {col: [] for col in schema_dict.keys()}
                    mock_tables[dep_name] = ibis.memtable(data, schema=ibis.schema(schema_dict))
                except Exception as e:
                    logger.debug(f"Could not create mock table for {dep_name}: {e}")
                    # Create minimal mock with string columns
                    mock_tables[dep_name] = ibis.memtable({"__placeholder__": []}, schema={"__placeholder__": "string"})
            else:
                # No schema available, create placeholder
                mock_tables[dep_name] = ibis.memtable({"__placeholder__": []}, schema={"__placeholder__": "string"})

        return mock_tables

    def _get_schema_for_model(
        self,
        model_name: str,
        available_models: dict[str, dict[str, Any]],
    ) -> dict[str, str] | None:
        """Get schema dictionary for a model."""
        if model_name not in available_models:
            return None

        model_info = available_models[model_name]
        fields = model_info.get("fields")

        if fields is None:
            return None

        if isinstance(fields, dict):
            # Convert Python types to ibis type strings
            schema_dict = {}
            for col_name, col_type in fields.items():
                if isinstance(col_type, type):
                    if col_type is int:
                        schema_dict[col_name] = "int64"
                    elif col_type is float:
                        schema_dict[col_name] = "float64"
                    elif col_type is str:
                        schema_dict[col_name] = "string"
                    elif col_type is bool:
                        schema_dict[col_name] = "boolean"
                    else:
                        schema_dict[col_name] = "string"
                else:
                    schema_dict[col_name] = str(col_type)
            return schema_dict
        elif hasattr(fields, "items"):
            # ibis.Schema-like object
            return {name: str(dtype) for name, dtype in fields.items()}

        return None

    def _call_with_mocks(self, func: Any, mock_tables: dict[str, ir.Table]) -> ir.Table | None:
        """
        Call the model function with mock tables as arguments.

        Maps function parameters to mock tables by name.
        """
        sig = inspect.signature(func)
        kwargs = {}

        for param_name, param in sig.parameters.items():
            if param_name in mock_tables:
                kwargs[param_name] = mock_tables[param_name]
            elif param.kind == inspect.Parameter.VAR_KEYWORD:
                continue
            elif param.default is not inspect.Parameter.empty:
                continue
            else:
                # Can't resolve parameter, skip analysis
                return None

        # Call the function (may raise if logic depends on data)
        if inspect.iscoroutinefunction(func):
            # Can't easily call async functions synchronously for analysis
            return None

        return func(**kwargs)

    def _analyze_ibis_table(
        self,
        table: ir.Table,
        model_name: str,
        dependencies: list[str],
        available_models: dict[str, dict[str, Any]],
    ) -> list[ColumnInfo]:
        """
        Analyze an ibis Table expression to extract column lineage.

        Walks the expression tree to trace column origins.
        """
        columns: list[ColumnInfo] = []

        # Get output schema
        try:
            schema = table.schema()
        except Exception:
            return columns

        for col_name in schema.names:
            try:
                col_expr = table[col_name]
                sources = self._trace_column_expression(col_expr, model_name, dependencies, available_models)

                col_info = ColumnInfo(
                    name=col_name,
                    data_type=str(schema[col_name]),
                    sources=sources,
                )
                columns.append(col_info)
            except Exception as e:
                logger.debug(f"Could not trace column {col_name}: {e}")
                columns.append(ColumnInfo(name=col_name))

        return columns

    def _trace_column_expression(
        self,
        expr: ir.Column,
        model_name: str,
        dependencies: list[str],
        available_models: dict[str, dict[str, Any]],
        visited: set[int] | None = None,
    ) -> list[ColumnLineageEdge]:
        """
        Trace an ibis column expression to find source columns.

        Walks the expression tree recursively.
        """
        if visited is None:
            visited = set()

        expr_id = id(expr)
        if expr_id in visited:
            return []
        visited.add(expr_id)

        sources = []

        try:
            # Get the operation from the expression
            op = expr.op()
            sources.extend(self._analyze_operation(op, model_name, dependencies, available_models, visited))
        except Exception as e:
            logger.debug(f"Could not analyze expression: {e}")

        return sources

    def _analyze_operation(
        self,
        op: Any,
        model_name: str,
        dependencies: list[str],
        available_models: dict[str, dict[str, Any]],
        visited: set[int],
    ) -> list[ColumnLineageEdge]:
        """
        Analyze an ibis operation node to find source columns.

        Handles different operation types and traces through the tree.
        """
        from ibis.expr import operations as ops

        sources = []

        # Field/Column reference - this is a leaf node
        if isinstance(op, ops.Field):
            # Get the source table relation
            rel = op.rel
            col_name = op.name

            # Try to identify the source model
            source_model = self._identify_source_model(rel, dependencies)

            if source_model:
                sources.append(
                    ColumnLineageEdge(
                        source_model=source_model,
                        source_column=col_name,
                        output_model=model_name,
                        output_column="",  # Will be set by caller
                        transformation_type=TransformationType.DIRECT,
                    )
                )
            return sources

        # Alias/rename operation
        if isinstance(op, ops.Alias):
            arg = op.arg
            if hasattr(arg, "op"):
                inner_sources = self._analyze_operation(arg.op(), model_name, dependencies, available_models, visited)
                # Mark as renamed
                for src in inner_sources:
                    src.transformation_type = TransformationType.RENAMED
                sources.extend(inner_sources)
            return sources

        # Cast operation
        if isinstance(op, ops.Cast):
            arg = op.arg
            if hasattr(arg, "op"):
                inner_sources = self._analyze_operation(arg.op(), model_name, dependencies, available_models, visited)
                for src in inner_sources:
                    src.transformation_type = TransformationType.CAST
                sources.extend(inner_sources)
            return sources

        # Aggregation operations
        agg_ops = (
            getattr(ops, "Sum", type(None)),
            getattr(ops, "Mean", type(None)),
            getattr(ops, "Min", type(None)),
            getattr(ops, "Max", type(None)),
            getattr(ops, "Count", type(None)),
            getattr(ops, "CountStar", type(None)),
        )
        if isinstance(op, agg_ops):
            # Check if op has an 'arg' attribute for the aggregated column
            if hasattr(op, "arg"):
                arg = op.arg
                if hasattr(arg, "op"):
                    inner_sources = self._analyze_operation(
                        arg.op(), model_name, dependencies, available_models, visited
                    )
                    for src in inner_sources:
                        src.transformation_type = TransformationType.AGGREGATED
                    sources.extend(inner_sources)
            return sources

        # Window function operations
        window_ops = (
            getattr(ops, "WindowFunction", type(None)),
            getattr(ops, "RowNumber", type(None)),
            getattr(ops, "Lag", type(None)),
            getattr(ops, "Lead", type(None)),
        )
        if isinstance(op, window_ops):
            if hasattr(op, "arg"):
                arg = op.arg
                if hasattr(arg, "op"):
                    inner_sources = self._analyze_operation(
                        arg.op(), model_name, dependencies, available_models, visited
                    )
                    for src in inner_sources:
                        src.transformation_type = TransformationType.WINDOW
                    sources.extend(inner_sources)
            return sources

        # Binary operations (arithmetic, comparison, etc.)
        binary_ops = (
            getattr(ops, "Add", type(None)),
            getattr(ops, "Subtract", type(None)),
            getattr(ops, "Multiply", type(None)),
            getattr(ops, "Divide", type(None)),
            getattr(ops, "StringConcat", type(None)),
        )
        if isinstance(op, binary_ops):
            for attr in ("left", "right"):
                if hasattr(op, attr):
                    arg = getattr(op, attr)
                    if hasattr(arg, "op"):
                        inner_sources = self._analyze_operation(
                            arg.op(), model_name, dependencies, available_models, visited
                        )
                        for src in inner_sources:
                            src.transformation_type = TransformationType.DERIVED
                        sources.extend(inner_sources)
            return sources

        # Generic handling: check all attributes that might be expressions
        for attr_name in dir(op):
            if attr_name.startswith("_"):
                continue
            try:
                attr = getattr(op, attr_name)
                if hasattr(attr, "op"):
                    inner_sources = self._analyze_operation(
                        attr.op(), model_name, dependencies, available_models, visited
                    )
                    if inner_sources:
                        for src in inner_sources:
                            if src.transformation_type == TransformationType.UNKNOWN:
                                src.transformation_type = TransformationType.DERIVED
                        sources.extend(inner_sources)
            except Exception:
                continue

        return sources

    def _identify_source_model(self, rel: Any, dependencies: list[str]) -> str | None:
        """
        Identify which dependency a relation refers to.

        Uses the relation's name to match against known dependencies.
        """
        # Try to get table name from relation
        if hasattr(rel, "name"):
            name = rel.name
            if name in dependencies:
                return name  # type: ignore[no-any-return]

        # Try to match by checking if it's a UnboundTable or similar
        rel_name = getattr(rel, "name", None)
        if rel_name:
            for dep in dependencies:
                if dep in str(rel_name):
                    return dep

        # Check op chain
        if hasattr(rel, "op"):
            op = rel.op()
            if hasattr(op, "name"):
                name = op.name
                if name in dependencies:
                    return name  # type: ignore[no-any-return]

        return None

    def _lineage_from_fields(
        self,
        model_name: str,
        model_info: dict[str, Any],
        dependencies: list[str],
    ) -> ColumnLineage:
        """
        Create lineage based on field definitions when analysis is not possible.

        Creates placeholder lineage showing columns exist but sources unknown.
        """
        columns = []
        fields = model_info.get("fields", {})

        if isinstance(fields, dict):
            for col_name, col_type in fields.items():
                columns.append(
                    ColumnInfo(
                        name=col_name,
                        data_type=str(col_type) if col_type else None,
                        sources=[],  # Unknown sources
                    )
                )
        elif hasattr(fields, "items"):
            for col_name, col_type in fields.items():
                columns.append(
                    ColumnInfo(
                        name=col_name,
                        data_type=str(col_type),
                        sources=[],
                    )
                )

        return ColumnLineage(
            model_name=model_name,
            columns=columns,
            computed_at=datetime.utcnow(),
        )
