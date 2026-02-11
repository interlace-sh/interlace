"""
SQL lineage extractor using SQLGlot.

Extracts column-level lineage from SQL models by parsing SQL
and analyzing column references and transformations.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import sqlglot
from sqlglot import exp
from sqlglot.lineage import lineage as sqlglot_lineage

from interlace.lineage.extractor import (
    ColumnInfo,
    ColumnLineage,
    ColumnLineageEdge,
    LineageExtractor,
    TransformationType,
)

logger = logging.getLogger("interlace.lineage.sql")


class SqlLineageExtractor(LineageExtractor):
    """
    Extracts column-level lineage from SQL models using SQLGlot.

    Uses SQLGlot's lineage module when available, with fallback
    to manual AST analysis.
    """

    def __init__(self, dialect: str = "duckdb"):
        """
        Initialize SQL lineage extractor.

        Args:
            dialect: SQL dialect for parsing (default: duckdb)
        """
        self.dialect = dialect

    def extract(
        self,
        model_name: str,
        model_info: Dict[str, Any],
        available_models: Dict[str, Dict[str, Any]],
    ) -> ColumnLineage:
        """
        Extract column lineage from a SQL model.

        Args:
            model_name: Name of the model
            model_info: Model metadata including the SQL query
            available_models: All available models for resolving dependencies

        Returns:
            ColumnLineage for this model
        """
        sql = model_info.get("query", "")
        if not sql:
            return ColumnLineage(model_name=model_name, computed_at=datetime.utcnow())

        dependencies = model_info.get("dependencies", [])

        # Build schema definitions for dependencies
        schema_dict = self._build_schema_dict(dependencies, available_models)

        columns = []

        try:
            # Parse the SQL
            parsed = sqlglot.parse_one(sql, read=self.dialect)

            if parsed is None:
                return ColumnLineage(model_name=model_name, computed_at=datetime.utcnow())

            # Get output columns
            output_columns = self._get_output_columns(parsed)

            for col_name, col_expr in output_columns:
                sources = self._trace_column(
                    col_expr, col_name, model_name, parsed, schema_dict, dependencies
                )

                col_info = ColumnInfo(
                    name=col_name,
                    sources=sources,
                )
                columns.append(col_info)

        except Exception as e:
            logger.debug(f"Could not parse SQL for {model_name}: {e}")
            # Fallback to basic extraction
            columns = self._basic_lineage_extraction(
                sql, model_name, dependencies, available_models
            )

        return ColumnLineage(
            model_name=model_name,
            columns=columns,
            computed_at=datetime.utcnow(),
        )

    def _build_schema_dict(
        self,
        dependencies: List[str],
        available_models: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, str]]:
        """
        Build schema dictionary for SQLGlot lineage analysis.

        Format: {table_name: {column_name: column_type}}
        """
        schema_dict = {}

        for dep_name in dependencies:
            if dep_name not in available_models:
                continue

            model_info = available_models[dep_name]
            fields = model_info.get("fields")

            if fields is None:
                continue

            col_types = {}
            if isinstance(fields, dict):
                for col_name, col_type in fields.items():
                    col_types[col_name] = self._normalize_type(col_type)
            elif hasattr(fields, "items"):
                for col_name, col_type in fields.items():
                    col_types[col_name] = self._normalize_type(col_type)

            if col_types:
                schema_dict[dep_name] = col_types

        return schema_dict

    def _normalize_type(self, col_type: Any) -> str:
        """Normalize a column type to a string."""
        if isinstance(col_type, type):
            if col_type == int:
                return "BIGINT"
            elif col_type == float:
                return "DOUBLE"
            elif col_type == str:
                return "VARCHAR"
            elif col_type == bool:
                return "BOOLEAN"
            else:
                return "VARCHAR"
        return str(col_type).upper()

    def _get_output_columns(
        self, parsed: exp.Expression
    ) -> List[Tuple[str, exp.Expression]]:
        """
        Get output columns from a SELECT statement.

        Returns list of (column_name, column_expression) tuples.
        """
        columns = []

        if not isinstance(parsed, exp.Select):
            # Try to find SELECT within CTE or subquery
            select = parsed.find(exp.Select)
            if select is None:
                return columns
            parsed = select

        # Get the SELECT expressions
        for expr in parsed.expressions:
            col_name = self._get_column_alias(expr)
            if col_name:
                columns.append((col_name, expr))

        return columns

    def _get_column_alias(self, expr: exp.Expression) -> Optional[str]:
        """Get the output name of a column expression."""
        # Check for explicit alias
        if isinstance(expr, exp.Alias):
            return expr.alias

        # Check for column reference
        if isinstance(expr, exp.Column):
            return expr.name

        # Check for star
        if isinstance(expr, exp.Star):
            return "*"

        # Try to get name from output_name
        if hasattr(expr, "output_name"):
            return expr.output_name

        return None

    def _trace_column(
        self,
        col_expr: exp.Expression,
        col_name: str,
        model_name: str,
        parsed: exp.Expression,
        schema_dict: Dict[str, Dict[str, str]],
        dependencies: List[str],
    ) -> List[ColumnLineageEdge]:
        """
        Trace a column expression to find source columns.

        Uses SQLGlot's built-in lineage when available, with
        fallback to manual AST traversal.
        """
        sources = []

        try:
            # Try using SQLGlot's lineage function
            lineage_result = sqlglot_lineage(
                col_name,
                parsed,
                dialect=self.dialect,
                schema=schema_dict if schema_dict else None,
            )

            if lineage_result:
                sources.extend(
                    self._process_lineage_result(lineage_result, col_name, model_name)
                )
        except Exception as e:
            logger.debug(f"SQLGlot lineage failed for {col_name}: {e}")

        # Also do manual analysis for completeness
        manual_sources = self._manual_trace_column(
            col_expr, col_name, model_name, dependencies
        )

        # Merge sources, avoiding duplicates
        seen = {(s.source_model, s.source_column) for s in sources}
        for src in manual_sources:
            key = (src.source_model, src.source_column)
            if key not in seen:
                sources.append(src)
                seen.add(key)

        return sources

    def _process_lineage_result(
        self, lineage_result: Any, col_name: str, model_name: str
    ) -> List[ColumnLineageEdge]:
        """Process SQLGlot lineage result into ColumnLineageEdge objects."""
        sources = []

        # SQLGlot lineage returns a Node with downstream references
        def process_node(node, depth=0):
            nonlocal sources
            if depth > 10:  # Prevent infinite recursion
                return

            if hasattr(node, "downstream"):
                for downstream in node.downstream:
                    process_node(downstream, depth + 1)

            if hasattr(node, "source") and hasattr(node, "expression"):
                source = node.source
                if isinstance(source, exp.Table):
                    table_name = source.name
                    # Get column name from the expression
                    if isinstance(node.expression, exp.Column):
                        src_col_name = node.expression.name
                    else:
                        src_col_name = str(node.expression)

                    sources.append(
                        ColumnLineageEdge(
                            source_model=table_name,
                            source_column=src_col_name,
                            output_model=model_name,
                            output_column=col_name,
                            transformation_type=TransformationType.DIRECT,
                        )
                    )

        if hasattr(lineage_result, "downstream"):
            for downstream in lineage_result.downstream:
                process_node(downstream)

        return sources

    def _manual_trace_column(
        self,
        col_expr: exp.Expression,
        col_name: str,
        model_name: str,
        dependencies: List[str],
    ) -> List[ColumnLineageEdge]:
        """
        Manually trace column references in an expression.

        Walks the AST to find all column references.
        """
        sources = []
        visited = set()

        def trace_expr(expr: exp.Expression, transformation: TransformationType):
            nonlocal sources

            if id(expr) in visited:
                return
            visited.add(id(expr))

            # Direct column reference
            if isinstance(expr, exp.Column):
                table_name = self._resolve_table_name(expr, dependencies)
                if table_name:
                    sources.append(
                        ColumnLineageEdge(
                            source_model=table_name,
                            source_column=expr.name,
                            output_model=model_name,
                            output_column=col_name,
                            transformation_type=transformation,
                        )
                    )
                return

            # Alias - trace the inner expression
            if isinstance(expr, exp.Alias):
                trace_expr(expr.this, transformation)
                return

            # Determine transformation type based on expression
            if isinstance(expr, (exp.Sum, exp.Avg, exp.Min, exp.Max, exp.Count)):
                transformation = TransformationType.AGGREGATED
            elif isinstance(expr, exp.Cast):
                transformation = TransformationType.CAST
            elif isinstance(expr, (exp.Add, exp.Sub, exp.Mul, exp.Div)):
                transformation = TransformationType.DERIVED
            elif isinstance(expr, exp.Window):
                transformation = TransformationType.WINDOW
            elif isinstance(expr, (exp.Concat, exp.Coalesce)):
                transformation = TransformationType.DERIVED

            # Recurse into child expressions
            for child in expr.iter_expressions():
                trace_expr(child, transformation)

        trace_expr(col_expr, TransformationType.DIRECT)
        return sources

    def _resolve_table_name(
        self, col: exp.Column, dependencies: List[str]
    ) -> Optional[str]:
        """
        Resolve which dependency a column reference belongs to.

        Uses table qualifier if present, otherwise infers from dependencies.
        """
        # Check for explicit table qualifier
        if col.table:
            table_name = col.table
            # Might be an alias, try to match to dependency
            for dep in dependencies:
                if dep == table_name or dep.endswith(f".{table_name}"):
                    return dep
            # Return as-is if in dependencies
            if table_name in dependencies:
                return table_name
            # Might be a qualified name
            return table_name

        # No qualifier - column might come from any dependency
        # Return first dependency that has this column (if known)
        # For now, return None to indicate unknown source
        return None

    def _basic_lineage_extraction(
        self,
        sql: str,
        model_name: str,
        dependencies: List[str],
        available_models: Dict[str, Dict[str, Any]],
    ) -> List[ColumnInfo]:
        """
        Basic lineage extraction when full parsing fails.

        Creates column info from explicit output columns with
        best-effort source tracking.
        """
        columns = []

        try:
            # Parse to get output columns
            parsed = sqlglot.parse_one(sql, read=self.dialect)
            if parsed is None:
                return columns

            output_cols = self._get_output_columns(parsed)

            # For each output column, find any referenced columns
            for col_name, col_expr in output_cols:
                sources = []

                # Find all column references in the expression
                for col_ref in col_expr.find_all(exp.Column):
                    table_name = self._resolve_table_name(col_ref, dependencies)
                    if table_name:
                        sources.append(
                            ColumnLineageEdge(
                                source_model=table_name,
                                source_column=col_ref.name,
                                output_model=model_name,
                                output_column=col_name,
                                transformation_type=TransformationType.UNKNOWN,
                            )
                        )

                columns.append(
                    ColumnInfo(
                        name=col_name,
                        sources=sources,
                    )
                )

        except Exception as e:
            logger.debug(f"Basic lineage extraction failed: {e}")

        return columns
