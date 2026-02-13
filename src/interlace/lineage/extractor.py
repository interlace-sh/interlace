"""
Base lineage extractor class and data structures.

Provides the foundation for column-level lineage extraction from both
Python (ibis) and SQL models.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class TransformationType(StrEnum):
    """Type of transformation applied to a column."""

    DIRECT = "direct"  # Column passed through unchanged
    RENAMED = "renamed"  # Column renamed but not transformed
    DERIVED = "derived"  # Column computed from other columns
    AGGREGATED = "aggregated"  # Column result of aggregation
    JOINED = "joined"  # Column from a join operation
    FILTERED = "filtered"  # Column filtered/masked
    CAST = "cast"  # Column type cast
    WINDOW = "window"  # Window function result
    UNKNOWN = "unknown"  # Transformation type couldn't be determined


@dataclass
class ColumnLineageEdge:
    """
    Represents a lineage relationship between columns.

    An edge represents the flow of data from a source column in one model
    to an output column in another model (or the same model).
    """

    source_model: str
    source_column: str
    output_model: str
    output_column: str
    transformation_type: TransformationType = TransformationType.UNKNOWN
    transformation_expression: str | None = None  # SQL or ibis expression if available
    confidence: float = 1.0  # Confidence score (0-1) for inferred lineage

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "source_model": self.source_model,
            "source_column": self.source_column,
            "output_model": self.output_model,
            "output_column": self.output_column,
            "transformation_type": self.transformation_type.value,
            "transformation_expression": self.transformation_expression,
            "confidence": self.confidence,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ColumnLineageEdge":
        """Create from dictionary."""
        return cls(
            source_model=data["source_model"],
            source_column=data["source_column"],
            output_model=data["output_model"],
            output_column=data["output_column"],
            transformation_type=TransformationType(data.get("transformation_type", "unknown")),
            transformation_expression=data.get("transformation_expression"),
            confidence=data.get("confidence", 1.0),
        )


@dataclass
class ColumnInfo:
    """Information about a column in a model."""

    name: str
    data_type: str | None = None
    nullable: bool = True
    description: str | None = None
    is_primary_key: bool = False
    sources: list[ColumnLineageEdge] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "name": self.name,
            "data_type": self.data_type,
            "nullable": self.nullable,
            "description": self.description,
            "is_primary_key": self.is_primary_key,
            "sources": [s.to_dict() for s in self.sources],
        }


@dataclass
class ColumnLineage:
    """
    Complete column lineage for a model.

    Contains all columns and their lineage edges.
    """

    model_name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    computed_at: datetime | None = None

    def get_column(self, name: str) -> ColumnInfo | None:
        """Get column by name."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_all_edges(self) -> list[ColumnLineageEdge]:
        """Get all lineage edges across all columns."""
        edges = []
        for col in self.columns:
            edges.extend(col.sources)
        return edges

    def get_upstream_columns(self, column_name: str, depth: int = -1) -> list[ColumnLineageEdge]:
        """
        Get all upstream column sources for a specific column.

        Args:
            column_name: Name of the column to trace
            depth: Max depth to traverse (-1 for unlimited)

        Returns:
            List of lineage edges representing upstream columns
        """
        col = self.get_column(column_name)
        if col is None:
            return []
        return col.sources

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "model_name": self.model_name,
            "columns": [c.to_dict() for c in self.columns],
            "computed_at": self.computed_at.isoformat() if self.computed_at else None,
        }


@dataclass
class LineageGraph:
    """
    Complete lineage graph across all models.

    Stores column-level lineage for the entire pipeline.
    """

    models: dict[str, ColumnLineage] = field(default_factory=dict)
    edges: list[ColumnLineageEdge] = field(default_factory=list)
    computed_at: datetime | None = None

    def add_model_lineage(self, lineage: ColumnLineage) -> None:
        """Add or update lineage for a model."""
        self.models[lineage.model_name] = lineage
        # Update global edges
        self.edges = [e for e in self.edges if e.output_model != lineage.model_name]
        self.edges.extend(lineage.get_all_edges())

    def get_column_lineage(self, model_name: str, column_name: str) -> list[ColumnLineageEdge]:
        """Get lineage edges for a specific column."""
        if model_name not in self.models:
            return []
        return self.models[model_name].get_upstream_columns(column_name)

    def trace_column_upstream(
        self,
        model_name: str,
        column_name: str,
        depth: int = 3,
        visited: set[tuple] | None = None,
    ) -> list[ColumnLineageEdge]:
        """
        Trace a column upstream through multiple models.

        Args:
            model_name: Starting model
            column_name: Starting column
            depth: Maximum depth to traverse
            visited: Set of (model, column) tuples already visited

        Returns:
            All lineage edges in the upstream path
        """
        if visited is None:
            visited = set()

        if depth <= 0:
            return []

        key = (model_name, column_name)
        if key in visited:
            return []
        visited.add(key)

        result = []
        edges = self.get_column_lineage(model_name, column_name)
        result.extend(edges)

        # Recursively trace upstream
        for edge in edges:
            upstream = self.trace_column_upstream(
                edge.source_model,
                edge.source_column,
                depth - 1,
                visited,
            )
            result.extend(upstream)

        return result

    def trace_column_downstream(
        self,
        model_name: str,
        column_name: str,
        depth: int = 3,
        visited: set[tuple] | None = None,
    ) -> list[ColumnLineageEdge]:
        """
        Trace a column downstream to find all dependent columns.

        Args:
            model_name: Starting model
            column_name: Starting column
            depth: Maximum depth to traverse
            visited: Set of (model, column) tuples already visited

        Returns:
            All lineage edges in the downstream path
        """
        if visited is None:
            visited = set()

        if depth <= 0:
            return []

        key = (model_name, column_name)
        if key in visited:
            return []
        visited.add(key)

        result = []

        # Find edges where this column is the source
        for edge in self.edges:
            if edge.source_model == model_name and edge.source_column == column_name:
                result.append(edge)
                # Recursively trace downstream
                downstream = self.trace_column_downstream(
                    edge.output_model,
                    edge.output_column,
                    depth - 1,
                    visited,
                )
                result.extend(downstream)

        return result

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "models": {name: m.to_dict() for name, m in self.models.items()},
            "edges": [e.to_dict() for e in self.edges],
            "computed_at": self.computed_at.isoformat() if self.computed_at else None,
        }


class LineageExtractor(ABC):
    """
    Abstract base class for lineage extractors.

    Subclasses implement extraction logic for specific model types
    (Python/ibis models, SQL models, etc.).
    """

    @abstractmethod
    def extract(
        self,
        model_name: str,
        model_info: dict[str, Any],
        available_models: dict[str, dict[str, Any]],
    ) -> ColumnLineage:
        """
        Extract column lineage from a model.

        Args:
            model_name: Name of the model to analyze
            model_info: Model metadata (from discovery)
            available_models: All available models (for resolving dependencies)

        Returns:
            ColumnLineage containing all columns and their sources
        """
        pass

    def _get_dependency_columns(
        self,
        dep_name: str,
        available_models: dict[str, dict[str, Any]],
    ) -> list[str]:
        """
        Get column names from a dependency model.

        Args:
            dep_name: Name of the dependency
            available_models: All available models

        Returns:
            List of column names (empty if unknown)
        """
        if dep_name not in available_models:
            return []

        dep_info = available_models[dep_name]
        fields = dep_info.get("fields")

        if fields is None:
            return []

        if isinstance(fields, dict):
            return list(fields.keys())
        elif hasattr(fields, "names"):
            # ibis.Schema
            return list(fields.names)

        return []
