"""
Impact analysis for pre-execution change detection.

Provides tools to analyze what will happen before executing models,
including detecting breaking changes and affected downstream models.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.impact")


class RunReason(StrEnum):
    """Reason why a model will be executed."""

    FILE_CHANGED = "file_changed"  # Model file has been modified
    UPSTREAM_CHANGED = "upstream_changed"  # Upstream dependency changed
    FIRST_RUN = "first_run"  # Model hasn't been run before
    FORCE = "force"  # Explicitly requested
    CONFIG_CHANGED = "config_changed"  # Configuration changed
    SCHEMA_CHANGED = "schema_changed"  # Output schema changed


class BreakingChangeType(StrEnum):
    """Type of breaking change detected."""

    COLUMN_REMOVED = "column_removed"  # Column removed that's used downstream
    COLUMN_TYPE_CHANGED = "column_type_changed"  # Column type changed incompatibly
    MODEL_REMOVED = "model_removed"  # Model removed that's used downstream
    PRIMARY_KEY_CHANGED = "primary_key_changed"  # Primary key definition changed


@dataclass
class SchemaChange:
    """Represents a single schema change."""

    column_name: str
    change_type: str  # "added", "removed", "type_changed"
    old_type: str | None = None
    new_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "column_name": self.column_name,
            "change_type": self.change_type,
            "old_type": self.old_type,
            "new_type": self.new_type,
        }


@dataclass
class BreakingChange:
    """Represents a breaking change that may cause downstream failures."""

    model_name: str
    change_type: BreakingChangeType
    description: str
    affected_downstream: list[str] = field(default_factory=list)
    column_name: str | None = None
    severity: str = "warning"  # "warning" or "error"

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "change_type": self.change_type.value,
            "description": self.description,
            "affected_downstream": self.affected_downstream,
            "column_name": self.column_name,
            "severity": self.severity,
        }


@dataclass
class ModelImpact:
    """Impact analysis for a single model."""

    model_name: str
    will_run: bool
    run_reason: RunReason | None = None
    downstream_affected: list[str] = field(default_factory=list)
    schema_changes: list[SchemaChange] = field(default_factory=list)
    breaking_changes: list[BreakingChange] = field(default_factory=list)
    file_changed: bool = False
    upstream_changed: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "will_run": self.will_run,
            "run_reason": self.run_reason.value if self.run_reason else None,
            "downstream_affected": self.downstream_affected,
            "schema_changes": [sc.to_dict() for sc in self.schema_changes],
            "breaking_changes": [bc.to_dict() for bc in self.breaking_changes],
            "file_changed": self.file_changed,
            "upstream_changed": self.upstream_changed,
        }


@dataclass
class ImpactAnalysisResult:
    """Complete impact analysis result."""

    models_to_run: list[ModelImpact] = field(default_factory=list)
    models_skipped: list[str] = field(default_factory=list)
    breaking_changes: list[BreakingChange] = field(default_factory=list)
    total_affected: int = 0
    has_breaking_changes: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "models_to_run": [m.to_dict() for m in self.models_to_run],
            "models_skipped": self.models_skipped,
            "breaking_changes": [bc.to_dict() for bc in self.breaking_changes],
            "total_affected": self.total_affected,
            "has_breaking_changes": self.has_breaking_changes,
            "summary": {
                "will_run": len(self.models_to_run),
                "skipped": len(self.models_skipped),
                "warnings": len([bc for bc in self.breaking_changes if bc.severity == "warning"]),
                "errors": len([bc for bc in self.breaking_changes if bc.severity == "error"]),
            },
        }


class ImpactAnalyzer:
    """
    Analyzes the impact of running models before execution.

    Provides preview of what will change, which models will run,
    and detects potential breaking changes.
    """

    def __init__(
        self,
        models: dict[str, dict[str, Any]],
        graph: Any,  # DependencyGraph
        change_detector: Any | None = None,
        state_store: Any | None = None,
    ):
        """
        Initialize impact analyzer.

        Args:
            models: Dictionary of model name to model info
            graph: DependencyGraph for dependency traversal
            change_detector: Optional ChangeDetector for file change detection
            state_store: Optional StateStore for schema history
        """
        self.models = models
        self.graph = graph
        self.change_detector = change_detector
        self.state_store = state_store
        self._column_usage: dict[str, dict[str, set[str]]] = {}

    def analyze(
        self,
        target_models: list[str] | None = None,
        force: bool = False,
    ) -> ImpactAnalysisResult:
        """
        Analyze impact of running specified models.

        Args:
            target_models: Models to analyze (None for all)
            force: If True, treat all models as needing to run

        Returns:
            ImpactAnalysisResult with details about what will happen
        """
        if target_models is None:
            target_models = list(self.models.keys())

        result = ImpactAnalysisResult()
        models_to_run: set[str] = set()
        model_reasons: dict[str, RunReason] = {}
        file_changes: dict[str, bool] = {}

        # First pass: determine which models need to run
        for model_name in target_models:
            if model_name not in self.models:
                continue

            reason = self._get_run_reason(model_name, force)
            if reason:
                models_to_run.add(model_name)
                model_reasons[model_name] = reason
                file_changes[model_name] = reason == RunReason.FILE_CHANGED
            else:
                result.models_skipped.append(model_name)

        # Second pass: propagate to downstream
        all_affected: set[str] = set(models_to_run)
        downstream_map: dict[str, list[str]] = {}

        for model_name in list(models_to_run):
            downstream = self._get_downstream_affected(model_name, all_affected)
            downstream_map[model_name] = downstream
            for ds_model in downstream:
                all_affected.add(ds_model)
                if ds_model not in models_to_run:
                    models_to_run.add(ds_model)
                    model_reasons[ds_model] = RunReason.UPSTREAM_CHANGED

        # Build column usage map for breaking change detection
        self._build_column_usage()

        # Third pass: build detailed impact info
        for model_name in models_to_run:
            impact = ModelImpact(
                model_name=model_name,
                will_run=True,
                run_reason=model_reasons.get(model_name),
                file_changed=file_changes.get(model_name, False),
            )

            # Add downstream affected
            if model_name in downstream_map:
                impact.downstream_affected = downstream_map[model_name]

            # Detect upstream that changed
            if model_reasons.get(model_name) == RunReason.UPSTREAM_CHANGED:
                impact.upstream_changed = self._get_changed_upstream(model_name, models_to_run)

            # Detect schema changes
            schema_changes = self._detect_schema_changes(model_name)
            impact.schema_changes = schema_changes

            # Detect breaking changes
            breaking = self._detect_breaking_changes(model_name, schema_changes, downstream_map.get(model_name, []))
            impact.breaking_changes = breaking
            result.breaking_changes.extend(breaking)

            result.models_to_run.append(impact)

        # Sort models by execution order (topological)
        if self.graph:
            try:
                order = self.graph.get_topological_order()
                order_map = {name: i for i, name in enumerate(order)}
                result.models_to_run.sort(key=lambda m: order_map.get(m.model_name, float("inf")))
            except Exception:
                pass

        result.total_affected = len(all_affected)
        result.has_breaking_changes = len(result.breaking_changes) > 0

        return result

    def _get_run_reason(self, model_name: str, force: bool) -> RunReason | None:
        """Determine why a model should run."""
        if force:
            return RunReason.FORCE

        # Check if model has ever been run
        if self.state_store:
            try:
                # Check model_metadata for last_run_at
                # This is a simplified check - full implementation would query the database
                pass
            except Exception:
                pass

        # Check if file changed
        if self.change_detector:
            try:
                should_run, reason = self.change_detector.should_run_model(model_name)
                if should_run:
                    if "file" in reason.lower() or "changed" in reason.lower():
                        return RunReason.FILE_CHANGED
                    elif "first" in reason.lower() or "never" in reason.lower():
                        return RunReason.FIRST_RUN
                    else:
                        return RunReason.FILE_CHANGED
            except Exception as e:
                logger.debug(f"Change detection failed for {model_name}: {e}")
                return RunReason.FILE_CHANGED

        # Default: assume needs to run if no change detector
        return RunReason.FILE_CHANGED

    def _get_downstream_affected(self, model_name: str, already_running: set[str]) -> list[str]:
        """Get list of downstream models that will be affected."""
        if not self.graph:
            return []

        downstream = []
        visited = set(already_running)

        def traverse(name: str):
            if name in visited:
                return
            visited.add(name)

            dependents = self.graph.get_dependents(name)
            for dep in dependents:
                if dep not in already_running:
                    downstream.append(dep)
                traverse(dep)

        traverse(model_name)
        return downstream

    def _get_changed_upstream(self, model_name: str, running_models: set[str]) -> list[str]:
        """Get list of upstream models that changed."""
        if not self.graph:
            return []

        dependencies = self.graph.get_dependencies(model_name)
        return [dep for dep in dependencies if dep in running_models]

    def _build_column_usage(self):
        """Build map of which columns each model uses from its dependencies."""
        self._column_usage = {}

        for model_name, model_info in self.models.items():
            model_type = model_info.get("type", "python")
            dependencies = model_info.get("dependencies", [])

            if not dependencies:
                continue

            usage: dict[str, set[str]] = {}

            if model_type == "sql":
                # Parse SQL to find column references
                query = model_info.get("query", "")
                if query:
                    usage = self._extract_sql_column_usage(query, dependencies)
            else:
                # For Python models, we can't easily determine column usage
                # without executing the code, so we assume all columns are used
                for dep in dependencies:
                    if dep in self.models:
                        dep_fields = self.models[dep].get("fields", {})
                        if isinstance(dep_fields, dict):
                            usage[dep] = set(dep_fields.keys())

            self._column_usage[model_name] = usage

    def _extract_sql_column_usage(self, sql: str, dependencies: list[str]) -> dict[str, set[str]]:
        """Extract column references from SQL query."""
        usage: dict[str, set[str]] = {}

        try:
            import sqlglot
            from sqlglot import exp

            parsed = sqlglot.parse_one(sql, read="duckdb")
            if parsed is None:
                return usage

            # Find all column references
            for col in parsed.find_all(exp.Column):
                col_name = col.name
                table_name = col.table if col.table else None

                # Try to match to a dependency
                if table_name and table_name in dependencies:
                    if table_name not in usage:
                        usage[table_name] = set()
                    usage[table_name].add(col_name)
                elif not table_name:
                    # Column without table qualifier - add to all dependencies
                    for dep in dependencies:
                        if dep not in usage:
                            usage[dep] = set()
                        usage[dep].add(col_name)

        except Exception as e:
            logger.debug(f"Failed to parse SQL for column usage: {e}")

        return usage

    def _detect_schema_changes(self, model_name: str) -> list[SchemaChange]:
        """Detect schema changes for a model compared to last execution."""
        changes = []

        if not self.state_store:
            return changes

        # Get current schema from model definition
        model_info = self.models.get(model_name, {})
        current_fields = model_info.get("fields", {})
        if not isinstance(current_fields, dict):
            return changes

        # Get previous schema from state store
        try:
            previous_columns = self.state_store.get_model_columns(model_name)
            previous_fields = {col["column_name"]: col.get("data_type") for col in previous_columns}

            # Compare schemas
            current_cols = set(current_fields.keys())
            previous_cols = set(previous_fields.keys())

            # Added columns
            for col in current_cols - previous_cols:
                changes.append(
                    SchemaChange(
                        column_name=col,
                        change_type="added",
                        new_type=str(current_fields.get(col)),
                    )
                )

            # Removed columns
            for col in previous_cols - current_cols:
                changes.append(
                    SchemaChange(
                        column_name=col,
                        change_type="removed",
                        old_type=previous_fields.get(col),
                    )
                )

            # Type changes
            for col in current_cols & previous_cols:
                current_type = str(current_fields.get(col))
                previous_type = previous_fields.get(col)
                if current_type != previous_type:
                    changes.append(
                        SchemaChange(
                            column_name=col,
                            change_type="type_changed",
                            old_type=previous_type,
                            new_type=current_type,
                        )
                    )

        except Exception as e:
            logger.debug(f"Failed to detect schema changes for {model_name}: {e}")

        return changes

    def _detect_breaking_changes(
        self,
        model_name: str,
        schema_changes: list[SchemaChange],
        downstream: list[str],
    ) -> list[BreakingChange]:
        """Detect breaking changes based on schema changes and downstream usage."""
        breaking = []

        # Check for removed columns
        for change in schema_changes:
            if change.change_type == "removed":
                # Check if any downstream model uses this column
                affected_downstream = []

                for ds_model in downstream:
                    usage = self._column_usage.get(ds_model, {})
                    model_usage = usage.get(model_name, set())
                    if change.column_name in model_usage:
                        affected_downstream.append(ds_model)

                if affected_downstream:
                    breaking.append(
                        BreakingChange(
                            model_name=model_name,
                            change_type=BreakingChangeType.COLUMN_REMOVED,
                            description=f"Column '{change.column_name}' removed but used by downstream models",
                            affected_downstream=affected_downstream,
                            column_name=change.column_name,
                            severity="warning",
                        )
                    )

            elif change.change_type == "type_changed":
                # Type changes might be breaking
                # For now, flag all type changes as warnings
                breaking.append(
                    BreakingChange(
                        model_name=model_name,
                        change_type=BreakingChangeType.COLUMN_TYPE_CHANGED,
                        description=f"Column '{change.column_name}' type changed from {change.old_type} to {change.new_type}",
                        affected_downstream=downstream,
                        column_name=change.column_name,
                        severity="warning",
                    )
                )

        return breaking
