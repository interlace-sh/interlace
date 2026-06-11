"""
Relationships (foreign key) check.

Check referential integrity between tables.
"""

import time

import ibis

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.types.relationships")


class RelationshipsCheck(Check):
    """
    Check referential integrity between two tables.

    Validates that every value in a child column exists in a parent table's column
    (foreign key validation).

    Usage:
        RelationshipsCheck(
            column="customer_id",
            to_table="customers",
            to_column="id",
        )
    """

    def __init__(
        self,
        column: str,
        to_table: str,
        to_column: str | None = None,
        to_schema: str | None = None,
        severity: CheckSeverity = CheckSeverity.ERROR,
        name: str | None = None,
        description: str | None = None,
    ):
        super().__init__(
            column=column,
            severity=severity,
            name=name,
            description=description,
        )

        if not column:
            raise ValueError("RelationshipsCheck requires a column")
        if not to_table:
            raise ValueError("RelationshipsCheck requires a to_table")

        self.to_table = to_table
        self.to_column = to_column or column
        self.to_schema = to_schema

    @property
    def check_type(self) -> str:
        return "relationships"

    @property
    def check_name(self) -> str:
        if self.name:
            return self.name
        return f"relationships_{self.column}_to_{self.to_table}_{self.to_column}"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute referential integrity check."""
        start_time = time.time()

        try:
            child_table = self._get_table(connection, table_name, schema)
            # Use the same schema as the child table if to_schema not explicitly set
            parent_schema = self.to_schema or schema
            parent_table = self._get_table(connection, self.to_table, parent_schema)

            total_rows = int(child_table.count().execute())

            if total_rows == 0:
                duration = time.time() - start_time
                return self._make_result(
                    status=CheckStatus.SKIPPED,
                    table_name=table_name,
                    message="Table is empty, skipping relationships check",
                    total_rows=0,
                    duration=duration,
                )

            # Get non-null child values that don't exist in parent
            child_col = child_table[self.column]
            parent_values = parent_table.select(self.to_column).distinct()

            # Filter: non-null child values not in parent
            orphaned = child_table.filter(child_col.notnull() & ~child_col.isin(parent_values[self.to_column]))
            orphan_count = int(orphaned.count().execute())

            duration = time.time() - start_time

            if orphan_count == 0:
                return self._make_result(
                    status=CheckStatus.PASSED,
                    table_name=table_name,
                    message=(
                        f"All non-null values in '{self.column}' exist in "
                        f"'{self.to_table}.{self.to_column}' ({total_rows} rows checked)"
                    ),
                    failed_rows=0,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "to_table": self.to_table,
                        "to_column": self.to_column,
                    },
                )
            else:
                # Get sample of orphaned values
                orphaned_values: list[object] = []
                try:
                    sample = orphaned.select(self.column).distinct().limit(10).execute()
                    orphaned_values = sample[self.column].tolist()
                except Exception:
                    pass

                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=(
                        f"Found {orphan_count} rows in '{self.column}' with values not in "
                        f"'{self.to_table}.{self.to_column}'. Sample orphaned values: {orphaned_values[:5]}"
                    ),
                    failed_rows=orphan_count,
                    total_rows=total_rows,
                    duration=duration,
                    details={
                        "column": self.column,
                        "to_table": self.to_table,
                        "to_column": self.to_column,
                        "orphan_count": orphan_count,
                        "orphaned_values_sample": orphaned_values[:10],
                    },
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running relationships check ({self.column} -> {self.to_table}.{self.to_column}): {e}")
            return self._make_result(
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running relationships check ({self.column} -> {self.to_table}.{self.to_column}): {e}",
                duration=duration,
                details={"error": str(e), "to_table": self.to_table, "to_column": self.to_column},
            )
