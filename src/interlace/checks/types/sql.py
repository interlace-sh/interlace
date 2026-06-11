"""
SQL check.

Check using a raw SQL query that returns failing rows.
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

logger = get_logger("interlace.checks.types.sql")


class SqlCheck(Check):
    """
    Check using a raw SQL query.

    The query should return rows that fail the check.
    0 rows returned = pass; any rows = fail.

    Supports ``{table}`` and ``{schema}`` placeholders in the SQL string.

    Usage:
        SqlCheck(
            sql="SELECT * FROM orders WHERE total < 0",
            name="no_negative_totals",
        )
    """

    def __init__(
        self,
        sql: str,
        name: str,
        severity: CheckSeverity = CheckSeverity.ERROR,
        description: str | None = None,
    ):
        super().__init__(
            severity=severity,
            name=name,
            description=description,
        )

        if not sql:
            raise ValueError("SqlCheck requires a sql query")
        if not name:
            raise ValueError("SqlCheck requires a name")

        self.sql = sql

    @property
    def check_type(self) -> str:
        return "sql"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute SQL check."""
        start_time = time.time()

        try:
            # Substitute placeholders
            effective_schema = schema or "public"
            resolved_sql = self.sql.replace("{table}", table_name)
            resolved_sql = resolved_sql.replace("{schema}", effective_schema)

            # Set search path so unqualified table names resolve correctly
            try:
                connection.raw_sql(f"SET search_path = '{effective_schema}'")
            except Exception:
                pass  # Not all backends support SET search_path

            result_table = connection.sql(resolved_sql)
            failed_count = int(result_table.count().execute())

            duration = time.time() - start_time

            if failed_count == 0:
                return self._make_result(
                    status=CheckStatus.PASSED,
                    table_name=table_name,
                    message=f"SQL check '{self.name}' passed (0 failing rows)",
                    failed_rows=0,
                    duration=duration,
                    details={"sql": resolved_sql},
                    sql_query=resolved_sql,
                )
            else:
                return self._make_result(
                    status=CheckStatus.FAILED,
                    table_name=table_name,
                    message=f"SQL check '{self.name}' found {failed_count} failing rows",
                    failed_rows=failed_count,
                    duration=duration,
                    details={"sql": resolved_sql, "failed_count": failed_count},
                    sql_query=resolved_sql,
                )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running SQL check '{self.name}': {e}")
            return self._make_result(
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running SQL check: {str(e)}",
                duration=duration,
                details={"error": str(e), "sql": self.sql},
                sql_query=self.sql,
            )
