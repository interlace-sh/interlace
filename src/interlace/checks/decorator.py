"""
@check decorator for Python-defined data checks.

Allows defining checks as standalone functions that are discovered
during model discovery and attached to model check lists.
"""

import time
from collections.abc import Callable
from typing import Any

import ibis

from interlace.checks.base import (
    Check,
    CheckResult,
    CheckSeverity,
    CheckStatus,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.checks.decorator")

# Module-level registry of decorated check functions
_check_registry: list[dict[str, Any]] = []


def check(
    name: str,
    model: str | list[str],
    severity: str = "error",
    description: str | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator to define a custom check function.

    The decorated function receives ``(table: ibis.Table, connection: ibis.BaseBackend)``
    and should return one of:

    - ``ibis.Table`` of failing rows (0 rows = pass)
    - ``int`` count of failures (0 = pass)
    - ``bool`` (True = pass, False = fail)

    Args:
        name: Unique name for this check
        model: Model name(s) this check applies to
        severity: "error", "warn", or "info"
        description: Human-readable description

    Usage::

        from interlace.checks import check

        @check(name="no_future_dates", model="orders")
        def no_future_dates(table, connection):
            import ibis
            return table.filter(table["order_date"] > ibis.now())
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        metadata: dict[str, Any] = {
            "name": name,
            "model": [model] if isinstance(model, str) else list(model),
            "severity": severity,
            "description": description or func.__doc__,
            "function": func,
        }
        func._interlace_check = metadata  # type: ignore[attr-defined]
        _check_registry.append(metadata)
        return func

    return decorator


def get_registered_checks() -> list[dict[str, Any]]:
    """Return all registered @check functions."""
    return list(_check_registry)


def clear_check_registry() -> None:
    """Clear the check registry (used in testing)."""
    _check_registry.clear()


class PythonCheck(Check):
    """
    Wrapper that adapts a @check-decorated function into a Check instance.

    Created automatically during discovery — users don't instantiate this directly.
    """

    def __init__(
        self,
        func: Callable[..., Any],
        name: str,
        severity: CheckSeverity = CheckSeverity.ERROR,
        description: str | None = None,
    ):
        super().__init__(
            severity=severity,
            name=name,
            description=description,
        )
        self.func = func

    @property
    def check_type(self) -> str:
        return "python"

    def run(
        self,
        connection: ibis.BaseBackend,
        table_name: str,
        schema: str | None = None,
    ) -> CheckResult:
        """Execute the Python check function."""
        start_time = time.time()

        try:
            table = self._get_table(connection, table_name, schema)
            result = self.func(table, connection)

            duration = time.time() - start_time

            # Interpret the return value
            if isinstance(result, bool):
                if result:
                    return self._make_result(
                        status=CheckStatus.PASSED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' passed",
                        duration=duration,
                    )
                else:
                    return self._make_result(
                        status=CheckStatus.FAILED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' failed",
                        duration=duration,
                    )
            elif isinstance(result, int):
                if result == 0:
                    return self._make_result(
                        status=CheckStatus.PASSED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' passed (0 failures)",
                        failed_rows=0,
                        duration=duration,
                    )
                else:
                    return self._make_result(
                        status=CheckStatus.FAILED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' found {result} failing rows",
                        failed_rows=result,
                        duration=duration,
                    )
            elif isinstance(result, ibis.Table):
                failed_count = int(result.count().execute())
                if failed_count == 0:
                    return self._make_result(
                        status=CheckStatus.PASSED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' passed (0 failing rows)",
                        failed_rows=0,
                        duration=duration,
                    )
                else:
                    return self._make_result(
                        status=CheckStatus.FAILED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' found {failed_count} failing rows",
                        failed_rows=failed_count,
                        duration=duration,
                    )
            else:
                # Treat truthy as pass, falsy as fail
                if result:
                    return self._make_result(
                        status=CheckStatus.PASSED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' passed",
                        duration=duration,
                    )
                else:
                    return self._make_result(
                        status=CheckStatus.FAILED,
                        table_name=table_name,
                        message=f"Python check '{self.name}' failed",
                        duration=duration,
                    )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error running Python check '{self.name}': {e}")
            return self._make_result(
                status=CheckStatus.ERROR,
                table_name=table_name,
                message=f"Error running Python check: {str(e)}",
                duration=duration,
                details={"error": str(e)},
            )
