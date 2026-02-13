"""
Migration utilities.

Helper functions for migration system.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from interlace.utils.logging import get_logger

logger = get_logger("interlace.migrations")


@dataclass
class MigrationResult:
    """Result of migration execution."""

    migration_file: str
    success: bool
    error_message: str | None = None
    sql_preview: str | None = None  # SQL content for dry-run preview


def get_migration_files(migrations_dir: Path) -> list[Path]:
    """
    Get all SQL migration files from migrations directory.

    Args:
        migrations_dir: Path to migrations directory

    Returns:
        List of migration file paths, sorted by filename
    """
    if not migrations_dir.exists():
        return []

    migration_files = [f for f in migrations_dir.glob("*.sql") if f.is_file()]
    return sorted(migration_files)


def get_executed_migrations(connection: Any, environment: str) -> list[str]:
    """
    Get list of executed migration files for an environment.

    Args:
        connection: Database connection
        environment: Environment name

    Returns:
        List of executed migration file names
    """
    try:
        from interlace.core.state import _escape_sql_string

        safe_environment = _escape_sql_string(environment)
        query = f"""
            SELECT migration_file
            FROM interlace.migration_runs
            WHERE environment = '{safe_environment}' AND success = TRUE
        """
        result = connection.sql(query).execute()
        if result is not None and len(result) > 0:
            if hasattr(result, "iloc"):
                return [row["migration_file"] for _, row in result.iterrows()]
            else:
                return [row["migration_file"] for row in result]
        return []
    except Exception as e:
        logger.debug(f"Could not get executed migrations: {e}")
        return []


def get_all_migrations_with_status(project_dir: Path, connection: Any, environment: str) -> list[dict]:
    """
    Get all migrations with their execution status.

    Args:
        project_dir: Project directory
        connection: Database connection
        environment: Environment name

    Returns:
        List of dicts with keys: migration_file, status, executed_at, executed_by, error_message
        Status can be: 'pending', 'executed', 'failed'
    """
    migrations_dir = project_dir / "migrations"
    if not migrations_dir.exists():
        return []

    # Get all migration files
    all_migrations = get_migration_files(migrations_dir)
    all_migration_names = {f.name for f in all_migrations}

    # Get execution status from database
    migration_status = {}
    try:
        from interlace.core.state import _escape_sql_string

        safe_environment = _escape_sql_string(environment)
        query = f"""
            SELECT migration_file, executed_at, executed_by, success, error_message
            FROM interlace.migration_runs
            WHERE environment = '{safe_environment}'
            ORDER BY executed_at DESC
        """
        result = connection.sql(query).execute()
        if result is not None and len(result) > 0:
            if hasattr(result, "iloc"):
                for _, row in result.iterrows():
                    migration_file = row["migration_file"]
                    if migration_file in all_migration_names:
                        migration_status[migration_file] = {
                            "status": "executed" if row.get("success", False) else "failed",
                            "executed_at": row.get("executed_at"),
                            "executed_by": row.get("executed_by"),
                            "error_message": row.get("error_message"),
                        }
            else:
                for row in result:
                    migration_file = row["migration_file"]
                    if migration_file in all_migration_names:
                        migration_status[migration_file] = {
                            "status": "executed" if row.get("success", False) else "failed",
                            "executed_at": row.get("executed_at"),
                            "executed_by": row.get("executed_by"),
                            "error_message": row.get("error_message"),
                        }
    except Exception as e:
        logger.debug(f"Could not get migration status: {e}")

    # Build result list with all migrations
    results = []
    for migration_file in sorted(all_migration_names):
        if migration_file in migration_status:
            results.append({"migration_file": migration_file, **migration_status[migration_file]})
        else:
            results.append(
                {
                    "migration_file": migration_file,
                    "status": "pending",
                    "executed_at": None,
                    "executed_by": None,
                    "error_message": None,
                }
            )

    return results


def record_migration_run(
    connection: Any,
    migration_file: str,
    environment: str,
    success: bool,
    error_message: str | None = None,
    executed_by: str | None = None,
) -> None:
    """
    Record migration execution in database.

    Args:
        connection: Database connection
        migration_file: Migration file name
        environment: Environment name
        success: Whether migration succeeded
        error_message: Optional error message
        executed_by: Optional user/process name
    """
    from interlace.core.context import _execute_sql_internal

    try:
        from interlace.core.state import _escape_sql_string

        safe_migration_file = _escape_sql_string(migration_file)
        safe_environment = _escape_sql_string(environment)
        safe_error_message = _escape_sql_string(error_message) if error_message else None
        safe_executed_by = _escape_sql_string(executed_by) if executed_by else None

        # Delete existing entry if it exists (upsert)
        delete_query = f"""
            DELETE FROM interlace.migration_runs
            WHERE migration_file = '{safe_migration_file}' AND environment = '{safe_environment}'
        """
        _execute_sql_internal(connection, delete_query)

        # Insert new entry
        error_msg_sql = f"'{safe_error_message}'" if safe_error_message else "NULL"
        executed_by_sql = f"'{safe_executed_by}'" if safe_executed_by else "NULL"
        success_sql = "1" if success else "0"

        insert_query = f"""
            INSERT INTO interlace.migration_runs
            (migration_file, environment, executed_at, executed_by, success, error_message)
            VALUES ('{safe_migration_file}', '{safe_environment}', CURRENT_TIMESTAMP, {executed_by_sql}, {success_sql}, {error_msg_sql})
        """
        _execute_sql_internal(connection, insert_query)
    except Exception as e:
        logger.warning(f"Could not record migration run: {e}")
