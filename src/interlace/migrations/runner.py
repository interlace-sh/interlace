"""
Migration runner.

Executes SQL migrations from migrations directory.
"""

import os
from pathlib import Path
from typing import Any

from interlace.core.context import _execute_sql_internal
from interlace.migrations.utils import (
    MigrationResult,
    get_executed_migrations,
    get_migration_files,
    record_migration_run,
)
from interlace.utils.logging import get_logger

logger = get_logger("interlace.migrations")


def list_pending_migrations(project_dir: Path, connection: Any, environment: str) -> list[str]:
    """
    List pending migrations for an environment.

    Args:
        project_dir: Project directory
        connection: Database connection
        environment: Environment name

    Returns:
        List of pending migration file names
    """
    migrations_dir = project_dir / "migrations"
    if not migrations_dir.exists():
        return []

    all_migrations = get_migration_files(migrations_dir)
    executed = get_executed_migrations(connection, environment)

    executed_set = set(executed)
    pending = [f.name for f in all_migrations if f.name not in executed_set]

    return sorted(pending)


def run_migrations(
    project_dir: Path,
    connection: Any,
    environment: str,
    migration_file: str | None = None,
    dry_run: bool = False,
) -> list[MigrationResult]:
    """
    Run migrations for an environment.

    Args:
        project_dir: Project directory
        connection: Database connection
        environment: Environment name
        migration_file: Optional specific migration file to run
        dry_run: If True, don't execute, just return what would be run

    Returns:
        List of migration results
    """
    migrations_dir = project_dir / "migrations"
    if not migrations_dir.exists():
        logger.info(f"Migrations directory not found: {migrations_dir}")
        return []

    if migration_file:
        # Run specific migration
        migration_path = migrations_dir / migration_file
        if not migration_path.exists():
            logger.error(f"Migration file not found: {migration_path}")
            return [
                MigrationResult(
                    migration_file=migration_file,
                    success=False,
                    error_message=f"File not found: {migration_path}",
                )
            ]

        return _run_single_migration(migration_path, connection, environment, dry_run)
    else:
        # Run all pending migrations
        pending = list_pending_migrations(project_dir, connection, environment)
        if not pending:
            logger.info("No pending migrations")
            return []

        results = []
        for migration_name in pending:
            migration_path = migrations_dir / migration_name
            result = _run_single_migration(migration_path, connection, environment, dry_run)
            results.extend(result)

        return results


def _run_single_migration(
    migration_path: Path,
    connection: Any,
    environment: str,
    dry_run: bool = False,
) -> list[MigrationResult]:
    """
    Run a single migration file.

    Args:
        migration_path: Path to migration file
        connection: Database connection
        environment: Environment name
        dry_run: If True, don't execute, just return what would be run

    Returns:
        List with single migration result
    """
    migration_file = migration_path.name
    executed_by = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"

    if dry_run:
        logger.info(f"[DRY RUN] Would execute: {migration_file}")
        # Read SQL content for preview
        try:
            sql_content = migration_path.read_text()
            # Truncate if too long for preview
            sql_preview = sql_content[:500] if len(sql_content) > 500 else sql_content
        except Exception:
            sql_preview = None
        return [
            MigrationResult(
                migration_file=migration_file,
                success=True,
                sql_preview=sql_preview,
            )
        ]

    try:
        # Read migration SQL
        sql_content = migration_path.read_text()

        # Execute migration
        logger.info(f"Executing migration: {migration_file}")
        _execute_sql_internal(connection, sql_content)

        # Record successful execution
        record_migration_run(connection, migration_file, environment, True, None, executed_by)

        logger.info(f"Migration completed: {migration_file}")
        return [
            MigrationResult(
                migration_file=migration_file,
                success=True,
            )
        ]

    except Exception as e:
        error_message = str(e)
        logger.error(f"Migration failed: {migration_file} - {error_message}")

        # Record failed execution
        record_migration_run(connection, migration_file, environment, False, error_message, executed_by)

        return [
            MigrationResult(
                migration_file=migration_file,
                success=False,
                error_message=error_message,
            )
        ]
