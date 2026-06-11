"""
Tests for the migrations module.

Tests migration file discovery, execution tracking, and the runner.
"""

from pathlib import Path
from unittest.mock import patch

import ibis
import pytest

from interlace.migrations.runner import list_pending_migrations, run_migrations
from interlace.migrations.utils import (
    MigrationResult,
    get_executed_migrations,
    get_migration_files,
    record_migration_run,
)


@pytest.fixture
def duckdb_conn():
    """DuckDB connection with migration_runs table."""
    conn = ibis.duckdb.connect()
    conn.raw_sql("CREATE SCHEMA IF NOT EXISTS interlace")
    conn.raw_sql("""
        CREATE TABLE interlace.migration_runs (
            migration_file VARCHAR NOT NULL,
            environment VARCHAR NOT NULL,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            executed_by VARCHAR,
            success BOOLEAN NOT NULL,
            error_message TEXT
        )
        """)
    yield conn
    conn.disconnect()


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    """Create a temp project dir with migration files."""
    mdir = tmp_path / "migrations"
    mdir.mkdir()
    (mdir / "001_create_users.sql").write_text("CREATE TABLE users (id INTEGER, name VARCHAR);")
    (mdir / "002_add_email.sql").write_text("ALTER TABLE users ADD COLUMN email VARCHAR;")
    (mdir / "003_add_index.sql").write_text("CREATE INDEX idx_users_name ON users(name);")
    return tmp_path


# -- Migration file discovery tests --


class TestGetMigrationFiles:
    def test_returns_sorted_sql_files(self, migrations_dir: Path) -> None:
        files = get_migration_files(migrations_dir / "migrations")
        names = [f.name for f in files]
        assert names == ["001_create_users.sql", "002_add_email.sql", "003_add_index.sql"]

    def test_empty_dir(self, tmp_path: Path) -> None:
        mdir = tmp_path / "migrations"
        mdir.mkdir()
        assert get_migration_files(mdir) == []

    def test_nonexistent_dir(self, tmp_path: Path) -> None:
        assert get_migration_files(tmp_path / "nonexistent") == []

    def test_ignores_non_sql_files(self, tmp_path: Path) -> None:
        mdir = tmp_path / "migrations"
        mdir.mkdir()
        (mdir / "001_create.sql").write_text("CREATE TABLE t (id INT);")
        (mdir / "README.md").write_text("# Migrations")
        (mdir / "script.py").write_text("print('hello')")

        files = get_migration_files(mdir)
        assert len(files) == 1
        assert files[0].name == "001_create.sql"


# -- Executed migrations tracking tests --


class TestGetExecutedMigrations:
    def test_empty_database(self, duckdb_conn: ibis.BaseBackend) -> None:
        result = get_executed_migrations(duckdb_conn, "dev")
        assert result == []

    def test_with_executed_migrations(self, duckdb_conn: ibis.BaseBackend) -> None:
        record_migration_run(duckdb_conn, "001_create.sql", "dev", True)
        record_migration_run(duckdb_conn, "002_add.sql", "dev", True)

        result = get_executed_migrations(duckdb_conn, "dev")
        assert "001_create.sql" in result
        assert "002_add.sql" in result

    def test_only_returns_successful(self, duckdb_conn: ibis.BaseBackend) -> None:
        record_migration_run(duckdb_conn, "001_create.sql", "dev", True)
        record_migration_run(duckdb_conn, "002_fail.sql", "dev", False, error_message="syntax error")

        result = get_executed_migrations(duckdb_conn, "dev")
        assert "001_create.sql" in result
        assert "002_fail.sql" not in result

    def test_environment_isolation(self, duckdb_conn: ibis.BaseBackend) -> None:
        record_migration_run(duckdb_conn, "001_create.sql", "dev", True)
        record_migration_run(duckdb_conn, "001_create.sql", "prod", True)
        record_migration_run(duckdb_conn, "002_add.sql", "prod", True)

        dev_result = get_executed_migrations(duckdb_conn, "dev")
        prod_result = get_executed_migrations(duckdb_conn, "prod")
        assert len(dev_result) == 1
        assert len(prod_result) == 2


class TestRecordMigrationRun:
    def test_success(self, duckdb_conn: ibis.BaseBackend) -> None:
        record_migration_run(duckdb_conn, "001_test.sql", "dev", True, executed_by="testuser")

        result = get_executed_migrations(duckdb_conn, "dev")
        assert "001_test.sql" in result

    def test_failure(self, duckdb_conn: ibis.BaseBackend) -> None:
        record_migration_run(duckdb_conn, "001_test.sql", "dev", False, error_message="broken SQL")

        # Should not appear in executed (only successful)
        result = get_executed_migrations(duckdb_conn, "dev")
        assert "001_test.sql" not in result

    def test_upsert_replaces_existing(self, duckdb_conn: ibis.BaseBackend) -> None:
        record_migration_run(duckdb_conn, "001_test.sql", "dev", False, error_message="first try")
        record_migration_run(duckdb_conn, "001_test.sql", "dev", True)

        result = get_executed_migrations(duckdb_conn, "dev")
        assert "001_test.sql" in result


# -- Migration result dataclass --


class TestMigrationResult:
    def test_success_result(self) -> None:
        r = MigrationResult(migration_file="001.sql", success=True)
        assert r.success
        assert r.error_message is None

    def test_failure_result(self) -> None:
        r = MigrationResult(migration_file="001.sql", success=False, error_message="err")
        assert not r.success
        assert r.error_message == "err"

    def test_dry_run_with_preview(self) -> None:
        r = MigrationResult(migration_file="001.sql", success=True, sql_preview="CREATE TABLE t (id INT);")
        assert r.sql_preview is not None


# -- Runner tests --


class TestListPendingMigrations:
    def test_all_pending(self, duckdb_conn: ibis.BaseBackend, migrations_dir: Path) -> None:
        pending = list_pending_migrations(migrations_dir, duckdb_conn, "dev")
        assert len(pending) == 3

    def test_some_executed(self, duckdb_conn: ibis.BaseBackend, migrations_dir: Path) -> None:
        record_migration_run(duckdb_conn, "001_create_users.sql", "dev", True)

        pending = list_pending_migrations(migrations_dir, duckdb_conn, "dev")
        assert "001_create_users.sql" not in pending
        assert "002_add_email.sql" in pending
        assert len(pending) == 2

    def test_no_migrations_dir(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        pending = list_pending_migrations(tmp_path, duckdb_conn, "dev")
        assert pending == []


class TestRunMigrations:
    def test_run_specific_file(self, duckdb_conn: ibis.BaseBackend, migrations_dir: Path) -> None:
        results = run_migrations(migrations_dir, duckdb_conn, "dev", migration_file="001_create_users.sql")
        assert len(results) == 1
        assert results[0].success

    def test_run_specific_file_not_found(self, duckdb_conn: ibis.BaseBackend, migrations_dir: Path) -> None:
        results = run_migrations(migrations_dir, duckdb_conn, "dev", migration_file="999_nonexistent.sql")
        assert len(results) == 1
        assert not results[0].success
        assert "not found" in results[0].error_message.lower()

    def test_run_all_pending(self, duckdb_conn: ibis.BaseBackend, migrations_dir: Path) -> None:
        results = run_migrations(migrations_dir, duckdb_conn, "dev")
        assert len(results) == 3
        assert all(r.success for r in results)

    def test_no_migrations_dir(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        results = run_migrations(tmp_path, duckdb_conn, "dev")
        assert results == []

    @patch.dict("os.environ", {"USER": "testbot"})
    def test_dry_run(self, duckdb_conn: ibis.BaseBackend, migrations_dir: Path) -> None:
        results = run_migrations(migrations_dir, duckdb_conn, "dev", dry_run=True)
        assert len(results) == 3
        assert all(r.success for r in results)
        assert all(r.sql_preview is not None for r in results)

        # Dry run should NOT record execution
        executed = get_executed_migrations(duckdb_conn, "dev")
        assert len(executed) == 0

    def test_failed_migration_recorded(self, duckdb_conn: ibis.BaseBackend, tmp_path: Path) -> None:
        mdir = tmp_path / "migrations"
        mdir.mkdir()
        (mdir / "001_bad.sql").write_text("THIS IS NOT VALID SQL !!!")

        results = run_migrations(tmp_path, duckdb_conn, "dev")
        assert len(results) == 1
        assert not results[0].success
        assert results[0].error_message is not None
