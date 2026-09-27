"""full_merge deletes on DuckLake must not abort the process.

Inlining ``source EXCEPT target`` in the changed-key DELETE makes DuckLake's
delete finalizer throw ``Could not find matching file for written delete file``
and invalidate the catalog. The next statement, including ROLLBACK, is then a
fatal exception off the Python thread (exit 134). Staging the key sets first
keeps each DELETE to a temp-table read. A local file catalog does not always
reproduce the crash; the worker-thread rollback is still the path that aborts
when it does.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import duckdb
import pytest

from interlace.engines.base import EngineCaps
from interlace.engines.duckdb import DuckDBAdapter
from interlace.ir.relation import SqlRelation, TableRef
from interlace.strategies.base import RowCounts
from interlace.strategies.full_merge import FullMerge

pytestmark = pytest.mark.integration

_MODEL = "SELECT o.id, d.name FROM other AS o JOIN dim AS d USING (id)"


def _rollback(connection: duckdb.DuckDBPyConnection, sqls: list[str]) -> tuple[int, int, int]:
    """Run the batch on one cursor and roll it back. Returns delete counts and the in-transaction row count."""
    cur = connection.cursor()
    changed = vanished = 0
    try:
        cur.execute("BEGIN")
        try:
            for sql in sqls:
                cur.execute(sql)
                if sql.startswith("DELETE") and "_interlace_fm_changed" in sql:
                    changed = int(cur.fetchall()[0][0])
                elif sql.startswith("DELETE") and "_interlace_fm_vanished" in sql:
                    vanished = int(cur.fetchall()[0][0])
            mid = int(cur.execute("SELECT count(*) FROM main.target").fetchone()[0])
            cur.execute("ROLLBACK")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        return changed, vanished, mid
    finally:
        cur.close()


async def test_full_merge_ducklake_delete_rolls_back_without_aborting(tmp_path: Path) -> None:
    path = tmp_path / "warehouse.ducklake"
    engine = DuckDBAdapter.connect(f"ducklake:{path}")
    try:
        engine.execute_sync("SET threads = 32")
        catalog = str(engine.fetch_sync("SELECT current_database() AS catalog").read_all().to_pylist()[0]["catalog"])
        engine.execute_sync(f"CALL ducklake_set_option('{catalog}', 'data_inlining_row_limit', '0')")
        engine.execute_sync("CREATE TABLE other AS SELECT i AS id, 'n' || i AS name FROM range(1, 81) t(i)")
        engine.execute_sync("CREATE TABLE dim AS SELECT i AS id, 'v' || i AS name FROM range(3, 81) t(i)")
        engine.execute_sync("CREATE TABLE target AS SELECT i AS id, 'n' || i AS name FROM range(1, 72) t(i)")
        files = engine.fetch_sync(
            f"SELECT data_file, delete_file FROM ducklake_list_files('{catalog}', 'target')"
        ).read_all()
        assert files.num_rows == 1
        assert files.column("delete_file")[0].as_py() is None

        relation = SqlRelation.from_sql(_MODEL)
        target = TableRef(schema="main", name="target")
        statements = FullMerge(("id",)).plan_statements(relation, target, EngineCaps())
        sqls = [engine.transpile(statement) for statement in statements]
        assert any(sql.startswith("DELETE") for sql in sqls)
        assert all("EXCEPT" not in sql for sql in sqls if sql.startswith("DELETE"))

        before = int(engine.fetch_sync("SELECT count(*) AS n FROM main.target").read_all().to_pylist()[0]["n"])
        assert before == 71
        changed, vanished, mid = await asyncio.to_thread(_rollback, engine._conn, sqls)
        assert (changed, vanished, mid) == (69, 2, 78)
        after = int(engine.fetch_sync("SELECT count(*) AS n FROM main.target").read_all().to_pylist()[0]["n"])
        assert after == before
        assert engine.fetch_sync("SELECT 1 AS ok").read_all().to_pylist() == [{"ok": 1}]

        counts = await engine.execute_all(statements)
        assert FullMerge(("id",)).row_counts(counts) == RowCounts(inserted=9, updated=69, deleted=2)
        rows = engine.fetch_sync("SELECT id, name FROM main.target ORDER BY id").read_all().to_pylist()
        assert [row["id"] for row in rows] == list(range(3, 81))
        assert all(row["name"] == f"v{row['id']}" for row in rows)
    finally:
        engine.close()
