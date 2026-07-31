"""DuckDB engine adapter — the default local engine and federation hub.

Everything crosses the boundary as Arrow: :meth:`fetch` streams results as a
``pyarrow.RecordBatchReader`` (zero-copy, single pass) and :meth:`load` registers
an Arrow reader and writes it with one ``CREATE TABLE AS`` / ``INSERT``. Blocking
DuckDB calls run in a worker thread; each call uses its own ``cursor()`` so reads
proceed concurrently (DuckDB MVCC), while the DAG guarantees no two tasks write
the same table at once.

On **DuckLake** connections, statements that MUTATE THE CATALOG are serialised
on ``_write_lock``. DuckLake's catalog layer is not safe against concurrent DDL
on sibling cursors of one DatabaseInstance: under parallel builds a
``CREATE TABLE <schema>.<name>`` intermittently loses its schema qualification
and creates the table in the catalog's default schema instead, silently — no
error, no transaction conflict. The snapshot then disagrees with what the state
store recorded, and every later run fails resolving the table. Reads stay
unlocked, so parallelism is only lost where the catalog is actually being
written.

Plain DuckDB catalogs don't have that bug, and the DAG already guarantees no
two tasks write the same table — so there the "lock" is a no-op context and
builds genuinely run in parallel (measured ~4x on 4 concurrent CTAS). A
transaction conflict on genuinely contended catalog objects still surfaces as
``TransactionException`` and is retried where the batch is idempotent.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading
from collections.abc import Iterator, Sequence
from uuid import uuid4

import duckdb
import pyarrow as pa
import tenacity
from sqlglot import exp

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode
from interlace.ir.relation import TableRef

_DUCKDB_CAPS = EngineCaps(
    supports_create_or_replace=True,
    supports_star_exclude=True,
)


# DuckLake uses optimistic concurrency: a concurrent writer's commit surfaces as
# a TransactionException. Our write batches are whole-transaction idempotent
# (CREATE OR REPLACE / DELETE+INSERT run as one unit), so a short retry is safe.
_commit_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type(duckdb.TransactionException),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential_jitter(initial=0.1, max=1.0),
    reraise=True,
)


def _affected(cur: duckdb.DuckDBPyConnection) -> int:
    """DML/CTAS/COPY return their affected-row count as a one-cell result; DDL returns
    nothing. Never raises — row stats are best-effort decoration, not correctness."""
    try:
        rows = cur.fetchall()
        return int(rows[0][0]) if rows and rows[0] and isinstance(rows[0][0], int) else 0
    except Exception:
        return 0


class DuckDBAdapter(EngineAdapter):
    """Executes canonical ASTs and moves Arrow data in and out of a DuckDB database."""

    dialect = "duckdb"
    caps = _DUCKDB_CAPS

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        session_init: Sequence[str] = (),
        *,
        serialise_writes: bool = False,
    ) -> None:
        self._conn = connection
        # Statements re-applied on every cursor — SESSION-LOCAL state only (USE).
        # Anything instance-wide (LOAD, secrets, ATTACH) belongs at connect time:
        # re-running catalog writes here races across concurrent cursors.
        self._session_init = list(session_init)
        self._attached: list[str] = []  # aliases to DETACH on close (see close())
        # Serialises catalog-mutating statements on DuckLake catalogs only (see
        # module docstring); a no-op context elsewhere so builds run in parallel.
        # Plain Lock, not RLock: no locked path calls another, and a plain Lock
        # turns an accidental nesting into an obvious deadlock rather than silent
        # re-entry.
        self._write_lock: contextlib.AbstractContextManager[object] = (
            threading.Lock() if serialise_writes else contextlib.nullcontext()
        )

    def _cursor(self) -> duckdb.DuckDBPyConnection:
        cur = self._conn.cursor()
        for statement in self._session_init:
            cur.execute(statement)
        return cur

    @classmethod
    def in_memory(cls) -> DuckDBAdapter:
        return cls(duckdb.connect(":memory:"))

    @classmethod
    def connect(cls, path: str) -> DuckDBAdapter:
        return cls(duckdb.connect(path), serialise_writes=path.startswith("ducklake:"))

    @classmethod
    def connect_ducklake(
        cls,
        catalog: str,
        *,
        alias: str = "warehouse",
        data_path: str | None = None,
        metadata_schema: str | None = None,
        secrets: Sequence[str] = (),
        extensions: Sequence[str] = (),
    ) -> DuckDBAdapter:
        """Open a DuckLake warehouse that needs attach options and/or credentials —
        remote catalogs (``postgres:…``) and object-store ``data_path``s can't ride the
        plain ``duckdb.connect("ducklake:…")`` shortcut. Opens ``:memory:``, installs
        the extensions, issues the ``CREATE SECRET`` statements, ATTACHes the DuckLake
        with the options, and makes it the default catalog."""
        conn = duckdb.connect(":memory:")
        for extension in extensions:
            conn.execute(f"INSTALL {extension}; LOAD {extension};")
        for statement in secrets:
            conn.execute(statement)
        options: list[str] = []
        if data_path:
            options.append(f"DATA_PATH '{data_path.replace(chr(39), chr(39) * 2)}'")
        if metadata_schema:
            options.append(f"METADATA_SCHEMA '{metadata_schema.replace(chr(39), chr(39) * 2)}'")
        options_sql = f" ({', '.join(options)})" if options else ""
        escaped = catalog.replace("'", "''")
        alias_sql = exp.to_identifier(alias).sql("duckdb")
        conn.execute(f"ATTACH IF NOT EXISTS '{escaped}' AS {alias_sql}{options_sql}")
        conn.execute(f"USE {alias_sql}")
        # LOAD, secrets, and ATTACH are all instance-wide — they carry into every
        # cursor and must run ONCE (re-running CREATE OR REPLACE SECRET per cursor
        # races: concurrent cursors hit "catalog write-write conflict on alter").
        # Only the default catalog is session state, so that is all a cursor re-applies.
        return cls(conn, session_init=[f"USE {alias_sql}"], serialise_writes=True)

    def close(self) -> None:
        # DETACH long-lived attaches first: DuckLake leaks its DatabaseInstance when
        # concurrent cursors were used (duckdb 1.5.4), which would otherwise keep the
        # attached databases' file handles locked for the rest of the process.
        for alias in self._attached:
            with contextlib.suppress(Exception):
                self._conn.execute(f"DETACH {exp.to_identifier(alias).sql('duckdb')}")
        self._attached.clear()
        self._conn.close()

    def interrupt(self) -> None:
        """Cancel the currently-running statement(s) on this connection (best effort)."""
        with contextlib.suppress(Exception):
            self._conn.interrupt()

    def attach(self, alias: str, uri: str) -> None:
        """ATTACH another database (duckdb/sqlite/postgres/... URI) under ``alias``."""
        escaped = uri.replace("'", "''")
        self._conn.execute(f"ATTACH IF NOT EXISTS '{escaped}' AS {exp.to_identifier(alias).sql('duckdb')}")
        self._attached.append(alias)
        if uri.startswith("ducklake:"):  # writes may now reach a DuckLake catalog (e.g. table sinks)
            if isinstance(self._write_lock, contextlib.nullcontext):
                self._write_lock = threading.Lock()

    # --- identifier helpers -------------------------------------------------

    def _table_sql(self, table: TableRef) -> str:
        return table.to_expr().sql(dialect=self.dialect)

    # --- EngineAdapter ------------------------------------------------------

    async def execute(self, ast: exp.Expression) -> None:
        await self.execute_sql(self.transpile(ast))

    async def execute_all(self, statements: Sequence[exp.Expression]) -> list[int]:
        return await asyncio.to_thread(self._execute_all_sync, [self.transpile(s) for s in statements])

    async def fetch(self, ast: exp.Expression) -> pa.RecordBatchReader:
        return await self.fetch_sql(self.transpile(ast))

    async def load(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        return await asyncio.to_thread(self._load_sync, table, reader, mode)

    async def create_view(self, name: TableRef, target: TableRef) -> None:
        await self.execute_sql(
            f"CREATE OR REPLACE VIEW {self._table_sql(name)} AS SELECT * FROM {self._table_sql(target)}"
        )

    # --- raw / convenience (used by the state store and tests) --------------

    async def execute_sql(self, sql: str) -> None:
        await asyncio.to_thread(self._execute_sync, sql)

    async def fetch_sql(self, sql: str) -> pa.RecordBatchReader:
        return await asyncio.to_thread(self._fetch_sync, sql)

    async def create_schema(self, name: str) -> None:
        await self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {exp.to_identifier(name).sql(dialect=self.dialect)}")

    async def table_exists(self, table: TableRef) -> bool:
        return await asyncio.to_thread(self._table_exists_sync, table)

    async def describe(self, table: TableRef) -> dict[str, str]:
        return await asyncio.to_thread(self._describe_sync, table)

    # --- sync workers (run in a thread) -------------------------------------

    @_commit_retry
    def _execute_sync(self, sql: str) -> None:
        with self._write_lock:  # may be DDL (create_schema / create_view / migrations)
            cur = self._cursor()
            try:
                cur.execute(sql)
            finally:
                cur.close()

    @_commit_retry
    def _execute_all_sync(self, sqls: list[str]) -> list[int]:
        counts: list[int] = []
        with self._write_lock:  # a strategy's CREATE / DELETE / INSERT / DROP batch
            cur = self._cursor()
            try:
                cur.execute("BEGIN")
                for sql in sqls:
                    cur.execute(sql)
                    counts.append(_affected(cur))
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()
        return counts

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        # Read-only: deliberately not locked, so scans run concurrently (DuckDB MVCC).
        # The cursor must outlive the stream that reads from it, so it is closed when
        # that stream ends rather than left for the garbage collector to reclaim on
        # whatever thread happens to drop the last reference.
        cur = self._cursor()
        cur.execute(sql)
        reader = cur.to_arrow_reader()

        def batches() -> Iterator[pa.RecordBatch]:
            try:
                yield from reader
            finally:
                cur.close()

        return pa.RecordBatchReader.from_batches(reader.schema, batches())

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        # NOT wrapped in @_commit_retry: ``reader`` is a single-pass stream, and a
        # DuckLake conflict surfaces at COMMIT — i.e. after the stream has already
        # been drained. Re-running the body would re-register an exhausted reader and
        # write an empty table while reporting success. Failing loudly is correct; the
        # caller re-runs the model, which rebuilds the reader.
        with self._write_lock:
            cur = self._cursor()
            src = f"__interlace_src_{uuid4().hex}"
            cur.register(src, reader)
            try:
                target = self._table_sql(table)
                if mode == "create":
                    cur.execute(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {src}")
                else:
                    cur.execute(f"INSERT INTO {target} SELECT * FROM {src}")
                return _affected(cur)
            finally:
                cur.unregister(src)
                cur.close()

    def _table_exists_sync(self, table: TableRef) -> bool:
        # information_schema spans every attached catalog: pin to the ref's catalog
        # (or the session default) so same-named tables elsewhere don't collide.
        cur = self._cursor()
        try:
            row = cur.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? "
                "AND table_catalog = coalesce(?, current_database())",
                [table.schema, table.name, table.catalog],
            ).fetchone()
        finally:
            cur.close()
        return bool(row and row[0])

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        cur = self._cursor()
        try:
            rows = cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ? "
                "AND table_catalog = coalesce(?, current_database()) ORDER BY ordinal_position",
                [table.schema, table.name, table.catalog],
            ).fetchall()
        finally:
            cur.close()
        return dict(rows)
