"""DuckDB engine adapter — the default local engine and federation hub.

Everything crosses the boundary as Arrow: :meth:`fetch` streams results as a
``pyarrow.RecordBatchReader`` (zero-copy, single pass) and :meth:`load` registers
an Arrow reader and writes it with one ``CREATE TABLE AS`` / ``INSERT``. Blocking
DuckDB calls run in a worker thread; each call uses its own ``cursor()`` so reads
proceed concurrently (DuckDB MVCC), while the DAG guarantees no two tasks write
the same table at once. A second thread on a busy connection raises
``ResourceInUseException``, so the cursor per call is required. ``interrupt()``
has to reach those cursors: interrupting the parent connection does not cancel a
query running on one.

On **DuckLake** connections every cursor — reads included — is serialised on
``_write_lock``. DuckLake's catalog layer is not safe against sibling cursors of
one DatabaseInstance: under parallel builds a ``CREATE TABLE <schema>.<name>``
intermittently commits with an empty schema name and lands in the catalog's
default schema (``main``), silently. Serialising writes alone does not stop it;
a read cursor open across the commit is enough. A read that fits in memory is
copied there and the cursor closed before the lock is released. A larger read
is written to a temporary Arrow file, one batch at a time, on the same terms.
Placement — pinning the catalog name, confirming the schema, and retrying a
miss — lives in
:mod:`interlace.engines.placement` and runs only on these connections.

Plain DuckDB catalogs don't have that bug, and the DAG already guarantees no
two tasks write the same table — so there the "lock" is a no-op context and
builds genuinely run in parallel (measured ~4x on 4 concurrent CTAS). A
transaction conflict on genuinely contended catalog objects still surfaces as
``TransactionException`` and is retried where the batch is idempotent.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import re
import tempfile
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import replace
from uuid import uuid4

import duckdb
import pyarrow as pa
import tenacity
from sqlglot import exp

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode, note_statement, relation_is_absent
from interlace.engines.placement import (
    CatalogPlacementError,
    confirm_statement,
    drop_misplaced,
    pin_catalog,
    retry_placement,
)
from interlace.exceptions import ConfigurationError
from interlace.ir.relation import TableRef
from interlace.readers import own_reader, release_owned

_DUCKDB_CAPS = EngineCaps(
    supports_create_or_replace=True,
    supports_star_exclude=True,
    supports_merge=True,  # MERGE INTO ... (DuckDB >= 1.3)
    supports_transactions=True,  # execute_all wraps BEGIN/COMMIT
    # PRIMARY KEY / UNIQUE / FOREIGN KEY / CHECK are accepted and not enforced.
    # NOT NULL is enforced, and only via ALTER COLUMN (ADD CONSTRAINT is unimplemented).
    enforced_constraints=frozenset({"not_null"}),
    not_null_as_column=True,
    supports_attach=True,
)
# DuckLake's delete finalizer aborts when EXCEPT is nested in the DELETE.
# Staging the key set is safe; file DuckDB keeps the direct delete.
_DUCKLAKE_CAPS = replace(_DUCKDB_CAPS, except_in_delete=False)

# DuckLake cannot leave the read cursor open, so a large scan is copied to a
# file. A watermark or a catalog probe is a few rows: those stay in memory and
# skip the temp file. The cap is the buffered batch size, not the final table.
_SPILL_BYTES = 8 * 1024 * 1024


# DuckLake uses optimistic concurrency: a concurrent writer's commit surfaces as
# a TransactionException. Our write batches are whole-transaction idempotent
# (CREATE OR REPLACE / DELETE+INSERT run as one unit), so a short retry is safe.
_commit_retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type(duckdb.TransactionException),
    stop=tenacity.stop_after_attempt(3),
    wait=tenacity.wait_exponential_jitter(initial=0.1, max=1.0),
    reraise=True,
)


@contextlib.contextmanager
def _clean_lock_error(database: str) -> Iterator[None]:
    """Translate DuckDB's file-lock conflict into one actionable line.

    A DuckDB/DuckLake database is held by a single process. The common way to hit
    that is running a CLI command (`interlace query`, `plan`, `apply`) while
    `interlace serve` is up — an obvious thing to do, since serve is the daemon
    and query is the console's CLI counterpart. Raw, that surfaces as a dozen
    frames ending in `duckdb.IOException`, which names neither the cause nor the
    fix. The fix is `--quack`, and it is already documented; the error just never
    said so.
    """
    try:
        yield
    except duckdb.IOException as exc:
        message = str(exc)
        # "Conflicting lock is held in <exe> (PID n)" is the Linux rendering — DuckDB
        # names the holder from /proc/locks. Elsewhere the message is the bare "Could
        # not set lock on file", and a same-process conflict says "already held", so
        # match all three or macOS/Windows keep the raw traceback.
        if not any(marker in message for marker in ("Conflicting lock", "Could not set lock", "already held")):
            raise
        holder = re.search(r"\(PID (\d+)\)", message)
        held_by = f" (PID {holder.group(1)})" if holder else ""
        raise ConfigurationError(
            f"the warehouse {database!r} is already open in another process{held_by}. "
            "DuckDB allows one process at a time — stop `interlace serve`, or serve the "
            "warehouse over the quack protocol (`interlace serve --quack quack:localhost:4213`) "
            "and point this process at `database: quack:localhost:4213` to share it.",
            details={"database": database},
        ) from None


def _affected(cur: duckdb.DuckDBPyConnection) -> int:
    """DML/CTAS/COPY return their affected-row count as a one-cell result; DDL returns
    nothing. Never raises — row stats are best-effort decoration, not correctness."""
    try:
        rows = cur.fetchall()
        return int(rows[0][0]) if rows and rows[0] and isinstance(rows[0][0], int) else 0
    except Exception:
        return 0


_SETTING_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_NUMERIC = re.compile(r"-?\d+(?:\.\d+)?")


def _setting_literal(value: object) -> str:
    """A setting value as a SQL literal. Numbers go bare — including a numeric STRING,
    because ``${THREADS:-4}`` arrives from YAML as text and ``threads`` is a BIGINT."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    text = str(value)
    if _NUMERIC.fullmatch(text):
        return text
    return "'" + text.replace("'", "''") + "'"


class DuckDBAdapter(EngineAdapter):
    """Executes canonical ASTs and moves Arrow data in and out of a DuckDB database."""

    dialect = "duckdb"
    caps = _DUCKDB_CAPS
    _refresh_inputs: Callable[[], None] | None = None

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        session_init: Sequence[str] = (),
        *,
        serialise_writes: bool = False,
        catalog_alias: str | None = None,
    ) -> None:
        self._conn = connection
        # Attached DuckLake alias (``warehouse``, or the project name). CREATE targets
        # are written ``alias.schema.table`` so the schema is not resolved through the
        # session default, which is ``main``.
        self._catalog_alias = catalog_alias
        # Statements re-applied on every cursor — SESSION-LOCAL state only (USE).
        # Anything instance-wide (LOAD, secrets, ATTACH) belongs at connect time:
        # re-running catalog writes here races across concurrent cursors.
        self._session_init = list(session_init)
        self._attached: list[str] = []  # aliases to DETACH on close (see close())
        # Cursors with a statement in flight. interrupt() walks this set:
        # connection.interrupt() does not cancel a cursor's query.
        self._cursors: set[duckdb.DuckDBPyConnection] = set()
        # Abandoned readers must not close their cursor on the GC thread.
        # The next use of this connection, and close(), drains the list.
        self._deferred: list[duckdb.DuckDBPyConnection] = []
        self._cursor_lock = threading.Lock()
        self._closed = False
        # What apply_settings last set. The scratch database reads this so a Python
        # model that opens its own DuckDB inherits the warehouse cap.
        self.settings: dict[str, object] = {}
        self.caps = _DUCKLAKE_CAPS if serialise_writes else _DUCKDB_CAPS
        # Placement confirmation is DuckLake-only. Plain catalogs skip it, so a view
        # promotion does not probe information_schema for a same-named table.
        self._guard_catalog = serialise_writes
        # One cursor at a time on DuckLake (see module docstring); a no-op context
        # elsewhere so builds run in parallel. Plain Lock, not RLock: no locked
        # path calls another, and a plain Lock turns an accidental nesting into an
        # obvious deadlock rather than silent re-entry.
        self._write_lock: contextlib.AbstractContextManager[object] = (
            threading.Lock() if serialise_writes else contextlib.nullcontext()
        )

    def _cursor(self) -> duckdb.DuckDBPyConnection:
        self._drain_deferred()
        cur = self._conn.cursor()
        with self._cursor_lock:
            self._cursors.add(cur)
        try:
            for statement in self._session_init:
                cur.execute(statement)
        except Exception:
            self._close_cursor(cur)
            raise
        return cur

    def _close_cursor(self, cur: duckdb.DuckDBPyConnection) -> None:
        with self._cursor_lock:
            self._cursors.discard(cur)
            self._deferred = [pending for pending in self._deferred if pending is not cur]
        # Shutdown can close a cursor the reader is also closing.
        with contextlib.suppress(Exception):
            cur.close()

    def _defer_cursor(self, cur: duckdb.DuckDBPyConnection) -> None:
        """Remember a cursor whose reader was dropped, without closing it yet."""
        with self._cursor_lock:
            if cur not in self._cursors:
                return
            self._cursors.discard(cur)
            self._deferred.append(cur)

    def _drain_deferred(self) -> None:
        """Close cursors whose readers were dropped. Runs on a connection thread."""
        with self._cursor_lock:
            pending = self._deferred
            self._deferred = []
        for cur in pending:
            with contextlib.suppress(Exception):
                cur.close()

    @classmethod
    def in_memory(cls) -> DuckDBAdapter:
        return cls(duckdb.connect(":memory:"))

    @classmethod
    def connect(cls, path: str) -> DuckDBAdapter:
        with _clean_lock_error(path):
            conn = duckdb.connect(path)
        return cls(conn, serialise_writes=path.startswith("ducklake:"))

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
        with _clean_lock_error(catalog):
            conn.execute(f"ATTACH IF NOT EXISTS '{escaped}' AS {alias_sql}{options_sql}")
        conn.execute(f"USE {alias_sql}")
        # LOAD, secrets, and ATTACH are all instance-wide — they carry into every
        # cursor and must run ONCE (re-running CREATE OR REPLACE SECRET per cursor
        # races: concurrent cursors hit "catalog write-write conflict on alter").
        # Only the default catalog is session state, so that is all a cursor re-applies.
        return cls(conn, session_init=[f"USE {alias_sql}"], serialise_writes=True, catalog_alias=alias)

    def close(self) -> None:
        # Cursors first, then DETACH. A cursor still open at shutdown leaks
        # DuckLake's DatabaseInstance and keeps the connection busy, so closing
        # the connection aborts the process. interrupt() is what makes a cursor
        # that is inside a scan willing to close.
        if self._closed:
            return
        self.interrupt()
        self._drain_deferred()
        with self._cursor_lock:
            live = list(self._cursors)
            self._cursors.clear()
        for cur in live:
            with contextlib.suppress(Exception):
                cur.close()
        for alias in self._attached:
            with contextlib.suppress(Exception):
                self._conn.execute(f"DETACH {exp.to_identifier(alias).sql('duckdb')}")
        self._attached.clear()
        self._conn.close()
        self._closed = True

    def interrupt(self) -> None:
        """Cancel the currently-running statement(s) on this connection (best effort).

        Interrupting the parent connection leaves a cursor's query running, so every
        open cursor is interrupted too.
        """
        with contextlib.suppress(Exception):
            self._conn.interrupt()
        with self._cursor_lock:
            cursors = list(self._cursors)
        for cur in cursors:
            with contextlib.suppress(Exception):
                cur.interrupt()

    def search_files_from(self, directory: str) -> None:
        """Resolve relative read paths (``read_csv_auto('seeds/x.csv')``) against
        ``directory`` — the project root — as well as the process CWD.

        Additive: a CWD-relative path still resolves, so this only ever widens what a
        model can find. GLOBAL scope because a plain ``SET`` is session-scoped and would
        not reach the per-task cursors that actually run the queries. Reads only —
        ``COPY`` targets stay CWD-relative, which is why exports resolve their own paths
        against the root (``plan.apply._resolve_export_path``)."""
        escaped = directory.replace("'", "''")
        self._conn.execute(f"SET GLOBAL file_search_path='{escaped}'")

    def apply_settings(self, settings: Mapping[str, object]) -> None:
        """``SET GLOBAL`` each configured DuckDB setting on the instance — ``memory_limit``,
        ``threads``, ``temp_directory`` and the rest of ``duckdb_settings()``.

        GLOBAL for the same reason as ``search_files_from``: the per-task cursors that run
        the queries inherit instance settings, not a session's. Left unset, DuckDB takes 80%
        of the host's RAM and a thread per core, and does so per process — the wrong default
        for a daemon sharing a host with other daemons. The name is checked as an identifier
        because it is interpolated into SQL; DuckDB rejects an unknown setting itself."""
        self.settings = dict(settings)
        for name, value in settings.items():
            if not _SETTING_NAME.fullmatch(name):
                raise ValueError(f"duckdb setting {name!r} is not a setting name")
            self._conn.execute(f"SET GLOBAL {name} = {_setting_literal(value)}")

    def attach(self, alias: str, uri: str) -> None:
        """ATTACH another database (duckdb/sqlite/postgres/... URI) under ``alias``."""
        escaped = uri.replace("'", "''")
        with _clean_lock_error(uri):  # attaching a held duckdb/ducklake file conflicts just like opening one
            self._conn.execute(f"ATTACH IF NOT EXISTS '{escaped}' AS {exp.to_identifier(alias).sql('duckdb')}")
        self._attached.append(alias)
        if uri.startswith("ducklake:"):  # writes may now reach a DuckLake catalog (e.g. table sinks)
            self.caps = _DUCKLAKE_CAPS
            self._guard_catalog = True
            if isinstance(self._write_lock, contextlib.nullcontext):
                self._write_lock = threading.Lock()

    # --- identifier helpers -------------------------------------------------

    def _table_sql(self, table: TableRef) -> str:
        return table.to_expr().sql(dialect=self.dialect)

    # --- EngineAdapter ------------------------------------------------------

    async def execute(self, ast: exp.Expr) -> None:
        await self.execute_sql(self.transpile(ast))

    async def execute_all(self, statements: Sequence[exp.Expr]) -> list[int]:
        return await asyncio.to_thread(self._execute_all_sync, [self.transpile(s) for s in statements])

    async def commit_batch(self, source: str, reader: pa.RecordBatchReader, statements: Sequence[exp.Expr]) -> bool:
        """Register ``reader`` as ``source`` and run ``statements`` in one transaction.

        A stream landing uses this so the rows and the watermark commit together,
        without a stage table. The reader is single-pass, so a conflict is not
        retried: the caller rebuilds the batch.
        """
        sqls = [pin_catalog(self.transpile(statement), self._catalog_alias) for statement in statements]
        await asyncio.to_thread(self._commit_batch_sync, source, reader, sqls)
        return True

    async def fetch(self, ast: exp.Expr) -> pa.RecordBatchReader:
        return await self.fetch_sql(self.transpile(ast))

    async def load(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        try:
            return await asyncio.to_thread(self._load_sync, table, reader, mode)
        finally:
            # The scan has finished or failed. An owned reader closes whatever it
            # borrowed (a cursor, a scratch connection) here, on this thread.
            release_owned(reader)

    async def create_view(self, name: TableRef, target: TableRef) -> None:
        await self.execute_sql(
            f"CREATE OR REPLACE VIEW {self._table_sql(name)} AS SELECT * FROM {self._table_sql(target)}"
        )

    # --- raw / convenience (used by the state store and tests) --------------

    async def execute_sql(self, sql: str) -> None:
        await asyncio.to_thread(self._execute_sync, sql)

    async def fetch_sql(self, sql: str) -> pa.RecordBatchReader:
        return await asyncio.to_thread(self._fetch_sync, sql)

    def execute_sync(self, sql: str) -> None:
        """Run one statement on the calling thread. Fixture tests use this."""
        self._execute_sync(sql)

    def fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        """Read one statement on the calling thread. Fixture tests use this."""
        return self._fetch_sync(sql)

    # No fetch_sandboxed override: DuckDB cannot sandbox one query on the shared
    # warehouse connection without poisoning the writer (enable_external_access is
    # instance-wide and one-way, and a DuckLake catalog can be held by only one
    # connection per process). The untrusted /query path is fenced at parse time by
    # interlace.query.prepare_readonly — as the base contract expects.

    async def create_schema(self, name: str) -> None:
        await self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {exp.to_identifier(name).sql(dialect=self.dialect)}")

    async def table_exists(self, table: TableRef) -> bool:
        return await asyncio.to_thread(self._table_exists_sync, table)

    async def describe(self, table: TableRef) -> dict[str, str]:
        return await asyncio.to_thread(self._describe_sync, table)

    async def list_indexes(self, table: TableRef) -> list[str]:
        return await self._catalog_names(table, "duckdb_indexes()", "index_name")

    async def list_constraints(self, table: TableRef) -> list[str]:
        return await self._catalog_names(table, "duckdb_constraints()", "constraint_name")

    async def _catalog_names(self, table: TableRef, function: str, column: str) -> list[str]:
        schema = exp.Literal.string(table.schema).sql(dialect=self.dialect)
        name = exp.Literal.string(table.name).sql(dialect=self.dialect)
        catalog = exp.Literal.string(table.catalog).sql(dialect=self.dialect) if table.catalog else "current_database()"
        sql = (
            f"SELECT {column} FROM {function} "
            f"WHERE schema_name = {schema} AND table_name = {name} "
            f"AND database_name = coalesce({catalog}, current_database())"
        )
        try:
            reader = await self.fetch_sql(sql)
        except Exception as exc:
            if relation_is_absent(exc):
                return []
            raise
        return [str(row[column]) for row in reader.read_all().to_pylist() if row.get(column)]

    # --- sync workers (run in a thread) -------------------------------------

    def _execute_once(self, sql: str) -> None:
        cur = self._cursor()
        try:
            cur.execute(sql)
            if self._guard_catalog:
                try:
                    confirm_statement(cur, sql)
                except CatalogPlacementError:
                    # Autocommit: the empty-schema table is already stored. Drop it so the
                    # retry creates the named schema, and so a failed retry leaves nothing in main.
                    drop_misplaced(cur, sql)
                    raise
        except CatalogPlacementError:
            raise
        except Exception as exc:
            note_statement(exc, sql)
            raise
        finally:
            self._close_cursor(cur)

    @_commit_retry
    def _execute_sync(self, sql: str) -> None:
        sql = pin_catalog(sql, self._catalog_alias)
        with self._write_lock:  # may be DDL (create_schema / create_view / migrations)
            if not self._guard_catalog:
                self._execute_once(sql)
                return

            def attempt() -> None:
                try:
                    self._execute_once(sql)
                except CatalogPlacementError as exc:
                    note_statement(exc, sql)
                    raise

            retry_placement(attempt)

    def _execute_all_once(self, sqls: list[str]) -> list[int]:
        counts: list[int] = []
        cur = self._cursor()
        try:
            cur.execute("BEGIN")
            for sql in sqls:
                try:
                    cur.execute(sql)
                    counts.append(_affected(cur))
                    # Before COMMIT. A missing schema rolls this transaction back;
                    # the table never becomes the recorded snapshot.
                    if self._guard_catalog:
                        confirm_statement(cur, sql)
                except Exception as exc:
                    note_statement(exc, sql)
                    raise
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            self._close_cursor(cur)
        return counts

    def _commit_batch_sync(self, source: str, reader: pa.RecordBatchReader, sqls: list[str]) -> None:
        with self._write_lock:
            cur = self._cursor()
            try:
                try:
                    cur.execute("BEGIN")
                    cur.register(source, reader)
                    for sql in sqls:
                        try:
                            cur.execute(sql)
                            if self._guard_catalog:
                                confirm_statement(cur, sql)
                        except CatalogPlacementError:
                            raise
                        except Exception as exc:
                            note_statement(exc, sql)
                            raise
                    cur.execute("COMMIT")
                except Exception:
                    with contextlib.suppress(Exception):
                        cur.execute("ROLLBACK")
                    raise
                finally:
                    with contextlib.suppress(Exception):
                        cur.unregister(source)
            finally:
                self._close_cursor(cur)

    @_commit_retry
    def _execute_all_sync(self, sqls: list[str]) -> list[int]:
        sqls = [pin_catalog(sql, self._catalog_alias) for sql in sqls]
        with self._write_lock:  # a strategy's CREATE / DELETE / INSERT / DROP batch
            if not self._guard_catalog:
                return self._execute_all_once(sqls)
            return retry_placement(lambda: self._execute_all_once(sqls))

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        # Plain DuckDB streams: the cursor must outlive the reader, and is closed
        # when that stream ends rather than left for the GC. DuckLake cannot: a
        # cursor left open across another task's CREATE is what drops the schema,
        # so the scan is buffered inside the lock and the cursor is closed
        # before the caller sees a row. Past ``_SPILL_BYTES`` the buffer
        # becomes a temp Arrow file.
        if not self._guard_catalog:
            return self._stream_sync(sql)
        return self._spill_sync(sql)

    def _stream_sync(self, sql: str) -> pa.RecordBatchReader:
        cur = self._cursor()
        try:
            reader = self._arrow_reader(cur, sql)
        except Exception:
            self._close_cursor(cur)
            raise
        return self._stream_reader(cur, reader)

    def _arrow_reader(self, cur: duckdb.DuckDBPyConnection, sql: str) -> pa.RecordBatchReader:
        """Stream ``sql`` as Arrow.

        ``execute().to_arrow_reader()`` copies every row and warns. A relation streams.
        """
        return cur.sql(sql).to_arrow_reader()

    def _stream_reader(self, cur: duckdb.DuckDBPyConnection, reader: pa.RecordBatchReader) -> pa.RecordBatchReader:
        # Exhausting the reader closes the cursor on the caller's thread.
        # Dropping it only defers the close: the finalizer must not call into
        # DuckDB while another cursor is mid-query.
        return own_reader(reader, release=lambda: self._close_cursor(cur), defer=lambda: self._defer_cursor(cur))

    def _spill_sync(self, sql: str) -> pa.RecordBatchReader:
        """Read one DuckLake scan with its cursor closed before the caller starts.

        The write lock covers the scan. Up to ``_SPILL_BYTES`` stays in memory.
        A larger scan is written to a temp Arrow file one batch at a time, and
        the caller reads that file after the lock and the cursor are released.
        """
        path: str | None = None
        try:
            with self._write_lock:
                cur = self._cursor()
                try:
                    source = self._arrow_reader(cur, sql)
                    schema = source.schema
                    held, overflow = self._buffer_until_cap(source)
                    if overflow is None:
                        return pa.RecordBatchReader.from_batches(schema, held)
                    descriptor, path = tempfile.mkstemp(prefix="interlace-", suffix=".arrow")
                    os.close(descriptor)
                    self._write_spill(path, schema, held, overflow, source)
                finally:
                    self._close_cursor(cur)
            assert path is not None
            return self._read_spill(path, schema)
        except BaseException:
            if path is not None:
                with contextlib.suppress(OSError):
                    os.unlink(path)
            raise

    def _buffer_until_cap(self, source: pa.RecordBatchReader) -> tuple[list[pa.RecordBatch], pa.RecordBatch | None]:
        held: list[pa.RecordBatch] = []
        used = 0
        for batch in source:
            used += batch.nbytes
            if used > _SPILL_BYTES:
                return held, batch
            held.append(batch)
        return held, None

    def _write_spill(
        self,
        path: str,
        schema: pa.Schema,
        held: list[pa.RecordBatch],
        overflow: pa.RecordBatch,
        source: pa.RecordBatchReader,
    ) -> None:
        with pa.ipc.new_file(path, schema) as writer:
            for batch in held:
                writer.write_batch(batch)
            writer.write_batch(overflow)
            for batch in source:
                writer.write_batch(batch)

    def _read_spill(self, path: str, schema: pa.Schema) -> pa.RecordBatchReader:
        mapped = pa.memory_map(path, "r")
        stored = pa.ipc.open_file(mapped)
        released = False
        gate = threading.Lock()

        def release() -> None:
            nonlocal released
            with gate:
                if released:
                    return
                released = True
            with contextlib.suppress(Exception):
                mapped.close()
            with contextlib.suppress(OSError):
                os.unlink(path)

        def batches() -> Iterator[pa.RecordBatch]:
            for index in range(stored.num_record_batches):
                yield stored.get_batch(index)

        return own_reader(pa.RecordBatchReader.from_batches(schema, batches()), release=release, defer=release)

    def _load_sync(self, table: TableRef, reader: pa.RecordBatchReader, mode: LoadMode) -> int:
        # NOT wrapped in @_commit_retry: ``reader`` is a single-pass stream, and a
        # DuckLake conflict surfaces at COMMIT — i.e. after the stream has already
        # been drained. Re-running the body would re-register an exhausted reader and
        # write an empty table while reporting success. Failing loudly is correct; the
        # caller re-runs the model, which rebuilds the reader.
        with self._write_lock:
            cur = self._cursor()
            src = f"__interlace_src_{uuid4().hex}"
            try:
                cur.register(src, reader)
                try:
                    target = self._table_sql(table)
                    if mode == "create":
                        sql = pin_catalog(
                            f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {src}", self._catalog_alias
                        )
                        cur.execute(sql)
                        # Row count before the placement probe: that probe runs its own statements.
                        written = _affected(cur)
                        if self._guard_catalog:
                            try:
                                confirm_statement(cur, sql)
                            except CatalogPlacementError:
                                drop_misplaced(cur, sql)
                                raise
                        return written
                    cur.execute(f"INSERT INTO {target} SELECT * FROM {src}")
                    return _affected(cur)
                finally:
                    cur.unregister(src)
            finally:
                self._close_cursor(cur)

    def _table_exists_sync(self, table: TableRef) -> bool:
        # information_schema spans every attached catalog: pin to the ref's catalog
        # (or the session default) so same-named tables elsewhere don't collide.
        with self._write_lock:
            cur = self._cursor()
            try:
                row = cur.execute(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? "
                    "AND table_catalog = coalesce(?, current_database())",
                    [table.schema, table.name, table.catalog],
                ).fetchone()
            finally:
                self._close_cursor(cur)
        return bool(row and row[0])

    def _describe_sync(self, table: TableRef) -> dict[str, str]:
        with self._write_lock:
            cur = self._cursor()
            try:
                rows = cur.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = ? AND table_name = ? "
                    "AND table_catalog = coalesce(?, current_database()) ORDER BY ordinal_position",
                    [table.schema, table.name, table.catalog],
                ).fetchall()
            finally:
                self._close_cursor(cur)
        return dict(rows)
