"""DuckDB engine adapter — the default local engine and federation hub.

Everything crosses the boundary as Arrow: :meth:`fetch` streams results as a
``pyarrow.RecordBatchReader`` (zero-copy, single pass) and :meth:`load` registers
an Arrow reader and writes it with one ``CREATE TABLE AS`` / ``INSERT``. Blocking
DuckDB calls run in a worker thread; each call uses its own ``cursor()`` so reads
proceed concurrently (DuckDB MVCC), while the DAG guarantees no two tasks write
the same table at once.

On **DuckLake** connections every cursor — reads included — is serialised on
``_write_lock``. DuckLake's catalog layer is not safe against sibling cursors of
one DatabaseInstance: under parallel builds a ``CREATE TABLE <schema>.<name>``
intermittently commits with an empty schema name and lands in the catalog's
default schema (``main``), silently. Serialising writes alone does not stop it;
a read cursor open across the commit is enough. Reads are materialised inside
the lock so that cursor is closed first. On an attached catalog the CREATE is
rewritten to ``catalog.schema.table`` so the schema is part of the name, not
resolved through the default schema. If the table still is not in the schema the
statement named, the transaction is rolled back and retried — it is not kept
and copied out of ``main``.

Plain DuckDB catalogs don't have that bug, and the DAG already guarantees no
two tasks write the same table — so there the "lock" is a no-op context and
builds genuinely run in parallel (measured ~4x on 4 concurrent CTAS). A
transaction conflict on genuinely contended catalog objects still surfaces as
``TransactionException`` and is retried where the batch is idempotent.
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import threading
from collections.abc import Callable, Iterator, Sequence
from dataclasses import replace
from uuid import uuid4

import duckdb
import pyarrow as pa
import sqlglot
import tenacity
from sqlglot import exp

from interlace.engines.base import EngineAdapter, EngineCaps, LoadMode, note_statement, relation_is_absent
from interlace.exceptions import ConfigurationError
from interlace.ir.relation import TableRef

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


class CatalogPlacementError(Exception):
    """DuckLake committed a CREATE under a schema other than the one the statement named.

    A plain Exception, not an :class:`~interlace.exceptions.InterlaceError`: the scheduler
    wraps those as ``model '…' failed: …`` and does not record the snapshot.
    """


def _qualified(schema: str, name: str) -> str:
    return f"{exp.to_identifier(schema).sql(dialect='duckdb')}.{exp.to_identifier(name).sql(dialect='duckdb')}"


def _created_tables(sql: str) -> list[tuple[str, str, str]]:
    """``(schema, name, kind)`` for a CREATE TABLE/VIEW that names a schema. Empty when the
    statement is not one, or names no schema — an unqualified create cannot be told
    apart from the DuckLake bug that drops the schema."""
    try:
        expression = sqlglot.parse_one(sql, read="duckdb")
    except Exception:
        return []
    if not isinstance(expression, exp.Create):
        return []
    kind = str(expression.args.get("kind") or "").upper()
    if kind not in {"TABLE", "VIEW"}:
        return []
    target = expression.this
    if isinstance(target, exp.Schema):
        target = target.this
    if not isinstance(target, exp.Table) or not target.db or not target.name:
        return []
    return [(target.db, target.name, kind)]


def _placed_schemas(cur: duckdb.DuckDBPyConnection, name: str, *, kind: str) -> set[str]:
    """Schemas in the current catalog that hold this table, or this view — not both.

    Environment views share a short name (``main.a`` and ``dev__main.a``). Mixing the
    two types made a view promotion look like the DuckLake bug and drop the other view.
    """
    table_type = "VIEW" if kind == "VIEW" else "BASE TABLE"
    rows = cur.execute(
        "SELECT table_schema FROM information_schema.tables "
        "WHERE table_name = ? AND table_type = ? AND table_catalog = current_database()",
        [name, table_type],
    ).fetchall()
    return {str(row[0]) for row in rows if row and row[0]}


def _drop_elsewhere(cur: duckdb.DuckDBPyConnection, schema: str, name: str, *, kind: str) -> None:
    """Drop a table copy in ``main`` left by an autocommitted CREATE. A transaction
    rolls back instead. Views are never dropped: another environment's view may share
    the name."""
    if kind != "TABLE":
        return
    for placed in _placed_schemas(cur, name, kind=kind):
        if placed.casefold() == "main" and placed.casefold() != schema.casefold():
            cur.execute(f"DROP TABLE IF EXISTS {_qualified(placed, name)}")


def _ensure_placed(cur: duckdb.DuckDBPyConnection, schema: str, name: str, *, kind: str = "TABLE") -> None:
    """Reject a CREATE whose schema name did not survive.

    DuckLake sometimes records the new table under ``main`` (an empty schema name in
    the commit). That CREATE is wrong — copying the rows across would publish a table
    that was born without its schema. The caller rolls the statement back and retries
    it. A leftover *table* already in ``main`` beside one that did land in the named
    schema is the previous occurrence of this bug and is dropped. Views are left alone.
    """
    if not schema or schema.casefold() == "main":
        return
    found = _placed_schemas(cur, name, kind=kind)
    if any(placed.casefold() == schema.casefold() for placed in found):
        if kind == "TABLE":
            for placed in found:
                if placed.casefold() == "main":
                    cur.execute(f"DROP TABLE IF EXISTS {_qualified(placed, name)}")
        return
    if not found:
        return
    where = ", ".join(sorted(found))
    raise CatalogPlacementError(f"DuckLake created {schema}.{name} in schema {where} with an empty schema name")


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
        self.caps = _DUCKLAKE_CAPS if serialise_writes else _DUCKDB_CAPS
        # One cursor at a time on DuckLake (see module docstring); a no-op context
        # elsewhere so builds run in parallel. Plain Lock, not RLock: no locked
        # path calls another, and a plain Lock turns an accidental nesting into an
        # obvious deadlock rather than silent re-entry.
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

    def attach(self, alias: str, uri: str) -> None:
        """ATTACH another database (duckdb/sqlite/postgres/... URI) under ``alias``."""
        escaped = uri.replace("'", "''")
        with _clean_lock_error(uri):  # attaching a held duckdb/ducklake file conflicts just like opening one
            self._conn.execute(f"ATTACH IF NOT EXISTS '{escaped}' AS {exp.to_identifier(alias).sql('duckdb')}")
        self._attached.append(alias)
        if uri.startswith("ducklake:"):  # writes may now reach a DuckLake catalog (e.g. table sinks)
            self.caps = _DUCKLAKE_CAPS
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

    async def fetch(self, ast: exp.Expr) -> pa.RecordBatchReader:
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

    def _pin_create_catalog(self, sql: str) -> str:
        """Write an attached DuckLake CREATE as ``catalog.schema.table``.

        A two-part ``schema.table`` is resolved in the session's default schema when
        DuckLake drops the qualifier. The catalog alias makes the schema part of the
        name DuckLake commits.
        """
        alias = self._catalog_alias
        if not alias:
            return sql
        try:
            expression = sqlglot.parse_one(sql, read="duckdb")
        except Exception:
            return sql
        if not isinstance(expression, exp.Create):
            return sql
        target = expression.this
        if isinstance(target, exp.Schema):
            target = target.this
        if not isinstance(target, exp.Table) or not target.db or target.catalog:
            return sql
        target.set("catalog", exp.to_identifier(alias))
        return expression.sql(dialect="duckdb")

    def _guard_creates(self, cur: duckdb.DuckDBPyConnection, sql: str) -> None:
        for schema, name, kind in _created_tables(sql):
            _ensure_placed(cur, schema, name, kind=kind)

    def _execute_once(self, sql: str) -> None:
        cur = self._cursor()
        try:
            cur.execute(sql)
            self._guard_creates(cur, sql)
        except CatalogPlacementError:
            # Autocommit: the empty-schema table is already stored. Drop it so the
            # retry creates the named schema, and so a failed retry leaves nothing in main.
            for schema, name, kind in _created_tables(sql):
                _drop_elsewhere(cur, schema, name, kind=kind)
            raise
        except Exception as exc:
            note_statement(exc, sql)
            raise
        finally:
            cur.close()

    @_commit_retry
    def _execute_sync(self, sql: str) -> None:
        sql = self._pin_create_catalog(sql)
        with self._write_lock:  # may be DDL (create_schema / create_view / migrations)
            last: CatalogPlacementError | None = None
            for _attempt in range(3):
                try:
                    self._execute_once(sql)
                    return
                except CatalogPlacementError as exc:
                    note_statement(exc, sql)
                    last = exc
            assert last is not None
            raise last

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
                    self._guard_creates(cur, sql)
                except Exception as exc:
                    note_statement(exc, sql)
                    raise
            cur.execute("COMMIT")
        except Exception:
            cur.execute("ROLLBACK")
            raise
        finally:
            cur.close()
        return counts

    @_commit_retry
    def _execute_all_sync(self, sqls: list[str]) -> list[int]:
        sqls = [self._pin_create_catalog(sql) for sql in sqls]
        with self._write_lock:  # a strategy's CREATE / DELETE / INSERT / DROP batch
            last: CatalogPlacementError | None = None
            for _attempt in range(3):
                try:
                    return self._execute_all_once(sqls)
                except CatalogPlacementError as exc:
                    last = exc
            assert last is not None
            raise last

    def _fetch_sync(self, sql: str) -> pa.RecordBatchReader:
        # Plain DuckDB streams: the cursor must outlive the reader, and is closed
        # when that stream ends rather than left for the GC. DuckLake cannot: a
        # cursor left open across another task's CREATE is what drops the schema,
        # so the scan is materialised inside the lock and the cursor closed first.
        if isinstance(self._write_lock, contextlib.nullcontext):
            return self._stream_sync(sql)
        with self._write_lock:
            cur = self._cursor()
            try:
                cur.execute(sql)
                table = cur.to_arrow_reader().read_all()
            finally:
                cur.close()
        return table.to_reader()

    def _stream_sync(self, sql: str) -> pa.RecordBatchReader:
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
                    sql = self._pin_create_catalog(f"CREATE OR REPLACE TABLE {target} AS SELECT * FROM {src}")
                    cur.execute(sql)
                    # Row count before the placement probe: that probe runs its own statements.
                    written = _affected(cur)
                    try:
                        self._guard_creates(cur, sql)
                    except CatalogPlacementError:
                        for schema, name, kind in _created_tables(sql):
                            _drop_elsewhere(cur, schema, name, kind=kind)
                        raise
                    return written
                cur.execute(f"INSERT INTO {target} SELECT * FROM {src}")
                return _affected(cur)
            finally:
                cur.unregister(src)
                cur.close()

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
                cur.close()
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
                cur.close()
        return dict(rows)
