# Engines

An **engine** is where a model's SQL executes and where its table lives. Every model runs on
one engine; the strategy AST is transpiled to that engine's dialect at execution time.

## Types

The DuckDB family and Postgres are fully tested. Spark is tested against a local Spark+Delta
session (with a strategy caveat, below). The cloud warehouses are **alpha**. Snowflake has
been exercised against one account and is not in CI. MotherDuck, Redshift, and BigQuery are
wired and dialect-correct, but have not run against a live account, so treat them as
ready-to-try, not production-blessed.

| `type` | Backed by | Status | Role |
|---|---|---|---|
| `duckdb` (default) | a DuckDB file or `:memory:` | stable | Plain DuckDB — one file, single-process. The simplest warehouse. |
| `ducklake` | DuckDB + the DuckLake extension | stable | Snapshot storage as DuckLake tables over a catalog DB (SQLite or Postgres), data in local files or object storage. Catalog writes are serialised, so `interlace serve` and a separate CLI can share the warehouse concurrently. |
| `motherduck` | MotherDuck (`md:` cloud DuckDB) | alpha | DuckDB dialect over a cloud catalog. Set `database: md:<db>` (token via `motherduck_token`). |
| `quack` | a remote quack-served warehouse (`quack:host:port`) | stable | SQL routed over the quack protocol; Arrow loads stream over an attached catalog. |
| `postgres` | Postgres over ADBC | stable | Strategies execute *inside* Postgres; Arrow in/out via `adbc_ingest`. Needs the `adbc` extra. |
| `spark` | a PySpark `SparkSession` (local or Spark Connect) | beta | SQL runs in Spark; Arrow via `toArrow`/`createDataFrame` (no ADBC). Mutations need a Delta/Iceberg catalog. Needs the `spark` extra. **`scd`/`full_merge` unsupported** (below). |
| `redshift` | Redshift over the Postgres ADBC driver (PG wire) | alpha | Reuses the Postgres transport; Redshift dialect + a native `MERGE`. Needs the `adbc` extra. |
| `snowflake` | Snowflake over ADBC | alpha | Full strategy set (incl. `scd`). Exercised against one account, not in CI. Needs the `adbc-snowflake` extra. |
| `bigquery` | BigQuery over ADBC | alpha | Full strategy set (incl. `scd`). Needs the `adbc-bigquery` extra. |

The default warehouse is a plain DuckDB file (`.interlace/warehouse.duckdb`) — simplest to
start with. Switch to `ducklake:.interlace/warehouse.ducklake` when you need `interlace serve`
and a separate CLI to write the same warehouse concurrently (DuckLake serialises catalog
writes; a plain DuckDB file is single-writer). DuckDB is also the
**federation hub**: everything crosses the Python boundary as Arrow `RecordBatchReader`, and
DuckDB can ATTACH other databases for cross-engine reads. The remote ADBC engines
(`postgres`/`redshift`/`snowflake`/`bigquery`) share one base (`engines/adbc.py`): a new ADBC
backend is a dialect, a capability set, and a `connect`. Spark is its own transport
(`SparkSession`, not ADBC).

## Feature support

Every [strategy](strategies.md) runs on every engine, with two exceptions on Spark
(`scd`/`full_merge`). ✓ = supported · ✗ = not supported.

| Engine | Status | `replace` | `view` | `append` | `merge` | `full_merge` | `incremental` | `scd` |
|---|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| `duckdb` / `ducklake` | stable | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `quack` | stable | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `postgres` | stable | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ ¹ |
| `spark` | beta | ✓ | ✓ | ✓ | ✓ ² | ✗ ³ | ✓ ² | ✗ ³ |
| `motherduck` | alpha | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `redshift` | alpha | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ ¹ |
| `snowflake` | alpha | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `bigquery` | alpha | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

¹ `scd` enumerates the model's columns (no `SELECT * EXCLUDE`), so the model needs an explicit
projection — not `SELECT *`.
² Needs a Delta Lake / Iceberg catalog for row-level `MERGE`/`DELETE`; plain Hive/parquet Spark has neither.
³ Delta rejects subqueries in `UPDATE`/`DELETE` conditions (`DELTA_UNSUPPORTED_SUBQUERY`), which
`scd`'s close and `full_merge`'s delete rely on — they'd need a MERGE-based rewrite.

**Status:** *stable* = tested in CI (and locally). *beta* = tested against a local Spark + Delta
session, with the caveats above. *alpha* = wired and dialect-correct, unit-tested for SQL shape,
but **not yet run against a live account** (no local target). Not built: **Databricks** (its
connector is Arrow-native but has no `adbc_ingest` bulk-load path).

Notes: `replace` and `view` are always available; `append` requires `materialise: table` (an
external table). Every engine above does `merge` with a native `MERGE`; the portable
`DELETE`+`INSERT` fallback only runs when the target's column list isn't known yet (a first
delivery into a fresh table).

## Capabilities

Strategies adapt to capability flags (`EngineCaps`). Defaults are off, except
`supports_mutation_subquery` and `except_in_delete`, which default on and are
cleared where an engine aborts.

| Cap | DuckDB file | DuckLake | Quack | Postgres | Redshift | Snowflake / BigQuery | Spark |
|---|---|---|---|---|---|---|---|
| `supports_create_or_replace` | ✓ | ✓ | ✓ | | | ✓ | |
| `supports_star_exclude` | ✓ | ✓ | ✓ | | | ✓ | |
| `supports_merge` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `supports_transactions` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `supports_mutation_subquery` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `except_in_delete` | ✓ | | ✓ | ✓ | ✓ | ✓ | |
| `supports_attach` | ✓ | ✓ | | | | | |
| `not_null_as_column` | ✓ | ✓ | ✓ | | | | |
| Enforced constraints | NOT NULL | NOT NULL | NOT NULL | PK, unique, NOT NULL, check, FK | NOT NULL | NOT NULL | NOT NULL |

When a flag is off: `replace` emits `DROP` + `CREATE`; `scd` enumerates columns
instead of `SELECT * EXCLUDE` (so the model needs an explicit projection);
`merge` uses `DELETE`+`INSERT`; a stream flush refuses an engine without
transactions;
`scd` and `full_merge` raise at plan time when mutations cannot contain a
subquery; `full_merge` stages its key set when `EXCEPT` cannot sit inside
`DELETE`. `not_null_as_column` means `NOT NULL` is `ALTER COLUMN`, not
`ADD CONSTRAINT`.

`merge` upserts with a native `MERGE` on every engine in the table, falling
back to `DELETE`+`INSERT` when the column list isn't known yet. **`scd` runs
everywhere except Spark** — Postgres and Redshift enumerate the model's own
columns to compare open rows. `replace`, `view`, and `incremental` run on every
engine. `full_merge` runs everywhere except Spark.

## Multi-engine and cross-engine transfers

Declare named engines under `engines:` and pin models with `engine:`. A model's `engine` is
part of its fingerprint, so re-pinning a model to another engine forces a rebuild there (and
`gc` later drops the old table on the old engine).

When a model on engine B depends on a model on engine A, `apply` inserts an explicit
**transfer**: it fetches A's output as Arrow and loads it into a staging table on B, then B's
model reads the stage. Where B is a DuckDB engine and A is attachable, a **federated CTAS**
fast lane (`via: attach`) copies the data with no Python hop; otherwise it's a generic Arrow
`fetch → load`. Transfers are always explicit plan line-items (shown by `plan`), never hidden.
`:memory:` and quack engines are not attachable, so they always use the Arrow lane.

Streams always live on the default warehouse engine.

## Reverse-ETL targets

External databases are wired in with `attach: {alias: uri}`. A terminal model
(`materialise: table, target: alias.schema.table, ...`) then delivers into that
database — see [streaming § reverse ETL](streaming.md#reverse-etl-terminal-table--file).
A DuckDB warehouse `ATTACH`es the URI and writes it in SQL. A warehouse that cannot
`ATTACH` (Postgres, and the other remote engines) fetches the model as Arrow and
applies the same strategy inside that database, so `attach: {ext: external.duckdb}`
still lands in the DuckDB file. `materialise: file` on those engines is written
from Arrow on the host; DuckDB keeps `COPY`.

## Author dialect on Postgres and Redshift

Models stay DuckDB SQL. Rendering to Postgres or Redshift fixes the forms sqlglot
would otherwise emit as illegal SQL: `round(x, n)` casts `x` to `DECIMAL` and the
result back to `DOUBLE` (Postgres has no `round(double precision, integer)`, and an
unconstrained `NUMERIC` reaches Python as an opaque string); `unnest(generate_series(...))` becomes
the set-returning `generate_series` (and DuckDB `range(n)` becomes the same series);
a computed `INTERVAL <expr> <unit>` multiplies `INTERVAL '1' <unit>` instead of
dropping `<expr>`. DuckDB `hash()` becomes a non-negative `hashtextextended` on
Postgres only — a different function, so bucket values will not match DuckDB.
`read_csv_auto` and the other local file scans run in a short-lived DuckDB and
are loaded into the warehouse.

## Spark

`spark` runs canonical ASTs inside a PySpark `SparkSession` (local, or remote via Spark
Connect), moving data as Arrow with `DataFrame.toArrow()` / `SparkSession.createDataFrame` —
no ADBC. `replace`, `append`, `view`, `merge` (native `MERGE`) and `incremental`
(windowed `DELETE` + `INSERT`) are verified against a local **Spark + Delta Lake** session;
the mutating strategies need a Delta or Iceberg catalog (plain Hive/parquet has no row-level
`DELETE`/`MERGE`), configured on the session you hand the adapter.

**`scd` and `full_merge` are not supported on Spark.** Their close/delete conditions use a
subquery (`key IN (SELECT ...)`), and Delta rejects subqueries in `UPDATE`/`DELETE`
conditions (`DELTA_UNSUPPORTED_SUBQUERY`); making them work would need a MERGE-based rewrite of
those strategies. `execute_all` is also not one transaction on Spark (no multi-statement
transactions), and affected-row counts aren't surfaced (reported as 0).

## Alpha engines

`motherduck`, `redshift`, `snowflake` and `bigquery` are wired, dialect-correct, and share the
tested ADBC/DuckDB transport, but none is exercised against a live account in CI (no local
target). SQL generation and capabilities are unit-tested; the connection string and metadata
probes are the parts to confirm against a real account. Redshift is the safest bet — it rides
the same Postgres wire and driver that the test suite already covers. Databricks is not built:
its Python connector is Arrow-native (so the transport would fit) but there's no ADBC bulk-load
(`adbc_ingest`) path, so `load()` needs a bespoke staged-COPY implementation — deferred until a
user needs it.

Named `connections:` (HTTP clients and Postgres DSNs a Python model or a CDC block reads),
`inputs:` (DuckDB file scans), and `cdc:` are not engines. They are project config; see
[configuration](configuration.md).
