# Engines

An **engine** is where a model's SQL executes and where its table lives. Every model runs on
one engine; the strategy AST is transpiled to that engine's dialect at execution time.

## Types

The DuckDB family and Postgres are fully tested. Spark is tested against a local Spark+Delta
session (with a strategy caveat, below). The cloud warehouses are **alpha** — wired and
dialect-correct, but not yet exercised against a live account (there's no local target to test
them on), so treat them as ready-to-try, not production-blessed.

| `type` | Backed by | Status | Role |
|---|---|---|---|
| `ducklake` (default) | DuckDB + the DuckLake extension | stable | Snapshot storage as DuckLake tables over a catalog DB (SQLite or Postgres), data in local files or object storage. Catalog writes are serialised. |
| `duckdb` | a DuckDB file or `:memory:` | stable | Plain DuckDB. Builds run genuinely in parallel (no catalog-write lock). |
| `motherduck` | MotherDuck (`md:` cloud DuckDB) | alpha | DuckDB dialect over a cloud catalog. Set `database: md:<db>` (token via `motherduck_token`). |
| `quack` | a remote quack-served warehouse (`quack:host:port`) | stable | SQL routed over the quack protocol; Arrow loads stream over an attached catalog. |
| `postgres` | Postgres over ADBC | stable | Strategies execute *inside* Postgres; Arrow in/out via `adbc_ingest`. Needs the `adbc` extra. |
| `spark` | a PySpark `SparkSession` (local or Spark Connect) | beta | SQL runs in Spark; Arrow via `toArrow`/`createDataFrame` (no ADBC). Mutations need a Delta/Iceberg catalog. Needs the `spark` extra. **`scd`/`full_merge` unsupported** (below). |
| `redshift` | Redshift over the Postgres ADBC driver (PG wire) | alpha | Reuses the Postgres transport; Redshift dialect + a native `MERGE`. Needs the `adbc` extra. |
| `snowflake` | Snowflake over ADBC | alpha | Full strategy set (incl. `scd`). Needs the `adbc-snowflake` extra. |
| `bigquery` | BigQuery over ADBC | alpha | Full strategy set (incl. `scd`). Needs the `adbc-bigquery` extra. |

The default warehouse is `ducklake:.interlace/warehouse.ducklake`. DuckDB is also the
**federation hub**: everything crosses the Python boundary as Arrow `RecordBatchReader`, and
DuckDB can ATTACH other databases for cross-engine reads. The remote ADBC engines
(`postgres`/`redshift`/`snowflake`/`bigquery`) share one base (`engines/adbc.py`): a new ADBC
backend is a dialect, a capability set, and a `connect`. Spark is its own transport
(`SparkSession`, not ADBC).

## Feature support

Every [strategy](strategies.md) runs on every engine, with two exceptions on Spark
(`scd`/`full_merge`). ✓ = supported · ✗ = not supported.

| Engine | Status | `replace` | `view` | `append` | `merge` | `full_merge` | `incremental_by_time` | `scd` |
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

Strategies adapt to capability flags (`EngineCaps`):

| Cap | DuckDB family / Snowflake / BigQuery | Postgres / Redshift | Effect when absent |
|---|---|---|---|
| `supports_create_or_replace` | ✓ | ✗ | `replace` falls back to `DROP` + `CREATE TABLE AS`. |
| `supports_star_exclude` | ✓ | ✗ | `scd` enumerates the model's columns instead of `SELECT * EXCLUDE(...)` (so a `scd` model needs an explicit projection, not `SELECT *`). |
| `supports_merge` | ✓ | ✓ | `merge` uses a portable `DELETE`+`INSERT` instead of a native `MERGE`. |

Everything is portable by construction. `merge` upserts with a native `MERGE` where available
(DuckDB, Postgres, Redshift, Snowflake, BigQuery), falling back to `DELETE`+`INSERT` when the
column list isn't known or the engine lacks `MERGE`. **`scd` now runs everywhere** — engines
without `SELECT * EXCLUDE` (Postgres, Redshift) enumerate the model's own columns to compare
open rows, so history tracking is no longer DuckDB-only; it just needs an explicit projection.
`replace`, `view`, `full_merge` and `incremental_by_time` run on every engine.

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
(`materialise: table, target: alias.schema.table, ...`) then delivers into that attached
database (Postgres, SQLite, another DuckDB) — see [streaming § reverse ETL](streaming.md#reverse-etl-terminal-table--file).

## Spark

`spark` runs canonical ASTs inside a PySpark `SparkSession` (local, or remote via Spark
Connect), moving data as Arrow with `DataFrame.toArrow()` / `SparkSession.createDataFrame` —
no ADBC. `replace`, `append`, `view`, `merge` (native `MERGE`) and `incremental_by_time`
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
