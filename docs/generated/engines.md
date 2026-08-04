# Engines

An **engine** is where a model's SQL executes and where its table lives. Every model runs on
one engine; the strategy AST is transpiled to that engine's dialect at execution time.
interlace ships four engine types.

## Types

| `type` | Backed by | Role |
|---|---|---|
| `ducklake` (default) | DuckDB + the DuckLake extension | Snapshot storage as DuckLake tables over a catalog DB (SQLite or Postgres) with data in local files or object storage. Catalog writes are serialised (DuckLake isn't safe against concurrent DDL on sibling cursors). |
| `duckdb` | a DuckDB file or `:memory:` | Plain DuckDB. Builds run genuinely in parallel (no catalog-write lock). |
| `quack` | a remote quack-served warehouse (`quack:host:port`) | SQL is routed to the remote over the quack protocol; Arrow loads stream over an attached catalog. Needs a `quack_token`. |
| `postgres` | Postgres over ADBC | A native remote engine. Strategies execute *inside* Postgres; Arrow in/out via `adbc_ingest`. Needs the `adbc` extra. |

The default warehouse is `ducklake:.interlace/warehouse.ducklake`. DuckDB is also the
**federation hub**: everything crosses the Python boundary as Arrow `RecordBatchReader`, and
DuckDB can ATTACH other databases for cross-engine reads.

## Capabilities

Strategies adapt to two capability flags (`EngineCaps`):

| Cap | DuckDB family | Postgres | Effect when absent |
|---|---|---|---|
| `supports_create_or_replace` | ✓ | ✗ | `full` falls back to `DROP` + `CREATE TABLE AS`. |
| `supports_star_exclude` | ✓ | ✗ | `scd_type_2` is refused (it needs `SELECT * EXCLUDE(...)`). |

Everything else is portable by construction: keyed strategies use `DELETE`+`INSERT` (not a
native `MERGE`), so `merge_by_key`, `full_merge`, and `incremental_by_time` run on Postgres
too; `full` and `view` run everywhere. `scd_type_2` is DuckDB-family only.

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

External databases are wired in with `attach: {alias: uri}`. A sink model's
`export: {to: table, target: alias.schema.table, ...}` then delivers into that attached
database (Postgres, SQLite, another DuckDB) — see [streaming § reverse ETL](streaming.md#reverse-etl-sinks).

## Not yet built

Snowflake and BigQuery adapters are designed-for but not implemented — see the roadmap in
`docs/architecture/architecture.md`.
