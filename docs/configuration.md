# Configuration — `interlace.yaml`

A project is a directory with an `interlace.yaml` (all keys optional — a project works with
no config file at all) and a `models/` directory. `${VAR}` references anywhere in the YAML
are substituted before parsing, from the process environment first, then a `.env` file next
to the config — so DSNs and secrets never need to be committed. An unset variable is left
literal so it surfaces as an obvious `${VAR}` in errors.

## Project keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | str | `interlace` | Project name (also the default DuckLake catalog alias). |
| `database` | str | `ducklake:.interlace/warehouse.ducklake` | The default warehouse DSN — see [engines](engines.md). |
| `default_dialect` | str | `duckdb` | sqlglot dialect models are authored in unless they set `dialect:`. |
| `default_engine` | str | `default` | Which named engine models build on unless they set `engine:`. |
| `engines` | map | `{}` | Named engines `{name: EngineConfig}` for multi-engine projects. The top-level `database`/`data_path`/etc. synthesize the `default` engine. |
| `model_paths` | list | `["models"]` | Directories scanned for `*.sql` and `@model` `*.py`. |
| `parallelism` | int (≥1) | `4` | Max models built concurrently by `apply`/`run`. |
| `state_path` | str | `.interlace/state.db` | SQLite control-plane database. |
| `stream_path` | str | `.interlace/streams.db` | Durable stream log (SQLite WAL). |
| `attach` | map | `{}` | `{alias: uri}` — external databases wired into the warehouse (reverse-ETL targets, cross-engine ATTACH). |
| `alias`, `data_path`, `metadata_schema`, `secrets`, `quack_token` | — | Warehouse-engine options mirrored from `EngineConfig` (below) for the default engine. |

## `EngineConfig` (per named engine, and the default)

| Key | Type | Default | Meaning |
|---|---|---|---|
| `type` | str | `ducklake` | `ducklake` \| `duckdb` \| `quack` \| `postgres`. |
| `database` | str | — | DSN. DuckLake catalog (`ducklake:...`), a `.duckdb` file, `:memory:`, `quack:host:port`, or a Postgres DSN. |
| `dialect` | str | from `type` | sqlglot dialect (defaults to `duckdb` for the DuckDB family). |
| `alias` | str | project/name | DuckLake catalog attach alias. |
| `data_path` | str | — | Object-store or local path for DuckLake Parquet data (e.g. `s3://bucket/...`). |
| `metadata_schema` | str | — | Schema in the DuckLake catalog DB holding this warehouse's `ducklake_*` metadata. |
| `secrets` | map | `{}` | `{name: SecretConfig}` — object-store credentials (see below). |
| `quack_token` | str | — | Auth token for a `quack:` database (or `INTERLACE_QUACK_TOKEN`). |
| `attach` | map | `{}` | Databases attached into this engine at open. |

## `SecretConfig` (object-store credentials)

| Key | Default | Meaning |
|---|---|---|
| `type` | `s3` | Secret type. |
| `key_id`, `secret` | `""` | Access key / secret. |
| `endpoint` | — | `host[:port]`, no scheme (None = AWS default). |
| `region` | — | Region. |
| `url_style` | — | `path` for MinIO/RustFS-style endpoints. |
| `use_ssl` | — | TLS on/off. |
| `scope` | — | Pin the secret to one prefix, e.g. `s3://bucket`. |

## Example

```yaml
name: analytics
database: "ducklake:postgres:${WAREHOUSE_CATALOG_DSN}"
data_path: "s3://my-bucket/warehouse"
secrets:
  store: {type: s3, key_id: "${S3_KEY}", secret: "${S3_SECRET}", region: eu-west-1}
parallelism: 8

engines:
  reporting:
    type: postgres
    database: "${REPORTING_DSN}"      # postgresql://user@host:5432/db

attach:
  crm: "postgresql://etl@crm.internal:5432/crm"   # a reverse-ETL (materialise: table) target
```

Models then pin `engine: reporting` to build in Postgres, or deliver into
`crm.<schema>.<table>` with `materialise: table, target: crm.<schema>.<table>`.
Anything that dials a database must name its host explicitly — a Postgres DSN without a host
is rejected (libpq would silently default to a local socket).
