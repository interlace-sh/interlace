# Core concepts

interlace is a single-process data platform: **transformation** (a dbt/SQLMesh-style
plan/apply over a fingerprinted DAG), **orchestration** (a durable scheduler + run queue),
and **durable streaming ingestion** — one binary, one state store, one warehouse.

## The pieces

- **Model** — one node in the DAG: a SQL file or a Python function that produces a relation.
  See [models](models.md).
- **Snapshot** — an immutable, fingerprinted physical table for one version of a model
  (`interlace__<schema>.<base>__<fingerprint>`, `<base>` = the schema-stripped model name). A
  model's definition change mints a new fingerprint and a new snapshot; the old one survives
  until `gc`. (Owned planes only — terminal `table`/`file` models deliver externally instead.)
- **Environment** — a named set of views over snapshots. `prod` is the unprefixed namespace;
  everything else is a prefixed sandbox. Promotion is an atomic view swap. See
  [environments](environments.md).
- **Plan / apply** — `plan` diffs the compiled project against an environment and previews
  what would change; `apply` builds the changed snapshots, runs checks, and promotes.
- **Strategy** — how a model's query becomes a table (`full`, `merge`, `full_merge`,
  `hash_merge`, `incremental_by_time`, `scd`; `append` for an external `table`). See
  [strategies](strategies.md).
- **Materialise** — *where* the result lands: `virtual`/`view`/`ephemeral` (interlace-owned) or
  `table`/`file` (terminal, external — reverse ETL). See [models](models.md#materialisations).
- **Check** — a data-quality assertion that gates promotion. See [checks](checks.md).
- **Stream** — a durable ingestion endpoint; publishes land in a WAL log and materialise
  exactly-once into a warehouse table. See [streaming](streaming.md).
- **Engine** — where SQL executes: DuckDB/DuckLake by default, Postgres over ADBC, or a
  quack-served remote warehouse. Models can pin an `engine:`. See [engines](engines.md).

## Fingerprints and the plan lifecycle

Every model has a **data fingerprint** — a hash of its canonical SQL (or Python source), its
strategy config, and its upstreams' fingerprints. Anything that could change output changes
the fingerprint, which changes the snapshot table name. `plan` compares each model's compiled
fingerprint against the one promoted in the target environment and classifies the change:

- **breaking** — output data may differ → rebuild; downstream inherits breaking.
- **additive** — only new columns appeared → rebuild; downstream stays non-breaking.
- **clean** — output provably identical → **not rebuilt**; the new snapshot reuses the
  previous physical table and the environment view repoints. **Column pruning** extends this
  to semantic upstream changes: a downstream that provably consumes none of the columns a
  change touched is clean. Both proofs are conservative — any ambiguity rebuilds.

These are the differ's internal labels; the `category` shown in `interlace plan` is only
`breaking` / `non_breaking` / `forward_only` (additive and clean both surface as `non_breaking`
— the rebuild-vs-reuse distinction shows in the plan's Build column).

`apply` then, under a lock so one writer touches the warehouse at a time: builds the changed
snapshots (DAG-scheduled — each model starts when its in-plan ancestors finish, bounded by
`parallelism`), runs each model's checks, and — only if error-severity checks pass — repoints
the environment views and records the new promotion generation. A plan with breaking changes
refuses to apply without `--force`.

## The state store

A SQLite (WAL) database (`.interlace/state.db`) holds everything that isn't warehouse data:
snapshots, the interval ledger, environment pointers + promotion history, the durable run
queue (with leases, retries, cancellation), per-trigger state, the event log, API keys, and
check results. The warehouse (DuckLake by default) holds the actual model tables. The
stream log is a separate SQLite WAL database (`.interlace/streams.db`).

## The three surfaces

The same functionality is reachable three ways, and they're kept in sync:

- **[CLI](cli.md)** — `interlace <command>`; the primary local-development surface.
- **[HTTP API](api.md)** — a Litestar app (`interlace serve`), scoped API-key auth, OpenAPI
  at `/schema/scalar`.
- **[Web UI](ui.md)** — a zero-build SPA served at `/ui`, driven entirely by the HTTP API.

See [surface parity](parity.md) for the exact mapping and what is intentionally
surface-specific.
