# Multi-engine

interlace runs models on **named engines**. One project can hold a local DuckLake warehouse and
remote warehouses side by side, with each model pinned to the engine that should execute it.
This page is the contract; the tier table below says what exists today.

## The three tiers

| Tier | Meaning | Status |
|---|---|---|
| **T0 — federation hub** | Models run on the (DuckDB-family) default engine; external databases are reachable through `attach:` for reads and table exports (reverse ETL) | Shipped |
| **T1 — native remote engines** | A model's strategy executes *inside* Postgres/Snowflake/… — no DuckDB middleman | Shipped for Postgres (ADBC); further adapters per engine |
| **T2 — cross-engine plans** | The planner inserts explicit `TransferEdge`s when a model's inputs live on another engine | Shipped (Arrow path) — see Transfers below |

`attach:` and `engines:` are different things: an attachment is a *catalog visible to one
DuckDB-family engine* (federated reads, sink targets); an engine is a *place interlace executes
strategies and owns snapshots*.

## Config

```yaml
# interlace.yaml
name: analytics
default_engine: default        # optional; "default" if omitted

# Top-level warehouse fields ARE the `default` engine (single-engine projects
# never need an engines: block):
database: ducklake:.interlace/warehouse.ducklake

engines:
  analytics:
    type: duckdb               # duckdb | ducklake | quack | postgres
    database: analytics.duckdb
  pg:
    type: postgres
    database: ${PG_DSN}        # ${VAR} interpolation; unresolved vars fail at open
```

Engine types without an adapter fail at open with a pointer here. Adapters today: the DuckDB
family (`duckdb`/`ducklake`/`quack`/`motherduck`), the ADBC engines (`postgres`, plus the alpha
`redshift`/`snowflake`/`bigquery`, which share one `AdbcAdapter` base), and `spark` (beta, a
`SparkSession` transport — `scd`/`full_merge` unsupported on Delta). Engines open lazily — a
declared-but-unused remote engine is never contacted.

## Binding models

```sql
/* interlace: {engine: analytics, strategy: merge, key: id} */
SELECT ...
```

```python
@model(engine="pg", strategy="replace")
def dim_customers(...): ...
```

Unset → the project's `default_engine`. The model's dialect defaults from its engine.

## Semantics

- **The engine is part of the data fingerprint.** Moving a model between engines is a BREAKING
  change: it rebuilds on the new engine, and `interlace gc` later drops the old physical table on
  the old engine (snapshots record their owning engine).
- **Cross-engine dependencies transfer explicitly.** When a model's upstream lives on another
  engine, the plan carries a `TransferEdge` (rendered by `interlace plan` and the API) and apply
  moves the upstream as Arrow — `source.fetch → target.load` — into
  `interlace__xfer.<upstream>` on the consumer's engine, replaced on every apply so re-run
  upstreams are never read stale. One transfer per (upstream, target engine) per apply; the
  staged override is scoped to the cross-engine consumer, so same-engine readers keep the
  original. Ephemeral models still must share their consumers' engine (they inline as CTEs).
  **Fast lane:** when the target is DuckDB-family and the source is attachable (a file-backed
  DuckDB/DuckLake not currently held open, or a Postgres DSN), the transfer upgrades to one
  federated `ATTACH → CTAS → DETACH` — no Python hop; any failure falls back to Arrow. The plan
  line reports which lane ran. `interlace gc` sweeps `interlace__xfer` staging (scratch — the
  next apply that needs it re-stages).
- **Environments span engines; views don't.** `dev__main.report` is created on the engine that
  owns `report`. Promote repoints views per engine; the environment mapping itself lives in the
  control-plane state store as always.
- **Streams stay on the default engine.** Ingestion, `streams.<name>` tables, and watermarks are
  a default-warehouse concern; streaming into remote engines is a product decision for later.
- **The control plane never moves.** Snapshots/intervals/queue/events remain in SQLite (or
  Postgres) regardless of how many warehouses execute models.

## Hard rules (invariants)

1. Strategies emit canonical ASTs only; dialect appears exactly once, at `adapter.transpile()`.
2. No silent cross-engine data movement — a transfer is always a visible plan line-item.
3. Same-engine SQL models stay logical: one CTAS/MERGE inside that engine, zero rows in Python.
4. DuckDB is never an obligatory middleman once a model's inputs and target share a remote engine.
5. Arrow (`RecordBatchReader`) is the only Python hop, for transfers and Python models alike.

## Postgres (first native remote engine)

```yaml
engines:
  pg:
    type: postgres
    database: ${PG_DSN}          # postgresql://user:pass@host:5432/db
```

Requires the ``adbc`` extra (`pip install 'interlaced[adbc]'`). Strategies transpile to the
postgres dialect and execute over one ADBC connection; Arrow flows both ways (`fetch` streams
results, `adbc_ingest` bulk-loads Python model output). Checks, env views, contracts, and GC all
work natively there.

### Capability flags (`EngineCaps`, drive strategy fallbacks)

`EngineCaps` carries three flags; each strategy consults them and picks a portable path when
a capability is absent:

| Cap | DuckDB family / Snowflake / BigQuery | Postgres / Redshift |
|---|---|---|
| `supports_create_or_replace` | ✓ | ✗ → `Replace` emits DROP + CREATE |
| `supports_star_exclude` | ✓ | ✗ → `scd` enumerates the model's columns instead of `SELECT * EXCLUDE` |
| `supports_merge` | ✓ | ✓ → `merge` uses a native `MERGE` (else DELETE+INSERT) |

Everything else is portable by construction: Arrow ingest is `register` on DuckDB and
`adbc_ingest` on the ADBC engines, and cross-engine `ATTACH` is a DuckDB-only fast lane the
transfer planner opportunistically uses (falling back to Arrow fetch→load).

Every strategy runs on every SQL engine — `merge` upserts natively where `MERGE` exists, and
`scd` enumerates its columns where `SELECT * EXCLUDE` is missing (so on Postgres/Redshift an
`scd` model needs an explicit projection, not `SELECT *`). The one exception is **Spark**,
where `scd`/`full_merge` don't work: Delta forbids subqueries in `UPDATE`/`DELETE` conditions,
which those strategies' close/delete steps rely on (`merge`, `incremental`, `replace`,
`append` do run on Spark).

## Roadmap

1. Second cloud warehouse (Snowflake or BigQuery — driven by a named user)
2. Author-dialect ≠ run-dialect polish; native MERGE where caps allow
