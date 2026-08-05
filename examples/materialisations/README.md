# materialisations — every plane × strategy

A reference project that exercises **every materialisation and strategy** interlace
supports. `materialise` is the *destination / ownership plane*; `strategy` is *how the
result is written*. They compose.

```bash
interlace apply --path examples/materialisations           # build + deliver everything
duckdb examples/materialisations/external.duckdb            # inspect the reverse-ETL tables
ls   examples/materialisations/out/                         # the file exports
```

## The matrix

| Model | `materialise` | `strategy` | What it demonstrates |
|---|---|---|---|
| `seed` | `ephemeral` | — | inlined as a CTE; no table built |
| `customers` | `virtual` | `full` | owned snapshot, rebuilt whole (defaults) |
| `customers_view` | `view` | — | a VIEW over the query, fronted by an env view |
| `accounts_merge` | `virtual` | `merge` | keyed upsert into the owned table |
| `accounts_full_merge` | `virtual` | `full_merge` | full-state diff (changed + vanished keys) |
| `events_incremental` | `virtual` | `incremental_by_time` | windowed delete+insert, ledger-tracked |
| `customer_history` | `virtual` | `scd` | keyed history with `_valid_from`/`_valid_to` |
| `crm_replace` | `table` | `full` | reverse ETL: DELETE all + INSERT, never drops |
| `crm_append` | `table` | `append` | reverse ETL: add rows only (opted into `dev` too) |
| `crm_upsert` | `table` | `merge` | reverse ETL: keyed upsert into an external table |
| `crm_full_merge` | `table` | `full_merge` | reverse ETL: full-state diff into an external table |
| `crm_incremental` | `table` | `incremental_by_time` | **windowed delivery into an external table** |
| `export_parquet` | `file` | — | overwrite a Parquet file via `COPY` |
| `export_csv` | `file` | — | overwrite a CSV file (with header) |
| `export_json` | `file` | — | overwrite a newline-delimited JSON file |

## The two planes

- **virtual / view / ephemeral** — interlace *owns* the target: a content-addressed
  snapshot table (`interlace__<schema>.<base>__<fp>`) read through an environment view.
  This is what gives rebuild-skip, sandboxed environments, atomic view-swap promotion,
  rollback, and gc.
- **table / file** — a *terminal* destination interlace does **not** own. It delivers in
  place (external table) or overwrites (file), produces no environment view, is
  environment-gated (default: `prod` only — widen with `environments: [dev, prod]`), and
  evolves the destination additively but **never drops** it.

Strategies are destination-agnostic: `merge`, `full_merge`, `incremental_by_time`
and `scd` run identically on a `virtual` table or an external `table`. Only `full`
differs by ownership — `CREATE OR REPLACE` on the owned table, DELETE-all + INSERT on the
external one. `append` is terminal-only; `view` is virtual-only.
