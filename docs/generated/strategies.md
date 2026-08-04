# Materialisation strategies

A model's `materialise` and `strategy` config decide **how its query result becomes a
table**. Every strategy is an *AST builder*: given the model's resolved query, the target
table, and the engine's capabilities, it emits a short list of SQL statements that `apply`
runs **atomically** (one transaction, via `execute_all`). No strategy needs the model's
column list — they are all column-agnostic, which is why a model's schema can change
without hand-written migrations (a definition change simply mints a new snapshot table).

The resolver (`strategies/__init__.py::resolve_strategy`) maps config to a strategy:

| `materialise` | `strategy` | Strategy class | Requires |
|---|---|---|---|
| `view` | — | `View` | — |
| `table` | `full` (default) | `FullRefresh` | — |
| `table` | `merge_by_key` | `MergeByKey` | `key` |
| `table` | `full_merge` | `FullMerge` | `key` |
| `table` | `incremental_by_time` | `IncrementalByTime` | `time_column` (+ an interval) |
| `table` | `scd_type_2` | `ScdType2` | `key`, engine `supports_star_exclude` |

`materialise: ephemeral` produces no table at all (see [models](models.md)); a model with
an `export:` block is a **sink** and bypasses strategies entirely (see [reverse ETL](streaming.md#reverse-etl-sinks)).

Row movement is reported per model as `+inserted ~updated -deleted`, derived from each
statement's affected-row count (`row_counts`).

---

## `view` — a virtual table

`CREATE OR REPLACE VIEW target AS <query>`. Nothing is materialised; the view is
re-evaluated on every read. Cheapest to build, always fresh, but pushes compute to read
time. Use for thin projections and passthroughs. Views can't carry history and are never
incremental.

## `full` (FullRefresh) — replace the whole table

The default for `materialise: table`. Rebuilds the entire table from the query each run.

```
CREATE OR REPLACE TABLE target AS <query>
```

On an engine without `CREATE OR REPLACE TABLE` (Postgres, `supports_create_or_replace=false`)
it falls back to:

```
DROP TABLE IF EXISTS target;
CREATE TABLE target AS <query>
```

Simple and correct for any source; rewrites every row every run (on DuckLake that means new
files each run even if nothing changed — prefer `full_merge` when the source is a full
snapshot and you want change-only writes).

## `merge_by_key` (MergeByKey) — keyed upsert

Requires `key`. Upserts the query's rows by key without deleting untouched rows. Statements:

```
CREATE TABLE IF NOT EXISTS target AS (SELECT * FROM (<query>) _s LIMIT 0)   -- ensure shape
DELETE FROM target WHERE <key> IN (SELECT <key> FROM (<query>) _s)          -- clear re-supplied keys
INSERT INTO target SELECT * FROM (<query>)                                   -- insert current rows
```

A re-supplied key's old row is deleted then re-inserted, so it reads as an **update**; keys
present in the target but *absent* from this run are **left untouched** (this is a partial
upsert, not a full sync). Use when each run supplies a slice of changed/new rows (e.g. a
`cursor`-filtered incremental extract). Multi-column keys use a tuple `IN` predicate.

## `full_merge` (FullMerge) — full-state source, minimal diff

Requires `key`. For sources that can only supply the **complete current state** (an API
list endpoint with no updated-since filter, a snapshot export). Treats the query as the
desired state and applies only the difference, so an identical run writes nothing:

```
CREATE TABLE IF NOT EXISTS target AS (SELECT * FROM (<query>) _s LIMIT 0)
DELETE FROM target WHERE <key> IN (fresh keys)        -- old versions of changed rows
DELETE FROM target WHERE <key> NOT IN (source keys)   -- keys deleted upstream
INSERT INTO target SELECT * FROM (fresh rows)          -- new keys + new versions
```

where `fresh = source EXCEPT current` (set difference — `EXCEPT` *is* the row hash, no
column list needed). Because the source is the full state, a key that vanished from it is a
**delete**. Unchanged rows appear in no difference, so they aren't rewritten (no new
DuckLake files). Keys must be non-NULL (a NULL key never compares equal and would churn
every run). Duplicate source rows collapse via `EXCEPT`'s distinct semantics.

`full_merge` vs `merge_by_key`: both are keyed, but `merge_by_key` only touches the keys in
this run (no deletes), while `full_merge` treats the query as the whole world and deletes
what's missing.

## `incremental_by_time` (IncrementalByTime) — windowed rebuild

Requires `time_column`; the scheduler/planner supplies one interval `[start, end)` per run.
Processes exactly that window:

```
CREATE TABLE IF NOT EXISTS target AS (SELECT * FROM (<query>) _s LIMIT 0)
DELETE FROM target WHERE time_column >= start AND time_column < end          -- clear the window
INSERT INTO target SELECT * FROM (<query>) WHERE time_column >= start AND time_column < end
```

Delete-then-reinsert makes reprocessing a window **idempotent**, which is what makes
backfill and restatement safe. The window is driven explicitly:

- **`interlace run`** fills windows the interval ledger doesn't yet cover (catch-up), then
  records them; a second run over the same window is skipped.
- **`interlace restate`** reprocesses a window even if the ledger already covers it.
- `--start`/`--end` set the range; without them the default is the most recent grain window
  (`interval` config, e.g. `1d`). `backfill` config controls the first-build window: `auto`
  (default) derives `[min, max]` of the time column from the source and fills it as one
  interval; `none` keeps only the latest grain; an ISO date pins the start.

The interval ledger lives in the state store, keyed by `(model, fingerprint)`.

## `scd_type_2` (ScdType2) — slowly-changing dimension, history

Requires `key` and an engine with `supports_star_exclude` (DuckDB family / Snowflake /
BigQuery — **not** Postgres). The target carries the query's columns plus `_valid_from` /
`_valid_to` (`_valid_to IS NULL` = the current version). Each run compares the source
against the *open* rows using set difference:

```
CREATE TABLE IF NOT EXISTS target AS
    (SELECT *, now() AS _valid_from, NULL::timestamp AS _valid_to FROM (<query>) _s LIMIT 0)

-- close open rows whose content no longer matches any source row (changed or key deleted):
UPDATE target SET _valid_to = now()
    WHERE _valid_to IS NULL AND <key> IN (
        SELECT <key> FROM ((SELECT * EXCLUDE(_valid_from,_valid_to) FROM target WHERE _valid_to IS NULL)
                           EXCEPT DISTINCT (SELECT * FROM (<query>) _s)) _stale)

-- insert source rows with no exact open match (brand-new keys and new versions):
INSERT INTO target
    SELECT *, now(), NULL::timestamp FROM (
        (SELECT * FROM (<query>) _s) EXCEPT DISTINCT
        (SELECT * EXCLUDE(_valid_from,_valid_to) FROM target WHERE _valid_to IS NULL)) _fresh
```

An unchanged row appears in neither difference, so re-running is a no-op. A changed key gets
its old version *closed* (`_valid_to` stamped) and its new version *inserted* as current —
full history is preserved. The `EXCLUDE(_valid_from, _valid_to)` projection is why the
engine must support star-exclude; on Postgres, `scd_type_2` raises a clear `PlanError`.

### History and definition changes

History lives in the fingerprint's physical table. Under a **stable definition**, data
changes accumulate history across `interlace run`. A **definition change** mints a new
fingerprint and therefore a fresh, empty table (snapshot semantics — the old history stays
on the old table) — *unless* you apply with **`--forward-only`**, which copies the existing
history onto the new version (copy-on-write) so the new logic applies going forward while
history survives; checks still gate before the view moves, and the old table remains the
rollback target until `gc`. This applies to every history-keeping strategy
(`merge_by_key`, `full_merge`, `scd_type_2`, `incremental_by_time`).
