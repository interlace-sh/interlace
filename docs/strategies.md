# Materialisation strategies

A model's `materialise` and `strategy` config decide **how its query result becomes a
table**. Every strategy is an *AST builder*: given the model's resolved query, the target
table, and the engine's capabilities, it emits a short list of SQL statements that `apply`
runs **atomically** (one transaction, via `execute_all`). Strategies are column-agnostic —
a model's schema can change without hand-written migrations (a definition change simply
mints a new snapshot table). The one exception is `merge`'s native `MERGE`, which uses the
target's column list (which `apply` already knows on the delivery paths) to build its `SET`
clause, and falls back to a column-agnostic `DELETE`+`INSERT` when that list isn't available.

The resolver (`strategies/__init__.py::resolve_strategy`) maps config to a strategy:

| `materialise` | `strategy` | Strategy class | Requires |
|---|---|---|---|
| `view` | — | `View` | — |
| `virtual` | `replace` (default) | `Replace` (`CREATE OR REPLACE`) | — |
| `virtual` \| `table` | `merge` | `Merge` | `key` |
| `virtual` \| `table` | `full_merge` | `FullMerge` | `key` |
| `virtual` \| `table` | `incremental_by_time` | `IncrementalByTime` | `time_column` (+ an interval) |
| `virtual` \| `table` | `scd` | `Scd` | `key` (explicit projection on engines without `supports_star_exclude`) |
| `table` | `replace` | `ReplaceInPlace` (DELETE all + INSERT — never drops) | — |
| `table` | `append` | `Append` | — |

Strategies are **destination-agnostic**: the accumulating strategies run identically against
the interlace-owned `virtual` table and an external `table`. `replace` is the exception — it
rewrites the owned table (`CREATE OR REPLACE`) but empties an external one in place
(`ReplaceInPlace`), which never drops it. `materialise: ephemeral` produces no table (see
[models](models.md)); `materialise: file` bypasses strategies (overwrite via `COPY`, see
[reverse ETL](streaming.md#reverse-etl-terminal-table--file)).

Row movement is reported per model as `+inserted ~updated -deleted`, derived from each
statement's affected-row count (`row_counts`).

---

## `view` — a virtual table

`CREATE OR REPLACE VIEW target AS <query>`. Nothing is materialised; the view is
re-evaluated on every read. Cheapest to build, always fresh, but pushes compute to read
time. Use for thin projections and passthroughs. Views can't carry history and are never
incremental.

## `replace` — replace the whole table

The default for `materialise: virtual`. Rebuilds the entire table from the query each run
(`Replace`):

```
CREATE OR REPLACE TABLE target AS <query>
```

On an engine without `CREATE OR REPLACE TABLE` (Postgres, `supports_create_or_replace=false`)
it falls back to:

```
DROP TABLE IF EXISTS target;
CREATE TABLE target AS <query>
```

On an external `table` (`materialise: table`), `replace` resolves to **`ReplaceInPlace`** instead —
`DELETE FROM target` + `INSERT`, **never a drop**, so grants and readers on the live table
survive:

```
CREATE TABLE IF NOT EXISTS target AS (SELECT * FROM (<query>) _s LIMIT 0)
DELETE FROM target;                       -- empty in place
INSERT INTO target SELECT * FROM (<query>)
```

Simple and correct for any source; rewrites every row every run (on DuckLake that means new
files each run even if nothing changed — prefer `full_merge` when the source is a full
snapshot and you want change-only writes).

## `append` — insert only (external `table`)

Terminal-only (`materialise: table`). Adds the query's rows to the target, deleting nothing —
a growing log or event table:

```
CREATE TABLE IF NOT EXISTS target AS (SELECT * FROM (<query>) _s LIMIT 0)
INSERT INTO target SELECT * FROM (<query>)
```

## `merge` (Merge) — keyed upsert

Requires `key`. Upserts the query's rows by key without deleting untouched rows. Keys present
in the target but *absent* from this run are **left untouched** (a partial upsert, not a full
sync). Use when each run supplies a slice of changed/new rows (e.g. a `cursor`-filtered
incremental extract). Multi-column keys are supported.

**Native `MERGE`** — when the engine advertises `supports_merge` (DuckDB ≥ 1.3, Postgres ≥ 15)
*and* `apply` knows the target's column list (the delivery paths `describe` it to align the
source), the upsert is a single statement:

```
MERGE INTO target AS _t USING (<query>) AS _s
    ON _t.<key> = _s.<key>
    WHEN MATCHED THEN UPDATE SET <non-key col> = _s.<non-key col>, ...
    WHEN NOT MATCHED THEN INSERT (<cols>) VALUES (_s.<cols>)
```

Matched rows are **updated in place**, so surrogate ids, columns not in the query, and row
identity survive, and the engine fires `UPDATE` triggers (not `DELETE`+`INSERT`). The source
is **not** deduplicated: two source rows matching one target row is a real "your key isn't
unique" bug, and native `MERGE` surfaces it as a cardinality error rather than paying for a
`DISTINCT` on every run. A `MERGE` returns one combined affected-row count, so the native path
reports rows as `+written` without an insert/update split.

**Fallback** — with no column list (a first delivery into a fresh table) or an engine without
`MERGE`, the portable, column-agnostic path runs instead, and keeps the exact insert/update
split (`~` = a re-supplied key, `+` = a new one):

```
CREATE TABLE IF NOT EXISTS target AS (SELECT * FROM (<query>) _s LIMIT 0)   -- ensure shape
DELETE FROM target WHERE <key> IN (SELECT <key> FROM (<query>) _s)          -- clear re-supplied keys
INSERT INTO target SELECT * FROM (<query>)                                   -- insert current rows
```

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

`full_merge` vs `merge`: both are keyed, but `merge` only touches the keys in
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

## `scd` (Scd) — slowly-changing dimension, history

Requires `key`. Runs on **every engine**: on DuckDB-family/Snowflake/BigQuery it projects open
rows with `SELECT * EXCLUDE(...)`; on engines without that (Postgres, Redshift) it enumerates
the model's own columns instead — so an `scd` model there needs an explicit projection, not
`SELECT *`. The target carries the query's columns plus `_valid_from` / `_valid_to`
(`_valid_to IS NULL` = the current version). Each run compares the source against the *open*
rows using set difference:

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
full history is preserved. The key may be composite (a tuple `IN` predicate). The
`EXCLUDE(_valid_from, _valid_to)` projection shown above is used where the engine supports it;
on engines without it (Postgres, Redshift) the strategy enumerates the model's columns instead,
so `scd` runs there too — it just needs an explicit projection rather than `SELECT *`.

### Event-time windows (`time_column`)

By default the windows are stamped with **processing time** (`CURRENT_TIMESTAMP`). Pass a
`time_column` — an event timestamp carried in the source — and the windows follow the data
instead:

- a new version's `_valid_from` is its own event time (`CAST(<time_column> AS TIMESTAMP)`);
- the version it supersedes is closed at *that same* event time (found by joining the open
  rows to the fresh set on `key`), so the windows **abut on when the change actually
  happened** rather than when interlace saw it;
- a key that **vanished** upstream has no succeeding event, so it is still closed at
  processing time.

This adds a second close statement (changed keys via the event-time join, vanished keys at
`now()`) but keeps the re-run-is-a-no-op property: the event time is part of the row, so an
unchanged row still matches and nothing moves.

### History and definition changes

History lives in the fingerprint's physical table. Under a **stable definition**, data
changes accumulate history across `interlace run`. A **definition change** mints a new
fingerprint and therefore a fresh, empty table (snapshot semantics — the old history stays
on the old table) — *unless* you apply with **`--forward-only`**, which copies the existing
history onto the new version (copy-on-write) so the new logic applies going forward while
history survives; checks still gate before the view moves, and the old table remains the
rollback target until `gc`. This applies to every history-keeping strategy
(`merge`, `full_merge`, `scd`, `incremental_by_time`).
