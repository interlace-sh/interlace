# Changelog

## Unreleased

**Reset: a fresh start that does not touch tables you own.** `interlace reset --yes`,
`POST /reset`, and **reset…** on the System page wipe Interlace-owned state — environment
views, snapshot tables, runs, events, and the stream log — so the next `apply` rebuilds
from scratch. `materialise: table` / `file` destinations are not dropped, and those models
stay recorded so the next apply will not re-deliver into them. API keys are kept. `--dry-run`
previews; the UI requires typing `reset`.

**UI live updates are SSE-only.** The in-package UI dropped its `GET /events` polling fallback
and the 30-second badge poll. Keyed browsers already pass `?token=` on `/events/stream`
because EventSource cannot set `Authorization`. `GET /events` remains the snapshot/replay API.
The stream now sends a comment frame on connect (so EventSource opens on a quiet daemon) and
15s keepalives.

## 2.4.1 (2026-08-15)

**SQL macros.** `macros/*.sql` holds `CREATE MACRO` definitions, and any model can call them:

```sql
-- macros/money.sql
CREATE MACRO cents_to_dollars(amount) AS (amount / 100)::numeric(16, 2);
```

The call is expanded into the model's AST while it compiles — before the fingerprint, before
lineage, before transpilation — and that ordering is the point. Editing a macro re-plans every
model that calls it, because the expansion is part of the canonical SQL the fingerprint covers;
a macro created in the warehouse instead would be invisible to it, leaving callers stale with
nothing to notice. And one definition covers every engine: dbt writes `default__`, `postgres__`
and `bigquery__` variants because Jinja renders text, while an expanded AST is transpiled like
everything else, so Postgres gets its integer-division fix
(`CAST(amount AS DOUBLE PRECISION) / NULLIF(100, 0)`) from the same line. Scalar expressions
only; macros may call macros; recursion is a compile error. Configured with `macro_paths`
(default `["macros"]`). See [models](docs/models.md#macros).

**`interlace run` reports how long it took.** `Ran 19 model(s) (19 task(s)); promoted 19 to
'prod'.` is now `Ran 19 model(s) in 1.23s; promoted 19 to 'prod'.` — the task count restated the
model count in the common case, and wall-clock is what you were timing.

**Example: `jaffle-shop`.** dbt's *current* demo project
([`dbt-labs/jaffle-shop`](https://github.com/dbt-labs/jaffle-shop)) converted: 19 models, 27
checks, and its six raw tables read straight from dbt's repo over HTTP rather than vendored.
It covers what the classic project does not — `source()`, a project macro, a `dbt_utils`
package macro, `dbt_utils.expression_is_true` (which is the built-in `expression` check), and
MetricFlow, which has no equivalent. Two interlace fixes came out of building it, below.

**Fix: a model can import a helper module sitting next to it.** Discovery skips `.py` files
whose names start with `_` so they can be shared helpers — the closest thing to a dbt macro —
but the model's directory was never on `sys.path`, so `from _macros import ...` raised
`ModuleNotFoundError`. The directory is now importable for the duration of that model's import,
and the project's own modules are dropped from the import cache afterwards, so a second project
with its own `_macros.py` gets its own (a reload under `interlace serve` would otherwise reuse
the first).

**Fix: a check that reads a sibling model no longer runs before it is built.** `relationships`
(and `sql`) checks add scheduling edges, and an edge was kept only when the target happened to
sort earlier in the topological order — so a check against an independent sibling that sorted
later was dropped, and then failed with `Catalog Error: Table ... does not exist`. Edges are now
dropped only when they would actually close a cycle (a check pointing at a model built *from*
the model being checked), which is the case that would hang.

**Example: `jaffle-shop-classic`.** dbt's original demo project
([`jaffle-shop-classic`](https://github.com/dbt-labs/jaffle-shop-classic)) converted and shipped
as a reference project: seeds as ordinary models over `read_csv_auto`, `schema.yml`'s twenty
tests as promotion-gating checks, and the one Jinja-templated model (`{% for %}` over four
payment methods, twice) as a dynamic model that generates the same SQL from a Python list.
`interlace apply --env prod` builds 8 models and passes 20/20. Accompanies the
[migration walkthrough](https://interlace.sh/blog/migrating-jaffle-shop).

## 2.4.0 (2026-08-14)

**Breaking: the default warehouse is now plain DuckDB, not DuckLake.** `database` defaults to
`.interlace/warehouse.duckdb` (and `EngineConfig.type` to `duckdb`) — the simplest start: one
file, single-process, no catalog. DuckLake stays a first-class option (`database: ducklake:…`),
and is the one to pick when `interlace serve` and a separate CLI must write the same warehouse
concurrently (DuckLake serialises catalog writes; a plain DuckDB file is single-writer). On
upgrade, a project with no explicit `database:` will open a fresh empty `.duckdb` and ignore its
old `.ducklake` warehouse — set `database: ducklake:.interlace/warehouse.ducklake` to keep it.

**Fix: delivering into a table you share with another writer no longer wipes its other
columns.** Staged delivery aligned the model's output to the *whole* target — NULL-filling
every column the model didn't produce — and then wrote all of them, so a `merge` into a table
where another system owns some columns reset those columns to NULL on every run (`hash_merge`
did the same through its `SET` list). The keyed upserts and `append` are now handed only the
columns the model actually produces: a matched row keeps the rest, and an inserted row takes
the target's `DEFAULT`s instead of a NULL (which also fixes inserting into a `NOT NULL DEFAULT`
column). The flip side, on a table interlace owns: these strategies no longer clear a column
the model has *stopped* producing — it keeps its last value rather than being NULLed (moot for
`virtual` models, where dropping a column mints a fresh table anyway). `merge`'s portable
fallback is now `UPDATE` + `INSERT` rather than `DELETE` + `INSERT`, so it preserves those
columns too on an engine without native `MERGE` — though every shipped adapter has `MERGE`, so
that path stays a safety net. Whole-row strategies (`replace`, `full_merge`, `scd`) are
unchanged — they rewrite rows entire by design — but `apply` now warns, naming the columns it
will reset. `incremental` is unaffected either way: a windowed delivery never stages, so it
still requires the model to produce the target's full column set. And `hash_merge`/`scd`
pointed at a pre-existing external table that lacks their bookkeeping columns (`_hash`,
`_valid_from`/`_valid_to`) now fail with a clear error naming the missing column instead of a
raw engine binder error. See [strategies](docs/strategies.md#shared-destinations-columns-interlace-doesnt-own).

**Fix (first-run blocker): `interlace init` crashed after a `pip install`.** Templates are real
projects, so they ship `.py` model files — and pip byte-compiles every `.py` in a wheel at install
time, leaving `__pycache__/*.pyc` inside the installed template tree. `init` copied that tree file
by file with `read_text()`, so the first `.pyc` it reached raised
`UnicodeDecodeError: 'utf-8' codec can't decode byte 0xcb in position 0` — the 3.12 `.pyc` magic.
Every template was affected, and it landed on the second command in the README. Install artefacts
are now skipped, and a file that is not valid UTF-8 is copied byte for byte instead of decoded, so
a template may carry a binary fixture. (uv does not byte-compile by default, which is why this
only ever reproduced through the documented `pip install` path.)

**Fix: a locked warehouse reads like an error, not a traceback.** Running any CLI command while
`interlace serve` holds the warehouse — the obvious thing to try, since `interlace query` is the
console's CLI counterpart — dumped a dozen frames ending in `duckdb.IOException`, naming neither
the cause nor the fix. It is now one `error:` line that names the holding PID and points at
`--quack`, the documented way to share one warehouse across processes. It covers `attach:`
targets as well as the warehouse itself, and matches DuckDB's bare lock message too, so it
reads the same on macOS and Windows (only Linux names the holding process). Other
`IOException`s keep their traceback.

**Fix: `ModelDef(checks=…)` takes the same dict shorthand as `@model(checks=…)`.** It stored the
dicts raw and failed later at compile with `AttributeError: 'dict' object has no attribute
'type'`. That is the dynamic-model path — exactly what generated models and dbt migrations use —
so the "one spelling for both surfaces" promise only half-landed. Normalising now happens in
`ModelDef`, so a malformed check fails at declaration instead.

**Fix: a model's relative read path resolves against the project root.** `read_csv_auto('seeds/x.csv')`
is documented to resolve from the directory holding `interlace.yaml`, but DuckDB resolves against
the process CWD — the same thing only when you happen to run from the root. Under `--path`,
`interlace serve`, or the scheduler it failed with `IO Error: No files found that match the
pattern`. The warehouse connection now searches the project root as well as the CWD, so both
resolve. Reads only; `COPY` targets already resolved against the root.

**Fix: `scd` no longer corrupts its validity columns when the model grows a column.** Its insert
bound positionally, assuming `_valid_from`/`_valid_to` are the last two columns — true for a table
scd created in one shot, false after an additive `ALTER`, which appends the new column *after*
them. The new column's value then landed in `_valid_from` (a conversion error if the types
disagree, silent corruption if they don't). The insert now names its columns.

**Docs: seeds.** `docs/models.md` gains a "Seeds and static files" section — there is no `seed`
model type because a seed is a model over `read_csv_auto`. It covers the gotcha that a
fingerprint tracks the canonical SQL and never the file's bytes, so editing a CSV plans as "no
changes"; `interlace run --select <model>+` is the rebuild (`apply --force` is not — and mind
the trailing `+`, or every downstream model keeps the old data).

**Docs: the DuckLake default flip landed everywhere.** The README, `docs/concepts.md` and the
benchmark example still described DuckLake as the default warehouse. They now describe the
plain DuckDB file, and the benchmark — whose published timings were measured on DuckLake —
says so, with the one `database:` line needed to reproduce them.

**Docs: platforms.** The README now states plainly that Linux is what CI runs, and that macOS
and Windows are expected to work but untested, rather than saying nothing.

## 2.3.0 (2026-08-08)

**Breaking: `incremental_by_time` is now `incremental`, and it takes an optional `key`.**
The old name raises a migration error naming the replacement; there is no alias. Without a
`key` the behaviour is unchanged — `DELETE` the window, `INSERT` the window — so the period is
rewritten and a row that leaves the source disappears. That delete-then-reinsert is what keeps
reprocessing idempotent, and backfill and `restate` safe.

With a `key` the window stops being authoritative and only bounds *what is read*: rows are
upserted by key (native `MERGE` where the engine supports it, portable `DELETE`+`INSERT`
otherwise), so a target row inside the window that the source no longer produces is left alone.
That is the mode for late-arriving corrections to already-published rows.

**Python models can use `incremental` when they declare a `key`.** The Arrow output is staged
and the window's rows are upserted into it, with the interval ledger filling exactly as it does
for SQL. Unkeyed is refused on purpose: a SQL model has the window predicate pushed into its
query so the engine computes only that window, whereas a Python function has already run in full
by the time the window could be applied — the unkeyed form would look incremental while doing
all the work every run. Bound the fetch with `cursor` instead.

**Fix: the UI's security headers.** ASGI types a response message's `headers` as an iterable,
not a list, so appending to it was only sound if the server happened to hand over a list. The
wrapper builds a new list now.

**Dependencies.** sqlglot 28 → 29, rich 14 → 15, pyarrow 23 → 25, and the dev tooling (mypy 2,
pytest 9.1, ruff 0.16, black 26.5). sqlglot 30 is deliberately still out of range: it changes
`Expression`/`Expr` typing enough to produce 27 type errors across 12 files, which is a
migration rather than a bump.

## 2.2.0 (2026-08-07)

(Supersedes the never-released 2.1.1 — its fixes ship here.)

**New: `hash_merge` strategy — a change-detected keyed upsert.** Like `merge` (a keyed
upsert that keeps rows absent from the source) but it stores an `_hash` (md5 of the non-key
columns) and writes only the delta — new key inserts, changed hash updates, unchanged rows
skipped. Idempotent (identical data writes nothing) with counts that split cleanly into
`+inserted` / `~updated`, where `merge`'s native `MERGE` rewrites every matched row each run
and reports one lumped count. See `docs/strategies.md`.

**Fix: ephemeral models aren't counted in "promoted N".** An ephemeral model is inlined and
has no promotable table, so a project with one (e.g. the benchmark) no longer reads
"Ran 12 model(s); promoted 13" — the count matches the build rows.

**Improved: promoting existing logic to a second environment is a view-swap, not a rebuild.**
Snapshots are content-addressed and shared across environments, but a fresh environment
still rebuilt every model. `apply` now recognises a fingerprint already materialised by a
prior apply (typically in another environment) and reuses the shared table — recording the
snapshot, running its checks, and swapping the environment's view — instead of recomputing.
Scoped to virtual/view models (terminals always deliver); checks still gate promotion; falls
back to a real build if the table is gone (in-memory warehouse, gc).

**Fix: `interlace init --list` shows each template's extra and full description.** The
templates now declare `requires_env`, so the Needs column lists the extra
(service/sources/postgres) instead of "—"; and the descriptions no longer lose their
`[sources]`-style bracketed text, which Rich had been eating as markup.

**Fix: `interlace serve` picks up model edits without a restart.** The daemon compiled the
project once at startup, so editing a model and pressing Plan/Apply in the UI showed nothing
new — only a restart (or `interlace plan` in a fresh process) did. It now recompiles on demand
when a model file changes on disk (a cheap mtime probe; the graph, lineage and stream→consumer
map re-derive only when something changed). Changing engine/stream/path topology in
`interlace.yaml` still needs a restart.

**Fix: enqueued runs start immediately, not on the next tick.** A run enqueued from the UI/API
waited out the scheduler interval (up to `--interval`, 60s) before a worker picked it up. An
enqueue now wakes the drain at once; the interval remains the periodic fallback for schedules.

**UI: run detail redesign.** The expanded run is one compact table — a status tick (✓/✗/⊘) plus
model / output / strategy / engine / depends on / **checks** / rows / time — instead of a
build-results table *and* a separate tick timeline. The runs list gains **env** and overall
**duration** columns and drops the usually-empty *window* column (the backfill window now shows in
the detail header only when a run had one).

**UI: query console — a runnable starter.** Clicking a table into an empty editor inserts
`select * from <ref>`; mid-query it still drops just the ref at the cursor.

**API: CLI↔API↔UI parity.** `POST /apply` returns `checks` + `gated`; `GET /runs` and `/checks`
take `?limit=`; `GET /lineage` takes `?environment=`.

**Fix (2.1.0 regression): `gc` reclaims `materialise: view` snapshots.** A view model's
physical snapshot is a view (`CREATE OR REPLACE VIEW`), but `gc` dropped every superseded
snapshot by trying `DROP TABLE` first — and `DROP TABLE` on a view *raises* rather than
no-opping, so `gc` aborted before the `DROP VIEW` could run and reclaimed nothing. Any
project with view models could never `gc`. It now drops each object by its actual catalog
kind (`DROP VIEW` / `DROP TABLE`).

**UI: brand refresh.** The `interlace serve` web UI adopts the interlace.sh woven mark
(favicon + topbar) and the `interlace.sh` wordmark with a muted `.sh`.

**UI: activity timeline.** The overview feed now groups the event stream into expandable
build *episodes* — one per apply or run — each a per-model timeline (start → done/failed with
durations) instead of a flat list; the run detail renders the same timeline. Ad-hoc applies
(which aren't queued runs) surface in this feed.

## 2.1.0 (2026-08-07)

**New: source models — ingestion by pull (`interlace.sources`).** A small synchronous REST
client behind the `interlaced[sources]` extra — auth (bearer / API-key / basic), pagination
(page / offset / cursor / RFC-5988 link header), retry with jittered backoff that honours
`Retry-After`, and rate limiting — that turns an API into Arrow. A *source* is an ordinary
`@model` that pulls and yields batches; incremental via the reserved `cursor` param, idempotent
via `merge`. See `docs/sources.md`.

**New: init templates (`interlace init --template NAME` / `--list`).** `interlace init` is now a
registry over runnable starter projects, each shipped in the wheel with a README that doubles as
its landing page: `quickstart` (default, no-source SQL → Python → SQL), `github` (incremental
REST pull), `postgres` (incremental DB pull via psycopg, with a seeded docker-compose), and
`events` (durable `@stream` ingestion + live rollups — formerly the `event_stream` example).

**New: `interlace query "SELECT …"`.** A read-only query command — the CLI counterpart of the web
console, sharing one parse-and-fence path (SELECT-only; table functions and file/HTTP readers
rejected).

**Improved: the web UI.** A correctness / robustness / accessibility / security / performance pass
over the in-package UI — fixed the "failing checks" over-count and dead stream-column links,
keyboard-operable modals and navigation, a same-origin Content-Security-Policy + `nosniff` /
frame headers scoped to `/ui`, gzip compression, and lazy-loaded views.

**Fix: column lineage traces through Python models.** A Python model (no SQL to qualify) no
longer dead-ends the whole downstream subtree — the differ, UI and `interlace lineage`/`impact`
resolve columns through it — and a `count(*)` is no longer misread as a row-expanding `SELECT *`.

**Fix: adding an aggregate column to a rollup is non-breaking.** The additive-change class now
fires for aggregate models (same `count(*)`-as-star cause), so `apply` no longer gates a plain
column addition behind `--force`. A dimension add that changes the row set stays breaking.

**Fix: errors read like errors.** A model that fails to build — or a typo in a model file — now
surfaces as one `error:` line naming the model/file, not a raw traceback. `CheckSpec` is exported
from the package root, and a Python `@model(checks=…)` accepts the same dict shorthand as a SQL
config block.

## 2.0.3 (2026-08-06)

**Fix (correctness + security): the query console no longer disables warehouse writes.**
The read-only SQL console sandboxed untrusted queries by setting DuckDB's
`enable_external_access = false` on the shared warehouse connection. That setting is
**instance-wide and one-way**, so the first console query permanently disabled the
warehouse's own file writes — the stream flusher, `apply` and exports all started failing
("file system operations are disabled"), and publishes then returned 429 forever. The fence
now sits at parse time: a console `SELECT` may read tables and vetted row generators
(`range` / `generate_series`) only — every table function (`read_csv`, `read_parquet`,
`query`, `query_table`, `glob`, and unnamed or future ones) is rejected structurally, with a
file/network function backstop. No engine latch, so writes are never affected. (An
engine-level lockdown isn't possible here: a DuckLake catalog is held by one connection per
process, so the console necessarily shares the writer's.)

**Fix: `interlace serve` shuts down cleanly on Ctrl+C.** An open SSE stream (`/events/stream`,
the UI's live feed) blocked uvicorn's graceful shutdown until it timed out and force-cancelled
the held-open connection, dumping a `CancelledError` traceback ("Cancel 1 running task(s),
timeout graceful shutdown exceeded"). The daemon now ends open SSE streams the instant
shutdown begins, so the drain finds the connections already closed — Ctrl+C is immediate and
quiet.

**New example: `event_stream`** — durable ingestion, end to end. A `@stream` endpoint, the
exactly-once micro-batch materializer, backpressure, and live rollups over a moving stream,
with a standard-library load generator that fires events in parallel batches (a million per
burst, `--loop` for a million a minute).

## 2.0.2 (2026-08-06)

Docs and examples — no code changes.

- The `benchmark` example now exercises **every strategy** in one DAG: added `scd` (Type 2
  history), `full_merge` (composite key) and `append` (reverse ETL into an attached DuckDB),
  alongside the existing `replace` / `incremental_by_time` / `merge` / `view` / `file`.
- Install docs lead with `pip install interlaced` (interlace is CLI-first — `interlace init`
  scaffolds a project before one exists), with `uv tool install` for an isolated CLI and
  `uv add` for adding interlace as a project library; `interlace serve` still needs the
  `service` extra. `docs/engines.md` and the site carry the engine × strategy support matrix.

## 2.0.1 (2026-08-06)

**Breaking, despite the patch number.** Strategies carry short, plain names: `replace` (was
`full`), `merge` (was `merge_by_key`), `scd` (was `scd_type_2`). `full_merge`, `append` and
`incremental_by_time` are unchanged. There are no aliases — the old names are errors. This
landed after the 2.0.0 tag, so 2.0.0 on PyPI still uses the long names.

**`merge` upserts with a native `MERGE`** on engines that support it (DuckDB ≥ 1.3, Postgres
≥ 15) when the target's column list is known — rows update in place, so surrogate ids,
out-of-query columns and row identity survive, and `UPDATE` triggers fire. Falls back to the
portable `DELETE`+`INSERT` (which keeps the exact insert/update split) otherwise. The source is
not deduplicated: a duplicate key surfaces the engine's cardinality error.

**`scd` takes an optional `time_column`** — validity windows then use the event timestamp (a
new version's `_valid_from` and the closed version's `_valid_to` abut on the event time)
instead of processing time. Keys may be composite. `scd` no longer needs `SELECT * EXCLUDE`:
on engines without it, the model's own columns are enumerated instead, so history tracking
works there too.

**Engines.** Spark adapter (beta, SparkSession transport, tested on local Spark + Delta).
Alpha ADBC adapters for Redshift, Snowflake, BigQuery and MotherDuck — dialect-correct but not
yet validated against a live account. DuckDB-family, `quack` and `postgres` remain the tested
engines. `docs/engines.md` gains an engine × strategy support matrix.

## 2.0.0 (2026-08-05)

**Breaking: materialisation reframe.** `materialise` is now the destination/ownership plane,
and the old `export:` block is gone. Two planes:

- **owned** — `virtual` (was `table`; **now the default**), `view`, `ephemeral`. Full snapshot
  machinery: rebuild-skip, sandboxes, view-swap promotion, rollback, gc, forward-only.
- **terminal** — `table` (**new meaning**: an external `target: <alias>.<schema>.<table>`) and
  `file` (`path:` + `format:`). No snapshot table, no environment view; environment-gated;
  additive schema evolution only, never dropped.

Strategies now apply across both planes. `full` rewrites an owned table (`CREATE OR REPLACE`)
but replaces an external one in place (DELETE all + INSERT, never drops); new `append` strategy
(external `table` only); `incremental_by_time` now works **into an external table** (windowed
DELETE + INSERT), which the old `export:` sink could not do.

**Migration.**

- `materialise: table` (the old owned snapshot) → `materialise: virtual`, or drop it, since
  `virtual` is the default. A bare `materialise: table` without a `target:` now fails loudly
  ("did you mean materialise: virtual?").
- `export: {to: table, target: T, mode: M, key: K}` → `materialise: table, target: T,
  strategy: M, key: K`.
- `export: {to: parquet|csv|json, path: P}` → `materialise: file, format: <fmt>, path: P`.
- A lingering `export:` key or `export=` kwarg raises a migration error naming its replacement.

The API field `ModelInfo`/`ModelDetail.is_sink` is renamed `is_terminal`. `exports.py` is
removed (helpers moved to `sinks.py`); the delivery mode lives on `strategy`, not
`export.mode`.

## 1.0.2 (2026-08-03)

**Security.** The SQL query console (`POST /query`) could read arbitrary local files — and
reach the network on httpfs/S3 deployments — via DuckDB's `query()`/`query_table()` dynamic-SQL
functions, which the name-based deny-list did not match. The console now runs with external
access disabled at the engine level, closing every spelling of the escape hatch. `GET /engines`
and `interlace engines` no longer leak credentials for keyword-form or query-string DSNs
(redaction was URL-only). `interlace serve` on a non-loopback host with no API keys now warns
that the API is open.

**Correctness.** Every scheduled or stream run recorded a redundant promotion generation,
breaking rollback's default target and growing the history table unbounded; `apply`'s
check-edge cycle handling could let a downstream build before its upstream; the 1.0.1 Postgres
streaming fetch deadlocked multi-input Python models (reverted to materialised fetch); rollback
wrongly aborted on a since-deleted ephemeral; `state:modified` failed in `checks run`; the
stream backpressure gauge could be defeated by a mid-flush publish; `GET /models` misreported
engine and language; and the standalone `interlace scheduler` never flushed streams. Promotion
history is now capped by `trim_logs`.

**Cleanup.** Removed dead code — never-raised exceptions, the unrealised `SqlRelation` "logical
plane" and `ir/schema`, unused strategy and decorator fields, the `scd2` alias, the hidden
`list` CLI alias. `__version__` now reads package metadata. The architecture doc is renamed
`architecture.md`, with its roadmap-versus-shipped split corrected.

## 1.0.1 (2026-07-31)

**Rollback.** Every promote records the environment's full mapping as a promotion-history
generation; `interlace env rollback [--to N] [--list]` (and `POST
/environments/{name}/rollback`) repoints an environment's views at any earlier generation —
nothing rebuilds. The UI's environments view gained a history modal.

**CI selection.** `state:modified` selects models whose fingerprint drifted from the target
environment (transitive; affixes compose, as in `state:modified+`); an empty match is a clean
no-op. `interlace impact <model.column>` reports the column-level blast radius, with Python and
`*` consumers called out as opaque.

**Durability.** The stream log now runs `synchronous=FULL`, so "200-OK means fsynced" is
literally true — surviving power loss, not just process crash. Batched publishes amortise the
fsync.

**Performance.** `apply` schedules the true DAG: each model starts when its last in-plan
ancestor finishes, with no level barriers. Postgres fetch streams via ADBC instead of
materialising, so large cross-engine transfers no longer spike RSS. The stream flusher only
touches streams that received a publish.

Also: unscoped runs retire deleted models like `apply` does; default incremental windows are
complete and grain-aligned; incremental models backfill automatically on first build; the
worker logs run lifecycle and `apply` logs per-model failures.

## 1.0.0 (2026-07-31)

First stable release of the rebuilt platform, published to PyPI as `interlaced` (import and
CLI: `interlace`).

**Transformation.** SQL files and Arrow-native Python functions compile to a fingerprinted DAG
over a sqlglot IR. Terraform-style `plan` / `apply` with a breaking-change gate; virtual
environments as views over immutable snapshot tables, production being the unprefixed
namespace; column-pruned rebuild skipping, so a semantic change invalidates only consumers of
the touched columns; `--forward-only` copy-on-write for the history-keeping strategies (`full`,
`merge_by_key`, `full_merge`, `scd_type_2`, `incremental_by_time` with an interval ledger).
Data-quality checks gate promotion.

**Orchestration.** Built-in cron and interval scheduler over a durable run queue with leases,
retries and cooperative cancellation; interval-aware backfill (`run` catches up, `restate`
reprocesses).

**Streaming.** Durable ingestion log with fsync-before-ack and idempotency keys; exactly-once
micro-batch materialisation via an in-warehouse watermark; schema drift modes (reject / evolve
/ quarantine); retention; 429 backpressure.

**Multi-engine.** DuckDB + DuckLake by default, Postgres natively over ADBC, per-model
`engine:` pinning with explicit cross-engine Arrow transfers and an ATTACH fast lane where
possible. Reverse-ETL sinks, environment-gated to production by default.

**One daemon.** `interlace serve` runs the HTTP API (Litestar, scoped API keys), the scheduler,
stream ingestion and a zero-build web UI at `/ui` — lineage canvas with column-level tracing,
live build feedback over SSE, plan/apply, runs, a read-only query console, checks, environments
and system administration.

## Before 1.0

The 0.x line (0.1.0 through 0.2.0, February 2026) was a different codebase, built on ibis with
pandas at every model boundary and in-memory queues for streaming. It was never published and
shares no code with the platform above; three reviews found structural defects that could not
be patched incrementally, and it was replaced rather than refactored. The reasoning is in
`docs/architecture/architecture.md`; the source is on the `v0` branch and its release notes are
in this file's git history.
