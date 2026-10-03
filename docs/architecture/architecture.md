# Interlace — Architecture & Design

*Design of the current platform. A **Roadmap** section (§14) lists what is designed
but not yet built, ranked Now / Next / Later. When this document says "we do X",
read it as the shipped behaviour unless a note says otherwise.*

**Status:** Implemented, single-node. **Scope:** clean-slate design of the whole
platform.

**Goal.** One MIT-licensed process that replaces the stack people assemble from a
transformation tool (dbt, SQLMesh), an orchestrator (Airflow, Prefect), and an
ingestion service (dlt, Hevo, Cloudflare Pipelines). SQL and Python models share one
fingerprinted DAG. The daemon schedules that DAG, and a trigger refreshes the model
and everything downstream of it. Events fsync into a log and land in the same
warehouse the models read. The control plane stays on one machine until a named user
hits that ceiling. Store, queue, and log are Protocols so a later Postgres tier does
not require a redesign (§12, §14). That swap is not shipped.

**Not the goal.** A semantic layer, a package hub, a general-purpose task runner, a
connector marketplace, or a hosted multi-tenant ELT product. Ideas from other tools
are kept only when they deepen this one process or close a trust gap. Everything else
waits, or is rejected outright (§14).

---

## 1. Core thesis

> **The canonical IR is a sqlglot AST + Arrow schema. The canonical wire format is an
> Arrow `RecordBatchReader`. Materialisation happens exactly once, at the sink, as a
> single native SQL statement executed inside the owning engine.**

- **sqlglot** parses, qualifies, type-annotates, transpiles, diffs, and traces
  column lineage. `exp.Expr` is the universal node; `Expression` is a constructible
  subclass; `Query` / `Condition` are parallel traits (a `Select` is both).
  `.subquery()` lives on `Query`. `DROP` names its target in `tables=` — construct
  it with `interlace.ir.relation.drop`.
- **Arrow** is the only interchange format. pandas is an optional extra, not a
  core type. Remote engines connect via ADBC.

An **SQL model** never leaves the logical plane: a model selecting from three upstreams
in the same engine compiles to *one* `CREATE TABLE AS` (or `MERGE INTO`, via the keyed
strategies) executed inside that engine — zero rows enter the Python process. A
**Python model** is the physical escape hatch: it receives its upstreams as Arrow and
returns Arrow (§3).

---

## 2. Core abstractions

### 2.1 `Relation` — what a model produces

An SQL model is a `SqlRelation` — a sqlglot `Query` tagged with the engine that can
evaluate it natively. A Python model produces Arrow record batches. The schema is
always known — declared or inferred.

### 2.2 `EngineAdapter` — the only place dialect-specific code lives

```python
class EngineAdapter(ABC):
    dialect: str                             # sqlglot dialect name
    caps: EngineCaps                         # feature flags for strategy fallbacks

    async def execute(self, ast: exp.Expr) -> None: ...
    async def fetch(self, ast) -> pa.RecordBatchReader: ...        # extract: engine → Arrow
    async def load(self, table, reader, mode: Literal["create","append"]) -> int: ...  # Arrow → engine
    async def create_view(self, name, target) -> None: ...
    async def create_schema(self, name) -> None: ...
    async def describe(self, table) -> dict[str, str]: ...

    def transpile(self, ast) -> str:
        return render_sql(ast, self.dialect)  # canonical AST → engine SQL
```

`EngineCaps` carries the flags strategies and delivery branch on. The three that
rewrite SQL are `supports_create_or_replace`, `supports_star_exclude` (absent, `scd`
enumerates columns instead of `SELECT * EXCLUDE`), and `supports_merge` (absent,
`merge` falls back to `DELETE`+`INSERT`). The rest decide transactions, which
constraints are real, how `NOT NULL` is altered, whether a mutation may contain a
subquery, whether `EXCEPT` may sit inside `DELETE`, and whether the engine can
`ATTACH` another database. All default off except the mutation and `EXCEPT` flags,
which default on and are turned off where an engine aborts (DuckLake, Spark). The
per-engine table is in `docs/engines.md`.

### 2.3 `Snapshot` — versioned model state

```python
@dataclass(frozen=True)
class Snapshot:
    name: str                  # "silver.orders"
    fingerprint: str           # h(canonical_ast + strategy_config + sorted(upstream_fingerprints))
    metadata_hash: str         # comments/owner/tags — changes here never trigger rebuilds
    physical_table: TableRef   # "interlace__silver.orders__a1b2c3d4"
    intervals: IntervalSet     # which [start, end) ranges are filled
    change_category: ChangeCategory  # BREAKING | NON_BREAKING | METADATA | FORWARD_ONLY
```

SQL models fingerprint over their canonical (normalised, comment-free) AST plus their
strategy config plus their upstreams' fingerprints. **Python models fingerprint over
their dedented function source** (`textwrap.dedent(inspect.getsource(fn))`) plus the
same strategy config and upstreams. A literal written in the signature is already in
that source. A factory default (`tenant=tenant`) and every closure cell are hashed
too, so two models generated from one function body get different fingerprints and
editing the captured value replans. Plain data is hashed in full; a client or other
object is hashed by type only, because its `repr` can contain a memory address.
Bytecode is not hashed. (A `volatility`/`--forward-only` escape hatch for pure
refactors is a possible future refinement, not a current feature.)

### 2.4 `Plan` — terraform-style change preview

A plan carries the model changes (added / breaking / non-breaking / removed), the
backfill tasks (column-impact-narrowed, §6), the explicit cross-engine transfer edges
(never silent), and the virtual view swaps that promote is about to perform.

### 2.5 `Strategy` — AST builders, never strings

```python
class Strategy(ABC):
    def plan_statements(self, rel, target, caps: EngineCaps,
                        interval: Interval | None = None) -> list[exp.Expr]:
        """Return canonical-dialect ASTs. The adapter transpiles. NEVER returns strings."""
```

Built-ins: `replace`, `view`, `ephemeral` (AST-spliced as a CTE into consumers at compile
time), `incremental` (interval predicate injected as an AST filter),
`merge`, `full_merge`, and `hash_merge` (keyed upserts built from `exp` constructors;
`hash_merge` writes only the hash delta), and
`scd` (update-expire + insert-new sequence). Strategies build canonical ASTs and
consult `EngineCaps` for the fallbacks they actually need; the adapter transpiles.

### 2.6 `Trigger` — scheduling

```python
class Trigger(Protocol):
    id: str
    def due(self, now: datetime, last_fired: datetime | None) -> list[RunRequest]

@dataclass(frozen=True)
class RunRequest:
    flow_selector: list[str]
    partition: Interval | None = None
    priority: int = 0
    idempotency_key: str = ""   # e.g. "cron:daily_sales:2026-06-24T00:00:00" — dedupes refires
```

A cron or interval trigger is pure: given the current time and when it last fired,
it returns the runs now due. **Five ticking implementations ship: `CronTrigger`
(parsed by `cronsim`), `IntervalTrigger`, `WatchTrigger`** (a glob hashed from path,
size, and mtime on the same tick — no directory watcher), **`OnChangeTrigger`**
(`schedule: {on_change: column}` reads `max(column)` from the one table the model
reads, or from `table.column` / `schema.table.column`, and enqueues when that value
changes), **and `FreshTrigger`** (`schedule: {fresh: "updated_at 2h"}`, or
`{fresh: {column: updated_at, within: 2h}}`, enqueues when that maximum is older
than the window; an empty table counts, a missing table waits, and a source that
stays stale fires once per window). Each keys its `RunRequest` so a crash between
enqueue and the last-fired write re-lands on the same idempotency key and the
durable queue dedupes instead of double-running. An inbound `{webhook: name}` is
not a tick: `POST /hooks/{name}` enqueues that model. `{after: raw}` (or a list of
model names) is not a tick either: when that model reaches `model.done` — an apply,
a run, or a drained queue item — the waiting model and its descendants are enqueued.
A cycle in `after` is rejected. Stream arrival does *not* go
through a trigger: a flush enqueues the stream's downstream consumers directly (§9).

---

## 3. Python models

A Python model's function parameters name its upstream models; each is passed a lazy
`RelationHandle` streaming that upstream's physical table as Arrow. The handle exposes
exactly two accessors:

```python
@model(strategy="merge", key="order_id")  # materialise defaults to virtual
def enriched_orders(raw_orders, fx_rates, cursor=None, this=None):
    for batch in raw_orders.reader():      # bounded-memory single-pass Arrow batches
        yield transform(batch)
    # or, eager convenience:  df_table = raw_orders.table()   # whole upstream as one pa.Table
```

- **Handles are Arrow-only:** `handle.reader()` streams `RecordBatch`es (bounded
  memory); `handle.table()` reads the whole upstream into a `pa.Table`. Each handle is
  single-pass — call one, once. There is no `.polars()` / `.pandas()` accessor; those
  frames are opt-in extras a user pulls from `handle.table()` themselves.
- **Return value:** a `pyarrow.Table`, `RecordBatchReader`, `RecordBatch`, or an
  iterable/generator of `RecordBatch` (generators stream with bounded memory). The sink
  loads it via `adapter.load()` — directly for `replace`, or via a stage table for keyed
  strategies. Returning a sqlglot expression is **not** supported: to stay on the
  logical plane, write an SQL model. Sync functions run in a worker thread
  (`asyncio.to_thread`); async functions run on the event loop.
- **Reserved incremental parameters:** `cursor` receives the max of the model's declared
  `cursor` column in its previous materialisation (`None` on first build), derived from
  the warehouse so it can never drift from committed data; `this` receives a
  `RelationHandle` over the previous materialisation, for anti-join backfills. Neither
  names an upstream.

Python models run on the default execution lane (a worker thread per task). There is no
per-model process-pool opt-in and no `executor=` argument today (§8, §14).

---

## 4. DuckDB's two roles; DuckLake as opt-in storage

**Role 1 — the default local engine.** Physical storage defaults to a **plain DuckDB
file** — one file, single-process, zero extra setup, the simplest way to start. **DuckLake**
(Parquet data + SQL catalog) is the opt-in upgrade (`database: ducklake:…`): snapshot tables
map naturally onto DuckLake tables, DuckLake snapshots give time-travel and cheap rollback,
data inlining keeps small tables fast, and the catalog (SQLite locally) serialises catalog
writes so `interlace serve` and a separate CLI can share one warehouse — which a single-writer
`.duckdb` file cannot. DuckLake commit conflicts need retry; a `tenacity` policy lives in the adapter.

**Current state.** `database: .interlace/warehouse.duckdb` is the config default. The full
strategy surface — schemas, views, transactional DDL+DML (merge), `DESCRIBE`, Arrow ingest —
runs identically on plain DuckDB and on DuckLake (`ducklake:…` — a catalog file +
`<catalog>.files/` Parquet directory DuckDB opens as its primary database); the whole test
suite executes against DuckDB. Requires `duckdb>=1.5.3`.

**Serving the warehouse: the quack protocol.** DuckDB 1.5.3 ships **quack** (core
extension, beta): `CALL quack_serve('quack:host:port', token := ...)` turns the process
holding the warehouse into a server; clients speak the same SQL over HTTP. This is how
interlace solves **single-node multi-process access** *before* any shared-Postgres tier:
the daemon (`interlace serve --quack quack:localhost:4213`) owns the DuckLake and serves
it; CLI runs, schedulers, and ad-hoc DuckDB clients set `database: quack:localhost:4213`
(token via `quack_token` config or `INTERLACE_QUACK_TOKEN`) and share it concurrently.
The `QuackAdapter` ships each statement through the `quack_query` table function (full
SQL pass-through with Arrow results — quack's catalog `ATTACH` only resolves the
server's main schema while in beta), sends multi-statement plans as one `BEGIN…COMMIT`
payload so they stay atomic server-side, and routes Arrow loads through the attached
remote catalog. Verified end-to-end: a second OS process ran `interlace apply` through
quack while the daemon held the DuckLake catalog lock. When quack's catalog mapping
matures, the adapter can switch to native `ATTACH` without touching callers.

**Role 2 — the federation/transport hub (multi-engine).** Named engines: `engines:`
config + `default_engine` (top-level warehouse fields synthesise `default`); `engine:`
on models — fingerprinted, so a move is a BREAKING rebuild; snapshots record their owning
engine and GC drops on the right one; apply/CLI/worker/service route through a lazy
`EngineRegistry`. **The engines that ship are DuckDB (default), Postgres (ADBC — the
`adbc` extra), and quack.** When a model's inputs span engines, the planner inserts an
explicit **transfer edge**, visible in `interlace plan` output — no silent data movement.
Transfer execution picks the cheapest mechanism:

1. Source is attachable to the local engine (Postgres/SQLite/DuckDB/Parquet/DuckLake) and
   target is local → DuckDB `ATTACH` / scanner extensions: a federated `ATTACH → CTAS`
   fast lane, no Python hop.
2. Otherwise → **ADBC**: `source.fetch()` → `pa.RecordBatchReader` → `target.load()`.

Contract: `docs/architecture/MULTI_ENGINE.md`. Cloud-warehouse adapters (Redshift,
Snowflake, BigQuery, MotherDuck) **ship as alpha**. Snowflake has been exercised
against one account (not in CI). Redshift, BigQuery, and MotherDuck are wired and
dialect-correct via the `fetch/load` / ADBC contract, and have not run against a
live account (§14). Promoting any of them out of alpha is still validation work;
Arrow Flight can still land on the same contract later without a redesign.

---

## 5. SQL handling

- **Per-model dialect, canonical IR.** Project config sets `default_dialect` (default
  `duckdb`); any model may override (a `dialect:` in its SQL header or the `@model`
  decorator arg — both ship). At load time every statement is parsed with its declared
  dialect, then normalised: `qualify` (expand `*`, resolve aliases), type-annotate
  against the project schema graph, normalise identifiers. From then on the dialect is
  gone — it reappears only in `EngineAdapter.transpile()`. You can therefore author in
  one dialect and transpile to another; *running* against a given engine still requires
  that engine's adapter (DuckDB and Postgres today — §4).
- **Macros are SQL, expanded in the AST.** Python generates models. Scalar SQL
  macros live in `macros/*.sql` as `CREATE MACRO` and are expanded into the AST
  before fingerprint, lineage, and transpile — one definition, every engine. The SQL
  header is a YAML block comment namespaced under `interlace:`. Path tokens
  (`${date}` / `${datetime}` / `${workspace}`) expand in `inputs:` and file
  materialisation paths. Typed project vars live under `vars:` and a SQL model reads
  one with `var('name')`. That call is an AST node, replaced with a typed literal
  before fingerprint, lineage, and transpile. The syntax is `var('name')`, not
  `@name`: DuckDB already uses `@` for absolute value.
- **References resolve in the AST** during qualification, which is what makes
  lineage parseable.

---

## 6. State & environments

**State store:** SQLite (WAL mode) by default. `state_url: postgresql://…` keeps the same schema in Postgres, in a schema named `interlace` inside that database — not in the warehouse. The control-plane tables (with built-in migrations):

```
snapshots, intervals, environments, work_queue (runs, with lease columns for crash
reclaim), trigger_state, event_log, check_results, api_keys, promotion_history
```

(The stream log keeps its own database — SQLite at `stream_path`, or Postgres schema `interlace_streams` when `stream_url` is set — `stream_events`, `stream_heads`,
`consumer_state` — and stream watermarks live in the warehouse, committed atomically
with the data; see §9.)

The control plane and the warehouse are different databases:

| Plane | Holds | Access pattern | Engine |
|---|---|---|---|
| Data plane | model tables, materialisations | bulk scans/aggregations, few large writes | **DuckDB/DuckLake** |
| Control plane | state store, work queue, stream log | many small durable writes | **SQLite, or Postgres** (`state_url` / `stream_url`) |

The control plane claims a task row, heartbeats, commits a stream offset, appends an
event, and bumps an interval. On SQLite, `BEGIN IMMEDIATE` is the atomic work-queue claim. On Postgres the same claim is `SELECT … FOR UPDATE SKIP LOCKED`. The apply lock is exclusive on both: Postgres updates that row in one conditional upsert, so a second process cannot take a live lease. A stream publish commits before it returns 200 (SQLite `synchronous=FULL`, or Postgres `synchronous_commit`). This is still one process: several worker hosts, `LISTEN/NOTIFY`, and leader election are the scale-out contract (§12), not a shipped option.

**Environment naming:** production (`prod`) is the *unprefixed* namespace — its views
live at `<schema>.<model>` (`main.orders`), which is what BI tools connect to. Every
other environment is a prefixed sandbox (`dev__main.orders`). CLI/API/daemon default to
prod; `--env dev` opts into a sandbox.

**Virtual data environments:**

- Physical layer: `interlace__<schema>.<model>__<fp_short>` — one table per snapshot version.
- Virtual layer: `<env>__<schema>.<model>` views pointing at snapshot tables.
- `interlace plan dev` previews; `apply` backfills only missing (snapshot, interval)
  pairs — unchanged models in dev **reuse prod's physical tables** via views (instant
  dev environments, zero duplicate compute); `promote` repoints prod views (atomic,
  instant); `rollback` repoints back (`promotion_history` records the swaps). A janitor
  GCs unreferenced snapshots past `retention: 14d`; `reset` wipes Interlace-owned state
  (views, snapshots, runs, streams) without dropping terminal `table`/`file` destinations.

**Interval ledger:** per snapshot, a compact set of filled
`[start, end)` ranges at the model's declared grain (`interval="1d"`). Backfill, catchup
after downtime, and restatement (`interlace restate model --start … --end …`) reduce to
set arithmetic. Stream cursors are the same structure with offset grain — one
bookkeeping mechanism for batch and streaming.

**Change classification:** `sqlglot.diff` between old and new canonical ASTs. Added
column → NON_BREAKING (and because qualification expanded `SELECT *`, we *know* who
consumes what). Changed expression → BREAKING, but **column-impact-narrowed** (§7): only
downstream models that actually consume the changed columns are invalidated. This is the
concrete improvement over sqlmesh, whose invalidation is model-granular.

**The indirect non-breaking rebuild-skip.** The differ assigns every changed model an
impact: *semantic* (pre-existing column data may differ — changed expressions/filters/
strategy/Python source, or any semantic upstream), *additive* (existing columns provably
identical, new ones appeared — strictly additive projections with everything else
canonically equal, so a WHERE change is never additive), or *clean* (output provably
identical). Clean models are **not rebuilt**: their new snapshot is recorded pointing at
the previous physical table and the environment view repoints there. The implementation
needs no column lineage — an indirectly-changed model's SQL is unchanged and was
previously valid, so it cannot reference newly-added upstream columns; the only leaks
are a projection `*` (inherits new columns → rebuild) and Python models (see whole
upstream tables → always rebuild). Correctness hinge: reference resolution consults
recorded snapshots (a reused fingerprint lives at an *older* physical table than its
name implies), threaded through apply/resolve/runtime/checks as a physical-table map.

**The column-pruned skip.** The §7 narrowing above, concretely: a *semantic* direct
change computes its provably-**touched** output columns (projection-only edit: with both
projection lists erased the queries are canonically identical, so the row set is
untouched and unchanged projections stay byte-identical), and each downstream computes
the columns it provably **consumes** from that upstream (qualified refs attribute per
join alias; unqualified refs only in single-source queries). Disjoint ⇒ the downstream
is *clean* and skips. Both proofs bail to "everything" on ambiguity: `*`, DISTINCT,
positional/computed GROUP BY, a changed alias referenced from other clauses or sibling
projections, CTE indirection, duplicate output names. Conservative by construction — a
false "touched"/"consumed" only costs a rebuild, never correctness. When the touched set is proved, the plan records it on `impacted_columns` (a non-breaking change still lists columns that were added). An empty list means the proof could not name them.

### Two materialisation planes: virtual (owned) vs terminal (table / file)

`materialise` names **where a model's result lands and who owns it** — one axis, orthogonal
to `strategy` (*how* it is written):

- **virtual plane** (`virtual`, `view`, `ephemeral`) — interlace owns the target. A `virtual`
  model builds an immutable fingerprinted snapshot table `interlace__<schema>.<base>__<fp>`
  that consumers read through an *environment view*. Because the build target is decoupled
  from the read target, this plane gets the full machinery: breaking-change-via-new-table,
  rebuild-skip, sandboxed environments, view-swap promotion, rollback, and gc.
- **terminal plane** (`table`, `file`) — a destination interlace does *not* own. `table`
  delivers into an external, attached table (`target: <alias>.<schema>.<table>`); `file`
  writes a `path` (`format: parquet|csv|json`) via DuckDB `COPY`. A terminal model is still
  fingerprinted (change-tracking) and DAG-scheduled, but produces **no snapshot table and no
  environment view** — it is a side-effecting delivery.

**Strategies are destination-agnostic.** The accumulating strategies
(`merge`/`full_merge`/`incremental`/`scd`) are `CREATE IF NOT EXISTS` +
surgical `DELETE`/`UPDATE`/`INSERT` and run identically against an owned `virtual` table or an
external `table`. Only `replace` differs by ownership: it rewrites the owned table
(`CREATE OR REPLACE` → `Replace`) but empties an external one in place (DELETE all +
INSERT → `ReplaceInPlace`), which **never drops it**, so grants and readers survive. `append`
is external-only. `view` is virtual-only. `resolve_strategy(materialise, strategy, …)` is the
single dispatch; `plan.apply` routes a terminal build to `deliver_table` (stage → align →
strategy) or the file COPY instead of a snapshot build + view swap.

**Indexes and constraints** are a third hash (`physical_hash`), not part of the data
fingerprint. Apply reconciles them after the table exists and before checks: create missing
`il__*` (or explicitly named) objects, drop only names previously recorded. Hand-added
indexes, grants, and RLS are never dropped. Where the engine will not enforce a constraint
(DuckDB enforces `NOT NULL` only; Postgres enforces primary key, unique, not-null, check,
and foreign key), a primary key, unique, or foreign key becomes a non-unique index plus a
plan note, and checks remain the portable gate. On an external table the same spec applies
with a narrower column policy (`schema.columns`: `additive` default, `reject`, or `ignore`);
there is still no drop-column mode.

A terminal target is both the build target and the read target, so a **breaking change
cannot apply to a `table`**: there is no previous snapshot to serve during the build
and no view to cut over. A terminal table evolves **additively only** (new columns via
`ALTER … ADD COLUMN`, widening, NULL-fill/cast in `align_stage_to_target`) and is never
dropped; a definition change re-delivers. `schema.columns: reject` stops that delivery
when the live table is not a compatible superset; `ignore` skips `ALTER` entirely.
Reuse-skip, sandboxes, rollback, gc, and forward-only apply to owned snapshots and not
to a terminal. Its snapshot row exists so an unchanged fingerprint is not re-delivered.

**Spectrum of output kinds and their rollback story:**

| `materialise` | Physical model | Rollback |
|---|---|---|
| `virtual` / `view` / `incremental` | snapshot table + env view | instant view-swap, zero-copy dev envs |
| `table` (external, interlace delivers) | fixed-name table; replace/append/merge/incremental in place, never dropped; additive schema evolution | none (never dropped); re-deliver to correct — keyed strategies make it idempotent |
| `file` | overwrite via `COPY` | none; re-deliver overwrites |
| reverse-ETL to an API + delivery ledger *(roadmap)* | keyed upsert via connector | **forward-correction only** |

**Safety — the property that matters most:** virtual environments must never silently fan
side-effecting writes out to production. Terminal models are environment-gated: delivery only
*executes* when the plan's environment appears in the model's `environments` allow-list
(default: production only), so a dev apply never fires reverse-ETL at a live external table. In
a gated-off environment the terminal's snapshot is still recorded so the plan settles — nothing
leaves the warehouse. (The gating list is part of the fingerprint, so widening it re-plans the
model rather than classifying it UNCHANGED and never delivering.)

```sql
/* interlace: { materialise: file, format: parquet, path: exports/orders.parquet } */
SELECT * FROM orders
```

---

## 7. Dependency graph, lineage, selective execution

- **Load-time, not run-time.** After canonicalisation, run `sqlglot.lineage` per output
  column of every SQL model → a project-wide **column DAG**. Lineage is computed before
  any execution and *drives* planning (the column-pruned rebuild-skip, §6); the service
  computes it once at startup and serves it whole (`GET /lineage`, the UI's lineage
  canvas).
- **Python models** contribute table-level edges from function parameters. A Python
  model is a column-lineage barrier (all-to-all) — conservative and correct.
  (`@model(columns=…)` declares an *output contract* — column names/types validated
  after every build, before promotion — not lineage.)
- **Selector syntax:** `interlace run --select +silver.orders+ tag:finance`
  (`model`, `+model`, `model+`, `+model+`, `tag:x`; selectors union). Modified-ness
  is what the plan computes from fingerprints in the state store.
- **Impact analysis feeds plan:** changed columns → walk the column DAG → downstream
  models partition into *invalidated* (rebuild) vs *safe* (reuse the existing snapshot
  table). Also exposed to humans: `interlace lineage <model> --columns` and per-change
  impacted columns in `GET /plan` (added columns on a non-breaking change; columns whose expressions changed on a semantic one).

---

## 8. Concurrency model (single node)

An asyncio control plane (scheduler, state writes, event bus are genuinely async) over
two execution lanes:

1. **Local DuckDB:** within one process DuckDB supports concurrent connections with
   optimistic MVCC — the single-writer limit is *cross-process*. One `duckdb.connect()`
   per process, `.cursor()` per task; the DAG guarantees no two tasks write one table.
   DuckDB parallelises each query across cores internally, so the local lane is capped at
   a small number of concurrent statements.
2. **Remote engines (Postgres via ADBC):** blocking driver calls run in
   `asyncio.to_thread`, so a warehouse query holds a cheap thread and gives true overlap.
   Concurrency across all lanes is governed by a single `parallelism: int` config knob
   (default 4; `plan`/`apply` also accept `--parallelism`). There is no per-gateway pool
   config today.

**Python models** run on the thread-pool lane (`asyncio.to_thread`) — most are IO-bound
or release the GIL inside pyarrow. A `ProcessPoolExecutor` opt-in is roadmap (§14), not
shipped.

The engine emits events to an EventBus; **the Rich display, JSON logs, and metrics are
subscribers** — the display is never coupled to the executor. (Scheduling is
level/DAG-ordered with durable retries and backoff; a critical-path priority heuristic is
not implemented — `RunRequest.priority` exists but is a plain integer, unused by default.)

---

## 9. Durable streaming

A stream is a durable log. The materialiser lands it in the warehouse, and SQL
models read that table.

**Current state.** `SqliteStreamLog` (WAL; offsets from 1, idempotency-key dedup via a
partial unique index, consumer-group lease/commit with fencing tokens, trim, a waiting
read woken on append, `renew`/`release` for a held lease). `@stream` declarations publish at
`POST /streams/{name}` — schema-validated
(`on_schema_drift: reject` default; extra fields/wrong types → 400, missing → NULL),
durable before the 200, deduplicated on retry. External consumers tail that log with
`GET /streams/{name}/events` (SSE; `?group=` leases a consumer group) and ack with
`POST /streams/{name}/commit` — a delivered frame is not an ack. The materialiser flushes micro-batches
into `streams.<name>` (declared fields + `_offset`/`_ingested_at`) with the watermark
committed **in the same warehouse transaction** as the data — exactly-once *landing*
given a transactional `execute_all` (DuckDB / ADBC; Spark is refused) — without
coordinating with the log; SQL models just `FROM streams.<name>`. Publish only appends
(durable ack, no warehouse work on the hot path); a signal-driven flusher coalesces
publishes into one warehouse write moments later (`stream_flush_interval`, **50 ms**
default), applies pending flushes before planning, and a clean shutdown drains the
residue. Publish durability is fsync-bound (`synchronous=FULL`). CI guards a
regression ceiling — a single-event HTTP 200 under 250 ms across a short burst,
rows queryable within 2 s, the consumer run queued within 3 s — and does not
treat a tighter millisecond target as a contract. A flush **enqueues the models that read the stream** (plus their downstream
closure) onto the durable run queue, with the watermark as the idempotency key — repeated
flushes debounce, new data re-enqueues.

All three `on_schema_drift` modes are implemented:
- **reject** (default): unknown fields / wrong types → 400 before durability; missing → NULL.
- **evolve**: unknown fields become real columns at flush time (type inferred from data;
  conflicting inferences widen to TEXT; `ALTER ADD COLUMN IF NOT EXISTS` + `INSERT BY
  NAME` — verified on DuckLake). Declared fields accept *widening* coercions
  (int→double, scalar→text/json); an incompatible type change still rejects. The log
  stores raw payloads; evolution happens at flush, so daemon catch-up evolves
  identically.
- **quarantine**: failing events divert durably to a shadow stream `<name>__quarantine`
  (error + raw payload JSON, materialised to its own table); valid events flow; the
  publish response reports the quarantined count.

### 9.1 `StreamLog` — the durable ingestion log

```python
class StreamLog(Protocol):
    """Durable, ordered, replayable per-stream log. At-least-once."""
    async def append(self, stream, events) -> AppendResult          # MUST NOT return before durable
    async def read(self, stream, after_offset, limit, wait=None) -> list[StoredEvent]
    async def heads(self) -> dict[str, int]
    async def lease(self, stream, group, *, ttl, owner) -> Lease | None
    async def commit(self, stream, group, offset, lease_token) -> None   # fencing tokens
    async def trim(self, stream, *, before_offset=None, before_ts=None) -> int
```

**Backends.** SQLite (WAL, `synchronous=FULL`) is the default, fronted by a per-connection
lock. `stream_url` selects Postgres: the same statements, with `BEGIN` in place of `BEGIN IMMEDIATE` and durability from `synchronous_commit`. Redpanda/NATS are not built.

- **Durable append, honestly.** `append` runs a plain per-call `BEGIN IMMEDIATE` … 
  `INSERT` … `COMMIT` on a dedicated connection. `synchronous=FULL` (not NORMAL) is the
  deliberate price of the documented contract: a 200-OK means fsynced — survives power
  loss, not just a process crash. Single-event throughput is therefore disk-flush bound;
  batched publishes amortise the fsync (one commit per batch). There is **no group-commit
  deque and no `Backpressure` exception** — the log never rejects a durable append.
- **Overload is handled at the service edge, not in the log.** The publish endpoint
  tracks per-stream *pending* = (log head − flushed watermark); past
  `stream_max_pending` (default **100 000**) it returns **HTTP 429** so a warehouse that
  can't keep up applies backpressure to producers. There is no consumer-lag (`max_lag`)
  gate.
- **Idempotency:** dedup is keyed off a **configured payload field**
  (`@stream(idempotency_key="…")`) enforced by a partial unique index — transactional
  with the append; a duplicate returns the original offset. There is no
  `Idempotency-Key` HTTP header.
- **The cursor race is dead by construction:** `lease` + `commit` are rows in the same
  transaction domain as the events, with fencing tokens. A crashed consumer's lease
  expires; the next claimant resumes from `committed_offset`. Worst case is redelivery
  (at-least-once), never loss — and table materialisation dedups via the watermark
  (below). *(This lease/commit machinery is for external consumers; the built-in
  materialiser path does not use it — §9.2.)*
- **Retention:** the janitor trims events that are both **materialised** (at or below the
  watermark) **and** older than the stream's declared retention window. Unflushed events
  survive regardless of age; streams without a retention are never trimmed. (Retention is
  age + watermark only — there is no `max_events` / `min_unconsumed` / `ConsumerLapped`
  behaviour.)

The Postgres stream log ships (`stream_url`). `SKIP LOCKED` consumer leases, Redpanda/Kafka,
NATS JetStream, and an Arrow-IPC segment backend are roadmap (§14).

### 9.2 Ingest → table: the materialiser

A flush drains everything past the stream's **warehouse watermark** in `batch_rows`
chunks (default 5000). Each chunk stages one Arrow batch and moves `stage → target table
+ watermark` in a **single engine transaction** — so a crash leaves either the old
watermark (events re-read, stage overwritten, no duplicates) or the new one:
**exactly-once landing into the warehouse** (transactional `execute_all`) without
coordinating with the log. The watermark
lives in the warehouse (`streams._watermarks`) precisely so it commits atomically with
the data. Evolve-mode `ALTER ADD COLUMN` statements ride in the same batch.

This path does not use the log's consumer-group lease/commit machinery. That is
for external consumers. The flusher is signal-driven off publishes
and coalesces at `stream_flush_interval` (50 ms default); draining (not a single batch)
is what lets callers assume the warehouse has caught up when a flush returns. When a
flush lands new rows, the stream's consumers (models reading `streams.<name>`, plus their
downstream closure) are enqueued on the durable run queue with the watermark as the
idempotency key.

### 9.3 Streaming as micro-batch (design note)

"Streaming models" in interlace are just ordinary models reading `streams.<name>` and
re-run when a flush enqueues them — one execution engine, no separate streaming runtime.
There is **no** `kind="incremental_stream"`, `on_stream(...)` trigger,
`ctx.stream_batch(...)` accessor, outbound webhook/RabbitMQ consumer, `<stream>__dlq`
dead-letter, or GCRA rate-limiting today. External consumers subscribe over SSE
(`GET /streams/{name}/events`) and ack with the log's lease/commit. A first-class
incremental-stream model kind and push-style outbound consumers (webhook, RabbitMQ) are
roadmap (§14); the micro-batch-over-a-log design is what admits a
DBSP-style incremental engine as an optional accelerator later.

---

## 10. Orchestrator

**Durable `WorkQueue` in the state DB** — no global lock, nothing in-memory-only:

```python
class WorkQueue(Protocol):
    async def enqueue(self, task) -> str
    async def claim(self, worker_id, slots) -> list[ClaimedTask]
        # SQLite: BEGIN IMMEDIATE. Postgres: SELECT … FOR UPDATE SKIP LOCKED
    async def heartbeat(self, task_id, lease_token) -> Command   # returns CANCEL → cooperative cancel
    async def finish(self, task_id, lease_token, result) -> None
```

**Current state.** A `TriggerEngine` ticks `Trigger`s (`CronTrigger` via `cronsim`,
`IntervalTrigger`, `WatchTrigger`, `OnChangeTrigger`, `FreshTrigger`) against durable per-trigger state in the state DB; due runs enqueue
(idempotency-keyed) onto a **durable run queue** (`work_queue` table). `worker.drain`
claims runs under a **lease** (one minute, renewed from a thread — a crash window, not a limit on how long a model may run), heartbeats while executing (the heartbeat doubles as the
cooperative **cancellation** channel — `interlace cancel <id>` / `POST /runs/{id}/
cancel`), retries durably up to `max_attempts`, and executes
them as forced runs (so they pick up new data). There is no runtime cap unless a caller sets one. A retry rebuilds only models that
did not finish; models that reached `model.done` are promoted again and not
recomputed. A cron, interval, watch, table change, freshness, upstream completion, or webhook enqueues that model and its
downstream closure. `run --select` is not expanded. `schedule: {after: raw}` enqueues when `raw` reaches `model.done`, including an apply or an explicit run. Stream flushes enqueue the consuming
models with the watermark as the idempotency key. `interlace serve` applies the
project once while the API is already listening, then ties tick → enqueue → drain in one process (`--no-apply` skips
the apply; writes return 503 until the startup apply finishes; `interlace scheduler --once` is a single tick). `cronsim`
parses cron expressions. Models declare `schedule: {cron: …}`,
`{every: …}`, `{watch: "inbox/*.csv"}` (a glob of path, size, and mtime on the
existing tick — no directory watcher), `{on_change: column}`, `{fresh: "updated_at 2h"}`
(or `{fresh: {column, within}}`; a missing table waits, an empty one counts as stale),
`{after: raw}` (or a list of models), or `{webhook: name}` (`POST /hooks/{name}`).

The lease columns on `work_queue` provide crash-reclaim of *work items* (a dead worker's
lease expires and the task is re-claimed). This is **not** leader election: there is no
`leases` table for singleton loops and no multi-node coordination — single-node runs all
loops directly. Backfill/catchup is `interlace run` (forced) and `interlace restate
--start … --end …` (marks intervals pending and cascades via lineage); there is no
separate `interlace backfill` command.

**Not yet built (roadmap, §14):** SLA monitors + alerting (`@model(sla=…)`, an
`AlertRouter`, an `alerts` table) and leader election for multi-node singleton loops.
File-watch, table-change, freshness, upstream-completion (`after`), and inbound webhook schedules ship.

---

## 11. Service layer

**Litestar + msgspec + uvicorn** (the `service` extra):

- First-class SSE with `Last-Event-ID` replay (`?token=` accepted on `/events/stream`
  because EventSource cannot send `Authorization`); OpenAPI 3.1 generated from typed handlers;
  msgspec-native serialisation (the publish endpoint shares msgspec structs with ingest
  validation); guards/DI for scoped auth.
- **Auth:** scoped API keys (`read` / `write` / `admin`; `admin` satisfies any
  requirement) as `ilk_…` bearer tokens, **sha256-hashed** in the state DB. Auth enforces
  once at least one key exists — a fresh project stays open for local development;
  `interlace apikey create` locks it down. Routes declare their required scope; `/health`,
  `/schema`, and the `/ui` static shell stay open (the API calls the UI makes still
  enforce scopes). There is **no OIDC/JWKS/tenant/session** layer — API keys are the whole
  auth model (OIDC is roadmap, §14).
- **Durable event spine:** events are rows in `event_log(seq, ts, type, entity, payload)`
  with in-process fanout. SSE reconnect and `GET /events?after=N` replay from the table —
  the UI never misses a transition across restarts. The same log records
  apply/run/stream/gc lifecycle: one spine. Set `event_log_path` and each committed
  event is also one NDJSON line (the operator SSE poll stays, because a CLI apply
  is another process). Apply and run events carry `api_key`: the HTTP key name,
  `cli`, `scheduler`, `mcp`, or `anonymous`.
- **Process composition:** `interlace serve` runs everything as supervised background
  tasks inside the app lifespan — the event tail (one store poller feeds every SSE
  client), the stream flusher, and the scheduler loop. Background loops never die on an
  exception (log + retry); shutdown drains the stream residue and closes the store, log,
  and engines. **Components share zero objects** — they communicate only through the State
  DB, the StreamLog, and the Warehouse; `interlace serve --no-scheduler` plus a separate
  `interlace scheduler` process is that split today.
- **The web UI** ships inside the package (`service/ui/`, plain ES modules, zero build
  step), served at `/ui`: overview, lineage canvas with column-level tracing, models,
  plan/apply with SQL diffs, live runs, query console, streams, checks, environments, and
  system — live over the SSE spine. A selected model (and the lineage node) shows a row
  sample, a short column profile, and the SQL of the last failed statement.

---

## 12. Scale-out path (the designed contract, not yet shipped)

The store/queue/log abstractions are Protocols so a single-node deployment can grow into
a shared-Postgres, multi-worker one **without a caller-visible redesign**. Postgres as
the control plane and as the stream log ships (`state_url`, `stream_url`): one process,
the same store code, claim via `FOR UPDATE SKIP LOCKED`. The right-hand column is what
is still roadmap (§14).

| Substrate | Shipped | Still roadmap | Why no redesign |
|---|---|---|---|
| State DB / WorkQueue / EventLog | SQLite (WAL), or Postgres (`state_url`) | several worker hosts, LISTEN/NOTIFY, leader election | claim/lease/fence stay in the store |
| StreamLog | SQLite, or Postgres (`stream_url`) | Redpanda/NATS, or object-store Arrow segments | offsets/leases/idempotency stay on the Protocol |
| Warehouse | DuckDB/DuckLake (SQLite catalog) | DuckLake on Postgres catalog; MotherDuck; Snowflake/BigQuery | watermark pattern works everywhere; DuckLake catalog swap is config |
| Workers | in-process claim loop | same loop, more processes/hosts — the queue is the protocol | nothing to redesign |

A live check of enqueue, claim, the event log, and stream append runs when Postgres is reachable (`tests/test_control_plane_pg.py`). It is not a second copy of the suite. Still one process until a named limit: local-DuckDB-file concurrency, and a `ProcessPoolExecutor` (per worker host).

---

## 13. Package layout & dependencies

```
src/interlace/
  dsl/         # @model @stream @check; SQL loader; discovery; dynamic register_model
  ir/          # Relation types; canonicalisation; fingerprints; macros; vars; Arrow schema
  graph/       # dag (toposort, stdlib), column_lineage, selectors
  state/       # store (SQLite or Postgres control plane + migrations), snapshot, interval, janitor (gc, reset)
  plan/        # differ, plan, apply (schedule + promote), backfill, delivery, fit, transfer,
               #   schedule, result, run, orchestrate, comment, table_diff
  physical/    # indexes/constraints specs, drift, reconcile DDL (third hash, not data fp)
  engines/     # base (EngineAdapter, EngineCaps); adbc (shared ADBC base); duckdb (+ DuckLake),
               #   postgres, redshift/snowflake/bigquery (alpha), spark (beta), quack, registry
  strategies/  # replace, view, full_merge, incremental, merge, hash_merge, scd
  checks/      # built-in check types + @check decorator — results gate promotion
  testing/     # fixture/golden CSV tests (`interlace test`)
  cdc/         # Postgres logical replication slot → @stream
  inspect.py   # row sample, column profile, and the rows a check rejected
  mcp_server.py # stdio MCP server; apply refuses unless confirm is true
  connections.py # named http/postgres connections a Python model reads while building
  inputs.py    # DuckDB file scans (parquet/csv/json/delta/iceberg) as FROM-able views
  scheduler/   # triggers (cron/interval/watch/on_change/fresh/after/webhook), engine, worker (leases/retries/cancel)
  runtime/     # execution context for Python models (Arrow handles)
  streaming/   # log (SqliteStreamLog), materializer (flush + watermark), schema (drift modes)
  service/     # types.py (msgspec wire structs), app.py (litestar), auth.py, ui/ (the /ui web app)
  config/      # config load; ${VAR} + .env interpolation
  cli/         # init plan apply diff run restate gc reset scheduler serve mcp models lineage env runs
               #   checks streams engines connections cancel apikey test
  sinks.py     # terminal delivery helpers: external table target + file COPY
  project.py   # Project.load/compile; engine + state + stream-log opening
```

**Core dependencies** (exactly `[project.dependencies]` in `pyproject.toml`):

| Package | Constraint | Why |
|---|---|---|
| `sqlglot` | `>=30.0,<31.0` | Canonical IR, transpilation, qualification/type annotation, semantic diff, column lineage. The single most load-bearing dep. |
| `duckdb` | `>=1.5.3` | Default engine, federation hub, DuckLake, quack serving. |
| `pyarrow` | `>=17.0` | The wire format; RecordBatchReader everywhere. |
| `pydantic` v2 | `>=2.5,<3.0` | Config + manifest validation only (cold paths). |
| `typer` | `>=0.12,<1.0` | CLI. |
| `rich` | `>=13.0,<16.0` | CLI display, strictly an event subscriber. |
| `cronsim` | `>=2.5,<3.0` | Cron parsing for the trigger engine. |
| `tenacity` | `>=8.2,<10.0` | Retries: tasks, DuckLake commit conflicts, transfers. |
| `pyyaml` | `>=6.0,<7.0` | Project config (config + env overlays). |

Logging is the **standard library `logging`** — there is no `structlog` dependency.

**Extras** (from `pyproject.toml`):

- **`service`** — the Litestar/uvicorn daemon: `litestar`, `uvicorn`, and **`msgspec`**
  (the wire types; msgspec is a service-extra dep, not core).
- **`adbc`** — the Postgres and Redshift engines via Arrow-native ADBC
  (`adbc-driver-manager`, `adbc-driver-postgresql`). **`adbc-snowflake`** / **`adbc-bigquery`**
  add those (alpha) drivers.
- **`spark`** — the Spark engine (beta): `pyspark` + `delta-spark` (Spark 4.0–4.2 / Delta 4.4),
  a `SparkSession` transport rather than ADBC.
- **`postgres`** — `psycopg[binary]`, used today by `cdc:` (logical replication into a
  `@stream`) and `connections:` of type `postgres`. A Postgres *control-plane* store
  (state/queue/log) is still unbuilt (§12, §14).
- **`polars`** — `polars`, the preferred eager frame a user can build from
  `handle.table()`.
- **`pandas`** — `pandas`, optional DataFrame interop.
- **`all`** — `service,adbc,postgres,polars,sources`.
- **`dev`** — test/lint toolchain: `pytest`, `pytest-asyncio`, `ruff`, `black`, `mypy`,
  and **`httpx`** (litestar's TestClient transport). httpx is not a runtime dependency.

The stream log and work queue are built on `sqlite3`, or on Postgres when `state_url` / `stream_url` is set (`psycopg`, the `postgres` extra).

---

## 14. Roadmap — not yet built

Shipped behaviour is described in the body. This section is only what is still unbuilt,
ranked so a later reader does not treat every bullet as equal. Principle: deepen the
one-process wedge, close trust gaps, defer scale-out until a named user hits the ceiling.

A feature from another tool is in scope only when it serves that principle.

| Tool | Take, because it fits the goal | Leave |
|---|---|---|
| SQLMesh | Plan/apply, virtual environments, and the interval ledger are already the state model. A semantic edit records the output columns that changed (`impacted_columns`). | Their Airflow and Dagster scheduler integrations. This process owns the loop. |
| dbt | Selector grammar, and tests that gate promotion. `on_change` runs a model when a source column's max moves. `fresh` runs it when that maximum is older than a window. | MetricFlow, the package hub, and Jinja. Python is the macro language. |
| Airflow | Retries, leases, and data-aware scheduling of *our* DAG. A trigger already enqueues the model and its descendants. | Arbitrary operators, executors, and a second deployment. |
| Prefect | Event-driven automations: `schedule: {after: model}` runs a model when another one finishes. | Work pools and general Python flows. Those are a task runner, which this is not. |
| dlt | Call it inside a Python model when a connector is the job. Schema drift on streams is the same idea as their schema evolution, already shipped for `@stream`. | Becoming a connector catalog. |
| Hevo, Census, Hightouch | The terminal plane: deliver into a table the warehouse does not own, and later a delivery ledger for API sinks. | Hosted ELT and a long list of SaaS connectors as the product. |
| Cloudflare Pipelines | The ingestion reference: fsync before ack, then land where SQL can read it. | Edge scale and a managed Iceberg catalog. Iceberg/R2 remains a sink idea, not the runtime. |

Already shipped (do not look for these here): snapshots and virtual environments,
column-pruned plan/apply, AST macros, `hash_merge`, indexes/constraints, `reset`,
cross-process apply lock, fixture tests (`interlace test`), cron/interval/`watch`/
`on_change`/`fresh`/`after`/webhook schedules (a trigger enqueues that model and its descendants; an explicit
`run --select` does not; `after` fires from `model.done`, including apply and run), retry that skips models already recorded as `model.done`,
lease renewal on a thread rather than a task timeout, Postgres CDC, named `connections:` / `inputs:` (including
`watch: true` content hashes), runtime `register_model`, MCP, inspect/preview,
stream SSE consumers, event-log NDJSON, `interlace diff` (env/table compare),
GitHub Action plan comment, daemon refusal when engines / connections / inputs /
cdc / warehouse change under `serve`, stream publish and flush regression ceilings,
macOS `init`+`apply` smoke in CI, typed `vars:` read from SQL with `var('name')`,
Python fingerprints that include factory defaults and closure cells.

### Next

- **Live-validate MotherDuck and BigQuery.** Adapters are wired and dialect-correct
  but have not run against a live account (§4). Snowflake has been exercised against
  one account, outside CI. That validation needs a named account; it is not something
  the tree can close on its own.

### Later

- **SLA + alerting** — `@model(sla=…)`, `AlertRouter` to Slack/webhook/email, `alerts`
  table, UI history (§10).
- **Leader election / multi-node** — `leases` for singleton loops, LISTEN/NOTIFY,
  and more than one worker host (§10, §12). Claim on the Postgres control plane
  already uses `FOR UPDATE SKIP LOCKED`; that is not multi-node by itself.
- **Spark `scd`/`full_merge`** — MERGE rewrite for Delta (subqueries in `UPDATE`/
  `DELETE` are forbidden). Databricks `load()` needs a staged-COPY path (§4).
- **Reverse-ETL SaaS connectors + delivery ledger** — `SinkConnector` beyond
  `table`/`file` (§6).
- **First-class streaming models** — `kind="incremental_stream"`, `on_stream`,
  `ctx.stream_batch`. SSE consumer groups already ship; webhook/RabbitMQ/`__dlq`/
  GCRA and a DBSP accelerator do not (§9).
- **Broker stream-log backends** — Redpanda/Kafka, NATS, Arrow-IPC
  segments; `max_lag`; richer retention (§9.1). The Postgres stream log ships via `stream_url`.
- **OIDC / JWKS** — browser SSO on top of API keys (§11).
- **Iceberg / R2 sink** — Iceberg via DuckDB REST catalog (incl. Cloudflare R2 Data
  Catalog) (§9).
- **Process-pool executor** — `@model(executor="process")` (§3, §8).
- **OpenLineage** emit from apply/run; a dlt-inside-`@model` template.

### Not a product bet

Semantic layer / MetricFlow; a package hub; arbitrary-Python-task orchestration;
Kafka as the stream log; a DBSP engine; matching dbt-mcp's remote Fusion
toolset.
