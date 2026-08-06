# Interlace — Architecture & Design

*Written during the 2026 rebuild of the 0.x line. This is the design rationale and
the contract for the platform now shipping as **v2.0** (`interlaced` on PyPI). Short
*current state* notes in each section record where the implementation actually stands;
a consolidated **Roadmap** section (§14) lists what is designed but not yet built. When
this document says "we do X", read it as the shipped behaviour unless a note says
otherwise.*

**Status:** Implemented, single-node. **Scope:** clean-slate design of the whole
platform. **Goal:** a comprehensive, independent, MIT-licensed alternative to
sqlmesh/dbt with built-in orchestration (no Airflow) and durable real-time streaming
ingestion (Cloudflare-data-platform-style functionality), deployable as a single
process. **Deployment model:** single-node first — one process does everything — with
the store/queue/log abstractions designed as Protocols so worker nodes and a shared
Postgres tier can be added later without a redesign (that swap is roadmap, not shipped;
see §12, §14).

---

## 1. Core thesis

> **The canonical IR is a sqlglot AST + Arrow schema. The canonical wire format is an
> Arrow `RecordBatchReader`. Materialisation happens exactly once, at the sink, as a
> single native SQL statement executed inside the owning engine.**

The framework's internal contract is deliberately independent of any dataframe library:

- **sqlglot** is the most load-bearing dependency: parsing, qualification, type
  annotation, transpilation across dialects, semantic diff, and column lineage.
- **Arrow** is the only interchange format. pandas never appears in core (optional
  extra only).
- **ibis** is not used. Its two roles are both covered without it: as a data plane it
  sits on Arrow anyway, and as an expression builder it compiles to sqlglot — which
  *is* our IR. Dropping it removes a heavyweight dependency and its governance risk for
  zero lost capability. Remote engines connect via ADBC, not ibis backends.

An **SQL model** never leaves the logical plane: a model selecting from three upstreams
in the same engine compiles to *one* `CREATE TABLE AS` (or `MERGE INTO`, via the keyed
strategies) executed inside that engine — zero rows enter the Python process. A
**Python model** is the physical escape hatch: it receives its upstreams as Arrow and
returns Arrow (§3).

---

## 2. Core abstractions

### 2.1 `Relation` — what a model produces

An SQL model is a qualified, type-annotated sqlglot AST tagged with the engine that can
evaluate it natively (`SqlRelation`). A Python model produces Arrow record batches. The
schema is always known — declared or inferred.

### 2.2 `EngineAdapter` — the only place dialect-specific code lives

```python
class EngineAdapter(ABC):
    dialect: str                             # sqlglot dialect name
    caps: EngineCaps                         # feature flags for strategy fallbacks

    async def execute(self, ast: exp.Expression) -> None: ...
    async def fetch(self, ast) -> pa.RecordBatchReader: ...        # extract: engine → Arrow
    async def load(self, table, reader, mode: Literal["create","append"]) -> int: ...  # Arrow → engine
    async def create_view(self, name, target) -> None: ...
    async def create_schema(self, name) -> None: ...
    async def describe(self, table) -> dict[str, str]: ...

    def transpile(self, ast) -> str:
        return ast.sql(dialect=self.dialect)  # canonical AST → engine SQL, one line
```

`EngineCaps` carries the flags strategies branch on: `supports_create_or_replace`,
`supports_star_exclude` (for `scd`'s `SELECT * EXCLUDE` — absent, `scd` enumerates the
model's columns instead), and `supports_merge` (present, `merge` emits a native single
`MERGE`; absent, it falls back to `DELETE`+`INSERT`). All default off (conservative);
adapters set what their engine supports and strategies degrade accordingly.

### 2.3 `Snapshot` — versioned model state (sqlmesh, adopted)

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
same strategy config and upstreams. This is honest about Python's opacity at the
source level; it does **not** hash closure state or bytecode, so a change that only
mutates a captured constant will not be detected — keep model logic in the function
body. (A `volatility`/`--forward-only` escape hatch for pure refactors is a possible
future refinement, not a current feature.)

### 2.4 `Plan` — terraform-style change preview

A plan carries the model changes (added / breaking / non-breaking / removed), the
backfill tasks (column-impact-narrowed, §6), the explicit cross-engine transfer edges
(never silent), and the virtual view swaps that promote is about to perform.

### 2.5 `Strategy` — AST builders, never strings

```python
class Strategy(ABC):
    def plan_statements(self, rel, target, caps: EngineCaps,
                        interval: Interval | None = None) -> list[exp.Expression]:
        """Return canonical-dialect ASTs. The adapter transpiles. NEVER returns strings."""
```

Built-ins: `replace`, `view`, `ephemeral` (AST-spliced as a CTE into consumers at compile
time), `incremental_by_time` (interval predicate injected as an AST filter),
`merge` and `full_merge` (keyed upsert built from `exp` constructors), and
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

A trigger is pure: given the current time and when it last fired, it returns the runs
now due. **Two implementations ship: `CronTrigger` (parsed by `cronsim`) and
`IntervalTrigger`.** Both key their `RunRequest` on the schedule slot so a crash between
enqueue and the last-fired write re-lands on the same idempotency key and the durable
queue dedupes instead of double-running. Stream arrival does *not* go through a trigger:
a flush enqueues the stream's downstream consumers directly (§9). Sensor-style triggers
(freshness, upstream-completion, webhook, manual) are roadmap (§14).

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

## 4. DuckDB's two roles; DuckLake as default storage

**Role 1 — the default local engine.** Physical storage defaults to **DuckLake**
(Parquet data + SQL catalog) rather than a monolithic `.duckdb` file: snapshot tables
map naturally onto DuckLake tables, DuckLake snapshots give time-travel and cheap
rollback, data inlining keeps small tables fast, and the catalog is SQLite locally.
DuckLake commit conflicts need retry; a `tenacity` policy lives in the adapter. A plain
`.duckdb` file remains a config option for zero-dependency toy projects.

**Current state.** `database: ducklake:.interlace/warehouse.ducklake` is the config
default (catalog file + `<catalog>.files/` Parquet directory; DuckDB opens the DuckLake
as its primary database). The full strategy surface — schemas, views, transactional
DDL+DML (merge), `DESCRIBE`, Arrow ingest — runs on DuckLake unchanged; the whole test
suite executes against it. Requires `duckdb>=1.5.3`.

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

Contract: `docs/architecture/MULTI_ENGINE.md`. Cloud-warehouse adapters (Snowflake,
BigQuery) — and with them the "author in Snowflake SQL, run it in Snowflake" prod story
— are **not built**; they are the primary multi-engine roadmap item (§14). The
`fetch/load` contract is what admits them (and Arrow Flight) later without a redesign.

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
- **Jinja macros: rejected.** Python is the macro language. The SQL header is a YAML
  block comment namespaced under `interlace:` — valid SQL, no Jinja, no text
  substitution. (A typed `{{ }}` / `@vars` templating layer is *not* implemented; there
  is no vars machinery at all today. It is a possible future addition — §14.)
- **`ref()` as text macro: rejected.** References resolve at the AST level during
  qualification — which is what makes lineage parseable.

---

## 6. State & environments

**State store:** SQLite (WAL mode). The control-plane schema (with built-in migrations):

```
snapshots, intervals, environments, work_queue (runs, with lease columns for crash
reclaim), trigger_state, event_log, check_results, api_keys, promotion_history
```

(The stream log keeps its own SQLite database — `stream_events`, `stream_heads`,
`consumer_state` — and stream watermarks live in the warehouse, committed atomically
with the data; see §9.)

**Why SQLite and not the warehouse DuckDB.** The architecture has two planes that want
opposite things from a database, so it uses the right engine for each:

| Plane | Holds | Access pattern | Engine |
|---|---|---|---|
| Data plane | model tables, materialisations | bulk scans/aggregations, few large writes | **DuckDB/DuckLake** (OLAP, columnar) |
| Control plane | state store, work queue, stream log | many tiny indexed point-writes, frequent durable commits | **SQLite** (OLTP, row store) |

The control plane is an OLTP workload — claim a task row, heartbeat, commit a stream
offset, append an event, bump an interval. DuckDB is the wrong tool for it: it is
columnar and built to scan/aggregate (thousands of tiny single-row commits/s is close
to pathological), it allows one writer per file (every heartbeat would contend with
materialisation), and its single-writer MVCC does not express atomic multi-worker row
claims. SQLite's B-tree row store does exactly this (WAL: ~10k–50k durable commits/s on
one node), which is what makes "HTTP 200 ⇒ fsynced" achievable on the stream path, and
`BEGIN IMMEDIATE` expresses the atomic work-queue claim cleanly. The cost of "an
additional technology" is near zero: `sqlite3` is in the standard library, and
DuckLake's catalog is *already* SQLite locally. This is the conventional split (sqlmesh
keeps state in a transactional DB; Airflow/Dagster use Postgres for metadata, never the
warehouse). The store protocols are written so the backend can be swapped to Postgres
for a shared/multi-node deployment — but **no Postgres store backend is built today**;
that is the scale-out contract (§12) and roadmap (§14), not a shipped option.

**Environment naming:** production (`prod`) is the *unprefixed* namespace — its views
live at `<schema>.<model>` (`main.orders`), which is what BI tools connect to. Every
other environment is a prefixed sandbox (`dev__main.orders`). CLI/API/daemon default to
prod; `--env dev` opts into a sandbox.

**Virtual data environments** (sqlmesh, adopted):

- Physical layer: `interlace__<schema>.<model>__<fp_short>` — one table per snapshot version.
- Virtual layer: `<env>__<schema>.<model>` views pointing at snapshot tables.
- `interlace plan dev` previews; `apply` backfills only missing (snapshot, interval)
  pairs — unchanged models in dev **reuse prod's physical tables** via views (instant
  dev environments, zero duplicate compute); `promote` repoints prod views (atomic,
  instant); `rollback` repoints back (`promotion_history` records the swaps). A janitor
  GCs unreferenced snapshots past `retention: 14d`.

**Interval ledger** (sqlmesh, adopted): per snapshot, a compact set of filled
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
false "touched"/"consumed" only costs a rebuild, never correctness.

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
(`merge`/`full_merge`/`incremental_by_time`/`scd`) are `CREATE IF NOT EXISTS` +
surgical `DELETE`/`UPDATE`/`INSERT` and run identically against an owned `virtual` table or an
external `table`. Only `replace` differs by ownership: it rewrites the owned table
(`CREATE OR REPLACE` → `Replace`) but empties an external one in place (DELETE all +
INSERT → `ReplaceInPlace`), which **never drops it**, so grants and readers survive. `append`
is external-only. `view` is virtual-only. `resolve_strategy(materialise, strategy, …)` is the
single dispatch; `plan.apply` routes a terminal build to `_deliver_table` (stage → align →
strategy) or the file COPY instead of a snapshot build + view swap.

**Why the six virtual-plane powers can't apply to a terminal.** The snapshot+view layer works
only because interlace owns its tables — it shadow-builds `model__<fp>` beside the live one
and atomically repoints a view. A terminal target conflates the build target with the read
target, so a **breaking change cannot apply to a `table`**: there is no old version to serve
during the build and no atomic cutover. A terminal table therefore evolves **additively only**
(new columns via `ALTER … ADD COLUMN`, widening, NULL-fill/cast in `_align_stage_to_target`)
and is never dropped; a definition change simply re-delivers. Reuse-skip, sandboxes, rollback,
gc, and forward-only are likewise inherent to content-addressing and do not exist for a
terminal (its phantom snapshot row exists only so an unchanged fingerprint isn't re-delivered).
This mirrors how Census/Hightouch split "model in the warehouse" from "sync to destination".

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
- **Selector syntax:** dbt's, adopted — `interlace run --select +silver.orders+
  tag:finance` (`model`, `+model`, `model+`, `+model+`, `tag:x`; selectors union). dbt's
  `state:modified` needs no selector here: modified-ness is what the plan computes from
  fingerprints in the state store (improvement over dbt's fragile `--defer --state`).
- **Impact analysis feeds plan:** changed columns → walk the column DAG → downstream
  models partition into *invalidated* (rebuild) vs *safe* (reuse the existing snapshot
  table). Also exposed to humans: `interlace lineage <model> --columns` and per-change
  impacted columns in `GET /plan`.

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

Vocabulary deliberately mirrors Cloudflare's **Streams → Pipelines → Sinks** model; the
pitch is "self-hosted Cloudflare Pipelines that lands in DuckDB/DuckLake."

**Current state.** `SqliteStreamLog` (WAL; offsets from 1, idempotency-key dedup via a
partial unique index, consumer-group lease/commit with fencing tokens, trim, long-poll
read). `@stream` declarations publish at `POST /streams/{name}` — schema-validated
(`on_schema_drift: reject` default; extra fields/wrong types → 400, missing → NULL),
durable before the 200, deduplicated on retry. The materialiser flushes micro-batches
into `streams.<name>` (declared fields + `_offset`/`_ingested_at`) with the watermark
committed **in the same warehouse transaction** as the data — exactly-once without
coordinating with the log; SQL models just `FROM streams.<name>`. Publish only appends
(durable ack, no warehouse work on the hot path); a signal-driven flusher coalesces
publishes into one warehouse write moments later (`stream_flush_interval`, **50 ms**
default), applies pending flushes before planning, and a clean shutdown drains the
residue. A flush **enqueues the models that read the stream** (plus their downstream
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

**The only backend is SQLite** (WAL, `synchronous=FULL`), fronted by a per-connection
lock:

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

Alternative broker backends behind the same Protocol (Postgres `SKIP LOCKED` leases,
Redpanda/Kafka, NATS JetStream), and an Arrow-IPC segment backend, are roadmap (§14).

### 9.2 Ingest → table: the materialiser

A flush drains everything past the stream's **warehouse watermark** in `batch_rows`
chunks (default 5000). Each chunk stages one Arrow batch and moves `stage → target table
+ watermark` in a **single engine transaction** — so a crash leaves either the old
watermark (events re-read, stage overwritten, no duplicates) or the new one:
**exactly-once into the warehouse** without coordinating with the log. The watermark
lives in the warehouse (`streams._watermarks`) precisely so it commits atomically with
the data.

Note this path deliberately does **not** use the log's consumer-group lease/commit
machinery — that is for external consumers. The flusher is signal-driven off publishes
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
dead-letter, or GCRA rate-limiting today. A first-class incremental-stream model kind and
outbound consumers are roadmap (§14); the micro-batch-over-a-log design is what admits a
DBSP-style incremental engine as an optional accelerator later.

---

## 10. Orchestrator

**Durable `WorkQueue` in the state DB** — no global lock, nothing in-memory-only:

```python
class WorkQueue(Protocol):
    async def enqueue(self, task) -> str
    async def claim(self, worker_id, slots) -> list[ClaimedTask]
        # SQLite: BEGIN IMMEDIATE; UPDATE … WHERE state='queued' ORDER BY priority LIMIT n
    async def heartbeat(self, task_id, lease_token) -> Command   # returns CANCEL → cooperative cancel
    async def finish(self, task_id, lease_token, result) -> None
```

**Current state.** A `TriggerEngine` ticks `Trigger`s (`CronTrigger` via `cronsim`,
`IntervalTrigger`) against durable per-trigger state in the state DB; due runs enqueue
(idempotency-keyed) onto a **durable run queue** (`work_queue` table). `worker.drain`
claims runs under a **lease**, heartbeats while executing (the heartbeat doubles as the
cooperative **cancellation** channel — `interlace cancel <id>` / `POST /runs/{id}/
cancel`), retries durably up to `max_attempts` with a per-attempt timeout, and executes
them as forced runs (so they pick up new data). Stream flushes enqueue the consuming
models with the watermark as the idempotency key. `interlace serve` ties tick → enqueue →
drain in one process (`interlace scheduler --once` for a single pass). No APScheduler — we
own the loop; `cronsim` only parses. Models declare `schedule: {cron: …}` or
`{every: …}`.

The lease columns on `work_queue` provide crash-reclaim of *work items* (a dead worker's
lease expires and the task is re-claimed). This is **not** leader election: there is no
`leases` table for singleton loops and no multi-node coordination — single-node runs all
loops directly. Backfill/catchup is `interlace run` (forced) and `interlace restate
--start … --end …` (marks intervals pending and cascades via lineage); there is no
separate `interlace backfill` command.

**Deliberately not built:** arbitrary-Python-task orchestration (Airflow's operator zoo),
distributed executors (K8s-pod-per-task, Celery), DAG-versioning UI, multi-region, and
any Redis dependency. We match Dagster on asset-centric scheduling + partitions, beat
Airflow/Dagster/Prefect on built-in durable ingestion (none have it), and concede their
executor ecosystems.

**Not yet built (roadmap, §14):** SLA monitors + alerting (`@model(sla=…)`, an
`AlertRouter`, an `alerts` table), leader election for multi-node singleton loops, and
the sensor triggers (freshness, upstream-completion, webhook) that a richer scheduler
would fire.

---

## 11. Service layer

**Litestar + msgspec + uvicorn** (the `service` extra):

- First-class SSE with `Last-Event-ID` replay; OpenAPI 3.1 generated from typed handlers;
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
  apply/run/stream/gc lifecycle: one spine.
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
  system — live over the SSE spine.

---

## 12. Scale-out path (the designed contract, not yet shipped)

The store/queue/log abstractions are Protocols so a single-node deployment can grow into
a shared-Postgres, multi-worker one **without a caller-visible redesign**. This table is
the *intended* contract; the right-hand column is roadmap (§14), not a shipped option —
only the single-node defaults exist today.

| Substrate | Single-node default (shipped) | Designed scale-out swap (roadmap) | Why no redesign |
|---|---|---|---|
| State DB / WorkQueue / EventLog | SQLite (WAL) | Postgres (`SKIP LOCKED`, advisory locks, LISTEN/NOTIFY) | claim/lease/fence semantics live in the Protocols |
| StreamLog | SQLite | Postgres → Redpanda/NATS; or object-store Arrow segments | offsets/leases/idempotency are interface-level concepts |
| Warehouse | DuckDB/DuckLake (SQLite catalog) | DuckLake on Postgres catalog; MotherDuck; Snowflake/BigQuery | watermark pattern works everywhere; DuckLake catalog swap is config |
| Workers | in-process claim loop | same loop, more processes/hosts — the queue is the protocol | nothing to redesign |

A cross-backend conformance suite for these swaps is *planned* (it would be the thing that
guarantees identical claim/lease semantics); it does not exist yet because the Postgres
backends it would test have not been built. Forever single-node-only regardless:
local-DuckDB-file concurrency, the SQLite backends, and a `ProcessPoolExecutor` (per
worker host).

---

## 13. Package layout & dependencies

```
src/interlace/
  dsl/         # @model @stream @check decorators; SQL file loader; project discovery
  ir/          # Relation types; canonicalisation; fingerprints; Arrow schema handling
  graph/       # dag (toposort, stdlib), column_lineage, selectors
  state/       # store (SQLite control plane + migrations), snapshot, interval, janitor (gc)
  plan/        # differ (sqlglot.diff + classification), plan, apply, run
  engines/     # base (EngineAdapter, EngineCaps); adbc (shared ADBC base); duckdb (+ DuckLake),
               #   postgres, redshift/snowflake/bigquery (alpha), spark (beta), quack, registry
  strategies/  # replace, view, full_merge, incremental_by_time, merge, scd
  checks/      # built-in check types + @check decorator — results gate promotion
  scheduler/   # triggers (cron/interval), engine (TriggerEngine), worker (leases/retries/cancel)
  runtime/     # execution context for Python models (Arrow handles)
  streaming/   # log (SqliteStreamLog), materializer (flush + watermark), schema (drift modes)
  service/     # app.py (litestar), auth.py, ui/ (the /ui web app)
  config/      # config load; ${VAR} + .env interpolation
  cli/         # init plan apply run restate gc scheduler serve models lineage env runs
               #   checks streams engines cancel apikey
  sinks.py     # terminal delivery helpers: external table target + file COPY
  project.py   # Project.load/compile; engine + state + stream-log opening
```

**Core dependencies** (exactly `[project.dependencies]` in `pyproject.toml`):

| Package | Constraint | Why |
|---|---|---|
| `sqlglot` | `>=25.0,<29.0` | Canonical IR, transpilation, qualification/type annotation, semantic diff, column lineage. The single most load-bearing dep. |
| `duckdb` | `>=1.5.3` | Default engine, federation hub, DuckLake, quack serving. |
| `pyarrow` | `>=17.0` | The wire format; RecordBatchReader everywhere. |
| `pydantic` v2 | `>=2.5,<3.0` | Config + manifest validation only (cold paths). |
| `typer` | `>=0.12,<1.0` | CLI. |
| `rich` | `>=13.0,<15.0` | CLI display, strictly an event subscriber. |
| `cronsim` | `>=2.5,<3.0` | Cron parsing for the trigger engine (we own the loop; APScheduler rejected). |
| `tenacity` | `>=8.2,<10.0` | Retries: tasks, DuckLake commit conflicts, transfers. |
| `pyyaml` | `>=6.0,<7.0` | Project config (config + env overlays). |

Logging is the **standard library `logging`** — there is no `structlog` dependency.

**Extras** (from `pyproject.toml`):

- **`service`** — the Litestar/uvicorn daemon: `litestar`, `uvicorn`, and **`msgspec`**
  (the wire types; msgspec is a service-extra dep, not core).
- **`adbc`** — the Postgres and Redshift engines via Arrow-native ADBC
  (`adbc-driver-manager`, `adbc-driver-postgresql`). **`adbc-snowflake`** / **`adbc-bigquery`**
  add those (alpha) drivers.
- **`spark`** — the Spark engine (beta): `pyspark` + `delta-spark` (Spark 4.0 / Delta 4.0),
  a `SparkSession` transport rather than ADBC.
- **`postgres`** — `psycopg[binary]`, reserved for the *future* Postgres state/log
  backends (§12); no such backend ships today.
- **`polars`** — `polars`, the preferred eager frame a user can build from
  `handle.table()`.
- **`pandas`** — `pandas`, compatibility only.
- **`all`** — `service,adbc,postgres,polars`.
- **`dev`** — test/lint toolchain: `pytest`, `pytest-asyncio`, `ruff`, `black`, `mypy`,
  and **`httpx`** (litestar's TestClient transport — httpx is dev-only, not a runtime
  dep). No `argon2-cffi` / `joserfc` (those would come with OIDC — roadmap) and no
  `watchfiles`.

**Build vs buy:** the StreamLog + WorkQueue are built over `sqlite3` — they *are* the
product; no off-the-shelf embeddable Python option has consumer groups/offsets/replay.

**Rejected outright:** pandas in core, Jinja2, SQLAlchemy, networkx (toposort is a few
dozen lines), APScheduler, Celery, Redis, Airflow-anything, ibis.

---

## 14. Roadmap — not yet built

Everything above (unless a note says otherwise) is shipped in v2.0. The following are
*designed for* but **not implemented**; they are collected here so the body can describe
only shipped behaviour:

- **Sensor triggers** — freshness (table-staleness), upstream-completion, webhook, and
  manual triggers beyond the shipped cron/interval (§2.6).
- **SLA + alerting** — `@model(sla=…)` sensors emitting breach events, an `AlertRouter`
  fanning out to Slack/webhook/email with a firing/resolved state machine, an `alerts`
  table, and UI alert history (§10).
- **Leader election / multi-node** — a `leases` table for singleton loops
  (TriggerEngine/janitor/flusher), Postgres `SKIP LOCKED` + advisory locks +
  LISTEN/NOTIFY, and multi-worker/multi-host operation (§10, §12).
- **Postgres state/log backends + conformance suite** — the OLTP control-plane swap
  SQLite→Postgres and the cross-backend test suite that would guarantee identical
  claim/lease semantics (§6, §12). The `postgres` extra ships the driver; the backend
  does not.
- **Cloud-warehouse adapters (alpha)** — Redshift, Snowflake, BigQuery and MotherDuck
  `EngineAdapter`s now ship (ADBC is Arrow-native end-to-end; they share one `AdbcAdapter`
  base), unlocking "author in Snowflake SQL, run it in Snowflake in prod" (§4, §5). They are
  wired and dialect-correct but **not yet run against a live account** — promoting them out of
  alpha needs live validation (connection strings, metadata probes).
- **Spark (beta)** — a `SparkSession` transport (Arrow via `toArrow`/`createDataFrame`),
  tested against a local Spark + Delta Lake session. `merge` and `incremental_by_time` run
  natively; `scd`/`full_merge` need a MERGE-based rewrite to work on Delta (which forbids
  subqueries in `UPDATE`/`DELETE` conditions). Databricks is still open: its connector is
  Arrow-native but lacks an `adbc_ingest` bulk-load, so `load()` needs a bespoke staged-COPY
  path.
- **Reverse-ETL SaaS connectors + delivery ledger** — a `SinkConnector` (batch HTTP) for
  API/SaaS destinations (a third terminal plane beyond `table`/`file`), a per-target
  delivery ledger (cursor / last-synced hash per key) for change-only pushes (§6).
- **First-class streaming models & outbound consumers** — a `kind="incremental_stream"`
  model with `on_stream(...)` triggers and a `ctx.stream_batch(...)` accessor; outbound
  consumer groups (webhook, RabbitMQ, …) with read→process→ack, `<stream>__dlq`
  dead-lettering, and GCRA rate limiting; and a DBSP-style incremental accelerator (§9).
- **Broker stream-log backends** — Postgres, Redpanda/Kafka, NATS JetStream, and an
  Arrow-IPC segment backend behind the `StreamLog` Protocol, plus a consumer-lag
  (`max_lag`) gate and richer retention (`max_events`/`min_unconsumed`) (§9.1).
- **OIDC / JWKS** — browser SSO for the UI (would add `argon2-cffi`, `joserfc`) on top of
  the shipped API-key auth (§11).
- **Iceberg / R2 interoperability sink** — landing stream/model output as Iceberg via
  DuckDB's REST catalog support (incl. Cloudflare R2 Data Catalog) and Parquet/JSON on
  object storage (§9).
- **Typed `{{ }}` vars** — a lintable, AST-resolved `@vars`/`ctx` templating layer for
  SQL (no vars machinery exists today) (§5).
- **Process-pool executor** — a `@model(executor="process")` opt-in mapping to a
  `ProcessPoolExecutor` (handles serialise as engine refs + AST; results return as Arrow
  IPC) (§3, §8).
- **Latency SLOs** — the target envelope (200-OK p99 < 25 ms; POST→queryable p95 < 1 s;
  POST→downstream start < 3 s) is a design goal, **not** a measured/tested guarantee (§9).

---

## Appendix — historical context

This document was written to justify a first-principles rebuild of the 0.x line; that
argument is preserved here in brief. It reasons against the old v0.2.1 codebase, which
does not live in this tree (it is on branch `v0`).

**Why the rebuild.** Three independent reviews of v0.2.1 found structural defects that
could not be patched incrementally: (1) broken laziness — ibis was a veneer, every model
boundary ran `.execute()` → pandas → `ibis.memtable()`; (2) dialect lock-in — strategies
emitted raw DuckDB SQL strings; (3) no state model — file-hash change detection only, no
versioned snapshots / virtual environments / plan-apply / interval backfill; (4)
non-durable streaming — in-memory asyncio queues (restart = loss), ack-before-process;
(5) cron-loop orchestration — one global run lock serialising all flows; (6) fake async —
sync `.execute()` blocking workers, a semaphore-of-fresh-connections "pool"; (7) a
1,000-line Executor coupled to the Rich display; (8) column lineage computed but never
used for planning. The design above maps each defect to a fix (sqlglot IR + Arrow
contract, snapshots + virtual environments, durable StreamLog, durable WorkQueue,
event-subscriber display, lineage-driven planning).

**Market timing (verified June 2026).** Fivetran completed its dbt Labs merger (June 1,
2026) having already acquired Tobiko (SQLMesh/SQLGlot, Sept 2025); SQLMesh went to the
Linux Foundation (March 2026) — both major transformation frameworks now sit in one
company's portfolio. dbt Fusion (Rust) is beta and ELv2-licensed. DuckLake 1.0 is
production-ready (April 2026). No OSS tool owns ingestion + transformation + orchestration
in one process — that is the niche.

**Scorecard vs sqlmesh & dbt.** Adopt: sqlmesh snapshots + fingerprints + virtual
environments + plan/apply + interval ledger (the state-of-the-art state model), and dbt's
selector syntax verbatim. Improve: sqlmesh change classification (column-level impact
narrows invalidation); sqlmesh Python models (lazy Arrow handles + streaming generators
instead of eager DataFrames); dbt `state:modified` (fingerprints in the state store, not
artifact diffing); dbt tests (typed checks + `@check`, gating promotion). Reject: the
Jinja/macro layer and `ref()`-as-text (Python is the macro language; references resolve
at the AST level), the external orchestrator (built-in durable work queue), and pandas as
interchange (Arrow only). Build (neither has it): durable streaming ingestion. *(The
"typed `@vars`" and first-class streaming models that this scorecard originally cited as
differentiators are design intent, not shipped — see §14.)*

**What ported from v0.2.1** (concepts, not code): the `@model`/`@stream`/`@check`
decorator DX; unified Python+SQL models; YAML config with env interpolation; the checks
subsystem (with results now *gating promotion*); the event-bus concept (made durable);
the `plan` CLI concept (upgraded to real plan/apply).

---

## Sources (verified June 2026)

DuckLake 1.0 (ducklake.select, 2026-04-13); DuckDB 1.5.3 + Iceberg features (duckdb.org,
2026-05); DuckDB concurrency docs; ducklake#233 (commit conflicts); ADBC driver status
(adbc-drivers.org, 2026-01); ibis releases (12.0.0, 2026-02) + Voltron Data layoffs (The
Information, 2024-11); sqlglot lineage API; Fivetran–dbt merger completion (fivetran.com,
2026-06-01); SQLMesh → Linux Foundation (2026-03-25); dbt Fusion ELv2 licensing;
Cloudflare Data Platform / Pipelines pricing (2026-05-11); Vector.dev buffering model
(disk_v2); APScheduler release status; Feldera/DBSP; litestar vs FastAPI benchmarks.
