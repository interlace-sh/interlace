# Interlace v2 — Greenfield Architecture

**Status:** Implemented (single-node). This document is the design rationale and the contract;
short *current state* notes in each section record where the implementation stands.
**Scope:** Clean-slate design of the whole platform. v0.2.1 is reference material, not a constraint.
**Goal:** A comprehensive, independent, MIT-licensed alternative to sqlmesh/dbt with built-in
orchestration (no Airflow) and durable real-time streaming ingestion (covering Cloudflare
data-platform-style functionality), deployable as a single process.
**Deployment model:** Single-node first — one process does everything brilliantly — with
abstractions designed so worker nodes can be added later without redesign.

---

## 0. Why a rebuild

A first-principles review of v0.2.1 (three independent code assessments) found structural
defects that cannot be patched incrementally:

| # | Defect | Evidence in v0.2.1 |
|---|--------|--------------------|
| 1 | **Broken laziness** — ibis is a veneer; every model boundary runs `.execute()` → pandas → `ibis.memtable()`, forcing full materialization in RAM | `materialization/table.py`, `core/execution/data_converter.py` |
| 2 | **Dialect lock-in** — strategies emit raw DuckDB SQL strings; parsing hardcodes `read="duckdb"` | `strategies/merge_by_key.py`, `lineage/sql_extractor.py` |
| 3 | **No state model** — file-hash change detection only; no versioned snapshots, virtual environments, plan/apply, or interval-based backfill | `core/execution/change_detector.py`, `core/state/store.py` |
| 4 | **Non-durable streaming** — publishes flow through in-memory asyncio queues (restart = loss), no backpressure, consumer-cursor races, ack-before-process | `core/stream.py`, `streaming/adapters/` |
| 5 | **Cron-loop orchestration** — one global run lock serializes all flows; no event triggers, timeouts, catchup, or multi-node safety | `service/server.py` |
| 6 | **Fake async** — sync `.execute()` blocks workers; the Postgres "pool" is a semaphore creating fresh connections | `connections/postgres.py`, `core/executor.py` |
| 7 | **God object** — a 1,000-line Executor coupled to the Rich display | `core/executor.py` |
| 8 | **Lineage wasted** — column lineage computed at run time, never used for planning | `lineage/`, `core/impact.py` |

Every numbered defect maps to a design section below: §2–3 fix 1 and 6; §4–5 fix 2; §6 fixes 3;
§9 fixes 4; §10 fixes 5; §8 and §11 fix 6 and 7; §7 fixes 8.

### Market timing (verified June 2026)

- Fivetran completed its merger with dbt Labs (June 1, 2026) and had already acquired Tobiko
  (SQLMesh + SQLGlot, Sept 2025); SQLMesh was donated to the Linux Foundation (March 2026).
  Both major transformation frameworks are now one company's portfolio.
- dbt Fusion (Rust engine) is still beta and ELv2-licensed; dbt Core's trajectory is subordinate
  to the commercial platform.
- DuckLake 1.0 is production-ready (April 2026); DuckDB 1.5.3 ships mature Iceberg writes,
  MERGE INTO, and REST catalog support (works against Cloudflare R2 Data Catalog).
- ADBC drivers are stable for Postgres, Snowflake, BigQuery, SQL Server, Redshift.
- No OSS tool owns ingestion + transformation + orchestration in one process. That is the niche.

---

## 1. Core thesis

> **The canonical IR is a sqlglot AST + Arrow schema. The canonical wire format is an Arrow
> `RecordBatchReader`. Materialization happens exactly once, at the sink, as a single native SQL
> statement executed inside the owning engine.**

The v0.2.1 failure mode was picking ibis as the spine and then breaking it at every joint. The
fix is not "use ibis harder" — it is to make the framework's internal contract independent of
any dataframe library:

- **sqlglot** is the most load-bearing dependency: parsing, qualification, type annotation,
  transpilation (31 dialects), semantic diff, and column lineage.
- **Arrow** is the only interchange format. pandas never appears in core (optional extra only).
- **ibis** is dropped entirely. Its two possible roles are both covered without it: as a data plane it
  sits on Arrow anyway, and as an expression builder it compiles to sqlglot — which *is* our
  IR, so a Python-authored logical model can simply return a sqlglot expression. Dropping it
  removes a heavyweight dependency and its governance risk (4 of 5 steering members are
  Voltron Data employees; Voltron laid off ~50% of staff in late 2024) for zero lost
  capability. Remote engines connect via ADBC, not ibis backends.

SQL models never leave the logical plane: a model selecting from three upstreams in the same
engine compiles to *one* `CREATE TABLE AS` / `MERGE INTO` executed inside that engine. Zero rows
enter the Python process.

---

## 2. The six core abstractions

### 2.1 `Relation` — what every model produces

```python
class Relation(Protocol):
    """Logical until a sink forces it."""
    schema: ArrowSchema                      # always known (declared or inferred)

class SqlRelation(Relation):                 # logical plane
    ast: sqlglot.exp.Select                  # canonical dialect, qualified, type-annotated
    engine: EngineRef                        # which engine can evaluate it natively

class StreamRelation(Relation):              # physical plane
    def reader(self) -> pa.RecordBatchReader: ...   # pull-based, batched, single-pass
```

### 2.2 `EngineAdapter` — the only place dialect-specific code lives

```python
class EngineAdapter(ABC):
    dialect: str                             # sqlglot dialect name
    caps: EngineCaps                         # supports_merge, supports_clone, supports_qualify, ...

    async def execute(self, ast: sqlglot.exp.Expression) -> None: ...
    async def fetch(self, ast) -> pa.RecordBatchReader: ...        # extract: engine → Arrow
    async def load(self, table: TableRef, reader: pa.RecordBatchReader,
                   mode: Literal["create", "append"]) -> None: ... # load: Arrow → engine
    async def create_view(self, name: TableRef, target: TableRef) -> None: ...

    def transpile(self, ast) -> str:
        return ast.sql(dialect=self.dialect)  # canonical AST → engine SQL, one line
```

### 2.3 `Snapshot` — versioned model state (sqlmesh, adopted)

```python
@dataclass(frozen=True)
class Snapshot:
    name: str                  # "silver.orders"
    fingerprint: str           # h(normalized_ast + strategy_config + sorted(upstream_fingerprints))
    metadata_hash: str         # comments/owner/tags — changes here never trigger rebuilds
    physical_table: TableRef   # "interlace__silver.orders__a1b2c3d4"
    intervals: IntervalSet     # which [start, end) ranges are filled
    change_category: ChangeCategory  # BREAKING | NON_BREAKING | METADATA | FORWARD_ONLY
```

Python models fingerprint as `h(dedented_function_source + closure_bytecode)` — honest about
Python's opacity — with a `volatility="deterministic"` escape hatch so pure refactors can be
applied `--forward-only`.

### 2.4 `Plan` — terraform-style change preview

```python
class Plan:
    environment: str
    changes: list[ModelChange]       # added / breaking / non-breaking / removed
    backfills: list[BackfillTask]    # (snapshot, interval_batch) pairs, column-impact-narrowed
    transfers: list[TransferEdge]    # explicit cross-engine data movement (never silent)
    virtual_updates: list[ViewSwap]  # instant promote operations
```

### 2.5 `Strategy` — AST builders, never strings

```python
class Strategy(ABC):
    def plan_statements(self, rel: SqlRelation, target: TableRef,
                        caps: EngineCaps, interval: Interval | None
                        ) -> list[exp.Expression]:
        """Return canonical-dialect ASTs. The adapter transpiles. NEVER returns strings."""
```

Built-ins: `full`, `view`, `ephemeral` (AST-spliced as a CTE into consumers at compile time),
`incremental_by_time` (interval predicate injected as an AST filter), `merge_by_key`
(`exp.Merge`; when `caps.supports_merge` is false, rewrite to `DELETE USING` + `INSERT` in a
transaction), `scd2` (update-expire + insert-new statement sequence from `exp` constructors).

### 2.6 `Trigger` — one abstraction for all scheduling

```python
class Trigger(Protocol):
    id: str
    def due(self, now: datetime, state: TriggerState) -> list[RunRequest]

# Implementations: CronTrigger, IntervalTrigger, StreamTrigger (consumes the event log),
# FreshnessTrigger (table staleness sensor), UpstreamTrigger (RunCompleted events),
# WebhookTrigger (POST /triggers/{id}/fire), ManualTrigger.

@dataclass
class RunRequest:
    flow_selector: list[str]        # model names / tags / selector syntax
    partition: Interval | None      # first-class data interval
    priority: int = 0               # backfills enqueue at priority −10
    idempotency_key: str            # e.g. "cron:daily_sales:2026-06-12" — dedupes refires
```

---

## 3. Python models without breaking laziness

Functions receive lazy `RelationHandle`s and may return anything Arrow-coercible:

```python
@model(materialise="table", strategy="merge_by_key", key="order_id")
def enriched_orders(ctx, raw_orders, fx_rates):
    # Path A (stays logical — preferred): compose/return a sqlglot expression; runs in-engine.
    return sqlglot.select("o.*", "fx.rate").from_("raw_orders o").join("fx_rates fx", on="...")

    # Path B (physical, streaming): bounded-memory single-pass Arrow batches.
    for batch in raw_orders.reader():
        yield transform(batch)

    # Path C (explicit eager — opt-in only, never implicit):
    df = raw_orders.polars()   # or .pandas() if the extra is installed
```

- Path A returns a sqlglot expression — already the IR — so the model **stays on the logical
  plane**: laziness is preserved through Python models that only compose.
- Path B yields a `StreamRelation`; the sink consumes the reader directly via `adapter.load()`
  (DuckDB registers it zero-copy; remote engines bulk-ingest via ADBC).
- Path C is the only place data is eagerly pulled into Python, and the user asked for it.

---

## 4. DuckDB's two roles; DuckLake as default storage

**Role 1 — the default local engine.** Physical storage defaults to **DuckLake** (Parquet data +
SQL catalog) rather than a monolithic `.duckdb` file:

- snapshot tables map naturally onto DuckLake tables;
- DuckLake snapshots give time-travel and cheap rollback for free;
- data inlining keeps small tables fast;
- the catalog starts as SQLite locally and moves to Postgres for teams — which unlocks
  **cross-process multi-writer** without changing a single model. (DuckLake commit conflicts
  need retry; a `tenacity` policy lives in the adapter.)

A plain `.duckdb` file remains a config option for zero-dependency toy projects.

**Current state.** `database: ducklake:.interlace/warehouse.ducklake` is the config default
(catalog file + `<catalog>.files/` Parquet directory; DuckDB opens the DuckLake as its primary
database). The full strategy surface — schemas, views, transactional DDL+DML (merge),
`DESCRIBE`, Arrow ingest — runs on DuckLake unchanged; the whole test suite executes against
it. Requires `duckdb>=1.5.3`.

**Serving the warehouse: the quack protocol.** DuckDB 1.5.3 ships **quack** (core extension,
beta): `CALL quack_serve('quack:host:port', token := ...)` turns the process holding the
warehouse into a server; clients speak the same SQL over HTTP. This is how interlace solves
**single-node multi-process access** *before* the Postgres-catalog tier: the daemon
(`interlace serve --quack quack:localhost:4213`) owns the DuckLake and serves it; CLI runs,
schedulers, and ad-hoc DuckDB clients set `database: quack:localhost:4213` (token via
`quack_token` config or `INTERLACE_QUACK_TOKEN`) and share it concurrently. The
`QuackAdapter` ships each statement through the `quack_query` table function (full SQL
pass-through with Arrow results — quack's catalog `ATTACH` only resolves the server's main
schema while in beta), sends multi-statement plans as one `BEGIN…COMMIT` payload so they stay
atomic server-side, and routes Arrow loads through the attached remote catalog. Verified
end-to-end: a second OS process ran `interlace apply` through quack while the daemon held the
DuckLake catalog lock. When quack's catalog mapping matures (stable targeted for DuckDB 2.0),
the adapter can switch to native `ATTACH` without touching callers.

**Current state (multi-engine).** Named engines: `engines:` config + `default_engine`
(top-level warehouse fields synthesise `default`); `engine:` on models — fingerprinted, so a
move is a BREAKING rebuild; snapshots record their owning engine and GC drops on the right one;
apply/CLI/worker/service route through a lazy `EngineRegistry`. Postgres is the first native
remote engine (ADBC; the `adbc` extra). Cross-engine dependencies become explicit transfer
edges in the plan — Arrow `fetch → load`, upgraded to a federated `ATTACH → CTAS` fast lane
when the source is attachable. Contract: docs/architecture/MULTI_ENGINE.md.

**Role 2 — the federation/transport hub.** When a model's inputs span engines, the planner
inserts an explicit **transfer edge**, visible in `interlace plan` output — no silent data
movement. Transfer execution picks the cheapest mechanism:

1. Source is Postgres/MySQL/SQLite/Parquet/Iceberg/DuckLake and target is local →
   DuckDB `ATTACH` / scanner extensions: federated query, no Python hop at all.
2. Otherwise → **ADBC**: `source.fetch()` → `pa.RecordBatchReader` → `target.load()`.
   Snowflake/BigQuery ADBC drivers are Arrow-native end-to-end.

When all of a model's inputs live in Snowflake, the model **runs in Snowflake**. DuckDB is never
an obligatory middleman. Arrow Flight is out of scope for v1; the `fetch/load` contract admits
it later.

---

## 5. SQL handling

- **Per-file dialect, canonical IR.** Project config sets `default_dialect` (default `duckdb`);
  any model may override (`-- dialect: snowflake` header or decorator arg). At load time every
  statement is parsed with its declared dialect, then normalized: `qualify` (expand `*`, resolve
  aliases), `annotate_types` against the project schema graph, normalize identifiers. From then
  on dialect is gone — it reappears only in `EngineAdapter.transpile()`. Author in Snowflake
  SQL, run on DuckDB in dev, Snowflake in prod.
- **Jinja macros: rejected.** Python is the macro language. SQL files may use `{{ }}` only for
  typed `@vars` / `ctx` accessors resolved to AST nodes at parse time — lintable, no text
  substitution. dbt's Jinja-first model is the root cause of its unparseable lineage.
- **`ref()` as text macro: rejected.** References resolve at the AST level during qualification.

---

## 6. State & environments

**State store:** SQLite (WAL mode) by default; Postgres for shared/team deployments (and it can
double as the DuckLake catalog). Versioned schema with built-in migrations:

```
snapshots, intervals, environments, work_queue (runs, with lease columns),
trigger_state, event_log, check_results, api_keys
```

(The stream log keeps its own SQLite database — `stream_events`, `stream_heads`,
`consumer_state` — and stream watermarks live in the warehouse, committed atomically with the
data; see §9.)

**Why SQLite and not the existing DuckDB.** The architecture has two planes that want opposite
things from a database, so it uses the right engine for each rather than forcing one to do both:

| Plane | Holds | Access pattern | Engine |
|---|---|---|---|
| Data plane | model tables, materializations | bulk scans/aggregations, few large writes | **DuckDB/DuckLake** (OLAP, columnar) |
| Control plane | state store, work queue, stream log | many tiny indexed point-writes, frequent durable commits | **SQLite → Postgres** (OLTP, row store) |

The control plane is an OLTP workload — claim a task row, heartbeat, commit a stream offset,
append an event, bump an interval — i.e. high-frequency, single-row, commit-heavy traffic.
Reasons DuckDB is the wrong tool for it:

1. **OLAP vs OLTP.** DuckDB is columnar, built to scan/aggregate large tables; thousands of
   small single-row commits per second is close to a pathological case. SQLite's B-tree row
   store does exactly that (WAL: ~10k–50k durable commits/s on one node), which is what makes
   "HTTP 200 ⇒ fsynced" achievable on the stream ingest path.
2. **Single-writer contention.** DuckDB allows one writer per file. Sharing the warehouse DuckDB
   for state would make every heartbeat/offset-commit contend with model materialization; a
   *separate* DuckDB file gains nothing (two files of an engine still doing OLTP badly).
3. **Concurrent claim semantics.** The work queue needs atomic multi-worker row claims —
   SQLite (`BEGIN IMMEDIATE`) and Postgres (`FOR UPDATE SKIP LOCKED`) express this cleanly; DuckDB's
   single-writer MVCC does not.
4. **Scale-out path.** The store protocols swap SQLite → Postgres — two OLTP row stores with
   identical transactional semantics. DuckDB isn't on that path (the multi-node target is Postgres).

The cost of "an additional technology" is near zero: `sqlite3` is in the Python standard library
(no dependency, no service), and DuckLake's catalog is *already* SQLite locally. This is the
conventional split (sqlmesh keeps state in a transactional DB; Airflow/Dagster use Postgres for
metadata, never the warehouse). SQLite is the default, not mandatory — the requirement is "a
transactional store separate from the analytical engine"; pointing the store at Postgres from
day one is the only other sanctioned option. What we do **not** do is back the OLTP control plane
with the OLAP DuckDB engine.

**Environment naming:** production (``prod``) is the *unprefixed* namespace —
its views live at ``<schema>.<model>`` (``main.orders``), which is what BI tools and consumers
connect to. Every other environment is a prefixed sandbox (``dev__main.orders``). CLI/API/daemon
default to prod; ``--env dev`` opts into a sandbox.

**Virtual data environments** (sqlmesh, adopted):

- Physical layer: `interlace__<schema>.<model>__<fp_short>` — one table per snapshot version.
- Virtual layer: `<env>__<schema>.<model>` views pointing at snapshot tables.
- `interlace plan dev` previews; `apply` backfills only missing (snapshot, interval) pairs —
  unchanged models in dev **reuse prod's physical tables** via views (instant dev environments,
  zero duplicate compute); `promote` repoints prod views (atomic, instant); `rollback` repoints
  back. A janitor GCs unreferenced snapshots past `retention: 14d`.

**Interval ledger** (sqlmesh, adopted): per snapshot, a compact set of filled `[start, end)`
ranges at the model's declared grain (`interval="1d"`). Backfill, catchup after downtime, and
restatement (`interlace restate model --start … --end …`) reduce to set arithmetic. Stream
cursors are the same structure with offset grain — one bookkeeping mechanism for batch and
streaming.

**Change classification:** `sqlglot.diff` between old and new canonical ASTs. Added column →
NON_BREAKING (and because qualification expanded `SELECT *`, we *know* who consumes what).
Changed expression → BREAKING, but **column-impact-narrowed** (§7): only downstream models that
actually consume the changed columns are invalidated. This is the concrete improvement over
sqlmesh, whose invalidation is model-granular.

**The indirect non-breaking rebuild-skip.** The differ
assigns every changed model an impact: *semantic* (pre-existing column data may differ — changed
expressions/filters/strategy/Python source, or any semantic upstream), *additive* (existing
columns provably identical, new ones appeared — strictly additive projections with everything
else canonically equal, so a WHERE change is never additive), or *clean* (output provably
identical). Clean models are **not rebuilt**: their new snapshot is recorded pointing at the
previous physical table and the environment view repoints there. The implementation needs no
column lineage — an indirectly-changed model's SQL is unchanged and was previously valid, so it
cannot reference newly-added upstream columns; the only leaks are a projection ``*`` (inherits
new columns → rebuild) and Python models (see whole upstream tables → always rebuild).
Correctness hinge: reference resolution consults recorded snapshots (a reused fingerprint lives
at an *older* physical table than its name implies), threaded through apply/resolve/runtime/
checks as a physical-table map.

**The column-pruned skip.** The §7 narrowing above, concretely: a
*semantic* direct change computes its provably-**touched** output columns (projection-only edit:
with both projection lists erased the queries are canonically identical, so the row set is
untouched and unchanged projections stay byte-identical), and each downstream computes the
columns it provably **consumes** from that upstream (qualified refs attribute per join alias;
unqualified refs only in single-source queries). Disjoint ⇒ the downstream is *clean* and skips.
Both proofs bail to "everything" on ambiguity: ``*``, DISTINCT, positional/computed GROUP BY,
a changed alias referenced from other clauses or sibling projections (lateral aliases), CTE
indirection, duplicate output names. Conservative by construction — a false "touched"/"consumed"
only costs a rebuild, never correctness.

### Reverse ETL & external sinks (where the snapshot+view layer stops)

**Current state.** ``attach: {alias: uri}`` config wires external databases
(Postgres/SQLite/DuckDB/...) into the warehouse engine at open; sink models declare
``export: {to: table, target: <alias>.<schema>.<table>, mode: ...}``. Modes: ``replace``
(DELETE + INSERT in place — the live table is never dropped, so grants and readers survive),
``append``, and keyed ``merge_by_key`` / ``full_merge``, which reuse the managed-model strategy
AST builders pointed at the external catalog. ``export.environments`` gates the side effect
(default: production only — see Safety below). Deferred: delivery ledger, SaaS/API connectors.

The fingerprinted-snapshot-plus-view layer only works because **interlace owns those tables** —
it can freely create `model__<fp>` shadow copies and atomically repoint a view. Reverse ETL
breaks every one of those assumptions, so it does **not** go through that machinery:

- the target is **owned by an external system** (a CRM object, a SaaS API, a live OLTP table an
  app reads/writes) — you can't shadow-and-swap a hardcoded table, and an API endpoint isn't a
  table at all;
- writes are **side-effecting and non-recreatable** — you can't roll back a Salesforce upsert by
  repointing a view;
- the natural semantics are **upsert/merge or append**, not create-and-replace.

So delivery is separated from transformation. A reverse-ETL output is a distinct category — a
**`sink`** (`@sink` / `@export`) that reads an already-built, already-checked **managed** model
and pushes deltas to a live destination. The transformation stays declarative, versioned,
lineage-tracked, and rollback-able; only the *delivery* is side-effecting. (This mirrors how
Census/Hightouch split "model in the warehouse" from "sync to destination.")

```python
@sink(source="silver.account_summary",  # a managed model = the source of truth
      destination="salesforce", target="Account",
      mode="upsert", key="account_id")   # mirror | upsert | append
```

- **No fingerprinted shadow table.** A sink writes to the *literal* destination, never
  `target__<fp>`; `compile_models`/`_physical_table` assign physical snapshot tables only to
  managed materialisations (`table`/`view`/`incremental`/`ephemeral`). The materialisation
  taxonomy gains a `sink`/`external` category that bypasses the physical layer.
- **Idempotency from the key, not recreation.** `upsert`/`merge_by_key` makes re-runs safe; for
  append-only destinations reuse the stream log's idempotency-key/dedup mechanism (§9).
- **Reuses control-plane state, not the physical layer.** The model's fingerprint detects "logic
  changed → full resync"; a per-sink **delivery ledger** (cursor / last-synced hash per key)
  pushes only changed rows — the point of reverse-ETL efficiency.
- **Connectors.** SQL destinations reuse `EngineAdapter.load` with a new `merge` mode; SaaS/API
  destinations go through a separate `SinkConnector` (batch HTTP), not `EngineAdapter`.

**Spectrum of output kinds and their rollback story:**

| Output kind | Physical model | Rollback |
|---|---|---|
| managed `table`/`view`/`incremental` | snapshot table + env view | instant view-swap, zero-copy dev envs |
| external SQL table interlace solely owns | fixed-name table, transactional replace/merge in place (stage → `BEGIN; swap; COMMIT`) | atomic within that DB, no cross-env views |
| reverse-ETL `sink` (live/shared table or API) | keyed upsert / append via connector + delivery ledger | **forward-correction only** — replay a window + keyed upsert converges; *not* view-swap rollback |

**Safety — the property that matters most:** virtual environments must never silently fan
side-effecting writes out to production. Sinks are environment-gated: the export only
*executes* when the plan's environment appears in its `environments` allow-list (default:
production only), so a dev apply never fires reverse-ETL at a live external table. In a
gated-off environment the sink's snapshot is still recorded so the plan settles — nothing
leaves the warehouse. This is the reverse-ETL analogue of the dev-environment isolation the
snapshot layer gives for free.

**Representation.** A sink is a **model with an `export` block** (the uniform approach — a sink
is a DAG node that runs a query and does I/O; the `@sink(source=…)` form above is future sugar
over it). `export` presence makes a model a sink: no snapshot table, no environment view, but
it is still fingerprinted (change-tracking) and built (so `interlace run` re-exports). File
destinations (`parquet`/`csv`/`json`) go through DuckDB `COPY`; table destinations deliver into
attached databases with `replace`/`append`/`merge_by_key`/`full_merge` (`exports.py`). Still to
come: SaaS-API destinations via `SinkConnector` and the delivery ledger.

```sql
/* interlace: { export: { to: parquet, path: exports/orders.parquet } } */
SELECT * FROM orders
```

---

## 7. Dependency graph, lineage, selective execution

- **Load-time, not run-time.** After canonicalization, run `sqlglot.lineage` per output column
  of every SQL model → a project-wide **column DAG**. Lineage is computed before any execution
  and *drives* planning (the column-pruned rebuild-skip, §6); the service computes it once at
  startup and serves it whole (`GET /lineage`, the UI's lineage canvas).
- **Python models** contribute table-level edges from function parameters. A Python model is a
  column-lineage barrier (all-to-all) — conservative and correct. (`@model(columns=…)` declares
  an *output contract* — column names/types validated after every build, before promotion —
  not lineage.)
- **Selector syntax:** dbt's adopted — `interlace run --select +silver.orders+ tag:finance`
  (`model`, `+model`, `model+`, `+model+`, `tag:x`; selectors union). dbt's `state:modified`
  needs no selector here: modified-ness is what the plan computes from fingerprints in the
  state store (improvement over dbt's fragile `--defer --state` workflow).
- **Impact analysis feeds plan:** changed columns → walk the column DAG → downstream models
  partition into *invalidated* (rebuild) vs *safe* (reuse the existing snapshot table). Also
  exposed to humans: `interlace lineage <model> --columns` (`--format dot` for Graphviz) and
  per-change impacted columns in `GET /plan`.

---

## 8. Concurrency model (single node)

Asyncio control plane (scheduler, state writes, event bus are genuinely async) over three
execution lanes:

1. **Remote engines** (Postgres/Snowflake/BigQuery): blocking driver calls in
   `asyncio.to_thread` against real bounded per-gateway pools (`concurrency: snowflake: 8`).
   A warehouse query holds a cheap thread; this gives true overlap. No semaphore-of-fresh-
   connections.
2. **Local DuckDB:** within one process DuckDB supports concurrent connections with optimistic
   MVCC — the single-writer limit is *cross-process*. One `duckdb.connect()` per process,
   `.cursor()` per task, scheduler holds per-table write locks (the DAG already guarantees no
   two tasks write one table; concurrent interval batches of the same model serialize or
   partition-write). Cap the local lane at ~2–4 concurrent statements — DuckDB parallelizes each
   query across cores internally. DuckLake later unlocks cross-process writers with zero
   abstraction change.
3. **Python models:** thread pool by default (most are IO-bound or release the GIL inside
   pyarrow/polars/numpy); `@model(executor="process")` opts into a `ProcessPoolExecutor` —
   handles serialize as engine refs + AST (tiny), results return as Arrow IPC streams.

Scheduler: ready-queue over `(snapshot, interval_batch)` tasks; priority = critical-path length;
per-lane caps; durable retries with backoff. The engine emits events to the EventBus; **Rich
display, JSON logs, and metrics are subscribers** — the v0.2.1 god-object/display coupling is
dissolved structurally.

---

## 9. Durable streaming

Vocabulary deliberately mirrors Cloudflare's **Streams → Pipelines → Sinks** model; the pitch is
"self-hosted Cloudflare Pipelines that lands in DuckDB/DuckLake/Iceberg."

**Current state.** `SqliteStreamLog` (WAL; offsets from 1,
idempotency-key dedup via partial unique index, consumer-group lease/commit with fencing
tokens, trim, long-poll read). `@stream` declarations publish at ``POST /streams/{name}`` —
schema-validated (``on_schema_drift: reject``; extra fields/wrong types → 400, missing → NULL),
durable before the 200, deduplicated on retry. The materializer flushes micro-batches into
``streams.<name>`` (declared fields + ``_offset``/``_ingested_at``) with the watermark committed
**in the same warehouse transaction** as the data — exactly-once without coordinating with the
log; SQL models just ``FROM streams.<name>``. Publish only appends (durable ack, no warehouse
work on the hot path); a signal-driven flusher task coalesces publishes into one warehouse
write moments later (``stream_flush_interval``, 50 ms default), applies flush pending events
before planning, and a clean shutdown drains the residue. A flush **triggers the models
that read the stream** (plus their downstream closure) through the durable run queue, with the
watermark as the idempotency key — repeated flushes debounce, new data re-enqueues.

All three ``on_schema_drift`` modes are implemented:
- **reject** (default): unknown fields / wrong types → 400 before durability; missing → NULL.
- **evolve**: unknown fields become real columns at flush time (type inferred from data;
  conflicting inferences widen to TEXT; ALTER ADD COLUMN IF NOT EXISTS + INSERT BY NAME —
  verified on DuckLake). Declared fields accept *widening* coercions (int→double,
  scalar→text/json); an incompatible type change still rejects — evolution never hides
  breakage. The log stores raw payloads; evolution happens at flush, so daemon catch-up
  evolves identically.
- **quarantine**: failing events divert durably to a shadow stream
  ``<name>__quarantine`` (error + raw payload JSON, materialized to its own table);
  valid events flow; the publish response reports the quarantined count.

Deferred: broker backends, rate limits.

### 9.1 `StreamLog` — the durable ingestion log

```python
class StreamLog(Protocol):
    """Durable, ordered, replayable per-stream log. At-least-once."""
    async def append(self, stream, events, idempotency_keys) -> AppendResult
        # MUST NOT return before durable (group-commit fsync).
        # Raises Backpressure when the bounded commit queue is full → HTTP 429.
    async def read(self, stream, after_offset, limit, wait=None) -> list[StoredEvent]
    async def lease(self, stream, group, *, ttl, owner) -> Lease | None
    async def commit(self, stream, group, offset, lease_token) -> None
        # Fencing tokens; atomic with the offset row.
    async def trim(self, stream, *, before_offset=None, before_ts=None) -> int
```

**Default backend: SQLite** (WAL, `synchronous=NORMAL`, one dedicated writer thread):

- **Group commit:** appends funnel into a bounded deque drained every ≤5 ms or 500 events in one
  transaction; awaiting futures resolve after commit. 200-OK stays p99 < 25 ms *and* means
  "durable". SQLite in this configuration sustains 10k–50k durable writes/s on one node.
- **Backpressure:** bounded deque → `429 Retry-After`; a second gate trips when consumer lag
  exceeds `max_lag`. No unbounded queues anywhere.
- **Idempotency:** optional `Idempotency-Key` header (or configured payload field) enforced by a
  partial unique index — dedup is transactional with the append; duplicates return the original
  offset.
- **The cursor race is dead by construction:** lease + commit are rows in the same transaction
  domain as the events, with fencing tokens. A crashed consumer's lease expires; the next
  claimant resumes from `committed_offset`. Worst case is redelivery (at-least-once), never
  loss — and table materialization dedups (below).
- **Retention:** per-stream `retention(max_age, max_events, min_unconsumed)`; the janitor trims
  below `min(committed_offset)` unless age forces it (lagging consumers get `ConsumerLapped`,
  Kafka semantics).

Optional backends behind the same protocol: Postgres (`SKIP LOCKED` leases — the cheap
multi-node path), Redpanda/Kafka, NATS JetStream (managed subprocess, not embedded) for
>50k events/s. An Arrow-IPC segment backend (Vector.dev disk_v2 style, directly readable by
DuckDB) is a planned optimization, not v1.

### 9.2 Ingest → table: `StreamMaterializer`

One tailing consumer per stream in group `_table`:

1. Lease, `read(after=committed, limit=5000, wait=…)`.
2. Flush at `batch_rows` (default 5000) or `batch_interval` (default **500 ms**, floor ~50 ms).
   Decode (msgspec) → `pyarrow.RecordBatch` → DuckDB Arrow ingestion.
3. **Effectively-exactly-once:** the warehouse transaction writes the batch *and* updates
   `stream_watermarks(stream, last_offset)` atomically; on restart, rows ≤ watermark are
   filtered even if the log redelivers.
4. Emit `StreamFlushed(stream, table, offset_range, rows)` — this is what triggers downstream
   models.

Ingest validation uses **msgspec** structs generated from the declared schema (~µs/event);
failures → 422 with row-level errors or quarantine table per
`on_schema_drift: evolve | reject | quarantine`. Additive evolution bumps `schema_version` and
ALTERs the target; narrowing is rejected.

### 9.3 Streaming models = incremental models

```python
@model(kind="incremental_stream",
       triggers=[on_stream("orders_raw", debounce="2s", max_wait="30s")])
def orders_enriched(ctx):
    new = ctx.stream_batch("orders_raw")        # Arrow batch of (committed, watermark]
    return new.join(ctx.table("dim.customers"), "customer_id")
```

Each such model is a consumer group `model:<name>`; the trigger engine turns `StreamFlushed`
into debounced `RunRequest`s; output write + offset commit follow the same watermark pattern. So
"streaming models" are micro-batch incremental models whose cursor is a log offset — **one
execution engine, no separate streaming runtime**. The model contract is changeset-shaped
(insert/update/delete batches + watermarks) so a DBSP-style engine (e.g. Feldera) can slot in
later as an optional accelerator; research confirmed nothing production-ready does IVM over
DuckDB today, and micro-batching covers the real use cases.

Outbound adapters (webhook, RabbitMQ, …) become consumer groups: **read → process → ack** (never
ack-first), redelivery via lease expiry, dead-letter to `{stream}__dlq` after N attempts.
Rate limiting is a GCRA counter row in the state DB (survives restart, multi-node safe on
Postgres, no Redis).

**Latency targets (documented and tested):** 200-OK p99 < 25 ms; POST → queryable p95 < 1 s at
defaults; POST → downstream model start < 3 s with debounce.

**Interoperability sink:** Iceberg via DuckDB's REST catalog support — including Cloudflare
R2 Data Catalog — plus Parquet/JSON on object storage.

### 9.4 End-to-end walkthrough: one webhook event

1. `POST /streams/orders_raw` with `Authorization: Bearer ilk_…`, `Idempotency-Key: order-991`.
2. Guard checks key hash + scope `streams:publish:orders_raw`; GCRA rate-limit row checked;
   msgspec validates against schema v3 (unknown field + `evolve` → schema v4 recorded, ALTER
   queued).
3. `StreamLog.append` → group-commit fsync → offset 18442 assigned → **200 `{offset: 18442}`**.
   Crash after this point loses nothing; deque full would have returned 429.
4. `StreamMaterializer` flushes at 500 ms: Arrow batch → DuckDB txn (append to `raw.orders` +
   watermark = 18442) → log commit → `StreamFlushed`. A crash between the two commits is
   filtered by the watermark: exactly-once in the table.
5. `TriggerEngine` (lease-holder) sees `StreamFlushed`; `on_stream` for `orders_enriched` fires
   a `RunRequest` (idempotency key `stream:orders_enriched:18442`). Planner persists a flow;
   dispatcher queues the task.
6. A worker slot claims it, runs under `asyncio.timeout(600)`, processes offsets
   `(17900, 18442]`, writes output + advances the model cursor transactionally. `RunCompleted`
   feeds `UpstreamTrigger`s and SLA sensors; SSE pushes the transition to the UI with replayable
   sequence ids.

Durable in <25 ms, queryable in <1 s, downstream model running in ~2–3 s — killable at any line
without loss or duplication in the table.

---

## 10. Orchestrator

**Durable `WorkQueue` in the state DB** — no global lock, no in-memory-only anything:

```python
class WorkQueue(Protocol):
    async def enqueue(self, task: TaskSpec) -> str
    async def claim(self, worker_id, slots) -> list[ClaimedTask]
        # SQLite: BEGIN IMMEDIATE; UPDATE … WHERE state='queued' ORDER BY priority LIMIT n
        # Postgres: … FOR UPDATE SKIP LOCKED — identical semantics, defined by the Protocol
    async def heartbeat(self, task_id, lease_token) -> Command   # returns CANCEL → cooperative cancel
    async def finish(self, task_id, lease_token, result) -> None
```

Pipeline: `TriggerEngine` (durable per-trigger state) → `RunRequest` → Planner (expands selector
+ partition into a persisted flow/task DAG) → Dispatcher (queues tasks when upstreams succeed) →
`WorkerPool` (N async slots; per-task `asyncio.timeout`; cancellation via the heartbeat
channel; durable retries `retries: 3, backoff: exp(2s, max=5m, jitter)`; per-model concurrency
keys so distinct flows run in parallel).

**Backfill/catchup first-class:** cron triggers carry `catchup: all | latest | none`; missing
intervals enqueue at priority −10 with bounded parallelism. `interlace backfill orders --start
2026-01-01` bulk-inserts pending intervals; `--restate` marks done intervals pending and
cascades via the lineage graph.

**SLA + alerting:** `@model(sla=SLA(freshness="30m", run_duration="10m",
on_breach=["slack:#data-alerts"]))` compiles to sensors emitting `SlaBreached`; an `AlertRouter`
consumes the event log and fans out (webhook/Slack/email) with a firing/resolved state machine
in the `alerts` table — UI alert history for free.

**Multi-node safety, built now:** a `leases` table (owner, token, expiry, heartbeat) provides
leader election for singleton loops (TriggerEngine, janitor, materializer coordinator);
single-node trivially holds all leases. Workers need no election — `claim()` is already mutually
exclusive. On Postgres, advisory locks + LISTEN/NOTIFY replace polling.

**Deliberately not built:** arbitrary-Python-task orchestration (Airflow's operator zoo),
distributed executors (K8s-pod-per-task, Celery), DAG-versioning UI, multi-region, and any Redis
dependency — Postgres covers every multi-node need. We match Dagster on asset-centric
scheduling + partitions, beat Airflow/Dagster/Prefect on built-in durable ingestion (none have
it), and concede their executor ecosystems.

**Current state.** A `TriggerEngine` ticks `Trigger`s (`CronTrigger` via `cronsim`,
`IntervalTrigger`) against durable per-trigger state in the state DB; due runs enqueue
(idempotency-keyed) onto a **durable run queue** (`work_queue` table). `worker.drain` claims
runs under a **lease**, heartbeats while executing (the heartbeat doubles as the cooperative
**cancellation** channel — `interlace cancel <id>` / `POST /runs/{id}/cancel`), retries
durably up to `max_attempts` with a per-attempt timeout, and executes them as forced runs (so
they pick up new data). Stream flushes enqueue the consuming models with the watermark as the
idempotency key. `interlace serve` ties tick → enqueue → drain in one process
(`interlace scheduler --once` for a single pass). No APScheduler — we own the loop; `cronsim`
only parses. Models declare `schedule: {cron: …}` or `{every: …}`. Still to come: leader
election for multi-node, SLA monitors + alerting, and further sensor triggers (freshness,
upstream-completion, webhook).

---

## 11. Service layer

**Litestar + msgspec + uvicorn** (the `service` extra):

- First-class SSE with `Last-Event-ID` replay; OpenAPI 3.1 generated from typed handlers
  (rendered via Scalar at `/schema/scalar`); msgspec-native serialization (~10× pydantic on hot
  paths — the publish endpoint shares msgspec structs with ingest validation); guards/DI for
  scoped auth. (FastAPI would work but drags pydantic onto the ingest hot path; APScheduler 4 is
  still pre-release in 2026 — we own the trigger loop, which is the product anyway.)
- **Auth:** scoped API keys (`read` / `write` / `admin`; `admin` satisfies any requirement) as
  `ilk_…` bearer tokens, sha256-hashed in the state DB. Auth enforces once at least one key
  exists — a fresh project stays open for local development; `interlace apikey create` locks it
  down. Routes declare their required scope; `/health` and `/schema` stay open.
- **Durable event spine:** events are rows in `event_log(seq, ts, type, entity, payload)` with
  in-process fanout. SSE reconnect and `GET /events?after=N` replay from the table — the UI
  never misses a transition across restarts. The same log records apply/run/stream/gc
  lifecycle: one spine.
- **Process composition:** `interlace serve` runs everything as supervised background tasks
  inside the app lifespan — the event tail (one store poller feeds every SSE client), the
  stream flusher (micro-batch materializer), and the scheduler loop. Background loops never die
  on an exception (log + retry); shutdown drains the stream residue and closes the store, log,
  and engines. **The rule that buys scale-out: components share zero objects** — they
  communicate only through the State DB, the StreamLog, and the Warehouse; `interlace serve
  --no-scheduler` plus a separate `interlace scheduler` process is that split today.
- **The web UI** ships inside the package (`service/ui/`, plain ES modules, zero build step),
  served at `/ui`: overview, lineage canvas with column-level tracing, models, plan/apply with
  SQL diffs, live runs, query console, streams, checks, environments, and system — live over
  the SSE spine.

---

## 12. Scale-out path (exact contract)

| Substrate | Single-node default | Scale-out swap | Why no redesign |
|---|---|---|---|
| State DB / WorkQueue / EventLog / leases | SQLite (WAL) | Postgres (`SKIP LOCKED`, advisory locks, LISTEN/NOTIFY) | claim/lease/fence semantics live in the Protocols; both backends pass one conformance suite |
| StreamLog | SQLite | Postgres → Redpanda/NATS; or object-store Arrow segments | offsets/leases/idempotency are interface-level concepts |
| Warehouse | DuckDB/DuckLake (SQLite catalog) | DuckLake on Postgres catalog; MotherDuck; Snowflake/BigQuery | watermark pattern works everywhere; DuckLake catalog swap is config |
| Workers | in-process claim loop | same loop, more processes/hosts — **no gRPC; the queue is the protocol** | nothing to redesign |

Forever single-node-only: local-DuckDB-file concurrency, SQLite backends,
`ProcessPoolExecutor` (per worker host, which is fine).

---

## 13. Package layout & dependencies

```
src/interlace/
  dsl/         # @model @stream @check decorators; SQL file loader; project discovery
  ir/          # Relation types; canonicalisation; fingerprints; Arrow schema handling
  graph/       # dag (toposort, stdlib), column_lineage, selectors
  state/       # store (SQLite control plane + migrations), snapshot, interval, janitor (gc)
  plan/        # differ (sqlglot.diff + classification), plan, apply, run
  engines/     # base (EngineAdapter, EngineCaps); duckdb (+ DuckLake), postgres (ADBC),
               #   quack, registry (named engines, lazy open)
  strategies/  # full, view, full_merge, incremental_by_time, merge_by_key, scd_type_2
  checks/      # 10 built-in types + @check decorator — results gate promotion
  scheduler/   # triggers (cron/interval), engine (TriggerEngine), worker (leases/retries/cancel)
  runtime/     # execution context for Python models
  streaming/   # log (SqliteStreamLog), materializer (flush + watermark), schema (drift modes)
  service/     # app.py (litestar), auth.py, ui/ (the /ui web app)
  config/      # pydantic config; ${VAR} + .env interpolation
  cli/         # init plan apply run restate gc scheduler serve models lineage env runs
               #   checks streams engines cancel apikey
  exports.py   # sinks: file + table delivery, environment gating
  project.py   # Project.load/compile; engine + state + stream-log opening
```

**Core dependencies (2026-verified):**

| Package | Why |
|---|---|
| `sqlglot` | Canonical IR, transpilation, qualification/type annotation, semantic diff, column lineage. The single most load-bearing dep. |
| `duckdb` ≥1.5 | Default engine, federation hub, DuckLake, Iceberg read/write incl. REST catalogs. |
| `pyarrow` | The wire format; RecordBatchReader everywhere; IPC for the process lane. |
| `msgspec` | Ingest validation + API serialization (~10× pydantic on hot paths); msgpack payloads. |
| `litestar` + `uvicorn` | Service layer: SSE, generated OpenAPI, guards, msgspec-native. |
| `pydantic` v2 | Config + manifest validation only (cold paths). |
| `typer` + `rich` | CLI; display strictly as an event subscriber. |
| `cronsim` | Cron parsing (replaces the hand-rolled parser; APScheduler 4 rejected — still pre-release, wrong shape: triggers must be durable in *our* state DB). |
| `tenacity` | Connector retries, DuckLake commit-conflict retries, transfers. |
| `structlog` | Logging (+ optional opentelemetry-sdk). |
| `httpx` | (service extra) Outbound alerts/webhooks (async, HTTP/2). |
| `argon2-cffi`, `joserfc` | (service extra) Reserved for OIDC/JWKS UI sessions. |
| `watchfiles` | (service extra) Dev-server hot reload. |

**Extras:** `service` (Litestar/uvicorn daemon), `adbc` (Postgres engine via ADBC), `postgres`
(psycopg 3 for future Postgres state/log backends), `polars` (preferred eager frame), `pandas`
(compat only), `all`. (`ibis-framework` is rejected — see §1.)

**Build vs buy:** the StreamLog + WorkQueue (~2–3 kLOC over `sqlite3`/`psycopg`) are built —
they *are* the product; no off-the-shelf embeddable Python option exists (NATS isn't
embeddable in-process; litequeue-class libraries lack consumer groups/offsets/replay).

**Rejected outright:** pandas in core, Jinja2, SQLAlchemy, networkx (toposort is 30 lines),
APScheduler 4, Celery, Redis, Airflow-anything, ibis (even as an extra — sqlglot is already
the expression IR and Arrow already the data plane; see §1).

---

## 14. Scorecard vs sqlmesh & dbt

| Concept | Verdict |
|---|---|
| sqlmesh snapshots + fingerprints + virtual environments + plan/apply + interval ledger | **Adopt** — the state-of-the-art state model |
| sqlmesh change classification | **Improve** — column-level impact narrows invalidation |
| sqlmesh Python models (eager DataFrame return) | **Improve** — lazy `RelationHandle` + streaming Arrow generators |
| sqlmesh Jinja/dbt-compat macro layer | **Reject** — Python is the macro language; typed AST-resolved `@vars` only |
| dbt selector syntax (`+model`, `tag:`) | **Adopt** verbatim |
| dbt `state:modified` via artifact diffing | **Improve** — fingerprints in the state store |
| dbt Jinja-first SQL, `ref()` as text macro | **Reject** — AST-level resolution |
| dbt tests as SQL macros | **Improve** — typed checks + `@check` (ported from v0.2.1); checks gate promotion |
| External orchestrator (both) | **Reject** — built-in durable work queue + unified triggers |
| Ingestion (neither has it) | **Build** — durable StreamLog, Cloudflare Streams/Pipelines/Sinks semantics |
| pandas as interchange (v0.2.1) | **Reject** — Arrow only |

---

## 15. What ports from v0.2.1

Concepts, not code: the `@model`/`@stream`/`@check` decorator DX; unified Python+SQL models;
YAML config with env interpolation; the checks subsystem (10 check types) with results now
*gating promotion*; the event-bus concept (made durable); the `plan` CLI concept (upgraded to
real plan/apply).

## 16. Phasing

All five planned phases are built: **core** (`ir/`, `engines/`, `state/`, `plan/`,
`strategies/`, `graph/` — plan/apply/run against local DuckLake with virtual environments),
**orchestrator + service** (durable run queue with leases/retries/cancellation, cron/interval
triggers, Litestar API, durable events, the `/ui` app), **streaming** (StreamLog,
materializer, all three drift modes, consumer triggering, backpressure), **remote engines**
(Postgres via ADBC, the transfer planner with the attach fast lane), and **polish** (checks
gating promotion, selectors, sinks with environment gating).

Open items live with their sections: SLA monitors/alerting, sensor triggers, and multi-node
leader election (§10); SaaS sink connectors and the delivery ledger (§6); broker stream-log
backends and rate limits (§9); a second cloud warehouse adapter (MULTI_ENGINE.md); the
Iceberg/R2 interoperability sink (§9).

---

## Sources (verified June 2026)

DuckLake 1.0 (ducklake.select, 2026-04-13); DuckDB 1.5.3 + Iceberg features (duckdb.org,
2026-05); DuckDB concurrency docs; ducklake#233 (commit conflicts); ADBC driver status +
ADBC Driver Foundry (adbc-drivers.org, 2026-01); ibis releases (12.0.0, 2026-02) + Voltron Data
layoffs (The Information, 2024-11); sqlglot lineage API; Fivetran–dbt merger completion
(fivetran.com, 2026-06-01); SQLMesh → Linux Foundation (2026-03-25); dbt Fusion ELv2 licensing;
Cloudflare Data Platform / Pipelines pricing (2026-05-11) / Queues free plan (2026-02-04);
Vector.dev buffering model (disk_v2); Redpanda Connect transaction model; APScheduler release
status; Feldera/DBSP; litestar vs FastAPI benchmarks.
