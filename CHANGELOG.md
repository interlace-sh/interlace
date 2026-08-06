# Changelog

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
