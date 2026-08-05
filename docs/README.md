# interlace — reference documentation

The base reference for interlace, written from the shipped code (v2.0.0). interlace is a
single-process Python/SQL data platform: transformation (plan/apply over a fingerprinted DAG),
built-in orchestration, and durable streaming ingestion. Published to PyPI as `interlaced`
(import and CLI: `interlace`).

## Start here

- **[Concepts](concepts.md)** — models, snapshots, environments, the plan/apply lifecycle,
  the state store, and the three surfaces. Read this first.

## Authoring

- **[Models](models.md)** — defining SQL and Python models, materialisations, the full
  `@model` / config-key reference, and fingerprint-based rebuild-skip.
- **[Strategies](strategies.md)** — how each strategy turns a query into a table: `replace`,
  `append`, `merge`, `full_merge`, `incremental_by_time`, `scd` — across the
  owned and external planes, with the exact statements each emits and when to use it.
- **[Checks](checks.md)** — the ten built-in data-quality checks, severity, and promotion
  gating.
- **[Streaming](streaming.md)** — `@stream` ingestion, publishing, schema-drift modes,
  exactly-once materialisation, and reverse-ETL (terminal `table` / `file`).

## Operating

- **[Environments](environments.md)** — production vs sandboxes, promotion, rollback, drift,
  and garbage collection.
- **[Engines](engines.md)** — DuckLake / DuckDB / quack / Postgres, capabilities, and
  cross-engine transfers.
- **[Configuration](configuration.md)** — the full `interlace.yaml` reference.

## Interfaces

- **[CLI](cli.md)** — every command, option, and the selector grammar.
- **[HTTP API](api.md)** — every endpoint, auth scopes, and wire types.
- **[Web UI](ui.md)** — the ten views served at `/ui` and what each does.
- **[Surface parity](parity.md)** — the CLI ↔ API ↔ UI map and what's intentionally
  surface-specific.

## Quick start

```bash
pip install 'interlaced[service]'
interlace init my-project && cd my-project
interlace plan      # preview
interlace apply     # build + promote
interlace serve     # daemon: web UI at /ui + HTTP API + scheduler + streams
```

---

*These docs are written from the code directly. The broader design rationale and roadmap live
in [`architecture/architecture.md`](architecture/architecture.md).*
