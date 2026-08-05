# Data-quality checks

Checks run against a model's built table **before its environment view moves**. An
error-severity failure blocks promotion; a warning is recorded but doesn't block. Results
are stored in the state store (`check_results`) and surfaced by `interlace checks list`,
`GET /checks`, and the UI Checks view.

Each check compiles to a SQL query returning a single `failures` count; `failures = 0` is a
pass. Two families:

- **Column checks** operate on one or more columns and count failing rows: `not_null`,
  `unique`, `accepted_values`, `range`, `pattern`, `freshness`, and `relationships` (a
  per-row foreign-key check that references another model).
- **Table checks** evaluate a table-wide condition: `row_count`, `expression`, `sql`.

## Severity and gating

`severity: error` (the default) makes a failure **blocking** — `apply` raises `CheckError`
and the promotion is aborted (the environment view does not move). `severity: warn` records
the result without blocking. A check that errors at the engine level (bad SQL, missing
column) is recorded with status `error` and, at error severity, also blocks.

Status is one of `passed` (failures = 0), `failed` (failures > 0), or `error` (the check
query itself threw). `blocking = status != "passed" and severity == "error"`.

## Declaring checks

In a SQL model's `/* interlace: */` config block or a Python `@model(...)`:

```yaml
checks:
  - not_null: customer_id            # shorthand: {check_type: column}
  - unique: [customer_id, day]       # composite
  - accepted_values: {column: status, values: [active, churned]}
  - row_count: {min: 1}
  - {type: expression, expression: "amount >= 0", severity: warn}
```

Two forms are accepted: **shorthand** `{check_type: column}` / `{check_type: {params}}`,
and **explicit** `{type: ..., column: ..., ...params}`. Python `@check` functions attach by
model name for logic a SQL check can't express.

## The ten built-in types

| Type | Level | Params | Fails when |
|---|---|---|---|
| `not_null` | column | — | any value in the column is NULL |
| `unique` | column(s) | — | a value (or tuple) appears more than once |
| `accepted_values` | column | `values: [...]` | a non-NULL value is outside the allowed set |
| `range` | column | `min` and/or `max` | a value is `< min` or `> max` |
| `pattern` | column | `regex` | a non-NULL value doesn't match the regex |
| `freshness` | column | `max_age` (e.g. `24h`, `7d`) | the newest value is older than `now() - max_age`, or the table is empty |
| `row_count` | table | `min` and/or `max` | the row count is `< min` or `> max` |
| `expression` | table | `expression` (a boolean SQL predicate) | any row makes the predicate false |
| `relationships` | column | `to` (model), `field` (its column) | a non-NULL value has no matching row in the referenced model (orphan / foreign-key check) |
| `sql` | table | `query` | the custom query returns rows (its row count is the failure count); `{table}` in the query is substituted with the model's physical table |

`relationships` and `sql` reference *other* models; `apply` schedules those referenced
models to build first so the check runs against fresh data.

## Running checks

- **During `apply`** — every model's checks run after it builds; a blocking failure stops
  the apply before promotion.
- **Ad hoc** — `interlace checks run [--env E] [--select ...]` (CLI) or `POST /checks/run`
  (API) re-run checks against an environment's already-promoted tables without rebuilding,
  recording the results. Both use each snapshot's recorded engine, exit non-zero / report
  `blocking_failures` when an error-severity check fails, and skip sinks and
  declared-but-not-promoted models.
