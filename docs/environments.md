# Environments, promotion, and rollback

An **environment** is a set of views over immutable snapshot tables. Models build into
fingerprinted physical tables (`interlace__<schema>.<model>__<fp>`); an environment is just
a mapping from model name to the fingerprint currently promoted there, materialised as
views the environment's readers query.

- **Production** (`prod`) is the **unprefixed** namespace: its views are `main.orders`,
  `analytics.daily`, etc. — the names your BI tools already point at.
- Any other environment is a **prefixed sandbox**: environment `dev` materialises its views
  under `dev__main.orders`, `dev__analytics.daily`. Building into a new environment name
  creates it; nothing touches `prod`.

Because promotion is a view swap over shared immutable tables, it's atomic and cheap, and
two environments promoting the same fingerprint share one physical table.

## Promotion

`apply` builds the changed snapshots, runs their checks, and — only if checks pass —
repoints the environment's views at the new tables and records the new fingerprint mapping.
A model whose output is provably unchanged is *reused* (its view repoints to the existing
table, nothing is rebuilt). Deleting a model drops its view and demotes it.

`plan` previews all of this without touching anything; `apply` refuses to proceed when the
plan contains **breaking** changes unless given `--force` (CLI) / `force: true` (API).

## Drift

`interlace env list` / `GET /environments` show each environment's **drift**: how many
compiled models have a fingerprint different from the one promoted there. `--select
state:modified` scopes a plan/apply to exactly those drifted models (plus everything
downstream) — the CI diff.

## Rollback

Every promote records the environment's **full** model→fingerprint mapping as a numbered
*generation* in `promotion_history` (only when the mapping actually changed, so a busy
scheduler re-promoting identical fingerprints doesn't bury the real history). Rollback
repoints the environment's views at an earlier generation — **nothing rebuilds**, the views
just move:

```
interlace env rollback              # to the generation before the latest
interlace env rollback --to 3       # to a specific generation
interlace env rollback --list       # show the promotion history (generations)
```

or `POST /environments/{name}/rollback` with `{"generation": N}` (admin scope), and
`GET /environments/{name}/history` for the generations. Rollback itself records a new
generation, so it's reversible — apply again to return to the latest state. It requires the
target generation's snapshots to still exist; a target already reclaimed by `gc` is refused
per-model with a clear message (rebuild from an older definition instead). Ephemerals (no
snapshot) and sinks (no relation) are handled correctly. `trim_logs` caps the number of
generations kept per environment.

## Dropping and garbage collection

`interlace env drop <name>` / `DELETE /environments/{name}` removes an environment's views
and releases its snapshots to `gc` (dropping `prod` needs `--force` / `force=true`).
`interlace gc` / `POST /gc` then removes snapshot rows no environment references and drops
their physical tables — reference-aware, so a table shared through reuse survives as long as
any environment still points at it. `gc` also trims the event log, check results, and
finished runs older than 30 days, caps promotion history at the newest 50 generations per
environment, and sweeps expired stream events per their retention.
