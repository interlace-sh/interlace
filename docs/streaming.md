# Streaming ingestion

A `@stream` declares a durable ingestion endpoint. Publishing to it appends events to a
SQLite WAL log that is **fsynced before the 200 response** (a 200-OK means the event is
durable, surviving power loss, not just process crash). A micro-batch flusher then
materialises events **exactly-once** into a warehouse table `streams.<name>`, which SQL
models read like any other table.

```python
from interlace import stream

@stream("orders", schema={"order_id": "string", "customer_id": "int", "total": "double"},
        idempotency_key="order_id", retention="7d")
def orders(event):
    return event
```

`interlace init --template events` scaffolds a complete, runnable streaming project (ingestion
endpoint, live rollups, and a load generator).

## `@stream` config

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | str | required | Stream identifier (positional); the table is `streams.<name>`. |
| `schema` | map | required | `{field: type}` declared shape. Types: `int`/`integer`/`bigint`, `double`/`float`/`decimal`, `string`/`text`/`varchar`, `bool`/`boolean`, `timestamp`, `json`. |
| `idempotency_key` | str | — | Payload field used to dedupe; a repeat publish of the same key is deduplicated. |
| `retention` | str | — | Age after which materialised events are swept (e.g. `7d`); unset = kept forever. |
| `on_schema_drift` | str | `reject` | Drift policy — see below. |

## Publishing

`POST /streams/<name>` with a single JSON object or an array of them (write scope). The
response is a `PublishResult`: `accepted`, `deduplicated`, `last_offset`, `quarantined`.
Publishing is durable immediately; materialisation into `streams.<name>` follows within the
flush interval. There is no CLI publish — publishing is an HTTP operation.

**Offsets and watermarks.** Each stream has a monotonic offset (the log `head`) and a
`watermark` (the highest offset materialised into the warehouse); their difference is the
pending backlog. When the pending count exceeds `stream_max_pending` (100 000), the publish
endpoint returns **HTTP 429** — the warehouse is behind; retry with backoff.

**Exactly-once.** The flusher drains everything past the watermark in micro-batches; each
batch stages an Arrow batch and moves `stage → target table + watermark` in one engine
transaction. A crash leaves either the old watermark (events re-read, stage overwritten — no
duplicates) or the new one. The watermark lives *in the warehouse* (`streams._watermarks`)
so it commits atomically with the data. A flush enqueues the models that read the stream (an
apply also flushes first, so it always sees every accepted event).

## Schema drift (`on_schema_drift`)

Events are validated **before** they become durable. Missing declared fields become NULL.
The policy for fields that don't match the declared schema:

- **`reject`** (default) — refuse the batch on the first unknown field or wrong type
  (`StreamError` → HTTP 400). Nothing is stored.
- **`evolve`** — unknown fields are welcome; they become real columns on `streams.<name>` at
  flush time. An *incompatible* type change on a declared field still rejects, so evolution
  never hides breakage.
- **`quarantine`** — failing payloads are diverted to a shadow stream `<name>__quarantine`
  (durable, with the error + raw payload) while the good rows proceed; the publish reports
  `quarantined: N`.

## Reverse-ETL: terminal `table` / `file`

A `materialise: table` or `materialise: file` model is **terminal** — it delivers its
(resolved) query result to a destination interlace does *not* own, producing no managed
snapshot table and no environment view. Terminal models are **environment-gated** (default:
`prod` only) so a dev apply never fires a side effect at a live destination; widen with
`environments: [dev, prod]`.

```sql
/* interlace:
  materialise: table
  target: crm.main.customer_scores
  strategy: merge
  key: customer_id
*/
SELECT customer_id, score FROM customer_value
```

- **`materialise: file`** — `format: parquet | csv | json` + `path`, written via DuckDB
  `COPY` (overwrite; `strategy: replace`).
- **`materialise: table` (reverse ETL)** — `target: <alias>.<schema>.<table>` where `alias`
  is a database wired in via the project's `attach:` config (Postgres, SQLite, another
  DuckDB). `strategy` picks the delivery — the **same strategies as virtual models**, pointed
  at the external table: `replace` (DELETE all + INSERT — the live table is never dropped, so
  grants and readers survive), `append`, `merge`, `full_merge`, `incremental`
  (windowed DELETE + INSERT), and `scd`. The external table is only ever created,
  appended, or evolved additively — never dropped, and never mutated by a breaking change.
