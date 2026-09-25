# Streaming ingestion

A `@stream` declares a durable ingestion endpoint. Publishing to it appends events to a
SQLite WAL log that is **fsynced before the 200 response** (a 200-OK means the event is
durable, surviving power loss, not just process crash). A micro-batch flusher then
materialises events **exactly-once into the warehouse** (data + watermark commit in
one transactional ``execute_all`` on DuckDB/ADBC engines; the durable log itself is
at-least-once) into a warehouse table `streams.<name>`, which SQL
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
| `name` | str | required | Stream identifier (positional); must match `[A-Za-z_][A-Za-z0-9_]*`. The table is `streams.<name>`. |
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

## Consuming

`GET /streams/<name>/events` is a Server-Sent Events tail of the durable log, for a
consumer that is not an Interlace model. Warehouse materialisation is separate and does
not use this cursor.

```bash
# live only — events already in the log are not replayed
curl -N localhost:8000/streams/orders/events

# replay from the start, then follow
curl -N 'localhost:8000/streams/orders/events?after=0'
```

Each data frame is `{"offset", "ts", "payload", "idempotency_key", "headers"}` and its
SSE `id` is the offset, so a reconnect with `Last-Event-ID` resumes there. The server
blocks until an event is appended; it does not poll the log. A comment frame is sent
on connect, and again after 15s of quiet, so a proxy keeps the connection. Delivery is
**at-least-once**: sending a frame does not acknowledge it.

A `group` query parameter takes that consumer group's lease and, unless you also pass a
cursor, resumes from the group's committed offset. The first frame is `event: lease`
with `{group, token, committed_offset}`. Ack with the token while the tail is still
open:

```bash
curl -X POST localhost:8000/streams/orders/commit \
  -H 'content-type: application/json' \
  -d '{"group": "billing", "offset": 42, "token": "<from the lease frame>"}'
```

A second subscriber to the same group gets **409** until the first disconnects (the
lease is released then, and the committed offset stays). A commit with a stale token is
**400**. The lease lasts 30s and is renewed for as long as the connection is open.
`<name>__quarantine` is the same kind of tail for rows the drift policy diverted.

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

`interlace reset --yes` (or `POST /reset`) clears the durable stream log and the
`streams` landing tables so ingestion starts empty again. It does not drop terminal
`table`/`file` destinations.

## Postgres CDC

A `cdc:` block on a `postgres` connection reads a logical replication slot
(`pgoutput`) and appends each change to a declared `@stream`. Deletes are a row
whose `_change` column is `delete`, so a downstream `merge` can drop them.
Inserts and updates use `insert` and `update`. The stored LSN advances only after
`flush_streams` has committed those log offsets: a crash re-reads from the last
confirmed LSN (at-least-once into the log, exactly-once into the warehouse, same
as HTTP publish). The daemon does this while `interlace serve` is running. The
slot and publication are created in Postgres; interlace does not create them.

```yaml
connections:
  app: {type: postgres, dsn: "postgresql://etl@db.internal:5432/app"}
cdc:
  orders:
    connection: app
    slot: interlace_orders
    publication: orders_pub
    tables: [public.orders]
    stream: orders
```

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
  `COPY` (overwrite; `strategy: replace`). `${date}`, `${datetime}`, and `${workspace}`
  expand in `path` when the file is written.
- **`materialise: table` (reverse ETL)** — `target: <alias>.<schema>.<table>` where `alias`
  is a database wired in via the project's `attach:` config (Postgres, SQLite, another
  DuckDB). `strategy` picks the delivery — the **same strategies as virtual models**, pointed
  at the external table: `replace` (DELETE all + INSERT — the live table is never dropped, so
  grants and readers survive), `append`, `merge`, `full_merge`, `incremental`
  (windowed DELETE + INSERT), and `scd`. The external table is only ever created,
  appended, or evolved under `schema.columns` (additive by default) — never dropped, and never
  rewritten by a breaking change. `reject` stops delivery when the live table is not a
  compatible superset; `ignore` issues no `ALTER`. See [models](models.md#indexes-and-constraints).
