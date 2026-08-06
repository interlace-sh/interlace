# event_stream

Durable event ingestion, end to end: an HTTP endpoint that swallows a firehose of
events, materializes them **exactly-once** into the warehouse, and drives a handful
of live rollups — all in one `interlace serve` process.

```
POST /streams/events ──▶ streams.events   (durable log → micro-batch materializer)
                          ├─ events_by_minute   (per-minute rollup; scheduled 1m + flush-triggered)
                          ├─ events_by_type     (breakdown; re-triggered by each flush)
                          └─ user_spend ─ top_users (view)
```

`generate.py` is the **external producer** — it stands in for your webhooks / event
source, firing synthetic events at the endpoint in parallel batches.

## Run it

Two shells:

```bash
cd examples/event_stream

# 1) the daemon: ingestion endpoint + micro-batch materializer + scheduler
interlace serve --path .

# 2) the producer (another shell): one burst of a million events
python generate.py
#   python generate.py --loop           # a million every minute, sustained
#   python generate.py --total 20000    # a small burst to start with
```

Watch it land:

```bash
curl -s localhost:8000/streams/events            # watermark climbs as events materialize
curl -s -X POST localhost:8000/apply -d '{}'     # rebuild the rollups on demand
curl -s localhost:8000/query -H 'content-type: application/json' \
  -d '{"sql": "SELECT * FROM events_by_type"}'   # or read a rollup straight out
```

Without the daemon it still plans and builds — the rollups just read an empty
`streams.events`:

```bash
interlace plan --path .
interlace apply --path .
```

## What it shows

- **Durable before the ack.** Every publish is fsynced to the stream log before the
  200 returns — "200 means it survives power loss," not just a process crash. Batched
  publishes amortize the fsync, which is what makes a million a minute feasible.
- **Exactly-once materialization.** A micro-batch flusher coalesces publishes into one
  warehouse write; an in-warehouse watermark means a crash mid-flush never
  double-counts. Re-POST the same `event_id` and it deduplicates (`"deduplicated"` in
  the ack, not a second row).
- **Backpressure, not blow-up.** The durable-but-unmaterialized backlog is capped
  (~100k events per stream); past that the endpoint returns **429** instead of eating
  memory and disk, and `generate.py` backs off. Throughput self-limits to whatever the
  materializer can flush.
- **Live models over a moving stream.** `events_by_minute` is scheduled every minute
  *and* re-triggered by each flush; `events_by_type`, `user_spend` and the `top_users`
  view rebuild whenever new events land — the same `plan` / `apply` / checks machinery
  as any other model.

## Knobs

- `generate.py`: `--total` / `--batch` / `--concurrency` (defaults 1,000,000 / 2,000 / 16),
  `--loop` for a sustained per-minute firehose.
- `models/events.py`: `on_schema_drift` (reject / evolve / quarantine), `retention` (how
  long the durable log keeps events), and `idempotency_key`.
