#!/usr/bin/env python3
"""Load generator for the `events` stream — the external producer.

Fires synthetic events at the daemon's ingestion endpoint in parallel batches,
standing in for your webhooks / event source. Start the daemon first:

    interlace serve --path .          # ingestion endpoint + materializer + scheduler

then, in another shell:

    python generate.py                # one burst of 1,000,000 events
    python generate.py --loop         # a million every minute, sustained
    python generate.py --total 20000  # a small burst to try it out

Events batch into arrays (one HTTP request carries many events) — that is what
lets a laptop push a million a minute. If the warehouse falls behind, the endpoint
returns 429 (backpressure) once ~100k events are un-materialized; this backs off
and retries, so throughput self-limits to whatever the materializer can flush.

Standard library only — nothing to install.
"""

from __future__ import annotations

import argparse
import itertools
import json
import random
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

EVENT_TYPES = ("view", "click", "add_to_cart", "purchase")
EVENT_WEIGHTS = (60, 25, 10, 5)

_ids = itertools.count(1)  # unique event_id per event; next() is atomic under the GIL


def _make_batch(n: int) -> list[dict]:
    now = time.strftime("%Y-%m-%dT%H:%M:%S")
    rows = []
    for _ in range(n):
        kind = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS)[0]
        rows.append(
            {
                "event_id": f"e{next(_ids)}",
                "user_id": random.randint(1, 100_000),
                "event_type": kind,
                "amount": round(random.uniform(5, 200), 2) if kind == "purchase" else 0.0,
                "ts": now,
            }
        )
    return rows


def _post(url: str, rows: list[dict]) -> tuple[int, object]:
    body = json.dumps(rows).encode()
    req = urllib.request.Request(url, data=body, headers={"content-type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()[:160]
    except OSError as exc:
        return 0, str(exc)[:160]


def _send_batch(url: str, size: int) -> dict:
    rows = _make_batch(size)
    for attempt in range(8):
        status, payload = _post(url, rows)
        if status == 429:  # backpressure: the warehouse is behind — slow down and retry
            time.sleep(min(2.0, 0.25 * (attempt + 1)))
            continue
        if 200 <= status < 300 and isinstance(payload, dict):  # publish returns 201 Created
            return {"accepted": payload.get("accepted", 0), "deduplicated": payload.get("deduplicated", 0)}
        return {"error": f"{status}: {payload}"}
    return {"error": "429 backpressure — gave up after backoff"}


def fire(url: str, total: int, batch: int, concurrency: int) -> None:
    sizes = [batch] * (total // batch)
    if total % batch:
        sizes.append(total % batch)
    started = time.perf_counter()
    accepted = deduped = errors = 0
    last_error = ""
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for result in pool.map(lambda size: _send_batch(url, size), sizes):
            if "error" in result:
                errors += 1
                last_error = result["error"]
            else:
                accepted += result["accepted"]
                deduped += result["deduplicated"]
    elapsed = time.perf_counter() - started
    rate = int(total / elapsed) if elapsed else 0
    note = f"  last error: {last_error}" if errors else ""
    print(
        f"  sent {total:,} in {elapsed:5.1f}s ({rate:,}/s) — "
        f"accepted={accepted:,} deduped={deduped:,} batch_errors={errors}{note}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fire synthetic events at the `events` stream.")
    parser.add_argument("--url", default="http://127.0.0.1:8000/streams/events")
    parser.add_argument("--total", type=int, default=1_000_000, help="events per burst")
    parser.add_argument("--batch", type=int, default=2_000, help="events per HTTP request")
    parser.add_argument("--concurrency", type=int, default=16, help="parallel in-flight requests")
    parser.add_argument("--loop", action="store_true", help="fire --total every minute, forever")
    args = parser.parse_args()

    print(f"→ {args.url}  ({args.total:,} events/burst, batch={args.batch}, concurrency={args.concurrency})")
    while True:
        minute_start = time.perf_counter()
        fire(args.url, args.total, args.batch, args.concurrency)
        if not args.loop:
            break
        time.sleep(max(0.0, 60.0 - (time.perf_counter() - minute_start)))  # hold the per-minute cadence


if __name__ == "__main__":
    main()
