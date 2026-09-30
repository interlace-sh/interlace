"""HTTP coverage for durable streams.

Publish, flush, drift, backpressure, and the timing ceiling live here so the
rest of the service tests stay a catalog of the other routes.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path

import httpx
import pytest
from litestar.testing import AsyncTestClient, TestClient
from service_support import _drained, _drive_sse, _make_project, _wait_for

from interlace.service.app import create_app

pytestmark = pytest.mark.unit


def test_stream_publish_and_inspect(tmp_path: Path) -> None:
    project_dir = _make_project(tmp_path)
    (project_dir / "models" / "clicks_stream.py").write_text(
        "from interlace import stream\n\n"
        '@stream("clicks", schema={"event_id": "string", "amount": "double"}, idempotency_key="event_id")\n'
        "def clicks(event):\n    return event\n"
    )
    with TestClient(app=create_app(project_dir, "dev")) as client:
        streams = client.get("/streams").json()
        assert [(s["name"], s["head"], s["watermark"]) for s in streams] == [("clicks", 0, 0)]

        # retention + pending are on the wire (parity with `interlace streams`)
        info = client.get("/streams").json()[0]
        assert info["pending"] == 0 and "retention" in info

        one = client.post("/streams/clicks", json={"event_id": "e1", "amount": 5.0}).json()
        assert one == {"accepted": 1, "deduplicated": 0, "last_offset": 1, "quarantined": 0}

        batch = client.post(
            "/streams/clicks",
            json=[{"event_id": "e1", "amount": 5.0}, {"event_id": "e2", "amount": 7.5}],  # e1 = retry
        ).json()
        assert batch["accepted"] == 1 and batch["deduplicated"] == 1

        _wait_for(_drained(client, "clicks"))  # the micro-batch flusher lands both events
        detail = client.get("/streams/clicks").json()
        assert detail["head"] == 2 and detail["watermark"] == 2  # durable and materialized
        assert detail["table"] == "streams.clicks"
        assert [e["event_id"] for e in detail["recent"]] == ["e1", "e2"]

        assert client.post("/streams/clicks", json={"event_id": "e3", "nope": 1}).status_code == 400
        assert client.post("/streams/ghost", json={}).status_code == 404
        _wait_for(lambda: any(e["type"] == "stream.flushed" for e in client.get("/events").json()))


async def test_stream_consumer_sse_replays_and_acks(tmp_path: Path) -> None:
    """External consumers tail the durable log over SSE and ack with a fenced commit.

    A frame is not an ack. A grouped tail holds the lease until the connection
    closes; another subscriber to that group is refused while it is held.
    """
    project_dir = _make_project(tmp_path)
    (project_dir / "models" / "clicks_stream.py").write_text(
        "from interlace import stream\n\n"
        '@stream("clicks", schema={"event_id": "string", "amount": "double"})\n'
        "def clicks(event):\n    return event\n"
    )
    app = create_app(project_dir, "dev")
    async with AsyncTestClient(app=app) as client:
        await client.post("/streams/clicks", json={"event_id": "e1", "amount": 1.0})
        await client.post("/streams/clicks", json={"event_id": "e2", "amount": 2.0})
        assert (await client.get("/streams/nope/events")).status_code == 404
        missing = await client.post("/streams/nope/commit", json={"group": "g", "offset": 1, "token": "x"})
        assert missing.status_code == 404

        status, headers, first, live = await _drive_sse(app, "/streams/clicks/events", stop_after=0)
        assert status == 200
        assert headers["content-type"].startswith("text/event-stream")
        assert headers.get("content-encoding") != "gzip"
        assert first.startswith(": ok")
        assert live == []  # no cursor: already-durable events are not replayed

        _status, _headers, _first, replay = await _drive_sse(
            app, "/streams/clicks/events", query="after=0", stop_after=2
        )
        assert [frame["id"] for frame in replay] == ["1", "2"]
        assert [json.loads(frame["data"])["payload"]["event_id"] for frame in replay] == ["e1", "e2"]

        _status, _headers, _first, resumed = await _drive_sse(
            app, "/streams/clicks/events", headers=[(b"last-event-id", b"1")], stop_after=1
        )
        assert json.loads(resumed[0]["data"])["offset"] == 2

        async def ack(records: list[dict[str, str]]) -> None:
            lease = json.loads(records[0]["data"])
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as other:
                held = await other.get("/streams/clicks/events", params={"group": "billing"})
                assert held.status_code == 409
                acked = await other.post(
                    "/streams/clicks/commit",
                    json={"group": "billing", "offset": 2, "token": lease["token"]},
                )
                assert acked.status_code == 201
                assert acked.json() == {"group": "billing", "committed_offset": 2}

        _status, _headers, _first, grouped = await _drive_sse(
            app, "/streams/clicks/events", query="group=billing", stop_after=3, on_ready=ack
        )
        assert grouped[0]["event"] == "lease"
        lease = json.loads(grouped[0]["data"])
        assert lease["group"] == "billing" and lease["committed_offset"] == 0
        assert "id" not in grouped[0]
        assert [json.loads(frame["data"])["offset"] for frame in grouped[1:]] == [1, 2]

        stale = await client.post(
            "/streams/clicks/commit",
            json={"group": "billing", "offset": 2, "token": lease["token"]},
        )
        assert stale.status_code == 400  # the tail released the lease when it was cancelled

        await client.post("/streams/clicks", json={"event_id": "e3", "amount": 3.0})
        _status, _headers, _first, again = await _drive_sse(
            app, "/streams/clicks/events", query="group=billing", stop_after=2
        )
        assert json.loads(again[0]["data"])["committed_offset"] == 2
        assert json.loads(again[1]["data"])["offset"] == 3


def test_stream_evolve_mode_over_http(tmp_path: Path) -> None:
    project_dir = _make_project(tmp_path)
    (project_dir / "models" / "signals_stream.py").write_text(
        "from interlace import stream\n\n"
        '@stream("signals", schema={"id": "string"}, on_schema_drift="evolve")\n'
        "def signals(event):\n    return event\n"
    )
    with TestClient(app=create_app(project_dir, "dev")) as client:
        assert client.get("/streams").json()[0]["on_schema_drift"] == "evolve"
        first = client.post("/streams/signals", json={"id": "a"}).json()
        assert first["accepted"] == 1

        drifted = client.post("/streams/signals", json={"id": "b", "region": "eu", "score": 9}).json()
        assert drifted["accepted"] == 1  # new fields became columns, not errors

        _wait_for(_drained(client, "signals"))  # drift evolved the table rather than erroring
        detail = client.get("/streams/signals").json()
        assert detail["recent"][-1]["region"] == "eu"


def test_stream_quarantine_mode_over_http(tmp_path: Path) -> None:
    project_dir = _make_project(tmp_path)
    (project_dir / "models" / "orders_stream.py").write_text(
        "from interlace import stream\n\n"
        '@stream("orders", schema={"id": "string", "total": "double"}, on_schema_drift="quarantine")\n'
        "def orders(event):\n    return event\n"
    )
    app = create_app(project_dir, "dev")
    with TestClient(app=app) as client:
        result = client.post(
            "/streams/orders",
            json=[
                {"id": "o1", "total": 5.0},
                {"id": "o2", "total": "not-a-number"},  # would 400 under reject
                {"id": "o3", "rogue_field": 1},
            ],
        ).json()
        assert result["accepted"] == 1 and result["quarantined"] == 2

        _wait_for(_drained(client, "orders"))
        detail = client.get("/streams/orders").json()
        assert detail["head"] == 1 and detail["watermark"] == 1  # only the good event flowed

        _status, _headers, _first, frames = asyncio.run(
            _drive_sse(app, "/streams/orders__quarantine/events", query="after=0", stop_after=2)
        )
        assert len(frames) == 2
        assert client.get("/streams/nope__quarantine/events").status_code == 404


def test_publish_and_flush_stay_inside_the_regression_ceiling(tmp_path: Path) -> None:
    """Durability is fsync-bound. CI guards a ceiling, not a 25 ms design target.

    Twenty single-event publishes must each return within 250 ms. The rows must
    be queryable (watermark caught up) within 2 s, and the consumer run must be
    queued within 3 s of the first publish.
    """
    project_dir = _make_project(tmp_path)
    (project_dir / "models" / "clicks_stream.py").write_text(
        "from interlace import stream\n\n"
        '@stream("clicks", schema={"event_id": "string", "amount": "double"}, idempotency_key="event_id")\n'
        "def clicks(event):\n    return event\n"
    )
    (project_dir / "models" / "click_totals.sql").write_text("SELECT sum(amount) AS total FROM streams.clicks")
    with TestClient(app=create_app(project_dir, "dev")) as client:
        started = time.perf_counter()
        samples: list[float] = []
        for index in range(20):
            one = time.perf_counter()
            response = client.post("/streams/clicks", json={"event_id": f"e{index}", "amount": 1.0})
            samples.append(time.perf_counter() - one)
            assert response.json()["accepted"] == 1
        assert max(samples) < 0.25
        _wait_for(_drained(client, "clicks"), timeout=2.0)
        _wait_for(lambda: len(client.get("/runs").json()) >= 1, timeout=3.0)
        assert time.perf_counter() - started < 8.0


def test_stream_flush_enqueues_consumer_models(tmp_path: Path) -> None:
    project_dir = _make_project(tmp_path)
    (project_dir / "models" / "clicks_stream.py").write_text(
        "from interlace import stream\n\n"
        '@stream("clicks", schema={"event_id": "string", "amount": "double"}, idempotency_key="event_id")\n'
        "def clicks(event):\n    return event\n"
    )
    (project_dir / "models" / "click_totals.sql").write_text("SELECT sum(amount) AS total FROM streams.clicks")
    (project_dir / "models" / "click_report.sql").write_text("SELECT total FROM click_totals")  # downstream too

    with TestClient(app=create_app(project_dir, "dev")) as client:
        client.post("/streams/clicks", json={"event_id": "e1", "amount": 5.0})
        _wait_for(lambda: len(client.get("/runs").json()) == 1)  # flush enqueues after materializing
        runs = client.get("/runs").json()
        assert len(runs) == 1
        assert runs[0]["flow_selector"] == ["click_report", "click_totals"]  # reader + its downstream

        client.post("/streams/clicks", json={"event_id": "e1", "amount": 5.0})  # dupe: nothing to flush
        _wait_for(_drained(client, "clicks"))
        assert len(client.get("/runs").json()) == 1  # nothing new enqueued

        client.post("/streams/clicks", json={"event_id": "e2", "amount": 1.0})  # new data: new run
        _wait_for(lambda: len(client.get("/runs").json()) == 2)


def test_publish_backpressure_returns_429(tmp_path: Path) -> None:
    """A durable-but-unmaterialized backlog past the limit rejects producers with
    429 instead of growing without bound; draining clears it."""
    project_dir = _make_project(tmp_path)
    (project_dir / "models" / "clicks_stream.py").write_text(
        "from interlace import stream\n\n"
        '@stream("clicks", schema={"event_id": "string", "amount": "double"})\n'
        "def clicks(event):\n    return event\n"
    )
    with TestClient(app=create_app(project_dir, "dev")) as client:
        ok = client.post("/streams/clicks", json={"event_id": "bp-1", "amount": 1.0})
        assert ok.status_code == 201

        state = client.app.state
        state.log_heads["clicks"] = 500
        state.flushed_heads["clicks"] = 0
        state.stream_max_pending = 100
        throttled = client.post("/streams/clicks", json={"event_id": "bp-2", "amount": 1.0})
        assert throttled.status_code == 429
        assert "behind" in throttled.json()["detail"]

        state.flushed_heads["clicks"] = 500  # the flusher caught up
        recovered = client.post("/streams/clicks", json={"event_id": "bp-3", "amount": 1.0})
        assert recovered.status_code == 201
