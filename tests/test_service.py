"""HTTP API (Litestar) — exercised with the in-process test client."""

from __future__ import annotations

import shutil
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from litestar.testing import TestClient

from interlace.project import Project
from interlace.service.app import create_app

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    """Publishing is durable immediately but materializes via a micro-batch flusher."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError("condition not met within timeout")


def _drained(client: TestClient, name: str) -> Callable[[], bool]:
    def check() -> bool:
        detail = client.get(f"/streams/{name}").json()
        return bool(detail["head"]) and detail["watermark"] == detail["head"]

    return check


def _make_project(tmp_path: Path) -> Path:
    project_dir = tmp_path / "getting_started"
    # never copy runtime state: a locally-exercised example must not poison tests
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))
    return project_dir


@pytest.fixture()
def client(tmp_path: Path) -> Iterator[TestClient]:
    with TestClient(app=create_app(_make_project(tmp_path), "dev")) as test_client:
        yield test_client


def test_health(client: TestClient) -> None:
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert body["version"]  # the UI's nav foot shows it


def test_ui_shell_is_served(client: TestClient) -> None:
    """The daemon serves the in-package UI; / redirects to it."""
    page = client.get("/ui/")
    assert page.status_code == 200
    assert "text/html" in page.headers["content-type"]
    assert 'id="rail"' in page.text  # the shell chrome
    root = client.get("/", follow_redirects=False)
    assert root.status_code in (301, 302, 307, 308)
    assert root.headers["location"].rstrip("/") + "/" == "/ui/"

    # every module the shell imports must actually be in the package
    ui_dir = Path(__file__).resolve().parents[1] / "src" / "interlace" / "service" / "ui"
    for asset in sorted(p.relative_to(ui_dir) for p in ui_dir.rglob("*") if p.is_file()):
        assert client.get(f"/ui/{asset}").status_code == 200, f"missing UI asset: {asset}"
    app_js = (ui_dir / "js" / "app.js").read_text()
    for view in (ui_dir / "js" / "views").glob("*.js"):
        assert f"views/{view.name}" in app_js, f"view {view.name} is not routed in app.js"


def test_ui_sends_security_headers(client: TestClient) -> None:
    """The air-gapped UI is served with a same-origin CSP + sniff/frame protection,
    scoped to /ui so the API and OpenAPI docs are untouched."""
    page = client.get("/ui/")
    csp = page.headers.get("content-security-policy", "")
    assert "default-src 'self'" in csp
    assert "script-src 'self'" in csp and "'unsafe-inline'" not in csp.split("script-src")[1].split(";")[0]
    assert "frame-ancestors 'none'" in csp
    assert page.headers.get("x-content-type-options") == "nosniff"
    assert page.headers.get("x-frame-options") == "DENY"
    # a static asset carries it too; the JSON API does not (headers are /ui-scoped)
    assert "default-src 'self'" in client.get("/ui/app.css").headers.get("content-security-policy", "")
    assert "content-security-policy" not in client.get("/health").headers


def test_environments_carry_promoted_at(client: TestClient) -> None:
    client.post("/apply", json={"environment": "prod"})
    envs = client.get("/environments").json()
    assert envs and envs[0]["promoted_at"]


def test_list_models(client: TestClient) -> None:
    body = client.get("/models").json()
    names = {m["name"] for m in body}
    assert {"raw_events", "event_totals", "recent_clicks"} <= names
    recent = next(m for m in body if m["name"] == "recent_clicks")
    assert recent["output"] == "view"


def test_model_detail_with_lineage(client: TestClient) -> None:
    resp = client.get("/models/event_totals")
    assert resp.status_code == 200
    body = resp.json()
    assert body["upstream"] == ["raw_events"]
    assert "top_kind" in body["downstream"]
    assert body["columns"]["total_amount"] == ["raw_events.amount"]
    assert "from raw_events" in body["sql"].lower()


def test_column_impact_endpoint(client: TestClient) -> None:
    """GET /models/{name}/impact mirrors the CLI `interlace impact` — the column
    blast radius, previously CLI-only."""
    body = client.get("/models/raw_events/impact", params={"column": "amount"}).json()
    assert body["source"] == "raw_events.amount"
    hits = {(row["model"], row["column"]) for row in body["impacted"]}
    assert ("event_totals", "total_amount") in hits  # total_amount = sum(amount)
    assert client.get("/models/nope/impact", params={"column": "x"}).status_code == 404


def test_unknown_model_is_404(client: TestClient) -> None:
    assert client.get("/models/nope").status_code == 404


def test_plan_lists_pending_models(client: TestClient) -> None:
    body = client.get("/plan", params={"environment": "prod"}).json()
    assert body["environment"] == "prod"
    assert {c["name"] for c in body["changes"]} >= {"raw_events", "event_totals"}


def test_create_and_list_runs(client: TestClient) -> None:
    created = client.post("/runs", json={"selectors": ["event_totals"], "environment": "prod"}).json()
    assert created["enqueued"] == 1
    assert created["models"] == ["event_totals"]

    runs = client.get("/runs").json()
    run = next(r for r in runs if r["flow_selector"] == ["event_totals"])
    # the enqueue key's prefix names the trigger (api: for POST /runs)
    assert run["idempotency_key"].startswith("api:prod:")


def test_create_run_rejects_bad_selector(client: TestClient) -> None:
    assert client.post("/runs", json={"selectors": ["nope"]}).status_code == 400


def test_create_run_with_window_and_restate(client: TestClient) -> None:
    created = client.post(
        "/runs",
        json={
            "selectors": ["event_totals"],
            "environment": "prod",
            "start": "2026-07-01T00:00:00",
            "end": "2026-07-02T00:00:00",
            "restate": True,
        },
    ).json()
    assert created["enqueued"] == 1
    run = next(r for r in client.get("/runs").json() if r["flow_selector"] == ["event_totals"])
    assert run["partition"] == ["2026-07-01T00:00:00", "2026-07-02T00:00:00"]
    assert run["restate"] is True

    assert client.post("/runs", json={"selectors": ["event_totals"], "start": "not-a-time"}).status_code == 400


def test_events_endpoint_records_enqueue(client: TestClient) -> None:
    assert client.get("/events").json() == []  # empty to start
    client.post("/runs", json={"selectors": ["raw_events"], "environment": "prod"})

    events = client.get("/events").json()
    assert [e["type"] for e in events] == ["run.enqueued"]
    assert events[0]["payload"] == {"models": ["raw_events"]}
    assert events[0]["seq"] == 1
    # replay from a cursor returns nothing new
    assert client.get("/events", params={"after": events[0]["seq"]}).json() == []


def test_models_enriched(client: TestClient) -> None:
    body = client.get("/models").json()
    recent = next(m for m in body if m["name"] == "recent_clicks")
    assert recent["materialise"] == "view"
    assert recent["is_terminal"] is False
    assert recent["fingerprint"]  # compiled fingerprint surfaced for the catalog
    assert {"owner", "schedule", "tags"} <= recent.keys()


def test_plan_carries_sql_and_fingerprints(client: TestClient) -> None:
    body = client.get("/plan", params={"environment": "prod"}).json()
    change = next(c for c in body["changes"] if c["name"] == "event_totals")
    assert change["change_type"] == "added"  # nothing promoted yet
    assert change["previous_fingerprint"] is None
    assert change["new_fingerprint"]
    assert change["new_sql"]  # SQL model carries its canonical definition for diffing


def test_plan_preview_accepts_select_and_forward_only(client: TestClient) -> None:
    """What you preview must be what POST /apply will do — same select grammar,
    same forward_only semantics."""
    body = client.get("/plan", params={"environment": "prod", "select": "raw_events"}).json()
    assert {c["name"] for c in body["changes"]} == {"raw_events"}

    assert client.get("/plan", params={"environment": "prod", "forward_only": "true"}).status_code == 200
    assert client.get("/plan", params={"select": "nope:bad"}).status_code == 400  # selector errors are 4xx


def test_apply_builds_promotes_and_clears_plan(client: TestClient) -> None:
    assert client.get("/environments").json() == []  # nothing promoted yet

    applied = client.post("/apply", json={"environment": "prod"}).json()
    assert applied["environment"] == "prod"
    assert applied["built"]  # built at least one model
    assert applied["promoted"] > 0

    assert client.get("/plan", params={"environment": "prod"}).json()["changes"] == []  # now up to date

    envs = {e["name"]: e for e in client.get("/environments").json()}
    assert envs["prod"]["models"] > 0
    assert envs["prod"]["changed"] == 0  # no drift after apply

    again = client.post("/apply", json={"environment": "prod"}).json()  # re-applying is a no-op
    assert again["built"] == [] and again["promoted"] == 0


def test_run_detail_includes_lifecycle_events(client: TestClient) -> None:
    client.post("/runs", json={"selectors": ["raw_events"], "environment": "prod"})
    run = client.get("/runs").json()[0]
    assert run["enqueued_at"]

    detail = client.get(f"/runs/{run['id']}").json()
    assert detail["id"] == run["id"]
    assert detail["flow_selector"] == ["raw_events"]
    assert any(e["type"] == "run.enqueued" for e in detail["events"])


def test_unknown_run_is_404(client: TestClient) -> None:
    assert client.get("/runs/99999").status_code == 404


def test_cancel_run_endpoint(client: TestClient) -> None:
    client.post("/runs", json={"selectors": ["raw_events"], "environment": "prod"})
    run = client.get("/runs").json()[0]
    cancelled = client.post(f"/runs/{run['id']}/cancel").json()
    assert cancelled == {"id": run["id"], "state": "cancelled"}  # queued: immediate
    assert client.post(f"/runs/{run['id']}/cancel").status_code == 404  # already finished


def test_openapi_and_scalar_docs(client: TestClient) -> None:
    schema = client.get("/schema/openapi.json").json()
    assert {"/models", "/runs", "/apply", "/environments"} <= schema["paths"].keys()
    assert "/runs/{run_id}" in schema["paths"]
    assert client.get("/schema/scalar").status_code == 200  # Scalar UI


def test_checks_endpoint_returns_recorded_results(tmp_path: Path) -> None:
    project_dir = _make_project(tmp_path)
    model = project_dir / "models" / "checked.sql"
    model.write_text("/* interlace: {checks: [{not_null: kind}]} */\nSELECT kind FROM event_totals")

    with TestClient(app=create_app(project_dir, "dev")) as client:
        assert client.get("/checks").json() == []
        client.post("/apply", json={})
        results = client.get("/checks").json()
        assert [(r["model"], r["check_name"], r["status"]) for r in results] == [("checked", "not_null_kind", "passed")]
        assert client.get("/checks", params={"model": "nope"}).json() == []


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
    with TestClient(app=create_app(project_dir, "dev")) as client:
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


def test_combined_daemon_executes_enqueued_runs(tmp_path: Path) -> None:
    import time

    app = create_app(_make_project(tmp_path), "dev", scheduler=True, scheduler_interval=0.05)
    with TestClient(app=app) as client:
        created = client.post("/runs", json={"selectors": ["+event_totals"]}).json()  # incl. ancestors
        assert created["enqueued"] == 1

        deadline = time.monotonic() + 10
        state = None
        while time.monotonic() < deadline:
            run = client.get("/runs").json()[0]
            state = run["state"]
            if state == "succeeded":
                break
            time.sleep(0.05)
        assert state == "succeeded"

        detail = client.get(f"/runs/{run['id']}").json()
        types = [e["type"] for e in detail["events"]]
        assert "run.started" in types and "run.succeeded" in types


async def test_auth_enforced_once_a_key_exists(tmp_path: Path) -> None:
    project_dir = _make_project(tmp_path)
    # create a read-only key out of band (same state DB the app will open)
    store = await Project.load(project_dir).open_state()
    read_key = await store.create_api_key("ci", ["read"])
    await store.close()

    with TestClient(app=create_app(project_dir, "dev")) as client:
        assert client.get("/models").status_code == 401  # now locked down
        assert client.get("/models", headers={"Authorization": f"Bearer {read_key}"}).status_code == 200
        # read scope can't trigger runs
        denied = client.post(
            "/runs", json={"selectors": ["raw_events"]}, headers={"Authorization": f"Bearer {read_key}"}
        )
        assert denied.status_code == 403
        assert client.get("/health").status_code == 200  # health stays open


def test_ui_loads_when_api_is_keyed(tmp_path: Path) -> None:
    """With keys configured the API locks down, but the shell itself still loads —
    it carries no data; every API call it makes enforces scopes."""
    import asyncio

    project_dir = _make_project(tmp_path)

    async def make_key() -> None:
        store = await Project.load(project_dir).open_state()
        try:
            await store.create_api_key("ui", ["read"])
        finally:
            await store.close()

    asyncio.run(make_key())
    with TestClient(app=create_app(project_dir, "dev")) as client:
        assert client.get("/ui/").status_code == 200  # shell is public
        assert client.get("/models").status_code == 401  # data is not
        assert client.get("/health").status_code == 200


def test_query_console_selects_and_refuses_writes(client: TestClient) -> None:
    client.post("/apply", json={"environment": "prod"})
    body = client.post("/query", json={"sql": "SELECT count(*) AS n FROM main.raw_events"}).json()
    assert body["columns"] == ["n"] and body["row_count"] == 1
    assert body["rows"][0][0] >= 1 and body["elapsed_ms"] >= 0

    limited = client.post("/query", json={"sql": "SELECT * FROM range(100) t(i)", "limit": 10}).json()
    assert limited["row_count"] == 10 and limited["truncated"] is True

    assert client.post("/query", json={"sql": "DROP TABLE main.raw_events"}).status_code == 400
    assert client.post("/query", json={"sql": "SELECT 1; SELECT 2"}).status_code == 400
    assert client.post("/query", json={"sql": "SELECT FROM WHERE"}).status_code == 400


def test_query_console_cannot_read_local_files(client: TestClient) -> None:
    """The console must never become a local-file reader or HTTP client, however the
    read is spelled — including DuckDB's dynamic-SQL query()/read_csv() escape hatches."""
    client.post("/apply", json={"environment": "prod"})
    bypasses = [
        "SELECT * FROM read_csv('/etc/hostname')",
        "SELECT * FROM read_parquet('/etc/hostname')",
        "SELECT * FROM query('SELECT * FROM read_text(''/etc/hostname'')')",  # deny-list bypass attempt
        "SELECT * FROM query_table('main.raw_events')",
        "SELECT * FROM glob('/etc/*')",
        "SELECT * FROM some_future_reader('/etc/hostname')",  # unknown table fn: the allowlist still blocks it
    ]
    for sql in bypasses:
        resp = client.post("/query", json={"sql": sql})
        assert resp.status_code == 400, f"leak not blocked: {sql}"


def test_query_console_does_not_poison_writes(client: TestClient) -> None:
    """Regression: a console query must not disable the warehouse's own file writes.
    The old sandbox set DuckDB's instance-wide, one-way enable_external_access on the
    shared connection, so the first /query bricked the flusher and every later apply."""
    client.post("/apply", json={"environment": "prod"})
    assert client.post("/query", json={"sql": "SELECT 1 AS x"}).status_code == 201  # the old trigger
    # a subsequent write path (apply builds/promotes over the file warehouse) still works
    assert client.post("/apply", json={"environment": "prod"}).status_code in (200, 201)
    assert client.post("/query", json={"sql": "SELECT count(*) FROM main.raw_events"}).status_code == 201


def test_engines_schedules_lineage_endpoints(client: TestClient) -> None:
    engines = client.get("/engines").json()
    assert any(e["default"] for e in engines)
    assert all("@" not in e["database"] or "…" in e["database"] for e in engines)

    assert isinstance(client.get("/schedules").json(), list)  # example has no schedules; shape only

    lineage = client.get("/lineage").json()
    names = {m["name"] for m in lineage["models"]}
    assert {"raw_events", "event_totals"} <= names
    assert ["raw_events", "event_totals"] in lineage["edges"]


def test_checks_run_endpoint(client: TestClient) -> None:
    assert client.post("/checks/run", json={}).status_code == 404  # nothing promoted yet
    client.post("/apply", json={"environment": "prod"})
    body = client.post("/checks/run", json={"environment": "prod"}).json()
    assert body["environment"] == "prod"
    assert body["passed"] == len(body["outcomes"]) and body["blocking_failures"] == 0


def test_apikey_lifecycle_over_http(client: TestClient) -> None:
    created = client.post("/apikeys", json={"name": "ci", "scopes": ["read"]}).json()
    assert created["token"].startswith("ilk_")
    # a key now exists: auth is enforced, so authenticate the remaining calls
    auth = {"Authorization": f"Bearer {created['token']}"}
    assert client.get("/models", headers=auth).status_code == 200
    admin = client.post("/apikeys", json={"name": "root", "scopes": ["admin"]}).status_code
    assert admin in (401, 403)  # a read key cannot mint keys

    # unauthenticated is refused once keys exist
    assert client.get("/models").status_code in (401, 403)


def test_apply_emits_per_model_progress_events(client: TestClient) -> None:
    client.post("/apply", json={"environment": "prod"})
    _wait_for(lambda: any(e["type"] == "model.done" for e in client.get("/events").json()))
    events = client.get("/events").json()
    started = {e["entity"] for e in events if e["type"] == "model.start"}
    finished = {e["entity"] for e in events if e["type"] == "model.done"}
    assert "raw_events" in started and started == finished  # every started model resolved


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


def test_rollback_over_http(client: TestClient) -> None:
    client.post("/apply", json={"environment": "prod"})
    # drift the project? no — promote twice by restating an apply after env drop of one model is
    # heavy here; instead promote once more via a scoped apply (same fingerprints -> still records
    # a generation only when something promotes). Use two applies with the seed project: the second
    # is a no-op plan, so force a second generation via the first apply of a sandbox is not needed —
    # single-generation environments refuse rollback with a clear message.
    history = client.get("/environments/prod/history").json()
    assert [g["generation"] for g in history] == [1]
    refused = client.post("/environments/prod/rollback", json={})
    assert refused.status_code == 400
    assert "valid targets" in refused.json()["detail"]
