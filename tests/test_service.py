"""HTTP API (Litestar) — exercised with the in-process test client."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from pathlib import Path

import pytest
from litestar.testing import TestClient

from interlace.project import Project
from interlace.service.app import create_app

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


def _make_project(tmp_path: Path) -> Path:
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir)
    return project_dir


@pytest.fixture()
def client(tmp_path: Path) -> Iterator[TestClient]:
    with TestClient(app=create_app(_make_project(tmp_path), "dev")) as test_client:
        yield test_client


def test_health(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}


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
    assert any(r["flow_selector"] == ["event_totals"] for r in runs)


def test_create_run_rejects_bad_selector(client: TestClient) -> None:
    assert client.post("/runs", json={"selectors": ["nope"]}).status_code == 400


def test_events_endpoint_records_enqueue(client: TestClient) -> None:
    assert client.get("/events").json() == []  # empty to start
    client.post("/runs", json={"selectors": ["raw_events"], "environment": "prod"})

    events = client.get("/events").json()
    assert [e["type"] for e in events] == ["run.enqueued"]
    assert events[0]["payload"] == {"models": ["raw_events"]}
    assert events[0]["seq"] == 1
    # replay from a cursor returns nothing new
    assert client.get("/events", params={"after": events[0]["seq"]}).json() == []


def test_openapi_and_scalar_docs(client: TestClient) -> None:
    schema = client.get("/schema/openapi.json").json()
    assert "/models" in schema["paths"]
    assert "/runs" in schema["paths"]
    assert client.get("/schema/scalar").status_code == 200  # Scalar UI


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
