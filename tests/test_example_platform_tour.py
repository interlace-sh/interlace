"""The platform_tour example runs end-to-end: stream ingest, scd2, a Python
model, checks, and the reverse-ETL sink into an attached database."""

from __future__ import annotations

import shutil
from pathlib import Path

import duckdb
import pytest
from litestar.testing import TestClient

from interlace.service.app import create_app

pytestmark = pytest.mark.unit

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "platform_tour"


def test_platform_tour_end_to_end(tmp_path: Path) -> None:
    project_dir = tmp_path / "platform_tour"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace", "*.duckdb*"))

    with TestClient(app=create_app(project_dir, "dev")) as client:
        # ingest: durable, deduplicated
        first = client.post("/streams/orders", json={"order_id": "o1", "customer_id": 1, "total": 49.5}).json()
        assert first["accepted"] == 1 and first["materialized"] == 1
        retry = client.post(
            "/streams/orders",
            json=[
                {"order_id": "o1", "customer_id": 1, "total": 49.5},  # retry: deduped
                {"order_id": "o2", "customer_id": 2, "total": 12.0},
            ],
        ).json()
        assert retry["deduplicated"] == 1 and retry["accepted"] == 1

        # build everything: seed, scd2 dimension, stream aggregate, python model, sink
        applied = client.post("/apply", json={}).json()
        assert set(applied["built"]) >= {
            "raw_customers",
            "dim_customers",
            "order_stats",
            "customer_value",
            "crm_push",
        }

        # checks ran and gate promotion
        checks = client.get("/checks").json()
        assert {(c["model"], c["status"]) for c in checks} >= {
            ("dim_customers", "passed"),
            ("order_stats", "passed"),
        }

        # the stream aggregate saw both events
        stats = client.get("/models/order_stats").json()
        assert stats["depends_on"] == []  # external table ref, not a model dependency

    # reverse ETL: the sink upserted into the attached CRM database
    external = duckdb.connect(str(project_dir / "crm.duckdb"))
    rows = external.execute("SELECT customer_id, score FROM customer_scores ORDER BY customer_id").fetchall()
    external.close()
    assert [r[0] for r in rows] == [1, 2, 3]
    assert all(score is not None for _, score in rows)
