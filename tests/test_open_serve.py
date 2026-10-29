"""An open daemon must not honour a cross-site write or a rebound Host."""

from __future__ import annotations

from pathlib import Path

import pytest
from litestar.testing import TestClient

from interlace.service.app import create_app
from interlace.service.auth import LOOPBACK_HOSTS

pytestmark = pytest.mark.unit


def _project(root: Path) -> None:
    (root / "models").mkdir(parents=True)
    (root / "models" / "n.sql").write_text("SELECT 1 AS id")


def test_text_plain_post_is_refused(tmp_path: Path) -> None:
    _project(tmp_path)
    app = create_app(tmp_path, "dev", allowed_hosts=LOOPBACK_HOSTS)
    with TestClient(app=app, base_url="http://127.0.0.1") as client:
        cross_site = client.post(
            "/reset",
            content=b'{"confirm": true}',
            headers={"content-type": "text/plain", "origin": "https://evil.example", "host": "127.0.0.1"},
        )
        assert cross_site.status_code == 403

        rebound = client.get("/models", headers={"host": "attacker.example"})
        assert rebound.status_code == 403

        ok = client.post(
            "/reset",
            json={"dry_run": True},
            headers={"host": "127.0.0.1", "origin": "http://127.0.0.1"},
        )
        assert ok.status_code == 201


def test_public_bind_requires_the_session_token(tmp_path: Path) -> None:
    _project(tmp_path)
    app = create_app(tmp_path, "dev", local_token="session-token-value")
    with TestClient(app=app) as client:
        missing = client.post("/reset", json={"dry_run": True})
        assert missing.status_code == 401
        sent = client.post(
            "/reset",
            json={"dry_run": True},
            headers={"x-interlace-token": "session-token-value"},
        )
        assert sent.status_code == 201
