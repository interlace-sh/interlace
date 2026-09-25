"""Named connections: env interpolation, redaction, and the build-time helper."""

from __future__ import annotations

from pathlib import Path

import pytest

from interlace.config.config import HttpConnection, PostgresConnection, load_config
from interlace.connections import bind_connections, connection, redacted, unbind_connections
from interlace.exceptions import ConfigurationError

pytestmark = pytest.mark.unit


def test_missing_connection_variable_is_a_config_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NO_SUCH_TOKEN", raising=False)
    (tmp_path / "interlace.yaml").write_text(
        "connections:\n"
        "  api:\n"
        "    type: http\n"
        "    base_url: https://example.com\n"
        "    headers: {Authorization: 'Bearer ${NO_SUCH_TOKEN}'}\n"
    )
    with pytest.raises(ConfigurationError, match="invalid config") as raised:
        load_config(tmp_path / "interlace.yaml")
    assert "NO_SUCH_TOKEN" in str(raised.value.details)


def test_connection_resolves_while_bound_and_redacts_secrets() -> None:
    http = HttpConnection(
        type="http",
        base_url="https://example.com",
        headers={"Authorization": "Bearer secret", "Accept": "application/json"},
    )
    token = bind_connections({"api": http})
    try:
        assert connection("api") is http or connection("api").base_url == "https://example.com"
        assert connection("api").base_url == "https://example.com"
    finally:
        unbind_connections(token)
    with pytest.raises(ConfigurationError, match="not available"):
        connection("api")
    public = redacted("api", http)
    assert public["headers"]["Authorization"] == "…"
    assert public["headers"]["Accept"] == "application/json"
    pg = PostgresConnection(type="postgres", dsn="postgresql://user:secret@db.internal:5432/app")
    assert "secret" not in redacted("src", pg)["dsn"]


def test_cli_lists_connections_with_secrets_redacted(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    from typer.testing import CliRunner

    from interlace.cli.main import app

    monkeypatch.setenv("BILLING_TOKEN", "super-secret")
    (tmp_path / "interlace.yaml").write_text(
        "connections:\n"
        "  billing:\n"
        "    type: http\n"
        "    base_url: https://api.example.com\n"
        "    headers: {Authorization: 'Bearer ${BILLING_TOKEN}', Accept: application/json}\n"
    )
    result = CliRunner().invoke(app, ["connections", "--path", str(tmp_path), "--json"])
    assert result.exit_code == 0, result.output
    body = json.loads(result.stdout)
    assert body[0]["name"] == "billing"
    assert body[0]["headers"]["Authorization"] == "…"
    assert "super-secret" not in result.stdout
