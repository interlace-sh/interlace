"""MCP stdio server: tool list, plan-before-apply, and framing."""

from __future__ import annotations

import json
import shutil
from io import BytesIO
from pathlib import Path

from interlace.mcp_server import _read_message, _write_message, handle

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "getting_started"


def _project(tmp_path: Path) -> Path:
    project_dir = tmp_path / "getting_started"
    shutil.copytree(EXAMPLE, project_dir, ignore=shutil.ignore_patterns(".interlace"))
    return project_dir


def _call(path: Path, method: str, params: dict | None = None, msg_id: int = 1) -> dict:
    response = handle(path, {"jsonrpc": "2.0", "id": msg_id, "method": method, "params": params or {}})
    assert response is not None
    return response


def test_initialize_and_tool_list(tmp_path: Path) -> None:
    path = _project(tmp_path)
    hello = _call(path, "initialize", {"protocolVersion": "2024-11-05"})
    assert hello["result"]["serverInfo"]["name"] == "interlace"
    assert hello["result"]["protocolVersion"] == "2024-11-05"

    listed = _call(path, "tools/list", msg_id=2)
    names = {tool["name"] for tool in listed["result"]["tools"]}
    assert {
        "list_models",
        "preview_model",
        "plan",
        "apply",
        "query",
        "lineage",
        "list_checks",
        "failing_rows",
        "list_runs",
    } <= names


def test_apply_without_confirm_does_not_build(tmp_path: Path) -> None:
    path = _project(tmp_path)
    response = _call(path, "tools/call", {"name": "apply", "arguments": {}})
    assert response["result"]["isError"] is True
    assert "confirm" in response["result"]["content"][0]["text"]
    assert not (path / ".interlace").exists()


def test_list_models(tmp_path: Path) -> None:
    path = _project(tmp_path)
    response = _call(path, "tools/call", {"name": "list_models", "arguments": {}})
    text = response["result"]["content"][0]["text"]
    assert "raw_events" in text
    assert response["result"].get("isError") is not True


def test_notifications_have_no_response(tmp_path: Path) -> None:
    path = _project(tmp_path)
    assert handle(path, {"jsonrpc": "2.0", "method": "notifications/initialized"}) is None


def test_content_length_framing_roundtrip() -> None:
    payload = {"jsonrpc": "2.0", "id": 1, "method": "ping"}
    buffer = BytesIO()
    _write_message(buffer, payload)
    buffer.seek(0)
    assert _read_message(buffer) == payload
    # a second read hits EOF
    assert _read_message(buffer) is None
    json.dumps(payload)  # payload stays plain JSON; the frame is only on the wire
