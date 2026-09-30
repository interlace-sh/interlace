"""Helpers shared by the HTTP service tests."""

from __future__ import annotations

import asyncio
import shutil
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from litestar.testing import TestClient

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


def _sse_frames(buffer: str) -> tuple[list[str], str]:
    """Split a byte stream of SSE into frames. Litestar separates frames with CRLF."""
    buffer = buffer.replace("\r\n", "\n")
    frames: list[str] = []
    while "\n\n" in buffer:
        frame, buffer = buffer.split("\n\n", 1)
        frames.append(frame)
    return frames, buffer


def _parse_sse(frame: str) -> dict[str, str]:
    record: dict[str, str] = {}
    for line in frame.splitlines():
        if not line or line.startswith(":"):
            continue
        key, _, value = line.partition(":")
        record[key] = value.lstrip()
    return record


async def _drive_sse(  # noqa: C901
    app: object,
    path: str,
    *,
    query: str = "",
    headers: list[tuple[bytes, bytes]] | None = None,
    stop_after: int,
    on_ready: Callable[[list[dict[str, str]]], Awaitable[None]] | None = None,
) -> tuple[int, dict[str, str], str, list[dict[str, str]]]:
    """Read an SSE response as frames arrive, then cancel it.

    Litestar's TestClient and httpx's ASGI transport both buffer until the body
    ends, and this tail does not end. ``stop_after`` counts data frames; 0 stops
    at the opening comment. ``on_ready`` runs before the tail is cancelled, so a
    commit can land while the lease is still held.
    """
    queue: asyncio.Queue[bytes | None] = asyncio.Queue()
    status = 0
    response_headers: dict[str, str] = {}
    request_sent = False

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": query.encode(),
        "headers": [(b"host", b"testserver"), *(headers or [])],
        "client": ("127.0.0.1", 123),
        "server": ("testserver", 80),
        "root_path": "",
    }

    async def receive() -> dict[str, object]:
        nonlocal request_sent
        if not request_sent:
            request_sent = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await asyncio.Event().wait()
        return {"type": "http.disconnect"}

    async def send(message: dict[str, object]) -> None:
        nonlocal status
        if message["type"] == "http.response.start":
            status = int(message["status"])  # type: ignore[arg-type]
            raw_headers = message.get("headers", [])
            if isinstance(raw_headers, list):
                response_headers.update({key.decode().lower(): value.decode() for key, value in raw_headers})
        elif message["type"] == "http.response.body":
            body = message.get("body", b"")
            if isinstance(body, bytes) and body:
                await queue.put(body)
            if not message.get("more_body", False):
                await queue.put(None)

    task = asyncio.create_task(app(scope, receive, send))  # type: ignore[operator]
    records: list[dict[str, str]] = []
    first = ""
    buffer = ""
    try:
        while True:
            try:
                chunk = await asyncio.wait_for(queue.get(), timeout=5)
            except TimeoutError:
                if task.done():
                    error = task.exception()
                    if error is not None:
                        raise error from None
                break
            if chunk is None:
                break
            buffer += chunk.decode()
            frames, buffer = _sse_frames(buffer)
            for frame in frames:
                if not first:
                    first = frame
                record = _parse_sse(frame)
                if "data" in record:
                    records.append(record)
            if (stop_after == 0 and first) or len(records) >= stop_after:
                if on_ready is not None:
                    await on_ready(records)
                break
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    return status, response_headers, first, records
