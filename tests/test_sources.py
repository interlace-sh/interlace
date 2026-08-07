"""The REST source client — auth, pagination, retry, and Arrow conversion.

Driven entirely through an ``httpx.MockTransport`` so the suite is offline and
deterministic; no network, no real sleeps (backoff is monkeypatched out).
"""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pyarrow as pa
import pytest

from interlace.sources import (
    ApiKeyAuth,
    BasicAuth,
    BearerAuth,
    Cursor,
    LinkHeader,
    Offset,
    PageNumber,
    RestClient,
    batches,
    to_batch,
)

pytestmark = pytest.mark.unit


def _client(handler: Callable[[httpx.Request], httpx.Response], **kwargs: object) -> RestClient:
    return RestClient("https://api.test", transport=httpx.MockTransport(handler), **kwargs)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("interlace.sources.rest.time.sleep", lambda _seconds: None)


# ---- auth ----------------------------------------------------------------------


def test_bearer_auth_sets_header() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization", "")
        return httpx.Response(200, json=[])

    _client(handler, auth=BearerAuth("tok123")).get_json("/x")
    assert seen["auth"] == "Bearer tok123"


def test_bearer_auth_reads_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_TOKEN", "from-env")
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization", "")
        return httpx.Response(200, json=[])

    _client(handler, auth=BearerAuth(env="MY_TOKEN")).get_json("/x")
    assert seen["auth"] == "Bearer from-env"


def test_missing_env_credential_is_a_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ABSENT_KEY", raising=False)
    with pytest.raises(ValueError, match="ABSENT_KEY"):
        _client(lambda r: httpx.Response(200, json=[]), auth=BearerAuth(env="ABSENT_KEY")).get_json("/x")


def test_api_key_in_header_and_in_query_param() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["header"] = request.headers.get("X-API-Key", "")
        seen["query"] = request.url.params.get("api_key", "")
        return httpx.Response(200, json=[])

    _client(handler, auth=ApiKeyAuth("hk")).get_json("/x")
    assert seen["header"] == "hk" and seen["query"] == ""

    _client(handler, auth=ApiKeyAuth("qk", param="api_key")).get_json("/x")
    assert seen["query"] == "qk"


def test_basic_auth_encodes_credentials() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("Authorization", "")
        return httpx.Response(200, json=[])

    _client(handler, auth=BasicAuth("user", "pass")).get_json("/x")
    assert seen["auth"] == "Basic dXNlcjpwYXNz"  # base64("user:pass")


def test_default_user_agent_is_sent() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["ua"] = request.headers.get("User-Agent", "")
        return httpx.Response(200, json=[])

    _client(handler).get_json("/x")
    assert "interlace" in seen["ua"]


# ---- pagination ----------------------------------------------------------------


def test_page_number_paginator_walks_until_short_page() -> None:
    pages = {1: [{"i": 1}, {"i": 2}], 2: [{"i": 3}, {"i": 4}], 3: [{"i": 5}]}  # size 2; page 3 is short → stop

    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params.get("page", "1"))
        return httpx.Response(200, json=pages[page])

    rows = list(_client(handler).records("/items", params={"page": 1}, paginator=PageNumber(size=2)))
    assert [r["i"] for r in rows] == [1, 2, 3, 4, 5]


def test_offset_paginator_advances_by_limit() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        offset = int(request.url.params.get("offset", "0"))
        data = [{"i": offset}, {"i": offset + 1}] if offset < 4 else [{"i": offset}]  # short page at offset 4
        return httpx.Response(200, json=data)

    rows = list(_client(handler).records("/items", paginator=Offset(limit=2)))
    assert [r["i"] for r in rows] == [0, 1, 2, 3, 4]


def test_cursor_paginator_follows_body_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        after = request.url.params.get("after")
        if after is None:
            return httpx.Response(200, json={"data": [{"i": 1}], "next": "abc"})
        if after == "abc":
            return httpx.Response(200, json={"data": [{"i": 2}], "next": None})
        raise AssertionError(f"unexpected cursor {after!r}")

    rows = list(
        _client(handler).records(
            "/items", paginator=Cursor(cursor_param="after", next_selector="next"), data_key="data"
        )
    )
    assert [r["i"] for r in rows] == [1, 2]


def test_link_header_paginator_follows_next() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("page") == "2":
            return httpx.Response(200, json=[{"i": 2}])
        headers = {"Link": '<https://api.test/items?page=2>; rel="next"'}
        return httpx.Response(200, json=[{"i": 1}], headers=headers)

    rows = list(_client(handler).records("/items", paginator=LinkHeader()))
    assert [r["i"] for r in rows] == [1, 2]


def test_data_key_selects_nested_records() -> None:
    handler = lambda r: httpx.Response(200, json={"result": {"items": [{"i": 1}]}})  # noqa: E731
    rows = list(_client(handler).records("/x", data_key="result.items"))
    assert rows == [{"i": 1}]


# ---- retry ---------------------------------------------------------------------


def test_retries_transient_status_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json=[{"ok": True}])

    assert _client(handler).get_json("/x") == [{"ok": True}]
    assert calls["n"] == 3


def test_retries_on_network_error_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200, json=[])

    _client(handler).get_json("/x")
    assert calls["n"] == 2


def test_gives_up_after_max_retries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    with pytest.raises(httpx.HTTPStatusError):
        _client(handler, max_retries=2).get_json("/x")


def test_honours_retry_after_header(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    monkeypatch.setattr("interlace.sources.rest.time.sleep", lambda seconds: slept.append(seconds))
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "7"})
        return httpx.Response(200, json=[])

    _client(handler).get_json("/x")
    assert slept == [7.0]  # the server's Retry-After, not jittered backoff


def test_non_retryable_4xx_raises_immediately() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404)

    with pytest.raises(httpx.HTTPStatusError):
        _client(handler).get_json("/missing")
    assert calls["n"] == 1  # 404 is the caller's bug — no retry


# ---- Arrow conversion ----------------------------------------------------------


def test_to_batch_selects_columns_and_fills_missing_with_null() -> None:
    batch = to_batch([{"a": 1, "b": 2, "junk": 9}, {"a": 3}], columns=["a", "b"])
    assert batch.schema.names == ["a", "b"]
    assert batch.to_pylist() == [{"a": 1, "b": 2}, {"a": 3, "b": None}]


def test_batches_skips_empty_pages_but_honours_schema() -> None:
    schema = pa.schema([("a", pa.int64())])
    produced = list(batches(iter([[{"a": 1}], [], [{"a": 2}]])))
    assert [b.num_rows for b in produced] == [1, 1]  # the empty page is skipped

    with_schema = list(batches(iter([[]]), schema=schema))
    assert len(with_schema) == 1 and with_schema[0].num_rows == 0  # schema lets an empty page through


def test_to_batch_empty_without_schema_is_a_clear_error() -> None:
    with pytest.raises(ValueError, match="infer a schema"):
        to_batch([])
