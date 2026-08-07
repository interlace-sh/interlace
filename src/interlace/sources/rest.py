"""A small synchronous REST client for source models — auth, pagination, retry
with backoff, and rate limiting — that turns an API into Arrow.

A *source* in interlace is nothing new: an ordinary ``@model`` that pulls from an
API and yields ``pyarrow.RecordBatch``. This removes the per-source boilerplate so
the model body stays about the data, not about HTTP. Incremental pulls use the
model's reserved ``cursor`` parameter (the max of the cursor column in the previous
build) to fetch only what's new.

Synchronous on purpose: Python models run in a worker thread (see
``runtime/python_model``), so a blocking ``httpx.Client`` is the simplest correct
fit — no event-loop juggling. Pagination is sequential and *streaming* (a generator
of pages), so memory stays bounded however large the pull.
"""

from __future__ import annotations

import random
import time
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

import httpx
import pyarrow as pa

from interlace.sources.auth import Auth, NoAuth

DEFAULT_TIMEOUT = 30.0
_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
_BACKOFF_BASE = 0.5
_BACKOFF_CAP = 30.0


def _backoff(attempt: int) -> float:
    """Exponential backoff with full jitter — spreads retries so a fleet of
    workers doesn't synchronise its hammering of a recovering API."""
    ceiling = min(_BACKOFF_CAP, _BACKOFF_BASE * (2**attempt))
    return random.uniform(0, ceiling)  # noqa: S311 — jitter, not cryptographic


def _dig(body: Any, path: str) -> Any:
    node = body
    for part in path.split("."):
        node = node[part]
    return node


def _records(body: Any, data_key: str | None) -> list[Any]:
    """The records array out of a response body. ``data_key`` is a dotted path
    (``data``, ``result.items``); ``None`` means the body itself is the array."""
    if data_key is None:
        if isinstance(body, list):
            return body
        raise ValueError("response body is not a list — pass data_key to point at the records array")
    found = _dig(body, data_key)
    if not isinstance(found, list):
        raise ValueError(f"data_key {data_key!r} did not resolve to a list")
    return found


# ---- pagination ----------------------------------------------------------------


class Paginator:
    """Given a page's response, produce the ``(url, params)`` for the next page, or
    ``None`` once the last page has been seen."""

    def next(
        self, response: httpx.Response, body: Any, records: Sequence[Any], url: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        raise NotImplementedError


class SinglePage(Paginator):
    """One request, no pagination."""

    def next(
        self, response: httpx.Response, body: Any, records: Sequence[Any], url: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        return None


class PageNumber(Paginator):
    """``?page=N&per_page=SIZE`` — stop when a page comes back short (the last one)."""

    def __init__(self, *, page_param: str = "page", size_param: str = "per_page", size: int = 100) -> None:
        self._page_param = page_param
        self._size_param = size_param
        self._size = size

    def next(
        self, response: httpx.Response, body: Any, records: Sequence[Any], url: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        if len(records) < self._size:
            return None
        nxt = dict(params)
        nxt[self._page_param] = int(params.get(self._page_param, 1)) + 1
        nxt.setdefault(self._size_param, self._size)
        return url, nxt


class Offset(Paginator):
    """``?offset=N&limit=SIZE`` — advance by ``limit`` until a short page."""

    def __init__(self, *, offset_param: str = "offset", limit_param: str = "limit", limit: int = 100) -> None:
        self._offset_param = offset_param
        self._limit_param = limit_param
        self._limit = limit

    def next(
        self, response: httpx.Response, body: Any, records: Sequence[Any], url: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        if len(records) < self._limit:
            return None
        nxt = dict(params)
        nxt[self._offset_param] = int(params.get(self._offset_param, 0)) + self._limit
        nxt.setdefault(self._limit_param, self._limit)
        return url, nxt


class Cursor(Paginator):
    """A next-cursor token carried in the response body (``next_selector`` is a
    dotted path to it) and echoed back as ``cursor_param``. Stop when it's absent."""

    def __init__(self, *, cursor_param: str, next_selector: str) -> None:
        self._cursor_param = cursor_param
        self._next_selector = next_selector

    def next(
        self, response: httpx.Response, body: Any, records: Sequence[Any], url: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        try:
            token = _dig(body, self._next_selector)
        except (KeyError, TypeError):
            token = None
        if not token:
            return None
        nxt = dict(params)
        nxt[self._cursor_param] = token
        return url, nxt


class LinkHeader(Paginator):
    """RFC 5988 ``Link: <…>; rel="next"`` — GitHub and friends. The next URL is
    absolute and already carries its query, so params reset to empty."""

    def next(
        self, response: httpx.Response, body: Any, records: Sequence[Any], url: str, params: dict[str, Any]
    ) -> tuple[str, dict[str, Any]] | None:
        link = response.links.get("next")
        if not link or "url" not in link:
            return None
        return link["url"], {}


# ---- the client ----------------------------------------------------------------


class RestClient:
    """A configured REST endpoint: base URL + auth + a rate limit + retry policy.

    ``rate_limit`` is requests/second (``None`` = unthrottled). ``transport`` is a
    testing seam (pass an ``httpx.MockTransport`` to drive the client offline).
    """

    def __init__(
        self,
        base_url: str,
        *,
        auth: Auth | None = None,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        rate_limit: float | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int = 4,
        user_agent: str = "interlace-source/2",
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._auth = auth or NoAuth()
        self._max_retries = max_retries
        self._rate = rate_limit
        self._last_request = 0.0
        self._client = httpx.Client(
            base_url=base_url,
            headers={"User-Agent": user_agent, **dict(headers or {})},
            params=dict(params or {}),
            timeout=timeout,
            transport=transport,
            follow_redirects=True,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> RestClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _throttle(self) -> None:
        if not self._rate:
            return
        wait = self._last_request + (1.0 / self._rate) - time.monotonic()
        if wait > 0:
            time.sleep(wait)
        self._last_request = time.monotonic()

    def request(
        self, method: str, url: str, *, params: Mapping[str, Any] | None = None, json: Any = None
    ) -> httpx.Response:
        """One request with auth, rate limiting, and retry (network errors and
        429/5xx; honours ``Retry-After``). Raises for other 4xx — the caller's bug,
        not a transient fault."""
        merged: dict[str, Any] = {**self._auth.params(), **dict(params or {})}
        headers = self._auth.headers()
        attempt = 0
        while True:
            self._throttle()
            try:
                response = self._client.request(method, url, params=merged or None, headers=headers or None, json=json)
            except httpx.TransportError:
                if attempt >= self._max_retries:
                    raise
                time.sleep(_backoff(attempt))
                attempt += 1
                continue
            if response.status_code in _RETRYABLE_STATUS and attempt < self._max_retries:
                time.sleep(self._retry_after(response, attempt))
                attempt += 1
                continue
            response.raise_for_status()
            return response

    @staticmethod
    def _retry_after(response: httpx.Response, attempt: int) -> float:
        """Honour a ``Retry-After`` seconds value when the server sends one, else
        fall back to jittered backoff."""
        header = response.headers.get("Retry-After", "")
        if header.isdigit():
            return float(header)
        return _backoff(attempt)

    def get_json(self, url: str, *, params: Mapping[str, Any] | None = None) -> Any:
        """A single GET, decoded as JSON."""
        return self.request("GET", url, params=params).json()

    def paginate(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        paginator: Paginator | None = None,
        data_key: str | None = None,
    ) -> Iterator[list[Any]]:
        """Yield each page's records in turn, following ``paginator`` until it stops.
        Streaming: one page is held at a time."""
        pager = paginator or SinglePage()
        current = url
        page_params: dict[str, Any] = dict(params or {})
        while True:
            response = self.request("GET", current, params=page_params)
            body = response.json()
            records = _records(body, data_key)
            yield records
            nxt = pager.next(response, body, records, current, page_params)
            if nxt is None:
                return
            current, page_params = nxt

    def records(
        self,
        url: str,
        *,
        params: Mapping[str, Any] | None = None,
        paginator: Paginator | None = None,
        data_key: str | None = None,
    ) -> Iterator[Any]:
        """Flatten :meth:`paginate` into a stream of individual records."""
        for page in self.paginate(url, params=params, paginator=paginator, data_key=data_key):
            yield from page


def to_batch(
    records: Sequence[Any], *, columns: Sequence[str] | None = None, schema: pa.Schema | None = None
) -> pa.RecordBatch:
    """JSON records → one ``pyarrow.RecordBatch``. ``columns`` keeps just those top-
    level keys (missing ones become null); ``schema`` pins types (and lets an empty
    page produce a valid empty batch instead of failing type inference)."""
    rows = [{key: row.get(key) for key in columns} for row in records] if columns else list(records)
    if schema is not None:
        return pa.RecordBatch.from_pylist(rows, schema=schema)
    if not rows:
        raise ValueError("cannot infer a schema from zero records — pass schema= (or filter empty pages)")
    return pa.RecordBatch.from_pylist(rows)


def batches(
    pages: Iterator[list[Any]], *, columns: Sequence[str] | None = None, schema: pa.Schema | None = None
) -> Iterator[pa.RecordBatch]:
    """Turn a stream of record pages (from :meth:`RestClient.paginate`) into a stream
    of Arrow batches, one per non-empty page — the shape a ``@model`` yields."""
    for page in pages:
        if page or schema is not None:
            yield to_batch(page, columns=columns, schema=schema)
