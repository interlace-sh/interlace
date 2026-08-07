"""Reusable machinery for building *source* models — pulls from external systems.

A source in interlace is an ordinary ``@model`` that fetches data and yields Arrow;
this package removes the boilerplate. Today it ships a synchronous REST client with
auth, pagination, retry/backoff, and rate limiting (:mod:`interlace.sources.rest`),
plus the auth strategies (:mod:`interlace.sources.auth`). Requires the ``sources``
extra: ``pip install "interlaced[sources]"``.

Typical incremental source model::

    from interlace import model
    from interlace.sources import RestClient, LinkHeader, batches

    @model(cursor="updated_at")  # resume from the max updated_at already loaded
    def github_issues(cursor=None):
        with RestClient("https://api.github.com") as api:
            params = {"since": cursor, "state": "all", "per_page": 100} if cursor else {"per_page": 100}
            pages = api.paginate("/repos/duckdb/duckdb/issues", params=params, paginator=LinkHeader())
            yield from batches(pages, columns=["id", "number", "title", "state", "updated_at"])
"""

from __future__ import annotations

from interlace.sources.auth import ApiKeyAuth, Auth, BasicAuth, BearerAuth, NoAuth
from interlace.sources.rest import (
    Cursor,
    LinkHeader,
    Offset,
    PageNumber,
    Paginator,
    RestClient,
    SinglePage,
    batches,
    to_batch,
)

__all__ = [
    "ApiKeyAuth",
    "Auth",
    "BasicAuth",
    "BearerAuth",
    "NoAuth",
    "Cursor",
    "LinkHeader",
    "Offset",
    "PageNumber",
    "Paginator",
    "RestClient",
    "SinglePage",
    "batches",
    "to_batch",
]
