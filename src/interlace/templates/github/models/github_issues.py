"""Pull a repository's issues from the GitHub REST API — incrementally.

This is a *source* model: an ordinary ``@model`` that fetches from an API and yields
Arrow. The heavy lifting (auth, pagination, retry, rate limiting) is the shared
``interlace.sources`` client — the body stays about the data.

Incremental by design:
- ``@model(cursor="updated_at")`` injects the max ``updated_at`` already loaded (or
  ``None`` on the first build), and we pass it to GitHub's ``since`` filter — so each
  run fetches only issues touched since last time.
- ``strategy="merge", key="id"`` upserts by issue id, so re-fetching the boundary
  row (``since`` is inclusive) is idempotent — no duplicates, no lost updates.

Auth is optional: unauthenticated works but GitHub caps it at ~60 requests/hour, so
set ``GITHUB_TOKEN`` (a fine-grained or classic PAT) to lift it to 5,000/hour. Point
it at any repo by editing ``REPO``. If the rate limit is hit mid-pull, the model keeps
what it loaded and stops cleanly — because it's incremental and merges by id, the next
``interlace run`` simply resumes from where it left off.

Requires the sources extra:  pip install "interlaced[sources]"
"""

import logging
import os

import httpx
import pyarrow as pa

from interlace import model
from interlace.sources import BearerAuth, LinkHeader, NoAuth, RestClient, batches

REPO = "duckdb/ducklake"  # owner/name — change me
_log = logging.getLogger("interlace.templates.github")


def _rate_limited(response: httpx.Response) -> bool:
    """GitHub signals its primary rate limit with 403 (or 429) + a spent quota header."""
    return response.status_code in (403, 429) and response.headers.get("X-RateLimit-Remaining") == "0"

# A fixed schema keeps types stable across runs (and lets an empty incremental page
# still produce a valid, typed batch). GitHub's /issues also returns pull requests;
# a downstream model can filter them out if you only want issues.
COLUMNS = ["id", "number", "title", "state", "comments", "user", "created_at", "updated_at"]
SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("number", pa.int64()),
        ("title", pa.string()),
        ("state", pa.string()),
        ("comments", pa.int64()),
        ("user", pa.string()),  # flattened from the nested user object below
        ("created_at", pa.string()),
        ("updated_at", pa.string()),
    ]
)


def _flatten(issue: dict) -> dict:
    """Lift the fields we keep to the top level (the API nests author under `user`)."""
    return {**issue, "user": (issue.get("user") or {}).get("login")}


@model(cursor="updated_at", strategy="merge", key="id")
def github_issues(cursor=None):
    auth = BearerAuth(env="GITHUB_TOKEN") if os.environ.get("GITHUB_TOKEN") else NoAuth()
    params = {"state": "all", "per_page": 100, "sort": "updated", "direction": "asc"}
    if cursor:
        params["since"] = cursor  # only issues updated at/after the newest we've loaded
    with RestClient("https://api.github.com", auth=auth) as api:
        pages = (
            [_flatten(issue) for issue in page]
            for page in api.paginate(f"/repos/{REPO}/issues", params=params, paginator=LinkHeader())
        )
        try:
            yield from batches(pages, columns=COLUMNS, schema=SCHEMA)
        except httpx.HTTPStatusError as exc:
            if not _rate_limited(exc.response):
                raise
            # keep what we've loaded; merge + the updated_at cursor resume the rest next run
            _log.warning(
                "GitHub rate limit hit for %s — loaded up to here; re-run to continue "
                "(set GITHUB_TOKEN to raise 60 -> 5,000 requests/hour).",
                REPO,
            )
