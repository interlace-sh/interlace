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
it at any repo by editing ``REPO``.

Requires the sources extra:  pip install "interlaced[sources]"
"""

import os

import pyarrow as pa

from interlace import model
from interlace.sources import BearerAuth, LinkHeader, NoAuth, RestClient, batches

REPO = "duckdb/duckdb"  # owner/name — change me

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
        yield from batches(pages, columns=COLUMNS, schema=SCHEMA)
