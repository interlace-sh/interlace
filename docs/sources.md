# Source models — pulling from APIs

Streaming (`@stream`) is ingestion by **push**: something POSTs events to the daemon. A
**source model** is ingestion by **pull**: an ordinary `@model` that reaches out to an
external system, fetches, and yields Arrow. There is no new subsystem — a source is just a
model whose body happens to make network calls.

`interlace.sources` (the `interlaced[sources]` extra) removes the boilerplate every such model
would otherwise repeat: authentication, pagination, retry with backoff, and rate limiting. It
is a small **synchronous** REST client — Python models run in a worker thread, so a blocking
client is the simplest correct fit — that streams pages and hands you `pyarrow.RecordBatch`es.

```bash
pip install "interlaced[sources]"
```

```python
from interlace import model
from interlace.sources import RestClient, BearerAuth, NoAuth, LinkHeader, batches

@model(cursor="updated_at", strategy="merge", key="id")
def github_issues(cursor=None):
    auth = BearerAuth(env="GITHUB_TOKEN") if cursor is not None else NoAuth()
    params = {"state": "all", "per_page": 100, "sort": "updated", "direction": "asc"}
    if cursor:
        params["since"] = cursor                       # only what changed since last run
    with RestClient("https://api.github.com", auth=auth) as api:
        pages = api.paginate("/repos/duckdb/duckdb/issues", params=params, paginator=LinkHeader())
        yield from batches(pages, columns=["id", "number", "title", "state", "updated_at"])
```

`interlace init --template github` (a REST source) and `--template postgres` (a DB source, via
psycopg) scaffold complete, runnable versions of this pattern.

## Incremental and idempotent

A source uses the same incremental machinery as any Python model (the reserved `cursor`
parameter — see [Models](models.md)):

- `@model(cursor="<column>")` injects the **max value of that column already loaded** (or
  `None` on the first build) into the function's `cursor` parameter. Pass it to the API's
  "changed since" filter and each run fetches only new rows.
- `strategy="merge", key="<pk>"` **upserts** by primary key, so re-reading the boundary row
  (most "since" filters are inclusive) is idempotent — no duplicates, no lost updates.

Refresh is `interlace run` (or a schedule) — `apply` only rebuilds when a model's *code*
changes, not when the upstream data does.

## `RestClient`

```python
RestClient(
    base_url,
    *,
    auth=None,              # an Auth (default NoAuth)
    headers=None,           # extra default headers
    params=None,            # default query params on every request
    rate_limit=None,        # requests/second (None = unthrottled)
    timeout=30.0,
    max_retries=4,
    user_agent="interlace-source/2",
)
```

Use it as a context manager (`with RestClient(...) as api:`) so the connection closes.

| Method | Returns | Notes |
|---|---|---|
| `get_json(url, *, params=None)` | decoded JSON | a single request |
| `paginate(url, *, params=None, paginator=None, data_key=None)` | iterator of **pages** (each a `list` of records) | streams one page at a time — memory stays bounded |
| `records(url, ...)` | iterator of **records** | flattens `paginate` |

`data_key` selects the records array from a response body (a dotted path like `"data"` or
`"result.items"`); omit it when the body *is* the array.

**Retry & rate limiting.** Requests retry on network errors and `429`/`5xx` with exponential
backoff + jitter, honouring a `Retry-After` header; other `4xx` raise (that's your bug, not a
transient fault). `rate_limit` throttles to N requests/second.

## Pagination

Pass a paginator to `paginate`/`records`. It decides the next page from the response until
there is none.

| Paginator | Follows | For |
|---|---|---|
| `SinglePage()` | nothing (one request) | endpoints that return everything at once |
| `PageNumber(page_param="page", size_param="per_page", size=100)` | increments the page number until a short page | classic `?page=N` APIs |
| `Offset(offset_param="offset", limit_param="limit", limit=100)` | advances the offset by `limit` until a short page | `?offset=&limit=` APIs |
| `Cursor(cursor_param=..., next_selector=...)` | a next-cursor token read from the body (`next_selector` is a dotted path) | opaque-cursor APIs (Stripe, Slack) |
| `LinkHeader()` | the RFC 5988 `Link: …; rel="next"` header | GitHub and friends |

## Authentication

Each credential is a literal value **or** an `env=` variable name (so the secret stays out of
the repository — it's read from the environment at run time). A missing `env` var raises a
clear error rather than sending an empty credential.

| Auth | Sends | Example |
|---|---|---|
| `NoAuth()` | nothing (the default) | public APIs |
| `BearerAuth(token=None, env=None)` | `Authorization: Bearer <token>` | GitHub, Stripe, OAuth2 |
| `ApiKeyAuth(key=None, env=None, header="X-API-Key", param=None, scheme=None)` | a header, or a query param when `param` is set | key-in-header / key-in-query APIs |
| `BasicAuth(username, password)` | `Authorization: Basic …` | HTTP Basic |

## Records → Arrow

The client yields JSON; a model yields Arrow. Two helpers bridge it:

- `to_batch(records, *, columns=None, schema=None)` — one `pyarrow.RecordBatch` from a list of
  records. `columns` keeps just those top-level keys (missing ones become null); `schema` pins
  types (and lets an empty page produce a valid empty batch instead of failing type inference).
- `batches(pages, *, columns=None, schema=None)` — turns a stream of pages (from `paginate`)
  into a stream of batches, one per non-empty page — the shape a `@model` yields.

For non-REST sources (a database, a file), skip the client entirely: connect with the right
driver inside the model and yield `pyarrow.RecordBatch`es the same way — the `postgres`
template does exactly this with `psycopg`.
