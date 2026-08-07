# __PROJECT_NAME__ — GitHub issues → DuckDB with interlace

Pulls a repository's issues from the GitHub REST API into your warehouse,
**incrementally**, and models them with plain SQL:

```
github_issues (Python source: REST + pagination + merge-by-id, incremental)
  └─ issues_by_state (SQL: open vs closed, with checks)
```

## Run it

```bash
pip install "interlaced[sources]"      # the REST source client
export GITHUB_TOKEN=ghp_…              # optional but recommended (60/hr → 5,000/hr)
interlace apply                        # pull + build + promote
interlace query "SELECT * FROM issues_by_state"
```

`interlace apply` builds the model; to pull fresh data afterwards use **`interlace
run`** (or a schedule). It resumes from the newest `updated_at` already loaded
(GitHub's `since` filter) and **upserts by issue id**, so you only fetch what changed
and never get duplicates. Point it at any repo by editing `REPO` in
`models/github_issues.py`.

## How it works

`models/github_issues.py` is an ordinary `@model` — a *source* is just a model that
fetches and yields Arrow. The boilerplate lives in `interlace.sources`:

- **`RestClient`** — auth, retry with backoff (honours `Retry-After`), rate limiting.
- **`LinkHeader`** paginator — follows GitHub's `Link: …; rel="next"` until the end.
- **`batches(...)`** — turns each page of JSON into a `pyarrow.RecordBatch`.
- **`@model(cursor="updated_at", strategy="merge", key="id")`** — `cursor` is the max
  `updated_at` already loaded; `merge`/`key` makes the incremental re-load idempotent.

Swap `BearerAuth`/`NoAuth`, the paginator, or the columns to point the same pattern
at any REST API.
