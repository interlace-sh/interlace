# Fixture tests

`interlace test` builds the selected models in an ephemeral DuckDB and diffs the
result against `tests/golden/<model>.csv`. It does not use the warehouse, and it
does not run live checks or the promotion gate.

`tests/fixtures/<model>.csv` stands in for an upstream model, so that model is
loaded instead of built. The golden file's header is `column:type` and `\N` is
null. `--update-golden` rewrites the expected file. A mismatch exits non-zero.
`POST /tests/run` is the same check.

With no `--select`, the command tests every model that already has a golden file.
With neither goldens nor `--select`, it exits 1.
