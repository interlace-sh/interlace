-- dbt builds this with {{ dbt.date_spine(...) }}, a cross-database macro. interlace
-- transpiles one dialect to another with sqlglot instead of templating around them,
-- so the model is the engine's own date spine.
--
-- Nothing downstream reads it: it exists for MetricFlow, dbt's semantic layer, which
-- has no interlace equivalent. It converts, and then it sits there — see the README.
select cast(unnest(generate_series(date '2000-01-01', date '2030-01-01', interval 1 day)) as date) as date_day
