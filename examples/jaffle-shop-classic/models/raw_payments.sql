-- dbt loads this CSV with `dbt seed`, a separate concept and a separate command.
-- interlace has no seed concept: a seed is a model with no upstreams. The CSV joins
-- the DAG, gets a fingerprint, and rebuilds what depends on it when it changes.
SELECT * FROM read_csv_auto('seeds/raw_payments.csv')
