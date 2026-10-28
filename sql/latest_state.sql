-- Newest row per customer via ROW_NUMBER. Columns are listed explicitly to drop rn.
-- Ordering key: event_timestamp, event_sequence (descending here, to pick the newest).
-- engines: duckdb, spark, trino, athena
-- Plain ROW_NUMBER, so the same text runs on every engine listed.
-- CI checks DuckDB, Spark and Trino. This file was run on Athena engine version 3
-- against the golden outputs (results/athena_sql_check.md); Athena is not in CI.
-- On sparse events the newest row holds nulls for untouched fields; use
-- sparse_current_value.sql for the last known value of each field.

SELECT
  customer_id,
  event_timestamp,
  event_sequence,
  email,
  phone,
  address
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY event_timestamp DESC, event_sequence DESC
    ) AS rn
  FROM customer_profile_changes
) t
WHERE rn = 1;
